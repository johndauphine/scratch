import csv
import io
import os
import time
import sys
import argparse
import logging
import boto3
from urllib.parse import urlparse

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

def get_athena_table_row_count_after_date(
    database,
    table,
    date_column,
    after_date,
    output_bucket,
    region="us-east-1"
):
    """
    Returns the row count of a given table from AWS Athena,
    counting only rows where 'date_column' > 'after_date'.

    Logs and raises an exception if the Athena query fails,
    including the reason from Athena's StateChangeReason.
    """
    logger = logging.getLogger(__name__)
    athena_client = boto3.client("athena", region_name=region)

    # Using CAST(... AS DATE) = DATE 'YYYY-MM-DD' for the filter
    query_string = (
        f"SELECT COUNT(*) AS total "
        f"FROM {database}.{table} "
        f"WHERE CAST({date_column} AS DATE) = DATE '{after_date}'"
    )

    logger.info(f"Starting Athena query {query_string} for rows on {after_date} in {database}.{table}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{output_bucket}/"}
    )

    query_execution_id = response["QueryExecutionId"]

    # Poll until SUCCEEDED, FAILED, or CANCELLED
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status_info = query_status["QueryExecution"]["Status"]
        status = status_info["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)

    # If not succeeded, retrieve the reason for failure/cancellation
    if status != "SUCCEEDED":
        reason = status_info.get("StateChangeReason", "No reason provided by Athena.")
        logger.exception(f"Athena query failed. Status: {status}, Reason: {reason}")
        raise Exception(f"Athena query failed or was cancelled. Reason: {reason}")

    logger.info(f"Athena query succeeded for {database}.{table}. Fetching results...")
    result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # The first row is usually the header; the second row (index 1) has the actual count
    row_count_str = result_response["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
    return int(row_count_str)


def move_s3_object(s3_client, source_bucket, source_key, destination_bucket, destination_key):
    """
    Copies an object from one S3 location to another, then deletes the source.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Copying s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{destination_key}")
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    try:
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=destination_key
        )
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info("Copy and delete completed.")
    except Exception:
        logger.exception("Error copying/deleting S3 object.")
        raise


def construct_processed_key(source_key):
    """
    Construct the processed S3 key by placing the file under a 'processed' directory
    while keeping the original file name.
    """
    parts = source_key.split('/')
    if len(parts) <= 1:
        filename = source_key
        return f"processed/{filename}"
    else:
        top_level = parts[0]
        filename = parts[-1]
        return f"{top_level}/processed/{filename}"


def upload_csv_report_to_s3(s3_client, rows, report_s3_uri):
    """
    Writes the reconciliation report (CSV) to the specified S3 URI.
    """
    logger = logging.getLogger(__name__)
    parsed_uri = urlparse(report_s3_uri, allow_fragments=False)
    dest_bucket = parsed_uri.netloc
    dest_key = parsed_uri.path.lstrip("/")

    # The columns in your final reconciliation CSV
    fieldnames = [
        "EventDate",
        "BusinessUnit",
        "ObjectType",
        "AthenaTable",
        "SFMCRowCount",
        "AthenaRowCount",
        "Difference",
        "Status",          # Match, NoMatch, or Error
        "ErrorDescription" # store the error if Status=Error, else None
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for row in rows:
        writer.writerow(row)

    csv_bytes = output.getvalue().encode("utf-16")

    logger.info(f"Uploading reconciliation report to {report_s3_uri}")
    try:
        s3_client.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=csv_bytes
        )
        logger.info("Reconciliation report uploaded successfully.")
    except Exception:
        logger.exception("Failed to upload reconciliation report.")
        raise


def main():
    """
    Main script for SFMC Reconciliation with final CSV report.

    Changes:
      - Now includes 'Status' column that can be "Match", "NoMatch", or "Error".
      - New column 'ErrorDescription' to store Athena query error or 'None' if no error.
    """
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="SFMC Reconciliation with Athena error reasons.")

    # Required arguments
    parser.add_argument("--landing-s3-uri", required=True,
                        help="S3 URI to the CSV file to process. E.g., s3://my-bucket/sfmc/daily-row-counts/file.csv")
    parser.add_argument("--athena-output-bucket", required=True,
                        help="S3 bucket (no 's3://') for Athena query results.")
    parser.add_argument("--sns-topic-arn", required=True,
                        help="SNS topic ARN for mismatch notifications.")
    parser.add_argument("--athena-date-column", required=True,
                        help="Name of the date column in Athena table.")
    parser.add_argument("--report-s3-uri", required=True,
                        help="S3 URI where the final reconciliation report CSV will be uploaded.")
    parser.add_argument("--athena-database", required=True,
                        help="Name of the Athena database to query.")

    # Optional arguments
    parser.add_argument("--region", default="us-east-1",
                        help="AWS region for Athena / SNS (default: us-east-1)")
    parser.add_argument("--threshold", type=float, default=10.0,
                        help="Percent difference threshold for mismatch.")
    parser.add_argument("--athena-table-pattern", default="bronze_{business_unit}_{object}_delta",
                        help=(
                            "Pattern for the Athena table name. "
                            "Placeholders '{business_unit}' and '{object}' will be replaced by CSV values. "
                            "Default: 'bronze_{business_unit}_{object}_delta'."
                        ))

    args = parser.parse_args()

    # Extract parameters
    landing_s3_uri = args.landing_s3_uri
    athena_output_bucket = args.athena_output_bucket
    sns_topic_arn = args.sns_topic_arn
    date_column = args.athena_date_column
    report_s3_uri = args.report_s3_uri
    athena_database = args.athena_database
    region = args.region
    threshold = args.threshold
    table_pattern = args.athena_table_pattern

    s3_client = boto3.client("s3", region_name=region)
    sns_client = boto3.client("sns", region_name=region)

    # Parse the landing S3 URI
    parsed_uri = urlparse(landing_s3_uri, allow_fragments=False)
    source_bucket = parsed_uri.netloc
    source_key = parsed_uri.path.lstrip("/")
    destination_bucket = source_bucket
    destination_key = construct_processed_key(source_key)

    # 1. Download CSV file from S3
    logger.info(f"Downloading CSV file from s3://{source_bucket}/{source_key}")
    try:
        csv_obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    except Exception:
        logger.exception(f"Could not fetch CSV from {landing_s3_uri}.")
        sys.exit(1)

    try:
        csv_content = csv_obj["Body"].read().decode("utf-16")
    except UnicodeDecodeError:
        # If you suspect another encoding, handle it here or log the error
        logger.exception("Failed to decode CSV as UTF-16. Check file encoding.")
        sys.exit(1)

    logger.info("CSV file downloaded and decoded successfully.")

    mismatches = []
    report_rows = []
    reader = csv.DictReader(io.StringIO(csv_content))

    for row in reader:
        number = row.get("Number")
        mid = row.get("MID")
        business_unit = row.get("BusinessUnit")
        object_type = row.get("ObjectType")
        processing_date = row.get("ProcessingDate")
        event_date_str = row.get("EventDate")
        row_count_str = row.get("RowCount", "0")

        # Optional transformations
        if business_unit and business_unit.lower() == "lennar corporation":
            business_unit = "lennar"

        if business_unit and business_unit.lower() == "california coastal":
            business_unit = "cal coastal"

        # Convert row_count to int
        try:
            sfmc_row_count = int(row_count_str)
        except ValueError:
            logger.error(f"Invalid RowCount '{row_count_str}' for row {number} (MID={mid}). Skipping.")
            continue

        if not event_date_str:
            logger.error(f"No EventDate found in row {number} (MID={mid}). Skipping.")
            continue

        # Build table name
        sanitized_bu = (business_unit.lower().replace(" ", "").replace("-","")) if business_unit else "unknown"
        object_dictionary ={
            'Bounce': 'BounceEvent',
            'Click': 'ClickEvent',
            'Email': 'Email',
            'ForwardedEmail': 'ForwardedEmailEvent',
            'NotSent': 'NotSentEvent',
            'Open': 'OpenEvent',
            'Send': 'Send',
            'Sent': 'SentEvent',
            'Unsubscribe': 'UnsubEvent'
        }
        # Look up object type in dictionary, if not found, you might handle KeyError
        try:
            object_type = object_dictionary[object_type]
        except KeyError:
            logger.error(f"ObjectType '{object_type}' not recognized in dictionary. Skipping row {number}.")
            continue

        sanitized_obj = (object_type.lower().replace(" ", "")) if object_type else "unknown"

        try:
            table_name = table_pattern.format(
                business_unit=sanitized_bu,
                object=sanitized_obj
            )
        except KeyError as e:
            logger.error(
                f"The table pattern '{table_pattern}' expects placeholders not found in code: {str(e)}. Skipping."
            )
            continue

        # Prepare placeholders for the final report row
        row_status = None
        error_description = None
        athena_row_count = None
        difference = None

        # 2a. Query Athena row count
        try:
            athena_row_count = get_athena_table_row_count_after_date(
                database=athena_database,
                table=table_name,
                date_column=date_column,
                after_date=event_date_str,
                output_bucket=athena_output_bucket,
                region=region
            )
        except Exception as e:
            # Capture the error message for the final CSV
            logger.exception(f"Athena query failed for {athena_database}.{table_name}, row {number} (MID={mid}).")
            row_status = "Error"
            error_description = str(e)

        if row_status != "Error":
            # Calculate difference and percent diff only if no error
            difference = sfmc_row_count - (athena_row_count or 0)
            if sfmc_row_count == 0:
                percent_diff = None
            else:
                percent_diff = (athena_row_count - sfmc_row_count) / sfmc_row_count * 100

            # Check mismatch threshold
            if percent_diff is not None and abs(percent_diff) > threshold:
                mismatches.append({
                    "number": number,
                    "mid": mid,
                    "business_unit": business_unit,
                    "object_type": object_type,
                    "database": athena_database,
                    "table": table_name,
                    "event_date": event_date_str,
                    "sfmc_row_count": sfmc_row_count,
                    "athena_row_count": athena_row_count,
                    "percent_diff": percent_diff
                })

            # Determine row status
            if difference == 0:
                row_status = "Match"
            else:
                row_status = "NoMatch"
            
            error_description = "None"

        # 2b. Build final report row
        report_rows.append({
            "EventDate": event_date_str,
            "BusinessUnit": business_unit,
            "ObjectType": object_type,
            "AthenaTable": f"{athena_database}.{table_name}",
            "SFMCRowCount": sfmc_row_count,
            "AthenaRowCount": athena_row_count if athena_row_count is not None else 0,
            "Difference": difference if difference is not None else 0,
            "Status": row_status,
            "ErrorDescription": error_description
        })

    # 3. SNS notification if mismatches above threshold
    if mismatches:
        logger.warning("MISMATCHES FOUND (threshold-based). Preparing SNS notification...")
        message_lines = ["MISMATCHES FOUND (above threshold):"]
        for m in mismatches:
            line = (
                f"RowNumber={m['number']}, "
                f"MID={m['mid']}, "
                f"BusinessUnit={m['business_unit']}, "
                f"ObjectType={m['object_type']}, "
                f"DB={m['database']}, "
                f"Table={m['table']}, "
                f"EventDate={m['event_date']}, "
                f"AthenaRowCount={m['athena_row_count']}, "
                f"SFMCRowCount={m['sfmc_row_count']}, "
                f"PercentDiff={m['percent_diff']:.2f}%"
            )
            message_lines.append(line)

        message_body = "\n".join(message_lines)
        try:
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject="SFMC Reconciliation Mismatch",
                Message=message_body
            )
            logger.info("SNS notification sent successfully.")
        except Exception:
            logger.exception("Failed to publish SNS notification.")
    else:
        logger.info("No mismatches found above threshold. No SNS notification sent.")

    # 4. Upload final CSV report to S3
    logger.info("Uploading final reconciliation report to S3...")
    upload_csv_report_to_s3(s3_client, report_rows, report_s3_uri)

    # 5. Move CSV file from landing to processed folder
    logger.info("Moving CSV to 'processed' folder...")
    try:
        move_s3_object(
            s3_client,
            source_bucket=source_bucket,
            source_key=source_key,
            destination_bucket=destination_bucket,
            destination_key=destination_key
        )
        logger.info(f"Moved file from s3://{source_bucket}/{source_key} "
                    f"to s3://{destination_bucket}/{destination_key}")
    except Exception:
        logger.exception("Could not move file to processed directory.")


if __name__ == "__main__":
    main()
