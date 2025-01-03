import csv
import io
import os
import time
import sys
import argparse
import logging
import boto3
from urllib.parse import urlparse

# Set up default logging configuration.
# 'logger.exception()' will include the stack trace automatically.
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

    Parameters
    ----------
    database : str
        The name of the AWS Athena database.
    table : str
        The name of the table.
    date_column : str
        The date column in the Athena table to filter on.
    after_date : str
        The date string from the CSV file, e.g. '2024-01-01'.
    output_bucket : str
        S3 bucket URI (without the 's3://' prefix) where Athena query results
        will be stored, e.g., "my-athena-results-bucket/my-folder".
    region : str, optional
        AWS region for the Athena client, default is "us-east-1".

    Returns
    -------
    int
        The number of rows in the specified table where date_column > after_date.
    """
    logger = logging.getLogger(__name__)
    athena_client = boto3.client("athena", region_name=region)

    # Adjust query based on your schema (DATE vs. TIMESTAMP vs. STRING).
    query_string = (
        f"SELECT COUNT(*) AS total "
        f"FROM {database}.{table} "
        f"WHERE {date_column} > DATE '{after_date}'"
    )

    logger.info(f"Starting Athena query for rows after {after_date} in {database}.{table}")
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": f"s3://{output_bucket}/"}
    )

    query_execution_id = response["QueryExecutionId"]

    # Poll the query status until completion (or failure).
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

        time.sleep(2)

    if status == "SUCCEEDED":
        logger.info(f"Athena query succeeded for {database}.{table}. Fetching results...")
        result_response = athena_client.get_query_results(QueryExecutionId=query_execution_id)

        # First row is header; second row has the actual count
        row_count_str = result_response["ResultSet"]["Rows"][1]["Data"][0]["VarCharValue"]
        return int(row_count_str)
    else:
        # Log the error with a stack trace
        logger.exception(f"Athena query failed or was cancelled. Status: {status}")
        raise Exception(f"Athena query failed or was cancelled. Status: {status}")


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
        # Delete the original object
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)
        logger.info("Copy and delete completed.")
    except Exception:
        # Log the error with a stack trace
        logger.exception("Error copying/deleting S3 object.")
        raise


def construct_processed_key(source_key):
    """
    Construct the processed S3 key by placing the file under a 'processed' directory
    while keeping the original file name.

    Example:
      If source_key = 'sfmc/daily-row-counts/file.csv',
      we return 'sfmc/processed/file.csv'.
    """
    parts = source_key.split('/')
    if len(parts) <= 1:
        # If there's no folder structure, just prepend 'processed/'
        filename = source_key
        return f"processed/{filename}"
    else:
        # Keep the top-level folder, then 'processed', then filename
        # Example: sfmc/daily-row-counts/file.csv -> sfmc/processed/file.csv
        top_level = parts[0]
        filename = parts[-1]
        return f"{top_level}/processed/{filename}"


def main():
    """
    Main script for SFMC Reconciliation. Steps:
    1. Read CSV file from S3 (landing) containing SFMC daily row counts.
    2. For each row (BU, Object, Date, Row Count), call get_athena_table_row_count_after_date
       to count only rows with date_column > the CSV Date.
    3. Compare actual vs. expected. Compute percent difference.
    4. If difference above threshold, add to mismatch list.
    5. Send SNS notification if mismatch list is not empty.
    6. Move CSV to 'processed' folder in the same bucket.
    """
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description="SFMC Reconciliation Process (Date-Filtered, with stack traces)")

    # Required arguments
    parser.add_argument("--landing-s3-uri", required=True,
                        help="S3 URI to the CSV file to process, e.g. s3://my-bucket/sfmc/daily-row-counts/file.csv")
    parser.add_argument("--athena-output-bucket", required=True,
                        help="S3 bucket (no 's3://') for Athena query results, e.g. my-athena-results-bucket/folder")
    parser.add_argument("--sns-topic-arn", required=True,
                        help="SNS topic ARN to send mismatch notifications.")
    parser.add_argument("--athena-date-column", required=True,
                        help="Name of the date column in the Athena table to filter on, e.g. 'created_date'.")

    # Optional arguments
    parser.add_argument("--region", default="us-east-1",
                        help="AWS region for Athena / SNS (default: us-east-1)")
    parser.add_argument("--threshold", type=float, default=10.0,
                        help="Percent difference threshold for mismatch (default: 10.0)")
    parser.add_argument("--athena-database-prefix", default="",
                        help="Optional prefix for Athena database name if needed, e.g., 'sfmc_'")
    parser.add_argument("--athena-table-pattern", default="bronze_{object}",
                        help=(
                            "String pattern for the Athena table name. Use Python format syntax. "
                            "E.g., 'bronze_{object}_delta'. "
                            "Placeholder '{object}' will be replaced by the CSV 'Object' value. "
                            "You could also use '{business_unit}'."
                        ))

    args = parser.parse_args()

    # Extract parameters
    landing_s3_uri = args.landing_s3_uri
    athena_output_bucket = args.athena_output_bucket
    sns_topic_arn = args.sns_topic_arn
    date_column = args.athena_date_column
    region = args.region
    threshold = args.threshold
    db_prefix = args.athena_database_prefix
    table_pattern = args.athena_table_pattern

    s3_client = boto3.client("s3", region_name=region)
    sns_client = boto3.client("sns", region_name=region)

    # Parse the landing S3 URI
    parsed_uri = urlparse(landing_s3_uri, allow_fragments=False)
    source_bucket = parsed_uri.netloc
    source_key = parsed_uri.path.lstrip("/")

    # Construct the processed key in the same bucket
    destination_bucket = source_bucket
    destination_key = construct_processed_key(source_key)
    
    # 1. Download CSV file from S3 to memory
    logger.info(f"Downloading CSV file from s3://{source_bucket}/{source_key}")
    try:
        csv_obj = s3_client.get_object(Bucket=source_bucket, Key=source_key)
    except Exception:
        logger.exception(f"Could not fetch CSV from {landing_s3_uri}.")
        sys.exit(1)

    csv_content = csv_obj['Body'].read().decode('utf-8')
    logger.info("CSV file downloaded successfully.")

    # 2. Parse CSV
    mismatches = []
    reader = csv.DictReader(io.StringIO(csv_content))
    # CSV columns expected: Business Unit, Object, Date, Row Count
    # e.g., {"Business Unit": "BU1", "Object": "MyObject", "Date": "2024-01-01", "Row Count": "1000"}

    for row in reader:
        business_unit = row.get("Business Unit")
        obj = row.get("Object")
        date_str = row.get("Date")  # We'll use this for WHERE date_column > date_str
        expected_str = row.get("Row Count", "0")

        # Convert expected row count to int
        try:
            expected_row_count = int(expected_str)
        except ValueError:
            logger.error(f"Invalid expected row count '{expected_str}' for BU: {business_unit}, Object: {obj}")
            continue

        if not date_str:
            logger.error(f"No Date found in CSV row for BU: {business_unit}, Object: {obj}")
            continue

        # Build the database name
        if business_unit:
            sanitized_bu = business_unit.lower().replace(" ", "_")
        else:
            sanitized_bu = "unknown"
        database_name = f"{db_prefix}{sanitized_bu}"

        # Build the table name from the pattern
        if obj:
            sanitized_obj = obj.lower().replace(" ", "_")
        else:
            sanitized_obj = "unknown"

        try:
            table_name = table_pattern.format(object=sanitized_obj)
        except KeyError as e:
            logger.error(
                f"The table pattern '{table_pattern}' expects a placeholder not found in code: {str(e)}"
            )
            continue

        # 2a. Get the actual row count from Athena, filtered by date_column > date_str
        try:
            actual_row_count = get_athena_table_row_count_after_date(
                database=database_name,
                table=table_name,
                date_column=date_column,
                after_date=date_str,
                output_bucket=athena_output_bucket,
                region=region
            )
        except Exception:
            logger.exception(f"Athena query failed for {database_name}.{table_name}")
            continue

        # 2b. Calculate the percent difference
        if expected_row_count == 0:
            percent_diff = None
        else:
            percent_diff = (actual_row_count - expected_row_count) / expected_row_count * 100

        # 2c. Check threshold
        if percent_diff is not None and abs(percent_diff) > threshold:
            mismatches.append({
                "business_unit": business_unit,
                "object": obj,
                "database": database_name,
                "table": table_name,
                "date_filter": date_str,
                "expected": expected_row_count,
                "actual": actual_row_count,
                "percent_diff": percent_diff
            })

    # 3. If mismatches, send SNS notification
    if mismatches:
        logger.warning("MISMATCHES FOUND. Preparing SNS notification...")
        message_lines = ["MISMATCHES FOUND:"]
        for m in mismatches:
            line = (
                f"BusinessUnit={m['business_unit']}, "
                f"Object={m['object']}, "
                f"DB={m['database']}, "
                f"Table={m['table']}, "
                f"DateFilter=>{m['date_filter']}, "
                f"Expected={m['expected']}, "
                f"Actual={m['actual']}, "
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
        logger.info("No mismatches found. No SNS notification sent.")

    # 4. Move CSV file from landing to processed folder
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
