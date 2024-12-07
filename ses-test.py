import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def send_email(sender, recipient, subject, body_text, body_html):
    # AWS Region
    aws_region = "us-east-1"  # Replace with your region

    # Create a new SES client
    ses_client = boto3.client('ses', region_name=aws_region)

    # Email content
    email_content = {
        "Source": sender,
        "Destination": {
            "ToAddresses": [recipient]
        },
        "Message": {
            "Subject": {
                "Data": subject,
                "Charset": "UTF-8"
            },
            "Body": {
                "Text": {
                    "Data": body_text,
                    "Charset": "UTF-8"
                },
                "Html": {
                    "Data": body_html,
                    "Charset": "UTF-8"
                }
            }
        }
    }

    try:
        # Send the email
        response = ses_client.send_email(**email_content)
        print("Email sent successfully!")
        print(f"Message ID: {response['MessageId']}")
    except NoCredentialsError:
        print("Error: No AWS credentials found.")
    except PartialCredentialsError:
        print("Error: Incomplete AWS credentials configuration.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Replace with your email details
sender_email = "johndauphine@hotmail.com"
recipient_email = "jdauphine@gmail.com"
email_subject = "Test Email from Amazon SES"
email_body_text = "This is the plain text body of the email."
email_body_html = """
<html>
<head></head>
<body>
  <h1>Test Email</h1>
  <p>This is the HTML body of the email.</p>
</body>
</html>
"""

send_email(sender_email, recipient_email, email_subject, email_body_text, email_body_html)
