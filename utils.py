import boto3
import botocore.exceptions


def create_s3_client(access_key_id: str, secret_access_key: str) -> boto3.client:
    """Creates and returns an S3 client using an AWS Access Key."""
    try:
        return boto3.client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Error creating S3 client: {e}") from e


def create_ses_client(access_key_id: str, secret_access_key: str) -> boto3.client:
    """Creates and returns an SES client using an AWS Access Key."""
    try:
        return boto3.client(
            "ses",
            region_name="eu-west-2",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )
    except botocore.exceptions.ClientError as e:
        raise RuntimeError(f"Error creating SES client: {e}") from e


def download_xml(
    s3_client: boto3.client, source_bucket: str, source_key: str, output_filename: str
) -> None:
    """Downloads the XML file from the input  S3 bucket."""
    s3_client.download_file(source_bucket, source_key, output_filename)


def upload_csv_to_bucket(
    s3_client: boto3.client, target_bucket: str, target_key: str, csv_filename: str
) -> None:
    """Uploads the created CSV to the output S3 bucket."""
    s3_client.upload_file(csv_filename, target_bucket, target_key)


def send_html_email(ses_client: boto3.client, filename: str, task_status: str) -> None:
    """Sends an email describing the pipeline task's status using SES."""

    primary_color = "#FF9900"
    secondary_color = "#F8F9FA"
    body_font_family = "Arial, sans-serif"

    start_template = f"""
    <html>
    <head>
        <style>
            body {{ font-family: {body_font_family}; background-color: {secondary_color}; padding: 20px; }}
            h1 {{ color: {primary_color}; text-align: center; margin-bottom: 20px; }}
            p {{ line-height: 1.6; margin-bottom: 10px; }}
        </style>
    </head>
    <body>
        <h1>Pipeline Notification</h1>
        <p>{filename} found - Pipeline starting...</p>
    </body>
    </html>
    """

    end_template = f"""
    <html>
    <head>
        <style>
            body {{ font-family: {body_font_family}; background-color: {secondary_color}; padding: 20px; }}
            h1 {{ color: {primary_color}; text-align: center; margin-bottom: 20px; }}
            p {{ line-height: 1.6; margin-bottom: 10px; }}
        </style>
    </head>
    <body>
        <h1>Pipeline Notification</h1>
        <p>{filename} has been created and uploaded to the output bucket!</p>
    </body>
    </html>
    """

    if task_status == "start":
        HTML_EMAIL_CONTENT = start_template
    elif task_status == "end":
        HTML_EMAIL_CONTENT = end_template
    else:
        print("Invalid task_status value. It should be either 'start' or 'end'.")
        return

    response = ses_client.send_email(
        Destination={
            "ToAddresses": [
                "trainee.eyuale.lemma@sigmalabs.co.uk",
            ],
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": "UTF-8",
                    "Data": HTML_EMAIL_CONTENT,
                }
            },
            "Subject": {
                "Charset": "UTF-8",
                "Data": "Automated PharmaZer Pipeline Notification",
            },
        },
        Source="trainee.eyuale.lemma@sigmalabs.co.uk",
    )
