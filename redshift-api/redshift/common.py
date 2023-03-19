import boto3
import json


def get_ssm_secret(parameter_name):
    ssm = boto3.client("ssm", region_name='us-east-1')
    secret =  ssm.get_parameter( Name=parameter_name, WithDecryption=True  )
    value = secret.get('Parameter').get('Value')
    del ssm
    return value

def format_response(status_code, message ):
    
    header = {"Content-Type": "application/json", "Accept": "application/json"}
    format_output = {"statusCode": int(status_code), "headers": header, "body": json.dumps(str(message))}
    
    return format_output