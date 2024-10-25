from datetime import datetime
import boto3
import logging
import json

def get_ssm_secret(parameter_name):
    ssm = boto3.client("ssm", region_name='us-east-1')
    secret =  ssm.get_parameter( Name=parameter_name, WithDecryption=True  )
    value = secret.get('Parameter').get('Value')
    del ssm
    return value

def format_response(status_code, message ):
    
    header = {"Content-Type": "application/json", "Accept": "application/json"}
    format_output = {"statusCode": int(status_code), "headers": header, "body": json.dumps(message)}
    
    return format_output

def lambda_handler(event, context):
    try:
        #Logging
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        #currentTime = datetime.now()
        #currentTimeStr = currentTime.strftime("%Y-%m-%d-%H-%M-%S")
        print(event)
        # Define the Redshift cluster identifier and the new node type and count
        body = json.loads(event['Records'][0]['Sns']['Message'])
        
        clusterName = body['clusterName']
        newNodeType = body['nodeType']
        newNodeCount = body['nodeCount']
        regionName = body['regionName']
        
        # clusterName = get_ssm_secret('/MF/PROD/DW/REDSHIFT/NAME')
        logger.info(f"Cluster name: {clusterName}") 
        logger.info(f"New Node Type: {newNodeType}") 
        logger.info(f"New Node Count: {newNodeCount}") 
        logger.info(f"Region Name: {regionName}") 

        
        
        # Create the Redshift client
        redshift = boto3.client('redshift',region_name=regionName)
        
        # Call the modify_cluster API with the new node type and count
        response = redshift.modify_cluster(
            ClusterIdentifier=clusterName,
            NodeType=newNodeType,
            NumberOfNodes= newNodeCount
        )
        logger.info('Redshift resize started....')
    except Exception as exc:
        logger.error('Unhandled exception', exc_info=True )
        return format_response(400,str(exc))
    
    # Log the API response
    logger.info(response)
    
    return format_response(200,str(response))
