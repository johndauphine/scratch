import json
import logging

import boto3

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


def main():
    # Define the Redshift cluster identifier and the new node type and count
    cluster_identifier = 'redshift-cluster-1'
    new_node_type = 'dc2.large'
    new_node_count = 4

    #Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        
        # Create the Redshift client
        redshift = boto3.client('redshift',region_name='us-east-1')

        # Call the modify_cluster API with the new node type and count
        response = redshift.modify_cluster(
            ClusterIdentifier=cluster_identifier,
            NodeType=new_node_type,
            NumberOfNodes=new_node_count
        )

        # Print the API response
        return format_response(200,response)
    except Exception as exc:
        logger.error('Unhandled exception', exc_info=True )
        return format_response(400,str(exc))



if __name__ == "__main__":
    main()
    
