import logging
from redshift.common import get_ssm_secret,format_response
import boto3

def pause(cluster_identifier):

    #Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        
        # Create the Redshift client
        redshift = boto3.client('redshift',region_name='us-east-1')

        response = redshift.pause_cluster(ClusterIdentifier=cluster_identifier)

        # Print the API response
        return format_response(200,response)
    except Exception as exc:
        logger.error('Unhandled exception', exc_info=True )
        return format_response(400,str(exc))
    
def resize(cluster_identifier,new_node_type ,new_node_count ):
    # Define the Redshift cluster identifier and the new node type and count


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

def resume(cluster_identifier):
    # Define the Redshift cluster identifier and the new node type and count
    


    #Logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    try:
        
        # Create the Redshift client
        redshift = boto3.client('redshift',region_name='us-east-1')

        response = redshift.resume_cluster(ClusterIdentifier=cluster_identifier)

        # Print the API response
        return format_response(200,str(response))
    except Exception as exc:
        logger.error('Unhandled exception', exc_info=True )
        return format_response(400,str(exc))
