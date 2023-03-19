import json
import logging
from redshift.common import get_ssm_secret,format_response

import boto3




def resume(cluster_identifier = 'redshift-cluster-1'):
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



if __name__ == "__main__":
    return_value = resume( cluster_identifier='redshift-cluster-1')
    print(return_value)
