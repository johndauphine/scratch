import json
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



if __name__ == "__main__":
  response = pause_cluster(cluster_identifier = 'redshift-cluster-1')
  print(response)
    
