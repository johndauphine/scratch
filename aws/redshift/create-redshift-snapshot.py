from datetime import datetime
import boto3
import logging




def get_ssm_secret(parameter_name):
    ssm = boto3.client("ssm", region_name='us-east-1')
    secret =  ssm.get_parameter( Name=parameter_name, WithDecryption=True  )
    value = secret.get('Parameter').get('Value')
    del ssm
    return value


def lambda_handler(event, context):
    
    try:
        #Logging
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        currentTime = datetime.now()
        currentTimeStr = currentTime.strftime("%Y-%m-%d-%H-%M-%S")
        snapshotId = f'monthly-snapshot-{currentTimeStr}'
        clusterName = get_ssm_secret('/MF/PROD/DW/REDSHIFT/NAME')
        retentionDays = get_ssm_secret('/MF/PROD/DW/REDSHIFT/SNAPSHOT/RETENTIONDAYS')


        logger.info(f"Creating snapshot {snapshotId}...")
        logger.info(f"Cluster name: {clusterName}") 
        logger.info(f"Snapshot retention perion in days: {retentionDays}") 
         
        client = boto3.client('redshift')
        response = client.create_cluster_snapshot(
            SnapshotIdentifier=snapshotId,
            ClusterIdentifier=clusterName,
            ManualSnapshotRetentionPeriod=int(retentionDays) 
        )

    except Exception as exc:
        logger.error('Unhandled exception', exc_info=True )
        raise exc