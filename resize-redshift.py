import boto3
import logging

# Define the Redshift cluster identifier and the new node type and count
cluster_identifier = 'redshift-cluster-1'
new_node_type = 'dc2.large'
new_node_count = 2

#Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create the Redshift client
redshift = boto3.client('redshift',region_name='us-east-1')

# Call the modify_cluster API with the new node type and count
response = redshift.modify_cluster(
    ClusterIdentifier=cluster_identifier,
    NodeType=new_node_type,
    NumberOfNodes=new_node_count
)

# Print the API response
print(response)

