import configparser
import boto3
import json
import time

def create_iam_role(iam, iam_role_name):
    """Creates the IAM Role, attaches S3ReadOnlyAccess policy,
    and fetches the ARN.
    
    Keyword arguments:
    iam -- a boto3 iam instance.
    iam_role_name - the name of the iam role to be created.
    """
    print('Creating IAM Role')
    try:
        iam_role = iam.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print('Error. Maybe the IAM Role already exists.')
        print(e)
    
    try:
        print('Attaching IAM Role Policy')
        iam.attach_role_policy(RoleName=iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print('Error. Maybe the IAM Role already exists and the policy is already attached.')
        print(e)
        
    print('Fetching ARN')
    roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    
    return roleArn


def create_cluster(redshift, cluster_config, arn):
    """Creates the redshift cluster.
    
    Keyword arguments:
    redshift - a boto3 redshift instance.
    cluster_config - a config object containing the connection variables.
    arn - the IAM Role ARN.
    """
    response = redshift.create_cluster(        
        # Hardware
        ClusterType=cluster_config.get('CLUSTER', 'CLUSTER_TYPE'),
        NodeType=cluster_config.get('CLUSTER', 'NODE_TYPE'),
        NumberOfNodes=int(cluster_config.get('CLUSTER', 'NUM_NODES')),

        # Identifiers & Credentials
        ClusterIdentifier=cluster_config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
        DBName=cluster_config.get('CLUSTER', 'DB_NAME'),
        MasterUsername=cluster_config.get('CLUSTER', 'DB_USER'),
        MasterUserPassword=cluster_config.get('CLUSTER', 'DB_PASSWORD'),
        Port=int(cluster_config.get('CLUSTER', 'DB_PORT')),
        
        # Roles (for s3 access)
        IamRoles=[arn]
    )
    return response


def open_port(ec2, vpc_id, port):
    """Opens the TCP port for the database to accept any IP.
    
    Keyword arguments:
    ec2 - a boto3 ec2 instance.
    vpc_id - vpc id returned by redshift.describe_clusters.
    port - the database port on the redshift cluster.
    """
    vpc = ec2.Vpc(id=vpc_id)
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(port),
        ToPort=int(port)
    )

    
def main():
    """Creates the IAM Role and creates Redshift cluster using
    the retrieved ARN. Retrieves endpoint and Role ARN. Also
    opens the appropriate port.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Get credentials
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    
    # Create the IAM Role
    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                  )
    
    iam_role_name = config.get('IAM_ROLE', 'IAM_ROLE_NAME')
    arn = create_iam_role(iam, iam_role_name)
        
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                       )
    
    # Create the cluster
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    try:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        if str(clusterProps['ClusterStatus']) == 'available':
            print('Cluster {} is already available!'.format(cluster_identifier))
    except:
        try:
            redshift_response = create_cluster(redshift, config, arn)
        except Exception as e:
            print(e)
            print('Error creating Redshift cluster.')
        
    # Interrogate the cluster until it becomes available
    clusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    
    retry_idx = 0
    while str(clusterProps['ClusterStatus']).lower() != 'available':
        print('Waiting for cluster {} to become available. Check {} - Status: {}'.format(config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
                                                                                        retry_idx,
                                                                                        str(clusterProps['ClusterStatus'])),
             end="\r", flush=True)
        clusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))['Clusters'][0]
        retry_idx += 1
        time.sleep(5)
        
    print('\nCluster Available!')
    
    # Gather the endpoint and arn
    ENDPOINT = clusterProps['Endpoint']['Address']
    ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']
    
    print('Endpoint: {}'.format(ENDPOINT))
    print('ARN: {}'.format(ROLE_ARN))
          
    # Open ports
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    try:
        open_port(ec2, clusterProps['VpcId'], config.get('CLUSTER', 'DB_PORT'))
        print('The database port has been opened.')
    except Exception as e:
        print('The database port is already open.')
        
    
if __name__ == "__main__":
    main()
