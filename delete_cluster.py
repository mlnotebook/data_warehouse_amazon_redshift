import configparser
import psycopg2
import boto3
import time

def delete_redshift(redshift, cluster_identifier):
    """Deletes the specified redshift cluster.
    
    Keyword arguments:
    redshift -- a boto3 reshift instance.
    cluster_identifier - the cluster to terminate.
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier,
                                SkipFinalClusterSnapshot=True)
    except Exception as e:
        print('\n{}'.format(e))
        print('Error deleting cluster {}. It may already be deleted.'.format(cluster_identifier))
        
    
def cluster_check(redshift, cluster_identifier):
    """Checks if the cluster is contactable.
    
    Keyword arguments:
    redshift -- a boto3 reshift instance.
    cluster_identifier - the cluster to terminate.
    """
    try:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        return True
    except Exception as e:
        return False
    
    
def delete_iam_role(iam, iam_role_name):
    """Deletes the specified IAM Role.
    
    Keyword arguments:
    iam -- a boto3 iam instance.
    iam_role_name - the IAM Role Name to terminate.
    """
    try:
        iam.detach_role_policy(RoleName=iam_role_name,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=iam_role_name)
    except Exception as e:
        print('\n{}'.format(e))
        print('Error deleting IAM Role {}. It may already be deleted.'.format(iam_role_name))        


def main():
    """Deletes the Redshift cluster and deletes IAM Role."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Get credentials
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    print('Deleting Redshift Cluster.', flush=True)
    cluster_identifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    delete_redshift(redshift, config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))
    
    wait_time = 0
    while cluster_check(redshift, cluster_identifier):
        print('Waiting for cluster to be removed... {} s'.format(wait_time), end='\r', flush=True)
        wait_time += 5
        time.sleep(5)
    
    print('\nDeleting IAM Role...', end='', flush=True)
    iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )
    
    delete_iam_role(iam, config.get('IAM_ROLE','IAM_ROLE_NAME'))
    print('done!')
    
if __name__ == "__main__":
    main()

    