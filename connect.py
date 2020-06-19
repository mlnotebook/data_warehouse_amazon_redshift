import configparser
import psycopg2
import boto3

def connect(config_path):
    """Checks whether the Redshift cluster is active, retrieves endpoint
    and IAM Role ARM then connects to the cluster with PostgreSQL.
    Returns the cursor and connection object.
    
    Keyword arguments:
    config_path -- path to the .cfg file containing connection variables.
    """
    config = configparser.ConfigParser()
    config.read(config_path)

    # Get credentials
    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                           )
        
    try:
        clusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))['Clusters'][0]
        if str(clusterProps['ClusterStatus']) == 'available':
            ENDPOINT = clusterProps['Endpoint']['Address']
            ROLE_ARN = clusterProps['IamRoles'][0]['IamRoleArn']
        else:
            raise
    except Exception as e:
        print('Error getting Endpoint and ARN. Check the cluster.')
        raise(e)
       
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(ENDPOINT,
                                                                                   DB_NAME,
                                                                                   DB_USER,
                                                                                   DB_PASSWORD,
                                                                                   DB_PORT))
    print('Conn: ', conn)
    cur = conn.cursor()

    return cur, conn
