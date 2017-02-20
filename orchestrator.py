from time import sleep
from aggregator.main import Aggregator, LoadToRDS
import boto3
import json


def retrieve_raw_data():
    return True


class Orchestrator(object):
    def __init__(self, config):
        self.config = config
        self.load_rds = LoadToRDS(config=self.config)
        self.rds = AwsRds(self.config)
        self.s3 = AwsS3(self.config)
        self.red_shift = AwsRedShift(self.config)

    def run(self):
        self.rds.ensure()
        try:
            self.red_shift.setup()
            aggregator = Aggregator(self.config)
            aggregator.load_ref_data()
            aggregator.aggregate()
            aggregator.add_extention()
        except Exception as e:
            print(e)
        finally:
            self.red_shift.teardown()
        self.load_rds.load_ref_data_rds()


class AwsInfrastructure(object):
    def __init__(self, config):
        self.config = config

    def ensure(self):
        raise NotImplementedError("Please Implement this method")

    def setup(self):
        raise NotImplementedError("Please Implement this method")

    def teardown(self):
        raise NotImplementedError("Please Implement this method")


class AwsRds(AwsInfrastructure):
    def __init__(self, config):
        super(AwsRds, self).__init__(config)
        self.rds_client = boto3.client('rds')

    def setup(self):
        pass

    def teardown(self):
        pass

    def ensure(self):
        result = self.rds_client.describe_db_instances(
            DBInstanceIdentifier=self.config['rds_instance_identifier']
        )
        instances = result['DBInstances']
        return len(instances) == 1


class AwsS3(AwsInfrastructure):
    def setup(self):
        pass

    def teardown(self):
        pass

    def ensure(self):
        pass


class AwsRedShift(AwsInfrastructure):
    def __init__(self, config):
        super(AwsRedShift, self).__init__(config)
        self.rs_client = boto3.client('redshift')

    def setup(self):
        redshift_config = self.config['redshift']
        try:
            self.rs_client.create_cluster(
                ClusterIdentifier=redshift_config.get('ClusterIdentifier'),
                DBName=redshift_config.get('DBName'),
                MasterUsername=redshift_config.get('MasterUsername'),
                MasterUserPassword=redshift_config.get('MasterUserPassword'),
                NodeType=redshift_config.get('NodeType'),
                ClusterType=redshift_config.get('ClusterType', 'single-node'),
                PreferredMaintenanceWindow=redshift_config.get('PreferredMaintenanceWindow', 'sun:01:30-sun:02:00'),
                AllowVersionUpgrade=redshift_config.get('AllowVersionUpgrade', True),
                PubliclyAccessible=redshift_config.get('PubliclyAccessible', True),
                IamRoles=redshift_config.get('IamRoles')
            )
            seconds = 0
            while True:
                response = self.rs_client.describe_clusters(ClusterIdentifier=redshift_config.get('ClusterIdentifier'))
                if response['Clusters'][0]['ClusterStatus'] == "available":
                    break
                print("%s: %d seconds elapsed" % (response['Clusters'][0]['ClusterStatus'], seconds))
                sleep(10)
                seconds += 10
            print('Redshift cluster created', 'status: %s' % response['Clusters'][0]['ClusterStatus'])
            return True

        except Exception as e:
            print (e.message)
            return False

    def teardown(self):
        redshift_config = self.config['redshift']
        try:
            self.rs_client.delete_cluster(
                ClusterIdentifier='cg-analytics-boto-test',
                SkipFinalClusterSnapshot=True
            )
            response = self.rs_client.describe_clusters(ClusterIdentifier=redshift_config.get('ClusterIdentifier'))
            seconds = 0
            while True:
                try:
                    response = self.rs_client.describe_clusters(ClusterIdentifier=redshift_config.get('ClusterIdentifier'))
                    print(
                        "%s Redshift Cluster: %d seconds elapsed" % (response['Clusters'][0]['ClusterStatus'], seconds))
                    sleep(10)
                    seconds += 10
                except Exception as e:
                    print('Redshift cluster is deleted')
                    break
            return True

        except Exception as e:
            print (e.message)
            return False

    def ensure(self):
        pass


if __name__ == "__main__":
    with open('config.json') as config_file:
        conf = json.load(config_file)
        orchestrator = Orchestrator(conf)
        orchestrator.run()
