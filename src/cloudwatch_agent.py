import boto3
import random

class CloudWatchAgent:
    def __init__(self):
        self.client = boto3.client('cloudwatch', 'us-west-2')

    def put_latency_metrics(self, latency, worker_ip, spider_name, metrics_sample_mods):
        metrics = {
            'MetricName': 'ActionLatency',
            'Dimensions': [
                {
                    'Name': 'WorkerNodeIp',
                },
                {
                    'Name': 'SpiderName',
                },
            ],
            'Unit': 'Seconds',
        }

        metrics['Value'] = latency
        metrics['Dimensions'][0]['Value'] = worker_ip
        metrics['Dimensions'][1]['Value'] = str(spider_name)

        # Reduce put metrics frequency to avoid throttling.
        if random.randint(1, metrics_sample_mods) == 1:
            self.client.put_metric_data(
                MetricData=[metrics],
                Namespace='Latency'
            )