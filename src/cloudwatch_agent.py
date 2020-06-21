import boto3

class CloudWatchAgent:
    def __init__(self):
        self.client = boto3.client('cloudwatch', 'us-west-2')

    def put_latency_metrics(self, latecny, worker_ip, spider_name):
        metrics = {
            'MetricName': 'ActionLatecny',
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

        metrics['Value'] = latecny
        metrics['Dimensions'][0]['Value'] = worker_ip
        metrics['Dimensions'][1]['Value'] = spider_name

        self.client.put_metric_data(
            MetricData=[metrics],
            Namespace='Latency'
        )