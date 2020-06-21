from collections import defaultdict
import boto3
import requests

get_ec2_instance_id():
    # Get ipv4.
    return requests.get('http://169.254.169.254/latest/meta-data/public-ipv4').content
    