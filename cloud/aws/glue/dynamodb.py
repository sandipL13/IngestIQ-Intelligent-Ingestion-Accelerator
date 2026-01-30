import boto3

def get_table(table_name, region="us-east-1"):
    return boto3.resource("dynamodb", region_name=region).Table(table_name)
