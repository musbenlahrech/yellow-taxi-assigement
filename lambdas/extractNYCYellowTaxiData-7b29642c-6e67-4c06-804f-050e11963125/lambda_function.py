import json
import urllib.parse
import boto3
import botocore.vendored.requests.packages.urllib3 as urllib3

print('Loading function')

s3 = boto3.client('s3')
bucket_name = 'taxi-cab-spark-temp'


def taxi_csv_downloader(url, bucket, key): 
    # download csv file directly to s3 
    print("Downloading url : {} to buckt {} with key: {}".format(url, bucket, key))
    
    try:
        
        s3=boto3.client('s3')
        http=urllib3.PoolManager()
        content = http.request('GET', url,preload_content=False)
        s3.upload_fileobj(content, bucket, key)
        
    except Exception as e:
        print(e)
        print('Error getting object {} to bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(url, bucket))
        raise e

def lambda_handler(event, context):
    #1 Read the input parameters
    url = event['Url']
    filename = event['FileName']

    try:
        
       taxi_csv_downloader(url ,bucket_name,filename)
           
    except Exception as e:
        print(e)
        print('Error getting object'.format())
        raise e
