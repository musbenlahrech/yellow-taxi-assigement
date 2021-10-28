import json
import boto3
 
# Define the client to interact with AWS Lambda
lam = boto3.client('lambda')
baseUrl = "https://nyc-tlc.s3.amazonaws.com/trip+data/"
def url_generator(baseUrl):
    # Function that generates 12 urls for each month
    return (baseUrl + "yellow_tripdata_2020-{:02d}.csv".format(month) for month in range(1,13))
    

def lambda_handler(event,context):
 
    # Define the input parameters that will be passed
    # on to the child function

    try:
        for url in url_generator(baseUrl):
            inputParams = {
                "Url":url,
                "FileName": 'raw/'+ url.split('/')[-1]
            }
            print(inputParams)
            lam.invoke(
                FunctionName = 'arn:aws:lambda:us-east-1:387414121929:function:extractNYCYellowTaxiData',
                InvocationType = 'Event',
                Payload = json.dumps(inputParams)
            )
        
    except Exception as e:
        print(e)
        print('Error getting object'.format())
        raise e

