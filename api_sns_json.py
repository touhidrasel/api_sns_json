#The Lambda function gets triggered by the S3 bucket object creation. 
#The Lambda function needs to read file name, content and sends the data to factoryworkx using WebEDI_UploadMessage_V3 API.
#The lambda function gets the apiKey from the file tags.
#Author@Touhid
#Version@1.0

import os
import json
import logging
import http.client
import urllib.parse
import boto3
import csv

logger = logging.getLogger()
logger.setLevel(logging.INFO)
api_key=''
email_body=''
file_name=''

print('factoryworkx-ams-importer function starts')

# Initiate boto3 client
s3 = boto3.client('s3')
sns_client= boto3.client('sns')

def get_timestamp():
    current = datetime.now()
    return(str(current.year) + '-' + str(current.month) + '-' + str(current.day) + '-' + str(current.hour) + '-' + str(current.minute) + '-' + str(current.second))

#gets envaironment variables        
def GetEnviron(name, default_value = ''):
    if name in os.environ:
        return os.environ[name]
    return default_value

#reads the envaironment variables, if not present use default one
target_server = GetEnviron('ams_fwximporter_hostname', 'xxx.com')
target_module = GetEnviron('ams_fwximporter_module', 'XXX_API')
target_function_name = GetEnviron('ams_fwximporter_function_name', 'API_V3')
target_topic_arn = GetEnviron('ams_fwximporter_topic_arn', 'arn:aws:sns:us-east-1:633240887674:_sendErrorMail')

print("Env Variables : target_server " + target_server)
print("Env Variables : target_module " + target_module)
print("Env Variables : target_function_name " + target_function_name)
print("Env Variables : target_topic_arn " + target_topic_arn)


def lambda_handler(event, context):
    #print("Recevied event: " + json.dumps(event, indent=2))
    
    #Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    folder= os.path.dirname(key)
    print('bucket : '+bucket)
    print('key : '+key)
    global file_name
    file_name= os.path.basename(key)
    print('file_name :'+file_name)
    
    response = s3.get_object_tagging(
        Bucket=bucket,
        Key=key,
    )
    print(response)
    key_value = json.dumps(response['TagSet'][0])
    api_keyJSON = json.loads(key_value)
    global api_key
    api_key= api_keyJSON['Value']
    print('api_key : ' + api_key)
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    body = s3_object['Body']
    raw_message = body.read()
    print('raw_message : '+ raw_message.decode("utf-8"))
    if send_data_to_factoryworkx(file_name, raw_message):
        print('Data transfer successful')
    else:
        print('Data send not successful..')
        send_sns()
        print('Sending error sns..')
    
#Calls API to send data to factoryworkx 
def send_data_to_factoryworkx(file_name, raw_message):
    try:
        post_data = {'APIKey': api_key, 'FuncName': target_function_name, 'Filename': file_name, 'RawMessage': raw_message}
        print('sending data using api..')
        post_data = urllib.parse.urlencode(post_data)
        print('Post data using api..')
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        conn = http.client.HTTPSConnection(target_server, port=443)
        conn.request('POST', '/?mod=' + target_module, post_data, headers)
        response = conn.getresponse().read().decode()
        print('response :'+ response)
        global email_body
        email_body=str("")
        email_body = str(response)
        print('sns email_body: '+email_body)
        API_result = json.loads(response)
        print('API_result :'+ str(API_result))
        conn.close()
        if API_result['ErrorCode'] == 0:
            return True
        else:
            logger.error('ERROR: API returned non-zero ErrorCode: ' + str(API_result['ErrorCode']) + ' = ' + str(API_result['ErrorMessage']))
            return False
    except Exception as e:
        logger.error(e)
        return False
        
#send sns using sns topic arn  
def send_sns():
    sns_client.publish(
        TopicArn= target_topic_arn,
        Subject='ams-error-notification',
        Message= 'API responce. '+email_body+' '+'File : '+file_name
    )