import boto3
import requests
from requests_aws4auth import AWS4Auth

host = 'domain-endpoint/'
region = 'eu-central-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Register repository
path = '_plugins/_ml/connectors/_create'
url = host + path
payload={
        "name": "OpenAI Embedding Connector",
        "description": "The connector to public OpenAI model service for Ada Embedding",
        "version": 1,
        "protocol": "http",
        "parameters": {
            "endpoint": "api.openai.com",
            "model": "text-embedding-ada-002"
        },
        "credential": {
            "openAI_key": "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"
        },
        "actions": [
            {
                "action_type": "predict",
                "method": "POST",
                "url": "https://${parameters.endpoint}/v1/embeddings",
                "headers": {
                    "Authorization": "Bearer ${credential.openAI_key}"
                },
                "request_body": "{ \"model\": \"${parameters.model}\", \"input\": ${parameters.messages}, \"encoding_format\":\"float\"}"
            }
        ]
    }
headers = {"Content-Type": "application/json"}

r = requests.post(url, auth=awsauth, json=payload, headers=headers)
print(r.status_code)
print(r.text)


curl -XPOST "http://localhost:9200/_plugins/_ml/connectors/_create" -H 'Content-Type: application/json' -d'
{
        "name": "OpenAI Embedding Connector",
        "description": "The connector to public OpenAI model service for Ada Embedding",
        "version": 1,
        "protocol": "http",
        "parameters": {
            "endpoint": "api.openai.com",
            "model": "text-embedding-ada-002"
        },
        "credential": {
            "openAI_key": "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"
        },
        "actions": [
            {
                "action_type": "predict",
                "method": "POST",
                "url": "https://${parameters.endpoint}/v1/embeddings",
                "headers": {
                    "Authorization": "Bearer ${credential.openAI_key}"
                },
                "request_body": "{ \"model\": \"${parameters.model}\", \"input\": ${parameters.messages}, \"encoding_format\":\"float\"}"
            }
        ]
    }'

curl -XGET "http://localhost:9200/_cluster/settings?include_defaults=true"