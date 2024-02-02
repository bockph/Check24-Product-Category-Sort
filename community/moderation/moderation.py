import os
from tqdm import tqdm
import pandas as pd
from azure.ai.contentsafety import ContentSafetyClient
from azure.ai.contentsafety.models import TextCategory
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError
from azure.ai.contentsafety.models import AnalyzeTextOptions
endpoint = "https://moderationapi.cognitiveservices.azure.com/"
credential = AzureKeyCredential("f3b92aa56c3f449da5f0abab1f503f70")
client = ContentSafetyClient(endpoint, credential)
# blocklist_client = BlocklistClient(endpoint, credential)

def azure_moderate(text):
    request = AnalyzeTextOptions(text=text)
    # Analyze text
    result = dict()
    response = client.analyze_text(request)
    for item in response.categories_analysis:
        result[item.category] = item.severity


    return result

if __name__ == '__main__':
    posts = pd.read_csv("../content search/posts.csv")
    comments = pd.read_csv("../content search/comments.csv")

    results =[]

    for idx, row in tqdm(posts.iterrows()):
        try:
            result = azure_moderate(row['content'])
            result['id'] = row['post_id']
            result['content'] = row['content']
            result['type'] = 'post'
            results.append(result)
        except Exception as e:
            print(e)
            continue

    for idx, row in tqdm(comments.iterrows()):
        try:
            result = azure_moderate(row['content'])
            result['id'] = row['comment_id']
            result['content'] = row['content']
            result['type'] = 'comment'
            results.append(result)
        except Exception as e:
            print(e)
            continue

    bad_text="Ich bringe alle um!"
    result = azure_moderate(bad_text)
    result['id'] = 0
    result['content'] = bad_text
    result['type'] = 'test'
    results.append(result)

    end_result = pd.DataFrame(results)
    end_result.to_csv("azure_moderate.csv", index=False)
