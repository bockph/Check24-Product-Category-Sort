import openai
from openai import AzureOpenAI
import pandas as pd
from datetime import datetime
endpoint="https://checkgpt.openai.azure.com"
deployment="c24_embedding"
api_key="5161a6aa2c904b6abefc0d58fe03583b"
api_version="2023-05-15"
openai.api_key = api_key
openai.api_base = endpoint # your endpoint should look like the following https://YOUR_RESOURCE_NAME.openai.azure.com/
openai.api_type = 'azure'
openai.api_version = api_version

deployment_name=deployment

def index_without_embedding(index_name):
    posts = pd.read_csv("posts.csv")
    comments = pd.read_csv("comments.csv")
    client = AzureOpenAI(
        api_key=api_key,
        api_version=api_version,
        azure_endpoint=endpoint
    )
    id = 0
    posts[['content', 'external_source_type', 'reaction_count']] = posts[
        ['content', 'external_source_type', 'reaction_count']].fillna(value='')
    posts[['comment_count', 'view_count']] = posts[['comment_count', 'view_count']].fillna(value=0)
    posts[['post_id', 'group_id']] = posts[['post_id', 'group_id']].fillna(value=-1)
    posts['is_question'] = posts['is_question'].astype('bool')
    comments[['content', 'external_source_type']] = comments[['content', 'external_source_type']].fillna(value='')
    comments[['reaction_count']] = comments[['reaction_count']].fillna(value=0)
    comments[['post_id', 'group_id', 'comment_id']] = comments[['post_id', 'group_id', 'comment_id']].fillna(value=-1)


    translate={"ä":"a","ö":"o","ü":"u","ß":"ss","Ä":"A","Ö":"O","Ü":"U"}
    def translation(string,x):
        for key in x.keys():
            string=string.replace(key,x[key])
        return string
    posts['content']=posts.apply(lambda x: translation(x['content'],translate),axis=1)
    comments['content']=comments.apply(lambda x: translation(x['content'],translate),axis=1)

    tokens=0
    request = 0
    seconds=0.1
    start_time =datetime.now()
    counter=0
    for idx, post in posts.iterrows():
        counter +=1

        if counter%1000==0:
            seconds = (datetime.now() - start_time).seconds
            print("Requests:%s, Tokens:%s, Seconds:%s, Requests/second:%s"%(request,tokens,seconds,request/seconds))

        id += 1
        content_encoded=post['content'].encode('ascii','ignore').decode('utf-8','ignore').strip()
        if content_encoded=="":continue

        if len(comments[comments.post_id == post['post_id']]['content'])>0:
            context= ". ".join(comments[comments.post_id == post['post_id']]['content'].tolist()).encode('ascii','ignore').decode('utf-8','ignore')
        try:
            seconds = (datetime.now() - start_time).seconds
            request+=1
            tokens+=len(content_encoded.split(" "))

            response = client.embeddings.create(model=deployment_name, input=[content_encoded])
            embedding_available = response.data[0].embedding[0]
            if context:

                response = client.embeddings.create(model=deployment_name, input=[context])
                tokens += len(context.split(" "))

                request += 1
#7048
                embedding_available = response.data[0].embedding[0]
        except Exception as e:
            print("Requests:%s, Tokens:%s, Seconds:%s, Requests/second:%s"%(request,tokens,seconds,request/seconds))
            continue

if __name__=="__main__":
    index_without_embedding("embedding_test4")
    # client = AzureOpenAI(
    #     api_key=api_key,
    #     api_version=api_version,
    #     azure_endpoint=endpoint
    # )
    #
    # print('Sending a test completion job')
    # start_phrase = 'Write a tagline for an ice cream shop. '
    # response = client.embeddings.create(model=deployment_name, input=start_phrase)
    # # text = response['choices'][0]['text'].replace('\n', '').replace(' .', '.').strip()
    # print(response)
