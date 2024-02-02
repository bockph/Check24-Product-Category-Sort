import json
import time
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from opensearchpy import OpenSearch
import BertEncoder as be
import pickle
import openai
import numpy as np
import time
# Constants
THRESHOLD = 0.8
PCA_MODEL_FILE = 'pca_model_ada.pkl'
EMBEDDING_MODEL = 'text-embedding-ada-002'
OPENAI_API_KEY = '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'

host = 'localhost'
port = 9200
auth = ('admin', 'admin')  # For testing only. Don't store credentials in code.


def get_connection():
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=True,
        http_auth=auth,
        # use_ssl=True,
        # verify_certs=True,
        # ssl_assert_hostname='localhost',
        # ssl_show_warn=True
    )
    return client


def get_openai_embedding(question):
    # Load PCA model
    # with open(PCA_MODEL_FILE, 'rb') as f:
    #     pca_trafo = pickle.load(f)

    openai.api_key = OPENAI_API_KEY

    def get_embedded_question(question: str) -> np.ndarray[np.float64, ...]:
        """
        Embeds a question with the openai api and transforms it with the pca model
        """
        # response = embedding_client.create(input=[question], model=EMBEDDING_MODEL)
        response = openai.Embedding.create(
            model=EMBEDDING_MODEL,
            input=[question]
        )
        embedded_question = [np.array(resp.embedding) for resp in response.data][0]
        # transformed_embedding = pca_trafo.transform([embedded_question])[0]
        return embedded_question

    return get_embedded_question(question)


def create_index(index_name, embedding_dim):
    client = get_connection()
    with open("german_stopwords_full.txt", "r") as f:
        german_stopwords = f.read().splitlines()
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "default_pipeline": "azure_content_embedding",
                "number_of_shards": 1,
                "number_of_replicas": 1,
                "analysis": {
                    "filter": {
                        "german_stop": {
                            "type": "stop",
                            "stopwords": german_stopwords
                        },
                        "german_stemmer": {
                            "type": "stemmer",
                            "language": "light_german"
                        }
                    },
                    "analyzer": {
                        "rebuilt_german": {
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                'asciifolding',
                                "german_stop",
                                "german_normalization",
                                "german_stemmer"
                            ]
                        }
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "content_type": {
                    "type": "keyword"
                },
                "last_indexed_at": {
                    "type": "date",
                    "format": "yyyy-MM-dd'T'HH:mm:ss"
                },
                "post_id": {
                    "type": "integer"
                },
                "comment_id": {
                    "type": "integer"
                },
                "group_id": {
                    "type": "integer"
                },
                "content": {
                    "type": "text",
                },
                "content_clean": {
                    "type": "text",
                    "analyzer": "rebuilt_german"
                },
                "context": {
                    "type": "text",
                },
                "created_at": {
                    "type": "date",
                    "format": 'yyyy-MM-dd HH:mm:ss'
                },
                "reaction_count": {
                    "type": "integer"
                },
                "comment_count": {
                    "type": "integer"
                },
                "view_count": {
                    "type": "integer"
                },
                "is_question": {
                    "type": "boolean"
                },
                "external_source_type": {
                    "type": "keyword"
                },
                "content_embedding": {
                    "type": "knn_vector",
                    "dimension": 1536,
                    "method": {
                        "engine": "nmslib",
                        "space_type": "cosinesimil",
                        "name": "hnsw",
                        "parameters": {}
                    }
                }
            }
        }
    }

    response = client.indices.create(index_name, body=index_body)
    print('\nCreating index:')
    print(response)


def index_without_embedding(index_name):
    client = get_connection()
    posts = pd.read_csv("posts.csv")
    comments = pd.read_csv("comments.csv")

    id = 0
    posts[['content', 'external_source_type', 'reaction_count']] = posts[
        ['content', 'external_source_type', 'reaction_count']].fillna(value='')
    posts[['comment_count', 'view_count']] = posts[['comment_count', 'view_count']].fillna(value=0)
    posts[['post_id', 'group_id']] = posts[['post_id', 'group_id']].fillna(value=-1)
    posts['is_question'] = posts['is_question'].astype('bool')
    comments[['content', 'external_source_type']] = comments[['content', 'external_source_type']].fillna(value='')
    comments[['reaction_count']] = comments[['reaction_count']].fillna(value=0)
    comments[['post_id', 'group_id', 'comment_id']] = comments[['post_id', 'group_id', 'comment_id']].fillna(value=-1)

    def calc_reaction_count(x):
        try:
            reactions = json.loads(x['reaction_count'])
            return sum(reactions.values())
        except:
            return 0

    posts['reaction_count'] = posts.apply(lambda x: calc_reaction_count(x), axis=1)
    comments['reaction_count'] = comments.apply(lambda x: calc_reaction_count(x), axis=1)
    translate={"ä":"ae","ö":"oe","ü":"ue","ß":"ss","Ä":"AE","Ö":"OE","Ü":"UE"}
    def translation(string,x):
        for key in x.keys():
            string=string.replace(key,x[key])
        return string
    posts['content']=posts.apply(lambda x: translation(x['content'],translate),axis=1)
    comments['content']=comments.apply(lambda x: translation(x['content'],translate),axis=1)


    for idx, post in tqdm(posts.iterrows()):
        start_time = time.time()
        id += 1
        content_encoded=post['content'].encode('ascii','ignore').decode('utf-8','ignore').strip()
        if content_encoded=="":continue
        document = {
            "content_type": "post",
            "post_id": post['post_id'],
            "group_id": post['group_id'],
            "content": content_encoded,
            "content_clean": content_encoded,
            "created_at": post['created_at'],
            "reaction_count": post['reaction_count'],
            "comment_count": post['comment_count'],
            "view_count": post['view_count'],
            "is_question": post['is_question'],
            "external_source_type": post['external_source_type']
        }
        if len(comments[comments.post_id == post['post_id']]['content'])>0:
            document["context"]= ". ".join(comments[comments.post_id == post['post_id']]['content'].tolist()).encode('ascii','ignore').decode('utf-8','ignore')

        response = client.index(
            index=index_name,
            body=document,
            id=id,
            refresh=True,
            request_timeout=30
        )
        end_time = time.time()
        time_diff = end_time - start_time
        if time_diff<0.2:
            time.sleep(0.2-time_diff)

    for idx, comment in tqdm(comments.iterrows()):
        start_time = time.time()

        id += 1
        content_encoded=comment['content'].encode('ascii','ignore').decode('utf-8','ignore').strip()

        if content_encoded=="":continue
        document = {
            "content_type": "comment",

            "post_id": comment['post_id'],
            "comment_id": comment['comment_id'],
            "group_id": comment['group_id'],
            "content": content_encoded,
            "content_clean": content_encoded,
            "context": ". ".join(posts[posts.post_id == comment['post_id']]['content'].tolist()).encode('ascii','ignore').decode('utf-8','ignore'),

            "created_at": comment['created_at'],
            "reaction_count": comment['reaction_count'],
            "external_source_type": comment['external_source_type']
        }
        response = client.index(
            index=index_name,
            body=document,
            id=id,
            refresh=True,
            request_timeout=30
        )
        end_time = time.time()
        time_diff = end_time - start_time
        if time_diff < 0.2:
            time.sleep(0.2 - time_diff)


def insert_embeddings_openai(index_name):
    client = get_connection()
    question_embeddings = pd.read_csv('question_embeddings.csv', sep=';')
    answer_embeddings = pd.read_csv('answer_embeddings.csv', sep=';')

    # Transform embeddings to numpy arrays
    question_embeddings['embedding'] = question_embeddings['embedding'].apply(lambda x: np.array(eval(x)))
    answer_embeddings['embedding'] = answer_embeddings['embedding'].apply(lambda x: np.array(eval(x)))
    for idx, post in tqdm(question_embeddings.iterrows()):
        embedding = get_openai_embedding(post['text'])
        document = {
            'text': post['text'],
            'type': 1,
            'embedding': embedding,
            'group_id': post['hotel_id']
        }
        id = idx

        response = client.index(
            index=index_name,
            body=document,
            id=id,
            refresh=True
        )
    print("stop")


def insert_embeddings_bert():
    client = get_connection()
    posts1 = pd.read_csv("../prod2_post.csv")[:10].reset_index(drop=True)
    posts2 = pd.read_csv("../prod2_post.csv")[100:200].reset_index(drop=True)
    posts3 = pd.read_csv("../prod2_post.csv")[200:300].reset_index(drop=True)
    encoder = be.BertEncoder(bert_url='bert-base-german-dbmdz-uncased')

    #   posts['embedding']=posts.apply(lambda x: encoder(x['content']).squeeze().numpy(),axis=1)

    post_embeddings = [x for x in encoder(posts1['content']).squeeze().numpy()]
    for idx, post in posts1.iterrows():
        document = {
            'text': post['content'],
            'type': 1,
            'embedding': post_embeddings[idx],
            'group_id': post['group_id']
        }
        id = idx

        response = client.index(
            index=index_name,
            body=document,
            id=id,
            refresh=True
        )
        print('\nAdding document:')
        print(response)


if __name__ == "__main__":
    index_name = "content_search_jan_final3"
    create_index(index_name,1536)
    index_without_embedding(index_name)
    # insert_embeddings_openai(index_name)
