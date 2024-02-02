import json

import pandas as pd
from tqdm import tqdm
from opensearchpy import OpenSearch


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



def create_index(index_name, embedding_dim):
    client = get_connection()
    with open("german_stopwords_full.txt", "r") as f:
        german_stopwords = f.read().splitlines()
    index_body = {
        # 'settings': {
        #     'index.knn': "true"
        # },
        "settings": {
            "index": {
                "knn": True,
                # "default_pipeline": "openai-text2embedding-pipeline",

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
        #         "analysis": {
        #             "filter": {
        #                 "filter_german_stemmer": {
        #                     "type": "stemmer",
        #                     "language": "light_german"
        #                 }
        #             },
        #             "normalizer": {
        #                 "lowercase_normalizer": {
        #                     "type": "custom",
        #                     "filter": [
        #                         "lowercase",
        #                         "asciifolding",
        #                         "trim"
        #                     ]
        #                 }
        #             },
        #             "analyzer": {
        #                 "german_analyzer": {
        #                     "filter": [
        #                         "lowercase",
        #                         "asciifolding",
        #                         "filter_german_stemmer"
        #                     ],
        #                     "tokenizer": "standard"
        #                 }
        #             }
        #         }
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

                "embedding": {
                    "type": "knn_vector",
                    "dimension": 1536,
                    "method": {
                        "engine": "nmslib",
                        "space_type": "cosinesimil",
                        "name": "hnsw",
                        "parameters": {}
                    }
                },
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
    translate={"ä":"a","ö":"o","ü":"u","ß":"ss","Ä":"A","Ö":"O","Ü":"U"}
    def translation(string,x):
        for key in x.keys():
            string=string.replace(key,x[key])
        return string
    posts['content']=posts.apply(lambda x: translation(x['content'],translate),axis=1)
    comments['content']=comments.apply(lambda x: translation(x['content'],translate),axis=1)


    for idx, post in tqdm(posts.iterrows()):
        id += 1
        content_encoded=post['content'].encode('ascii','ignore').decode('utf-8','ignore').strip()
        if content_encoded=="":continue
        document = {
            "content_type": "post",

            "post_id": post['post_id'],
            # "comment_id":
            "group_id": post['group_id'],
            # "content": post['content'].encode("utf-8",'ignore').decode('utf-8', 'ignore'),
            "content": content_encoded,
            "content_clean": content_encoded,
            "context": ". ".join(comments[comments.post_id == post['post_id']]['content'].tolist()).encode('ascii','ignore').decode('utf-8','ignore'),
            "created_at": post['created_at'],
            "reaction_count": post['reaction_count'],
            "comment_count": post['comment_count'],
            "view_count": post['view_count'],
            "is_question": post['is_question'],
            "external_source_type": post['external_source_type']

        }
        response = client.index(
            index=index_name,
            body=document,
            id=id,
            refresh=True
        )

    for idx, comment in tqdm(comments.iterrows()):
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
            refresh=True
        )




if __name__ == "__main__":
    index_name = "content_search6"
    create_index(index_name,1536)
    index_without_embedding(index_name)
    # insert_embeddings_openai(index_name)
