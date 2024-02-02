from connection import get_connection
from opensearchpy import OpenSearch
import BertEncoder as be
import json
import pickle
import openai
import numpy as np
from numpy.linalg import norm

# Constants
THRESHOLD = 0.8
PCA_MODEL_FILE = 'pca_model_ada.pkl'
EMBEDDING_MODEL = 'text-embedding-ada-002'
OPENAI_API_KEY = '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'


def get_openai_embedding(question):
    # Load PCA model
    with open(PCA_MODEL_FILE, 'rb') as f:
        pca_trafo = pickle.load(f)

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
        transformed_embedding = pca_trafo.transform([embedded_question])[0]
        return transformed_embedding

    return get_embedded_question(question)


def get_bert_embedding(question):
    encoder = be.BertEncoder(bert_url='bert-base-german-dbmdz-uncased')
    return encoder([question]).squeeze().numpy()


if __name__ == "__main__":
    client = get_connection()
    q = 'windsurfen'
    # for i in range(100):
    # get_openai_embedding(
    #     "Gibt es in dem Hotel Familienzimmer mit jeweils zwei Zimmern mit Verbindungstür?"
    #     "Oder gibt es Familienzimmer mit 2 Schlafzimmern?")
    embedding = get_openai_embedding(q)
    query = {
        "size": 5,
        'query': {
            # 'multi_match': {
            #     'fields_': ['text'],
            #     'query': 'windsurfen',
            #     'type': 'best_fields',
            #     'fuzziness': 'AUTO'
            # }
            "knn":{
                "embedding": {
                    "vector": embedding,
                    "k": 10,

                }
            }

        }
    }

    response = client.search(
        body=query,
        index='embedding_test4'
    )
    print("Question: ", q)
    for hit in response["hits"]["hits"]:
        print(hit["_source"]["text"].replace("Frage von Check24 KundIn am", ""))
    with open('response.json', 'w') as outfile:
        json.dump(response, outfile)
    print(response)
