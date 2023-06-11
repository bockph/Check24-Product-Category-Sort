"""
Creates a Uvicorn webapp backend
@author: Philipp
"""

import os
import pickle
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from search import get_categories
from BertEncoder import BertEncoder
from sort import compare
import translators as ts




class inferenceRequest(BaseModel):
    search_query: str



app = FastAPI()
app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent.absolute() / "web_app_templates"),
    name="static",
)
templates = Jinja2Templates(directory="web_app_templates")
encoder = BertEncoder(bert_url='bert-base-uncased')#german-cased')


# @app.post("/doc")
# async def perform_eval(document: Document):
#     # document contains the text submitted via the webpage
#     # 1. segment into sentences
#     sentences = segmentation_pipeline.run_segmenter(document.text)
#     # 2. encode with bert
#     encoded_sentences = sentence_encoder.sentence_bert_embeddings(sentences, model=1)
#     # 3 get model and do prediction
#     path = os.path.dirname(os.path.abspath(__file__))
#     weight_path = path + "\..\data\model_weights\LSTM Net_balanced_DICE_batch_size_1.dat"
#     predicted_sentences = classifier.predict_role(encoded_sentences, LSTM_Net(), weight_path)
#     # 4. sort sentences by occuring in document
#     predicted_sentences = predicted_sentences.sort_values('start_char', ascending=True)
#
#     response = []
#     # round probabilities
#     predicted_sentences['prob'] = predicted_sentences['prob'].round(2)
#
#     # create dataobject for webpage
#     for index, sentence in predicted_sentences.iterrows():
#         response.append({"sentence": sentence['text'], "role": sentence['role'], "prob": sentence['prob']})
#
#     return response

@app.post("/process")
async def processQuery(request: inferenceRequest):

    search_query = request.search_query
    category_list = get_categories(search_query)
    search_query_eng = ts.google(search_query,from_language='de', to_language='en')
    category_list_both = [(ts.baidu(category,from_language='de', to_language='en'),category) for category in category_list]
    category_list_eng = [english for english,german in category_list_both]
    print(category_list)
    sorted_list, string_sorted, string_unsorted = compare(search_query_eng, category_list_eng=category_list_eng,category_list_ger=category_list, encoder=encoder)
    answer={"org_list":category_list,"new_list":sorted_list}

    return answer
# Schuhkipper (7.89)
# Schlafzimmer (8.06)
# Wohnzimmerschränke (8.32)
# Kleiderschränke (8.63)
# Büroregale (8.7)
# Wohnzimmer (8.79)
# Esszimmer (8.86)
# Spiegelschränke (8.95)
# Aktenregale (8.97)
# Schuhregale (9.04)
# Stoffschränke (9.09)
# Badregale (9.2)
# Bücherregale (9.24)
# Schuhbänke (9.59)
# Garderobenschränke (9.69)
# Rollcontainer (9.88)
# Regale (9.89)
# Mehrzweckschränke (9.94)
# Schuhschränke (10.11)
# Drehtürenschränke (10.18)

@app.get("/getDocIds")
async def doc_ids(request: Request):
    print("hello")



    # return {"doc_ids": doc_ids, "model_names": model_names}


@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/styles.css")
async def css(request: Request):
    print("hello")
    return templates.TemplateResponse("styles.css", {"request": request})


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=80)
