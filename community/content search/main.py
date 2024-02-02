import torch
import pandas as pd
import BertEncoder as be
import openai
openai.api_key="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"


def get_openai_embedding(text_to_embed_list):
    # Embed a line of text
    response = openai.Embedding.create(
        model="babbage-002",
        input=text_to_embed_list
    )
    # Extract the AI output embedding as a list of floats
    embedding = response["data"][0]["embedding"]

    return embedding
if __name__ == "__main__":
    search_query = "How high is the temperature of the pool in summer"

    # search_query = "How good is the food. I would like to eat vegetarian"
    search_query = "vegetarian cuisine?"
    search_query = "What transportation is available from the airport to the hotel?"

    # get_openai_embedding(["What transportation is available from the airport to the hotel?"])

    # posts = [
    #     'Was ist denn alles in All Inclusive + enthalten? Sind hier alle Getränke auch Alkohol mit enthalten? ',
    #     'Fährt ein Bus in der Nähe des Hotels in die Hauptstadt?',
    #     "Hallo, sind die Sonnenschirme am Pool gratis und in ausreichender Stückzahl für alle Gäste vorhanden ?",
    #     'Gibt es im Hotel tagsüber Animation (z.B. Sport)?',
    #     "Hi, wir würden gern Weihnachten im Helios Bay Hotel verbringen. Baden im Meer wird vermutlich zu kalt, aber können sie mir sagen, ob der Pool beheizt ist? VG Max",
    #     "Hallo zusammen, welche Wassersportangebote gibt es am Strand?",
    #     "Hallo, wie ist die Qualität des Essens? Gibt es nur Buffet oder auch à la carte Restaurants? Viele Grüße. Dominik",
    #     "Wie Grad hat der Pool im Winter? ",
    #     'Gibt es vegetarische Küche?',
    #     'Hallo. Ist der Strand am Hotel privat oder öffentlich ?',
    #     'Ist es einfach möglich von HP zu AI vor Ort umzubuchen?',
    #     'Wie lange ist bitte das Cafe, das Hotel Restaurant sowie die Snackbar geöffnet? Oder wann öffnet es wieder?!',
    #     'Im Oktober 22 gab es weder ein Cafe, noch eine Snackbar und schon gar kein geöffnetes Restaurant.',
    #     'Adults only Zimmer was heist das',
    #     'Wie sehen die Doppelzimmer Club mit gartenblick aus?',
    #     'Gibt es Parkplätze in der Nähe?'
    #
    # ]
    posts = [
        'What is included in All Inclusive +? Are all drinks, including alcohol, included?',
        'Is there a bus near the hotel that goes to the capital?',
        'Hello, are the pool umbrellas free and are there enough for all guests?',
        'Is there daytime entertainment (e.g., sports) at the hotel?',
        "Hi, we would like to spend Christmas at the Helios Bay Hotel. Swimming in the sea will probably be too cold, but can you tell me if the pool is heated? Regards, Max",
        "Hello everyone, what water sports activities are available at the beach?",
        "Hello, what is the quality of the food? Are there only buffets or also à la carte restaurants? Best regards, Dominik",
        "What temperature does the pool reach in winter?",
        'Is there vegetarian cuisine?',
        'Hello. Is the hotels beach private or public?',
                                                       'Is it easy to upgrade from Half-Board to All-Inclusive on-site?',
    'How long is the café, hotel restaurant, and snack bar open? Or when will they reopen?!',
    'In October 22, there was neither a café, a snack bar, nor any restaurant open.',
    'What does "Adults only Zimmer" mean?',
    'What do the Double Club Rooms with garden view look like?',
    'Are there parking spaces nearby?'
    ]

    encoder = be.BertEncoder(bert_url='bert-base-german-dbmdz-uncased')
    posts_encoded = encoder(posts)
    search_query_encoded = encoder([search_query])
    cos = torch.nn.CosineSimilarity(dim=2, eps=1e-6)
    dist = cos(search_query_encoded, posts_encoded)
    data=pd.DataFrame()
    data["posts"]=posts
    data["dist"]=dist
    data['dist2'] = torch.cdist(search_query_encoded, posts_encoded).squeeze().numpy()
    print(data.sort_values(by="dist2",ascending=True))
    print("stop")
