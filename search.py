import requests
from bs4 import BeautifulSoup
import ast
import unicodedata
def get_html_result(search_query):
    url = "http://www.check24.de/suche/?q=" + str(search_query)
    try:
        with requests.get(url, params={"sort": "3Apopularity", "page": "1"}) as response:
            html = response.text
              # the HTML code you've written above
            parsed_html = BeautifulSoup(html)
            return parsed_html

    except Exception as e:
        print(e)
        print("Suche: " + str(search_query) + " hat nicht funktioniert,bitte verwende einen anderen Suchbegriff.")


def get_categories(search_query):
    try:
        parsed_html = get_html_result(search_query)
        div = parsed_html.find("div", {"id": "c24-content"})
        js = div.find("script")
        # test =json.loads(js.contents[0].replace("window.__INIT_C9_SERP_STATE__=",""))
        js_text = js.contents[0].replace("\n", "").replace("\t", "").replace(
            "window.__INIT_C9_SERP_STATE__=", "")
        js_text = js_text.split("\"label\":\"Kategorie\",\"values\":[")[1].split("]")[0].encode().decode("unicode_escape")
        # js_text=unicode(js_text, "utf-8")
        # js_text = unicodedata.normalize('NFKD', js_text).encode('ascii', 'ignore').decode()
        js_text = "[" + str(js_text).replace("false", "False").replace("true", "True") + "]"
        dicts = ast.literal_eval(js_text)
        categories = [dict["label"] for dict in dicts]
        return categories
    except Exception as e:
        print(e)
        print("Die Kategorien zu dem Suchbegriff: " + str(
            search_query) + " konnten nicht gefunden werden, bitte verwende einen anderen Suchbegriff ( klassische Produktsuche).")