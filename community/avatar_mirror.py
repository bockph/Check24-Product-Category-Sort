import requests
import json
import os
from PIL import Image
from io import BytesIO
api_key ="cb515bd4ebf943bcaf8f34709d2f95bc"
from tqdm import tqdm
def generate_face(png="test_img.jpg",style="mau"):
    url = "https://public-api.mirror-ai.net/v2/generate?style="+style
    files = {"photo": open(png, 'rb')}
    headers = {
        "X-Token": api_key,
    }

    response = requests.post(url, files=files, headers=headers)

    print(response.json())
    return response.json()
def get_all_parts(face_id):
    url = "https://public-api.mirror-ai.net/v2/get_all_parts?face_id="+face_id
    headers = {
        "X-Token": api_key,
    }
    response = requests.get(url, headers=headers)
    print(response.json())
    return response.json()

def apply_part():
    url = "https://public-api.mirror-ai.net/v2/apply_parts?face_id=TCE6mRO2TP64TwVdfUSm-A&parts=%7B%22eye%22%3A7%7D&colors=&clothes=&preview=0";

    headers = {
        "X-Token": api_key,
    }
    response = requests.post(url, headers=headers)
    print(response.json())
    return response.json()
def download_resource(dict,folder):
    if 'material' in dict.keys() and dict['material']=="skin": return
    name = dict["name"]
    parts = dict['templates']
    for part in parts:
        url = part["url"]
       # if "mau" in url :continue
       # else: return

        id = part["id"]
        img_data=requests.get(url).content
        img = Image.open(BytesIO(img_data))
        img = img.resize((62, 62))
        # img=img.compress_image(img_data)

        if not os.path.exists(os.path.join("mirror_data",folder,name)):
            os.makedirs(os.path.join("mirror_data",folder,name))
        path_to_store=os.path.join("mirror_data",folder,name,str(id)+".png")
        img.save(path_to_store,optimize=True)
        # with open(os.path.join("mirror_data",folder,name,str(id)+".png"), 'wb') as outfile:
        #     outfile.write(img_data)
    # img_data=requests.get(url).content
    # with open(os.path.join(folder,type,id+".jpg"), 'wb') as outfile:
    #     outfile.write(img_data)
#/Users/philipp.bock/PycharmProjects/Check24-Product-Category-Sort/community/mirror_data

# write a function to load all image in folder mirror_data/body_parts using pillow, resize to 62x62 and save to mirror_data/body_parts_compressed
def compress_images(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith((".jpg", ".JPG", ".jpeg", ".JPEG", ".png", ".PNG")):
                img = Image.open(os.path.join(root, file))
                img = img.resize((62, 62))
                out_path = os.path.join(root.replace("body_parts","body_parts_compressed"), file.replace(".jpg",".png"))
                img.save(out_path, optimize=True, bits=4)

    # return img


if __name__=='__main__':

    compress_images("mirror_data/body_parts")
    #result = generate_face()

    # result = get_all_parts("TCE6mRO2TP64TwVdfUSm-A")
    # with open('mirror_getallparts_response.json', 'w') as outfile:
    #     json.dump(result, outfile)

    with open('mirror_oldmenhat.json',"rb") as json_file:
        data = json.load(json_file)
    #     # clothes = data["clothes"]
        tabs = data["tabs"]
    #     # download_resource(clothes['bottom'][0],"clothes")
    #     # download_resource(clothes['bottom'][1],"clothes")
    #     # download_resource(clothes['dress'], "clothes")
    #     # download_resource(clothes['outer'][0], "clothes")
    #     # download_resource(clothes['outer'][1], "clothes")
    #     # download_resource(clothes['shoes'][0], "clothes")
    #     #
    #     # download_resource(clothes['upper'][0], "clothes")
    #     # download_resource(clothes['upper'][1], "clothes")
        for tab in tqdm(tabs):
            download_resource(tab, "body_parts_compressed")

    #test apply part
    # result = apply_part()