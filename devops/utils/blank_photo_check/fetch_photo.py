import requests
import time
import os
from PIL import Image, ImageFile
import imagehash

no_photo_list = []
not_found = []

def download_image(image_id):
    image_id = str(image_id)
    url = f"https://hrp.cmsk1979.com/hrmsPath/downFile/headUrl/{image_id}.jpg?lodingImgKey=dGhpc0lzTG9kaW5nTG9jYWxIZWFkSW1nS2V5"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        save_dir = "./photo_base"
        os.makedirs(save_dir, exist_ok=True)
        filename = os.path.join(save_dir, f"{image_id}.jpg")

        with open(filename, "wb") as f:
            f.write(response.content)
        return 'done'
    except requests.exceptions.RequestException as e:
        print(f"❌ 下载失败（ID: {image_id}）：{e}")
        # 没有这个人
        not_found.append(image_id)

    time.sleep(1)  # 每次请求后等待 0.5 秒
def photo_match(id):
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    hash2 = imagehash.average_hash(Image.open('response.jpeg'))
    try:
        hash1 = imagehash.average_hash(Image.open(f'./photo_base/{id}.jpg'))
    except Exception as e:
        # 下载的图片报错
        not_found.append(id)

    #判断图片是否为空白
    return 0 if hash1 == hash2 else 1

with open('id_list', 'r') as id_list:
    for line in id_list:
        line = line.replace('\n', '')
        done = download_image(line)

        # 下载的文件为空
        if os.path.exists(f'./photo_base/{line}.jpg') and os.path.getsize(f'./photo_base/{line}.jpg') == 0:
            not_found.append(line)
        else:
            if done == 'done':
                result = photo_match(line)
                if result == 0:
                    no_photo_list.append(line)

                print(line, '@', result, ';')


print(no_photo_list)
print('--------')
print(not_found)

if no_photo_list:
    with open('no_photo_list.txt', 'w', encoding='utf-8') as file:
        file.write(no_photo_list)
if not_found:
    with open('not_found.txt', 'w', encoding='utf-8') as file:
        file.write(not_found)
