import pandas as pd
import requests
import time
import sys
import os
from tqdm import tqdm

from kbutil.dbutil import db_connector

sys.path.append(os.getcwd())

def create_directory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Failed to create the directory.")


def make_file_path(data, big_list):
    
    str_big = ""

    for val_big in big_list:

        if val_big == data['big_category']:
            str_big = val_big

    file_path = str_big + "/" + data['small_category']               
    file_path = "./data/data_n3_2/" + file_path

    # print(file_path)
    create_directory(file_path)

    return file_path


# 키워드 수정
def replace_keyword(data, data_replace):
    # print(data)
    # print(data_replace)

    replace_keyword = data['object_hangle']

    temp = data_replace.where(data_replace['keyword'] == data['object_hangle'])
    
    df_temp = temp.dropna(axis=0)
    df_temp = df_temp.reset_index(drop=True)

    if df_temp.size > 0:        
        replace_keyword = df_temp['replace_keyword'][0]

    return replace_keyword



# URL 요청
def request_url(row_data):    
    
    # DB 연결
    conn = db_connector("real")
    cur = conn.cursor()

    search_word = row_data['object_hangle']

    select_sql = """ SELECT COUNT(*) AS CNT
                        FROM knowlegebase_db
                       WHERE (search_word = ? OR e1_label = ?)
                         AND collect_target= 'ko.dbpedia'
                 """
    values_select_word = (search_word, search_word)
    cur.execute(select_sql, values_select_word)
    result_word = cur.fetchone()
    time.sleep(0.005)    
    conn.close()

    if result_word[0] == 0:
        # 데이터가 없을 경우에만 다운로드
        url = 'http://ko.dbpedia.org/data/' + search_word + '.n3'
        
        time.sleep(2.05)
        r_data = requests.get(url)
        r_text = r_data.text

        save_path = row_data['file_path'] + '/' + search_word + '.n3'

        with open(save_path, "w", encoding="utf8") as file:
            file.write(r_text)

    

if __name__ == '__main__':
    
    print('=== 객체 한글 키워드 기준으로 DBpedia 데이터 수집 코드 (START)')
    keyword_file = './data_info/korea_image_obejct_list_1123.xlsx'

    df_info = pd.read_excel(keyword_file, engine='openpyxl')
    df_info = df_info.fillna('')

    np_big_category = df_info['big_category'].unique()

    big_category_list = []
    
    for idx, val_big in enumerate(np_big_category):
        big_category_list.append(val_big)
    
    # 저장 경로 생성
    df_info['file_path'] = df_info.apply(lambda x: make_file_path(x, big_category_list), axis = 1)

    # URL 요청 및 파일 저장
    tqdm.pandas()
    df_info.progress_apply(lambda x: request_url(x), axis=1)

    print('=== 객체 한글 키워드 기준으로 DBpedia 데이터 수집 코드 (SUCCESS) ======================')    
    print('====================================================================================')
