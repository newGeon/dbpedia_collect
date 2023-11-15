import pandas as pd
import datetime
import mariadb
import json
import time
import os
import re

from tqdm import tqdm
from rdflib import Graph

from kbutil.dbutil import db_connector
from kbutil.prefix import TEXT_PREFIX, DICT_PREFIX

if __name__ == '__main__':

    # Property 정보 파일
    ontology_property_file = './data_info/ontology_property_1011.xlsx'
    df_property = pd.read_excel(ontology_property_file, engine='openpyxl')

    top_path = './data/data_n3_2'
    big_class_list = os.listdir(top_path)

    # DB 연결
    conn = db_connector("real")
    cur = conn.cursor()

    big_class_list = ['한국']

    for big_one in tqdm(big_class_list):
        
        big_path = top_path + '/' + big_one
        big_class = big_one

        # 대분류 기준 공통 프로퍼티
        # big_property_list = df_property['property'][(df_property['big_category'] == big_class) & (df_property['small_category'] == '공통')].to_list()
        big_property_list = df_property['property'].to_list()

        small_class_list = os.listdir(big_path)
        # small_class_list = ['허브']

        for small_one in small_class_list :
            
            small_path = big_path + '/' + small_one
            small_class = small_one

            ont_file_list = os.listdir(small_path)
            # ont_file_list = ['고수_(식물).n3']
            for ont_one in ont_file_list:
                
                # n3 파일
                file_path = small_path + '/' + ont_one
                # print(file_path) 

                g_ont = Graph()

                try:
                    g_ont.parse(file_path)
                    property_list = df_property['property'][(df_property['big_category'] == big_class) & (df_property['small_category'] == small_class)].to_list()

                    total_property_list = []

                    for one in big_property_list:
                        total_property_list.append(one)

                    for one in property_list:
                        total_property_list.append(one)
                    
                    total_property_list = list(set(total_property_list))

                    search_word = ont_one.split('.', 1)[0]

                    # 데이터 체크
                    select_sql =  """ SELECT COUNT(*) AS CNT
                                        FROM knowlegebase_db
                                       WHERE search_word = ?
                                          OR e1_label = ?
                                  """
                    values_select_word = (search_word, search_word)
                    cur.execute(select_sql, values_select_word)
                    
                    result_word = cur.fetchone()
                    
                    if result_word[0] == 0:
                        print(search_word)

                        # 데이터가 없을 경우에만 INSERT
                        search_text = "dbpedia-ko:" + search_word
                        search_url = "<http://ko.dbpedia.org/resource/" + search_word + ">"

                        e1_split_list = search_text.split(':')

                        # 저장 데이터 선언
                        e1_label = e1_split_list[1]
                        e1 = DICT_PREFIX[e1_split_list[0]] + e1_split_list[1]

                        print('e1_label : {} '.format(e1_label))
                        print('e1 : {} '.format(e1))



                        for one_property in total_property_list:
                            
                            r_property = ""
                            
                            # abstract 인 경우만 제외 하고 나머지는 동일하게
                            split_list = one_property.split(':')

                            one_prefix = split_list[0]
                            property_nm = split_list[1]

                            r_property = DICT_PREFIX[one_prefix] + property_nm

                            # print('r : {} '.format(r_property))

                            text_query = """
                                SELECT DISTINCT ?o
                                WHERE { """ + search_url + """ 
                                    """ + one_property + """ ?o 
                                }
                            """

                            fianal_query = TEXT_PREFIX + text_query
                            fianal_query = fianal_query.strip()
                            
                            for ont_result in g_ont.query(fianal_query):

                                e2_label = ""
                                e2 = ""
                                
                                surface_ko = []                        
                                context = ""                        
                                ont_text = ont_result["o"]
                                
                                if re.search('^http', ont_text) == None:
                                    # print("HTTP 없음 >>> 그냥 라벨임")
                                
                                    if property_nm == "abstract":
                                        context = ont_text
                                    else:
                                        e2_label = ont_text
                                        e2 = None
                                else:
                                    # http가 있는 경우는 링크
                                    temp_split = ont_text.rsplit("/", 1)

                                    e2_label = temp_split[1]
                                    e2_label = re.sub(r'^분류\:', "", e2_label)

                                    e2 = temp_split[0] + '/' + temp_split[1]

                                    surface_ko.append(e1_label)
                                    surface_ko.append(property_nm)
                                    surface_ko.append(e2_label)


                                if len(surface_ko) == 0:
                                    surface_ko = None

                                # print('e2_label : {} '.format(e2_label))
                                # print('e2 : {} '.format(e2))
                                
                                if property_nm == "abstract":
                                    # abstract 인 경우는 다르게 진행
                                    ts_column = time.time()
                                    timestamp_column = datetime.datetime.fromtimestamp(ts_column).strftime('%Y-%m-%d %H:%M:%S')

                                    insert_column_sql = """ INSERT INTO knowlegebase_db 
                                        (collect_type, collect_target, big_class, small_class, search_word, e1_label, r, context, e1, use_yn, reg_date) 
                                        VALUES('간접', 'ko.dbpedia', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """
                                    values_select_word = (big_class, small_class, e1_label, e1_label, r_property, context, e1, 'Y', timestamp_column)
                                    cur.execute(insert_column_sql, values_select_word)
                                    conn.commit()
                                else:
                                    ts_column = time.time()
                                    timestamp_column = datetime.datetime.fromtimestamp(ts_column).strftime('%Y-%m-%d %H:%M:%S')
                                    insert_column_sql =  """
                                                        INSERT INTO knowlegebase_db
                                                        (collect_type, collect_target, big_class, small_class, search_word, e1_label, e2_label, surface_ko, r, e1, e2, use_yn, reg_date)
                                                        VALUES('간접', 'ko.dbpedia', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?)
                                                        """
                                    values_select_word = (big_class, small_class, e1_label, e1_label, e2_label, surface_ko, r_property, e1, e2, timestamp_column)
                                    cur.execute(insert_column_sql, values_select_word)
                                    conn.commit()

                                time.sleep(0.001)
                        print('------------------')
                            
                    else:
                        print('데이터 있는 경우!!!!!!')
                        print(search_word)

                except:
                    print("오류 파일 : " + str(file_path))                    
    conn.close()

    print("------------------------------------------")
