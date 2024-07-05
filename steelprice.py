# Filename: steelprice.py
# Author: eilhyo
# Date: July 4, 2024

import requests
import json
import pyodbc

#GLOBAL_VAR = 1000

class CustomError(Exception):
    def __init__(self, message):
        self.message = message

def get_latest_data():
    
    url = "https://mds.mysteel.com/dynamic/order/api/"
    headers = {
        "Content-Type":"application/json",
        "accessTokenSign":"",
        "infoOrData":"info"
        }
    payload = {"pageNum":1,"pageSize":1000,"includeInfo":True}
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if (response.status_code==200):
        return response.json()
    else:
        print(f"请求失败: {response.status_code}")
    pass

def get_history_data(index_code_list):

    url = "https://mds.mysteel.com/dynamic/order/api/"
    headers = {
        "Content-Type":"application/json",
        "accessTokenSign":"",
        "infoOrData":"data"
        }
    payload = {
        "indexCodes":index_code_list,
        "startTime":"2024-06-01",
        "endTime":"2024-07-04",
        "order":"desc"
        }
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if (response.status_code==200):
        return response.json()
    else:
        print(f"请求失败: {response.status_code}")
    pass

def sql_execute(insert_data_list):

    # 构建连接字符串
    server = '10.1.188.160'  
    port = '1433'
    database = 'DataReport'
    username = 'sa'
    password = ""
    conn_str = f'DRIVER={{SQL Server}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password}'

    # 建立连接
    conn = pyodbc.connect(conn_str)

    # 创建游标对象
    cursor = conn.cursor()

    sql_insert = """
    INSERT INTO etl_webprice 
    (ID,REGION_NAME,PROVINCE_NAME,CITY_NAME,CP_NAME,BREED_NAME,STANDARD_NAME,MQ_NAME,SC_NAME,METRIC_NAME,UNIT_NAME,DATA_VALUE,UPDATE_DATE,IS_NEW) 
    VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # 执行查询语句
    cursor.executemany(sql_insert, insert_data_list)

    # 提交事务   
    conn.commit()
    # 关闭游标和连接
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        json_data1 = get_latest_data()
        data_list1 = json_data1.get("data",{}).get("list",[])
        index_code_list = []
        index_code_dict = {}
        for i in data_list1:
            index_code = i.get("INDEX_CODE","")
            index_code_list.append(index_code)
            index_code_dict[index_code]={
                "ID": i.get("ID"),
                "REGION_NAME": i.get("REGION_NAME",""),
                "PROVINCE_NAME": i.get("PROVINCE_NAME",""),
                "CITY_NAME": i.get("CITY_NAME",""),
                "CP_NAME": i.get("CP_NAME",""),
                "BREED_NAME": i.get("BREED_NAME",""),
                "STANDARD_NAME": i.get("STANDARD_NAME",""),
                "MQ_NAME": i.get("MQ_NAME",""),
                "SC_NAME": i.get("SC_NAME",""),
                "METRIC_NAME": i.get("METRIC_NAME",""),
                "UNIT_NAME": i.get("UNIT_NAME",""),
                }

        json_data2 = get_history_data(index_code_list)
        data_list2 = json_data2.get("data",[])
        insert_data_list = []
        for i in data_list2:
            index_code = i.get("INDEX_CODE","")
            info_dict = index_code_dict.get(index_code,{})

            ID = info_dict.get("ID")
            REGION_NAME = info_dict.get("REGION_NAME","")
            PROVINCE_NAME = info_dict.get("PROVINCE_NAME","")
            CITY_NAME = info_dict.get("CITY_NAME","")
            CP_NAME = info_dict.get("CP_NAME","")
            BREED_NAME = info_dict.get("BREED_NAME","")
            STANDARD_NAME = info_dict.get("STANDARD_NAME","")
            MQ_NAME = info_dict.get("MQ_NAME","")
            SC_NAME = info_dict.get("SC_NAME","")
            METRIC_NAME = info_dict.get("METRIC_NAME","")
            UNIT_NAME = info_dict.get("UNIT_NAME","")

            data_list3 = i.get("dataList",[])
            for j in data_list3:
                DATA_VALUE = j.get("DATA_VALUE")
                DATA_DATE = j.get("DATA_DATE","")

                insert_data_list.append((ID,REGION_NAME,PROVINCE_NAME,CITY_NAME,CP_NAME,BREED_NAME,STANDARD_NAME,MQ_NAME,SC_NAME,METRIC_NAME,UNIT_NAME,DATA_VALUE,DATA_DATE,0))
        print("插入："+str(len(insert_data_list)))  
        sql_execute(insert_data_list)
        #raise ValueError("值不能为负数")
        #raise CustomError("自定义错误发生")
        
    except Exception as e:
        print(f"捕获到异常: {e}")
    else:
        print("程序运行成功")
    finally:
        print("程序执行完毕")