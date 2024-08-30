import requests
import pandas as pd
from common import flatten,add_new_column
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor


def call_api(token,url,proj_id):
    headers = {
                "accept": "application/json",
                "authorization": f"Bearer {token['timesheet']}"
            }
    l=[]
    try:
        while url is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            
            ## Just for project phases
            if jsondata['data']!=[]:
                l[-1]['project_id']=proj_id
            url=jsondata['nextPage']
            # print(f'{a}/{len(id_list)}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)
    return l


def phases(token,engine,proj_df,target_schema_name,target_table_name,apiurl):
    id_list = proj_df['id'].unique().tolist()
    urls = []
    apiurl = apiurl+"?pageSize=200"
    for id in id_list:
        # urls.append([f"https://sedin.keka.com/api/v1/psa/projects/{id}/phases?pageSize=200",id])
        urls.append([apiurl.format(projectId = id),id])

    executor = ThreadPoolExecutor(max_workers=12)
    futures = []
    for url,proj_id in urls:
        future = executor.submit(call_api,(token),(url),(proj_id))
        futures.append(future)

    yo = []
    for future in futures:
        yo = yo + future.result()    
            
    flat_l=[flatten(item) for item in yo]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()
    df.rename(columns=rename_map,
          inplace=True)
    
    #   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)
    # df.to_csv(f"csv/phases.csv")
    try:
        with engine.begin() as connection:
            result = connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)
        

def task_call_api(token,url):
    headers = {
                "accept": "application/json",
                "authorization": f"Bearer {token['timesheet']}"
            }
    l=[]
    a=0
    try:
        while url is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            url=jsondata['nextPage']
            a+=1
            # print(a)
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)
    return l


def tasks(token,engine,proj_df,target_schema_name,target_table_name,apiurl):
    id_list = proj_df['id'].unique().tolist()
    urls = []
    apiurl = apiurl + "?pageSize=200"
    for id in id_list:
        # urls.append(f"https://sedin.keka.com/api/v1/psa/projects/{id}/tasks?pageSize=200")
        urls.append(apiurl.format(projectId=id))
    executor = ThreadPoolExecutor(max_workers=12)
    futures = []
    for url in urls:
        future = executor.submit(task_call_api,(token),(url))
        futures.append(future)

    yo = []
    for future in futures:
        yo = yo + future.result()    
            
    flat_l=[flatten(item) for item in yo]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()
    df.rename(columns=rename_map,
          inplace=True)
    
    #   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)
    # df.to_csv(f"csv/tasks.csv")
    try:
        with engine.begin() as connection:
            result = connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)
    return df
        