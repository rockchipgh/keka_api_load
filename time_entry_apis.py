import pandas as pd
from common import flatten,call_api,add_new_column
from sqlalchemy import text
from concurrent.futures import ThreadPoolExecutor

def time_entries(token,engine,date_range,target_schema_name,target_table_name,apiurl):
    url = []
    dates = []
    apiurl = apiurl+'?pageSize=200&from={startdate}&to={enddate}'
    for i in date_range:
        # url.append(f"https://sedin.keka.com/api/v1/psa/timeentries?pageSize=200&from={i[0]}&to={i[1]}")
        url.append(apiurl.format(startdate = i[0],enddate = i[1]))
        dates.append([i[0],i[1]])

    # Create a thread pool for parallel API calls
    executor = ThreadPoolExecutor(max_workers=12)
    futures = []
    # Call apis in parallel
    for i,date in zip(url,dates):
        future = executor.submit(call_api,(token),(i),(date))
        futures.append(future)

    yo = []
    # get results from parallel calls
    for future in futures:
        yo = yo + future.result()

        
    flat_l=[flatten(item) for item in yo]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()

    df.rename(columns=rename_map,
          inplace=True)
    # df.to_csv("csv/timeentries.csv")

    #   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)
    try:
        with engine.begin() as connection:
            result = connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)
# 11 m 30 s
# 5 m 


def project_timesheet_entries(token,engine,date_range,proj_df,target_schema_name,target_table_name,apiurl):
    id_list = proj_df['id'].unique().tolist()
    urls = []
    apiurl = apiurl+'?pageSize=200&from={startdate}&to={enddate}'
    for j in id_list:
        for i in date_range:
            # urls.append([f"https://sedin.keka.com/api/v1/psa/projects/{j}/timeentries?pageSize=200&from={i[0]}&to={i[1]}",[i[0],i[1]]])
            urls.append([apiurl.format(projectId= j,startdate = i[0],enddate = i[1]),[i[0],i[1]]])

    executor = ThreadPoolExecutor(max_workers=12)
    futures = []
    for url,date in urls:
        future = executor.submit(call_api,(token),(url),(date))
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
    # df.to_csv(f"csv/project_timesheet_entries.csv")
    try:
        with engine.begin() as connection:
            result = connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)



def task_time_entries(token,engine,date_range,tasks_df,target_schema_name,target_table_name,apiurl):
    data_dict = tasks_df[['projectid', 'id']].values.tolist()
    urls = []
    apiurl = apiurl+'?pageSize=200&from={startdate}&to={enddate}'
    for dates in date_range:
        for itera in data_dict:
            # urls.append([f"https://sedin.keka.com/api/v1/psa/projects/{itera[0]}/tasks/{itera[1]}/timeentries?pageSize=200&from={dates[0]}&to={dates[1]}",dates])
            urls.append([apiurl.format(projectId=itera[0],taskId=itera[1],startdate=dates[0],enddate=dates[1]),dates])
    executor = ThreadPoolExecutor(max_workers=12)
    futures = []
    for url,date in urls:
        future = executor.submit(call_api,(token),(url),(date))
        futures.append(future)

    yo = []
    for future in futures:
        yo = yo + future.result()
        
    flat_l=[flatten(item) for item in yo]
    df=pd.DataFrame(flat_l)
    try:
        df = df.drop(['Unnamed: 0'],axis=1)
    except Exception as e:
        print(e)
        
    rename_map = {}
    for api in df.columns:
        rename_map[api] = api.lower()
    df.rename(columns=rename_map,
          inplace=True)
    
    #   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)
    # df.to_csv(f"csv/task_time_entries.csv")
    # return df
    try:
        with engine.begin() as connection:
            result = connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)
