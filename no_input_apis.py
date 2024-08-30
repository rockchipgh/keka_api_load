# Import the necessary libraries and modules
import requests
import pandas as pd
from common import flatten,add_new_column
from sqlalchemy import text

# Define a function to fetch data for 'clients' and load it into a database table
def clients(token,engine,target_schema_name,target_table_name,apiurl):
    # Set the API URL to fetch data for 'clients' with a page size of 200 (max = 200)
    url = apiurl+"?pageSize=200"
    # url = "https://sedin.keka.com/api/v1/psa/clients?pageSize=200"
    # Define the headers for the HTTP request
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token['timesheet']}",
        'User-Agent': 'GodMode',
    }
    # Initialize an empty list to store the retrieved data
    l = []
    try:
        # Continue fetching data while the 'url' is not None (nextPage is none at lastpage)
        while url is not None:
            # Send an HTTP GET request to the specified 'url' with the defined headers
            response = requests.get(url, headers=headers)
            # Parse the JSON response from the API
            jsondata=response.json()
            # Append the data from the 'data' field in the JSON response to the 'l' list
            l += jsondata['data']
            # Update the 'url' to the next page URL in the response
            url=jsondata['nextPage']
            # Print the progress by showing the current page number and the total number of pages
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        # Handle exceptions if they occur during the API request
        print(e)

    # Transform the retrieved data into a list of flattened dictionaries
    flat_l=[flatten(item) for item in l]
    df=pd.DataFrame(flat_l)

    # Rename the DataFrame columns using the 'rename_map' (due to postgres collate constraints)
    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()
    df.rename(columns=rename_map,
          inplace=True)
    
    # Add new columns to the database table if they are not already present in sql
    add_new_column(engine,df,target_schema_name,target_table_name)

    # df.to_csv(f"csv/clients.csv")
    # return df
    try:
        with engine.begin() as connection:
            connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    # Insert the data from the DataFrame into the database table
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)


def projects(token,engine,target_schema_name,target_table_name,apiurl):
    url =apiurl+"?pageSize=200"

    # url = "https://sedin.keka.com/api/v1/psa/projects?pageSize=200"
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
            url=jsondata['nextPage']
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)

    flat_l=[flatten(item) for item in l]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower().replace(" -","_").replace(" ","_")
    df.rename(columns=rename_map,
          inplace=True)
    # df.to_csv(f"csv/projects.csv")
    try:
        with engine.begin() as connection:
            connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)

    # Filter out data if status is not 0
    df = df[df['status']==0]
    return df

def employees(token,engine,target_schema_name,target_table_name,apiurl):
    url =apiurl+"?pageSize=200"

    # url = "https://sedin.keka.com/api/v1/hris/employees?pageSize=200"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token['employees']}"
    }
    l=[]
    try:
        a=0
        while url is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            url=jsondata['nextPage']
            a+=1
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)


    for i in range(len(l)):
        flattened_data = {}
        for group in l[i]['groups']:
            group_type = group['groupType']
            group_id = group['id']
            group_title = group['title']

            flattened_data[f'groups_{group_type}_id'] = group_id
            flattened_data[f'groups_{group_type}_title'] = group_title
            flattened_data[f'groups_{group_type}_groupType'] = group_type

        l[i].pop('groups')
        l[i].update(flattened_data)

    flat_l=[flatten(item) for item in l]
    df=pd.DataFrame(flat_l)
    # print(df.head())
    
    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()

    df.rename(columns=rename_map,
        inplace=True)
    
#   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)


    # df.to_csv(r"csv\employees.csv")
    try:
        with engine.begin() as connection:
            connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)


def groups(token,engine,target_schema_name,target_table_name,apiurl):
    url =apiurl+"?pageSize=200"

    # url = "https://sedin.keka.com/api/v1/hris/groups?pageSize=200"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token['employees']}"
    }
    l=[]
    try:
        while url is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            url=jsondata['nextPage']
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)

    flat_l=[flatten(item) for item in l]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()
    df.rename(columns=rename_map,
          inplace=True)
    
    #   Add new column code
    add_new_column(engine,df,target_schema_name,target_table_name)
    # df.to_csv(f"csv/groups.csv")
    try:
        with engine.begin() as connection:
            connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)

def grouptypes(token,engine,target_schema_name,target_table_name,apiurl):
    url =apiurl+"?pageSize=200"

    # url = "https://sedin.keka.com/api/v1/hris/grouptypes?pageSize=200"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token['employees']}"
    }
    l=[]
    try:
        while url is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            url=jsondata['nextPage']
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
            #searches every webpage till link to webpage is not found
    except Exception as e:
        print(e)

    flat_l=[flatten(item) for item in l]
    df=pd.DataFrame(flat_l)

    rename_map = {}
    for item in df.columns:
        rename_map[item] = item.lower()
    df.rename(columns=rename_map,
          inplace=True)
    # df.to_csv(f"csv/grouptypes.csv")
    try:
        with engine.begin() as connection:
            connection.execute(text(f"truncate {target_schema_name}.{target_table_name}"))
    except:
        print("Truncation not done. Most probably due to table doesnt exist")
    df.to_sql(con=engine, name=target_table_name, if_exists='append',index=False,schema=target_schema_name)