# Import necessary libraries and modules
from datetime import datetime,timedelta
import requests
import yaml
from sqlalchemy import text
import pandas as pd

# Define a function to generate a date range based on load type and days to load
def date_gen(loadtype,daystoload,etl_last_run):
    date_range = []
    now = datetime.now()

    if loadtype.lower() == 'full':
        start_date = end_date = datetime(2023,4,1)
    elif loadtype.lower() == 'incremental':
        start_date = now - timedelta(days = daystoload)
        start_date = end_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    elif loadtype.lower() == 'last_run':
        start_date = end_date = datetime.strptime(etl_last_run, "%Y-%m-%d %H:%M:%S.%f")

    else:
        raise Exception(f"Invalid Loadtype. Value should be either Full or Incremental or last_run.")

    while end_date<now:
        end_date = end_date + timedelta(days = 15)
        if end_date >= now:
            end_date = now
        date_range.append([start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),end_date.strftime("%Y-%m-%dT%H:%M:%SZ")])
        start_date = end_date

    return date_range

# Define a function to flatten nested dictionaries or lists
def flatten(dictOrList, parent_key='', sep='_'):
    items = []
    for key, value in dictOrList.items():
        new_key = parent_key + sep + str(key) if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            items.extend(flatten(dict(enumerate(value)), new_key, sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

# Define a function to generate API tokens
def gen_api_token():
    stream = open("config.yaml","r")
    config = yaml.safe_load(stream)
    api_key = {
    "timesheet":config['api']['timesheet'],
    "employees":config['api']['employees']
    }

    token = {}
    url = "https://login.keka.com/connect/token"
    headers = {
        'accept': 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
        'User-Agent': 'sedin',
    }

    for type in api_key:
        payload = {
            'grant_type': config['client']['grant_type'],
            'scope':  config['client']['scope'],
            'client_id':  config['client']['client_id'],
            'client_secret':  config['client']['client_secret'],
            'api_key': api_key[type],
        }
        
        response = requests.request("POST", url, data=payload, headers=headers)
        if not response.ok:
            print(f"\n{response.json()}\n")
            raise Exception("Error, cannot connect to the keka server. Check response above.")
        # print(api_key[api])
        token[type] = response.json()['access_token']

    return token

# Define a function to make API calls
def call_api(token,url,dates):
    headers = {
            "accept": "application/json",
            "authorization": f"Bearer {token['timesheet']}"
        }
    l=[]
    try:
        flag = 1
        while flag is not None:
            response = requests.get(url, headers=headers)
            jsondata=response.json()
            l+=jsondata['data']
            flag=jsondata['nextPage']
            url=f"{flag}&from={dates[0]}&to={dates[1]}"
            print(f'{jsondata["pageNumber"]}/{jsondata["totalPages"]}')
    except Exception as e:
        print(e)
    return l

# Define a function to add new columns to a database table if api and sql columns do not match
def add_new_column(engine,df,target_schema_name,target_table_name):
    df_columns = df.columns.tolist()

    with engine.connect() as connection:
        result = connection.execute(text(f"select COLUMN_NAME from information_schema.columns where table_schema = '{target_schema_name}' and table_name='{target_table_name}'"))

    sql_columns = []

    # Iterate through the result rows and extract the values from the "COLUMN_NAME" column
    for row in result:
        sql_columns.append(row[0])

    if sql_columns != df_columns:
        difference = [item for item in df_columns if item not in sql_columns]

        types_dict = {
            'object': 'TEXT COLLATE public.nocase',
            'float64': 'TIMESTAMP',
            'int32' : 'INTEGER',
            'int64' : 'INTEGER',
            'float64': 'REAL',
            'bool': 'BOOLEAN',
            'datetime64': 'TIMESTAMP',
            'timedelta64': 'INTERVAL'
        }

        queries = []
        for extra_column in difference:
            dtype = df[extra_column].dtype
            query = f"ALTER TABLE {target_schema_name}.{target_table_name} ADD COLUMN {extra_column} {types_dict.get(dtype.name)};"
            print(f"Extra columns in source. Adding column {extra_column}")
            queries.append(query)

        with engine.begin() as connection:
            for query in queries:
                result = connection.execute(text(query))

        with engine.connect() as connection:
            result = connection.execute(text(f"select COLUMN_NAME from information_schema.columns where table_schema = '{target_schema_name}' and table_name='{target_table_name}'"))

        sql_columns = []

        # Iterate through the result rows and extract the values from the "COLUMN_NAME" column
        for row in result:
            sql_columns.append(row[0])

    return sql_columns