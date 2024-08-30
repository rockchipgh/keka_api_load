# Import necessary libraries and modules
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from common import date_gen,gen_api_token
from misc_apis import tasks,phases
from no_input_apis import employees,groups,grouptypes,clients,projects
from time_entry_apis import time_entries, task_time_entries, project_timesheet_entries
from handler import setup_logger,clean_dump
import yaml
import warnings
import time
import os
import logging

# Create a directory 'csv' if it doesn't already exist (try-except block)
# try:
#     os.mkdir("csv")
# except:
#     pass

# Record the start time for measuring execution time
start_time = time.time()

# Open the 'config.yaml' file for reading configuration settings
stream = open("config.yaml", 'r')
config = yaml.safe_load(stream)

# Generate an API token
token = gen_api_token()

# Clean any previous data dumps
clean_dump()

# Set up a logger for SQLAlchemy and store the logs in 'sql.log'
sql = setup_logger('sqlalchemy.engine', 'sql.log')
all_errors = setup_logger(None, 'complete.log', logging.DEBUG)


# Define a custom warning handler function
def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
    with open("warnings.log", "a") as warning_file:
        warning_file.write(f"{category.__name__}: {message} (File: {filename}, Line: {lineno})\n")

# Set the custom warning handler to capture and log warnings
warnings.showwarning = custom_warning_handler

# Choose the database location ('conn' or 'localconn')
conn_type = 'conn'

# Create a SQLAlchemy engine to connect to the PostgreSQL database
engine = create_engine("postgresql://{user}:{password}@{ip}:{port}/{db}".format(
    user=config[conn_type]['user'],
    password=config[conn_type]['pw'],
    ip=config[conn_type]['ip'],
    port=config[conn_type]['port'],
    db=config[conn_type]['db']
))

# Define dictionaries of functions that correspond to different data loading tasks
fun_set_1 = {
    'clients':clients,
    'projects':projects,
    'employees':employees,
    'groups':groups,
    'grouptypes':grouptypes
}

fun_set_2 = {
    'phases':phases,
    'tasks':tasks
}

fun_set_3 = {
    'task_time_entries':task_time_entries
}

# Create placeholders for DataFrames to store project and task data
proj_df = tasks_df = None

# Define a function to control data loading for different tables
def run_control(table,loadtype,daystoload,target_schema_name,target_table_name,apiurl,etl_last_run):
    print(table)
    global proj_df
    global tasks_df
    date_range = date_gen(loadtype, daystoload, str(etl_last_run))


    if table in fun_set_1:
        if table =='projects':
            proj_df = fun_set_1[table](token,engine,target_schema_name,target_table_name,apiurl)
        else:
            fun_set_1[table](token,engine,target_schema_name,target_table_name,apiurl)

    elif table in fun_set_2:
        try:
            if proj_df == None:
                proj_df = fun_set_1['projects'](token,engine,'stg','stg_projects','https://sedin.keka.com/api/v1/psa/projects')
        except ValueError:
            pass
        if table =='tasks':
            tasks_df = fun_set_2[table](token,engine,proj_df,target_schema_name,target_table_name,apiurl)
        else:
            fun_set_2[table](token,engine,proj_df,target_schema_name,target_table_name,apiurl)

    elif table in fun_set_3:
        
        try:
            if tasks_df == None:
                if proj_df == None:
                    proj_df = fun_set_1['projects'](token,engine,'stg','stg_projects','https://sedin.keka.com/api/v1/psa/projects')
                tasks_df = fun_set_2['tasks'](token,engine,proj_df,'stg','stg_tasks','https://sedin.keka.com/api/v1/psa/projects/{projectId}/tasks')
        except ValueError:
            pass
        try:
            if proj_df == None:
                proj_df = fun_set_1['projects'](token,engine,'stg','stg_projects','https://sedin.keka.com/api/v1/psa/projects')
        except ValueError:
            pass

        fun_set_3[table](token,engine,date_range,tasks_df,target_schema_name,target_table_name,apiurl)
    else:
        raise("Table has no appropriate function in code")
    # if table == 'clients':
    #     clients(token,engine,target_schema_name,target_table_name,apiurl)
    # elif table == 'projects':
    #     proj_df = projects(token,engine,target_schema_name,target_table_name,apiurl)
    # elif table == 'employees':
    #     employees(token,engine,target_schema_name,target_table_name,apiurl)
    # elif table == 'groups':
    #     groups(token,engine,target_schema_name,target_table_name,apiurl)
    # elif table == 'grouptypes':
    #     grouptypes(token,engine,target_schema_name,target_table_name,apiurl)
    # elif table == 'phases':
    #     phases(token,engine,proj_df,target_schema_name,target_table_name,apiurl)
    # elif table == 'tasks':
    #     tasks_df = tasks(token,engine,proj_df,target_schema_name,target_table_name,apiurl)
    # elif table == 'task_time_entries':
    #     task_time_entries(token,engine,date_range,tasks_df,target_schema_name,target_table_name,apiurl)


    ## NOT USED
    # elif table == 'time_entries':
    # time_entries(token,engine,date_range,target_schema_name,target_table_name,apiurl)
    # elif table == 'project_timesheet_entries':
    # ## NOT USED ANYMORE
    # project_timesheet_entries(token,engine,date_range,proj_df,target_schema_name,target_table_name,apiurl)


# Read the control table data into a Pandas DataFrame

try:
    controltbl = pd.read_sql_table(con=engine, table_name=config['controltbl']['name'],schema=config['controltbl']['schema'])
except Exception as e:
    raise ConnectionError("Cannot connect to database. Check connection to the database.")

# Filter rows where 'dataflowflag' is 'SrcToStg' and sort by 'priorityorder'
controltbl=controltbl[controltbl['dataflowflag']=='SrcToStg']
controltbl = controltbl.sort_values(by=['priorityorder'])


# Loop through each row in the control table
# Call the 'run_control' function to load data for the specified table
for ind in controltbl.index:
    if controltbl['isapplicable'][ind]:
        run_control(table=controltbl['sourceid'][ind],loadtype=controltbl['loadtype'][ind],
                    daystoload=int(controltbl['daystoload'][ind]),target_schema_name=controltbl['targetschemaname'][ind],
                    target_table_name=controltbl['targetobject'][ind],apiurl=controltbl['apiurl'][ind],etl_last_run=controltbl['etllastrundate'][ind]
                    )
        controltbl['latestbatchid'][ind]+=1
        curr_time = datetime.now()
        with engine.begin() as connection:
            result = connection.execute(text(
                f"UPDATE {config['controltbl']['schema']}.{config['controltbl']['name']} SET etllastrundate = '{curr_time}' where id = {controltbl['id'][ind]};"))
            result = connection.execute(text(f"UPDATE {config['controltbl']['schema']}.{config['controltbl']['name']} SET latestbatchid = '{(controltbl['latestbatchid'][ind])+1}' where id = {controltbl['id'][ind]};"
                ))
        controltbl['etllastrundate'][ind] = datetime.now()
    
# Record the end time and calculate the elapsed time for execution and print it
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Time taken for execution: {elapsed_time} seconds")
result = connection.execute(text(
                f"UPDATE {config['controltbl']['schema']}.{config['controltbl']['name']} SET etllastrundate = '{curr_time}' where id = {controltbl['id'][ind]};"))
result = connection.execute(text(f"UPDATE {config['controltbl']['schema']}.{config['controltbl']['name']} SET latestbatchid = '{(controltbl['latestbatchid'][ind])+1}' where id = {controltbl['id'][ind]};"
                ))