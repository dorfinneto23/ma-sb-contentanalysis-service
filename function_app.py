import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 

#Azure Blob Storage connection string
connection_string_blob = os.environ.get('BlobStorageConnString')

#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

#OpenAI Details 
client = AzureOpenAI(
  api_key = os.environ.get('AzureOpenAI_pi_key'),
  api_version = "2024-02-01",
  azure_endpoint = "https://openaisponsorship.openai.azure.com/"
)

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'

#Openai function - content analysis
def openai_content_analysis(path, caseid):
    try:
        logging.info(f"openai_content_analysis function strating")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read()
        logging.info(f"data from the txt file is {filecontent}")
        #preparing data for response 
        data = { 
            "status" : "success", 
            "Description" : f"content analysis sucess"
        } 
        json_data = json.dumps(data)
        return json_data
    except Exception as e:
        #preparing data for response 
        data = { 
            "status" : "error", 
            "Description" : f"{str(e)}"
        } 
        json_data = json.dumps(data)
        return json_data

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="contentanalysis",
                               connection="medicalanalysis_SERVICEBUS") 
def sbcontentanalysisservice(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info(f"Received messageesds: {message_data}")
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    doc_id = message_data_dict['doc_id']
    path = message_data_dict['path']
    url = message_data_dict['url']
    filename = message_data_dict['filename']
    openai_result = openai_content_analysis(path,caseid)
    logging.info(f"Received messageesds: {openai_result}")



