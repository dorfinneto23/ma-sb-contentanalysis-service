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


# Generic Function to update documents  in the 'documents' table
def update_documents_generic(doc_id,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE documents SET {field} = ? WHERE id = ?", (value, doc_id))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"document id:  {doc_id} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False  

#save openai content response 
def save_openai_response(content,caseid,filename):
    try:
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        destinationPath = f"{basicPath}/openai/{filename}"
        blob_client = container_client.upload_blob(name=destinationPath, data=content)
        logging.info(f"the openai content file url is: {blob_client.url}")
    
    except Exception as e:
        print("An error occurred:", str(e))



#Openai function - content analysis
def openai_content_analysis(path, caseid):
    try:
        logging.info(f"openai_content_analysis function strating")
        container_name = "medicalanalysis"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(path)
        download_stream = blob_client.download_blob()
        filecontent  = download_stream.read().decode('utf-8')
        logging.info(f"data from the txt file is {filecontent}")
        mission = f"The mission based on the uploaded file is as follows:\n{filecontent}\nPlease provide insights based on this information."
        #chat request for content analysis 
        response = client.chat.completions.create(
                    model="proofitGPT4", # model = "deployment_name".
                    response_format={ "type": "json_object" },
                    messages=[
                        {"role": "system", "content": mission},
                        {"role": "user", "content": """**Task Summary:**
                            Your task is to review a set of files to identify and document medical information. You need to extract:
                            - Diagnosis summaries
                            - Severity, stage, or level of each diagnosis, if available
                            - Diagnosis dates, if mentioned
                            - Prescribed or recommended treatments
                            **Step-by-Step Guide:**
                            1. **Review Files:** Carefully examine all provided files to find any medical diagnoses.
                            2. **Extract Information:** Record the following for each diagnosis:
                            - Diagnosis name.
                            - Severity, stage, or level (if specified).
                            - Date of diagnosis (if mentioned).
                            - Recommended or current treatments.
                            3. **Categorize Diagnoses:** Group the extracted diagnoses into the following clinical areas:
                            - Blood_and_Coagulation
                            - Cardiovascular
                            - Diabetes 
                            - Neurology
                            - Skin_and_Scars
                            - Endocrinology_excluding_Diabetes
                            - Gastroenterology
                            - Ears
                            - Lungs_excluding_Asthma
                            - Asthma
                            - Eyes
                            - Visual_Impairment
                            - Oral_and_Maxillofacial
                            - Psychiatry_and_ADHD
                            - Urogenital
                            - Orthopedics_and_Trauma
                            - Nose_Mouth_and_Throat
                            - Organ_Transplantation
                            4. **Prepare JSON Output:** Organize the collected information into a JSON format with the following structure:
                            - File number
                            - Diagnosis details, including name, diagnosis date, severity/level/stage, and treatment
                            - Associated clinical area
                            **Example JSON Structure:**
                            {
                            "FileNumber": "123456",
                            "Diagnoses": [
                                {
                                "Diagnosis": "Diabetes",
                                "DateOfDiagnosis": "1998",
                                "LevelStageSeverity": "episode of metabolic acidosis",
                                "Treatment": "insulin dependent",
                                "ClinicalArea": "Diabetes"
                                }
                            ]
                            }
                            """}
                    ]
                )
        logging.info(f"Response from openai: {response.choices[0].message.content}")
        #preparing data for response 
        data = { 
            "status" : "success", 
            "response" : response.choices[0].message.content,
            "Description" : f"content analysis sucess"
        } 
        json_data = json.dumps(data)
        return json_data
    except Exception as e:
        #preparing data for response 
        data = { 
            "status" : "error", 
            "response" : "Not respose",
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
    openai_result_dict = json.loads(openai_result) 
    if openai_result_dict['status']=="success":
        openai_content = openai_result_dict['status']
        logging.info(f"openai_content: {openai_content}")
        save_openai_response(openai_content,caseid,filename)
    else: 
        logging.info(f"openai not content response - error, {openai_result}")
    



