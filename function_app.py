import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
from openai import AzureOpenAI #for using openai services 
from azure.data.tables import TableServiceClient, TableClient, UpdateMode # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError # in order to use azure storage table  exceptions 
import pandas as pd #helping convert json to csv

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


#conver json to csv 
def json_to_csv(json_data):
    """
    Converts JSON data to a CSV format string.

    Parameters:
    json_data (str or dict): JSON data in string format or as a dictionary.

    Returns:
    str: CSV formatted string.

    Raises:
    ValueError: If the input data is not valid JSON or if the conversion fails.
    """
    try:
        # If json_data is a string, parse it to a dictionary
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
        
        # Create a DataFrame from the JSON data
        df = pd.DataFrame(json_data)
        
        # Create a StringIO buffer
        csv_buffer = io.StringIO()
        
        # Write the DataFrame to the buffer as CSV
        df.to_csv(csv_buffer, index=False)
        
        # Retrieve the CSV string from the buffer
        csv_string = csv_buffer.getvalue()
        
        return csv_string
    
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON data provided.") from e
    except Exception as e:
        raise ValueError("Failed to convert JSON to CSV.") from e


# Update field on specific entity/ row in storage table 
def update_documents_entity_field(table_name, partition_key, row_key, field_name, new_value,field_name2,new_value2,field_name3,new_value3,field_name4,new_value4):
    """
    Updates a specific field of an entity in an Azure Storage Table.

    Parameters:
    - account_name: str, the name of the Azure Storage account
    - account_key: str, the key for the Azure Storage account
    - table_name: str, the name of the table
    - partition_key: str, the PartitionKey of the entity
    - row_key: str, the RowKey of the entity
    - field_name: str, the name of the field to update
    - new_value: the new value to set for the field
    """
    try:
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Update the field
        entity[field_name] = new_value
        entity[field_name2] = new_value2
        entity[field_name3] = new_value3
        entity[field_name4] = new_value4

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_documents_entity_field:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"An error occurred: {e}")

# Generic Function to update case  in the 'cases' table
def update_case_generic(caseid,field,value):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? WHERE id = ?", (value, caseid))
        conn.commit()

        # Close connections
        cursor.close()
        conn.close()
        
        logging.info(f"case {caseid} updated field name: {field} , value: {value}")
        return True
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return False    

# Clean Json string - clean spaces  if cant clean return the same value 
def clean_json(json_string):
    try:
        # Parse the JSON string into a Python dictionary
        data = json.loads(json_string)

        # Function to recursively remove white spaces from dictionary keys and values
        def remove_spaces(obj):
            if obj is None:
                return json_string
            elif isinstance(obj, dict):
                return {key.strip(): remove_spaces(val) if isinstance(val, (dict, list)) else val.strip() for key, val in obj.items()}
            elif isinstance(obj, list):
                return [remove_spaces(item) for item in obj]
            elif isinstance(obj, str):
                return obj.strip()
            else:
                return obj
    except Exception as e:
        return json_string

    # Clean the data dictionary
    cleaned_data = remove_spaces(data)

    # Convert the cleaned dictionary back to JSON string
    cleaned_json_string = json.dumps(cleaned_data, indent=4)

    return cleaned_json_string

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
    pagenumber = message_data_dict['pagenumber']
    totalpages = message_data_dict['totalpages']
    url = message_data_dict['url']
    filename = message_data_dict['filename']
    openai_result = openai_content_analysis(path,caseid)
    openai_result_dict = json.loads(openai_result) 
    if openai_result_dict['status']=="success":
        openai_content = openai_result_dict['response']
        logging.info(f"openai_content: {openai_content}")
        openai_content_cleaned = clean_json(openai_content)
        save_openai_response(openai_content_cleaned,caseid,filename)
        clinicData = json.loads(openai_content_cleaned)
        # Extract unique ClinicalArea values
        clinical_areas = set(diagnosis["ClinicalArea"] for diagnosis in clinicData["Diagnoses"])
        # Concatenate unique ClinicalArea values into a single string
        clinical_areas_concatenated = ';'.join(clinical_areas)
        logging.info(f"clinical_areas_concatenated: {clinical_areas_concatenated}")
        content_csv = json_to_csv(openai_content_cleaned)
        update_documents_entity_field("documents", caseid, doc_id, "contentAnalysisJson", openai_content_cleaned,"clinicAreas",clinical_areas_concatenated,"status",4,"contentAnalysisCsv",content_csv)
        if pagenumber==totalpages: #check if the last file passed 
            update_case_generic(caseid,"status",6) #update case status to 7 "content analysis done"
            logging.info(f"content analysis process - done")
        else:
            logging.info(f"content analysis on {pagenumber} out of {totalpages} - done")

    else: 
        logging.info(f"openai not content response - error message, {openai_result}")
    



