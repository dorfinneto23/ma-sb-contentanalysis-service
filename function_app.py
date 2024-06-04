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
import csv #helping convert json to csv
import time # in order to wait one minute on check_openai_available_resurces function 

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

openai_model = "ProofitGPT4o"

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'



# Reset tokens and requests usage 
def reset_tokens_requests_usage(table_name, partition_key, row_key):

    try:
        logging.info(f"reset_tokens_requests_usage start :table_name:{table_name},partition_key:{partition_key}")
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Assign updated values back to the entity
        entity["currentlyTokens"] = 0
        entity["currentlyRequests"] = 0

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"reset_tokens_requests_usage:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"update_openaiRequestsMng :An error occurred: {e}")



#check openai tokens and requtsts usage , if exceed limitation waiting one minute and reset values 
def check_openai_available_resurces(table_name, partition_key, row_key,contentTokens):
    try:
        logging.info(f"start running get_openai_tokens_usage")
        # Create a TableServiceClient using the connection string
        service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient for the specified table
        table_client = service_client.get_table_client(table_name=table_name)

        # Retrieve the entity using PartitionKey and RowKey
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)

        # Return the values
        currentlyTokens = entity.get('currentlyTokens')
        currentlyRequests = entity.get('currentlyRequests')
        requestsPerMinute = entity.get('requestsPerMinute')
        tokensPerMinute = entity.get('tokensPerMinute')
        totalTokens= currentlyTokens+contentTokens
        totalRequests = currentlyRequests+1
        logging.info(f"get_openai_tokens_usage:currentlyTokens: {currentlyTokens},currentlyRequests: {currentlyRequests},requestsPerMinute: {requestsPerMinute},tokensPerMinute: {tokensPerMinute}")
        if (totalTokens>tokensPerMinute or totalTokens == tokensPerMinute):
            logging.info(f"waiting one minute - total tokens exceed the maximum limitation of openai,totalTokens:{totalTokens},maximum:{tokensPerMinute} ")
            # Wait for 60 seconds
            time.sleep(60)
            reset_tokens_requests_usage(table_name, partition_key, row_key)
            return True
        elif (totalRequests>requestsPerMinute or totalRequests == requestsPerMinute):
            logging.info(f"waiting one minute - total Requests  exceed the maximum limitation of openai,totalRequests:{totalRequests},maximum:{requestsPerMinute} ")
            # Wait for 60 seconds
            time.sleep(60)
            reset_tokens_requests_usage(table_name, partition_key, row_key)
            return True
        else:
            logging.info(f"The process can continue; no potential token limitation exceeded.")
            return True
         
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


# Update tokens and request number on openaiRequestsMng
def update_openaiRequestsMng(table_name, partition_key, row_key, pageTokens):

    try:
        logging.info(f"update_openaiRequestsMng start :table_name:{table_name},partition_key:{partition_key},pageTokens:{pageTokens}")
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)

        # Get a TableClient
        table_client = table_service_client.get_table_client(table_name)

        # Retrieve the entity
        entity = table_client.get_entity(partition_key, row_key)

        # Extract values from EntityProperty and ensure they are integers
        currently_tokens = entity["currentlyTokens"]
        currently_requests = entity["currentlyRequests"]
        logging.info(f"update_openaiRequestsMng values from table:{table_name}:currently_tokens:{currently_tokens},currently_requests:{currently_requests}")
        # Update the fields
        currently_tokens += pageTokens
        currently_requests += 1

        # Assign updated values back to the entity
        entity["currentlyTokens"] = currently_tokens
        entity["currentlyRequests"] = currently_requests

        # Update the entity in the table
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        logging.info(f"update_openaiRequestsMng:Entity updated successfully.")

    except ResourceNotFoundError:
        logging.info(f"The entity with PartitionKey '{partition_key}' and RowKey '{row_key}' was not found.")
    except Exception as e:
        logging.info(f"update_openaiRequestsMng :An error occurred: {e}")



#get valid clinic areas from assistants table on azure storage 
def get_filtered_partition_keys_from_azure_table(table_name, row_key, approved_value):
    try:
        logging.info(f"get_filtered_partition_keys_from_azure_table:table_name:{table_name},row_key:{row_key},approved_value:{row_key}")
        # Create a TableServiceClient using the connection string
        table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
        
        # Get a reference to the table
        table_client = table_service_client.get_table_client(table_name=table_name)
        
  
        filter_query = f"RowKey eq '{row_key}' and approved eq '{approved_value}'"
        logging.info(f"get_filtered_partition_keys_from_azure_table: filter_query:{filter_query}")
        # Query the table for entities matching the filter
        entities = table_client.query_entities(query_filter=filter_query)
        logging.info(f"get_filtered_partition_keys_from_azure_table,entities:{entities}")
        # Extract all unique PartitionKey values
        partition_keys = set()
        for entity in entities:
            logging.info(f"get_filtered_partition_keys_from_azure_table:entity PartitionKey: {entity['PartitionKey']}")
            partition_keys.add(entity['PartitionKey'])
        
        return list(partition_keys)
    
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        return []


#  Function check how many rows in partition of azure storage table
def count_rows_in_partition( table_name,partition_key):
    # Create a TableServiceClient object using the connection string
    service_client = TableServiceClient.from_connection_string(conn_str=connection_string_blob)
    
    # Get the table client
    table_client = service_client.get_table_client(table_name=table_name)
    
    # Define the filter query to count entities with the specified partition key and where contentAnalysisCsv is not null or empty
    filter_query = f"PartitionKey eq '{partition_key}' and contentAnalysisCsv ne ''"
    
    # Query the entities and count the number of entities
    entities = table_client.query_entities(query_filter=filter_query)
    count = sum(1 for _ in entities)  # Sum up the entities
    
    if count>0:
        return count
    else:
        return 0


 #Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

#conver json to csv 
def json_to_csv(json_string):
   # Parse the JSON string into a Python dictionary
    data = json.loads(json_string)
    
    # Create an in-memory string buffer
    output = io.StringIO()
    
    # Create a CSV writer object using the string buffer
    writer = csv.writer(output)
    
    # Write the header row
    header = ["filenumber", "diagnosis", "dateofdiagnosis", "levelstageseverity", "treatment", "clinicalarea"]
    writer.writerow(header)
    
    # Extract the file number
    file_number = data.get("filenumber")
    
    #get valid clinic areas from assistants table on azure storage 
    clinicAreasList  = get_filtered_partition_keys_from_azure_table("assistants","1","1")
    logging.info(f"clinicAreasList:{clinicAreasList}")
    # if not found clinic area , put "Not Specified"
    for diagnosis in data.get("diagnoses", []):
        clinical_area = diagnosis.get("clinicalarea", "Not Specified")
        logging.info(f"diagnosis for loop clinical_area value :{clinical_area}")
        if clinical_area not in clinicAreasList:
            clinical_area = "Not Specified"
            logging.info(f"clinical_area value :{clinical_area}, not in the list")

        row = [
            file_number,
            diagnosis.get("diagnosis", "Not Specified"),
            diagnosis.get("dateofdiagnosis", "Not Specified"),
            diagnosis.get("levelstageseverity", "Not Specified"),
            diagnosis.get("treatment", "Not Specified"),
            clinical_area
        ]
        writer.writerow(row)
    
    # Get the CSV content as a string
    csv_string = output.getvalue()
    
    # Close the StringIO object
    output.close()
    
    return csv_string

# Update field on specific entity/ row in storage table 
def update_documents_entity_field(table_name, partition_key, row_key, field_name, new_value,field_name2,new_value2,field_name3,new_value3,field_name4,new_value4):

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
def update_case_generic(caseid,field,value,field2,value2):
    try:
        # Establish a connection to the Azure SQL database
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Insert new case data into the 'cases' table
        cursor.execute(f"UPDATE cases SET {field} = ? ,{field2} = ?  WHERE id = ?", (value,value2, caseid))
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
                return None
            elif isinstance(obj, dict):
                return {key.strip() if isinstance(key, str) else key: remove_spaces(val) if isinstance(val, (dict, list, str)) else val for key, val in obj.items()}
            elif isinstance(obj, list):
                return [remove_spaces(item) for item in obj]
            elif isinstance(obj, str):
                return obj.strip()
            else:
                return obj

        # Clean the data dictionary
        cleaned_data = remove_spaces(data)
        if cleaned_data is None:
            return json_string

        # Convert the cleaned dictionary back to JSON string
        cleaned_json_string = json.dumps(cleaned_data, indent=4)
        return cleaned_json_string

    except Exception as e:
        return json_string

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
                    model=openai_model,
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
            "response" : response.choices[0].message.content.lower(),
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
    pageTokens = int(message_data_dict['pageTokens'])
    filename = message_data_dict['filename']
    availableResurces = check_openai_available_resurces("openaiRequestsMng", openai_model, "1",pageTokens)
    logging.info(f"availableResurces: {availableResurces}")
    update_openaiRequestsMng("openaiRequestsMng",openai_model,"1",pageTokens)
    openai_result = openai_content_analysis(path,caseid)
    openai_result_dict = json.loads(openai_result) 
    if openai_result_dict['status']=="success":
        openai_content = openai_result_dict['response']
        logging.info(f"openai_content: {openai_content}")
        openai_content_cleaned = clean_json(openai_content)
        save_openai_response(openai_content_cleaned,caseid,filename)
        clinicData = json.loads(openai_content_cleaned)
        # Extract unique ClinicalArea values
        clinical_areas = set(diagnosis["clinicalarea"] for diagnosis in clinicData["diagnoses"])
        # Concatenate unique ClinicalArea values into a single string
        clinical_areas_concatenated = ';'.join(clinical_areas)
        logging.info(f"clinical_areas_concatenated: {clinical_areas_concatenated}")
        content_csv = json_to_csv(openai_content_cleaned)
        # Encode the CSV string to preserve newlines
        encoded_content_csv = content_csv.replace('\n', '\\n')
        # Decode the CSV string
        #retrieved_csv = encoded_content_csv.replace('\\n', '\n') -- for testing retrieving csv
        update_documents_entity_field("documents", caseid, doc_id, "contentAnalysisJson", openai_content_cleaned,"clinicAreas",clinical_areas_concatenated,"status",4,"contentAnalysisCsv",encoded_content_csv)
        #preparing data for service bus
        data = { 
                "doc_id" : doc_id, 
                "storageTable" :"documents",
                "caseid" :caseid,
                "pagenumber" :pagenumber,
                "totalpages" :totalpages
            } 
        json_data = json.dumps(data)
        create_servicebus_event("clinicareasconsolidation",json_data)
        logging.info(f"service bus event sent")
        pages_done = count_rows_in_partition("documents",caseid) # check how many pages proccess done 
        if pages_done==totalpages: #check if the last file passed 
            update_case_generic(caseid,"status",7,"contentAnalysis",1) #update case status to 7 "content analysis done"
            logging.info(f"content analysis process - done")
        else:
            logging.info(f"content analysis on {pagenumber} out of {totalpages} - done")

    else: 
        logging.info(f"openai not content response - error message, {openai_result}")
    



