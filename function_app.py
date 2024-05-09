import azure.functions as func
import logging

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="contentanalysis",
                               connection="medicalanalysis_SERVICEBUS") 
def sbcontentanalysisservice(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBus Queue trigger processed a messagee: %s',
                azservicebus.get_body().decode('utf-8'))
