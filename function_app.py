'''
            FUNCTION_APP.PY                     
Python file Azure Function defined for task 1.

Name of Student: Vindhyaa Saravanan
Module: Distributed Systems CWK 2
Student ID: 201542641
Username: sc21vs
'''
# Importing required libraries
import azure.functions as func
from azure.functions.decorators.core import DataType
import logging
import datetime
from random import uniform
from time import time

# Creating instance of the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Registering the output binding to the Azure SQL database table dbo.SensorData
# (refer sql connection string in local.settings.json)
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
@app.generic_output_binding(arg_name="toAddSensorData", type="sql", CommandText="dbo.SensorData", ConnectionStringSetting="SqlConnectionString", data_type=DataType.STRING)

# Function and route for Task 1
@app.function_name(name="task1_datafunction_httptrigger")
@app.route(route="task1_datafunction_httptrigger")
def task1_datafunction_httptrigger(req: func.HttpRequest, toAddSensorData: func.Out[func.SqlRowList]) -> func.HttpResponse:
    
    logging.info('Starting HTTP Trigger Data Function...')

    # Try to get 
    number_of_records = req.params.get('number_of_records')
    if not number_of_records:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            number_of_records = req_body.get('number_of_records')

    if number_of_records:
        # Log details of request
        logging.info('{number_of_records} records requested per sensor...')
        
        # Run sensor simulation and add the new data to the database, checking time before and after
        logging.info('Starting sensor simulation...')
        start_time = time()
        toAddSensorData.set(run_sensor_simulation(int(number_of_records)))
        end_time = time()
        
        # Calculating response time taken
        time_taken = end_time - start_time  
        
        # Log info about successful function call and return HTTP response
        logging.info('20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.')
        return func.HttpResponse(f"Hello, 20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.")
    
    else:
        # Return Bad Request (Code 400) and explain need for 'number_of_records' parameter
        logging.info('Bad Request - Required parameters not passed.')
        return func.HttpResponse(
        "This HTTP triggered function executed successfully. Pass the 'number_of_records' parameter in the query string or in the request body for a personalized response.",
        status_code=400
        )

# Defining an object class that emulates a Sensor's behaviour
class Sensor:
    # Method to initialize a sensor object
    def __init__(self, sensor_id):
        self.TEMP_RANGE = (8, 12)
        self.WIND_RANGE = (15, 25)
        self.HUMID_RANGE = (40, 70)
        self.CO2_RANGE = (500, 1500)
        self.sensor_id = sensor_id

    # Method for sensor to produce a number_of_records/rows of the database
    def simulate_sensor_reply(self, number_of_records: int) -> func.SqlRowList:
        return [self.generate_single_record() for _ in range(number_of_records)]

    # Method to generate values for a single record/row of the database
    def generate_single_record(self) -> func.SqlRow:  
        return func.SqlRow(
        SensorId=self.sensor_id,
        Timestamp=str(datetime.datetime.utcnow()),
        Temperature=uniform(*self.TEMP_RANGE),
        WindSpeed=uniform(*self.WIND_RANGE),
        RelativeHumidity=uniform(*self.HUMID_RANGE),
        CO2=uniform(*self.CO2_RANGE)
    )

# Function to run 20 sensors and combine the data received from them
def run_sensor_simulation(number_of_records):
    sensors = [Sensor(sensor_id=i + 1) for i in range(20)]
    result = []
    for sensor in sensors:
        result.extend(sensor.simulate_sensor_reply(number_of_records))
    return result
