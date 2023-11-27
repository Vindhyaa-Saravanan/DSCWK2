'''
            FUNCTION_APP.PY                     
Python file with Azure Functions 
defined for Coursework 2.

Functions for specific tasks are as follows:
# Task 1: task1_datafunction_httptrigger
# Task 2: task2_statfunction_httptrigger
# Task 3: task3_datafunction_timertrigger and task3_statfunction_sqltrigger

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
import json

# Defining the value ranges which are constants
TEMP_RANGE = tuple([8, 15])
WIND_RANGE = tuple([15, 25])
HUMID_RANGE = tuple([40, 70])
CO2_RANGE = tuple([500, 1500])

# Creating instance of the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Registering the output binding to the Azure SQL database table dbo.SensorData
# (refer sql connection string in local.settings.json)
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
# Lines 37 to 52 of the example were referred to, for the output binding and the 
@app.generic_output_binding(
    arg_name="toSendSensorData", 
    type="sql", 
    CommandText="dbo.SensorData", 
    ConnectionStringSetting="SqlConnectionString", 
    data_type=DataType.STRING
)
# FUNCTION AND ROUTE FOR TASK 1
@app.function_name(name="task1_datafunction_httptrigger")
@app.route(route="task1_datafunction_httptrigger")
def task1_datafunction_httptrigger(req: func.HttpRequest, toSendSensorData: func.Out[func.SqlRowList]) -> func.HttpResponse:
    
    logging.info('Starting Data Function...')

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
        toSendSensorData.set(run_sensor_simulation(int(number_of_records)))
        end_time = time()
        
        # Calculating response time taken
        time_taken = end_time - start_time  
        
        # Log info about successful function call and return HTTP response, with success code
        logging.info('20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.')
        return func.HttpResponse(
            f"Hello, 20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.",
            status_code=200)
    
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
        self.sensor_id = sensor_id

    # Method for sensor to produce a number_of_records/rows of the database
    def simulate_sensor_reply(self, number_of_records: int) -> func.SqlRowList:
        return [self.generate_single_record() for _ in range(number_of_records)]

    # Method to generate values for a single record/row of the database
    def generate_single_record(self) -> func.SqlRow:  
        return func.SqlRow(
        SensorId=self.sensor_id,
        Timestamp=str(datetime.datetime.utcnow()),
        Temperature=uniform(*TEMP_RANGE),
        WindSpeed=uniform(*WIND_RANGE),
        RelativeHumidity=uniform(*HUMID_RANGE),
        CO2=uniform(*CO2_RANGE)
    )

# Function to run 20 sensors and combine the data received from them
def run_sensor_simulation(number_of_records):
    sensors = [Sensor(sensor_id=i + 1) for i in range(20)]
    result = []
    for sensor in sensors:
        result.extend(sensor.simulate_sensor_reply(number_of_records))
    return result


# FUNCTION AND ROUTE FOR TASK 2
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
# Lines 13 to 32 of the example were referred to, for the input binding and the function response returning, etc.
@app.generic_input_binding(
    arg_name="toReadSensorData",
    type="sql",
    CommandText="SELECT * FROM SensorData",
    CommandType="Text",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)
# FUNCTION AND ROUTE FOR TASK 2
@app.function_name(name="task2_statfunction_httptrigger")
@app.route(route="task2_statfunction_httptrigger", auth_level=func.AuthLevel.ANONYMOUS)
def task2_statfunction_httptrigger(req: func.HttpRequest, toReadSensorData: func.SqlRowList) -> func.HttpResponse:
    
    logging.info('Starting Statistic Function...')
    
    # Converting each SqlRow in data_from_db to a dictionary,
    # where keys are the DB columns, values are the DB values
    records = [json.loads(row.to_json()) for row in toReadSensorData]
    
    # Creating a new dictionary to store sensor IDs and their corresponding lists of rows
    sorted_per_sensor = {}
    # For each row of data, check if a appropriate list exists for that sensor,
    # if not, add the list at the same index, then append the row to that list
    for record in records:
        sensor_id = record['SensorId']
        if sensor_id not in sorted_per_sensor:
            sorted_per_sensor[sensor_id] = []
        sorted_per_sensor[sensor_id].append(record)
    
    # Create dictionary to store the final result to be sent
    final_result = calculate_stats_for_sensor_sets(sorted_per_sensor)
        
    # Return JSON to be displayed with the statistics
    return func.HttpResponse(
        json.dumps(final_result, indent=2),
        status_code=200,
        mimetype="application/json"
    )
    
# Function to calculate statistics per sensor given dictionary of records sorted by sensor
# Created function to encapsulate this functionality as used frequently
def calculate_stats_for_sensor_sets(sorted_per_sensor):
    # Create dictionary to store the final result to be sent
    final_result = {}
    # For each sensor and its set of data
    for sensor_id, set_of_records in sorted_per_sensor.items():
        all_temperatures = [record['Temperature'] for record in set_of_records]
        all_wind_speeds = [record['WindSpeed'] for record in set_of_records]
        all_humidities = [record['RelativeHumidity'] for record in set_of_records]
        all_co2_levels = [record['CO2'] for record in set_of_records]

        # Prepare statistics for each sensor
        sensor_stats = {
            'SensorId': sensor_id,
            'MeanTemperature': sum(all_temperatures) / len(all_temperatures),
            'MeanWindSpeed': sum(all_wind_speeds) / len(all_wind_speeds),
            'MeanRelativeHumidity': sum(all_humidities) / len(all_humidities),
            'MeanCO2': sum(all_co2_levels) / len(all_co2_levels),
            'MaxTemperature': max(all_temperatures),
            'MaxWindSpeed': max(all_wind_speeds),
            'MaxRelativeHumidity': max(all_humidities),
            'MaxCO2': max(all_co2_levels),
            'MinTemperature': min(all_temperatures),
            'MinWindSpeed': min(all_wind_speeds),
            'MinRelativeHumidity': min(all_humidities),
            'MinCO2': min(all_co2_levels),
        }
        
        # Add to the final dictionary
        final_result[f"Sensor_{sensor_id}"] = sensor_stats
        
    return final_result


# Registering new output binding to the Azure SQL database table dbo.SensorData for Task 3
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
# Lines 37 to 52 of the example were referred to, for the output binding
@app.generic_output_binding(
    arg_name="toSendSensorDataTask3", 
    type="sql", 
    CommandText="dbo.SensorData", 
    ConnectionStringSetting="SqlConnectionString", 
    data_type=DataType.STRING
)
# FUNCTION FOR TASK 3 - DATA FUNCTION
@app.function_name(name="task3_datafunction_timertrigger")
@app.timer_trigger(schedule="0 */5 * * * *", 
                   arg_name="myTimer", 
                   run_on_startup=False,
                   use_monitor=True) 
def task3_datafunction_timertrigger(myTimer: func.TimerRequest, toSendSensorDataTask3: func.Out[func.SqlRowList]) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
        
    # Send exactly one record per sensor to the database
    current_time = datetime.datetime.now()
    required_sensor_simulation = run_sensor_simulation(1)
    
    # New data sent to database using exactly one operation 
    # to ensure sql trigger is triggered only once
    toSendSensorDataTask3.set(required_sensor_simulation)
    logging.info(f'Task 3 Data Function has added records at {current_time}')

    
    
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
# Lines 54 to 62 of the example were referred to, for the generic trigger
# Also, have attempted to ensure trigger only once for addition of all 20 rows using Sql_Trigger_BatchSize
@app.generic_trigger(arg_name="SQLDatabaseTrigger", 
                     type="sqlTrigger",
                     TableName="SensorData",
                     ConnectionStringSetting="SqlConnectionString",
                     data_type=DataType.STRING,
                     route="task3_statfunction_sqltrigger")
# New input binding for Task 3
# Referred to this example: https://github.com/Azure/azure-functions-sql-extension/tree/main/samples/samples-python-v2
# Lines 13 to 32 of the example were referred to, for the input binding
@app.generic_input_binding(
    arg_name="toReadSensorDataTask3",
    type="sql",
    CommandText="SELECT * FROM SensorData",
    CommandType="Text",
    ConnectionStringSetting="SqlConnectionString",
    data_type=DataType.STRING
)


# FUNCTION FOR TASK 3 - STATISTIC FUNCTION
@app.function_name(name="task3_statfunction_sqltrigger")
def task3_statfunction_sqltrigger(SQLDatabaseTrigger, toReadSensorDataTask3: func.SqlRowList) -> None:
    
    # Log that a change has occurred and thus triggered function
    logging.info("Change detected in SensorData table: %s", json.dumps(SQLDatabaseTrigger, indent=2))
    logging.info("Triggering Task 3 Statistics Function...")
        
    # Converting each SqlRow in data_from_db to a dictionary,
    # where keys are the DB columns, values are the DB values
    records = [json.loads(row.to_json()) for row in toReadSensorDataTask3]
        
    # Creating a new dictionary to store sensor IDs and their corresponding lists of rows
    sorted_per_sensor = {}
    # For each row of data, check if a appropriate list exists for that sensor,
    # if not, add the list at the same index, then append the row to that list
    for record in records:
        sensor_id = record['SensorId']
        if sensor_id not in sorted_per_sensor:
            sorted_per_sensor[sensor_id] = []
        sorted_per_sensor[sensor_id].append(record)
        
    # Create dictionary to store the final result to be sent
    final_result = calculate_stats_for_sensor_sets(sorted_per_sensor)
            
    # Log the final statistics
    # Unfortunately this output has to be returned to the log stream as returning
    # a HTTP response is not possible with a SQL-triggered Azure Function
    logging.info("Sensor Data Statistics: %s", json.dumps(final_result, indent=2))
    
    