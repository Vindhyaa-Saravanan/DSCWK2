import azure.functions as func
from azure.functions.decorators.core import DataType
import logging
import datetime
from random import uniform
from time import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
@app.generic_output_binding(arg_name="toAddSensorData", type="sql", CommandText="dbo.SensorData", ConnectionStringSetting="SqlConnectionString", data_type=DataType.STRING)

@app.function_name(name="task1_datafunction_httptrigger")
@app.route(route="task1_datafunction_httptrigger")
def task1_datafunction_httptrigger(req: func.HttpRequest, toAddSensorData: func.Out[func.SqlRowList]) -> func.HttpResponse:
    logging.info('Starting HTTP Trigger Data Function...')

    number_of_records = req.params.get('number_of_records')
    if not number_of_records:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            number_of_records = req_body.get('number_of_records')

    if number_of_records:
        start_time = time()  # Record the start time
        rows = run_sensor_generation(int(number_of_records))
        toAddSensorData.set(rows)
        end_time = time()  # Record the end time
        time_taken = end_time - start_time  # Calculate the total time taken
        return func.HttpResponse(f"Hello, 20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass the 'number_of_records' parameter in the query string or in the request body for a personalized response.",
             status_code=400
        )

class Sensor:
    def __init__(self, sensor_id):
        self.TEMP_RANGE = (8, 12)
        self.WIND_RANGE = (15, 25)
        self.HUMID_RANGE = (40, 70)
        self.CO2_RANGE = (500, 1500)
        self.sensor_id = sensor_id

    def simulate_sensor_reply(self, number_of_records: int) -> func.SqlRowList:
        return [self.generate_single_record() for _ in range(number_of_records)]

    def generate_single_record(self) -> func.SqlRow:  
        return func.SqlRow(
        SensorId=self.sensor_id,
        Timestamp=str(datetime.datetime.utcnow()),
        Temperature=uniform(*self.TEMP_RANGE),
        WindSpeed=uniform(*self.WIND_RANGE),
        RelativeHumidity=uniform(*self.HUMID_RANGE),
        CO2=uniform(*self.CO2_RANGE)
    )

def run_sensor_generation(number_of_records):
    sensors = [Sensor(sensor_id=i + 1) for i in range(20)]
    result = []
    for sensor in sensors:
        result.extend(sensor.simulate_sensor_reply(number_of_records))
    return result
