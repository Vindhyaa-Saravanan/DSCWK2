import azure.functions as func
import logging
import asyncio
from random import uniform
from time import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

class Sensor:
    def __init__(self):
        self.TEMP_RANGE = (8, 12)
        self.WIND_SPEED_RANGE = (15, 25)
        self.HUMIDITY_RANGE = (40, 70)
        self.CO2_RANGE = (500, 1500)

    def simulate_sensor_reply(self, number_of_records: int) -> list:
        return [self.generate_single_record() for _ in range(number_of_records)]

    def generate_single_record(self) -> dict:
        return {
            "temperature": uniform(*self.TEMP_RANGE),
            "wind_speed": uniform(*self.WIND_SPEED_RANGE),
            "relative_humidity": uniform(*self.HUMIDITY_RANGE),
            "co2": uniform(*self.CO2_RANGE)
        }

async def run_sensor_generation(number_of_records):
    sensors = [Sensor() for _ in range(20)]
    # Run sensor generation asynchronously
    await asyncio.gather(*[sensor.simulate_sensor_reply(number_of_records) for sensor in sensors])

        
@app.route(route="task1_datafunction_httptrigger")
def task1_datafunction_httptrigger(req: func.HttpRequest) -> func.HttpResponse:
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
        run_sensor_generation(int(number_of_records))
        end_time = time()  # Record the end time
        time_taken = end_time - start_time  # Calculate the total time taken
        return func.HttpResponse(f"Hello, 20 sensors generated {number_of_records} each. Time taken = {time_taken} seconds.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass the 'number_of_records' parameter in the query string or in the request body for a personalized response.",
             status_code=400
        )
