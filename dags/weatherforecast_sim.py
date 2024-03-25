from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests, json, logging, pendulum, logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from httpx import Client, HTTPError
from faker import Faker
from enum import Enum
import avro.schema, io
from avro.io import DatumWriter, BinaryEncoder


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.today('UTC').add(days=-1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# kafka_config = {
#     "sasl_mechanism": "PLAIN",
#     "security_protocol": "SASL_PLAINTEXT", 
#     "sasl_plain_username": Variable.get("KAFKA_USR"), 
#     "sasl_plain_password": Variable.get("KAFKA_PWD"),
#     "bootstrap_servers": Variable.get("KAFKA_CONTROLLER", deserialize_json=True)["controllers"],
# }

kafka_config = {
    "bootstrap_servers": Variable.get("KAFKA_CONTROLLER")
}

topic = Variable.get("KAFKA_TOPIC")
locales = ["nl_NL", "nl_BE"]


class Producer():
    producer: KafkaProducer

    def __init__(self, topic: str, serializer=None, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(value_serializer=serializer, **kwargs)

    def publish_json(self, message):
        try:
            self.producer.send(topic=self.topic, value=message)
            self.producer.flush()
        except KafkaError as ex:
            logging.exception(ex)
        else:
            logging.info(f"Published message {message} into topic {self.topic}")

    def publish_avro(self, message, schema):
        try:
            writer = DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = BinaryEncoder(bytes_writer)
            writer.write(message, encoder)
            self.producer.send(topic=self.topic, value=bytes_writer.getvalue())
            self.producer.flush()
        except KafkaError as ex:
            logging.exception(ex)
        else:
            logging.info(f"Published message {message} into topic {self.topic}")
    

class WeatherApi():
    """Details about the api can be found at: https://rapidapi.com/weatherapi/api/weatherapi-com"""
    api_url: str = "https://weatherapi-com.p.rapidapi.com"
    headers = {
        "X-RapidAPI-Key": "redacted",
        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
    }
    client: Client

    def __init__(self):
        self.client = Client(headers=self.headers, base_url=self.api_url)

    def get_weather_forecast(self, city: str):
        try:
            querystring = {"q": city, "days": "3"}
            response = self.client.get(url="forecast.json", params=querystring)
            return json.loads(response.text)
        except HTTPError as ex:
            logging.exception(ex)


@dag(schedule="*/5 * * * *", default_args=default_args, tags=["weather"], catchup=False, max_active_runs=1)
def weatherforecast_sim():
    
    @task()
    def execute_rest_api():

        url = "https://schemas.devops.svc:8443/avro/weatherforecast"
        headers = { 'Authorization': 'Basic dmFncmFudDp2YWdyYW50' }
        response = requests.request("GET", url, headers=headers, verify=False)
        schema = avro.schema.parse(json.loads(response.text))
        Producer(topic=topic, **kafka_config).publish_avro(WeatherApi().get_weather_forecast(Faker(locales).city()), schema)

    rest_api_task = execute_rest_api()

my_dag = weatherforecast_sim()