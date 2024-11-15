

import requests
import json
import time
from requests.exceptions import ChunkedEncodingError

from confluent_kafka import Producer, KafkaException, KafkaError
from utils.logger import logger
from utils.variables import Variables
var = Variables()


bootstrap_server = var["kafka_config"]["bootstrap_servers"]
acks = var["kafka_config"]["acks"]

api_topic =  var["kafka_config"]["api_topic"]
batch_size = var["kafka_config"]["batch_size"]

def get_api_data(API_URL):
    return requests.get(API_URL, stream=True, timeout=10)


def connect_and_stream(API_URL,max_retries=5):
    
    try:
        logger.info(f"Fetching data from API: {API_URL}")
        retries = 0
        while retries < max_retries:
            try:
                response = get_api_data(API_URL=API_URL)
                
                if response.status_code == 200:
                    logger.info(f"API data fetched successfully")
                    batch_count = 0
                    
                    for line in response.iter_lines():
                        decoded_line = line.decode('utf-8')  # Decode bytes to string
                        if decoded_line.startswith("data:"):
                            json_data = decoded_line[5:].strip()
                            # json_data = json.loads(json_data) # just in case if we need key from json data
                            
                            try:
                                producer = kafka_producer(json_data)
                                
                                batch_count += 1                 
                                if batch_count >= batch_size:
                                    producer.flush()  # Ensure batch is sent
                                    batch_count = 0  
                                    
                            except json.JSONDecodeError as e:
                                logger.info(f"Error decoding JSON: {e} - Line: {line}")
                            
                    producer.flush()
                    
                else:
                    logger.info(f"Failed to connect to API. Status code: {response.status_code}")
            
            except ChunkedEncodingError:
                logger.info("Connection lost. Retrying...")
                retries += 1
                time.sleep(5)  # Wait before retrying
                
            except Exception as e:
                logger.info(f"Unexpected error: {e}")
                retries += 1
                time.sleep(5)  # Wait before retrying
                
        logger.info("Max retries reached. Exiting.")
        producer.flush()  # Final flush on exit
         
            
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout error when fetching data: {e}")
        raise


    except Exception as e:
        logger.info(f"any other exception : {e}")
        raise e

def delivery_report(err,msg):
    """
    Callback for Kafka message delivery reports.
    Triggered by producer.poll() or producer.flush().
    """
    if err is not None:
        parsed_data = json.loads(msg.value().decode("utf-8"))
        if isinstance(parsed_data, str):
            logger.warning("Parsed data is still a string; attempting to re-parse.")
            parsed_data = json.loads(parsed_data)
            
            if isinstance(parsed_data, dict):
                logger.info(f"Top-level keys in parsed_data: {parsed_data.keys()}")
                request_id = parsed_data.get("meta", {}).get("request_id")
                
                if request_id:
                    logger.info(f"Message delivered failed for request_id: {request_id}")
                else:
                    logger.warning("Request ID not found in parsed data.")
            
        logger.error(f"Message delivery failed for topic {msg.topic()} [{msg.partition()}]: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

            
        
def kafka_producer(event_data):
    kafka_config = {
        'bootstrap.servers': bootstrap_server,
        'acks': acks
    }    
    try:
        producer = Producer(kafka_config)
        
        producer.produce(
            topic=api_topic,
            value=json.dumps(event_data),
            callback=delivery_report
        )
        producer.poll(timeout=0)  # Poll to handle delivery events


        return producer
        
    except KafkaException as ke:
        
        logger.info(f"Kafka exception encountered: {ke}")
        # Inspect the exception details
        if ke.args[0].code() == KafkaError._ALL_BROKERS_DOWN:
            logger.info("All brokers are down. Please check your Kafka cluster.")
        elif ke.args[0].code() == KafkaError._TIMED_OUT:
            logger.info("Message timed out. Consider reviewing the timeout configuration.")
        else:
            logger.info("Other Kafka-related error occurred.")
    
    except BufferError:
        logger.info("Local producer queue is full. Consider increasing `queue.buffering.max.messages` or checking for a message send bottleneck.")

    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}") 
    
    finally:
        try:
            producer.flush(1)
        except Exception as flush_error:
            logger.info(f"Error during final flush: {flush_error}")
            raise flush_error
          



