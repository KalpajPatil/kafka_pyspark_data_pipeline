import datetime
from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json
import socket
import logging
from logging.handlers import RotatingFileHandler
import os

app = Flask(__name__)

# Configure logging
def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, 'flask.log')
    
    # Set up root logger
    logging.basicConfig(level=logging.INFO)
    
    # Flask app logger
    handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=1024 * 1024,  # 1MB
        backupCount=5
    )
    handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
    ))
    handler.setLevel(logging.INFO)
    
    # Clear any existing handlers
    app.logger.handlers.clear()
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)
    
    # Also log to console for development
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    app.logger.addHandler(console_handler)

# Call the logging setup function before any routes are registered
setup_logging()

# Kafka Configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(kafka_config)
KAFKA_TOPIC = 'temperature_readings_april19'

@app.route('/temperature', methods=['POST'])
def receive_temperature():
    """Receive temperature data from M5Go device and forward to Kafka"""
    app.logger.info("Received request to /temperature endpoint")
    app.logger.debug(f"Request headers: {request.headers}")
    app.logger.debug(f"Request data: {request.data}")
    
    if not request.is_json:
        app.logger.error("Invalid request: No JSON data received")
        return jsonify({'error': 'Invalid request, JSON required'}), 400
    
    try:
        data = request.get_json()
        app.logger.debug(f"Parsed JSON data: {data}")
        
        # Validate required fields
        required_fields = ['device_id', 'temperature', 'client_timestamp']
        for field in required_fields:
            if field not in data:
                app.logger.error(f"Missing required field: {field}")
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        data['server_timestamp'] = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
        message = json.dumps(data)
        
        app.logger.debug(f"Sending to Kafka: {message}")
        producer.produce(
            KAFKA_TOPIC,
            key=data['device_id'],
            value=message.encode('utf-8'),
            callback=delivery_report
        )
        producer.flush()
        
        app.logger.info("Successfully sent data to Kafka")
        return jsonify({'status': 'success', 'message': 'Data sent to Kafka'}), 200
        
    except Exception as e:
        app.logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return jsonify({'error': f'Failed to process request: {str(e)}'}), 500

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        app.logger.error(f'Message delivery failed: {err}')
    else:
        app.logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.partition()}')

if __name__ == '__main__':
    app.logger.info("Starting Flask application")
    app.run(host='127.0.0.1', port=5000, debug=True)