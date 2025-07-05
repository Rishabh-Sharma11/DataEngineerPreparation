#!/usr/bin/env python
# coding: utf-8


from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId  # Import ObjectId from bson
import re

app = Flask(__name__)

# MongoDB configuration
mongo_client = MongoClient('mongodb+srv://absf1r3:Potato123@mongodbtest.qa6icb1.mongodb.net/?retryWrites=true&w=majority')  
db = mongo_client['gds_db']
collection = db['logistics_data']

@app.route('/api/filter', methods=['GET'])
def filter_documents():
    try:
        # Get the value of the 'vehicle_no' query parameter
        vehicle_no = request.args.get('vehicle_no')

        if not vehicle_no:
            return jsonify({"error": "Missing 'vehicle_no' parameter"}), 400

        # Example: Get documents with a specific 'vehicle_no'
        result = collection.find({'vehicle_no': vehicle_no})

        # Convert the result to a list, and convert ObjectId to string for JSON serialization
        result_list = [{'_id': str(doc['_id']), 'vehicle_no': doc['vehicle_no']} for doc in result]

        if result_list:
            return jsonify({"result": result_list}), 200
        else:
            return jsonify({"result": "No matching documents found"}), 404

    except Exception as e:
        # Check if the exception has a meaningful error message
        error_message = str(e) if e and str(e) else "An unexpected error occurred."

        # Return a JSON response with the error message and a 500 Internal Server Error status code
        return jsonify({"error": error_message} if error_message else {"error": "An unexpected error occurred."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)




