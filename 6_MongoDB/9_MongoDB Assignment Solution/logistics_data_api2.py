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

@app.route('/api/count', methods=['GET'])
def count_vehicles_by_gps_provider():
    try:
        # Aggregate the count of vehicles by GpsProvider
        pipeline = [
            {"$group": {"_id": "$GpsProvider", "count": {"$sum": 1}}}
        ]

        result = list(collection.aggregate(pipeline))

        if result:
            return jsonify({"result": result}), 200
        else:
            return jsonify({"result": "No data available"}), 404

    except Exception as e:
        # Check if the exception has a meaningful error message
        error_message = str(e) if e and str(e) else "An unexpected error occurred."

        # Return a JSON response with the error message and a 500 Internal Server Error status code
        return jsonify({"error": error_message} if error_message else {"error": "An unexpected error occurred."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)





