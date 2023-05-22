
import os

import functions_framework
import redis

client = redis.Redis(host='', port=6379, db=0)

@functions_framework.http
def hello_get(request):
    data = request.get_json()
    if data["ops"] == "SET":
        client.set(data["key"], data["val"])
        return "Successful"

    if data["ops"] == "GET":
        resp = client.get(data["key"])
        return str(resp)