import requests

req_header={'Content-Type': 'application/json', 'Accept': 'application/json'}

task_def_endpoint='http://localhost:8080/api/metadata/taskdefs'
task_def = '''[{"name": "verify_if_idents_are_added", "retryCount": 3, "retryLogic": "FIXED", "retryDelaySeconds": 10, "timeoutSeconds": 300, "timeoutPolicy": "TIME_OUT_WF", "responseTimeoutSeconds": 180, "ownerEmail": "aasdasdadsdasds@gmail.com"}]'''

response= requests.post(task_def_endpoint, data=task_def, headers=req_header)
print(response.status_code)
