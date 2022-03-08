# Hello World Codelab 
## Part 2



 In [Part 1](../helloworld) of the Hello World code lab, we created a simple "Hello World" application with a Java worker in the Orkes [Conductor Playground](https://play.orkes.io).


<p align="center"><img src="../img/hw_workflow1.png" alt="first workflow" width="400" style={{paddingBottom: 40, paddingTop: 40}} /></p>

In this section, we'll extend the workflow, adding another task to personalize our workflow.

**Topics Covered**

* Workflow versioning
* [HTTP task](../../reference-docs/http-task)


## Improving on "Hello World"

The initial workflow was very simple, so in this version of Hello World, we'll use the users' IP to determine their location (and thus their time zone and current time).

Since all web requests include the requestor's IP, it can be assumed that the workflow will include the users IP address. We will add another task to make an API call to get the user's location from their IP address.

## The HTTP Task

Conductor has several System Tasks that can be run on the Conductor server, and do not need separate worker applications to complete the task. The [HTTP task](/content/docs/reference-docs/system-tasks/http-task) is one of these.  We can use this task to ping [https://ip-api.com/](https://ip-api.com/) with our users' IP address, and this API will respond with information that we can use as a part of our workflow. 

> Note: System Tasks do not require uniqueIds. They also do not require a seperate definition: they can be defined inside the workflow.

## Updating the workflow

To add the HTTP Task, we simply update the workflow.  We make 2 changes to the workflow outside of the new task:

1. ```Version``` is set to 2.
2. We add an ```outputParameter``` from the new task.

### Adding the HTTP Task

To add this task to the workflow, we add it after the ```hello_world_<uniqueid>``` task.  

The parameters to this task are:
1. The HTTP Request. There are 2 parameters, the URI and the method (in this case GET).
2. Type is defined as ```HTTP```.

> The URI parameter uses a Conductor variable in the string.  The parameter ```${workflow.input.ipaddress}``` indicates that the workflow will have an input parameter called ```ipaddress```.

```json
{
  "name": "hello_world_<uniqueId>",
  "description": "hello world Workflow",
  "version": 2,
  "tasks": [
    {
      "name": "hello_world_<uniqueid>",
      "taskReferenceName": "hello_world_ref",
      "inputParameters": {},
      "type": "SIMPLE",
      "decisionCases": {},
      "defaultCase": [],
      "forkTasks": [],
      "startDelay": 0,
      "joinOn": [],
      "optional": false,
      "defaultExclusiveJoinTask": [],
      "asyncComplete": false,
      "loopOver": []
    },
    {
    "name": "Get_IP",
    "taskReferenceName": "get_IP",
    "inputParameters": {
        "http_request": {
        "uri": "http://ip-api.com/json/${workflow.input.ipaddress}?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,offset,isp,org,as,query",
        "method": "GET"
        }
    },
    "type": "HTTP"
    }

  ],
  "outputParameters": {

    "hw_response": "${hello_world_ref.output.hw_response}",
    "hw_location": "We hope the weather is nice near ${get_IP.output.response.body.city}"

  },
  "schemaVersion": 2,
  "restartable": true,
  "workflowStatusListenerEnabled": true,
  "ownerEmail": "devrel@orkes.io",
  "timeoutPolicy": "ALERT_ONLY",
  "timeoutSeconds": 0,
  "variables": {},
  "inputTemplate": {}
}
```

The ```get_IP``` task sends a GET request to the url - with the users' IP address as part of the query string.

The response comes back with details about this IP address in JSON format. One of the parameters is the City where the IP address is located.  We'll use the output of this API call to return the city name:

 ```   "hw_location": "We hope the weather is nice near ${get_IP.output.response.body.city}"```

When this change is made to the workflow, there are now two versions of the workflow.  In the screenshot below, there is a  "2" in the URL path, and as a dropdown next to the workflow name:

<p align="center"><img src="../img/hw2_workflow.png" alt="workflow version 2 screenshot" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>

## Running the new workflow

Clicking the Run Workflow button - now there are two options for the version. Let's pick version 2, and in the input add:

```
{"ipaddress":"<your IP address>"}
```

> To can find your IP address: Googling "What's my IP address?"

Click run workflow, and click on the workflowId.  If either of the tasks are blue, click the refresh until they are green:

<p align="center"><img src="../img/hw2_completed.png" alt="hello world 2 completed" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>

Clicking the ```Workflow Input/Output``` tab should display something similar to:

```json
{
"ipaddress":"98.11.11.125"
}
Output

{
"hw_location":"We hope the weather is nice near Kennebunk"
"hw_response":"Hello World!"
}
```

### Task Results

From the completed workflow diagram, click on the ```get_IP``` green box.  This will provide details about the task that ran during the workflow.  A side panel will open on the right.  If you click the ```Output``` tab, we can get more details on the API response that was generated by this task:


<p align="center"><img src="../img/hw2_httptaskresponse.png" alt="HTTP Task Response" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>


The response is shown here in completion: with all of the data in ```response.body```.  Here is where we see that the city is Kennebunk, ME, USA; the timezone, the offset from GMT (in seconds) and the ISP name (along with the IP address that was teh initial query). 

We'll use more of these details as we continue through the codelab.

## Next Steps

We've completed part 2 of the codelab.

In [Part 1](../helloworld), we created a workflow using the Netflix Conductor in the Orkes Playground

In Part 2, we extended the workflow using versioning, and added a HTTP Task.

In [Part 3](../helloworld3), we'll investigate using FORK tasks in your Workflow. Ready to continue? Let's go! [On to Part 3](../helloworld3)

