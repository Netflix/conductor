
# Hello World Codelab 
## Part 1

"Hello World" is the traditional *first application* that a developer builds in any product that is new to them.  This Hello World demonstration is no different, kicking off the tutorial that outputs those famous two words, but we will also dig deeper into the powerful capabilities of Conductor. We'll extend our Hello World with additional messages.

After Part 1, the workflow that is created will look like:

<p align="center"><img src="../img/hw1_diagram.png" alt="version 1 diagram" width="400" style={{paddingBottom: 40, paddingTop: 40}} /></p>


As we complete this code lab, we'll be familiar with these aspects of Netflix Conductor:

* Workflows
* Workflow versioning
* Tasks
* Simple Java Task
* System Tasks
    * HTTP Task
    * Inline Task
* System Operators
    * Fork/Join
    * Switch
    * Set Variable

ON completion of the codelab, the final Hello World workflow looks a lot more complicated:


<p align="center"><img src="../img/hw5_workflow.png" alt="part 5 workflow diagram" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>

## Requirements

This demo will use the Orkes [Conductor playground](https://play.orkes.io) to run the workflow.  Create  an account for free by signing up with an email address or Google account.  Alternatively, the open source Conductor [installed locally](/docs/getting-started/install/running-locally) can also be used, using the API to add Tasks and Workflows.  The UI in the Playground screenshots will be otherwise similar.

To run the Java worker, Java must be installed on yur local computer.

## A Simple Hello World 


Our initial Hello World workflow will call a task (and it's underlying Java Worker) that will return the text "Hello World."  It is a very simple use case, and not one that would typically use Conductor for, but we'll use this extremely simple example as the foundation of our knowledge to build a larger orchestration workflow.

To get this workflow up and running, we'll need to create a task, a workflow and a Java application worker.  The Orkes Playground will also require some basic permissioning (that we will also cover in the codelab).

### Creating the task

Our ```helloWorld``` task will be called by the workflow.  When it is called, it will place a task in the worker queue for the workers to pickup and run.  The results are returned to the task from the worker - which is then passed back to the workflow.

Our ```helloworld``` task takes no inputs, and provides no inputs to the Java application (they would appear in the ```inputKeys``` array).  The output of this task will be the response from the Java app, and will have a JSON key of ```hw_response```.  Based on this definition, we can create the JSON that defines this task:

 > Note: All tasks and workflows require a unique name in the Playground, so your task will need a ```<uniquetag>``` added to the name attribute.


helloworld_task.json
``` json
{
"name": "hello_world_<uniquetag>",
"retryCount": 3,
"timeoutSeconds": 5,
"pollTimeoutSeconds": 5,
"inputKeys": [
  
],
"outputKeys": [
  "hw_response"
],
"timeoutPolicy": "TIME_OUT_WF",
"retryLogic": "FIXED",
"retryDelaySeconds": 5,
"responseTimeoutSeconds": 5,
"concurrentExecLimit": 100,
"rateLimitFrequencyInSeconds": 30,
"rateLimitPerFrequency": 50,
"ownerEmail": "<your_email>"
}
```

This task will add a job this to the worker queue.  It will retry 3 times, timing out after 5 seconds.  The output will appear in the response JSON with a key of ```hw_response```.

To add this Task in your Playground:
1. Click "Task Definitions" in the left navigation.
2. lick the "Define Task" button in the upper right.  
3. Paste the workflow into the edit field. Remember to change ```<uniquetag>``` in the ```name``` field, and update your e-mail address in the ```ownerEmail``` field.
4. Press ```Save``` and ```Confirm Save```.  The Task is now visible in the list of task definitions.


### Creating the workflow

The Conductor workflow defines all of the tasks that will be run (and the order that they will run in).  In part 1 of this codelab, we will define a workflow with just one single task.

We will define this workflow as version 1. Version numbers can only be integers, and as we walk through the codelab, we can increment the version as the complexity increases.  This also allows us to run the different versions at the same time.  Imagine a use case where team a is ready to jump to an updated workflow, but team b is not.  The versioning makes it easy to support both teams.

Things to note:

1. We need a ```<uniquetag>``` added to the name here as well, because the playground cannot have multiple workflows of the same name.
2. The ```hello_world_<uniquetag>``` task is called a ```SIMPLE``` task.  All tasks that call external workers have this naming convention.
3. The output of the workflow will be JSON. The value references the output of the Task.
4. Feel free to change the owner email.


```json
{
  "name": "hello_world_<uniquetag>",
  "description": "hello world Workflow",
  "version": 1,
  "tasks": [
    {
      "name": "hello_world_<uniquetag>",
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
    }
  ],
  "outputParameters": {

    "hw_response": "${hello_world_ref.output.hw_response}"

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

To create this workflow in The Orkes Playground:
1. Click Workflow Definitions
2. Click the ```Define Workflow``` button at the top.
3. Paste in the JSON (with your edits).
4. Press ```Save``` and ```Confirm Save```.

We now have everything in our Conductor instance ready to go to run this application.  The only missing piece is the Java app that will create the "hello world!" message.

### Application Permissions

The Orkes Playground is a secure workspace, so we will need to create application credentials for your worker.  To do this, click ```Applications``` in the left navigation.  

> Note: if you already have created an application and JWT for the OrkesWorkers github repository, you can reuse that application and JWT to add the new workflow and task.

Create a new application by pressing the ```create application``` button and naming your application. 

There are two sections on the Application management page "Access Keys" and "Workflow and Tasks Permissions."  We'll go a bit out of order here, and start with "Workflow and Tasks Permissions." Click the ```+``` icon.

Add your Workflow:
1. Target Type: ```Workflow```
2. Target: ```hello_world_<uniqueid>```
3. Access: ```Execute```

And repeat the process to add the task
1. Target Type: ```Task```
2. Target: ```hello_world_<uniqueid>```
3. Access: ```Execute```

**In general, all tasks (and workflows) must be added to an application in the Playground for the application to run.**

Now, create an Access Key by pressing the ```+``` button in the top table. This will generate a Key and Secret Id for the application.  Record these in a safe place. 


### The Java app

The Java app can be found in the [orkesworkers](https://github.com/orkes-io/orkesworkers) repository. The easiest way to get this to run is to clone this repository, and then run the ```OrkesWorkersApplication.java``` application. 

There are 2 small changes to be made to the [application.properties](https://github.com/orkes-io/orkesworkers/blob/main/src/main/resources/application.properties) file:

```java
conductor.server.url=https://play.orkes.io/api/

conductor.security.client.key-id=
conductor.security.client.secret=

```

Add in your key and secret you just generated.

The worker is called ```helloworld.java```, and looks as follows:

```js
@Component
public class helloworld implements Worker {
    @Override
    public String getTaskDefName() {
        return "hello_world_<uniquetag>";
    }

    @Override
    public TaskResult execute(Task task) {
        TaskResult result = new TaskResult(task);
        result.addOutputData("hw_response", "Hello World!");
        result.setStatus(TaskResult.Status.COMPLETED);
        return result;
    }
}
```

We name the task (**update the name in the ```return "hello_world_<uniqueId>"``` string to match your task's name**), and in the TaskResult - send the "hello world!" response back to Conductor.

We can examine our workflow by browsing through ```Workflow definitions``` -> ```hello_world_<uniqueid>```.  The url for this page is ```https://play.orkes.io/workflowDef/hello_world_<uniqueId>```


<p align="center"><img src="../img/hw_playworkflow1.png" alt="workflow screenshot" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>



We now can test our workflow!

## Running our First Hello World

We can test our workflow by clicking the ```Run Workflow``` box in the left navigation. Select the workflow name, choose version 1, and leave the input blank (or empty {}), since there are no input parameters:


<p align="center"><img src="../img/hw1_runworkflow.png" alt="running Hello World" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>



As we can see in the screenshot above, submitting the workflow creates a workflowId - it appears below the ```Run Workflow``` button. Clicking this link will take open the Workflow execution page (the url is ```play.orkes.io/execution/<workflowid>```):


<p align="center"><img src="../img/hw1_execution.png" alt="Hello World execution" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>


Clicking through this page, we can see that the output of the workflow is, as expected "Hello World!"


<p align="center"><img src="../img/hw1_output.png" alt="output screenshot" width="800" style={{paddingBottom: 40, paddingTop: 40}} /></p>


## Next Steps

In the next step of this code lab, we'll extend the "hello world" to be more friendly and customized.  In the next step, we'll determine the location of our 'hello worlders.'

We'll also introduce versioning to ensure that both the original and the updated "Hello World" applications continue to run.

[Read on to Part 2](../helloworld2)