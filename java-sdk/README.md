# SDK for Conductor
Java SDK provides the ability to unit test the workflows.

### Why unit test workflows
Workflow definition can be quite complex with parallel flows, decisions and complex wiring of tasks.
The unit tests allows the following:
1. **Input/Output Wiring**: Ensure the tasks are wired up correctly
2. **Parameter check**: Workflow behavior with missing mandatory parameters is expected (fail if required)
3. **Task Failure behavior**: Ensure the task definitions have right no. of retries etc.  
e.g. If the task is not idempotent, it does not get retried. 
4. **Branch Testing**: Given a specific input, ensure the workflow executes specific branch of the forrk/decision.

### Unit Testing framework
The SDK provides a framework for running workflow definitions against the actual Conductor server.
When the tests are initialized a version of Conductor server is downloaded and started on port `8080` on your local machine.
The server is self-contained with no additional dependencies required and stores all the data 
in memory.  Once the tests complete, the server is terminated.

The location of the binary and version is controlled via following system properties - and defaults to maven repository using the latest conductor release.
* `conductorServerURL`: Full URL pointing to the server boot jar file. e.g. https://repo1.maven.org/maven2/com/netflix/conductor/conductor-server/3.3.4/conductor-server-3.3.4-boot.jar
* `conductorVersion`: If using the default maven repo, use this to override the version of Conductor you want to test against.
* `conductorServerPort`: Port on which to listen.  Defaults to 8080 if not specified.

[WorkflowExecutor](src/main/java/com/netflix/conductor/sdk/executor/WorkflowExecutor.java)  provides all the necessary APIs to load the metadata and run the tests.
#### Unit Testing APIs
##### startServerAndPolling(String basePackages)
Downloads the conductor server in a temporary directory and starts the local server.

#### loadTaskDefs(String resourcePath)
Loads all the task definitions from the given resource path.

#### loadWorkflowDefs(String resourcePath)
Loads all the workflow definitions from the given resource path.

#### executeWorkflow(String name, int version, Map<String, Object> input)
Executes the workflow and waits for it to complete.  Returns the completed execution which can be validated.

See [Test](src/test/java/com/netflix/conductor/testing/workflows/KitchenSinkTest.java) for an example.