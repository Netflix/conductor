# SDK for Conductor
This SDK provides two main features:
1. Ability to create and run workflows using code
2. Ability to unit test workflows

## Workflow SDK
The SDK for creating and running workflows and task workers makes it easier to define your workflows using code.

### Creating Task Workers
SDK provides `@WorkflowTask` annotation that can be used with any method to convert method in to a task worker.

**Examples**

Create a worker named `task1` that gets Task as input and produces TaskResult as output.
```java
@WorkflowTask("task1")
    public TaskResult task1(Task task) {
        task.setStatus(Task.Status.COMPLETED);
        return new TaskResult(task);
    }
```

Create a worker named `task2` that takes `name` as a String input and produces a
```java
@WorkflowTask("task2")
public @OutputParam("greetings") String task2(@InputParam("name") String name) {
    return "Hello, " + name;
}
```
Example Task Input/Output

Input:
```json
{
   "name": "conductor",
   ...
}
```

Output:
```json
{
   "greetings": "Hello, conductor"
}
```
A worker that takes complex java type as input and produces complex output:
```java
@WorkflowTask("get_insurance_quote")
 public InsuranceQuote getInsuranceQuote(GetInsuranceQuote quoteInput) {
     InsuranceQuote quote = new InsuranceQuote();
     //Implementation
     return quote;
 }
```

Example Task Input/Output

Input:
```json
{
   "name": "personA",
   "zipCode": "10121",
   "amount": 1000000
}
```

Output:
```json
{
   "name": "personA",
   "quotedPremium": 123.50,
   "quotedAmount": 1000000
}
```

#### Task Worker


## Unit testing framework
The framework allows you to test the workflow definitions against a specific version of Conductor server.

The unit tests allows the following:
1. **Input/Output Wiring**: Ensure the tasks are wired up correctly
2. **Parameter check**: Workflow behavior with missing mandatory parameters is expected (fail if required)
3. **Task Failure behavior**: Ensure the task definitions have right no. of retries etc.  
   e.g. If the task is not idempotent, it does not get retried.
4. **Branch Testing**: Given a specific input, ensure the workflow executes specific branch of the fork/decision.

The local test server is self-contained with no additional dependencies required and stores all the data
in memory.  Once the tests complete, the server is terminated and all the data is wiped out.

See [Testing Framework](testing_framework.md) for details on how to do unit testing with examples.




