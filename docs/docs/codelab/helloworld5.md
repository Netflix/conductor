# Hello World Codelab Part 5

What we've covered so far:

[Hello World Part 1](../helloworld) We created the Hello World Workflow.

[Hello World Part 2](../helloworld2)  We created V2 of Hello World (Versioning) and added a HTTP Task.

[Hello World Part 3](../helloworld3)  We created V3 of Hello World, and introduced the concept of FORK and JOIN tasks.

[Hello World Part 4](../helloworld4)  We created V4 of Hello World, adding an Inline task to calculate a formula in JavaScript.

# Part 5  

In part 5 of the Hello World codelab, we'll wrap up our ```hello_world``` application by adding a [Switch Task](../../reference-docs/switch-task), and several [Set a Variable Tasks](../../reference-docs/set-variable-task).

In Part 4, we calculated the local time - based on the users IP address, and converting GMT to the local time. In Part 5, we'll wish the user a "Good [time of day]" based on the hour calculation.  

> Note: Clearly, this would be much easier to do in the same Inline task with just a couple of more lines of JavaScript, but why not show off the power of Conductor's Switch task?

## The Switch

A Switch task takes an input value, and then based on the results, will run different workflows based on the result.

The expression below will return a different time of day based on the hour calculated.

1. Before 6 AM will return "night":
2. 6-12 AM returns "morning"
3. 12-18 (12 -6 PM) returns "afternoon"
4. After 1800 (after 6 PM) returns "evening" 

Based on this data, a different set of tasks will be run.  In this case we'll set a different variable for each time of day.


## The Workflow

The workflow is getting very long, so let's just walk through the differences. Here is the basics of the Switch task:

1. The ```hour``` input parameter comes from the calculate_local_time_ref output.
2. The expressing is a simple if/else statement in JavaScript (and minified).  It defines the periods of the day, and returns the category.
3. There are ```decisionCases``` set for each category determined in the switch case.

```json
{
  "name": "time_of_day",
  "taskReferenceName": "time_of_day_ref",
  "inputParameters": {
    "hour": "${calculate_local_time_ref.output.result.hour}"
  },
  "type": "SWITCH",
  "evaluatorType": "javascript",
  "expression": "$.hour <= 6 ? 'night': $.hour <= 12 ? 'morning':$.hour <= 18 ? 'afternoon':$.hour <= 24 ? 'evening'}",
"defaultCase": [
    {
      
    }
  ],
  "decisionCases": {
    "night": [
      {
        ...
      }
    ],
    "morning": [
      {
        ...
      }
    ],
    "afternoon": [
      {
        ...
      }
    ],
    "evening": [
      {
        ...
      }
    ]
    
  }
}
```

## What should we do in each case?

In each "case" of the switch, we'll call a [Set variable task](/../../reference-docs/set-variable-task) to set a variable named ```message```.

For example the morning task will be:

```json
   {
      "name": "set_morning_variable",
      "taskReferenceName": "set_morning_variable_ref",
      "type": "SET_VARIABLE",
      "inputParameters": {
        "message": "Have a great morning!!"
      }
    }
```

We'll build a similar SET_VARIABLE task for afternoon, evening and night - ensuring that ```message``` has a value in every possible case of the ```SWITCH```.

Putting all of this logic together to create a Switch task with 4 cases, each with one embedded task looks like this:

```json
{
		    "name": "time_of_day",
		    "taskReferenceName": "time_of_day_ref",
		    "inputParameters": {
		      "hour": "${calculate_local_time_ref.output.result.hour}"
		    },
		    "type": "SWITCH",
		    "evaluatorType": "javascript",
		    "expression": "function e(){return $.hour<6?'night':$.hour<12?'morning':$.hour<18?'afternoon':'evening'}e();",
		    "decisionCases": {
		      "night": [
				  {
				       "name": "set_night_variable",
				       "taskReferenceName": "set_night_variable_ref",
				       "type": "SET_VARIABLE",
				       "inputParameters": {
				         "message": "Have a great night!!"
				       }
				  }
		      ],
		      "morning": [
				  {
				       "name": "set_morning_variable",
				       "taskReferenceName": "set_morning_variable_ref",
				       "type": "SET_VARIABLE",
				       "inputParameters": {
				         "message": "Have a great morning!!"
				       }
				  }
		      ],
		      "afternoon": [
				  {
				       "name": "set_afternoon_variable",
				       "taskReferenceName": "set_afternoon_variable_ref",
				       "type": "SET_VARIABLE",
				       "inputParameters": {
				         "message": "Have a great afternoon!!"
				       }
				  }
		      ],
		      "evening": [
				  {
				       "name": "set_evening_variable",
				       "taskReferenceName": "set_evening_variable_ref",
				       "type": "SET_VARIABLE",
				       "inputParameters": {
				         "message": "Have a great evening!!"
				       }
				  }
		      ]
    
		    }
		  }

```
Place this task in the JSON inside the FORK, right after the ```calculate_local_time``` task.  Additionally:

1. Update the version of the workflow to 5.
2. Change the joinOn from ```calculate_local_time_ref``` to ```time_of_day_ref```
3. add ```"hw_time_of_day": "${workflow.variables.message}"``` to ```outputParameters```.

Now the workflow diagram will look like:  

<p align="center"><img src="../img/hw5_workflow.png" alt="version 5 workflow diagram" width="600" style={{paddingBottom: 40, paddingTop: 40}} /></p>

## How it works

The hour is calculated in the task before. The ```SWITCH``` decides if the hour is night, morning, afternoon or evening.  Based on the SWITCH decision, a different ```SET_VARIABLE``` is called to create a phrase matching that time of day. Then, we call this variable as one of the output parameters of the workflow.

## Results

The input for version 5 is the same - just the ipaddress.  The result of the workflow adds the variable phrase in the ```hw_time_of_day``` greeting:

```json
{
"hw_location":"We hope the weather is nice near Bengaluru"
"hw_time":"The Local time is 23:41"
"hw_response":"Hello World!"
"hw_time_of_day":"Have a great evening!!"
}
```


## Conclusion

Congratulations! You have completed the Hello World Codelab.  You've build several iterations of the application, from V1 with just a Java worker, to V5 with the Java Worker, FORK, SWITCH, VARIABLE, HTTP and INLINE tasks.

We've covered a lot of the features in Conductor workflows.  If you'd like to review, here are links to earlier sections of the codelab:

[Hello World Part 1](../helloworld) We created the Hello World Workflow.

[Hello World Part 2](../helloworld2)  We created V2 of Hello World (Versioning) and added a HTTP Task.

[Hello World Part 3](../helloworld3)  We created V3 of Hello World, and introduced the concept of FORK and JOIN tasks.

[Hello World Part 4](../helloworld4)  We created V4 of Hello World, adding an Inline task to calculate a formula in JavaScript.


Thank you. We'd love your [feedback](https://share.hsforms.com/1TmggEej4TbCm0sTWKFDahwcfl4g) on this codelab.  