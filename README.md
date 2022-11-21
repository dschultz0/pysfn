# PySFN
*A Python to AWS Step Functions transpiler for the CDK*

This package is an initial experiment in exploring ways to make AWS Step Functions more useful by allowing
developers to build state machines in the same way they would write a Python function. Users can define
state machines in their CDK Stack as simple functions, then apply a `@state_machine` decorator to declare
a Construct in their stack.

This is very much an experiment, and I welcome feedback on the viability and utility of this approach. 

Note that because it's Python-based it will only work when used with Python CDK stacks, and not TypeScript or 
other languages. Of course, your Lambdas can be written in any language, but Python Lambdas can take advantage
of some additional features.

## Quick start
There is a lot of good information below, but if you want to get started quickly and experiment with the
prototype app, clone the repo give it a shot. Assuming you have the AWS CDK installed, you should be able to
deploy the app by doing the following:

```shell
pip install pysfn
cd proto_app
cdk deploy
```

Once you've deployed it, you can submit the *basic*, *simple*, and *larger* step functions that have been 
created with the following input.

```json
{
  "str_value": "html",
  "list_value": [100, 100],
  "option": false
}
```

Replacing `html` with `image`, `pdf`, or some other value will trigger the different paths in the function, 
and you can also test how default values are used by leaving off the `list_value` and `option` values.

## Why Step Functions?
AWS Step Functions (SFN) is a useful tool for orchestrating processing steps in a serverless fashion. By providing 
the ability to invoke a range of AWS services such as Lambdas, DynamoDB, SNS, and many others, it's significantly
easier to componentize an application into reusable pieces that can leverage a range of programming languages,
libraries, and utilities, while keeping compute requirements and complexity low.

For example, I've built SFN applications that combine NodeJS lambdas, Python Lambdas using a range of 
different libraries, the AWS Textract service and DynamoDB into a single app that can be used in multiple
contexts for data processing. Building this in SFN avoids the need to launch a hosted service to manage
the processing flow, and keeps each resource focused on the processing needs of that particular function.

## States Language Hell
The biggest downside of SFN is the language that AWS developed to power it.  The
[Amazon States Language](https://states-language.net/) makes it possible to develop processing flows in
a technology-agnostic way, but it can be clumsy to pick up and use efficiently. Data moves through a state
machine definition as a JSON object, and each processing step must manipulate it using jsonpath.
To do this well, a developer needs to be aware of the inputs and outputs of each stage and handle them appropriately.
In addition, the use of jsonpath operations limits how these values can be assigned to the payload object. As a 
result it's common to follow each processing step with a Pass stage to restructure the results into the payload
appropriately. The alternative is to make each processing stage take on this responsibility within the processing
flow. This works, but forces a very tight connection between the SFN definition and the Lambda or other code, and
removes the ability to flexibly use that component in a different context.

# A new approach
PySFN allows you to define your state machines in the same way that you would define any other function in
Python. Look at the following function which executes a series of steps. The steps (`step[1-4]`) each refer
to a lambda operation that we want to execute. 

```python
@state_machine(self, "pysfn-basic", locals())
def basic(str_value: str, list_value: List[int] = None, option: bool = False):
    uri1: Union[str, None] = None
    uri2: Union[str, None] = None
    (
        available,
        mode,
        option,
        processing_seconds,
        code_value,
        type_value,
    ) = step1(str_value, option)

    if available:
        if mode == "html":
            (available, list_value, uri1) = step2(str_value, list_value)
        else:
            (available, uri2, uri1) = step3(str_value, mode, code_value)
        if uri1:
            uri2 = step4(uri1)
    return (
        mode,
        code_value,
        processing_seconds,
        available,
        uri1,
        uri2,
        option,
    )
```

By attaching the `@statemachine` decorator to the function, we instruct the CDK to generate a State Machine
Construct named *pysfn-basic* that has a definition aligned with the function contents. You can see the result
in the **long** detail below.

```json
{
  "StartAt": "Register Input [1:1]",
  "States": {
    "Register Input [1:1]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "Next": "Has list_value [1:2]"
    },
    "Has list_value [1:2]": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.register.list_value",
          "IsPresent": false,
          "Next": "Assign list_value default [1:3]"
        }
      ],
      "Default": "Has option [1:4]"
    },
    "Assign list_value default [1:3]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "list_value": "",
        "str_value.$": "$.str_value"
      },
      "Next": "Has option [1:4]"
    },
    "Has option [1:4]": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.register.option",
          "IsPresent": false,
          "Next": "Assign option default [1:5]"
        }
      ],
      "Default": "Assign uri1 [1:6]"
    },
    "Assign option default [1:5]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "option": false,
        "str_value.$": "$.str_value",
        "list_value.$": "$.list_value"
      },
      "Next": "Assign uri1 [1:6]"
    },
    "Assign uri1 [1:6]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "uri1": "",
        "option.$": "$.option",
        "str_value.$": "$.str_value",
        "list_value.$": "$.list_value"
      },
      "Next": "Assign uri2 [1:7]"
    },
    "Assign uri2 [1:7]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "uri2": "",
        "option.$": "$.option",
        "uri1.$": "$.uri1",
        "str_value.$": "$.str_value",
        "list_value.$": "$.list_value"
      },
      "Next": "Call step1 [1:8]"
    },
    "Call step1 [1:8]": {
      "Next": "Register step1 [1:9]",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Type": "Task",
      "ResultPath": "$.register.out",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:us-west-2:999999999999:function:pysfn-base-python",
        "Payload": {
          "str_value.$": "$.register.str_value",
          "bool_value.$": "$.register.option",
          "launcher_target": "step1"
        }
      }
    },
    "Register step1 [1:9]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "available.$": "$.out.Payload.arg0",
        "mode.$": "$.out.Payload.arg1",
        "option.$": "$.out.Payload.arg2",
        "processing_seconds.$": "$.out.Payload.arg3",
        "code_value.$": "$.out.Payload.arg4",
        "type_value.$": "$.out.Payload.arg5",
        "list_value.$": "$.list_value",
        "uri1.$": "$.uri1",
        "uri2.$": "$.uri2",
        "str_value.$": "$.str_value"
      },
      "Next": "If available [1:10]"
    },
    "If available [1:10]": {
      "Type": "Choice",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.register.available",
              "IsPresent": true
            },
            {
              "Or": [
                {
                  "And": [
                    {
                      "Variable": "$.register.available",
                      "IsBoolean": true
                    },
                    {
                      "Variable": "$.register.available",
                      "BooleanEquals": true
                    }
                  ]
                },
                {
                  "And": [
                    {
                      "Variable": "$.register.available",
                      "IsString": true
                    },
                    {
                      "Not": {
                        "Variable": "$.register.available",
                        "StringEquals": ""
                      }
                    }
                  ]
                },
                {
                  "And": [
                    {
                      "Variable": "$.register.available",
                      "IsNumeric": true
                    },
                    {
                      "Not": {
                        "Variable": "$.register.available",
                        "NumericEquals": 0
                      }
                    }
                  ]
                }
              ]
            }
          ],
          "Next": "If mode=='html' [1:11]"
        }
      ],
      "Default": "Return [1:19]"
    },
    "If mode=='html' [1:11]": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.register.mode",
          "StringEquals": "html",
          "Next": "Call step2 [1:12]"
        }
      ],
      "Default": "Call step3 [1:14]"
    },
    "Call step2 [1:12]": {
      "Next": "Register step2 [1:13]",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Type": "Task",
      "ResultPath": "$.register.out",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:us-west-2:999999999999:function:pysfn-js",
        "Payload": {
          "strValue.$": "$.register.str_value",
          "optParam.$": "$.register.list_value"
        }
      }
    },
    "Register step2 [1:13]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "available.$": "$.out.Payload.available",
        "list_value.$": "$.out.Payload.listValue",
        "uri1.$": "$.out.Payload.resultURI",
        "code_value.$": "$.code_value",
        "mode.$": "$.mode",
        "processing_seconds.$": "$.processing_seconds",
        "option.$": "$.option",
        "type_value.$": "$.type_value",
        "uri2.$": "$.uri2",
        "str_value.$": "$.str_value"
      },
      "Next": "If uri1 [1:16]"
    },
    "Call step3 [1:14]": {
      "Next": "Register step3 [1:15]",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Type": "Task",
      "ResultPath": "$.register.out",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:us-west-2:999999999999:function:pysfn-highmemory-python",
        "Payload": {
          "str_value.$": "$.register.str_value",
          "str_value2.$": "$.register.mode",
          "str_value3.$": "$.register.code_value",
          "launcher_target": "step3"
        }
      }
    },
    "Register step3 [1:15]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "available.$": "$.out.Payload.arg0",
        "uri2.$": "$.out.Payload.arg1",
        "uri1.$": "$.out.Payload.arg2",
        "code_value.$": "$.code_value",
        "mode.$": "$.mode",
        "processing_seconds.$": "$.processing_seconds",
        "list_value.$": "$.list_value",
        "option.$": "$.option",
        "type_value.$": "$.type_value",
        "str_value.$": "$.str_value"
      },
      "Next": "If uri1 [1:16]"
    },
    "If uri1 [1:16]": {
      "Type": "Choice",
      "Choices": [
        {
          "And": [
            {
              "Variable": "$.register.uri1",
              "IsPresent": true
            },
            {
              "Or": [
                {
                  "And": [
                    {
                      "Variable": "$.register.uri1",
                      "IsBoolean": true
                    },
                    {
                      "Variable": "$.register.uri1",
                      "BooleanEquals": true
                    }
                  ]
                },
                {
                  "And": [
                    {
                      "Variable": "$.register.uri1",
                      "IsString": true
                    },
                    {
                      "Not": {
                        "Variable": "$.register.uri1",
                        "StringEquals": ""
                      }
                    }
                  ]
                },
                {
                  "And": [
                    {
                      "Variable": "$.register.uri1",
                      "IsNumeric": true
                    },
                    {
                      "Not": {
                        "Variable": "$.register.uri1",
                        "NumericEquals": 0
                      }
                    }
                  ]
                }
              ]
            }
          ],
          "Next": "Call step4 [1:17]"
        }
      ],
      "Default": "Return [1:19]"
    },
    "Call step4 [1:17]": {
      "Next": "Register step4 [1:18]",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Type": "Task",
      "ResultPath": "$.register.out",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:us-west-2:999999999999:function:pysfn-base-python",
        "Payload": {
          "str_value.$": "$.register.uri1",
          "launcher_target": "step4"
        }
      }
    },
    "Register step4 [1:18]": {
      "Type": "Pass",
      "ResultPath": "$.register",
      "InputPath": "$.register",
      "Parameters": {
        "uri2.$": "$.out.Payload.arg0",
        "available.$": "$.available",
        "code_value.$": "$.code_value",
        "mode.$": "$.mode",
        "processing_seconds.$": "$.processing_seconds",
        "list_value.$": "$.list_value",
        "option.$": "$.option",
        "uri1.$": "$.uri1",
        "type_value.$": "$.type_value",
        "str_value.$": "$.str_value"
      },
      "Next": "Return [1:19]"
    },
    "Return [1:19]": {
      "Type": "Pass",
      "Parameters": {
        "mode.$": "$.register.mode",
        "code_value.$": "$.register.code_value",
        "processing_seconds.$": "$.register.processing_seconds",
        "available.$": "$.register.available",
        "uri1.$": "$.register.uri1",
        "uri2.$": "$.register.uri2",
        "option.$": "$.register.option"
      },
      "End": true
    }
  }
}
```

A few items to note with this result:
* To avoid name conflicts when the CDK generates the constructs, I've added an ID suffix to each stage.
* I treat the `register` object within the payload as my version of `locals()` to maintain a clean
  view of the current set of vars. The first step copies the inputs into the register.
* After this, we address any optional parameters defined in the function signature. If they aren't present,
  we set the default value.
* This function sets defaults for two `uri` values which we set using Pass states.
* The if operations are converted to Choice states with the appropriate conditions. Note that in the case
  of the first and last Choice states, the logic inserts a complex condition to mimic Python boolean type coercion.
* Each call to a Lambda function is followed by a generated Pass state to move the results into the register.

## About Lambdas...
One of the goals of this project is to make working with Python lambdas more flexible so that you don't have
to spend a lot of time writing code to parse the `event` object over and over. While it's not necessary to
use it to take advantage of the transpiler, most of the Lambda steps in the proto_app are based on 
**launcher** logic I've included.

The `step1` function in the `operations.py` module is defined as shown below. Note that this looks like any other 
python function and could be referenced anywhere in your code. 

```python
def step1(str_value: str, bool_value: bool) -> (bool, str, bool, int, int, str):
    return True, str_value, False, 4, 200, "text/html"
```

To pull this into our stack we have to start by creating a Lambda that will hold the function. 
This looks like this:

```python
base_lambda = PythonLambda(
    self,
    "pysfn-base-python",
    os.path.join(os.getcwd(), "python"),
    role=self.lambda_role,
    runtime=PythonLambda.PYTHON_3_9,
    timeout_minutes=1,
    memory_mb=1,
    environment=None,
)
```

The `PythonLambda` class allows us to define a Lambda Construct that can contain multiple functions to be 
executed via a launcher that it will generate. Now that we've defined the container, we can add our function
to the launcher.

```python
step1 = base_lambda.register(operations.step1)
```

The new `step1` variable has the same function signature as the original function, but can now be used
within our state machine function. The transpiler uses the details of this lambda to produce the following
state in our state machine. Note the `pysfn_operation` value that is included in the Payload.

```json
    "Call step1 [1:8]": {
      "Next": "Register step1 [1:9]",
      "Type": "Task",
      "ResultPath": "$.register.out",
      "Resource": "arn:aws:states:::lambda:invoke",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:us-west-2:999999999999:function:pysfn-base-python",
        "Payload": {
          "str_value.$": "$.register.str_value",
          "bool_value.$": "$.register.option",
          "pysfn_operation": "step1"
        }
      }
    }
```

Of course, existing Lambdas are also supported. For example, we can define a Lambda construct as we normally
would as shown below.

```python
js_lambda = lmbda.Function(
    self,
    "JSLambda",
    function_name="pysfn-js",
    code=lmbda.Code.from_asset(
        os.path.join(os.getcwd(), "js"), exclude=["node_modules"]
    ),
    handler="app.handler",
    runtime=lmbda.Runtime.NODEJS_14_X,
    role=self.lambda_role,
    timeout=Duration.minutes(10),
    memory_size=2096,
)
```

Then we can create a pysfn function from this construct as follows by declaring the input parameters and output 
values.

```python
step2 = function_for_lambda(
    js_lambda,
    {"strValue": str, "optParam": bool},
    {"available": bool, "listValue": List[int], "resultURI": str},
)
```

By specifying the output values in the function declaration, it allows PySFN to map the results from 
a call like this to the appropriate variables.

```python
(available, list_value, uri1) = step2(str_value, list_value)
```

In the step after the Lambda is invoked, a Pass state performs the mapping.

```json
"Register step2 [1:13]": {
  "Type": "Pass",
  "ResultPath": "$.register",
  "InputPath": "$.register",
  "Parameters": {
    "available.$": "$.out.Payload.available",
    "list_value.$": "$.out.Payload.listValue",
    "uri1.$": "$.out.Payload.resultURI",
    "code_value.$": "$.code_value",
    "mode.$": "$.mode",
    "processing_seconds.$": "$.processing_seconds",
    "option.$": "$.option",
    "type_value.$": "$.type_value",
    "uri2.$": "$.uri2",
    "str_value.$": "$.str_value"
  },
  "Next": "If uri1 [1:16]"
}
```

# More to do!
After a bunch of experiments and refactoring, I think I've been able to prove the utility of this approach,
at least for the range of projects I typically use SFN for. It's still undocumented and has a lot of
rough edges, but overall I've been thrilled at how easy it has been to iterate on new and existing SFNs
using this approach. It significantly reduces the cognitive load I felt when working with the stages language
and makes it much easier to build stable and well-managed data flows.

That said, feedback and PRs are welcome. Over the next few months I'll hopefully be able to address the
following:
1. Better support for `list`, `dict`, and `attribute` access
2. List comprehensions
3. Support for dataclasses
4. Real documentation
5. Take full advantage of Python type hints
6. Support functions with kwonly or posonly args
7. Add support for Parallel
8. Support the full range of likely conditions
9. Tree shaking to better handle if/elif/elif/else, as well as assigning multiple variables
10. Support some common integrations such as reading from S3 or performing DynamoDB writes
