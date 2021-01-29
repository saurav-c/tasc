# Building a TASC Lambda Function

With the desired workoad of interacting with the system by using serverless functions, we use AWS Lambda to determine/mock actual workloads that use TASC. You can write functions code that get executed in the system and these functions can call other functions and pass along critical data needed in the system.

## Steps to Create/Run the Function

In order to utilize these scripts, you will need to do the following:

### Step 1: Preparing to write the Function

* Download and install the AWS CLI [here](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html). Configure it using the `aws configure` command.
* Make an S3 bucket to store the Lambda zip file in (We need this since our zip file's size is above the limit of the one we can use in AWS CLI)
* Make a copy of the tasc-lambda directory in the current directory; this has all the correct dependecies (compiled in Amazon Linux, the same platform utilized by AWS Lambda) and it has the base code to work from.
* In case you would like to add to more dependencies to the function, spin up an Amazon Linux instance and clone/fetch all the code there.

### Step 2: Writing the Function

In order to generate the boiler plate for the Lambda code, run the script lambda-generation.sh inside a Amazon Linux EC2 instance. 
The folder that contains all the content for the lambda is in the `tasc-lambda-boilerplate` folder.
All of the code for the lambda in lambda_function.py is the code that will be run by AWS Lambda. All the code will be encapsulated in the `def lambda_handler(event, context):`. The `event` variable will contain the payload that can be passed in by the user. This is represented as a dictionary/map in Python so you can use that accordingly.

If you have any questions about this, don't hesitate to contact the owners of the repo.

### Step 3: Deploying the Function

Once you are done writing the function, you can get ready to upload it to AWS. We will be using the AWS CLI moving forward, as much as possible.
*  Start by creating a function if you haven't already by running the following command:
`aws lambda create-function --function-name <FUNCTION NAME> --cli-binary-format raw-in-base64-out`
* Zip up the function code you have just created by running the following command:
`zip -r <ZIP FILE> <DIR WITH LAMBDA CODE>/*`.
Make sure to not include the parent directory in the zip file.
* Upload the zip folder to S3 (since the lambdas are over 50 MB, the limit for uploading the zip file directly using the update-function-code functionality):
`aws s3 cp <ZIP FILE> <S3 BUCKET>`
* Upload the zip file to the Lambda function, by running the following command:
`aws lambda update-function-code --function-name <FUNCTION NAME> --s3-bucket <S3 BUCKET> --zip-file <PATH to FILE>`. Alternatively, you can upload the zip from S3 from the AWS Dashboard.

### Step 4: Invoking the Function

You can invoke the function by running the following command:
`aws lambda invoke --function-name <FUNCTION NAME> --payload '{ KEY1:VALUE1 }' response.json`

If you have any print statements in your code, you can look at the data by referencing the logs in  Amazon CloudWatch.