# biontech-rfp-mini-project

Copyright (c) 2023 Capgemini

This is a short guide on how to deploy and test the "BioNTech RFP Practical mini-project"


## Requirements

1. A valid AWS Account where all the resources can be deploy
2. Valid credentials (AWS_ACCOUNT_ID, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) for a user that contains the policies described [here](#example-of-iam-policy-for-deployment-user-role).
3. CDK must have been [bootstrapped](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html) in the target AWS Account.
   - If this is not the case you can execute within the provided image the command.
      ```cdk bootstrap```
4. In case of quicksight deployment: A valid user that is registered as a quicksight admin user, ideally the user have full access to quicsight resources as described [here](#example-of-iam-policy-for-quicksight-user-role).


## Usage
    
1. Execute the build of the docker image with:
   - Additionally the image is publicly available as [capoctemp/biontech-mini-project:latest](https://hub.docker.com/layers/capoctemp/biontech-mini-project/latest/images/sha256-67825a1c434e050f1146fc6108cd6fb021709636b5269fc7612cea1cdda7f451?tab=layers)
       ```docker build -t biontech-mini-project .``` 
2. Start the image specifying as environment variables the  credentials of a valid AWS user present in the TARGET account.
   - Note that since the Python CDK uses the PythonFunction, it is needed to execute the image in privilage mode
       ```
        docker run -it --privileged  \
            --entrypoint /bin/bash \
            -e AWS_ACCOUNT_ID=<TARGET AWS ACCOUNT ID> \
            -e AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID> \
            -e AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> \
            -e AWS_REGION=<TARGET AWS REGION> \
            test biontech-mini-project
        ```
3. The executing image should provides the required environment for the execution of the deployment, once within the image you can execute the deployment script with the following command:
   - Note that specifying `--quicksight <quicksight user>` is optional, if specified dashboard resources will be deployed.
   - Unfortunately the PythonFunction construct is not much optimized to run in this container, so it will take long and use a lot of space (~20GB)
       ```sh startup.sh create --quicksight valid_quicksight_user```
   - The command will request the credentials for the account containing the public source bucket so they can be properly stored and encrypted as parameters in the SSM Parameter Store..
4. This will deploy all the resources for the given solution in the target AWS account described in the section below.
5. To clean up the account and delete all the resources, execute the following command:
    ```sh startup.sh delete --quicksight valid_quicksight_user```
   - Take into consideration that the deletion of resources is done on best effort approach, this means the deletion will continue despite any error, in case of resources that were already manually deleted.

## List of resources deployed
1. Stack `biontech-etl-demo` that includes
   - 1 Bucket `biontechetldemocovid19`
   - 1 Step function `biontechetldemocovid19`
   - 1 Lambda function
   - 3 Glue jobs
   - 1 Glue Database `biontechetldemocovid19`
   - 1 IAM Role for each type of resource
2. 5 Parameters in SSM Parameter Store with prefixes `/biontech/etl-demo/`.
3. Quicksight resources
   - Data Source
   - Data Set
   - Theme 
   - Dashboard
   - User Group
   - User Membership

## Example of IAM Policy for Deployment User Role

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "CloudFormationPermissions",
            "Effect": "Allow",
            "Action": "cloudformation:*",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "IAMPermissions",
            "Effect": "Allow",
            "Action": [
                "iam:GetRole",
                "iam:PassRole",
                "iam:UpdateRoleDescription",
                "iam:DeletePolicy",
                "iam:CreateRole",
                "iam:DeleteRole",
                "iam:AttachRolePolicy",
                "iam:PutRolePolicy",
                "iam:CreatePolicy",
                "iam:CreateServiceLinkedRole",
                "iam:DetachRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:UpdateRole",
                "iam:DeleteServiceLinkedRole",
                "iam:CreatePolicyVersion"
            ],
            "Resource": [
                "arn:aws:iam::*:policy/*",
                "arn:aws:iam::*:role/*"
            ]
        },
        {
            "Sid": "QuicksightPermissions",
            "Effect": "Allow",
            "Action": "quicksight:*",
            "Resource": [
                "arn:aws:quicksight:eu-west-1:*:dashboard/*",
                "arn:aws:quicksight:eu-west-1:*:group/*",
                "arn:aws:quicksight:eu-west-1:*:user/*",
                "arn:aws:quicksight:eu-west-1:*:datasource/*",
                "arn:aws:quicksight:eu-west-1:*:dataset/*",
                "arn:aws:quicksight:eu-west-1:*:theme/*"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "SSMParameterStoreReadPermissions",
            "Effect": "Allow",
            "Action": "ssm:GetParameter",
            "Resource": "arn:aws:ssm:eu-west-1:*:parameter/cdk-bootstrap/*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "SSMParameterStoreWritePermissions",
            "Effect": "Allow",
            "Action": "ssm:PutParameter",
            "Resource": "arn:aws:ssm:eu-west-1:*:parameter/*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "SSMParameterStoreDeletePermissions",
            "Effect": "Allow",
            "Action": "ssm:DeleteParameter",
            "Resource": "arn:aws:ssm:eu-west-1:*:parameter/biontech/etl-demo/*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "S3Permissions",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": [
                "arn:aws:s3:::cdktoolkit-stagingbucket-*",
                "arn:aws:s3:::cdk-*",
                "arn:aws:s3:::biontechetldemocovid19/*",
                "arn:aws:s3:::biontechetldemocovid19"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                }
            }
        },
        {
            "Sid": "CloudFormationInvocationPermissions",
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:RequestedRegion": "eu-west-1"
                },
                "ForAnyValue:StringEquals": {
                    "aws:CalledVia": "cloudformation.amazonaws.com"
                }
            }
        }
    ]
}
```

## Example of IAM Policy for Quicksight User Role
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "quicksight:*"
            ],
            "Resource": "*"
        }
    ]
}
```
