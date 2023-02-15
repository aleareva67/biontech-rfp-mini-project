"""
Main application for cdk deploy.
"""

import os
import logging
from enum import Enum

from aws_cdk import (
    core,
    aws_lambda as _lambda,
    aws_lambda_python as _lambda_py,
    aws_stepfunctions as _sfn,
    aws_stepfunctions_tasks as _tasks,
    aws_s3 as _s3,
    aws_glue as _glue,
    aws_iam as _iam
)

# Initializing logger
logging.basicConfig()
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("MoveToRaw-fx")


class BionTechDemo(core.Stack):
    """
        Class extending a single stack that
    """

    def __init__(self, scope, id, common_tags, **kwargs):
        super().__init__(scope, id,
                         env=core.Environment(account=os.environ.get("AWSACCOUNTID"),
                                              region=os.environ.get("AWS_DEFAULT_REGION", "eu-west-1")),
                         **kwargs)

        self.common_tags = common_tags
        self.main_bucket_name = "biontechetldemocovid19"
        self.main_iam_roles = self.create_main_iam_roles("biontechetldemomainrole")

    def set_resource_tags(self, resource):
        """
            Puts the common common_tags of the stack to the resource.
        :param resource: AWS resource object where common_tags will be placed
        :return:
        """
        for key, value in self.common_tags.items():
            core.Tags.of(resource).add(key, value)

    def start(self):
        logger.info("Starting stack creation")

        # S3 Bucket
        bucket = _s3.Bucket(
            self,
            'biontech_etl_demo_covid19_s3',
            bucket_name=self.main_bucket_name,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            public_read_access=False,
            versioned=False,
            removal_policy=core.RemovalPolicy.DESTROY
        )
        self.set_resource_tags(bucket)

        # Glue Database
        database = _glue.Database(
            self,
            id='biontech_etl_demo_covid19_db',
            database_name=self.main_bucket_name
        )
        self.set_resource_tags(database)

        # Move to raw step
        move_to_raw_lambda_conf = {
            "description": "Lambda for ingesting files from source bucket to raw layer.",
            "entry": "lambdas/move_to_raw"
        }

        move_to_raw_task = self.make_lambda_function_task(
            "move_to_raw",
            move_to_raw_lambda_conf
        )

        # Glue jobs for cleaning
        glue_task_args = {"--ingestion_date.$": "$.ingestion_date"}

        raw_to_clean_glue_variants_conf = {
            "enable_bookmark": "conf-bookmark-disable",
            "extra_jars": "deequ-2.0.2-spark-3.3.jar",
            "timeout": 10,
            "glue_version": "4.0",
            "num_workers": 10,
            "worker_type": "G.1X",
            "initial_args":  {
                "additional-python-modules": "pycountry==22.3.5,pydeequ==1.0.1"
            }
        }
        raw_to_clean_glue_variants = self.make_glue_job_task("raw_to_clean_variants",
                                                             raw_to_clean_glue_variants_conf,
                                                             self.main_bucket_name,
                                                             glue_task_args)

        raw_to_clean_glue_indicators_conf = {
            "enable_bookmark": "conf-bookmark-disable",
            "extra_jars": "spark-excel_2.12-0.14.0.jar,poi-ooxml-schemas-4.0.0.jar,"
                          "xmlbeans-3.1.0.jar,spoiwo_2.12-1.8.0.jar,excel-streaming-reader-2.3.6.jar,poi-4.1.2.jar,"
                          "poi-ooxml-4.1.2.jar,poi-shared-strings-1.0.4.jar,deequ-2.0.2-spark-3.3.jar",
            "timeout": 10,
            "glue_version": "4.0",
            "num_workers": 10,
            "worker_type": "G.1X",
            "initial_args":  {
                "additional-python-modules": "pycountry==22.3.5,pydeequ==1.0.1"
            }
        }
        raw_to_clean_glue_indicators = self.make_glue_job_task("raw_to_clean_indicators",
                                                               raw_to_clean_glue_indicators_conf,
                                                               self.main_bucket_name,
                                                               glue_task_args)

        # Glue jobs for curation
        clean_to_curated_glue_variants_conf = {
            "enable_bookmark": "conf-bookmark-disable",
            "extra_jars": "deequ-2.0.2-spark-3.3.jar",
            "timeout": 10,
            "glue_version": "4.0",
            "num_workers": 10,
            "worker_type": "G.1X",
            "initial_args":  {
                "additional-python-modules": "pycountry==22.3.5,pydeequ==1.0.1"
            }
        }
        clean_to_curated_glue_variants = self.make_glue_job_task("clean_to_curated_variants_indicators",
                                                                 clean_to_curated_glue_variants_conf,
                                                                 self.main_bucket_name,
                                                                 glue_task_args)

        #clean_to_curated_glue_indicators_conf = {
        #    "enable_bookmark": "conf-bookmark-disable",
        #    "extra_jars": "deequ-2.0.2-spark-3.3.jar",
        #    "timeout": 10,
        #    "glue_version": "4.0",
        #    "num_workers": 10,
        #    "worker_type": "G.1X"
        #}
        #clean_to_curated_glue_indicators = self.make_glue_job_task("clean_to_curated_indicators",
        #                                                           clean_to_curated_glue_indicators_conf,
        #                                                           self.main_bucket_name,
        #                                                           glue_task_args)

        # Glue jobs for analysis
        analysis_glue_result_conf = {
            "enable_bookmark": "conf-bookmark-disable",
            "extra_jars": "deequ-2.0.2-spark-3.3.jar",
            "timeout": 10,
            "glue_version": "4.0",
            "num_workers": 10,
            "worker_type": "G.1X"
        }
        analysis_glue_result = self.make_glue_job_task("analysis_variants_and_indicators",
                                                       analysis_glue_result_conf,
                                                       self.main_bucket_name,
                                                       glue_task_args)

        # Defining tasks that can run in parallel
        parallel_raw_to_clean = _sfn.Parallel(self, "Raw to Clean", result_path=_sfn.JsonPath.DISCARD) \
            .branch(raw_to_clean_glue_variants) \
            .branch(raw_to_clean_glue_indicators)

        #parallel_clean_to_curated = _sfn.Parallel(self, "Clean to Curated", result_path=_sfn.JsonPath.DISCARD) \
        #    .branch(clean_to_curated_glue_variants) \
        #    .branch(clean_to_curated_glue_indicators)

        # Creating pipeline definition (to raw -> to clean -> to curated -> analysis)
        pipeline_definition = move_to_raw_task \
            .next(parallel_raw_to_clean) \
            .next(clean_to_curated_glue_variants) \
            .next(analysis_glue_result)

        # Define the Step Function
        step_function = _sfn.StateMachine(
            self, "sf-biontech-etl-demo",
            state_machine_name=f"{self.main_bucket_name}",
            definition=pipeline_definition,
            role=self.main_iam_roles[Resources.STEP_FUNCTIONS.value]
        )
        self.set_resource_tags(step_function)

    def make_lambda_function_task(self, name, conf) -> _tasks.LambdaInvoke:
        """
            Creates a Lambda function and the respective Step function Task.
        :param name: str - Name of lambda, that will also define the name of the step function task.
        :param conf: dict - configuration to be applied to lambda function.
        :return: Step function task invoking the created lambda.
        """
        logger.info(f"Creating Lambda FX: {name}")

        # Create lambda
        lambdafx = _lambda_py.PythonFunction(
            self,
            name,
            runtime=_lambda.Runtime.PYTHON_3_7,
            memory_size=conf.get("memory_size", 1028),
            description=conf.get("description"),
            function_name=name,
            timeout=core.Duration.seconds(conf.get("timeout", 900)),
            entry=conf.get("entry"),
            index=conf.get("index", "lambda_function.py"),
            handler=conf.get("handler", "handler"),
            environment=conf.get("environment"),
            role=self.main_iam_roles[Resources.LAMBDA.value]
        )

        # Tag the lambda functions
        self.set_resource_tags(lambdafx)

        # Create invocation task
        task = _tasks.LambdaInvoke(
            self,
            f"Lambda: {name}",
            timeout=core.Duration.seconds(conf.get("timeout", 900)),
            payload_response_only=True,
            lambda_function=lambdafx
        )

        return task

    def make_glue_job_task(self, name, conf, bucket_name, glue_task_args) -> _tasks.GlueStartJobRun:
        """
            Creates a Glue job and the respective Step function tasks that invokes it.
        :param name: str - Name of the glue job
        :param conf: dict - Configuration parameters
        :param bucket_name: - Name of bucket where glue resources will be present.
        :param glue_task_args: arguments to be passed to glue job task from step function.
        :return: Step function tasks that invokes the glue job
        """

        logger.info(f"Creating Glue job: {name}")

        self.create_glue_job(name, conf, bucket_name=bucket_name)

        step_arguments = _sfn.TaskInput.from_object(glue_task_args)

        task = _tasks.GlueStartJobRun(
            self,
            f'Glue Job: {name}',
            glue_job_name=f'{name}',
            integration_pattern=_sfn.IntegrationPattern.RUN_JOB,
            result_path=_sfn.JsonPath.DISCARD,
            arguments=step_arguments
        )
        return task

    def create_glue_job(self, name, conf, bucket_name) -> _glue.CfnJob:
        bookmark_property = conf.get(
            "enable_bookmark", "conf-bookmark-disable"
        )

        # Extra jars
        extra_jars = conf.get('extra_jars', '')
        if extra_jars:
            new_extra_jars = ""

            for jar in extra_jars.split(","):
                new_extra_jars = new_extra_jars + f",s3://{bucket_name}/glue/jars/{jar}"

            if new_extra_jars:
                new_extra_jars = new_extra_jars[1:]

            extra_jars = new_extra_jars
            
        # Jars
        jars = conf.get('jars', '')
        if jars:
            new_jars = ""

            for jar in jars.split(","):
                new_jars = new_jars + f",s3://{bucket_name}/glue/jars/{jar}"

            if new_jars:
                new_jars = new_jars[1:]

            jars = new_jars

        # Extra initial arguments
        init_args = conf.get("initial_args", {})
        if init_args:
            new_init_args = {}
            for arg, value in init_args.items():
                new_init_args[f"--{arg}"] = value
            init_args = new_init_args

        script_location_input = f"s3://{bucket_name}/glue/jobs/{name}/glue_main.py"

        logger.info(f"script_location_input: {script_location_input}")

        default_arguments = {
            "--extra-py-files": f"s3://{bucket_name}/glue/deps.zip",
            "--enable-glue-datacatalog": "",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--conf-bookmark-option": bookmark_property,
            "--BUCKET_NAME": self.main_bucket_name,
            **init_args
        }

        if extra_jars:
            default_arguments["--extra-jars"] = extra_jars
            
        if jars:
            default_arguments["--jars"] = jars

        # Glue Job Definition: glue etl
        return _glue.CfnJob(
            self,
            name,
            name=name,
            tags=self.common_tags,
            command=_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_location_input,
            ),
            default_arguments=default_arguments,
            timeout=int(conf.get("timeout", 20)),
            glue_version=conf.get("glue_version", "2.0"),
            number_of_workers=int(conf.get("num_workers", 10)),
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=int(conf.get("max_concurrent_runs", 1))),
            worker_type=conf.get("worker_type", "G.1X"),
            role=self.main_iam_roles[Resources.GLUE.value].role_arn
        )

    def create_main_iam_roles(self, base_name) -> dict:
        # Define the IAM role for the Glue job
        glue_role = self.create_glue_role(base_name)
        self.set_resource_tags(glue_role)

        # Define IAM role for the Step functions
        sf_role = self.create_sf_role(base_name)
        self.set_resource_tags(sf_role)

        # Define IAM role for the Step functions
        lambda_role = self.create_lambda_role(base_name)
        self.set_resource_tags(lambda_role)

        # Attach common policies
        self.add_common_policies(lambda_role)

        return {
            Resources.GLUE.value: glue_role,
            Resources.LAMBDA.value: lambda_role,
            Resources.STEP_FUNCTIONS.value: sf_role
        }

    def create_lambda_role(self, base_name):
        lambda_role = _iam.Role(self, f"{base_name}-lambdas",
                                assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"))
        # Attach policies for Lambda access
        lambda_role.add_to_policy(_iam.PolicyStatement(
            actions=["lambda:*"],
            resources=["*"]
        ))
        return lambda_role

    def create_sf_role(self, base_name):
        sf_role = _iam.Role(self, f"{base_name}-states",
                            assumed_by=_iam.ServicePrincipal("states.amazonaws.com"))
        # Attach policies for Step Functions
        sf_role.add_to_policy(_iam.PolicyStatement(
            actions=["states:*"],
            resources=["*"]
        ))
        # Attach common policies
        self.add_common_policies(sf_role)
        return sf_role

    def create_glue_role(self, base_name):
        glue_role = _iam.Role(self, f"{base_name}-glue",
                              assumed_by=_iam.ServicePrincipal("glue.amazonaws.com"))
        # Attach policies for Glue services
        glue_role.add_to_policy(_iam.PolicyStatement(
            actions=["glue:*"],
            resources=["*"]
        ))
        # Attach policies for Glue services
        glue_role.add_to_policy(_iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=["arn:aws:iam::*:role/biontech-etl-demo*"]
        ))
        # Attach common policies
        self.add_common_policies(glue_role)
        return glue_role

    def add_common_policies(self, role):

        # Attach policies for S3 bucket access
        self.add_s3_policies(role)
        
        # Attach policies for CloudWatch logging
        self.add_logging_policies(role)
        
        # Attach SSM Parameter store reading 
        self.add_ssm_policies(role)

    def add_logging_policies(self, role):
        role.add_to_policy(_iam.PolicyStatement(
            actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources=["*"]
        ))
        role.add_to_policy(_iam.PolicyStatement(
            actions=["cloudwatch:*"],
            resources=["*"]
        ))

    def add_s3_policies(self, role):
        role.add_to_policy(_iam.PolicyStatement(
            actions=["s3:*"],
            resources=[
                f"arn:aws:s3:::{self.main_bucket_name}",
                f"arn:aws:s3:::{self.main_bucket_name}/*"
                ]
        ))
        
    def add_ssm_policies(self, role):
        role.add_to_policy(_iam.PolicyStatement(
            actions=["ssm:GetParameter"],
            resources=[f"arn:aws:ssm:eu-west-1:*:parameter/biontech/*"]
        ))


class Resources(Enum):
    """
        Supported functions for evaluation in parameters string.
    """
    STEP_FUNCTIONS = "sf"
    GLUE = "glue"
    LAMBDA = "lambda"


def main():
    # Creating app
    app = core.App()

    # Creating stack
    stack = BionTechDemo(
        app,
        "biontech-etl-demo",
        {
            "app": "biontech-etl-demo"
        },
        description="Stack for ETL for Ingestion and Curation of Covid19 data"
    )

    # Building stack
    stack.start()

    # Synthesize stack
    app.synth()


if __name__ == "__main__":
    main()
