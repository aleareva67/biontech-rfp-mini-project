#!/bin/bash
## Startup scripts dependencies on AWS_ credentials avaialble in environment.
set -e

action=$1
quicksight=$2
qs_user=$3

if [ -z "$AWS_ACCOUNT_ID" ]; then
  echo "AWS_ACCOUNT_ID environment variable is missing!"
  echo "Usage: Specify the --quicksight <username> argument in case you want to activate the creation/deletion of the quicksight dashboards"
  echo "Usage: $0 <create|delete> [--quicksight username]"
  exit 1
fi

if [ -z "$action" ]; then
  echo "Please specify the proper action 'create' or 'delete'"
  echo "Usage: Specify the --quicksight <username> argument in case you want to activate the creation/deletion of the quicksight dashboards"
  echo "Usage: $0 <create|delete> [--quicksight username]"
  exit 1
fi

echo "ACTION: $action"
if [ "$action" = "create" ]; then
  echo -n "Please type the AWS_ACCESS_KEY of the public source bucket: "
  read -r aws_access_key
  echo

  echo -n "Please type the AWS_SECRET_ACCESS_KEY of the public source bucket: "
  read -r aws_secret_key
  echo

  echo "Creating necessary AWS Params in parameter store..."
  sh aws_params.sh create $aws_access_key $aws_secret_key

  echo "Deploying ETL resources..."
  sh deploy_resources.sh create

  # Quicksight deployment is optional
  if [ "$quicksight" = "--quicksight" ]; then

    if [ -z "$qs_user" ]; then
      echo "If you have activated --quicksight dashboard deployment please provide a valid quicksight user."
      echo "Usage: Specify the --quicksight <username> argument in case you want to activate the creation/deletion of the quicksight dashboards"
      echo "Usage: $0 <create|delete> [--quicksight username]"
      exit 1
    fi

    echo "Deploying Quicksight resources..."
    cd quicksight; sh qs_deploy.sh create "$qs_user"; cd ..
  else

    echo "IGNORING DEPLOYMENT OF QUICKSIGHT RESOURCES"
  fi


elif [ "$action" = "delete" ]; then

  echo "Deleting created parameters..."
  sh aws_params.sh delete

  echo "Deleting ETL resources..."
  sh deploy_resources.sh delete

  # Quicksight deployment is optional
  if [ "$quicksight" = "--quicksight" ]; then

    if [ -z "$qs_user" ]; then
      echo "If you have activated --quicksight dashboard deployment please provide a valid quicksight user."
      echo "Usage: Specify the --quicksight <username> argument in case you want to activate the creation/deletion of the quicksight dashboards"
      echo "Usage: $0 <create|delete> [--quicksight username]"
      exit 1
    fi

    echo "Deleting Quicksight resources..."
    cd quicksight; sh qs_deploy.sh delete $qs_user ; cd ..

  else

    echo "IGNORING DELETION OF QUICKSIGHT RESOURCES"

  fi

else

  echo "Please specify the proper action 'create' or 'delete'"
  echo "Usage: Specify the --quicksight <username> argument in case you want to activate the creation/deletion of the quicksight dashboards"
  echo "Usage: $0 <create|delete> [--quicksight username]"
  exit 1

fi
