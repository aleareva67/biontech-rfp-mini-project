set -e

## Parameters for Mini Project
action=$1
aws_access_key=$2
aws_secret_key=$3

if [ -z "$action" ]; then
  echo "Please provide a either 'create' or 'delete' as a valida action!"
  echo "Usage for creation: $0 create <aws_access_key> <aws_secret_key>"
  echo "Usage for deletion: $0 delete"
  exit 1
fi


if [ "$action" = "create" ]; then

  if [ -z "$aws_access_key" ] || [ -z "$aws_secret_key" ]; then
    echo "Please provide the AWS Access Key and AWS Secret Key for the source bucket, Note: that these keys are to connect to source bucket"
    echo "Usage for creation: $0 create <aws_access_key> <aws_secret_key>"
    echo "Usage for deletion: $0 delete"
    exit 1
  fi
  echo "===CREATING PARAMETERS===."

  ## Source Access keys
  aws ssm put-parameter --cli-input-json "{
    \"Name\": \"/biontech/etl-demo/source/access_key\",
    \"Value\": \"$aws_access_key\",
    \"Description\": \"AWS Access Key to access source public bucket.\",
    \"Type\": \"SecureString\"
  }"

  aws ssm put-parameter --cli-input-json "{
    \"Name\": \"/biontech/etl-demo/source/secret_key\",
    \"Value\": \"$aws_secret_key\",
    \"Description\": \"AWS Access Secret to access source public bucket.\",
    \"Type\": \"SecureString\"
  }"

  aws ssm put-parameter --cli-input-json '{
    "Name": "/biontech/etl-demo/source/region",
    "Value": "eu-central-1",
    "Description": "AWS Account region for source bucket.",
    "Type": "String"
  }'

  ## Source Bucket Params
  aws ssm put-parameter --cli-input-json '{
    "Name": "/biontech/etl-demo/source/s3/name",
    "Value": "rfp-public-demo-data",
    "Description": "Name of public bucket.",
    "Type": "String"
  }'

  aws ssm put-parameter --cli-input-json '{
    "Name": "/biontech/etl-demo/destination/s3/name",
    "Value": "biontechetldemocovid19",
    "Description": "Name of desteination bucket.",
    "Type": "String"
  }'
  
  echo "CREATION FINISHED SUCCESSFULLY!"

elif [ "$action" = "delete" ]; then

  echo "===== DELETING PARAMETERS ====="
  aws ssm delete-parameter --name /biontech/etl-demo/source/access_key || true
  aws ssm delete-parameter --name /biontech/etl-demo/source/secret_key || true
  aws ssm delete-parameter --name /biontech/etl-demo/source/region || true
  aws ssm delete-parameter --name /biontech/etl-demo/source/s3/name || true
  aws ssm delete-parameter --name /biontech/etl-demo/destination/s3/name || true
  
  echo "DELETION FINISHED SUCCESSFULLY!"

else

  echo "Please provide a either 'create' or 'delete' as a valida action instead of $action!"
  echo "Usage for creation: $0 create <aws_access_key> <aws_secret_key>"
  echo "Usage for deletion: $0 delete"
  exit 1

fi
