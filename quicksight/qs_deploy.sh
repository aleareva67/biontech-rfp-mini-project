#!/bin/bash
# Deploy or deletion of quicksight resources
set -e


action=$1
username=$2

if [ -z "$AWS_ACCOUNT_ID" ]; then
  echo "AWS_ACCOUNT_ID environment variable is missing!"
  echo "Usage: $0 <create|delete> <username>"
  exit 1
fi

if [ -z "$action" ]; then
  echo "Please provide a either 'create' or 'delete' as a valida action!"
  echo "Usage: $0 <create|delete> <username>"
  exit 1
fi

if [ -z "$username" ]; then
    echo "Please provide a quicksight registered uername for creation of resources."
    echo "Usage: $0 <create|delete> <username>"
    exit 1
fi


if [ "$action" = "create" ]; then

  echo "=== CREATING QUICKSIGHT RESOURCES==="
  mkdir -p out

  # Create Group
  echo "Creating Group BioNTechMiniProjectDemo... "
  aws quicksight create-group --aws-account-id=$AWS_ACCOUNT_ID --namespace=default --group-name="BioNTechMiniProjectDemo" --description="BioNTechMiniProjectDemo"  > out/out_group.json

  # Create membership
  echo "Creating Memeber $username in Group BioNTechMiniProjectDemo... "
  aws quicksight create-group-membership --aws-account-id=$AWS_ACCOUNT_ID --namespace=default --group-name="BioNTechMiniProjectDemo" --member-name="$username" > out/out_user-membership.json
  
  # Obtain principal quicksight ARN from member and creating Datasource
  echo "Creating Datasource biontech-demo-covid19-ds..."
  principal=$(cat out/out_user-membership.json | jq -r .GroupMember.Arn | tr -d '"')
  principal_escaped=$(printf '%s\n' "$principal" | sed -e 's/[\/&]/\\&/g')
  sed -e "s/__user_arn__/$principal_escaped/g" datasource.json > out/in_datasource.json
  
  aws quicksight create-data-source --aws-account-id $AWS_ACCOUNT_ID --cli-input-json file://./out/in_datasource.json > out/out_datasource.json
  
  # Obtain Datasource ARN and creating Dataset
  echo "Creating Dataset biontech-demo-covid19-dset..."
  datasource_arn=$(cat out/out_datasource.json | jq -r .Arn | tr -d '"')
  datasource_arn_escaped=$(printf '%s\n' "$datasource_arn" | sed -e 's/[\/&]/\\&/g')
  sed -e "s/__datasource_arn__/$datasource_arn_escaped/g" dataset.json > out/in_dataset.json
  sed -i -e "s/__user_arn__/$principal_escaped/g" out/in_dataset.json
  
  aws quicksight create-data-set --aws-account-id $AWS_ACCOUNT_ID --cli-input-json file://./out/in_dataset.json > out/out_dataset.json

  echo "Creating Theme biontech-demo-covid19-thm..."
  sed -e "s/__user_arn__/$principal_escaped/g" theme.json > out/in_theme.json
  aws quicksight create-theme --aws-account-id $AWS_ACCOUNT_ID --cli-input-json file://./out/in_theme.json > out/out_theme.json

  echo "Creating Dashboard biontech-demo-covid19-dashboard..."
  dataset_arn=$(cat out/out_dataset.json | jq -r .Arn | tr -d '"')
  dataset_arn_escaped=$(printf '%s\n' "$dataset_arn" | sed -e 's/[\/&]/\\&/g')
  theme_arn=$(cat out/out_theme.json | jq -r .Arn | tr -d '"')
  theme_arn_escaped=$(printf '%s\n' "$theme_arn" | sed -e 's/[\/&]/\\&/g')
  sed -e "s/__dataset_arn__/$dataset_arn_escaped/g" dashboard_definition.json > out/in_dashboard_definition.json
  sed -i -e "s/__theme_arn__/$theme_arn_escaped/g" out/in_dashboard_definition.json
  sed -i -e "s/__user_arn__/$principal_escaped/g" out/in_dashboard_definition.json
  aws quicksight create-dashboard --aws-account-id $AWS_ACCOUNT_ID --cli-input-json file://./out/in_dashboard_definition.json > out/out_dashboard_definition.json
  
  echo "CREATION SUCCESSFUL!"
  
elif [ "$action" = "delete" ]; then

  echo "===== DELETING QUICKSIGHT RESOURCES ====="

  # Delete Dashboard
  aws quicksight delete-dashboard --aws-account-id=$AWS_ACCOUNT_ID --dashboard-id="biontech-demo-covid19-dashboard" || true
  
  # Delete Theme
  aws quicksight delete-theme --aws-account-id=$AWS_ACCOUNT_ID --theme-id="biontech-demo-covid19-thm" || true

  # Delete Datasource
  aws quicksight delete-data-set --aws-account-id=$AWS_ACCOUNT_ID --data-set-id="biontech-demo-covid19-dset" || true

  # Delete Datasource
  aws quicksight delete-data-source --aws-account-id=$AWS_ACCOUNT_ID --data-source-id="biontech-demo-covid19-ds" || true

  # Delete Membership
  aws quicksight delete-group-membership --aws-account-id=$AWS_ACCOUNT_ID --namespace=default --group-name="BioNTechMiniProjectDemo" --member-name="$username" || true

  # Delete Group
  aws quicksight delete-group --aws-account-id=$AWS_ACCOUNT_ID --namespace=default --group-name="BioNTechMiniProjectDemo" || true
  
  echo "DELETION SUCCESSFUL!"

else

  echo "Please provide a either 'create' or 'delete' as a valida action instead of $action!"
  echo "Usage: $0 <create|delete> <username>"
  exit 1

fi