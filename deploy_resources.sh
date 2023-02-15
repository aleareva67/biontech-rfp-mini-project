set -e

action=$1

# Clean-up
rm -rf ./libs/
rm -f deps.zip


if [ -z "$action" ]; then
  echo "Please specify the proper action 'create' or 'delete'"
  echo "Usage: $0 <create|delete>"
  exit 1
fi

if [ "$action" = "create" ]; then

  echo "DEPLOYING..."
  cdk synth
  cdk deploy --require-approval never --all

  echo "BUILDING RESOURCES..."
  # For simplicity and speed we assume all the glue jobs share the same requirements
  mkdir libs
  pip install -r glue/requirements.txt -t ./libs
  cp -r glue/extra_libs/* libs/
  cd libs; zip -r ../deps.zip *; cd ..


  echo "COPYING RESOURCES..."
  # Copying python dependecies
  aws s3 cp deps.zip s3://biontechetldemocovid19/glue/deps.zip

  # Copying scripts
  aws s3 cp --recursive glue/jobs s3://biontechetldemocovid19/glue/jobs/

  # Copying external jars
  aws s3 cp --recursive glue/jars s3://biontechetldemocovid19/glue/jars/

  # Clean-up
  rm -rf ./libs/
  rm deps.zip

  echo "DEPLOY FINISHED SUCCESSFULLY!"

elif [ "$action" = "delete" ]; then

  echo "Cleaning bucket..."
  aws s3 rm s3://biontechetldemocovid19 --recursive || true

  echo "Deleting stack with cdk destroy..."
  cdk destroy --force || true

else

  echo "Please specify the proper action 'create' or 'delete'"
  echo "Usage: $0 <create|delete>"
  exit 1

fi

