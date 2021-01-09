#!/usr/bin/env bash
set -eo pipefail
export TOPIC="${TOPIC:=delete_gcp_resources}"


function deploy_function(){
  echo "Deploying cloud function"
  gcloud functions deploy delete_gcp_resources \
    --runtime="python38"  \
    --trigger-topic="${TOPIC}" \
    --timeout="500s"
}

function create_topic(){
  echo "Creating Pub/Sub topic"
  gcloud pubsub topics create "${TOPIC}"
}

function create_schedule(){
  echo "Creating cloud scheduler job"
  gcloud scheduler jobs create pubsub delete_gcp_resources \
    --schedule="0 2 * * *" \
    --topic="${TOPIC}"
    --message-body="trigger"
}

CMD=$1
case $CMD in
  function)
    deploy_function
    ;;
  schedule)
    create_schedule
    ;;
  pubsub)
    create_topic
    ;;
  *)
    create_topic
    deploy_function
    create_schedule
esac
