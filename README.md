# Cats Love Money 🐈‍⬛
Set of scripts to terminate various GCP resources to save cash and cats.

Currently we support deleting:
- Cloud Composer instances
- GKE clusters
- Cloud Compute instances
- Cloud Compute disks
- Cloud Dataproc clusters

The script by default deletes all resources older than one day.
If you want to exclude the resource from being deleted you need to set
a `please-do-not-kill-me` label on it.

## Usage

### Triggering manually
To use this tool manually do:
```
pip install -r requirements.txt
python main.py
```

### Scheduling on GCP

You may consider deploying this script as a cloud function that will be then
triggered on schedule using cloud scheduler. To do this execute:

```bash
TOPIC="delete_gcp_resources"
gcloud pubsub topics create "${TOPIC}"
gcloud functions deploy delete_gcp_resources --runtime=python38  --trigger-topic="${TOPIC}" --timeout=500s
gcloud scheduler jobs create http delete_gcp_resources --schedule="0 2 * * *" --topic="${TOPIC}"
```

We are using Pub/Sub instead of http trigger as cloud workflows seems to have some hard times with permissions
when invoking cloud functions.
