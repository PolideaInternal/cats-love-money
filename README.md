# Cats Love Money 🐈‍⬛
Set of scripts to terminate various GCP resources to save cash and cats.

Currently we support deleting:
- Cloud Composer instances
- Cloud Compute instances
- Cloud Compute disks
- Cloud Dataproc clusters

The script by default deletes all resources older than one day.
If you want to exclude the resource from being deleted you need to set
a `please-do-not-kill-me` label on it.

## Usage

To use this tool do:
```
pip install -r requirements.txt
python -r clean_all.py
```

And that's all!
