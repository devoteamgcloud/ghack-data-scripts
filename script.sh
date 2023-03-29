gcloud functions deploy python-finalize-function \                                                 
--gen2 \
--runtime=python311 \
--region=europe-north1 \
--source=. \
--entry-point=hello_gcs \
--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
--trigger-event-filters="bucket=BUCKET" \
--project=PROJECT \
--memory=2048Mi