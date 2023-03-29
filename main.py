import json
import functions_framework
from google.cloud import storage
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable

PROJECT_ID = "PROJECT_ID"
PUBSUB_TOPIC_NAME = "TOPIC_NAME"

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    bucket_name = data["bucket"]
    name = data["name"]

    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(name)
    content = blob.download_as_string()
    content = content.split(b'\n')
    objects = []
    for line in content:
        if len(line):
            objects.append(json.loads(line))
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_NAME)
    publish_futures = []
    
    def get_callback(
        publish_future: pubsub_v1.publisher.futures.Future, data: str
    ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
        def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
            try:
                # Wait 60 seconds for the publish call to succeed.
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")
        return callback

    for o in objects:
        data = json.dumps(o)
        # When you publish a message, the client returns a future.
        publish_future = publisher.publish(topic_path, data.encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(get_callback(publish_future, data))
        publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)