
### Provision Spark Clusters and Run Jobs Right in Your Notebook

![Spark!](./img/jez-timms-spark.jpg)
Photo by Jez Timms on [Unsplash](https://unsplash.com/photos/_Ch_onWf38o)

We'll be using GCP SDKs to provision Spark clusters and run PySpark jobs on them -- all from a notebook. In fact, the source for this document IS a notebook. You can clone this demo here:

[]()

This will take between five and ten minutes. I'm so spoiled by the public cloud that I don't even really understand how crazy this level of convenience is.

In this activity we will need a few prerequisites. They are:

1. A project directory with a python virtual environment

```
mkdir sparkDemo && cd sparkDemo
python -m virtualenv venv

// windows
call venv/Scripts/activate

// else
source venv/bin/activate

pip install google-cloud-dataproc jupyterlab
```

2. A billing-enabled project on GCP.

+ [Create a project](https://console.cloud.google.com/iam-admin/projects)

+ [Enable billing](https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project)

+ Note the project ID. It's listed front and center on the [GCP home screen](https://console.cloud.google.com/home/) (make sure you have the right project open). Also feel free to edit in a nearby region from [this list](https://cloud.google.com/compute/docs/regions-zones/#locations)


```python
project_id = 'sparkdemo'
region = 'us-east1-b'
```

3. A service account, and the JSON file with its credentials

+ Go to the [IAM console](https://console.cloud.google.com/iam-admin) and go to Service Accounts (left menu). Create a new account and fill in the details sensibly. Give the service account `dataproc worker` permissions. Remember to save the credentials file as JSON when prompted. My habit is to save it as `client_secrets.json` and **then I immediately add that filename to my gitignore file.**

#### With those in hand...lets set up our client


```python
from google.cloud import dataproc_v1
```


```python
client = dataproc_v1.ClusterControllerClient.from_service_account_json(
    './client_secrets.json')
```

Above, we imported the `dataproc_v1` SDK, and we instantiated a client with the special `from_service_account_json` method.


```python
# A parser for google's responses

def response_parser(client_obj, verbose=False):
    responses = []
    for page in client_obj.pages:
        for element in page:
            responses.append(element)
           
    if not responses and verbose:
        print('No responses found!')
        
    if responses and verbose:
        for resp in responses:
            print(resp)
    
    return responses
        
clusters = client.list_clusters(project_id, 'global')
responses = response_parser(clusters);
len(responses)
```




    0



We are showing zero clusters provisioned. Let's provision a cluster.


```python
## A response handler that we can attach to cluster actions

def handler(ops):
    result = ops.result()
    
    # Uncomment this to see results from the calls in this notebook
    # (this can also help you learn config options)
    # print(result)
```

The important thing here is to create a `dataproc_v1.types.Cluster` instance.


```python
config={'gce_cluster_config':{'zone_uri':region}}
cluster_config = dataproc_v1.types.Cluster(
    project_id=project_id, cluster_name="democluster", config=config)
```

GCP uses sensible defaults to configure this instance, but you can use the printout from the callback handler to examine the config fields and override where appropriate. For example to scale up workers add the following to the `config`:

    'worker_config':{'num_instances':3}

Also note that the `region` is `global` at the call to `create_cluster`, and not in the `gce_cluster_config`.


```python
creating = client.create_cluster(project_id, 'global', cluster_config)
creating.add_done_callback(handler)
```

If you enabled printing in the callback, you can just wait for that to fire now. Once the cluster is up we can repeat our call to `list_clusters` and see that we have one now.


```python
clusters = client.list_clusters(project_id, 'global')

responses = response_parser(clusters);
len(responses)
```




    1



Hooray! 

#### Run a job

Now let's set up a job controller.


```python
jobber = dataproc_v1.JobControllerClient.from_service_account_json(
    './client_secrets.json'
)
```

We also need somewhere to put our job files


```python
from google.cloud import storage
import uuid

gs = storage.Client.from_service_account_json('./client_secrets.json')
bucket = gs.create_bucket(f'sparkdemo-{uuid.uuid1()}')
```

We can store our code in the bucket. If you have Spark installed, this would be the part where you could start prototyping in local mode. For now, just copy the following code into a file `main.py` and save it in the root of your project.

```
import pyspark
import sys

sc = pyspark.SparkContext()

# Dummy task
terms = ["fodder" for x in range(9000)] + ["spartan", "sparring", "sparrow"]
words = sc.parallelize(terms)
words.filter(lambda w: w.startswith("spar")).take(2)

# Output target was passed to args config field
bucket = sys.argv[1]
output_directory = 'gs://{}/pyspark_output'.format(bucket)

# For csv output
sqlContext = pyspark.SQLContext(sc)
df = sqlContext.createDataFrame(words, pyspark.sql.types.StringType())
df.coalesce(1).write.format('com.databricks.spark.csv'
            ).options(header='true').save(output_directory)
```

Here's a simple function that is barely modified from the boilerplate on the storage client library docs. We'll use it to upload files to our bucket, specifically our `main.py` file.


```python
def upload_blob(bucket, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))
    
# Use the helper function to upload our code
upload_blob(bucket, './main.py', "input_files/main.py")
```

    File ./main.py uploaded to input_files/main.py.
    

Now that our code is in place, we need to make an instance of `dataproc_v1.types.PySparkJob`


```python
pyspark_job = dataproc_v1.types.PySparkJob(
    main_python_file_uri=f"gs://{bucket.name}/input_files/main.py",
    args={bucket.name}
)
```

`main_python_file_uri` points to our code. We could also pass `python_file_uris` and `file_uris` if we had more supporting files to send, but we don't. 

Our `main.py` script uses `sys.argv[1]` to construct an output directory in our bucket, so we pass that to `args`.

We also need to place the job in our cluster:


```python
placement = dataproc_v1.types.JobPlacement(cluster_name="democluster")
```

We're ready to submit a job.


```python
job = jobber.submit_job(project_id, 'global', {'placement': placement,
                                               'pyspark_job':pyspark_job})
```

After a minute or so the job finishes. We can check on it like so:


```python
job_status = jobber.get_job(project_id, 'global', job.job_uuid)
job_status.status
```




    state: RUNNING
    state_start_time {
      seconds: 1563547231
      nanos: 583000000
    }




```python
job_status = jobber.get_job(project_id, 'global', job.job_uuid)
job_status.status
```




    state: DONE
    state_start_time {
      seconds: 1563547265
      nanos: 793000000
    }



Jobs Done!

![We did it!](https://media.giphy.com/media/3o84U9arAYRM73AIvu/giphy.gif)

Let's download contents from our bucket.


```python
import os

try:
    os.mkdir('output')
except FileExistsError as e:
    pass

for page in bucket.list_blobs().pages:
    for element in page:
        print(element.name)
        blob = bucket.blob(element.name)
        destination = element.name.split('/')[-1]
        if destination:
            blob.download_to_filename(f"./output/{destination}")
```

    input_files/main.py
    pyspark_output/
    pyspark_output/_SUCCESS
    pyspark_output/part-00000-df4302c1-aec3-415b-84d6-66bd915ae938-c000.csv
    

Our outputs are downloaded locally. We're done, so let's tear it all down.


```python
destroying = client.delete_cluster(project_id, 'global', "democluster")
destroying.add_done_callback(handler)
```


```python
# Clean and delete bucket
for page in bucket.list_blobs().pages:
    for element in page:
        bucket.delete_blob(element.name)
bucket.delete()
```

There you have it. Now spark clusters are a disposable thing for you. When you want one, you can spin it up at any size your credit card will support. Prototype your jobs locally and submit them from your notebook. When you're done, tear it back down.

Why would you want to know how to do this? For one-off batch processing and reporting. When would you definitely not use this? In production.

#### Reference Docs

1. https://googleapis.github.io/google-cloud-python/latest/dataproc/
2. https://googleapis.github.io/google-cloud-python/latest/storage/
3. https://spark.apache.org/docs/2.1.0/api/python/

#### Thank you!

Thank you for reading. I hope this has been helpful for you.

I'm always learning, so if you see anything wrong in here I hope you'll leave me a comment to share your insights. Or just leave a comment anyway.

