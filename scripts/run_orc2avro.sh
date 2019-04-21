#!/bin/bash

PROJECT_ID=gflocks-makh-vault
PIPELINE_FOLDER=gs://${PROJECT_ID}

# Set the runner
RUNNER=DataflowRunner


# Build the template
mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.pso.HadoopInputFormatIOExample \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT_ID} \
        --jobName=dataflow-orc2avro \
        --usePublicIps=false \
        --serviceAccount=app1-services@gflocks-makh-vault.iam.gserviceaccount.com \
        --stagingLocation=${PIPELINE_FOLDER}/staging \
        --tempLocation=${PIPELINE_FOLDER}/temp \
        --schemaFile=gs://gflocks-makh-vault/hadoop-inputformat/schema/1.avsc \
        --inputDir=gs://gflocks-makh-vault/hadoop-inputformat/ \
        --outputDir=gs://gflocks-makh-vault/hadoop-inputformat/output/ \
        --runner=${RUNNER}"

