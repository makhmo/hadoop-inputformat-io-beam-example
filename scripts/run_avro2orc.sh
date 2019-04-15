#!/bin/bash

PROJECT_ID=gflocks-makh-vault
PIPELINE_FOLDER=gs://${PROJECT_ID}

# Set the runner
RUNNER=DataflowRunner


# Build the template
mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.pso.HadoopOutputFormatIOExample \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT_ID} \
        --jobName=dataflow-avro2orc \
        --usePublicIps=false \
        --serviceAccount=app1-services@gflocks-makh-vault.iam.gserviceaccount.com \
        --stagingLocation=${PIPELINE_FOLDER}/dataflow-avro2orc/staging \
        --tempLocation=${PIPELINE_FOLDER}/dataflow-avro2orc/temp \
        --schemaFile=gs://gflocks-makh-vault/dataflow-avro2orc/1.avsc \
        --inputDir=gs://gflocks-makh-vault/dataflow-avro2orc/1.avro \
        --outputDir=gs://gflocks-makh-vault/dataflow-avro2orc/ \
        --lockDir=gs://gflocks-makh-vault/dataflow-avro2orc/locks/ \
        --runner=${RUNNER}"
