#!/bin/bash

PROJECT_ID=gflocks-makh-vault
PIPELINE_FOLDER=gs://${PROJECT_ID}/dataflow/pipelines/sample-pipeline

# Set the runner
# RUNNER=DataflowRunner
 RUNNER=DirectRunner

# Build the template
mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.pso.dataflow.pipeline.StarterPipeline \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT_ID} \
        --stagingLocation=${PIPELINE_FOLDER}/staging \
        --tempLocation=${PIPELINE_FOLDER}/temp \
        --runner=${RUNNER}"
