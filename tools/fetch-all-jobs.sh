#!/usr/bin/env bash

FULLPATH_OF_HERE=$(cd $(dirname $0); pwd)
TOPDIR=$(dirname $FULLPATH_OF_HERE)
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
echo ${BRANCH_NAME}

# fetch job list
SUFFIX=$(date +%s%3N)
aws glue list-jobs | tee ${TOPDIR}/.cache/list-jobs-${SUFFIX}.json
NEXT_TOKEN=$(jq -r '.NextToken' ${TOPDIR}/.cache/list-jobs-${SUFFIX}.json)

while [ ${NEXT_TOKEN} != "null" ]; do
    sleep 1
    SUFFIX=$(date +%s%3N)
    aws glue list-jobs --next-token ${NEXT_TOKEN} | tee ${TOPDIR}/.cache/list-jobs-${SUFFIX}.json
    NEXT_TOKEN=$(jq -r '.NextToken' ${TOPDIR}/.cache/list-jobs-${SUFFIX}.json)
done

# make job directories
for list in $(find ${TOPDIR}/.cache -type f -name "list-jobs-*.json"); do
    case ${BRANCH_NAME} in
        "prd" | "stg" | "dev")
            jq -r '.JobNames[]' ${list} | grep "\-${BRANCH_NAME}\-" | xargs -I{} mkdir -p ${TOPDIR}/jobs/{}
            ;;
        *)
            jq -r '.JobNames[]' ${list} | xargs -I{} mkdir -p ${TOPDIR}/jobs/{}
    esac
done

# fetch job contents
mkdir -p ${TOPDIR}/cracked_jobs
for jobdir in $(find ${TOPDIR}/jobs -mindepth 1 -maxdepth 1 -type d); do
    echo
    echo "Fetch Contents for '$(basename ${jobdir})'"

    aws glue get-job --job-name $(basename ${jobdir}) > ${jobdir}/metadata.json
    jq -r '.Job.Command.ScriptLocation' ${jobdir}/metadata.json

    aws s3 cp $(jq -r '.Job.Command.ScriptLocation' ${jobdir}/metadata.json) ${jobdir}/
    if [ $? -ne 0 ]; then
        mv ${jobdir} ${TOPDIR}/cracked_jobs
    fi
done