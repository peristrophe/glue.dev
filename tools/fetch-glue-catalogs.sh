#!/usr/bin/env bash

FULLPATH_OF_HERE=$(cd $(dirname $0); pwd)
TOPDIR=$(dirname $FULLPATH_OF_HERE)
CATALOGDIR=${TOPDIR}/catalogs/glue
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

mkdir -p ${CATALOGDIR}
aws glue get-databases > ${CATALOGDIR}/databases.json

for database in $(jq -r '.DatabaseList[].Name' ${CATALOGDIR}/databases.json); do
    if [ "${BRANCH_NAME}" == "prd" ]; then
        [ -n "$(echo ${database} | grep stg)" ] && echo "SKIP: ${database}" && continue
        [ -n "$(echo ${database} | grep dev)" ] && echo "SKIP: ${database}" && continue
    elif [ "${BRANCH_NAME}" == "stg" -o "${BRANCH_NAME}" == "dev" ]; then
        [ -z "$(echo ${database} | grep ${BRANCH_NAME})" ] && echo "SKIP: ${database}" && continue
    fi

    mkdir -p ${CATALOGDIR}/${database}
    aws glue get-tables --database-name ${database} > ${CATALOGDIR}/${database}/tables.json

    for table in $(jq -r '.TableList[].Name' ${CATALOGDIR}/${database}/tables.json); do
        mkdir -p ${CATALOGDIR}/${database}/${table}
        jq ".TableList[] | select(.Name == \"${table}\")" ${CATALOGDIR}/${database}/tables.json > ${CATALOGDIR}/${database}/${table}/metadata.json
    done
done
