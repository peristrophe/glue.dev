#!/usr/bin/env bash

FULLPATH_OF_HERE=$(cd $(dirname $0); pwd)
TOPDIR=$(dirname $FULLPATH_OF_HERE)
CATALOGDIR=${TOPDIR}/catalogs/athena
BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

mkdir -p ${CATALOGDIR}
aws athena list-data-catalogs > ${CATALOGDIR}/data-catalogs.json

for catalog in $(jq -r '.DataCatalogsSummary[].CatalogName' ${CATALOGDIR}/data-catalogs.json); do
    mkdir -p ${CATALOGDIR}/${catalog}
    aws athena list-databases --catalog-name ${catalog} > ${CATALOGDIR}/${catalog}/databases.json

    for database in $(jq -r '.DatabaseList[].Name' ${CATALOGDIR}/${catalog}/databases.json); do
        if [ "${BRANCH_NAME}" == "prd" ]; then
            [ -n "$(echo ${database} | grep stg)" ] && echo "SKIP: ${database}" && continue
            [ -n "$(echo ${database} | grep dev)" ] && echo "SKIP: ${database}" && continue
        elif [ "${BRANCH_NAME}" == "stg" -o "${BRANCH_NAME}" == "dev" ]; then
            [ -z "$(echo ${database} | grep ${BRANCH_NAME})" ] && echo "SKIP: ${database}" && continue
        fi

        mkdir -p ${CATALOGDIR}/${catalog}/${database}
        aws athena list-table-metadata --catalog-name ${catalog} --database-name ${database} > ${CATALOGDIR}/${catalog}/${database}/tables.json

        for table in $(jq -r '.TableMetadataList[].Name' ${CATALOGDIR}/${catalog}/${database}/tables.json); do
            mkdir -p ${CATALOGDIR}/${catalog}/${database}/${table}
            jq ".TableMetadataList[] | select(.Name == \"${table}\")" ${CATALOGDIR}/${catalog}/${database}/tables.json > ${CATALOGDIR}/${catalog}/${database}/${table}/metadata.json
        done
    done
done
