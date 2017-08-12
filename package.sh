#!/usr/bin/env bash

OUTPUT_DIR="pkg"
BUILD_DIR="pkg/build"
FNAME="nimbus"


###########
# PACKAGE #
###########

FNAME_TAR="${OUTPUT_DIR}/${FNAME}.tar"
FNAME_ZIP="${FNAME_TAR}.gz"

if [ -f ${FNAME_ZIP} ]; then
    echo "Removing previous package"
    rm ${FNAME_ZIP}
fi

mkdir ${BUILD_DIR}
cp -r nimbus ${BUILD_DIR}

tar -c -f ${FNAME_TAR} --exclude=*.pyc --exclude=__pycache__ -C ${BUILD_DIR} .

gzip --best ${FNAME_TAR}

rm -r ${BUILD_DIR}
