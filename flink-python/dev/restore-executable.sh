#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# restore-executable.sh
# This script will restore executable permission of scripts in wheel packages.

function restore_wheel_file() {
    file_name_suffix=`echo "${2##*.}"`
    if [[ ${file_name_suffix} = 'whl' ]]; then
        pushd ${1} &> /dev/null
        # unpack wheel package
        result=`wheel unpack ${2}`
        unpack_wheel_dir_name=`echo ${result:16:$((${#result}-21))}`
        # restore script of executable permissions
        chmod a+x ${unpack_wheel_dir_name}/pyflink/bin/*
        # pack wheel package
        wheel pack ${unpack_wheel_dir_name}
        # remove temp wheel directory
        rm -rf ${unpack_wheel_dir_name}
        popd
    fi
}

function restore_wheel_files() {
    for wheel_file in ${WHEEL_FILES[@]}; do
        parent_dir="$(cd $(dirname ${wheel_file}); pwd)"
        restore_wheel_file ${parent_dir} `basename ${wheel_file}`
    done
}

function restore_wheel_directories() {
    for wheel_directory in ${WHEEL_DIRECTORIES[@]}; do
        parent_dir="$(cd $(dirname ${wheel_directory}); pwd)"
        wheel_directory="$(cd ${wheel_directory}; pwd)"
        pushd ${parent_dir} &> /dev/null
        for FILE in `ls ${wheel_directory}`; do
            restore_wheel_file ${wheel_directory} ${FILE}
        done
        popd
    done
}

USAGE="
usage: $0 [options]
-h          print this help message and exit
-d          specify directories that contains wheel packages that you want to restore executable permission.
            you can specify many directories which split by comma(,)
-f          specify a wheel package that you want to add executable permission.
            you can specify many wheel files which split by comma(,)
Examples:
    ./restore-executable.sh -d wheel_Darwin_build_wheels,wheel_Linux_build_wheels
    ./restore-executable.sh -f wheel_Darwin_build_wheels/apache_flink-1.11.dev0-cp37-cp37m-macosx_10_9_x86_64.whl
"
_OLD_IFS=$IFS
IFS=$'\n'
while getopts "hd:f:" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        d)
            echo $OPTARG
            WHEEL_DIRECTORIES=($(echo $OPTARG | tr ',' '\n' ))
            ;;
        f)
            WHEEL_FILES=($(echo $OPTARG | tr ',' '\n' ))
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done

# DEV_DIR is "flink/flink-python/dev/"
DEV_DIR="$(cd "$( dirname "$0" )" && pwd)"

# install conda
${DEV_DIR}/lint-python.sh -s basic

source ${DEV_DIR}/.conda/bin/activate ""

restore_wheel_directories

restore_wheel_files

conda deactivate
IFS=${_OLD_IFS}
