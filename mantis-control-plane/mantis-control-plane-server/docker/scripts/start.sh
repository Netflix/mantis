#!/bin/sh
#
# Copyright 2023 Netflix, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
set -x

bin_dir=$(dirname "$0")
root_dir=${bin_dir}/..
storage_dir=/tmp/mantis_storage

# copy the job clusters
mkdir -p ${storage_dir}/MantisNamedJobs
cp -r "${root_dir}"/job-clusters/* ${storage_dir}/MantisNamedJobs/

# start the server
${bin_dir}/mantis-control-plane-server -p ${root_dir}/conf/master-docker.properties
