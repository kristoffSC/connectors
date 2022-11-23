#!/usr/bin/env bash

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

RELATIVE_SCRIPT_PATH=$(dirname -- "${BASH_SOURCE[0]:-$0}")
WORKDIR=$(realpath "$RELATIVE_SCRIPT_PATH")
PROJECT_ROOT_DIR="$WORKDIR/../../"
TERRAFORM_DIR="$WORKDIR/terraform/"
KUBERNETES_DIR="$TERRAFORM_DIR/kubernetes/"

build_artifact() {
  echo "Building artifact."
  cd "$PROJECT_ROOT_DIR" || exit
  build/sbt "++ $SCALA_VERSION" flinkEndToEndTestsFatJar/assembly
  local return_code=$?
  cd "$WORKDIR" || exit
  return $return_code
}

export_fat_jar_path() {
  echo "Exporting fat jar path."
  local matching_jar
  matching_jar=$(find "$WORKDIR"/../end-to-end-tests-fatjar/target/scala-"$SHORT_SCALA_VERSION"/ -iname 'flink-end-to-end-tests-fatjar-assembly-*.jar' -type f)

  if [ -z "$matching_jar" ]; then
    echo "Cannot find artifact containing test jobs."
    exit 1
  else
    echo "Artifact found at path ${matching_jar[0]}."
    export JAR_PATH="${matching_jar[0]}"
  fi
}

create_terraform_infrastructure() {
  echo "Creating terraform infrastructure."
  # FIXME terraform init, validate and apply
  echo "It will be implemented in the PR-02-A"
}

destroy_terraform_infrastructure() {
  echo "Destroying terraform infrastructure."
  # FIXME terraform destroy
  echo "It will be implemented in the PR-02-A"
}

create_kubernetes_infrastructure() {
  echo "Creating kubernetes infrastructure."
  # FIXME: installing cert-manager and kubernetes-flink-operator, apply kubernetes configuration
  echo "It will be implemented in the PR-02-A"
}

kubernetes_cleanup() {
  echo "Kubernetes cleanup."
  # FIXME: uninstall flink-kubernetes-operator, close port-forward, unset kubernetes context
  echo "It will be implemented in the PR-02-A"
}

export_terraform_outputs() {
  export ACCOUNT_ID=$(terraform -chdir="$TERRAFORM_DIR" output account_id | tr -d '"')
  export AWS_REGION=$(terraform -chdir="$TERRAFORM_DIR" output region | tr -d '"')
  export SERVICE_ACCOUNT_ROLE_NAME=$(terraform -chdir="$TERRAFORM_DIR" output service_account_role_name | tr -d '"')
  export EKS_CLUSTER_NAME=$(terraform -chdir="$TERRAFORM_DIR" output eks_cluster_name | tr -d '"')
  export TEST_DATA_BUCKET_NAME=$(terraform -chdir="$TERRAFORM_DIR" output test_data_bucket_name | tr -d '"')
  export CREDENTIALS_ACCESS_KEY_ID=$(terraform -chdir="$TERRAFORM_DIR" output access_key | tr -d '"' | base64)
  export CREDENTIALS_SECRET_KEY=$(terraform -chdir="$TERRAFORM_DIR" output secret_key | tr -d '"' | base64)
}

jobmanager_port_forward() {
  echo "Run port forward"
  # FIXME: wait until pod is created & ready and then run port forward
  echo "It will be implemented in the PR-02-A"
}

run_end_to_end_tests() {
  echo "Running tests..."
  cd "$PROJECT_ROOT_DIR" || exit

  build/sbt "++ $SCALA_VERSION" \
    flinkEndToEndTests/test
  local return_code=$?
  cd "$WORKDIR" || exit
  return $return_code
}

main() {
  while [[ $# -gt 0 ]]; do
    case $1 in
    --preserve-s3-data)
      PRESERVE_S3_DATA="yes"
      shift # past argument
      ;;
    --scala-version)
      SCALA_VERSION="$2"
      # Take only the first four characters (e.g. 2.12).
      SHORT_SCALA_VERSION="${SCALA_VERSION:0:4}"
      shift # past argument
      shift # past value
      ;;
    -* | --*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      shift # past argument
      ;;
    esac
  done

  if ! build_artifact; then
    echo "[ERROR] Failed to build artifact."
    exit 1
  fi

  if ! export_fat_jar_path; then
    echo "[ERROR] Failed to find the test artifact path."
    exit 1
  fi

  #if ! create_terraform_infrastructure; then
  #  echo "[ERROR] Failed to create test infrastructure."
  #  exit 1
  #fi

  #if ! export_terraform_outputs; then
  #  echo "[ERROR] Failed to extract variables."
  #  exit 1
  #fi

  #if ! create_kubernetes_infrastructure; then
  #  echo "[ERROR] Failed to create kubernetes infrastructure."
  #  exit 1
  #fi

  if ! jobmanager_port_forward; then
    echo "[ERROR] Failed to extract Flink JobManager address."
    exit 1
  fi

  if ! run_end_to_end_tests; then
    echo "[ERROR] Failed to run tests."
    exit 1
  fi
}

cleanup() {
  echo "Clean up..."
  kubernetes_cleanup
  cd "$WORKDIR" || exit 1
  destroy_terraform_infrastructure
}

trap cleanup EXIT
main "$@"
