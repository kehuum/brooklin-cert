#!/usr/bin/env bash

BROOKLIN_ALL_OPTION="--brooklin"
BROOKLIN_CONTROL_OPTION="--ctrl"
BROOKLIN_EXP_OPTION="--exp"
TEST_DRIVER_OPTION="--driver"
TEST_DRIVER_HOST_OPTION="--td"
CLEAN_OPTION="--clean"
CONFIG_ONLY_OPTION="--config-only"
CONFIG_PATH_OPTION="--cp"
VERBOSE_OPTION="-v"

BROOKLIN_CONTROL_TAG="brooklin.cert.control"
BROOKLIN_EXP_TAG="brooklin.cert.candidate"

INCLUDE_TEST_DRIVER=0
INCLUDE_BROOKLIN_ALL=0
INCLUDE_BROOKLIN_CONTROL=0
INCLUDE_BROOKLIN_EXP=0
INCLUDE_CONFIG_ONLY=0
PROMPT_FOR_2FA=1
CLEAN=0
VERBOSE=0

CA_BUNDLE_FILE="/etc/riddler/ca-bundle.crt"
APPLICATION_CFG_FILE="application.cfg"
DEPENDENCIES_DIR=dependencies
DEPENDENCIES_TARBALL="$DEPENDENCIES_DIR.tar.gz"
DIST_DIR=dist
GEN_CERT_SCRIPT="gen-cert.sh"
INSTALL_AGENT_SCRIPT="install-agent.sh"
INSTALL_CONFIG_SCRIPT="install-config.sh"
INSTALL_DRIVER_SCRIPT="install-driver.sh"
SCRIPT_NAME=$(basename "$0")
SOURCES_DIR=src
TEST_DRIVER_HOST="lor1-app26891.prod.linkedin.com"
CONFIG_PATH=".."

function usage {
    echo "usage: $SCRIPT_NAME OPTIONS"
    echo "Deploy test scripts to remote hosts"
    echo
    echo "OPTIONS"
    echo "  $TEST_DRIVER_OPTION        Include test scripts on the default test driver host: $TEST_DRIVER_HOST"
    echo "  $TEST_DRIVER_HOST_OPTION hostname   Include test scripts on the specified hostname"
    echo "  $BROOKLIN_CONTROL_OPTION          Include test agent on $BROOKLIN_CONTROL_TAG cluster"
    echo "  $BROOKLIN_EXP_OPTION           Include test agent on $BROOKLIN_EXP_TAG cluster"
    echo "  $BROOKLIN_ALL_OPTION      Same as specifying both $BROOKLIN_CONTROL_OPTION and $BROOKLIN_EXP_OPTION"
    echo "  $CLEAN_OPTION         Remove files on all included host and clusters"
    echo "  $CONFIG_ONLY_OPTION   Include $APPLICATION_CFG_FILE file and install script on $BROOKLIN_CONTROL_TAG or $BROOKLIN_EXP_TAG"
    echo "  $CONFIG_PATH_OPTION path       Include $APPLICATION_CFG_FILE from a specified path. Defaults to '..' if not specified"
    echo "  $VERBOSE_OPTION              Turn on verbose logging"
    echo "  -h              Display help"
    exit "$1"
}

function validate_arguments() {
  # Validate Brooklin options
  if [[ $INCLUDE_BROOKLIN_ALL == 1 ]]; then
    sum=$((INCLUDE_BROOKLIN_CONTROL+INCLUDE_BROOKLIN_EXP))
    if [[ $sum == 1 ]]; then
      >&2 echo "Cannot specify $BROOKLIN_ALL_OPTION and one of"\
      "$BROOKLIN_CONTROL_OPTION or $BROOKLIN_EXP_OPTION"
      usage 1
    fi

    if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
      >&2 echo "Cannot specify $CONFIG_ONLY_OPTION/$CONFIG_PATH_OPTION and $BROOKLIN_ALL_OPTION."\
      "Must specify either $BROOKLIN_CONTROL_OPTION or $BROOKLIN_EXP_OPTION with $CONFIG_ONLY_OPTION/$CONFIG_PATH_OPTION"
      usage 1
    fi
  fi

  # Validate Brooklin config options
  if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
    sum=$((INCLUDE_BROOKLIN_CONTROL+INCLUDE_BROOKLIN_EXP))
    if [[ $sum != 1 ]]; then
      >&2 echo "Cannot specify $CONFIG_ONLY_OPTION/$CONFIG_PATH_OPTION and none or both of"\
      "$BROOKLIN_CONTROL_OPTION or $BROOKLIN_EXP_OPTION"
      usage 1
    fi
  fi
}

function exit_on_failure() {
  if [ $? -ne 0 ]; then
    >&2 echo "$1"
    >&2 echo "Try running ./$SCRIPT_NAME -v to see detailed logs"
    exit 1
  fi
}

function redirect_output() {
    if [ "$VERBOSE" -eq "0" ]; then
        "$@" > /dev/null 2>&1
    else
        "$@"
    fi
}

function prompt_for_2FA() {
  if [[ $PROMPT_FOR_2FA == 1 ]]; then
    read -p "Press Enter when you are you ready to type in your 2FA password" -r
    PROMPT_FOR_2FA=0  # Do not show this prompt again
  fi
}

function copy_scripts_to_cluster() {
  echo "Copying scripts to $1 ..."
  hosts=$(eh -e "$1")
  while read -r h; do
    redirect_output scp "../$INSTALL_AGENT_SCRIPT" $DIST_DIR/brooklin-certification-*.tar.gz "$USER"@"$h":~/
  done <<< "$hosts"
}

function cleanup_cluster() {
  echo "Cleaning up $1 ..."
  redirect_output mssh -r "$1" "./$INSTALL_AGENT_SCRIPT --clean $2"
}

function copy_configs_to_cluster() {
  echo "Copying configs to $1 from $CONFIG_PATH ..."
  hosts=$(eh -e "$1")
  while read -r h; do
    redirect_output scp "../$INSTALL_CONFIG_SCRIPT" "$CONFIG_PATH/$APPLICATION_CFG_FILE" "$USER"@"$h":~/
  done <<< "$hosts"
}

function cleanup_cluster_config() {
  echo "Cleaning up config $1 ..."
  redirect_output mssh -r "$1" "./$INSTALL_CONFIG_SCRIPT --clean"
}

if [[ $# -eq 0 ]]; then
  usage 2
fi

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    $TEST_DRIVER_HOST_OPTION)
    TEST_DRIVER_HOST="$2"
    INCLUDE_TEST_DRIVER=1
    shift # past argument
    shift # past value
    ;;
    $TEST_DRIVER_OPTION)
    INCLUDE_TEST_DRIVER=1
    shift # past argument
    ;;
    $BROOKLIN_CONTROL_OPTION)
    INCLUDE_BROOKLIN_CONTROL=1
    shift # past argument
    ;;
    $BROOKLIN_EXP_OPTION)
    INCLUDE_BROOKLIN_EXP=1
    shift # past argument
    ;;
    $BROOKLIN_ALL_OPTION)
    INCLUDE_BROOKLIN_ALL=1
    shift # past argument
    ;;
    $CLEAN_OPTION)
    CLEAN=1
    shift # past argument
    ;;
    $CONFIG_ONLY_OPTION)
    INCLUDE_CONFIG_ONLY=1
    shift # past argument
    ;;
    $CONFIG_PATH_OPTION)
    CONFIG_PATH="$2"
    INCLUDE_CONFIG_ONLY=1
    shift # past argument
    shift # past value
    ;;
    -v)
    VERBOSE=1
    shift # past argument
    ;;
    -h|--help)
    usage 0
    ;;
    *)    # unknown option
    echo "Unrecognized option: $1"
    usage 1
    ;;
esac
done

validate_arguments

if [[ $INCLUDE_BROOKLIN_ALL == 1 ]]; then
  INCLUDE_BROOKLIN_EXP=1
  INCLUDE_BROOKLIN_CONTROL=1
fi

if [[ $CLEAN == 0 ]]; then
  redirect_output pushd $SOURCES_DIR

  # Generate tarball containing all script files
  echo "Generating scripts tarball ..."
  redirect_output rm -rf $DIST_DIR
  redirect_output pipenv run python setup.py sdist
  exit_on_failure "Generating scripts tarball failed"

  # Test driver
  if [[ $INCLUDE_TEST_DRIVER == 1 ]]; then
    # Generate tarball for all dependencies
    echo "Generating dependencies tarball ..."
    redirect_output rm -rf $DEPENDENCIES_DIR
    redirect_output mkdir $DEPENDENCIES_DIR
    exit_on_failure "Creating $DEPENDENCIES_DIR failed"
    pipenv lock -r | tail -n +2 > $DEPENDENCIES_DIR/requirements.txt
    exit_on_failure "Generating requirements file failed"
    redirect_output pipenv run python -m pip download -r $DEPENDENCIES_DIR/requirements.txt -d $DEPENDENCIES_DIR
    exit_on_failure "Downloading dependencies failed"
    redirect_output tar -czvf $DEPENDENCIES_TARBALL $DEPENDENCIES_DIR
    exit_on_failure "Creating scripts tarball failed"

    prompt_for_2FA

    # Copy scripts and tarballs
    echo "Copying scripts and tarballs to $TEST_DRIVER_HOST"
    redirect_output scp "../$INSTALL_DRIVER_SCRIPT" "../$GEN_CERT_SCRIPT" $CA_BUNDLE_FILE $DEPENDENCIES_TARBALL $DIST_DIR/brooklin-certification-*.tar.gz "$USER"@"$TEST_DRIVER_HOST":~/
    exit_on_failure "Copying scripts tarball failed"

    echo "Installing test driver on $TEST_DRIVER_HOST"
    redirect_output mssh -n "$TEST_DRIVER_HOST" "./$INSTALL_DRIVER_SCRIPT"
    exit_on_failure "Installing test driver failed"
  fi

  # Brooklin control
  if [[ $INCLUDE_BROOKLIN_CONTROL == 1 ]]; then
    if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
      copy_configs_to_cluster "%prod-lor1.tag_hosts:$BROOKLIN_CONTROL_TAG"
    else
      copy_scripts_to_cluster "%prod-lor1.tag_hosts:$BROOKLIN_CONTROL_TAG" $BROOKLIN_ALL_OPTION
    fi
  fi

  # Brooklin experiment
  if [[ $INCLUDE_BROOKLIN_EXP == 1 ]]; then
    if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
      copy_configs_to_cluster "%prod-lor1.tag_hosts:$BROOKLIN_EXP_TAG"
    else
      copy_scripts_to_cluster "%prod-lor1.tag_hosts:$BROOKLIN_EXP_TAG" $BROOKLIN_ALL_OPTION
    fi
  fi

  # Clean up generated files
  redirect_output rm -rf $DEPENDENCIES_DIR $DEPENDENCIES_TARBALL $DIST_DIR *.egg-info
  exit_on_failure "Cleaning up generated files and directories failed"

  redirect_output popd

else # $CLEAN == 1
  # Test driver
  if [[ $INCLUDE_TEST_DRIVER == 1 ]]; then
    echo "Cleaning up test driver host $TEST_DRIVER_HOST"
    redirect_output mssh -n "$TEST_DRIVER_HOST" "./$INSTALL_DRIVER_SCRIPT --clean"
    exit_on_failure "Cleaning up test driver host $TEST_DRIVER_HOST failed"
  fi

  # Brooklin control
  if [[ $INCLUDE_BROOKLIN_CONTROL == 1 ]]; then
    if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
      cleanup_cluster_config "%prod-lor1.tag_hosts:$BROOKLIN_CONTROL_TAG"
    else
      cleanup_cluster "%prod-lor1.tag_hosts:$BROOKLIN_CONTROL_TAG" $BROOKLIN_ALL_OPTION
    fi
  fi

  # Brooklin experiment
  if [[ $INCLUDE_BROOKLIN_EXP == 1 ]]; then
    if [[ $INCLUDE_CONFIG_ONLY == 1 ]]; then
      cleanup_cluster_config "%prod-lor1.tag_hosts:$BROOKLIN_EXP_TAG"
    else
      cleanup_cluster "%prod-lor1.tag_hosts:$BROOKLIN_EXP_TAG" $BROOKLIN_ALL_OPTION
    fi
  fi
fi
