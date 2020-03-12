#!/usr/bin/env bash

SCRIPT_NAME=`basename "$0"`
SOURCES_DIR=src
DEPENDENCIES_DIR=dependencies
DEPENDENCIES_TARBALL=$DEPENDENCIES_DIR.tar.gz
DIST_DIR=dist
TEST_DRIVER_HOST=lor1-app26891.prod.linkedin.com
VERBOSE=0

function usage {
    echo "usage: $SCRIPT_NAME [-t hostname] [-v] [-h]"
    echo "  -t hostname          Specify test driver hostname"
    echo "  -v                   Turn on verbose logging"
    echo "  -h                   Display help"
    exit $1
}

if [ "$#" -ge 4 ]; then
    echo "Illegal number of parameters"
    usage
    exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -t)
    TEST_DRIVER_HOST="$2"
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

redirect_output pushd $SOURCES_DIR

# Generate tarball containing all script files
echo "Generating scripts tarball ..."
redirect_output rm -rf $DIST_DIR
redirect_output pipenv run python setup.py sdist
exit_on_failure "Generating scripts tarball failed"

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

# Optionally copy tarball to test driver machine
read -p "Would you like to copy the tarballs to your home dir on $TEST_DRIVER_HOST? (y/n): " -n 1 -r
echo # add an empty line
if [[ $REPLY =~ ^[Yy]$ ]]; then
  read -p "Press Enter when you are you ready to type in your 2FA password"
  redirect_output scp  $DIST_DIR/brooklin-certification-*.tar.gz $USER@$TEST_DRIVER_HOST:~/
  exit_on_failure "Copying scripts tarball failed"
  redirect_output scp  $DEPENDENCIES_TARBALL $USER@$TEST_DRIVER_HOST:~/
  exit_on_failure "Copying dependencies tarball failed"
fi

# Optionally clean up generated files
read -p "Would you like to clean up generated files and directories? (y/n): " -n 1 -r
echo # add an empty line
if [[ $REPLY =~ ^[Yy]$ ]]; then
  redirect_output rm -rf $DEPENDENCIES_DIR $DEPENDENCIES_TARBALL $DIST_DIR *.egg-info
  exit_on_failure "Cleaning up generated files and directories failed"
fi

redirect_output popd
