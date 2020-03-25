#!/usr/bin/env bash

SCRIPT_NAME=$(basename "$0")
CLEAN_OPTION="--clean"
DEPENDENCIES_DIR=dependencies
DEPENDECIES_TARBALL="$DEPENDENCIES_DIR.tar.gz"
SCRIPTS_DIR="brooklin-certification-*"
SCRIPTS_TARBALL="$SCRIPTS_DIR.tar.gz"
GEN_CERT_SCRIPT="gen-cert.sh"

CLEAN=0
VERBOSE=0

function usage {
    echo "usage: $SCRIPT_NAME OPTIONS"
    echo "Install the test driver on localhost"
    echo
    echo "OPTIONS"
    echo " $CLEAN_OPTION   Remove installed test driver"
    echo "  -v        Turn on verbose logging"
    echo "  -h        Display help"
    exit "$1"
}

function redirect_output() {
    if [ "$VERBOSE" -eq "0" ]; then
        "$@" > /dev/null 2>&1
    else
        "$@"
    fi
}

function exit_on_failure() {
  if [ $? -ne 0 ]; then
    >&2 echo "$1"
    >&2 echo "Try running ./$SCRIPT_NAME -v to see detailed logs"
    exit 1
  fi
}

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    $CLEAN_OPTION)
    CLEAN=1
    shift # past argument
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

SCRIPTS_DIR=$(find . -name "$SCRIPTS_DIR" -type d)
SCRIPTS_TARBALL=$(find . -name "$SCRIPTS_TARBALL" -type f)

if [[ $CLEAN == 0 ]]; then
  echo "Extracting scripts tarball ..."
  redirect_output tar -zxvf "$SCRIPTS_TARBALL"
  exit_on_failure "Extracting scripts tarball failed"

  echo "Extracting dependencies tarball ..."
  redirect_output tar -zxvf $DEPENDECIES_TARBALL
  exit_on_failure "Extracting dependencies tarball failed"

  echo "Installing dependencies ..."
  redirect_output /export/apps/python/3.7/bin/python3 -m pip install --user -r $DEPENDENCIES_DIR/requirements.txt --no-index --find-links $DEPENDENCIES_DIR
  exit_on_failure "Installing dependencies failed"
else
  echo "Uninstalling dependencies ..."
  redirect_output /export/apps/python/3.7/bin/python3 -m pip uninstall -y -r dependencies/requirements.txt
  exit_on_failure "Uninstalling dependencies failed"

  echo "Deleting scripts and tarballs ..."
  rm -rf "$GEN_CERT_SCRIPT" "$SCRIPTS_DIR" $DEPENDENCIES_DIR "$SCRIPTS_TARBALL" $DEPENDECIES_TARBALL

  echo "Deleting self ... bye"
  rm -- "$0"
fi