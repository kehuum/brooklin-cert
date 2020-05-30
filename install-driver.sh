#!/usr/bin/env bash

SCRIPT_NAME=$(basename "$0")
CLEAN_OPTION="--clean"
DEPENDENCIES_DIR=dependencies
TARBALL_EXT=".tar.gz"
DEPENDECIES_TARBALL="${DEPENDENCIES_DIR}${TARBALL_EXT}"
SCRIPTS_TARBALL="brooklin-certification-*$TARBALL_EXT"
GEN_CERT_SCRIPT="gen-cert.sh"
CA_BUNDLE_FILE="ca-bundle.crt"

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

SCRIPTS_TARBALL=$(find . -name "$SCRIPTS_TARBALL" -type f)
SCRIPTS_DIR=${SCRIPTS_TARBALL%$TARBALL_EXT}

if [[ $CLEAN == 0 ]]; then
  echo "Extracting scripts tarball ..."
  redirect_output tar -zxvf "$SCRIPTS_TARBALL"
  exit_on_failure "Extracting scripts tarball failed"

  echo "Moving $CA_BUNDLE_FILE into scripts directory"
  redirect_output mv $CA_BUNDLE_FILE $SCRIPTS_DIR
  exit_on_failure "Failed to move $CA_BUNDLE_FILE into scripts directory"

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