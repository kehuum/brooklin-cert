#!/usr/bin/env bash

SCRIPT_NAME=$(basename "$0")
CLEAN_OPTION="--clean"
DEPENDENCIES_DIR=dependencies
TARBALL_EXT=".tar.gz"
DEPENDECIES_TARBALL="${DEPENDENCIES_DIR}${TARBALL_EXT}"
SCRIPTS_TARBALL="brooklin-certification-*$TARBALL_EXT"
GEN_CERT_SCRIPT="gen-cert.sh"
CA_BUNDLE_FILE="ca-bundle.crt"
# Intentionally using a generic version (e.g. 3.7) instead of a specific version (e.g. 3.7.7)
# since the generic versions are symlinked to the latest specific version on LinkedIn machines
PYTHON_VERSION="3.7"
PYTHON_BIN_DIR="/export/apps/python/$PYTHON_VERSION/bin"
PYTHON_LIB_DIR="/export/apps/python/$PYTHON_VERSION/lib"
PYTHON="$PYTHON_BIN_DIR/python3"

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

echo "Ensuring pip ..."
redirect_output $PYTHON $PYTHON_LIB_DIR/python$PYTHON_VERSION/ensurepip --user
exit_on_failure "Ensuring pip failed"

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
  redirect_output $PYTHON -m pip install --user -r $DEPENDENCIES_DIR/requirements.txt --no-index --find-links $DEPENDENCIES_DIR
  exit_on_failure "Installing dependencies failed"
else
  echo "Uninstalling dependencies ..."
  redirect_output $PYTHON -m pip uninstall -y -r dependencies/requirements.txt
  exit_on_failure "Uninstalling dependencies failed"

  echo "Deleting scripts and tarballs ..."
  rm -rf "$GEN_CERT_SCRIPT" "$SCRIPTS_DIR" $DEPENDENCIES_DIR "$SCRIPTS_TARBALL" $DEPENDECIES_TARBALL

  echo "Deleting self ... bye"
  rm -- "$0"
fi