#!/usr/bin/env bash

SCRIPT_NAME=`basename "$0"`
VERBOSE=0

function usage {
    echo "usage: $SCRIPT_NAME [-v] [-h]"
    echo "Generate Grestin SSL certificates for the current user"
    echo
    echo "  -v   Turn on verbose logging"
    echo "  -h   Display help"
    exit $1
}

if [ "$#" -ge 2 ]; then
    echo "Illegal number of parameters"
    usage
    exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"

case $key in
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

redirect_output pushd src

rm -rf identity.*
exit_on_failure "Failed to delete existing certificate files"

echo "Generating Grestin certificate for the current user: $USER"
id-tool grestin sign -u $USER
exit_on_failure "Failed to generate Grestin certificate"

echo work_around_jdk-6879539 > work_around
exit_on_failure "Failed to write JDK workaround file"

redirect_output openssl pkcs12 -in identity.p12  -out identity.pem  -password file:work_around  -nodes
exit_on_failure "Failed to convert identity.p12 file to PEM format"

echo "Generated certificate file successfully: $PWD/identitiy.pem"

rm -rf identity.cert identity.key work_around

redirect_output popd
