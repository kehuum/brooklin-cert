#!/usr/bin/env bash

SCRIPT_NAME=$(basename "$0")
APPLICATION_CFG_FILE="application.cfg"
APPLICATION_CFG_FILE_MOVED="application.cfg_old"
CONFIG_PATH="/export/content/lid/apps/brooklin-service/i001/appsConfig/brooklin-service"
CLEAN_OPTION="--clean"
LOCKER_NAME="brooklin-service_i001"

CLEAN=0
VERBOSE=0

function usage {
    echo "usage: $SCRIPT_NAME OPTIONS"
    echo "Copy/restore custom $APPLICATION_CFG_FILE on localhost"
    echo
    echo "OPTIONS"
    echo "  $CLEAN_OPTION     Restore the original $APPLICATION_CFG_FILE"
    echo "  -v          Turn on verbose logging"
    echo "  -h          Display help"
    exit "$1"
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

function copy_config_into_locker() {
  echo "Copying config into locker ..."
  redirect_output sudo locker run "$LOCKER_NAME" "mv $CONFIG_PATH/$APPLICATION_CFG_FILE $CONFIG_PATH/$APPLICATION_CFG_FILE_MOVED"
  redirect_output sudo locker cp "$LOCKER_NAME" "$APPLICATION_CFG_FILE" "$CONFIG_PATH" --in
  exit_on_failure "Failed to copy config into locker"
}

function cleanup_locker() {
  echo "Cleaning up $LOCKER_NAME"

  echo "Replacing old $APPLICATION_CFG_FILE_MOVED as $APPLICATION_CFG_FILE in locker ..."
  redirect_output sudo locker run "$LOCKER_NAME" "mv $CONFIG_PATH/$APPLICATION_CFG_FILE_MOVED $CONFIG_PATH/$APPLICATION_CFG_FILE"
  exit_on_failure "Failed to replace config file in locker"
}

if [[ $# -gt 3 ]]; then
  usage 2
fi

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

if [[ $CLEAN == 0 ]]; then
  # Copy the application.cfg into locker
  copy_config_into_locker

else # $CLEAN = 1
  cleanup_locker
  rm $APPLICATION_CFG_FILE

  echo "Deleting self ... bye"
  rm -- "$0"
fi
