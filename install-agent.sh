#!/usr/bin/env bash

SCRIPT_NAME=$(basename "$0")
BROOKLIN_AGENT_OPTION="--brooklin"
CLEAN_OPTION="--clean"
BROOKLIN_SERVICE="brooklin-service"
BROOKLIN_CERTIFICATION_DIR="brooklin-certification-*"
BROOKLIN_CERTIFICATION_TARBALL="$BROOKLIN_CERTIFICATION_DIR.tar.gz"
RUN_AGENT_SCRIPT="run-agent.sh"
AGENT_LOG="../agent.log"

INCLUDE_BROOKLIN_AGENT=0
CLEAN=0
VERBOSE=0

function usage {
    echo "usage: $SCRIPT_NAME OPTIONS"
    echo "Install or uninstall Brooklin's test agent on localhost"
    echo
    echo "OPTIONS"
    echo "  $CLEAN_OPTION     Remove an installed agent"
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

function generate_run_agent_script() {
  echo "Generating $RUN_AGENT_SCRIPT ..."
  exit_on_failure "Cannot find scripts directory"

  cat > $RUN_AGENT_SCRIPT << EOL
#!/usr/bin/env bash

pkill -f agent.server   # Kill the agent if it's already running

cd /export/content/lid/apps/"$2"/i001/$BROOKLIN_CERTIFICATION_DIR
/export/apps/python/3.7/bin/python3 -m agent.server $1 >$AGENT_LOG &
EOL
  chmod +x $RUN_AGENT_SCRIPT
}

function deploy_agent_in_locker() {
  generate_run_agent_script "$1"  "$2" # $1 (e.g. --brooklin), # $2 (e.g. brooklin-service)
  exit_on_failure "Failed to generate script to run the agent"

  LOCKER_NAME="$2"_i001

  echo "Copying scripts into locker ..."
  redirect_output sudo locker cp "$LOCKER_NAME" "$BROOKLIN_CERTIFICATION_DIR" /export/content/lid/apps/"$2"/i001/ --in
  exit_on_failure "Failed to copy scripts into locker"

  echo "Copying $RUN_AGENT_SCRIPT into locker ..."
  redirect_output sudo locker cp "$LOCKER_NAME" $RUN_AGENT_SCRIPT /export/content/lid/apps/"$2"/i001/ --in
  exit_on_failure "Failed to copy $RUN_AGENT_SCRIPT into locker"

  echo "Launching agent ..."
  redirect_output sudo locker run "$LOCKER_NAME" "nohup sh ./$RUN_AGENT_SCRIPT &"

  rm -rf ./$RUN_AGENT_SCRIPT
}

function cleanup_locker() {
  LOCKER_NAME="$1_i001"
  echo "Cleaning up $LOCKER_NAME"
  redirect_output sudo locker run "$LOCKER_NAME" "pkill -f agent.server"
  redirect_output sudo locker run "$LOCKER_NAME" "find . -name $BROOKLIN_CERTIFICATION_DIR -type d -exec rm -rf -- '{}' +"
  redirect_output sudo locker run "$LOCKER_NAME" "rm -rf ./$RUN_AGENT_SCRIPT"
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

if [[ $CLEAN == 0 ]]; then
  # Extract scripts tarball
  echo "Extracting scripts tarball ..."
  redirect_output tar -zxvf "$(find . -name "$BROOKLIN_CERTIFICATION_TARBALL" -type f)"
  exit_on_failure "Failed to extract scripts tarball"

  BROOKLIN_CERTIFICATION_DIR=$(find . -name "$BROOKLIN_CERTIFICATION_DIR" -type d | cut -c3-)

  # Generate script to run agent and copy it within locker
  deploy_agent_in_locker $BROOKLIN_AGENT_OPTION $BROOKLIN_SERVICE

  rm -rf "$BROOKLIN_CERTIFICATION_DIR"

else # $CLEAN = 1
  cleanup_locker $BROOKLIN_SERVICE

  echo "Deleting scripts and tarballs ..."
  find . -name "$BROOKLIN_CERTIFICATION_DIR" -exec rm -rf -- '{}' +

  echo "Deleting self ... bye"
  rm -- "$0"
fi
