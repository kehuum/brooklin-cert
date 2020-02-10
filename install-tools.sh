#!/usr/bin/env bash

function info () {
    if [[ -n "$1" ]]; then
        echo "$1"
    fi
}

function error () {
    if [[ -n "$1" ]]; then
        >&2 echo "$1"
    fi
}

function is_python_installed () {
    command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1
}

function is_pip_installed () {
    command -v pip >/dev/null 2>&1
}

function is_pipenv_installed () {
    command -v pipenv >/dev/null 2>&1
}

function install_pip () {
    info "Downloading pip ..."
    
    curl -s https://bootstrap.pypa.io/get-pip.py -o get-pip.py
    if [ $? -ne 0 ]; then
        error "Downloading pip failed"
        return 1
    fi

    info "Installing pip ..."
    python get-pip.py >/dev/null 2>&1
    local result=$?
    if [ $result -ne 0 ]; then
        error "Installing pip failed"
    else
        info "pip installed successfully"
        rm get-pip.py
    fi
    return $result
}

function install_pipenv () {
    info "Installing pipenv ..."
    pip install --user pipenv >/dev/null 2>&1
    local result=$?
    if [ $result -ne 0 ]; then
        error "Installing pipenv failed"
    else
        USER_BIN_DIR="`python -m site --user-base`/bin"
        info "pipenv installed successfully at '$USER_BIN_DIR'. You may want to add this to your PATH"
    fi
    return $result
}

# Verify Python is installed
if ! is_python_installed; then
    error "Python is not installed. Please make sure Python is installed then try running this command."
    exit 1
fi

# Install pip if necessary
if ! is_pip_installed; then
    info "Detected pip is not installed"
    if ! install_pip; then
        exit 1
    fi
fi

# Install pipenv if necessary
if ! is_pipenv_installed; then
    info "Detected pipenv is not installed"
    if ! install_pipenv; then
        exit 1
    fi
fi

info "All necessary prerequisites are installed"
