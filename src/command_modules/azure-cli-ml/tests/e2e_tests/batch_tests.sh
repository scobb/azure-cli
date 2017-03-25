#!/usr/bin/env sh

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <env_file> [-v] [options]\n"
    exit 1
fi

# get directory of script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source $1; shift
python $DIR/batch_happy_path_tests.py $@
python $DIR/batch_unhappy_path_tests.py $@
