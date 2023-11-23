STANDALONE_OB_BIN_PATH=$1
STANDALONE_OB_HOME_PATH=$2
STANDALONE_OB_DATA_PATH=$3
COMMAND=$4
NEED_CREATE_SOFT_LINK=$5

function clear_env() {
    if [ "${NEED_CREATE_SOFT_LINK}" != "" ]; then
        rm -rf ${STANDALONE_OB_HOME_PATH}/observer
    fi
    rm -rf ${STANDALONE_OB_HOME_PATH}/audit ${STANDALONE_OB_HOME_PATH}/etc* \
           ${STANDALONE_OB_HOME_PATH}/log ${STANDALONE_OB_HOME_PATH}/run \
           ${STANDALONE_OB_HOME_PATH}/wallet
    rm -rf ${STANDALONE_OB_DATA_PATH}
}

function build_env() {
    if [ "${NEED_CREATE_SOFT_LINK}" != "" ]; then
        ln -s ${STANDALONE_OB_BIN_PATH} ${STANDALONE_OB_HOME_PATH}/observer
    fi
    mkdir ${STANDALONE_OB_DATA_PATH}
    cd ${STANDALONE_OB_DATA_PATH} && mkdir clog slog sstable
}

case $COMMAND in
    -C | --clear)
    clear_env
    ;;
    -B | --build)
    build_env
    ;;
esac