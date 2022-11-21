#!/bin/bash

#clear env
unalias -a

PWD="$(cd $(dirname $0); pwd)"

OS_ARCH="$(uname -m)" || exit 1
OS_RELEASE="0"

if [[ ! -f /etc/os-release ]]; then
  echo "[ERROR] os release info not found" 1>&2 && exit 1
fi

source /etc/os-release || exit 1

PNAME=${PRETTY_NAME:-"${NAME} ${VERSION}"}
PNAME="${PNAME} (${OS_ARCH})"

function compat_centos8() {
  echo_log "[NOTICE] '$PNAME' is compatible with CentOS 8, use el8 dependencies list"
  OS_RELEASE=8
}

function compat_centos7() {
  echo_log "[NOTICE] '$PNAME' is compatible with CentOS 7, use el7 dependencies list"
  OS_RELEASE=7
}

function not_supported() {
  echo_log "[ERROR] '$PNAME' is not supported yet."
}

function version_ge() {
  test "$(awk -v v1=$VERSION_ID -v v2=$1 'BEGIN{print(v1>=v2)?"1":"0"}' 2>/dev/null)" == "1"
}

function echo_log() {
  echo -e "[dep_create.sh] $@"
}

function echo_err() {
  echo -e "[dep_create.sh][ERROR] $@" 1>&2
}

function get_os_release() {
  if [[ "${OS_ARCH}x" == "x86_64x" ]]; then
    case "$ID" in
      alinux)
        version_ge "2.1903" && compat_centos7 && return
        ;;
      alios)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.2" && compat_centos7 && return
        ;;
      anolis)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.0" && compat_centos7 && return
        ;;
      ubuntu)
        version_ge "16.04" && compat_centos7 && return
        ;;
      centos)
        version_ge "8.0" && OS_RELEASE=8 && return
        version_ge "7.0" && OS_RELEASE=7 && return
        ;;
      debian)
        version_ge "9" && compat_centos7 && return
        ;;
      fedora)
        version_ge "33" && compat_centos7 && return
        ;;
      opensuse-leap)
        version_ge "15" && compat_centos7 && return
        ;;
      #suse
      sles)
        version_ge "15" && compat_centos7 && return
        ;;
      uos)
        version_ge "20" && compat_centos7 && return
        ;;
      arch)
        compat_centos8 && return
        ;;
      rocky)
        version_ge "8.0" && compat_centos8 && return
        ;;
      tencentos)
        version_ge "3.1" && compat_centos8 && return
        ;;
    esac
  elif [[ "${OS_ARCH}x" == "aarch64x" ]]; then
    case "$ID" in
      alios)
        version_ge "8.0" && compat_centos8 && return
        version_ge "7.0" && compat_centos7 && return
        ;;
      centos)
        version_ge "8.0" && OS_RELEASE=8 && return
        version_ge "7.0" && OS_RELEASE=7 && return
        ;;
    esac
  elif [[ "${OS_ARCH}x" == "sw_64x" ]]; then
    case "$ID" in
      UOS)
	version_ge "20" && OS_RELEASE=20 && return
      ;;
    esac
  fi
  not_supported && return 1 
}

get_os_release || exit 1

OS_TAG="el$OS_RELEASE.$OS_ARCH"
DEP_FILE="oceanbase.${OS_TAG}.deps"

MD5=`md5sum ${DEP_FILE} | cut -d" " -f1`

# 是否霢�要共享依赖缓存，默认为ON，在特定条件将OFF
NEED_SHARE_CACHE=ON

WORKSACPE_DEPS_DIR="$(cd $(dirname $0); cd ..; pwd)"
WORKSPACE_DEPS_3RD=${WORKSACPE_DEPS_DIR}/3rd
WORKSAPCE_DEPS_3RD_DONE=${WORKSPACE_DEPS_3RD}/DONE
WORKSAPCE_DEPS_3RD_MD5=${WORKSPACE_DEPS_3RD}/${MD5}

# 弢�始判断本地目录依赖目录是否存圄1�7
if [ -f ${WORKSAPCE_DEPS_3RD_MD5} ]; then
    if [ -f ${WORKSAPCE_DEPS_3RD_DONE} ]; then
        echo_log "${DEP_FILE} has been initialized due to ${WORKSAPCE_DEPS_3RD_MD5} and ${WORKSAPCE_DEPS_3RD_DONE} exists"
        exit 0
    else
        echo_log "${DEP_FILE} has been not initialized, due to ${WORKSAPCE_DEPS_3RD_DONE} not exists"
    fi
else
    echo_log "${DEP_FILE} has been not initialized, due to ${WORKSAPCE_DEPS_3RD_MD5} not exists"
fi		

# 依赖目录不存在，停止缓存
if [ "x${DEP_CACHE_DIR}" == "x" ]; then
    NEED_SHARE_CACHE=OFF
    echo_log "disable share dep cache due to env DEP_CACHE_DIR not set"
else
  if [ -d ${DEP_CACHE_DIR} ]; then
    echo_log "FOUND env DEP_CACHE_DIR: ${DEP_CACHE_DIR}"
  else
      NEED_SHARE_CACHE=OFF
      echo_log "disable share dep cache due to not exist env FOUND DEP_CACHE_DIR(${DEP_CACHE_DIR})"
  fi
fi

# 确定共享依赖缓存目录
CACHE_DEPS_DIR=${DEP_CACHE_DIR}/${MD5}
CACHE_DEPS_DIR_3RD=${DEP_CACHE_DIR}/${MD5}/3rd
CACHE_DEPS_DIR_3RD_DONE=${DEP_CACHE_DIR}/${MD5}/3rd/DONE
CACHE_DEPS_LOCKFILE=${DEP_CACHE_DIR}/${MD5}.lockfile

# 确定临时目录地址，如果关闭缓存，该目录也是实际目录的地址
UUID=`cat /proc/sys/kernel/random/uuid`
TARGET_DIR=${DEP_CACHE_DIR}/${MD5}.${UUID}
TARGET_DIR_3RD=${DEP_CACHE_DIR}/${MD5}.${UUID}/3rd

# 保留环境变量入口，停止共享依赖缓孄1�7
if [ "x${DISABLE_SHARE_DEP_CACHE}" == "x1" ]; then
    NEED_SHARE_CACHE=OFF
    echo_log "disable share deps cache due to env DISABLE_SHARE_DEP_CACHE=1"
fi

if [ $NEED_SHARE_CACHE == "OFF" ]; then
    # 不共享缓存，二则是相等的，直接下到当前的工作目录
    TARGET_DIR_3RD=${WORKSPACE_DEPS_3RD}
fi

# 删除本地依赖文件
rm -rf ${WORKSPACE_DEPS_3RD}

if [ ${NEED_SHARE_CACHE} == "ON" ]; then
    # 判断共享目录是否存在
    if [ -f ${CACHE_DEPS_DIR_3RD_DONE} ]; then
        echo_log "use cache deps ${WORKSPACE_DEPS_3RD} -> ${CACHE_DEPS_DIR_3RD}"
        ln -sf ${CACHE_DEPS_DIR_3RD} ${WORKSPACE_DEPS_3RD}
        exit $?
    else
        echo_log "cache deps ${CACHE_DEPS_DIR_3RD_DONE} not exist"
    fi
fi

if [[ ! -f "${DEP_FILE}" ]]; then
    echo_err "check dependencies profile for ${DEP_FILE}... NOT FOUND"
    exit 2
else
    echo_log "check dependencies profile for ${DEP_FILE}... FOUND"
fi

declare -A targets
declare -A packages
section="default"
content=""

function save_content {
    if [[ "$content" != "" ]] 
    then
        if [[ $(echo "$section" | grep -E "^target\-") != "" ]]
        then
            target_name=$(echo $section | sed 's|^target\-\(.*\)$|\1|g')
            targets["$target_name"]="$(echo "${content}" | grep -Eo "repo=.*" | awk -F '=' '{ print $2 }')"
            echo_log "target: $target_name, repo: ${targets["$target_name"]}"
        else
            packages["$section"]=$content
        fi
    fi
}
echo_log "check repository address in profile..."

while read -r line
do
    if [[ $(echo "$line" | grep -E "\[.*\]") != "" ]]
    then
        save_content
        content=""
        # section=${line//\[\(.*\)\]/\1}
        section=$(echo $line | sed 's|.*\[\(.*\)\].*|\1|g')
    else
        [[ "$line" != "" ]] && [[ "$line" != '#'* ]] && content+=$'\n'"$line"
    fi
done < $DEP_FILE 
save_content

# 真正弢�始下轄1�7
echo_log "start to download dependencies..."
mkdir -p "${TARGET_DIR_3RD}/pkg"
for sect in "${!packages[@]}"
do
    while read -r line
    do
        [[ "$line" == "" ]] && continue
        pkg=${line%%\ *}
        target_name="default"
        temp=$(echo "$line" | grep -Eo "target=(\S*)")
        [[ "$temp" != "" ]] && target_name=${temp#*=}
        if [[ -f "${TARGET_DIR_3RD}/pkg/${pkg}" ]]; then
            echo_log "find package <${pkg}> in cache"
        else
            echo_log "downloading package <${pkg}>"
            repo=${targets["$target_name"]}
            TEMP=$(mktemp -p "/" -u ".${pkg}.XXXX")
            wget "$repo/${pkg}" -O "${TARGET_DIR_3RD}/pkg/${TEMP}" &> ${TARGET_DIR_3RD}/pkg/error.log
            if (( $? == 0 )); then
                mv -f "${TARGET_DIR_3RD}/pkg/$TEMP" "${TARGET_DIR_3RD}/pkg/${pkg}"
                rm -rf ${TARGET_DIR_3RD}/pkg/error.log
            else
                cat ${TARGET_DIR_3RD}/pkg/error.log
                rm -rf "${TARGET_DIR_3RD}/pkg/$TEMP"
                echo_err "wget $repo/${pkg}"
                echo_err "Failed to init rpm deps"
                exit 4
            fi
        fi
        echo_log "unpack package <${pkg}>... \c"
        if [ "$ID" = "arch" ]; then
          (cd ${TARGET_DIR_3RD} && rpmextract.sh "${TARGET_DIR_3RD}/pkg/${pkg}")
        else
          (cd ${TARGET_DIR_3RD} && rpm2cpio "${TARGET_DIR_3RD}/pkg/${pkg}" | cpio -di -u --quiet)
        fi
        if [[ $? -eq 0 ]]; then
          echo "SUCCESS"
        else
          echo "FAILED" 1>&2
          echo_log "[ERROR] Failed to init rpm deps"
          exit 5
        fi
    done <<< "${packages["$sect"]}"
done

# 不进行缓存工作，直接结束
if [ ${NEED_SHARE_CACHE} == "OFF" ]; then
    touch ${WORKSAPCE_DEPS_3RD_MD5}
    touch ${WORKSAPCE_DEPS_3RD_DONE}
    exit $?
fi

# 链接缓存目录
LINK_CHACE_DIRECT=OFF
# 链接当前目标目录
LINK_TARGET_DIRECT=OFF

# 下载完成之后，发现目标已经存在，直接进行软链掄1�7
if [ -d ${CACHE_DEPS_DIR} ]; then
    echo_log "found ${CACHE_DEPS_DIR} exists"
    if [ -f ${CACHE_DEPS_DIR_3RD_DONE} ]; then
        echo_log "found ${CACHE_DEPS_DIR_3RD_DONE} exists"
        LINK_CHACE_DIRECT=ON
    else
        echo_log "not found ${CACHE_DEPS_DIR_3RD_DONE} exists"
        LINK_TARGET_DIRECT=ON
    fi
fi

if [ -f ${CACHE_DEPS_LOCKFILE} ];then
    # 超过丢�分钟的锁文件将失效，也将重新初始化缓孄1�7
    echo_log "found lock file ${CACHE_DEPS_LOCKFILE}"
    LINK_TARGET_DIRECT=ON
    if test `find "${CACHE_DEPS_LOCKFILE}" -mmin +1`; then
        echo_log "lock file ${CACHE_DEPS_LOCKFILE} escape 1 mins, and try to unlock"
        rm -rf ${CACHE_DEPS_LOCKFILE}
        if [ $? -eq 0 ]; then
            LINK_TARGET_DIRECT=OFF
            echo_log "unlock success"
        else
            echo_log "failed to unlock"
        fi
    else
        echo_log "lock file ${CACHE_DEPS_LOCKFILE} in 1 mins"
    fi    
fi

if [ ${LINK_CHACE_DIRECT}  == "ON" ]; then
    echo_log "give up current file and link direct, ${WORKSPACE_DEPS_3RD} -> ${CACHE_DEPS_DIR_3RD}"
    rm -rf ${TARGET_DIR}
    echo_log "link deps  ${WORKSPACE_DEPS_3RD} -> ${CACHE_DEPS_DIR_3RD}"
    ln -sf ${CACHE_DEPS_DIR_3RD} ${WORKSPACE_DEPS_3RD}
    exit $?
fi

if [ ${LINK_TARGET_DIRECT} == "ON" ]; then
    # 放弃缓存，直接软链接该目彄1�7
    echo_log "give up mv and link dirct, ${WORKSPACE_DEPS_3RD} -> ${TARGET_DIR_3RD}"
    ln -sf ${TARGET_DIR_3RD} ${WORKSPACE_DEPS_3RD}
    if [ $? -ne 0 ]; then
        echo_err "Failed to link ${WORKSPACE_DEPS_3RD} to ${TARGET_DIR_3RD}"
        exit 1
    fi
    touch ${WORKSAPCE_DEPS_3RD_MD5}
    touch ${WORKSAPCE_DEPS_3RD_DONE}
    exit $?
fi

touch ${CACHE_DEPS_LOCKFILE}
chmod 777 ${CACHE_DEPS_LOCKFILE}
echo_log "generate lock file ${CACHE_DEPS_LOCKFILE}"

mv ${TARGET_DIR} ${CACHE_DEPS_DIR}
if [ $? -ne 0 ]; then
    echo_err "Failed to mv ${TARGET_DIR} to ${CACHE_DEPS_DIR}"
    exit 1
fi

ln -sf ${CACHE_DEPS_DIR_3RD} ${WORKSPACE_DEPS_3RD}
if [ $? -ne 0 ]; then
    echo_err "Failed to link ${CACHE_DEPS_DIR_3RD} to ${WORKSPACE_DEPS_3RD}"
    exit 1
fi

rm -rf ${CACHE_DEPS_LOCKFILE}
echo_log "unlock lock file ${CACHE_DEPS_LOCKFILE}"

echo_log "link deps ${WORKSPACE_DEPS_3RD} -> ${CACHE_DEPS_DIR_3RD}"

# 标记md5和done文件
touch ${WORKSAPCE_DEPS_3RD_MD5}
touch ${WORKSAPCE_DEPS_3RD_DONE}
