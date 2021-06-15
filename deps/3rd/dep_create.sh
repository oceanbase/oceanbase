#!/bin/bash

#clear env
unalias -a

PWD="$(cd $(dirname $0); pwd)"

OS_ARCH="$(uname -p)" || exit 1
OS_RELEASE="0"

if [[ ! -f /etc/os-release ]]; then
  echo "[ERROR] os release info not found" 1>&2 && exit 1
fi

source /etc/os-release || exit 1

PNAME=${PRETTY_NAME:-${NAME} ${VERSION}}

function compat_centos8() {
  echo "[NOTICE] '$PNAME' is compatible with CentOS 8, use el8 dependencies list"
  OS_RELEASE=8
}

function compat_centos7() {
  echo "[NOTICE] '$PNAME' is compatible with CentOS 7, use el7 dependencies list"
  OS_RELEASE=7
}

function not_supported() {
  echo "[ERROR] '$PNAME' is not supported yet."
}

function version_ge() {
  test "$(awk -v v1=$VERSION_ID -v v2=$1 'BEGIN{print(v1>=v2)?"1":"0"}' 2>/dev/null)" == "1"
}

function get_os_release() {
  case "$ID" in
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
  esac
  not_supported && return 1 
}

get_os_release || exit 1

OS_TAG="el$OS_RELEASE.$OS_ARCH"
DEP_FILE="oceanbase.${OS_TAG}.deps"

echo -e "check dependencies profile for ${OS_TAG}... \c"

if [[ ! -f "${DEP_FILE}" ]]; then
    echo "NOT FOUND" 1>&2
    exit 2
else
    echo "FOUND"
fi

mkdir "${PWD}/pkg" >/dev/null 2>&1

echo -e "check repository address in profile... \c"
REPO="$(grep -Po '(?<=repo=).*' "${DEP_FILE}" 2>/dev/null)"
if [[ $? -eq 0 ]]; then
    echo "$REPO"
else
    echo "NOT FOUND" 1>&2
    exit 3
fi

echo "download dependencies..."
RPMS="$(grep '\.rpm' "${DEP_FILE}" | grep -Pv '^#')"

for pkg in $RPMS
do
  if [[ -f "${PWD}/pkg/${pkg}" ]]; then
    echo "find package <${pkg}> in cache"
  else
    echo -e "download package <${pkg}>... \c"
    TEMP=$(mktemp -p "/" -u ".${pkg}.XXXX")
    wget "$REPO/${pkg}" -q -O "${PWD}/pkg/${TEMP}"
    if [[ $? -eq 0 ]]; then
      mv -f "${PWD}/pkg/$TEMP" "${PWD}/pkg/${pkg}"
      echo "SUCCESS"
    else
      rm -rf "${PWD}/pkg/$TEMP"
      echo "FAILED" 1>&2
      exit 4
    fi
  fi
  echo -e "unpack package <${pkg}>... \c"
  rpm2cpio "${PWD}/pkg/${pkg}" | cpio -di -u --quiet

  if [[ $? -eq 0 ]]; then
    echo "SUCCESS"
  else
    echo "FAILED" 1>&2
    exit 5
  fi
done
