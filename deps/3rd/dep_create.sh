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

echo -e "check dependencies profile for ${OS_TAG}... \c"

if [[ ! -f "${DEP_FILE}" ]]; then
    echo "NOT FOUND" 1>&2
    exit 2
else
    echo "FOUND"
fi

mkdir "${PWD}/pkg" >/dev/null 2>&1

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
            echo "target: $target_name, repo: ${targets["$target_name"]}"
        else
            packages["$section"]=$content
        fi
    fi
}
echo -e "check repository address in profile..."

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

echo "download dependencies..."
for sect in "${!packages[@]}"
do
    if [[ "$1" != "all" ]]
    then
        [[ "$sect" == "test-utils" ]] && continue
    fi
    echo "${packages["$sect"]}" | while read -r line
    do
        [[ "$line" == "" ]] && continue
        pkg=${line%%\ *}
        target_name="default"
        temp=$(echo "$line" | grep -Eo "target=(\S*)")
        [[ "$temp" != "" ]] && target_name=${temp#*=}
        if [[ -f "${PWD}/pkg/${pkg}" ]]; then
            echo "find package <${pkg}> in cache"
        else
            echo -e "download package <${pkg}>... \c"
            repo=${targets["$target_name"]}
            TEMP=$(mktemp -p "/" -u ".${pkg}.XXXX")
            wget "$repo/${pkg}" -q -O "${PWD}/pkg/${TEMP}"
            if (( $? == 0 )); then
                mv -f "${PWD}/pkg/$TEMP" "${PWD}/pkg/${pkg}"
                echo "SUCCESS"
            else
                rm -rf "${PWD}/pkg/$TEMP"
                echo "FAILED" 1>&2
                exit 4
            fi
        fi
        echo -e "unpack package <${pkg}>... \c"
        if [ "$ID" = "arch" ]; then
          rpmextract.sh "${PWD}/pkg/${pkg}"
        else
          rpm2cpio "${PWD}/pkg/${pkg}" | cpio -di -u --quiet
        fi
        if [[ $? -eq 0 ]]; then
          echo "SUCCESS"
        else
          echo "FAILED" 1>&2
          exit 5
        fi
    done
done
