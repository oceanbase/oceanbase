#!/bin/bash

#clear env
unalias -a

PWD="$(cd $(dirname $0); pwd)"

OS_RELEASE="$(grep -Po '(?<=release )\d' /etc/redhat-release)" || exit 1
OS_ARCH="$(uname -p)" || exit 1

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
