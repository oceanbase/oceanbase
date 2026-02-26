#!/bin/bash -x
SOURCE_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})/../..")

# 解析参数
WITH_JAVA=false
BUILD_DIR=""

# 支持环境变量 OB_DO_WITH_JAVA
[[ "$OB_DO_WITH_JAVA" == "1" ]] && WITH_JAVA=true

for arg in "$@"; do
  case $arg in
    --with-java)
      WITH_JAVA=true
      ;;
    *)
      if [[ -z "$BUILD_DIR" ]]; then
        BUILD_DIR=$arg
      fi
      ;;
  esac
done

if [[ -z "$BUILD_DIR" ]]; then
  BUILD_DIR=$(find $SOURCE_DIR -maxdepth 1 -name 'build_*' -type d | grep -v 'build_ccls'  | head -1)
  if [[ "$BUILD_DIR" == "" ]]; then
    echo "Usage ./copy.sh [oceanbase_dev_dir] [--with-java]"
  else
    echo "Choose $BUILD_DIR as build directory of oceanbase."
  fi
fi

BIN_DIR=`pwd`/bin${VER}
LIB_DIR=`pwd`/lib
TOOL_DIR=`pwd`/tools
ETC_DIR=`pwd`/etc
DEBUG_DIR=`pwd`/debug
ADMIN_DIR=`pwd`/admin

function do_install {
  quiet=false
  if [ $# -eq 3 ] && [[ "$3" == "true" ]]
  then
    quiet=true
  fi
  [[ "$quiet" == "false" ]] && echo -n "Installing $1 "
  sources=$(ls $1 2>/dev/null)
  if [[ "$sources" == "" ]]
  then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\nNo such file: $1"
    if [ "$quiet" == "false" ]
    then
      return 1
    else
      return 0
    fi
  fi
  target=$2
  err_msg=$(libtool --mode=install cp $sources $target 2>&1 >/dev/null)
  if [ $? -eq 0 ]
  then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;32mOK\033[0m"
  else
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\n$err_msg"
  fi
}

function do_install_python3 {
  quiet=false
  if [ $# -eq 3 ] && [[ "$3" == "true" ]]
  then
    quiet=true
  fi
  [[ "$quiet" == "false" ]] && echo -n "Installing $1 "
  if [ ! -e "$1" ]; then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\nNo such file: $1"
    if [ "$quiet" == "false" ]
    then
      return 1
    else
      return 0
    fi
  fi
  if [ -d "$1" ]; then
    err_msg=$(cp -r "$1" "$2"/ 2>&1)
  else
    do_install "$@"
  fi
  if [ $? -eq 0 ]
  then
    [[ "$quiet" == "false" ]] && echo -e "\033[0;32mOK\033[0m"
  else
    [[ "$quiet" == "false" ]] && echo -e "\033[0;31mFAIL\033[0m\n$err_msg"
  fi
}

function do_install_java_extensions {
  local target_dir=$1
  local deps_compat_file="$SOURCE_DIR/tools/upgrade/deps_compat.yml"

  # 从 deps_compat.yml 读取 jar 版本
  if [[ ! -f "$deps_compat_file" ]]; then
    echo -e "\033[0;31mFAIL\033[0m deps_compat.yml not found"
    return 1
  fi

  local jar_version=$(grep -A1 '^java:' "$deps_compat_file" | grep 'jar:' | sed 's/.*jar:[[:space:]]*//')
  if [[ -z "$jar_version" ]]; then
    echo -e "\033[0;31mFAIL\033[0m jar version not found in deps_compat.yml"
    return 1
  fi

  echo "Looking for java-extensions version: $jar_version"

  # 查找匹配版本的 rpm
  local rpm_pattern="devdeps-java-extensions-${jar_version}-"
  local target_rpm=$(curl -fsSL "$base_url/" | grep -oE "devdeps-java-extensions-${jar_version}-[^\"]+\.rpm" | sort -V | tail -1)

  if [[ -z "$target_rpm" ]]; then
    echo -e "\033[0;33mWARN\033[0m No rpm found for version $jar_version, trying latest"
    target_rpm=$(curl -fsSL "$base_url/" | grep -oE 'devdeps-java-extensions-[^"]+\.rpm' | sort -V | tail -1)
  fi

  if [[ -z "$target_rpm" ]]; then
    echo -e "\033[0;31mFAIL\033[0m No java-extensions rpm found"
    return 1
  fi

  echo "Downloading $target_rpm"
  wget "$base_url/$target_rpm" -O ./java-extensions.rpm -o ./wget_java_ext.log
  if [[ $? -ne 0 ]]; then
    echo -e "\033[0;31mFAIL\033[0m Failed to download $target_rpm"
    return 1
  fi

  rpm2cpio ./java-extensions.rpm | cpio -idmv
  # 保留目录结构复制 jni_packages
  local jni_packages_dir="$target_dir/jni_packages"
  mkdir -p "$jni_packages_dir"
  cp -rf ./__PREFIX_REQUIRED__/oceanbase/jni_packages/* "$jni_packages_dir"/
  # 创建 current 软链接指向版本目录
  local version_dir=$(ls -d "$jni_packages_dir"/*/ 2>/dev/null | head -1)
  if [[ -n "$version_dir" ]]; then
    version_dir=${version_dir%/}  # 去掉末尾斜杠
    local version_name=$(basename "$version_dir")
    ln -sfn "$version_name" "$jni_packages_dir/current"
  fi
  # 清理临时文件和目录
  rm -f ./java-extensions.rpm
  rm -rf ./__PREFIX_REQUIRED__ ./usr
  echo -e "\033[0;32mOK\033[0m java-extensions installed to $jni_packages_dir"
}

if [ $# -lt 2 ]
then
  mkdir -p $BIN_DIR
  mkdir -p $LIB_DIR
  mkdir -p $TOOL_DIR
  mkdir -p $ETC_DIR
  mkdir -p $DEBUG_DIR
  mkdir -p $ADMIN_DIR
  if [ -f $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la ]; then
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/liblz4_1.0.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libnone.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libsnappy_1.0.la $LIB_DIR
    do_install $SOURCE_DIR/deps/oblib/src/lib/compress/libzlib_1.0.la $LIB_DIR
  fi
  do_install $BUILD_DIR/src/observer/observer $BIN_DIR/observer
  do_install "$BUILD_DIR/syspack_release/*" $ADMIN_DIR
  do_install $SOURCE_DIR/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-symbolizer $TOOL_DIR/
  do_install $SOURCE_DIR/rpm/.dep_create/lib/libstdc++.so.6 $LIB_DIR true
  do_install "$SOURCE_DIR/tools/timezone*.data" $ETC_DIR
  do_install $SOURCE_DIR/deps/oblib/src/lib/profile/obperf $TOOL_DIR/ true
  do_install $SOURCE_DIR/deps/3rd/home/admin/oceanbase/bin/obshell $BIN_DIR/obshell true
  do_install "$SOURCE_DIR/tools/spatial_reference_systems.data" $ETC_DIR


  do_install ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libclntsh.so.12.1 $LIB_DIR/libclntsh.so true
  do_install ./usr/lib/oracle/12.2/client64/lib/libclntshcore.so.12.1 $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libnnz12.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libons.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libociei.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libmql1.so $LIB_DIR true
  do_install ./usr/lib/oracle/12.2/client64/lib/libipc1.so $LIB_DIR true


  do_install_python3 ./usr/local/oceanbase/deps/devel/python3/lib/python3.13 $LIB_DIR true
  do_install_python3 ./usr/local/oceanbase/deps/devel/python3/lib/pkgconfig $LIB_DIR true
  do_install_python3 ./usr/local/oceanbase/deps/devel/python3/lib/libpython3.so $LIB_DIR true
  do_install_python3 ./usr/local/oceanbase/deps/devel/python3/lib/libpython3.13.so.1.0 $LIB_DIR true

  do_install $SOURCE_DIR/deps/3rd/usr/local/oceanbase/deps/devel/lib/libhdfs.so $LIB_DIR true

  # 下载 java-extensions jar 到 lib (仅当指定 --with-java 时)
  if [[ "$WITH_JAVA" == "true" ]]; then
    do_install_java_extensions $LIB_DIR
  fi

fi
