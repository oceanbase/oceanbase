#!/bin/bash
# Usage: oceanbase-build.sh <oceanbasepath> <version> <release>
# Usage: oceanbase-build.sh

REDHAT=`cat /etc/redhat-release|cut -d " " -f 4|cut -d "." -f 1`


if [ $# -ne 3 ]
then
	TOP_DIR=`pwd`/../../../
	VERSION=`grep 'VERSION' ${TOP_DIR}/CMakeLists.txt | awk 'NR==2' | xargs | cut -d ' ' -f 2`
	RELEASE="test.an${REDHAT}"
else
	TOP_DIR=$1
	VERSION=$2
	RELEASE="$3.an${REDHAT}"
fi

LTO_JOBS=${LTO_JOBS:-5}

echo "[BUILD] args: TOP_DIR=${TOP_DIR} VERSION=${VERSION} RELEASE=${RELEASE}"
echo "[BUILD] make rpms..."
rpmbuild --define "src_dir ${TOP_DIR}" --define "VERSION ${VERSION}" --define "RELEASE ${RELEASE}" --define "lto_jobs ${LTO_JOBS}" -ba oceanbase-ce.spec
echo "[BUILD] make rpms done"

mv ~/rpmbuild/RPMS/x86_64/oceanbase-ce*.rpm .