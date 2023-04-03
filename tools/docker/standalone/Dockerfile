FROM centos:centos7

ARG VERSION

RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    yum install -y ob-deploy obclient ob-sysbench libaio && \
    rm -rf /usr/obd/mirror/remote/* && \
    rm -rf /u01/mysql /u01/obclient/bin/mysqld* /u01/obclient/bin/aria* /u01/obclient/bin/maria* && \
    yum clean all

RUN mkdir /root/pkg && \
    cd /root/pkg && \
    yum install --downloadonly --downloaddir=. oceanbase-ce-${VERSION}.el7 oceanbase-ce-libs-${VERSION}.el7 obagent && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

COPY boot /root/boot/
ENV PATH /root/boot:$PATH

WORKDIR /root
CMD _boot
