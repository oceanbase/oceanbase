FROM oceanbase/centos7:latest

ARG VERSION=3.1.3-10100032022041510

RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    yum install -y ob-deploy obclient ob-sysbench wget libaio && \
    rm -rf /usr/obd/mirror/remote/* && \
    rm -rf /u01/mysql /u01/obclient/bin/mysqld* /u01/obclient/bin/aria* /u01/obclient/bin/maria* && \
    yum clean all

RUN mkdir /root/pkg && \
    cd /root/pkg && \
    wget https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/oceanbase-ce-${VERSION}.el7.x86_64.rpm -q && \
    wget https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/oceanbase-ce-libs-${VERSION}.el7.x86_64.rpm -q && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

COPY boot /root/boot/
ENV PATH /root/boot:$PATH

WORKDIR /root
CMD _boot
