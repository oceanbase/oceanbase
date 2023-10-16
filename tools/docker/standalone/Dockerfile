FROM openanolis/anolisos

# docker build --build-arg VERSION={VERSION} .
ARG VERSION

RUN yum install -y yum-utils && \
    yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    sed -i 's/$releasever/7/' /etc/yum.repos.d/OceanBase.repo && \
    yum install -y ob-deploy obclient ob-sysbench libaio && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

RUN mkdir -p /root/pkg && \
    cd /root/pkg && \
    yum install -y --downloadonly --downloaddir=. oceanbase-ce-${VERSION}.el7 oceanbase-ce-libs-${VERSION}.el7 obagent && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

COPY boot /root/boot/
ENV PATH /root/boot:$PATH

WORKDIR /root
CMD _boot

EXPOSE 2881
