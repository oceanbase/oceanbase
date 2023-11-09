FROM openanolis/anolisos

# docker build --build-arg --build-arg LOCAL_RPM="oceanbase-ce-4.3.0.0-1.alios7.aarch64.rpm" --build-arg LOCAL_LIB_RPM="oceanbase-ce-libs-4.3.0.0-1.alios7.aarch64.rpm" -t observer .

ARG VERSION="4.3.0.0"
ARG LOCAL_RPM
ARG LOCAL_LIB_RPM

RUN yum install -y yum-utils && \
    yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    sed -i 's/$releasever/7/' /etc/yum.repos.d/OceanBase.repo && \
    yum install -y ob-deploy obclient ob-sysbench libaio && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

RUN mkdir -p /root/pkg && \
    cd /root/pkg && \
    if [ "${LOCAL_RPM}" != "" ]; then \
    yum install -y --downloadonly --downloaddir=. obagent; \
    else \
    yum install -y --downloadonly --downloaddir=. oceanbase-ce-${VERSION}.el7 oceanbase-ce-libs-${VERSION}.el7 obagent; \
    fi && \
    yum clean all

COPY ${LOCAL_RPM} /root/pkg
COPY ${LOCAL_LIB_RPM} /root/pkg
COPY boot /root/boot/
ENV PATH /root/boot:$PATH
ENV LD_LIBRARY_PATH /root/ob/lib:$LD_LIBRARY_PATH

WORKDIR /root
CMD _boot

EXPOSE 2881
