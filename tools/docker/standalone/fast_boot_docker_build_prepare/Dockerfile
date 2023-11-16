FROM openanolis/anolisos

# docker build --build-arg LOCAL_RPM="oceanbase-ce-4.3.0.0-1.alios7.aarch64.rpm" --build-arg LOCAL_LIB_RPM="oceanbase-ce-libs-4.3.0.0-1.alios7.aarch64.rpm" -t raw_observer .

# Maybe another default version?
ARG VERSION="4.3.0.0"
ARG LOCAL_RPM
ARG LOCAL_LIB_RPM

RUN yum install -y yum-utils && \
    yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    sed -i 's/$releasever/7/' /etc/yum.repos.d/OceanBase.repo && \
    yum install -y libaio mysql && \
    yum clean all

RUN mkdir -p /root/pkg && mkdir -p /root/store && mkdir -p /root/dest && \
    if [ "${LOCAL_RPM}" == "" ]; then \
    yum install -y --downloadonly --downloaddir=. oceanbase-ce-${VERSION}.el7 oceanbase-ce-libs-${VERSION}.el7; \
    fi && \
    yum clean all

COPY ${LOCAL_RPM} /root/pkg
COPY ${LOCAL_LIB_RPM} /root/pkg
COPY boot /root/boot/
ENV PATH /root/boot:$PATH
ENV LD_LIBRARY_PATH /home/admin/oceanbase/lib:/root/ob/lib:$LD_LIBRARY_PATH
ENV BOOT_LOCAL_RPM ${LOCAL_RPM}
ENV BOOT_LOCAL_LIB_RPM ${LOCAL_LIB_RPM}

WORKDIR /root
CMD ["sh", "-c", "_boot ${BOOT_LOCAL_RPM} ${BOOT_LOCAL_LIB_RPM}"]