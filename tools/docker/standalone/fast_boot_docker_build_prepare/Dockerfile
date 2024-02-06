FROM openanolis/anolisos

# docker build --build-arg VERSION={VERSION} .
ARG VERSION

RUN yum install -y yum-utils && \
    yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    sed -i 's/$releasever/7/' /etc/yum.repos.d/OceanBase.repo && \
    yum install -y libaio mysql && \
    yum clean all

RUN mkdir -p /root/pkg && mkdir -p /root/store && mkdir -p /root/dest && \
    cd /root/pkg && \
    yum install -y --downloadonly --downloaddir=. oceanbase-ce-${VERSION}.el7 oceanbase-ce-libs-${VERSION}.el7 && \
    rm -rf /usr/obd/mirror/remote/* && \
    yum clean all

COPY boot /root/boot/
COPY init_store_for_fast_start.py /root/boot/
ENV PATH /root/boot:$PATH
ENV LD_LIBRARY_PATH /home/admin/oceanbase/lib:/root/ob/lib:$LD_LIBRARY_PATH

WORKDIR /root
CMD _boot