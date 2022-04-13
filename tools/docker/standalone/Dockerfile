FROM oceanbase/centos7:latest
RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && \
    yum install -y ob-deploy-1.2.1 obclient ob-sysbench wget libaio && \
    mkdir /root/pkg && \
    cd /root/pkg && \
    wget https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/oceanbase-ce-3.1.3-10000292022032916.el7.x86_64.rpm -q && \
    wget https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/oceanbase-ce-libs-3.1.3-10000292022032916.el7.x86_64.rpm -q && \
    rm -rf /usr/obd/mirror/remote/* && \
    obd mirror clone *.rpm && \
    obd mirror list local && \
    rm -rf /root/base /root/pkg && \
    yum clean all
COPY boot /root/boot/
ENV PATH /root/boot:$PATH
CMD _boot
