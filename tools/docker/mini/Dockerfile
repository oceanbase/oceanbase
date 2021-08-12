FROM oceanbase/centos7:latest
RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && yum install -y ob-deploy obclient ob-sysbench && mkdir /root/pkg && cd /root/pkg && rm -rf /usr/obd/mirror/remote/* && yumdownloader oceanbase-ce oceanbase-ce-libs && obd mirror clone *rpm && obd mirror list local && rm -rf * && yum clean all
COPY boot /root/boot/
ENV PATH /root/boot:$PATH
CMD _boot
