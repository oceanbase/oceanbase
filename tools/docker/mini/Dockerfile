FROM oceanbase/centos7:latest
ARG GIT_SOURCE=github

RUN yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo && yum install -y ob-deploy-1.2.1 obclient ob-sysbench wget rpm* cpio make glibc-devel glibc-headers m4 git obclient && mkdir /root/pkg && cd /root/pkg && rm -rf /usr/obd/mirror/remote/* && yumdownloader oceanbase-ce oceanbase-ce-libs && obd mirror clone *rpm && obd mirror list local && rm -rf * && yum clean all

WORKDIR /root/.obd/plugins/oceanbase/3.1.0
RUN sed -i 's/system_memory = max(4 << 30, system_memory)/system_memory = max(3 << 30, system_memory)/g' generate_config.py || echo 'system_memory changed'
RUN sed -i 's/if memory_limit < MIN_MEMORY:/if False and (memory_limit < MIN_MEMORY):/g' generate_config.py || echo 'memory_limit changed'
RUN sed -i '/_start_check(plugin_context, strict_check)/a\    success = True' start_check.py || echo 'check changed'

# clone code
RUN mkdir -p /root/source \
    && cd /root/source \
    && git clone https://${GIT_SOURCE}.com/oceanbase/oceanbase

WORKDIR /root/source/oceanbase

RUN sed -i 's/const int64_t ObRunningModeConfig::MINI_MEM_LOWER = 8L << 30;/const int64_t ObRunningModeConfig::MINI_MEM_LOWER = 4L << 30;/g' deps/oblib/src/lib/ob_running_mode.cpp || echo "ob_running_mode.cpp no need patched"
RUN sh build.sh debug --init \
    && sh build.sh --make -j4 \
    && cd build_debug \
    && make DESTDIR=. install \
    && obd mirror create -n oceanbase-ce -V 100.0.0 -p ./usr/local -t test -f

COPY boot /root/boot/
ENV PATH /root/boot:$PATH
WORKDIR /root
CMD _boot
