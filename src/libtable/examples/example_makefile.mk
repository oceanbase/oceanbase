all: pstore_example_d pstore_example_s

OBINSTALLDIR=/tmpfs/zhuweng.yzf/home/admin/oceanbase/
DEP_DIR=/home/zhuweng.yzf/myWorkspace/oceanbase/rpm/.dep_create
OSS_DIR=/home/zhuweng.yzf/myWorkspace/oceanbase/src/library/src/lib/restore/oss/
LIBS=-l:libvsclient.a -l:liboss_c_sdk.a -l:libaprutil-1.a -l:libapr-1.a -l:libmxml.a -l:libeasy.a -lpthread -lc -lm -lrt -ldl -laio -l:libcurl.a -lssl -lcrypt

LDFLAGS=-L/usr/lib64/ -L$(OBINSTALLDIR)/lib -L$(DEP_DIR)/lib -L$(OSS_DIR)/lib
CPPFLAGS=-I$(OBINSTALLDIR)/include -I$(OBINSTALLDIR)/include/easy  -I${DEP_DIR}/include -I${DEP_DIR}/include/mysql
CXXFLAGS=-std=gnu++11 -g -O2 -D_GLIBCXX_USE_CXX11_ABI=0 -D_OB_VERSION=1000 -D_NO_EXCEPTION -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Wall -Werror -Wextra -Wunused-parameter -Wformat -Wconversion -Wno-deprecated -Wno-invalid-offsetof -finline-functions -fno-strict-aliasing -mtune=core2 -Wno-psabi -Wno-sign-compare -Wno-literal-suffix

pstore_example_d_LDADD=-lobtable
pstore_example_d: ob_pstore_example.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) $(pstore_example_d_LDADD) $(LIBS) -o $@ $^

pstore_example_s_LDADD= -l:libeasy.a -l:libobtable.a -l:libeasy.a -l:libvsclient.a -l:liboss_c_sdk.a -l:libaprutil-1.a -l:libapr-1.a -l:libmxml.a
pstore_example_s: ob_pstore_example.o
	$(CXX) -v $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(pstore_example_s_LDADD) -lpthread -laio -lssl -lcrypt -lcrypto -l:libcurl.a -lrt -lmysqlclient_r -ldl




# -Wl,--allow-multiple-definition 
