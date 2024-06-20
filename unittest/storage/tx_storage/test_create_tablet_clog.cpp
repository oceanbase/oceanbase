/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>

#define USING_LOG_PREFIX STORAGETEST

#define protected public
#define private public

#include "logservice/ob_log_base_header.h"

#include "observer/omt/ob_tenant_mtl_helper.h"
#include "storage/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace common;
namespace storage
{
class TestCreateTabletClog : public ::testing::Test
{
public:
  TestCreateTabletClog() = default;
  virtual ~TestCreateTabletClog() = default;

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp();
  void TearDown();
  void get_write_clog_buf(const obrpc::ObBatchCreateTabletArg &arg,
                          char *&buffer,
                          int64_t &buffer_size);
};


void TestCreateTabletClog::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
}

void TestCreateTabletClog::TearDown()
{
}

void TestCreateTabletClog::get_write_clog_buf(
    const obrpc::ObBatchCreateTabletArg &arg,
    char *&buffer,
    int64_t &buffer_size)
{
  buffer = nullptr;
  buffer_size = 0;
  int64_t pos = 0;
  int64_t replay_hint = 0;
  bool need_replay_barrier = true;
  logservice::ObLogBaseHeader log_header(replay_hint,
                                         logservice::ObLogBaseType::DDL_TYPE,
                                         need_replay_barrier);
  buffer_size = buffer_size + log_header.get_serialize_size();
  buffer_size = buffer_size + arg.get_serialize_size();

  buffer = static_cast<char *>(ob_malloc(buffer_size));
  ASSERT_NE(nullptr, buffer);
  ASSERT_EQ(OB_SUCCESS, log_header.serialize(buffer, buffer_size, pos));
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buffer, buffer_size, pos));
}

TEST_F(TestCreateTabletClog, replay_create_tablet_clog_test)
{
  int ret = OB_SUCCESS;
  ObCreateLSArg arg;
  ObLS *ls = NULL;
  ObLSID ls_id(100);

  ObTabletID tablet_id(1001);
  ObTabletHandle tablet_handle;
  ObLSHandle handle;
  ObTablet *tablet =NULL;

  obrpc::ObCreateTabletBatchRes res;
  obrpc::ObBatchCreateTabletArg create_tablet_arg;

  LOG_INFO("replay_create_tablet_clog_test begin");
  // 1. prepare ls
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(env.tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, gen_create_tablet_arg(env.tenant_id, ls_id, tablet_id, create_tablet_arg));

  ObLSService *ls_service = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_service);

  ASSERT_EQ(OB_SUCCESS, ls_service->start());
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));

  // 2. replay clog
  const palf::LSN lsn;
  const int64_t log_timestamp = 0;
  char *buffer = nullptr;
  int64_t buffer_size = 0;
  get_write_clog_buf(create_tablet_arg, buffer, buffer_size);
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, handle));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->replay(lsn, log_timestamp, buffer_size, buffer));


  // 3. check tablet.
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);

  // 4. remove tablet
  ASSERT_EQ(OB_SUCCESS, ls_service->delete_tablet(ls_id, tablet_id));
  EXPECT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));

  if (buffer != nullptr) {
    ob_free(buffer);
  }
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_create_tablet_clog.log*");

  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_create_tablet_clog.log*", true);
  return RUN_ALL_TESTS();
}
