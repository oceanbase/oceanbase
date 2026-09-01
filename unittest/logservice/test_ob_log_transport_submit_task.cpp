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

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/init_basic_struct.h"

#define private public
#include "logservice/transportservice/ob_log_transport_service.h"
#undef private

namespace oceanbase
{
namespace logservice
{
namespace unittest
{

using namespace common;

class TestLogTransportSubmitTask : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, storage::MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    storage::MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(TestLogTransportSubmitTask, reset_iterator_accepts_log_end)
{
  const share::ObLSID ls_id(1001);
  obrpc::ObCreateLSArg create_ls_arg;
  storage::ObLSService *ls_service = MTL(storage::ObLSService *);
  storage::ObLSHandle ls_handle;
  palf::LSN end_lsn;
  share::SCN start_scn = share::SCN::base_scn();

  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, storage::gen_create_ls_arg(OB_SYS_TENANT_ID, ls_id, create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, storage::ObLSGetMod::STORAGE_MOD));
  ASSERT_NE(nullptr, ls_handle.get_ls());
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_log_handler()->get_end_lsn(end_lsn));
  ASSERT_TRUE(end_lsn.is_valid());

  LogTransportStatus transport_status;
  ObTransportServiceSubmitTask submit_task;
  ASSERT_EQ(OB_SUCCESS, submit_task.init(ls_id, end_lsn, start_scn, &transport_status));
  ASSERT_TRUE(submit_task.is_inited());
  ASSERT_FALSE(submit_task.iterator_.is_valid());
  EXPECT_EQ(OB_SUCCESS, submit_task.reset_iterator(ls_id, end_lsn));
  EXPECT_FALSE(submit_task.iterator_.is_valid());

  const share::ObLSID missing_ls_id(2001);
  ipalf::IPalfIterator<ipalf::IGroupEntry> missing_iterator;
  const int expected_ret = seek_log_iterator(missing_ls_id, end_lsn, missing_iterator);
  ASSERT_NE(OB_SUCCESS, expected_ret);
  ASSERT_NE(OB_ERR_UNEXPECTED, expected_ret);
  EXPECT_EQ(expected_ret, submit_task.reset_iterator(missing_ls_id, end_lsn));
  submit_task.destroy();

  ls_handle.reset();
  EXPECT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
}

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ob_log_transport_submit_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
