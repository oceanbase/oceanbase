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

#define USING_LOG_PREFIX SHARE
#include <gmock/gmock.h>
#define private public
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "storage/high_availability/ob_transfer_lock_info_operator.h"
#include "share/transfer/ob_transfer_info.h"

namespace oceanbase
{
namespace unittest
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace rootserver;
using namespace common;
class TestTransferLockInfoOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestTransferLockInfoOperator() : unittest::ObSimpleClusterTestBase("test_transfer_lock_operator") {}
protected:
  uint64_t tenant_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  int64_t task_id_;
};

TEST_F(TestTransferLockInfoOperator, TransferLockInfo)
{
  int ret = OB_SUCCESS;
  tenant_id_ = OB_SYS_TENANT_ID;
  src_ls_id_ = ObLSID(1001);
  dest_ls_id_ = ObLSID(1002);
  task_id_ = 1;
  const int32_t group_id = 0;
  ObTransferLockStatus start_status = ObTransferLockStatus(ObTransferLockStatus::START);
  ObTransferLockStatus doing_status = ObTransferLockStatus(ObTransferLockStatus::DOING);
  int64_t start_src_lock_owner = 111;
  int64_t start_dest_lock_owner = 222;
  ObString comment("LOCK_AT_START");

  // ObTransferTaskLockInfo INIT
  ObTransferTaskLockInfo start_src_lock_info;
  ObTransferTaskLockInfo start_dest_lock_info;
  ASSERT_EQ(OB_SUCCESS, start_src_lock_info.set(tenant_id_, src_ls_id_, task_id_, start_status, start_src_lock_owner, comment));
  ASSERT_EQ(OB_SUCCESS, start_dest_lock_info.set(tenant_id_, dest_ls_id_, task_id_, start_status, start_dest_lock_owner, comment));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  // insert
  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::insert(start_src_lock_info, group_id, sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::insert(start_dest_lock_info, group_id, sql_proxy));

  // select
  ObTransferLockInfoRowKey src_row_key;
  src_row_key.tenant_id_ = tenant_id_;
  src_row_key.ls_id_ = src_ls_id_;
  ObTransferLockInfoRowKey dest_row_key;
  dest_row_key.tenant_id_ = tenant_id_;
  dest_row_key.ls_id_ = dest_ls_id_;

  ObTransferTaskLockInfo new_start_src_lock_info;
  ObTransferTaskLockInfo new_start_dest_lock_info;

  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::get(src_row_key, task_id_, start_status, false, group_id, new_start_src_lock_info, sql_proxy));
  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::get(dest_row_key, task_id_, start_status, false, group_id, new_start_dest_lock_info, sql_proxy));

  LOG_INFO("[MITTEST]transfer_lock_info", K(new_start_src_lock_info));
  ASSERT_EQ(new_start_src_lock_info.tenant_id_, start_src_lock_info.tenant_id_);
  ASSERT_EQ(new_start_src_lock_info.ls_id_, start_src_lock_info.ls_id_);
  ASSERT_EQ(new_start_src_lock_info.task_id_, start_src_lock_info.task_id_);
  ASSERT_EQ(new_start_src_lock_info.status_.status_, start_src_lock_info.status_.status_);
  ASSERT_EQ(new_start_src_lock_info.lock_owner_, start_src_lock_info.lock_owner_);

  LOG_INFO("[MITTEST]transfer_lock_info", K(new_start_dest_lock_info));
  ASSERT_EQ(new_start_dest_lock_info.tenant_id_, start_dest_lock_info.tenant_id_);
  ASSERT_EQ(new_start_dest_lock_info.ls_id_, start_dest_lock_info.ls_id_);
  ASSERT_EQ(new_start_dest_lock_info.task_id_, start_dest_lock_info.task_id_);
  ASSERT_EQ(new_start_dest_lock_info.status_.status_, start_dest_lock_info.status_.status_);
  ASSERT_EQ(new_start_dest_lock_info.lock_owner_, start_dest_lock_info.lock_owner_);

  // remove
  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::remove(tenant_id_, src_ls_id_, task_id_, start_status, group_id, sql_proxy));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferLockInfoOperator::get(src_row_key, task_id_, start_status, false, group_id, new_start_src_lock_info, sql_proxy));

  ASSERT_EQ(OB_SUCCESS, ObTransferLockInfoOperator::remove(tenant_id_, dest_ls_id_, task_id_, start_status, group_id, sql_proxy));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTransferLockInfoOperator::get(src_row_key, task_id_, start_status, false, group_id, new_start_dest_lock_info, sql_proxy));
}

} // namespace
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
