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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define protected public
#define private public

#include "storage/schema_utils.h"
#include "storage/ob_storage_schema.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/ob_storage_schema_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{
class TestTransferBarrier : public ::testing::Test
{
public:
  TestTransferBarrier();
  virtual ~TestTransferBarrier() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp() override;
  virtual void TearDown() override;
};

TestTransferBarrier::TestTransferBarrier()
{
}

void TestTransferBarrier::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTransferBarrier::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTransferBarrier::SetUp()
{
}

void TestTransferBarrier::TearDown()
{
}

TEST_F(TestTransferBarrier, test_transfer_barrier_redo)
{
  int ret = OB_SUCCESS;
  logservice::ObReplayBarrierType barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  const ObTxLogType log_type = ObTxLogType::TX_MULTI_DATA_SOURCE_LOG;

  //PRE_BARRIER
  //START_TRANSFER_OUT
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_OUT);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::PRE_BARRIER);

  //START_TRANSFER_OUT_PREPARE
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_OUT_PREPARE);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::PRE_BARRIER);

  //FINISH_TRANSFER_OUT
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::FINISH_TRANSFER_OUT);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::PRE_BARRIER);

  //START_TRANSFER_IN
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_IN);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::PRE_BARRIER);

  //STRICT_BARRIER
  //FINISH_TRANSFER_IN
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::FINISH_TRANSFER_IN);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //START_TRANSFER_OUT_V2
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_OUT_V2);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //TRANSFER_MOVE_TX_CTX
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);
}

TEST_F(TestTransferBarrier, test_transfer_barrier_before_prepare)
{
  int ret = OB_SUCCESS;
  logservice::ObReplayBarrierType barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  const ObTxLogType log_type = ObTxLogType::TX_COMMIT_INFO_LOG;

  //STRICT_BARRIER
  //START_TRANSFER_IN
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_IN);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);
}

TEST_F(TestTransferBarrier, test_transfer_barrier_before_commit)
{
  int ret = OB_SUCCESS;
  logservice::ObReplayBarrierType barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  const ObTxLogType log_type = ObTxLogType::TX_COMMIT_LOG;

  //STRICT_BARRIER
  //START_TRANSFER_IN
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_IN);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //START_TRANSFER_OUT_V2
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_OUT_V2);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //TRANSFER_MOVE_TX_CTX
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //TRANSFER_MOVE_TX_CTX
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);
}

TEST_F(TestTransferBarrier, test_transfer_barrier_before_abort)
{
  int ret = OB_SUCCESS;
  logservice::ObReplayBarrierType barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  const ObTxLogType log_type = ObTxLogType::TX_ABORT_LOG;

  //STRICT_BARRIER
  //START_TRANSFER_IN
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_IN);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //START_TRANSFER_OUT_V2
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::START_TRANSFER_OUT_V2);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //TRANSFER_MOVE_TX_CTX
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);

  //TRANSFER_MOVE_TX_CTX
  barrier_flag = ObTxLogTypeChecker::need_replay_barrier(log_type, ObTxDataSourceType::TRANSFER_MOVE_TX_CTX);
  ASSERT_EQ(barrier_flag, logservice::ObReplayBarrierType::STRICT_BARRIER);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_transfer_barrier.log*");
  OB_LOGGER.set_file_name("test_transfer_barrier.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
