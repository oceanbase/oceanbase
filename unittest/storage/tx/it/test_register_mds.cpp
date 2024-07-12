/**
 * Copyright (c) 2023 OceanBase
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
#include <thread>
#define private public
#define protected public
#include "storage/tx/ob_committer_define.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#define USING_LOG_PREFIX TRANS
#include "../mock_utils/async_util.h"
#include "test_tx_dsl.h"
#include "tx_node.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share {

ObMdsThrottleGuard::~ObMdsThrottleGuard() {}
ObTxDataThrottleGuard::~ObTxDataThrottleGuard() {}

int ObTenantTxDataAllocator::init(const char *label)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr;
  throttle_tool_ = &(MTL_MEM_ALLOC_MGR.share_resource_throttle_tool());
  if (OB_FAIL(slice_allocator_.init(
                 storage::TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE, block_alloc_, mem_attr))) {
    SHARE_LOG(WARN, "init slice allocator failed", KR(ret));
  } else {
    slice_allocator_.set_nway(ObTenantTxDataAllocator::ALLOC_TX_DATA_MAX_CONCURRENCY);
    is_inited_ = true;
  }
  return ret;
}
int ObMemstoreAllocator::init()
{
  throttle_tool_ = &MTL_MEM_ALLOC_MGR.share_resource_throttle_tool();
  return arena_.init();
}
int ObMemstoreAllocator::AllocHandle::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1;
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}
};  // namespace share

namespace concurrent_control
{
int check_sequence_set_violation(const concurrent_control::ObWriteFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t,
                                 const ObTransID,
                                 const blocksstable::ObDmlFlag,
                                 const int64_t)
{
  return OB_SUCCESS;
}
} // namespace concurrent_control

class ReplayLogEntryFunctor
{
public:
  ReplayLogEntryFunctor(ObTxNode *n) : n_(n) {}

  int operator()(const void *buffer,
                 const int64_t nbytes,
                 const palf::LSN &lsn,
                 const int64_t ts_ns)
  {
    return n_->replay(buffer, nbytes, lsn, ts_ns);
  }

private:
  ObTxNode *n_;
};


OB_NOINLINE int ObTransService::acquire_local_snapshot_(const share::ObLSID &ls_id,
                                                        SCN &snapshot,
                                                        const bool is_read_only,
                                                        ObRole &role)
{
  int ret = OB_SUCCESS;
  snapshot = tx_version_mgr_.get_max_commit_ts(false);
  role = LEADER;
  return ret;
}

bool NOTIFY_MDS_ERRSIM = false;

OB_NOINLINE int ObPartTransCtx::errsim_notify_mds_()
{
  int ret = OB_SUCCESS;

  if (NOTIFY_MDS_ERRSIM) {
    ret = OB_ERR_UNEXPECTED;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "errsim notify mds", K(ret), K(NOTIFY_MDS_ERRSIM));
  }

  return ret;
}

class ObTestRegisterMDS : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    MTL_MEM_ALLOC_MGR.init();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
    LOG_INFO(">>>>>>starting>>>>>>>>", K(test_name));
  }
  virtual void TearDown() override
  {
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
    LOG_INFO(">>>>>teardown>>>>>>>>", K(test_name));
  }
  MsgBus bus_;
};

#define GC_MDS_RETAIN_CTX(node)                                                                    \
  {                                                                                                \
    ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;                                                        \
    ASSERT_EQ(OB_SUCCESS, node->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(node->ls_id_, ls_tx_ctx_mgr1)); \
    ls_tx_ctx_mgr1->get_retain_ctx_mgr().try_gc_retain_ctx(&node->mock_ls_);                       \
    ASSERT_EQ(OB_SUCCESS, node->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));            \
  }

TEST_F(ObTestRegisterMDS, basic)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 2001);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  const char *mds_str = "register mds basic";

  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.register_mds_into_tx(tx, n1->ls_id_, ObTxDataSourceType::DDL_TRANS,
                                                      mds_str, strlen(mds_str), 0));
  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));

  n2->set_as_follower_replica(*n1);
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  GC_MDS_RETAIN_CTX(n1)
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  GC_MDS_RETAIN_CTX(n2)
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
}

TEST_F(ObTestRegisterMDS, basic_big_mds)
{
#ifdef OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 2003);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  const int64_t char_count = 3 * 1024 * 1024;

  char mds_str[char_count];
  memset(mds_str, 'M', sizeof(char) * char_count);

  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.register_mds_into_tx(tx, n1->ls_id_, ObTxDataSourceType::DDL_TRANS,
                                                      mds_str, char_count, 0));
  n1->wait_all_redolog_applied();

  // TRANS_LOG(INFO, "try commit tx with expired_time", K(n1->ts_after_ms(0)),K(n1->ts_after_ms()))
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(100 * 1000)));

  n2->set_as_follower_replica(*n1);
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  GC_MDS_RETAIN_CTX(n1)
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  GC_MDS_RETAIN_CTX(n2)
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
#endif
}

TEST_F(ObTestRegisterMDS, notify_mds_error)
{
  START_TWO_TX_NODE_WITH_LSID(n1, n2, 2005);
  PREPARE_TX(n1, tx);
  PREPARE_TX_PARAM(tx_param);
  const char *mds_str = "register mds basic";

  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));

  NOTIFY_MDS_ERRSIM = true;
  ASSERT_EQ(OB_ERR_UNEXPECTED, n1->txs_.register_mds_into_tx(tx, n1->ls_id_, ObTxDataSourceType::DDL_TRANS,
                                                      mds_str, strlen(mds_str), 0));
  NOTIFY_MDS_ERRSIM = false;

  n2->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx, n1->ts_after_ms(500)));

  n2->set_as_follower_replica(*n1);
  ReplayLogEntryFunctor functor(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(functor));

  GC_MDS_RETAIN_CTX(n1)
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  GC_MDS_RETAIN_CTX(n2)
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  int64_t tx_id = 21533427;
  uint64_t h = murmurhash(&tx_id, sizeof(tx_id), 0);
  system("rm -rf test_register_mds.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_register_mds.log", true, false,
                       "test_register_mds.log",  // rs
                       "test_register_mds.log",  // election
                       "test_register_mds.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  TRANS_LOG(INFO, "mmhash:", K(h));
  return RUN_ALL_TESTS();
}
