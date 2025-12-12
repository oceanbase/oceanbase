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
#include <thread>
#include <functional>
#define private public
#define protected public
#include "storage/tx/ob_trans_define.h"
#include "rpc/ob_lock_wait_node.h"
#define USING_LOG_PREFIX TRANS
#include "tx_node.h"
#include "ob_mock_lock_wait_mgr.h"
#include "test_tx_dsl.h"
#include "../mock_utils/async_util.h"
namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;
using namespace rpc;

namespace omt {
  ObTenantConfig *ObTenantConfigMgr::get_tenant_config_with_lock(const uint64_t tenant_id,
                                                                 const uint64_t fallback_tenant_id /* = 0 */,
                                                                 const uint64_t timeout_us /* = 0 */) const
  {
    static ObTenantConfig cfg;
    cfg.writing_throttling_trigger_percentage = 100;
    cfg._enable_wait_remote_lock = true;
    cfg.freeze_trigger_percentage = 90;
    return &cfg;
  }
}

class ObTestObLockWaitMgr : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    const uint64_t tv = ObTimeUtility::current_time();
    ObCurTraceId::set(&tv);
    GCONF.rpc_timeout = 500;
    GCONF._lcl_op_interval = 0;
    ObClockGenerator::init();
    common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_4_2_0);
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> starting test : %s", test_name);
  }
  virtual void TearDown() override
  {
    const testing::TestInfo *const test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    auto test_name = test_info->name();
    _TRANS_LOG(INFO, ">>>> tearDown test : %s", test_name);
    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }
  MsgBus bus_;
};

TEST_F(ObTestObLockWaitMgr, local_lock_conflict)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);


  ASSERT_EQ(OB_SUCCESS, n1->start());

  ObTxDesc *tx_ptr1 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
  ObTxDesc &tx1 = *tx_ptr1;

  ObTxDesc *tx_ptr2 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
  ObTxDesc &tx2 = *tx_ptr2;

  ObTxDesc *tx_ptr3 = NULL;
  ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr3));
  ObTxDesc &tx3 = *tx_ptr3;

  ObTxParam tx_param;
  tx_param.timeout_us_ = 10000000;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.cluster_id_ = 100;
  tx_param.lock_timeout_us_ = 10000000; // 10s
  bool wait_succ = false;
  ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1001, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
  ASSERT_EQ(false, wait_succ);

  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1001, 2, 5 * 1000 * 1000L, tx_param, wait_succ));
  ASSERT_EQ(true, wait_succ);

  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx3, n1->addr_, 1001, 3, 5 * 1000 * 1000L, tx_param, wait_succ));
  ASSERT_EQ(true, wait_succ);

  // check node after handle lock conflict
  ObLockWaitNode *node2 = n1->get_wait_head(1001);
  ASSERT_EQ(true, node2 != NULL);
  ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
  ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
  ASSERT_EQ(true, node2->need_wait());
  ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
  ASSERT_EQ(n1->addr_, node2->get_exec_addr());
  ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
  usleep(50000); // 50ms

  // check only wake up once
  ObLockWaitNode *node3 = n1->get_wait_head(1001);
  ASSERT_EQ(true, node3 != NULL);
  ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
  ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
  ASSERT_EQ(true, node3->need_wait());
  ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node3->get_node_type());
  ASSERT_EQ(n1->addr_, node3->get_exec_addr());

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                              tx_param.isolation_,
                                              n1->ts_after_ms(100),
                                              snapshot));
  int64_t val1 = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1001, val1));
  // check write succ
  ASSERT_EQ(2, val1);
  ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
  usleep(50000); // 50ms

  ObTxReadSnapshot snapshot1;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx3,
                                              tx_param.isolation_,
                                              n1->ts_after_ms(100),
                                              snapshot1));
  int64_t val2 = 0;
  ASSERT_EQ(OB_SUCCESS, n1->read(snapshot1, 1001, val2));
  // check write succ
  ASSERT_EQ(3, val2);
  ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx3, n1->ts_after_ms(500)));

  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
  ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx3));
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  delete(n1);
}

TEST_F(ObTestObLockWaitMgr, local_lock_conflict_placeholder_test)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    // when seq change, should have placeholder in wait queue
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool wait_succ = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1001, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(false, wait_succ);

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1001, 2, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    // false because of req changed
    ASSERT_EQ(false, wait_succ);

    // check node after handle lock conflict
    ObLockWaitNode *node2 = n1->get_wait_head(1001);
    ASSERT_EQ(true, node2->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    // retry
    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1001, 2, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(true, wait_succ);

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx2, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
  }

  {
    // when seq change, only one request retry
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool wait_succ = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(false, wait_succ);

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1003, 2, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    // false because of req changed
    ASSERT_EQ(false, wait_succ);

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx3, n1->addr_, 1003, 3, 5 * 1000 * 1000L, tx_param, wait_succ));
    // true because there is placeholder in queue
    ASSERT_EQ(true, wait_succ);

    // check node is placeholder
    ObLockWaitNode *node2 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node2->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    // retry
    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1003, 2, 5 * 1000 * 1000L, tx_param, wait_succ, false));
    ASSERT_EQ(true, wait_succ);

    // check next node is not placeholder
    ObLockWaitNode *node3 = n1->get_wait_head(1003);
    ASSERT_EQ(false, node3->is_placeholder());
    ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node3->get_node_type());
    ASSERT_EQ(n1->addr_, node3->get_exec_addr());


    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx2, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx3, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx3));
  }
  delete(n1);
  delete(n2);
}

TEST_F(ObTestObLockWaitMgr, remote_lock_conflict)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    // remote lock conflict
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1001, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1001, 2, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(150000); // 150ms

    // check remote node after handle lock conflict
    ObLockWaitNode *node2 = n2->get_wait_head(1001);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    // check remote execution side node after handle lock conflict
    ObLockWaitNode *node3 = n1->get_wait_head(1001);
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    // commit with wakeup waiter
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    // successfully remote wake up
    ASSERT_EQ(NULL, n1->get_wait_head(1001));
    ASSERT_EQ(NULL, n2->get_wait_head(1001));

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n2->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1001, val1));
    // check write succ
    ASSERT_EQ(2, val1);
    ASSERT_EQ(OB_SUCCESS, n2->commit_with_retry_ctrl(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    std::cout << "remote normal test end" << std::endl;
  }

  {
    // when seq change, should have placeholder in wait queue
    n1->stop_check_timeout();
    n2->stop_check_timeout();
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool wait_succ = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1001, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(false, wait_succ);

    n2->stop_repost_node();
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1001, 222, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    usleep(100000); // 100ms

    // check node after handle lock conflict
    ObLockWaitNode *node2 = n1->get_wait_head(1001);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->is_placeholder());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node2->get_node_type());
    ASSERT_EQ(n2->addr_, node2->get_ctrl_addr());

    usleep(100000); // 100ms
    // has retry
    ASSERT_EQ(true, n2->get_store_repost_node() != NULL);
    ASSERT_EQ(node2->get_node_id(), n2->get_store_repost_node()->get_node_id());
    n2->allow_repost_node();

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(0, n1->wait_node_cnt());
    ASSERT_EQ(OB_SUCCESS, n2->abort_tx(tx2, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    std::cout << "remote seq change test end" << std::endl;
  }

  {
    // REMOTE_EXEC_SIDE node type placeholder should have timeout and clean it self strategy
    n1->stop_check_timeout();
    n2->stop_check_timeout();
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool wait_succ = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1001, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(false, wait_succ);

    n2->stop_repost_node();
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1001, 2, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx3, n1->addr_, 1001, 3, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    usleep(200000); // 200ms

    // check node after handle lock conflict
    ObLockWaitNode *node2 = n1->get_wait_head(1001);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(true, node2->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node2->get_node_type());
    ASSERT_EQ(n2->addr_, node2->get_ctrl_addr());
    ASSERT_EQ(2, n1->wait_node_cnt());
    // post lock may reset head node wait timeout ts
    // to verify placeholder node will wakeup subsequent node,
    // set wait timeou ts here
    node2->set_wait_timeout_ts(common::ObTimeUtil::current_time() + ObLockWaitMgr::WAIT_TIMEOUT_TS);

    usleep(150000); // 150ms
    // has retry
    ASSERT_EQ(node2->get_node_id(), n2->get_store_repost_node()->get_node_id());
    ASSERT_EQ(1, n2->wait_node_cnt());

    n1->start_check_timeout();
    n2->start_check_timeout();
    usleep(1.5 * ObLockWaitMgr::WAIT_TIMEOUT_TS); // 1.5s
    // clean up placeholder node and repost next node
    ASSERT_EQ(tx3.tid().get_id(), n2->get_store_repost_node()->get_tx_id());
    ASSERT_EQ(0, n1->wait_node_cnt());

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n2->abort_tx(tx2, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n2->abort_tx(tx3, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx3));
    std::cout << "remote REMOTE_EXEC_SIDE placeholder test end" << std::endl;
  }

  {
    n1->stop_check_timeout();
    n2->stop_check_timeout();
    // when seq change, only one request retry
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool wait_succ = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, wait_succ));
    ASSERT_EQ(false, wait_succ);

    n2->stop_repost_node();
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 2, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    usleep(100000); // 100ms

    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx3, n1->addr_, 1003, 3, 5 * 1000 * 1000L, tx_param, wait_succ, true));
    usleep(100000); // 100ms

    // check node is placeholder
    ObLockWaitNode *node2 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(true, node2->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node2->get_node_type());
    ASSERT_EQ(n2->addr_, node2->get_ctrl_addr());

    ASSERT_EQ(node2->get_node_id(), n2->get_store_repost_node()->get_node_id());
    ASSERT_EQ(2, n1->wait_node_cnt());
    n2->allow_repost_node();
    // retry
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 2, 5 * 1000 * 1000L, tx_param, wait_succ, false));
    usleep(100000); // 100ms
    ASSERT_EQ(2, n1->wait_node_cnt());

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    ASSERT_EQ(1, n1->wait_node_cnt());
    usleep(150000); // 150ms

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx3,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(3, val1);

    // check next node is not placeholder when lock release
    ObLockWaitNode *node3 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(false, node3->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    ObLockWaitNode *node4 = n2->get_wait_head(1003);
    ASSERT_EQ(true, node4 != NULL);
    ASSERT_EQ(false, node4->is_placeholder());
    ASSERT_EQ(tx2.tid().get_id(), node4->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node4->get_holder_tx_id());
    ASSERT_EQ(true, node4->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node4->get_node_type());
    ASSERT_EQ(n1->addr_, node4->get_exec_addr());


    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx3, n2->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(0, n1->wait_node_cnt());
    ObTxReadSnapshot snapshot2;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot2));
    int64_t val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot2, 1003, val2));
    ASSERT_EQ(2, val2);

    std::cout << "remote seq change retry test end" << std::endl;
    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n2->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx3));
  }
  delete(n1);
  delete(n2);
}

TEST_F(ObTestObLockWaitMgr, node_state_check)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    // node state check test
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 2, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(150000); // 150ms

    // check remote node after handle lock conflict
    ObLockWaitNode *node2 = n2->get_wait_head(1003);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    // fetch remote REMOTE_EXEC_SIDE node
    ObLockWaitNode *node3 = n1->fetch_wait_head(1003);
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));

    usleep(1.5 * ObLockWaitMgr::CHECK_TIMEOUT_INTERVAL); // 150ms, check alive dectector should dectect REMOTE_EXEC_SIDE node not exsit

    // successfully retry
    ASSERT_EQ(NULL, n1->get_wait_head(1003));
    ASSERT_EQ(NULL, n2->get_wait_head(1003));

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    // check retry write succ
    ASSERT_EQ(2, val1);

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
  }
  delete n1;
  delete n2;
}

TEST_F(ObTestObLockWaitMgr, priority_wait_local_lock_conflict)
{
  // when both local and remote conflict occurs, priorty to wait local conflict
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n2->addr_, 1005, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    // both have local and remote conflict
    SingleWrite sg1;
    SingleWrite sg2;
    sg1.key_value_ = {1005, 5};
    sg2.key_value_ = {1003, 5};
    sg1.runner_addr_ = n2->addr_;
    sg2.runner_addr_ = n1->addr_;
    bool unused = false;
    n1->write_req_with_lock_wait(tx3, {sg1, sg2}, 5 * 1000 * 1000L, tx_param, unused);
    usleep(150000); // 150ms

    // check wait node after handle lock conflict
    ObLockWaitNode *node2 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    ASSERT_EQ(NULL, n1->get_wait_head(1005));
    ASSERT_EQ(NULL, n2->get_wait_head(1005));

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(NULL, n1->get_wait_head(1003));
    ASSERT_EQ(NULL, n2->get_wait_head(1003));

    // check wait node after local lock release
    ObLockWaitNode *node3 = n1->get_wait_head(1005);
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx2.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_exec_addr());

    // check wait node after local lock release
    ObLockWaitNode *node4 = n2->get_wait_head(1005);
    ASSERT_EQ(tx3.tid().get_id(), node4->get_tx_id());
    ASSERT_EQ(tx2.tid().get_id(), node4->get_holder_tx_id());
    ASSERT_EQ(true, node4->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node4->get_node_type());
    ASSERT_EQ(n1->addr_, node4->get_ctrl_addr());

    ASSERT_EQ(OB_SUCCESS, n2->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
    usleep(150000); // 150ms

    // successfully retry
    ASSERT_EQ(NULL, n1->get_wait_head(1003));
    ASSERT_EQ(NULL, n2->get_wait_head(1003));
    ASSERT_EQ(NULL, n1->get_wait_head(1005));
    ASSERT_EQ(NULL, n2->get_wait_head(1005));

    ObTxReadSnapshot snapshot;
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx3,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(5, val1);

    ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx3,
                                                tx_param.isolation_,
                                                n2->ts_after_ms(100),
                                                snapshot));
    int64_t val2 = 0;
    ASSERT_EQ(OB_SUCCESS, n2->read(snapshot, 1005, val2));
    ASSERT_EQ(5, val2);

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx3, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx3));
  }
  delete n1;
  delete n2;
}

TEST_F(ObTestObLockWaitMgr, subsequent_waiter_timeout_test)
{
  // when remote wakeup is insane, subsequent node should retry after 100ms
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ATOMIC_STORE(&n2->stop_request_retry_, true);
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(50000); // 50ms

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx3, n1->addr_, 1003, 7, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(true, need_wait);

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(1.5 * ObLockWaitMgr::WAIT_TIMEOUT_TS); // 1.5s
    ObTxReadSnapshot snapshot;
    // tx3 should have been retry because of wait timeout
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx3,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(7, val1);

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx3, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx3));
  }
  delete n1;
  delete n2;
}

TEST_F(ObTestObLockWaitMgr, remote_lock_conflict_rpc_error)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    // inform destination enqueue rpc loss
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    n2->fake_lock_wait_mgr_.get_fake_rpc()->inject_msg_errsim(LOCK_WAIT_MGR_MSG_TYPE::LWM_DST_ENQUEUE);
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(1.5 * ObLockWaitMgr::CHECK_TIMEOUT_INTERVAL); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->commit_tx(tx1, n1->ts_after_ms(500)));
    usleep(110000); // 1150ms
    ObTxReadSnapshot snapshot;
    // tx2 should have been retry because of node state check
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(4, val1);

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    n2->fake_lock_wait_mgr_.get_fake_rpc()->reset_msg_errsim();
  }

  {
    // inform destination enqueue resp rpc loss
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    n1->fake_lock_wait_mgr_.get_fake_rpc()->inject_msg_errsim(LOCK_WAIT_MGR_MSG_TYPE::LWM_DST_ENQUEUE_RESP);
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(1.5 * ObLockWaitMgr::CHECK_TIMEOUT_INTERVAL); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ObTxReadSnapshot snapshot;
    // tx2 should have been retry because of node state check
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(4, val1);

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    n2->fake_lock_wait_mgr_.get_fake_rpc()->reset_msg_errsim();
  }

  {
    // lock release remote wake up rpc loss
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    n1->fake_lock_wait_mgr_.get_fake_rpc()->inject_msg_errsim(LOCK_WAIT_MGR_MSG_TYPE::LWM_LOCK_RELEASE);
    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx2, n1->addr_, 1003, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    usleep(1.5 * ObLockWaitMgr::CHECK_TIMEOUT_INTERVAL); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 110ms
    ObTxReadSnapshot snapshot;
    // tx2 should have been retry because of node state check
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx2,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(4, val1);

    ASSERT_EQ(OB_SUCCESS, n2->commit_tx(tx2, n1->ts_after_ms(500)));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx2));
    n2->fake_lock_wait_mgr_.get_fake_rpc()->reset_msg_errsim();
  }
  delete n1;
  delete n2;
}

TEST_F(ObTestObLockWaitMgr, last_wait_hash_wakeup)
{
  // retry not conflict in same row, should wakeup hold key
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    // local retry
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxDesc *tx_ptr4 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr4));
    ObTxDesc &tx4 = *tx_ptr4;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx2, n1->addr_, 1005, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    SingleWrite sg1;
    SingleWrite sg2;
    sg1.key_value_ = {1003, 5};
    sg2.key_value_ = {1005, 5};
    sg1.runner_addr_ = n1->addr_;
    sg2.runner_addr_ = n1->addr_;
    n1->write_req_with_lock_wait(tx3, {sg1, sg2}, 5 * 1000 * 1000L, tx_param, need_wait);
    // wait for 1003
    ASSERT_EQ(true, need_wait);
    usleep(150000); // 150ms

    ObLockWaitNode *node2 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    ObLockWaitNode *node3 = n1->get_wait_head(1005);
    ASSERT_EQ(NULL, node3);
    n1->tx_retry_control_map_[&tx3].req_writes_ = {sg2}; // reset writes only retry on row 1005

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx4, n1->addr_, 1003, 7, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(true, need_wait);

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ObTxReadSnapshot snapshot;
    // tx4 should have been retry because of wake up hold key
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx4,
                                                tx_param.isolation_,
                                                n1->ts_after_ms(100),
                                                snapshot));
    int64_t val1 = 0;
    ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
    ASSERT_EQ(7, val1);
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx3, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx4, n1->ts_after_ms(500)));
    usleep(150000); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx3));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx4));
  }

  {
    // remote retry and hold key request is local
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxDesc *tx_ptr4 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr4));
    ObTxDesc &tx4 = *tx_ptr4;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 7, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx2, n1->addr_, 1005, 8, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    SingleWrite sg1;
    SingleWrite sg2;
    sg1.key_value_ = {1003, 9};
    sg2.key_value_ = {1005, 9};
    sg1.runner_addr_ = n1->addr_;
    sg2.runner_addr_ = n1->addr_;
    n2->write_req_with_lock_wait(tx3, {sg1, sg2}, 5 * 1000 * 1000L, tx_param, need_wait);
    usleep(200000); // 200ms

    // wait for 1003
    ObLockWaitNode *node2 = n2->get_wait_head(1003);
    bool wait_for_1005 = false;
    if (node2 == NULL) {
      wait_for_1005 = true;
      node2 = n2->get_wait_head(1005);
    }
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    ObLockWaitNode *node3 = n1->get_wait_head(1003);
    if (wait_for_1005) {
      node3 = n2->get_wait_head(1005);
    }
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    if (wait_for_1005) {
      ObLockWaitNode *node4 = n1->get_wait_head(1003);
      ObLockWaitNode *node5 = n2->get_wait_head(1003);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    } else {
      ObLockWaitNode *node4 = n1->get_wait_head(1005);
      ObLockWaitNode *node5 = n2->get_wait_head(1005);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    }

    if (!wait_for_1005) {
      n2->tx_retry_control_map_[&tx3].req_writes_ = {sg2}; // reset writes only retry on row 1005
      ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx4, n1->addr_, 1003, 10, 5 * 1000 * 1000L, tx_param, need_wait));
      ASSERT_EQ(true, need_wait);
      ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
      usleep(150000); // 150ms
      ObTxReadSnapshot snapshot;
      // tx4 should have been retry because of hold key wakeup
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx4,
                                                  tx_param.isolation_,
                                                  n1->ts_after_ms(100),
                                                  snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
      ASSERT_EQ(10, val1);
    } else {
      n2->tx_retry_control_map_[&tx3].req_writes_ = {sg1}; // reset writes only retry on row 1003
      ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx4, n1->addr_, 1005, 11, 5 * 1000 * 1000L, tx_param, need_wait));
      ASSERT_EQ(true, need_wait);
      ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
      usleep(150000); // 150ms
      ObTxReadSnapshot snapshot;
      // tx4 should have been retry because of hold key wakeup
      ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx4,
                                                  tx_param.isolation_,
                                                  n1->ts_after_ms(100),
                                                  snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1005, val1));
      ASSERT_EQ(11, val1);
    }

    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n2->commit_with_retry_ctrl(tx3, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx4, n1->ts_after_ms(500)));
    usleep(150000); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx3));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx4));
  }

  {
    // remote retry and hold key request is remote
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxDesc *tx_ptr4 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr4));
    ObTxDesc &tx4 = *tx_ptr4;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx2, n1->addr_, 1005, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    SingleWrite sg1;
    SingleWrite sg2;
    sg1.key_value_ = {1003, 5};
    sg2.key_value_ = {1005, 5};
    sg1.runner_addr_ = n1->addr_;
    sg2.runner_addr_ = n1->addr_;
    n2->write_req_with_lock_wait(tx3, {sg1, sg2}, 5 * 1000 * 1000L, tx_param, need_wait);
    usleep(250000); // 200ms

    // wait for 1003
    ObLockWaitNode *node2 = n2->get_wait_head(1003);
    bool wait_for_1005 = false;
    if (node2 == NULL) {
      node2 = n2->get_wait_head(1005);
      wait_for_1005 = true;
    }
    ASSERT_EQ(tx3.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_CTRL_SIDE, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());

    ObLockWaitNode *node3 = n1->get_wait_head(1003);
    if (node3 == NULL) {
      node2 = n1->get_wait_head(1005);
      wait_for_1005 = true;
    }
    ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node3->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    if (wait_for_1005) {
      ObLockWaitNode *node4 = n1->get_wait_head(1003);
      ObLockWaitNode *node5 = n2->get_wait_head(1003);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    } else {
      ObLockWaitNode *node4 = n1->get_wait_head(1005);
      ObLockWaitNode *node5 = n2->get_wait_head(1005);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    }

    if (wait_for_1005) {
      ObLockWaitNode *node4 = n1->get_wait_head(1003);
      ObLockWaitNode *node5 = n2->get_wait_head(1003);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    } else {
      ObLockWaitNode *node4 = n1->get_wait_head(1005);
      ObLockWaitNode *node5 = n2->get_wait_head(1005);
      ASSERT_EQ(NULL, node4);
      ASSERT_EQ(NULL, node5);
    }

    if (!wait_for_1005) {
      n2->tx_retry_control_map_[&tx3].req_writes_ = {sg2}; // reset writes only retry on row 1005
      ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx4, n1->addr_, 1003, 7, 5 * 1000 * 1000L, tx_param, need_wait));
      ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx1, n1->ts_after_ms(500)));
      usleep(150000); // 150ms
      ObTxReadSnapshot snapshot;
      // tx4 should have been retry because of hold key wakeup
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx4,
                                                  tx_param.isolation_,
                                                  n1->ts_after_ms(100),
                                                  snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1003, val1));
      ASSERT_EQ(7, val1);
    } else {
      n2->tx_retry_control_map_[&tx3].req_writes_ = {sg1}; // reset writes only retry on row 1003
      ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx4, n1->addr_, 1005, 7, 5 * 1000 * 1000L, tx_param, need_wait));
      ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
      usleep(150000); // 150ms
      ObTxReadSnapshot snapshot;
      // tx4 should have been retry because of hold key wakeup
      ASSERT_EQ(OB_SUCCESS, n2->get_read_snapshot(tx4,
                                                  tx_param.isolation_,
                                                  n1->ts_after_ms(100),
                                                  snapshot));
      int64_t val1 = 0;
      ASSERT_EQ(OB_SUCCESS, n1->read(snapshot, 1005, val1));
      ASSERT_EQ(7, val1);
    }
    ASSERT_EQ(OB_SUCCESS, n1->commit_with_retry_ctrl(tx2, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n2->commit_with_retry_ctrl(tx3, n1->ts_after_ms(500)));
    usleep(150000); // 150ms
    ASSERT_EQ(OB_SUCCESS, n2->commit_with_retry_ctrl(tx4, n1->ts_after_ms(500)));
    usleep(150000); // 150ms

    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx3));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx4));
  }
  delete n1;
  delete n2;
}

TEST_F(ObTestObLockWaitMgr, ls_switch_to_follower)
{
  // retry not conflict in same row, should wakeup hold key
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);

  ASSERT_EQ(OB_SUCCESS, n1->start());
  ASSERT_EQ(OB_SUCCESS, n2->start());
  {
    ObTxDesc *tx_ptr1 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr1));
    ObTxDesc &tx1 = *tx_ptr1;

    ObTxDesc *tx_ptr2 = NULL;
    ASSERT_EQ(OB_SUCCESS, n1->acquire_tx(tx_ptr2));
    ObTxDesc &tx2 = *tx_ptr2;

    ObTxDesc *tx_ptr3 = NULL;
    ASSERT_EQ(OB_SUCCESS, n2->acquire_tx(tx_ptr3));
    ObTxDesc &tx3 = *tx_ptr3;

    ObTxParam tx_param;
    tx_param.timeout_us_ = 10000000;
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = ObTxIsolationLevel::RC;
    tx_param.cluster_id_ = 100;
    tx_param.lock_timeout_us_ = 10000000; // 10s
    bool need_wait = false;
    ASSERT_EQ(OB_SUCCESS, n1->single_atomic_write_request(tx1, n1->addr_, 1003, 1, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);

    ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, n1->single_atomic_write_request(tx2, n1->addr_, 1003, 4, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(true, need_wait);
    usleep(150000); // 150ms

    ASSERT_EQ(OB_SUCCESS, n2->single_atomic_write_request(tx3, n1->addr_, 1003, 7, 5 * 1000 * 1000L, tx_param, need_wait));
    ASSERT_EQ(false, need_wait);
    usleep(150000); // 150ms

    ObLockWaitNode *node2 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node2 != NULL);
    ASSERT_EQ(tx2.tid().get_id(), node2->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node2->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::LOCAL, node2->get_node_type());
    ASSERT_EQ(n1->addr_, node2->get_exec_addr());
    ASSERT_EQ(2, n1->wait_node_cnt());

    ObLockWaitNode* node3 = (ObLockWaitNode*)node2->next_;
    ASSERT_EQ(true, node3 != NULL);
    ASSERT_EQ(tx3.tid().get_id(), node3->get_tx_id());
    ASSERT_EQ(tx1.tid().get_id(), node3->get_holder_tx_id());
    ASSERT_EQ(true, node2->need_wait());
    ASSERT_EQ(ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE, node3->get_node_type());
    ASSERT_EQ(n2->addr_, node3->get_ctrl_addr());

    n1->stop_repost_node();
    n2->stop_repost_node();
    n1->ls_switch_to_follower(ObLSID(1));
    usleep(1000000); // 1s
    ObLockWaitNode *node4 = n1->get_wait_head(1003);
    ASSERT_EQ(true, node4 == NULL);
    ASSERT_EQ(0, n1->wait_node_cnt());

    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx1, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(tx2, OB_TRANS_CTX_NOT_EXIST));
    ASSERT_EQ(OB_SUCCESS, n2->abort_tx(tx3, OB_TRANS_CTX_NOT_EXIST));

    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx1));
    ASSERT_EQ(OB_SUCCESS, n1->release_tx(tx2));
    ASSERT_EQ(OB_SUCCESS, n2->release_tx(tx3));
  }

  delete n1;
  delete n2;
}

// ----------------correctness testing at high concurrency---------------------

TEST_F(ObTestObLockWaitMgr, concurrent_test)
{
  ObMockLockWaitMgr *lwm = new ObMockLockWaitMgr(1001, NULL, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888));
  lwm->set_tenant_id(1001);
  lwm->start();
  lwm->stop_check_timeout();

  int wait_worker_cnt = 2000;
  int wakeup_worker_cnt = 2;
  int test_time = 10;

  std::vector<std::thread> ths;
  bool stop = false;
  int64_t lock_seq = 0;
  uint64_t tablet_id = 100000;
  int64_t recv_ts1 = 1000;
  int64_t recv_ts2 = 1000;

  // hash2 is in the same slot with hash1
  int64_t test_hash1 = 1073743825;
  int64_t test_hash2 = test_hash1 + 4;
  uint64_t hash1_finish_num = 0;
  uint64_t hash2_finish_num = 0;

  auto wait_worker = [&] (int64_t wait_hash) {
    bool unused = true;
    void *node_ptr = ob_malloc(sizeof(Node), ObNewModIds::TEST);
    Node *node = new(node_ptr) Node();

    bool wait_succ = false;
    int64_t m_recv_ts = 0;
    // recv ts is the order of wait worker
    if (wait_hash == test_hash1) {
      m_recv_ts = ATOMIC_FAA(&recv_ts1, 1);
    } else {
      m_recv_ts = ATOMIC_FAA(&recv_ts2, 1);
    }
    lwm->setup(*node, m_recv_ts);

    while (!ATOMIC_LOAD(&stop) && !wait_succ) {
      // wait fail, retry wait
      node->set_lock_wait_expire_ts(1846674407370955161);
      node->set((void*)node,
                wait_hash,
                lock_seq,
                1001,
                100000,
                0,
                0,
                "1005",
                4,
                1,
                1,
                m_recv_ts, // tx id
                m_recv_ts-1, // hold tx id
                111,
                111111,
                0,
                Node::NODE_TYPE::LOCAL,
                ObAddr());
      node->node_type_ = (ObLockWaitNode::NODE_TYPE)(m_recv_ts % 3);
      node->node_id_ = node->tx_id_;
      node->sessid_ = 1;
      node->exec_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888);
      if (node->node_type_ == ObLockWaitNode::NODE_TYPE::REMOTE_EXEC_SIDE) {
        ObLockWaitMgrDstEnqueueMsg msg;
        ObLockWaitMgrRpcResult result;
        msg.tenant_id_ = 1001;
        msg.hash_ = node->hash_;
        msg.node_id_ = node->node_id_;
        msg.sender_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888);
        msg.lock_seq_ = node->lock_seq_;
        msg.lock_ts_ = node->lock_ts_;
        msg.tx_id_ = node->tx_id_;
        msg.holder_tx_id_ = node->holder_tx_id_;
        msg.recv_ts_ = node->recv_ts_;
        msg.sess_id_ = node->sessid_;
        msg.query_timeout_us_ = 20 * 1000 * 1000;
        ASSERT_EQ(OB_SUCCESS, lwm->handle_inform_dst_enqueue_req(msg, result));
        wait_succ = true;
      } else {
        wait_succ = lwm->post_process(true, unused);
        ASSERT_EQ(true, wait_succ);
      }
    }
  };

  auto wakeup_worker = [&] (int64_t wakeup_hash) {
    ObLockWaitNode *last_node = NULL;
    ObLockWaitNode *node = NULL;
    while (!ATOMIC_LOAD(&stop)) {
      uint64_t num = ATOMIC_LOAD(&hash1_finish_num);
      if (wakeup_hash == test_hash2) {
        num = ATOMIC_LOAD(&hash2_finish_num);
      }
      // 顺序唤醒
      node = lwm->get_wait_head(wakeup_hash);
      if (node == NULL) {
        usleep(100); // 100us
      } else if (node->tx_id_ == 1000
              || (last_node != NULL && node->tx_id_ == last_node->tx_id_+1)) {
        if (wakeup_hash == test_hash1) {
          if (ATOMIC_BCAS(&hash1_finish_num, num, num+1)) {
            node = lwm->fetch_wait_head(wakeup_hash);
            if (node->tx_id_ != 1000) {
              ASSERT_EQ(node->tx_id_, last_node->tx_id_ + 1);
              ASSERT_EQ(node->hash_, last_node->hash_);
              ob_free(last_node);
            } else {
              ASSERT_EQ(node->tx_id_, 1000);
            }
            last_node = node;
          }
        } else if (wakeup_hash == test_hash2){
          if (ATOMIC_BCAS(&hash2_finish_num, num, num+1)) {
            node = lwm->fetch_wait_head(wakeup_hash);
            if (node->tx_id_ != 1000) {
              ASSERT_EQ(node->tx_id_, last_node->tx_id_ + 1);
              ASSERT_EQ(node->hash_, last_node->hash_);
              ob_free(last_node);
            } else {
              ASSERT_EQ(node->tx_id_, 1000);
            }
            last_node = node;
          }
        }
      } else {
        usleep(100);
      }
    }
    ob_free(last_node);
  };

  for (int i = 0; i < wait_worker_cnt/2; i++) {
    std::thread th1(wait_worker, test_hash1);
    std::thread th2(wait_worker, test_hash2);
    ths.push_back(std::move(th1));
    ths.push_back(std::move(th2));
  }

  for (int i = 0; i < wakeup_worker_cnt/2; i++) {
    std::thread th1(wakeup_worker, test_hash1);
    ths.push_back(std::move(th1));
    std::thread th2(wakeup_worker, test_hash2);
    ths.push_back(std::move(th2));
  }

  for (int i=0;i<test_time;i++) {
    uint64_t finish_req1 = ATOMIC_LOAD(&hash1_finish_num);
    uint64_t finish_req2 = ATOMIC_LOAD(&hash2_finish_num);
    std::cout << "wait hash1 num:" << finish_req1 << std::endl;
    std::cout << "wait hash2 num:" << finish_req2 << std::endl;
    ::sleep(1);
  }

  ATOMIC_STORE(&stop, true);
  lwm->stop();

  for (auto &th : ths) {
    th.join();
  }
  ASSERT_EQ(wait_worker_cnt, ATOMIC_LOAD(&hash1_finish_num)+ATOMIC_LOAD(&hash2_finish_num));
  delete lwm;
}

} // oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_lock_wait_mgr.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_lock_wait_mgr.log", true, false,
                       "test_lock_wait_mgr_rs.log", // rs
                       "test_lock_wait_mgr_election.log", // election
                       "test_lock_wait_mgr_audit.log"); // audit
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}