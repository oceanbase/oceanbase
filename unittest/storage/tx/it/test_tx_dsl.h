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

#define START_ONE_TX_NODE(n1)                                           \
    auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_); \
    NAMED_DEFER(defer_n1, delete(n1));                                  \
    ASSERT_EQ(OB_SUCCESS, n1->start());                                 \
    LOG_INFO("##START_ONE_TX_NODE##", "node.addr_", n1->addr_);

#define START_TWO_TX_NODE(n1, n2)                                       \
    auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_); \
    auto n2 = new ObTxNode(2, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_); \
    NAMED_DEFER(defer_n1, delete(n1));                                  \
    NAMED_DEFER(defer_n2, delete(n2));                                  \
    ASSERT_EQ(OB_SUCCESS, n1->start());                                 \
    ASSERT_EQ(OB_SUCCESS, n2->start());                                 \
    LOG_INFO("##START_TWO_TX_NODE##", K(n1->addr_), K(n2->addr_));

#define START_TWO_TX_NODE_WITH_LSID(n1, n2, ls_id)                                       \
  auto n1 = new ObTxNode(ls_id, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);     \
  auto n2 = new ObTxNode(ls_id + 1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_); \
  NAMED_DEFER(defer_n1, delete (n1));                                                    \
  NAMED_DEFER(defer_n2, delete (n2));                                                    \
  ASSERT_EQ(OB_SUCCESS, n1->start());                                                    \
  ASSERT_EQ(OB_SUCCESS, n2->start());                                                    \
  LOG_INFO("##START_TWO_TX_NODE##", K(n1->addr_), K(n2->addr_));

#define PREPARE_TX(n1, tx)                                              \
    ObTxDescGuard guard = n1->get_tx_guard();                           \
    ObTxDesc &tx = guard.get_tx_desc();                                 \
    LOG_INFO("##PREPARE_TX##", "node", n1->addr_, "tx_id", tx.tx_id_);

#define PREPARE_TX_PARAM(tx_param)                      \
    ObTxParam tx_param;                                 \
    tx_param.timeout_us_ = 1000000;                     \
    tx_param.access_mode_ = ObTxAccessMode::RW;         \
    tx_param.isolation_ = ObTxIsolationLevel::RC;       \
    tx_param.cluster_id_ = 100;
#define GET_READ_SNAPSHOT(n1, tx, tx_param, snapshot)                   \
    ObTxReadSnapshot snapshot;                                          \
    ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

#define CREATE_IMPLICIT_SAVEPOINT(n1, tx, tx_param, sp)                 \
    ObTxSEQ sp;                                                         \
    ASSERT_EQ(OB_SUCCESS, n1->create_implicit_savepoint(tx, tx_param, sp));

#define CREATE_BRANCH_SAVEPOINT(n1, tx, branch, sp) \
    ObTxSEQ sp;                                                         \
    ASSERT_EQ(OB_SUCCESS, n1->create_branch_savepoint(tx, branch, sp));

#define ROLLBACK_TO_IMPLICIT_SAVEPOINT(n1, tx, sp, timeout_us)          \
    n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_us(timeout_us), nullptr)

#define ROLLBACK_TO_IMPLICIT_SAVEPOINT_X(n1, tx, sp, timeout_us, extra_touched_ls)          \
    n1->rollback_to_implicit_savepoint(tx, sp, n1->ts_after_us(timeout_us), extra_touched_ls)

#define INJECT_LINK_FAILURE(n1, n2)                                     \
    ASSERT_EQ(OB_SUCCESS, bus_.inject_link_failure(n1->addr_, n2->addr_)); \
    LOG_INFO("##JINECT_LINK_FAILURE##", K(n1->addr_), K(n2->addr_));

#define REPAIR_LINK_FAILURE(n1, n2)                                     \
    ASSERT_EQ(OB_SUCCESS, bus_.repair_link_failure(n1->addr_, n2->addr_)); \
    LOG_INFO("##REPAIR_LINK_FAILURE##", K(n1->addr_), K(n2->addr_));

#define ASYNC_DO(op_name, op)                                       \
    auto op_name ## _fff_ = [&] { return op; };                     \
    auto op_name = test::make_async(op_name ## _fff_);              \
    op_name.wait_started();                                         \
    ASSERT_FALSE(op_name.is_evaled());                              \
    LOG_INFO("##ASYNC_DO##", "op_name", #op_name);

#define ASYNC_WAIT(op_name, timeout_us, ret)            \
    LOG_INFO("##ASYNC_WAIT##", "op_name", #op_name);    \
    int ret = 0;                                        \
    do {                                                \
        int i = timeout_us / 1000;                      \
        while(!op_name.is_evaled() && i-- > 0) {        \
            usleep(1000);                               \
        }                                               \
        if (i > 0) { ret = op_name.get();  }            \
    } while(0);                                         \
    LOG_INFO("##ASYNC_DONE##", "op_name", #op_name);

#define ROLLBACK_TX(n1, tx)                     \
    n1->rollback_tx(tx);

#define COMMIT_TX(n1, tx, timeout_us)                   \
    n1->commit_tx(tx, n1->ts_after_us(timeout_us));

#define FLUSH_REDO(n1)                                                  \
    do {                                                                \
        ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;                             \
        ASSERT_EQ(n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr), OB_SUCCESS); \
        ObTransID fail_tx_id;                                           \
        ASSERT_EQ(ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX), OB_SUCCESS); \
    } while(0)

#define SWITCH_TO_FOLLOWER_FORCEDLY(n1)                                 \
    do {                                                                \
        ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;                             \
        ASSERT_EQ(n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr), OB_SUCCESS); \
        ASSERT_EQ(ls_tx_ctx_mgr->switch_to_follower_forcedly(), OB_SUCCESS); \
    } while(0)

#define SWITCH_TO_FOLLOWER_GRACEFULLY(n1)                               \
    do {                                                                \
        ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;                             \
        ASSERT_EQ(n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr), OB_SUCCESS); \
        ASSERT_EQ(ls_tx_ctx_mgr->switch_to_follower_gracefully(), OB_SUCCESS); \
    } while(0)

#define SWITCH_TO_LEADER(n1)                                            \
    do {                                                                \
        ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;                             \
        ASSERT_EQ(n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr), OB_SUCCESS); \
        ASSERT_EQ(ls_tx_ctx_mgr->switch_to_leader(), OB_SUCCESS);       \
        /* wait state to LEADER */                                      \
        while (!ls_tx_ctx_mgr->is_master()) {                           \
            if (REACH_TIME_INTERVAL(500_ms)) {                          \
                TRANS_LOG(INFO, "wait LS TxCtxMgr to be leader", KPC(ls_tx_ctx_mgr)); \
            }                                                           \
            usleep(10_ms);                                              \
        }                                                               \
    } while(0)
