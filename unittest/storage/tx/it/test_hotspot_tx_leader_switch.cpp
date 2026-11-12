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
#include <atomic>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <cerrno>
#include "io/easy_io.h"
#include "util/easy_pool.h"
#include <unistd.h>
#define protected public
#define private public
#define USING_LOG_PREFIX TRANS
#include "storage/tx/ob_tx_hotspot_define.h"
#include "storage/tx/ob_tx_hotspot_helper.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/ob_result_set.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_group_commit_struct.h"
#include "sql/ob_sql_trans_control.h"
#include "storage/tx/ob_trans_functor.h"
#include "test_tx_dsl.h"
#include "tx_node.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_sql_sock_session.h"
#include "observer/mysql/obsm_conn_callback.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
using namespace ::testing;
using namespace transaction;
using namespace share;

static ObSharedMemAllocMgr MTL_MEM_ALLOC_MGR;

namespace share
{
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
  ObSharedMemAllocMgr *mtl_alloc_mgr = &MTL_MEM_ALLOC_MGR;
  ObMemstoreAllocator &host = mtl_alloc_mgr->memstore_allocator();
  (void)host.init_handle(*this);
  return ret;
}
}  // namespace share

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

class ObTestHotspotTxLeaderSwitch : public ::testing::Test
{
public:
  virtual void SetUp() override
  {
    oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
    ObCurTraceId::init(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888));
    GCONF._ob_trans_rpc_timeout = 500;
    ObClockGenerator::init();
    MTL_MEM_ALLOC_MGR.init();
  }

  virtual void TearDown() override
  {
    // reset errsim to avoid contaminating other tests
    oceanbase::common::EventItem item;
    item.error_code_ = 0;
    item.occur_ = 0;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event(
        "ERRSIM_SWITCH_TO_FOLLOWER_GRACEFULLY_RETRY_TIMEOUT_US", item);

    ObClockGenerator::destroy();
    ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
  }

  void set_errsim_retry_timeout(const int64_t timeout_us)
  {
    oceanbase::common::EventItem item;
    item.error_code_ = timeout_us;
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    item.cond_ = 0;
    (void)oceanbase::common::EventTable::set_event(
        "ERRSIM_SWITCH_TO_FOLLOWER_GRACEFULLY_RETRY_TIMEOUT_US", item);
  }

  void run_replay_post_failure(const bool fail_enqueue, const int upgrade_phase = 0,
                               const int role_conflict = 0, const bool direct_success = false);
  void run_secondary_submitted_case(const int empty_members, const bool controlled_retry = false,
                                    const bool publication_abort = false, const int failure_mode = 0);

  MsgBus bus_;
};

// Passive EASY transport: run real response encoding and enqueue completion,
// but leave the event loop stopped so no network consumer owns these requests.
class MemberCallbackRequest
{
public:
  MemberCallbackRequest() : session_(1001), request_(rpc::ObRequest::OB_MYSQL),
      ez_request_(), message_(), connection_(), sockets_{-1, -1}
  {
    easy_list_init(&ez_request_.request_list_node);
  }
  ~MemberCallbackRequest()
  {
    easy_list_del(&ez_request_.request_list_node);
    if (nullptr != message_.pool) {
      easy_pool_destroy(message_.pool);
    }
    for (int fd : sockets_) {
      if (fd >= 0) { close(fd); }
    }
  }
  int init(easy_io_thread_t &io_thread)
  {
    int ret = OB_SUCCESS;
    if (0 != socketpair(AF_UNIX, SOCK_STREAM, 0, sockets_)) {
      ret = OB_ERROR;
    } else if (nullptr == (message_.pool = easy_pool_create(4096))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      connection_.fd = sockets_[0];
      connection_.ioth = &io_thread;
      connection_.user_data = &mysql_connection_;
      message_.c = &connection_;
      ez_request_.ms = &message_;
      request_.set_ez_req(&ez_request_);
      // Non-PL command keeps error encoding independent of a session manager.
      input_packet_.set_cmd(obmysql::COM_PING);
      request_.set_packet(&input_packet_);
      mysql_connection_.tenant_id_ = 1001;
      mysql_connection_.cap_flags_.cap_flags_.OB_CLIENT_PROTOCOL_41 = 1;
      session_.set_capability(mysql_connection_.cap_flags_);
      session_.tenant_id_ = session_.effective_tenant_id_ = 1001;
      observer::ObMPPacketSender &sender = session_.get_mysql_end_trans_cb().get_packet_sender();
      // Avoid reading an input packet: callbacks only need the existing request.
      ret = sender.do_init(&request_, 1, 1, true, false, ObTimeUtility::current_time());
      if (OB_SUCCESS == ret) {
        session_.get_mysql_end_trans_cb().sess_info_ = &session_;
        sql::ObEndTransCbPacketParam param;
        param.fill("", 1, 0, true, *ObCurTraceId::get_trace_id());
        ret = session_.get_mysql_end_trans_cb().set_packet_param(param);
      }
    }
    return ret;
  }
  void expect_response(const int result)
  {
    // reset_err() represents an empty error with OB_MAX_ERROR_CODE.
    EXPECT_EQ(OB_SUCCESS == result ? OB_MAX_ERROR_CODE : result,
              session_.show_warnings_buf_.get_err_code());
    ASSERT_NE(nullptr, ez_request_.opacket);
    auto *packet = static_cast<easy_buf_t *>(ez_request_.opacket);
    ASSERT_GE(packet->last - packet->pos, 5);
    const auto *bytes = reinterpret_cast<const unsigned char *>(packet->pos);
    EXPECT_EQ(1, bytes[3]);
    if (OB_SUCCESS == result) {
      EXPECT_EQ(0, bytes[4]); // MySQL OK packet.
    } else {
      ASSERT_GE(packet->last - packet->pos, 7);
      EXPECT_EQ(0xff, bytes[4]);
      EXPECT_EQ(ob_errpkt_errno(result, false), bytes[5] | (bytes[6] << 8));
    }
    EXPECT_EQ(EASY_OK, ez_request_.retcode);
    EXPECT_FALSE(easy_list_empty(&ez_request_.request_list_node));
    EXPECT_EQ(nullptr, session_.get_mysql_end_trans_cb().get_sess_info_ptr());
    EXPECT_FALSE(session_.get_mysql_end_trans_cb().pkt_param_.is_valid());
  }
  sql::ObSQLSessionInfo session_;
  rpc::ObRequest request_;
  easy_request_t ez_request_;
  easy_message_session_t message_;
  easy_connection_t connection_;
  observer::ObSMConnection mysql_connection_;
  obmysql::ObMySQLRawPacket input_packet_;
  int sockets_[2];
};

// Exercise ObSqlEndTransCb::callback itself; each subordinate callback is the
// real callback stored in its session, rather than a mirrored result helper.
static void run_member_callback_case(const bool *zero_rows, const int member_count,
                                    const int primary_result = OB_SUCCESS,
                                    const bool fail_primary_encoding = false)
{
  // Manually owned sessions carry no manager references. get_session cannot
  // acquire a reference and callback's final revert_session reports the absent
  // manager without transferring/freeing these fixture-owned sessions.
  ASSERT_EQ(nullptr, GCTX.session_mgr_);
  easy_io_thread_t io_thread = {};
  io_thread.loop = ev_loop_new(0);
  ASSERT_NE(nullptr, io_thread.loop);
  easy_list_init(&io_thread.request_list);
  ev_async_init(&io_thread.thread_watcher, +[](EV_P_ ev_async *, int) {});
  ev_async_start(io_thread.loop, &io_thread.thread_watcher);
  DEFER(ev_async_stop(io_thread.loop, &io_thread.thread_watcher);
        ev_loop_destroy(io_thread.loop));

  // Requests and their queue links are destroyed before the stopped event loop.
  MemberCallbackRequest requests[6];
  ASSERT_LE(member_count + 2, 6);
  sql::ObGroupCommitSubRequest members[6];
  for (int i = 0; i < member_count + 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, requests[i].init(io_thread));
    members[i].sess_ = &requests[i].session_;
    members[i].req_ = &requests[i].request_;
    members[i].is_trans_started_ = (0 == i || member_count + 1 == i);
    members[i].rollback_on_no_affected_rows_ = i > 0 && i <= member_count ? zero_rows[i - 1] : true;
    members[i].next_ = i < member_count + 1 ? &members[i + 1] : nullptr;
  }
  const int started_index = member_count + 1;
  requests[started_index].session_.show_warnings_buf_.set_error("untouched", OB_TIMEOUT);

  sql::ObGroupSqlValue sql_value;
  ASSERT_EQ(OB_SUCCESS, sql_value.allocator_.init(OB_MALLOC_NORMAL_BLOCK_SIZE,
      ObMemAttr(1001, "CallbackIT"), 128L * 1024 * 1024));
  sql::ObGroupValue group_value;
  group_value.sql_value_ = &sql_value;
  sql::ObGroupCommitAggInfo *agg_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_value.alloc_agg_info(agg_info));
  agg_info->inflight_req_handle_.head_ = &members[0];
  agg_info->inflight_req_handle_.sub_req_cnt_ = member_count + 2;
  agg_info->inflight_req_handle_.group_value_ = &group_value;
  // The fixture owns the subrequests, so the inflight handle is not registered
  // for aggregator-owned request destruction. free_agg_request still executes.
  requests[0].request_.set_group_commit_agg_info(agg_info);
  if (fail_primary_encoding) {
    // Existing send_ok_packet/save_trans_status error path, before encoding.
    requests[0].session_.trans_spec_status_ = static_cast<sql::ObTransSpecfiedStatus>(-1);
  }
  requests[0].session_.get_mysql_end_trans_cb().callback(primary_result);
  requests[0].request_.reset_group_commit_agg_info(); // agg_info has been freed.

  const int group_result = fail_primary_encoding ? OB_ERR_UNEXPECTED : primary_result;
  for (int i = 1; i <= member_count; ++i) {
    const int expected = OB_SUCCESS == group_result && zero_rows[i - 1]
        ? OB_ROLLBACK_ON_NO_AFFECTED_ROWS : group_result;
    SCOPED_TRACE(i);
    requests[i].expect_response(expected);
  }
  if (fail_primary_encoding) {
    EXPECT_EQ(nullptr, requests[0].ez_request_.opacket);
    EXPECT_FALSE(easy_list_empty(&requests[0].ez_request_.request_list_node));
    char byte;
    EXPECT_EQ(0, recv(requests[0].sockets_[0], &byte, 1, MSG_DONTWAIT)); // SHUT_RD.
    requests[0].session_.trans_spec_status_ = sql::TRANS_SPEC_NOT_SET;
  } else {
    requests[0].expect_response(primary_result);
    char byte;
    EXPECT_EQ(-1, recv(requests[0].sockets_[0], &byte, 1, MSG_DONTWAIT));
    EXPECT_EQ(EAGAIN, errno); // SQL failure encoded successfully did not disconnect.
  }
  EXPECT_EQ(OB_TIMEOUT, requests[started_index].session_.show_warnings_buf_.get_err_code());
  EXPECT_EQ(nullptr, requests[started_index].ez_request_.opacket);
  EXPECT_TRUE(easy_list_empty(&requests[started_index].ez_request_.request_list_node));
  EXPECT_EQ(&requests[started_index].session_,
            requests[started_index].session_.get_mysql_end_trans_cb().get_sess_info_ptr());
  EXPECT_TRUE(requests[started_index].session_.get_mysql_end_trans_cb().pkt_param_.is_valid());
}

TEST_F(ObTestHotspotTxLeaderSwitch, member_callback_zero_then_normal)
{
  const bool zero_rows[] = {true, false};
  run_member_callback_case(zero_rows, 2);
}

TEST_F(ObTestHotspotTxLeaderSwitch, member_callback_normal_then_zero)
{
  const bool zero_rows[] = {false, true};
  run_member_callback_case(zero_rows, 2);
}

TEST_F(ObTestHotspotTxLeaderSwitch, member_callback_interleaved_members)
{
  const bool zero_rows[] = {true, false, true, false};
  run_member_callback_case(zero_rows, 4);
}

TEST_F(ObTestHotspotTxLeaderSwitch, member_callback_primary_error_survives_error_encoding)
{
  const bool zero_rows[] = {true, false, true, false};
  run_member_callback_case(zero_rows, 4, OB_NOT_MASTER);
}

TEST_F(ObTestHotspotTxLeaderSwitch, member_callback_primary_encoding_failure)
{
  const bool zero_rows[] = {true, false, true, false};
  run_member_callback_case(zero_rows, 4, OB_SUCCESS, true);
}

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

struct PendingAbortReplayFunctor
{
  explicit PendingAbortReplayFunctor(ObTxNode *node) : node_(node), timestamp_(0) {}
  int operator()(const void *buffer, const int64_t size,
                 const palf::LSN &lsn, const int64_t timestamp)
  {
    const int ret = node_->replay(buffer, size, lsn, timestamp);
    if (OB_EAGAIN == ret) {
      log_.assign(static_cast<const char *>(buffer), size);
      lsn_ = lsn;
      timestamp_ = timestamp;
    }
    return ret;
  }
  ObTxNode *node_;
  std::string log_;
  palf::LSN lsn_;
  int64_t timestamp_;
};

class CountingCommitCallback : public ObITxCallback
{
public:
  CountingCommitCallback() : count_(0), result_(OB_ERR_UNEXPECTED), callback_mts_(0) {}
  void callback(int ret) override
  {
    result_.store(ret);
    callback_mts_.store(ObTimeUtility::current_monotonic_time());
    count_.fetch_add(1);
  }
  std::atomic<int> count_;
  std::atomic<int> result_;
  std::atomic<int64_t> callback_mts_;
};

// Schema used by the extensible commit message before hotspot P provenance
// was appended. The versioned box is intentionally identical to the product
// serde convention so both missing-tail and unknown-tail behavior are real.
struct OldExtensibleCommitMsgBox
{
  explicit OldExtensibleCommitMsgBox(ObTxCommitMsg &msg) : msg_(msg) {}
  ObTxCommitMsg &msg_;
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(OldExtensibleCommitMsgBox,
                    msg_.expire_ts_,
                    msg_.parts_,
                    msg_.app_trace_info_,
                    msg_.commit_start_scn_,
                    msg_.commit_parts_,
                    msg_.app_trace_id_);


void ObTestHotspotTxLeaderSwitch::run_secondary_submitted_case(
    const int empty_members, const bool controlled_retry, const bool publication_abort, const int failure_mode)
{
  CountingCommitCallback submitted_primary_cb;
  CountingCommitCallback submitted_secondary_cb[2];
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_self_addr = GCONF.self_addr_;
  NAMED_DEFER(restore_self_addr, GCONF.self_addr_ = saved_self_addr);
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  auto wait_apply = [&]() {
    const int64_t deadline = ObTimeUtility::current_monotonic_time() + 5_s;
    while (0 != n1->fake_tx_log_adapter_->get_inflight_cnt()
           && ObTimeUtility::current_monotonic_time() < deadline) {
      ob_usleep(1_ms);
    }
    return 0 == n1->fake_tx_log_adapter_->get_inflight_cnt();
  };
  memtable::ObMemtable *blocked_memtable = nullptr;
  // Run after ctx/transaction guards and worker-resume guards, while n1 lives.
  NAMED_DEFER(release_partial_memtable,
              if (nullptr != blocked_memtable) {
                const bool apply_drained = wait_apply();
                const int64_t deadline = ObTimeUtility::current_monotonic_time() + 5_s;
                while ((0 != blocked_memtable->get_ref()
                        || 0 != blocked_memtable->get_write_ref()
                        || 0 != blocked_memtable->get_unsubmitted_cnt()
                        || 0 != blocked_memtable->get_unsynced_cnt())
                       && ObTimeUtility::current_monotonic_time() < deadline) {
                  ob_usleep(1_ms);
                }
                if (apply_drained && 0 == blocked_memtable->get_ref()
                    && 0 == blocked_memtable->get_write_ref()
                    && 0 == blocked_memtable->get_unsubmitted_cnt()
                    && 0 == blocked_memtable->get_unsynced_cnt()) {
                  blocked_memtable->pre_batch_destroy_keybtree();
                  memtable::ObMemtableKeyBtree::batch_destroy();
                  delete blocked_memtable;
                  blocked_memtable = nullptr;
                  RecordProperty("partial_memtable_released", "1");
                } else {
                  ADD_FAILURE() << "retain partial memtable with outstanding work: apply_drained="
                      << apply_drained << " ref=" << blocked_memtable->get_ref()
                      << " write_ref=" << blocked_memtable->get_write_ref()
                      << " unsubmitted=" << blocked_memtable->get_unsubmitted_cnt()
                      << " unsynced=" << blocked_memtable->get_unsynced_cnt();
                  RecordProperty("partial_memtable_released", "0");
                }
              });
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard0 = n1->get_tx_guard();
  ObTxDescGuard secondary_guard1 = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc *secondaries[] = {
      &secondary_guard0.get_tx_desc(), &secondary_guard1.get_tx_desc()};
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 12000, 13000));
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, n1->get_read_snapshot(
      primary, tx_param.isolation_, n1->ts_after_ms(1000), snapshot));
  const bool partial_member = 4 == failure_mode;
  if (partial_member) {
    ASSERT_EQ(OB_SUCCESS, n1->create_memtable_(100002, blocked_memtable));
  }
  NAMED_DEFER(unblock_partial_member,
              if (nullptr != blocked_memtable) { blocked_memtable->unset_logging_blocked(); });
  ObAggregatedTxIDArray members;
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*secondaries[i], tx_param));
    secondaries[i]->set_hotspot_group_commit_info(primary.tx_id_, false);
    if (i < empty_members) {
      // Establish a real participant through the write API, without creating
      // any row callback. No status or cache counter is fabricated here.
      ObStoreCtx store_ctx;
      ASSERT_EQ(OB_SUCCESS, n1->write_begin(*secondaries[i], snapshot, store_ctx));
      ASSERT_EQ(OB_SUCCESS, n1->write_end(store_ctx));
    } else {
      ASSERT_EQ(OB_SUCCESS, n1->write(*secondaries[i], snapshot, 12001 + i, 13001 + i));
      if (partial_member && 0 == i) {
        memtable::ObMemtable *saved_memtable = n1->memtable_;
        n1->memtable_ = blocked_memtable;
        const int write_ret = n1->write(*secondaries[i], snapshot, 12003, 13003);
        n1->memtable_ = saved_memtable;
        ASSERT_EQ(OB_SUCCESS, write_ret);
      }
    }
    ASSERT_EQ(OB_SUCCESS, members.push_back(secondaries[i]->tx_id_));
  }
  ASSERT_TRUE(wait_apply());
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  NAMED_DEFER(revert_mgr, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
  ObPartTransCtx *primary_ctx = nullptr;
  ObPartTransCtx *secondary_ctx[2] = {nullptr, nullptr};
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary.tx_id_, false, primary_ctx));
  NAMED_DEFER(revert_primary_ctx,
              if (nullptr != primary_ctx) { mgr->revert_tx_ctx(primary_ctx); });
  NAMED_DEFER(revert_secondary_ctxs,
              for (int i = 0; i < 2; ++i) {
                if (nullptr != secondary_ctx[i]) {
                  mgr->revert_tx_ctx(secondary_ctx[i]);
                }
              });
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(secondaries[i]->tx_id_, false, secondary_ctx[i]));
  }

  auto abort_group_and_verify = [&](const bool was_collecting) {
    n1->fake_tx_log_adapter_->clear_pause();
    n1->fake_tx_log_adapter_->wakeup();
    ASSERT_TRUE(wait_apply());
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary, OB_TRANS_NEED_ROLLBACK));
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*secondaries[i], OB_TRANS_NEED_ROLLBACK));
    }
    n1->wait_all_msg_consumed();
    ASSERT_TRUE(wait_apply());
    const int64_t deadline = ObTimeUtility::current_monotonic_time() + 5_s;
    while (ObTimeUtility::current_monotonic_time() < deadline) {
      {
        CtxLockGuard guard(primary_ctx->lock_);
        if (!primary_ctx->is_exiting_) {
          (void)primary_ctx->do_local_abort_tx_();
        }
      }
      if (primary_ctx->is_exiting_ && 0 == primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count()) {
        break;
      }
      ob_usleep(1_ms);
    }
    ASSERT_EQ(0, mgr->get_hotspot_aggregation_count());
    if (was_collecting) {
      EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_FAILED, primary_ctx->redo_flush_status_);
    }
    EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
    EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_busy_cb_count());
    EXPECT_TRUE(primary_ctx->is_exiting_);
    EXPECT_EQ(OB_SUCCESS, primary_ctx->clean_hotspot_redo_cache());
    EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
    for (int i = 0; i < 2; ++i) {
      EXPECT_TRUE(secondary_ctx[i]->is_exiting_);
      EXPECT_EQ(OB_TRANS_NEED_ROLLBACK,
                secondary_ctx[i]->set_secondary_hotspot_redo_status_by_exit_state(
                    TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED));
      EXPECT_EQ(1, secondary_ctx[i]->get_ref());
    }
    ASSERT_EQ(OB_SUCCESS, primary_guard.release());
    ASSERT_EQ(OB_SUCCESS, secondary_guard0.release());
    ASSERT_EQ(OB_SUCCESS, secondary_guard1.release());
  };

  // The adapter accepts/queues real log callbacks while its apply worker is
  // paused. This separates submit success from sync without changing states.
  if (controlled_retry) {
    n1->add_drop_msg_type(TX_HOTSPOT_DISPATCH_REDO);
    n1->add_drop_msg_type(TX_HOTSPOT_SUBMIT_OTHER_REDO);
  }
  NAMED_DEFER(restore_submit_delivery,
              n1->del_drop_msg_type(TX_HOTSPOT_DISPATCH_REDO);
              n1->del_drop_msg_type(TX_HOTSPOT_SUBMIT_OTHER_REDO);
              n1->fake_tx_log_adapter_->clear_block_log_type());
  n1->fake_tx_log_adapter_->set_pause();
  NAMED_DEFER(resume_apply_worker,
              n1->fake_tx_log_adapter_->clear_pause();
              n1->fake_tx_log_adapter_->wakeup());
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  if (!controlled_retry) {
    ASSERT_EQ(OB_SUCCESS, n1->txs_.sync_hotspot_legality_validation(
        participant, primary.tx_id_, members, ObTimeUtility::fast_current_time() + 5_s));
  } else {
    // Use the same production setup and submission functions, but drive one
    // real log callback at a time instead of the asynchronous dispatcher.
    ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_legality_validation_(true));
    ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.init(primary.tx_id_, 2, primary_ctx));
    for (int i = 0; i < 2; ++i) {
      ObPartTransCtx *cache_member = nullptr;
      ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(secondaries[i]->tx_id_, false, cache_member));
      const int legality_ret = cache_member->hotspot_legality_validation_(false);
      const int insert_ret = OB_SUCCESS == legality_ret
          ? primary_ctx->hotspot_redo_cache_.insert_into(cache_member) : legality_ret;
      if (OB_SUCCESS != insert_ret) {
        mgr->revert_tx_ctx(cache_member);
      }
      ASSERT_EQ(OB_SUCCESS, insert_ret);
    }
    ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.assign_remapped_seq_ranges(
        primary_ctx->first_scn_, primary_ctx->last_scn_));
    if (2 == empty_members) {
      if (3 == failure_mode) {
        CtxLockGuard guard(secondary_ctx[1]->lock_);
        ASSERT_EQ(OB_EAGAIN, secondary_ctx[1]->do_local_abort_tx_());
      }
      ObPartTransCtx::HotspotCallStage call_stage = ObPartTransCtx::HotspotCallStage::INIT;
      const int init_ret = primary_ctx->set_primary_hotspot_migrating_status_(call_stage);
      EXPECT_EQ(ObPartTransCtx::HotspotCallStage::COLLECTING, call_stage);
      if (3 == failure_mode) {
        EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, init_ret);
        EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
        EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
        EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[0]->redo_flush_status_);
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_PREPARING, secondary_ctx[1]->redo_flush_status_);
        bool finished = true;
        ASSERT_EQ(OB_SUCCESS, primary_ctx->try_finish_hotspot_aggregation_(finished));
        EXPECT_FALSE(finished);
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_delay_rollback_tx_ctx("empty S aborted during initialization"));
        abort_group_and_verify(true);
        return;
      }
      ASSERT_EQ(OB_SUCCESS, init_ret);
      // Retry the actual COLLECTING initialization without fabricating a SCN.
      call_stage = ObPartTransCtx::HotspotCallStage::ADMITTED;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->set_primary_hotspot_migrating_status_(call_stage));
      EXPECT_EQ(ObPartTransCtx::HotspotCallStage::COLLECTING, call_stage);
      for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[i]->redo_flush_status_);
      }
      bool finished = false;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->try_finish_hotspot_aggregation_(finished));
      ASSERT_TRUE(finished);
    } else {
      ObPartTransCtx::HotspotCallStage call_stage = ObPartTransCtx::HotspotCallStage::INIT;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->set_primary_hotspot_migrating_status_(call_stage));
      EXPECT_EQ(ObPartTransCtx::HotspotCallStage::COLLECTING, call_stage);
      if (partial_member) {
        ASSERT_EQ(OB_SUCCESS, blocked_memtable->set_logging_blocked());
      }
      for (int i = 0; i < (partial_member ? 1 : 2); ++i) {
        ObTxLogCb *cb = nullptr;
        ASSERT_EQ(OB_SUCCESS, primary_ctx->get_log_cb_(false, cb));
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.append_hotspot_idle_cb(cb, 2));
      }
      ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.rearrange_hotspot_task());
      ObTxLogCb *first_cb = nullptr;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(first_cb));
      ASSERT_EQ(0, first_cb->get_hotspot_task(0));
      ASSERT_EQ(OB_SUCCESS, primary_ctx->fill_and_submit_hotspot_redo_(first_cb, false));
      EXPECT_EQ(partial_member ? TxRedoFlushStatus::SECONDARY_MIGRATING
                               : TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED,
                secondary_ctx[0]->redo_flush_status_);
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_PREPARING, secondary_ctx[1]->redo_flush_status_);
      EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
      EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
      EXPECT_EQ(1, primary_ctx->hotspot_redo_cache_.get_busy_cb_count());
      if (!partial_member) {
        {
          SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
          const auto &submitted = primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0];
          EXPECT_EQ(0, submitted.unsynced_node_cnt_);
          EXPECT_TRUE(submitted.max_submitted_scn_.is_valid());
        }
        EXPECT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.set_no_redo_secondaries_synced());
        EXPECT_FALSE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED, secondary_ctx[0]->redo_flush_status_);
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_PREPARING, secondary_ctx[1]->redo_flush_status_);
        EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
      }
      bool aggregation_finished = true;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->try_finish_hotspot_aggregation_(aggregation_finished));
      EXPECT_FALSE(aggregation_finished);
      EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());

      if (partial_member) {
        auto deliver_partial_success = [&]() {
          int delivered = 0;
          for (int64_t i = 0; i < ObFakeTxLogAdapter::TASK_QUEUE_CNT; ++i) {
            while (ObLink *task = n1->fake_tx_log_adapter_->apply_task_queue_arr[i].pop()) {
              auto *apply_task = static_cast<ObFakeTxLogAdapter::ApplyCbTask *>(task);
              EXPECT_EQ(OB_SUCCESS, apply_task->cb_->on_success());
              delete apply_task;
              ATOMIC_DEC(&n1->fake_tx_log_adapter_->inflight_cnt_);
              ++delivered;
            }
          }
          EXPECT_EQ(1, delivered);
        };
        {
          SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
          EXPECT_GT(primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].unsynced_node_cnt_, 0);
        }
        deliver_partial_success();
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATING, secondary_ctx[0]->redo_flush_status_);
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(first_cb));
        EXPECT_EQ(OB_BLOCK_FROZEN, primary_ctx->fill_and_submit_hotspot_redo_(first_cb, false));
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATING, secondary_ctx[0]->redo_flush_status_);
        EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
        blocked_memtable->unset_logging_blocked();

        n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(first_cb));
        EXPECT_EQ(OB_EAGAIN, primary_ctx->fill_and_submit_hotspot_redo_(first_cb, false));
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATING, secondary_ctx[0]->redo_flush_status_);
        EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
        n1->fake_tx_log_adapter_->clear_block_log_type();
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(first_cb));
        ASSERT_EQ(OB_SUCCESS, primary_ctx->fill_and_submit_hotspot_redo_(first_cb, false));
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED, secondary_ctx[0]->redo_flush_status_);
        EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
        EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
        deliver_partial_success();
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[0]->redo_flush_status_);
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.rearrange_hotspot_task());
      }

      // A duplicate registration must reject the completed submission without
      // consuming the real MVCC callback or decrementing the group's count.
      SCN first_submit_scn;
      {
        SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
        first_submit_scn = primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].max_submitted_scn_;
      }
      EXPECT_EQ(OB_ERR_UNEXPECTED, primary_ctx->hotspot_redo_cache_.after_flush_hotspot_redo(
          0, first_submit_scn, OB_SUCCESS));
      EXPECT_EQ(partial_member ? TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED
                               : TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED,
                secondary_ctx[0]->redo_flush_status_);
      EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());

      ObTxLogCb *second_cb = nullptr;
      ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(second_cb));
      ASSERT_EQ(1, second_cb->get_hotspot_task(0));
      n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
      const int failed_submit_ret = primary_ctx->fill_and_submit_hotspot_redo_(second_cb, false);
      EXPECT_EQ(OB_EAGAIN, failed_submit_ret);
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_PREPARING, secondary_ctx[1]->redo_flush_status_);
      EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
      EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
      EXPECT_EQ(partial_member ? 0 : 1, primary_ctx->hotspot_redo_cache_.get_busy_cb_count());
      n1->fake_tx_log_adapter_->clear_block_log_type();
      ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.get_free_cb(second_cb));
      ASSERT_EQ(1, second_cb->get_hotspot_task(0));
  #ifdef ENABLE_DEBUG_LOG
      if (publication_abort) {
        const int64_t old_debug_timeout = GCONF.debug_sync_timeout;
        ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("15s"));
        ObDebugSyncAction reset_action;
        ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
        NAMED_DEFER(reset_publication_sync,
                    (void)GDS.set_global_action(true, false, reset_action);
                    GCONF.debug_sync_timeout = old_debug_timeout);
        ObDebugSyncAction pause;
        pause.sync_point_ = BEFORE_SECONDARY_HOTSPOT_SUBMITTED;
        pause.timeout_ = 10_s;
        pause.execute_ = 1;
        pause.signal_ = "hs_submitted_ready";
        pause.wait_ = "hs_submitted_go";
        ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause));
        auto release_publication = [&]() {
          ObDebugSyncAction release;
          release.sync_point_ = NOW;
          release.timeout_ = 10_s;
          release.execute_ = 1;
          release.signal_ = "hs_submitted_go";
          return GDS.set_global_action(false, false, release);
        };
        std::atomic<int> submit_ret(OB_ERR_UNEXPECTED);
        std::thread submitter([&]() {
          ObTenantEnv::set_tenant(&n1->tenant_);
          submit_ret.store(primary_ctx->fill_and_submit_hotspot_redo_(second_cb, false));
        });
        NAMED_DEFER(join_submitter,
                    (void)release_publication();
                    if (submitter.joinable()) { submitter.join(); });
        ObDebugSyncAction wait;
        wait.sync_point_ = NOW;
        wait.timeout_ = 10_s;
        wait.execute_ = 1;
        wait.wait_ = "hs_submitted_ready";
        const int64_t wait_start = ObTimeUtility::current_monotonic_time();
        ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait));
        ASSERT_LT(ObTimeUtility::current_monotonic_time() - wait_start, 9_s);
        // The submitter holds P/cache locks, but no S lock. The controller must
        // use only S here: real local abort records force-abort and returns retry.
        {
          CtxLockGuard guard(secondary_ctx[1]->lock_);
          EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATING, secondary_ctx[1]->redo_flush_status_);
          EXPECT_EQ(0, secondary_ctx[1]->mt_ctx_.get_unsubmitted_cnt());
          EXPECT_EQ(OB_EAGAIN, secondary_ctx[1]->do_local_abort_tx_());
          EXPECT_TRUE(secondary_ctx[1]->sub_state_.is_force_abort());
          EXPECT_FALSE(secondary_ctx[1]->is_exiting_);
        }
        std::atomic<bool> finish_started(false);
        std::atomic<bool> finish_returned(false);
        bool published_success = true;
        int finish_ret = OB_ERR_UNEXPECTED;
        std::thread finisher([&]() {
          ObTenantEnv::set_tenant(&n1->tenant_);
          finish_started.store(true);
          finish_ret = primary_ctx->try_finish_hotspot_aggregation_(published_success);
          finish_returned.store(true);
        });
        NAMED_DEFER(join_finisher,
                    (void)release_publication();
                    if (finisher.joinable()) { finisher.join(); });
        const int64_t start_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
        while (!finish_started.load() && ObTimeUtility::current_monotonic_time() < start_deadline) {
          ob_usleep(1_ms);
        }
        ASSERT_TRUE(finish_started.load());
        EXPECT_FALSE(finish_returned.load());
        ASSERT_EQ(OB_SUCCESS, release_publication());
        submitter.join();
        finisher.join();
        EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, submit_ret.load());
        EXPECT_EQ(OB_SUCCESS, finish_ret);
        EXPECT_FALSE(published_success);
        EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.all_redo_flushed());
        EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
        EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATING, secondary_ctx[1]->redo_flush_status_);
        EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
        EXPECT_EQ(2, primary_ctx->hotspot_redo_cache_.get_busy_cb_count());
        // Preserve the accepted append's busy callback/ref until real delivery.
        abort_group_and_verify(true);
        return;
      }
  #else
      UNUSED(publication_abort);
  #endif
      ASSERT_EQ(OB_SUCCESS, primary_ctx->fill_and_submit_hotspot_redo_(second_cb, false));
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED, secondary_ctx[1]->redo_flush_status_);
      EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, primary_ctx->redo_flush_status_);
      EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
    }
  }
  {
    CtxLockGuard guard(primary_ctx->lock_);
    EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, primary_ctx->redo_flush_status_);
  }
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.all_redo_flushed());
  EXPECT_EQ(2 == empty_members, primary_ctx->hotspot_redo_cache_.all_redo_synced());
  if (empty_members < 2) {
    EXPECT_GT(n1->fake_tx_log_adapter_->get_inflight_cnt(), 0);
  }
  if (8 == failure_mode) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, n1->submit_commit_tx(
          *secondaries[i], n1->ts_after_ms(5000), submitted_secondary_cb[i], nullptr));
      EXPECT_EQ(ObTxDesc::State::IN_TERMINATE, secondaries[i]->state_);
      EXPECT_EQ(static_cast<int64_t>(ObPartTransAction::COMMIT), secondary_ctx[i]->get_part_trans_action());
    }
    ASSERT_EQ(OB_SUCCESS, n1->submit_commit_tx(
        primary, n1->ts_after_ms(5000), submitted_primary_cb, nullptr));
    const int64_t commit_submit_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
    bool state_log_submitted = false;
    while (!state_log_submitted && ObTimeUtility::current_monotonic_time() < commit_submit_deadline) {
      {
        CtxLockGuard guard(primary_ctx->lock_);
        state_log_submitted = primary_ctx->sub_state_.is_state_log_submitted();
      }
      if (!state_log_submitted) { ob_usleep(1_ms); }
    }
    ASSERT_TRUE(state_log_submitted);
  }
  if (1 == failure_mode || 7 == failure_mode || 8 == failure_mode) {
    {
      CtxLockGuard guard(secondary_ctx[0]->lock_);
      ASSERT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED, secondary_ctx[0]->redo_flush_status_);
      EXPECT_EQ(OB_EAGAIN, secondary_ctx[0]->do_local_abort_tx_());
      EXPECT_TRUE(secondary_ctx[0]->sub_state_.is_force_abort());
      EXPECT_FALSE(secondary_ctx[0]->is_exiting_);
    }
    EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
    EXPECT_GT(primary_ctx->hotspot_redo_cache_.get_busy_cb_count(), 0);
    EXPECT_FALSE(primary_ctx->sub_state_.is_force_abort());
    EXPECT_FALSE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
    if (7 == failure_mode) {
      const SCN version_before = secondary_ctx[0]->mt_ctx_.get_trans_version();
      SCN prepare_version;
      ASSERT_EQ(OB_SUCCESS, prepare_version.convert_for_tx(n1->ts_after_ms(1000)));
      EXPECT_EQ(OB_TRANS_NEED_ROLLBACK,
                primary_ctx->hotspot_redo_cache_.before_prepare_secondaries(prepare_version));
      EXPECT_EQ(version_before, secondary_ctx[0]->mt_ctx_.get_trans_version());
      EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
    }
    if (8 == failure_mode) {
      CtxLockGuard guard(primary_ctx->lock_);
      EXPECT_EQ(OB_TRANS_NEED_ROLLBACK, primary_ctx->publish_mvcc_prepare_barrier_(
          primary_ctx->ctx_tx_data_.get_commit_version()));
      EXPECT_FALSE(primary_ctx->sub_state_.is_force_abort());
    }
    int successes = 0;
    for (int64_t i = 0; i < ObFakeTxLogAdapter::TASK_QUEUE_CNT; ++i) {
      while (ObLink *task = n1->fake_tx_log_adapter_->apply_task_queue_arr[i].pop()) {
        auto *apply_task = static_cast<ObFakeTxLogAdapter::ApplyCbTask *>(task);
        EXPECT_EQ(OB_SUCCESS, apply_task->cb_->on_success());
        delete apply_task;
        ATOMIC_DEC(&n1->fake_tx_log_adapter_->inflight_cnt_);
        ++successes;
      }
    }
    EXPECT_EQ(8 == failure_mode ? 3 : 2, successes);
    if (8 == failure_mode) {
      EXPECT_FALSE(primary_ctx->sub_state_.is_force_abort());
      n1->fake_tx_log_adapter_->clear_pause();
      n1->fake_tx_log_adapter_->wakeup();
      ASSERT_TRUE(wait_apply());
      const int64_t commit_done_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
      while (0 == submitted_primary_cb.count_.load()
             && ObTimeUtility::current_monotonic_time() < commit_done_deadline) {
        ob_usleep(1_ms);
      }
      EXPECT_EQ(1, submitted_primary_cb.count_.load());
      EXPECT_EQ(OB_SUCCESS, submitted_primary_cb.result_.load());
      EXPECT_EQ(ObTxDesc::State::COMMITTED, primary.state_);
      EXPECT_FALSE(primary_ctx->sub_state_.is_force_abort());
      for (int i = 0; i < 2; ++i) {
        const int64_t secondary_done_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
        while (0 == submitted_secondary_cb[i].count_.load()
               && ObTimeUtility::current_monotonic_time() < secondary_done_deadline) {
          ob_usleep(1_ms);
        }
        EXPECT_EQ(1, submitted_secondary_cb[i].count_.load());
        EXPECT_EQ(OB_SUCCESS, submitted_secondary_cb[i].result_.load());
        EXPECT_EQ(ObTxDesc::State::COMMITTED, secondaries[i]->state_);
      }
      n1->wait_all_msg_consumed();
      ASSERT_TRUE(wait_apply());
      const int64_t cleanup_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
      while (0 != primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count()
             && ObTimeUtility::current_monotonic_time() < cleanup_deadline) {
        ob_usleep(1_ms);
      }
      EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
      for (int i = 0; i < 2; ++i) {
        EXPECT_TRUE(secondary_ctx[i]->is_exiting_);
        EXPECT_EQ(1, secondary_ctx[i]->get_ref());
      }
      ASSERT_EQ(OB_SUCCESS, primary_guard.release());
      ASSERT_EQ(OB_SUCCESS, secondary_guard0.release());
      ASSERT_EQ(OB_SUCCESS, secondary_guard1.release());
      return;
    }
    EXPECT_TRUE(primary_ctx->sub_state_.is_force_abort());
    EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.need_rollback_primary_tx());
    auto &redo = secondary_ctx[0]->mt_ctx_.get_redo_generator();
    EXPECT_EQ(1, redo.get_redo_sync_succ_count());
    EXPECT_EQ(redo.get_redo_filled_count(), redo.get_redo_sync_succ_count());
    abort_group_and_verify(false);
    return;
  } else if (2 == failure_mode || 5 == failure_mode) {
    if (5 == failure_mode) {
      ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_delay_rollback_tx_ctx("abort members before log failure"));
      {
        CtxLockGuard guard(primary_ctx->lock_);
        ASSERT_EQ(OB_SUCCESS, primary_ctx->hotspot_redo_cache_.abort_secondary_txs(
            ObTxAbortCause::PRIMARY_TX_ABORTED));
      }
      for (int i = 0; i < 2; ++i) {
        CtxLockGuard guard(secondary_ctx[i]->lock_);
        EXPECT_TRUE(secondary_ctx[i]->is_exiting_);
        EXPECT_EQ(0, secondary_ctx[i]->mt_ctx_.get_main_list_length());
        EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, secondary_ctx[i]->redo_flush_status_);
      }
      EXPECT_EQ(2, primary_ctx->hotspot_redo_cache_.get_busy_cb_count());
    }
    // The worker is paused before any of these submissions. Consume each
    // queued transport task exactly once and call its real failure callback.
    int failures = 0;
    for (int64_t i = 0; i < ObFakeTxLogAdapter::TASK_QUEUE_CNT; ++i) {
      while (ObLink *task = n1->fake_tx_log_adapter_->apply_task_queue_arr[i].pop()) {
        auto *apply_task = static_cast<ObFakeTxLogAdapter::ApplyCbTask *>(task);
        EXPECT_EQ(OB_SUCCESS, apply_task->cb_->on_failure());
        delete apply_task;
        ATOMIC_DEC(&n1->fake_tx_log_adapter_->inflight_cnt_);
        ++failures;
      }
    }
    EXPECT_EQ(2, failures);
    EXPECT_EQ(0, n1->fake_tx_log_adapter_->get_inflight_cnt());
    {
      SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      ASSERT_NE(nullptr, primary_ctx->hotspot_redo_cache_.cache_);
      EXPECT_FALSE(primary_ctx->hotspot_redo_cache_.cache_->has_gap_cb_());
    }
    EXPECT_TRUE(primary_ctx->sub_state_.is_force_abort());
    for (int i = 0; i < 2; ++i) {
      CtxLockGuard guard(secondary_ctx[i]->lock_);
      EXPECT_NE(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[i]->redo_flush_status_);
      auto &redo = secondary_ctx[i]->mt_ctx_.get_redo_generator();
      EXPECT_EQ(1, redo.get_redo_sync_fail_count());
      EXPECT_EQ(redo.get_redo_filled_count(),
                redo.get_redo_sync_succ_count() + redo.get_redo_sync_fail_count());
    }
    abort_group_and_verify(false);
    return;
  }
  if (6 == failure_mode) {
    {
      SpinWLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      auto *cache = primary_ctx->hotspot_redo_cache_.cache_;
      auto &slot = cache->hotspot_cache_[0];
      slot.set_resp_status(TxSecondaryRespStatus::RESP_COMMITTED);
      EXPECT_EQ(OB_STATE_NOT_MATCH, cache->set_secondary_terminal_redo_status_(0));
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED, secondary_ctx[0]->redo_flush_status_);
      slot.set_resp_status(TxSecondaryRespStatus::RESP_NOT_SENT);
    }
    // Defensive fixture injection: real callbacks are delivered below, but
    // an illegal local redo status makes their final SYNCED publication fail.
    // This does not claim NORMAL_START can occur naturally after SUBMITTED.
    {
      CtxLockGuard guard(secondary_ctx[0]->lock_);
      secondary_ctx[0]->redo_flush_status_ = TxRedoFlushStatus::NORMAL_START;
    }
    int successes = 0;
    for (int64_t i = 0; i < ObFakeTxLogAdapter::TASK_QUEUE_CNT; ++i) {
      while (ObLink *task = n1->fake_tx_log_adapter_->apply_task_queue_arr[i].pop()) {
        auto *apply_task = static_cast<ObFakeTxLogAdapter::ApplyCbTask *>(task);
        EXPECT_EQ(OB_SUCCESS, apply_task->cb_->on_success());
        delete apply_task;
        ATOMIC_DEC(&n1->fake_tx_log_adapter_->inflight_cnt_);
        ++successes;
      }
    }
    EXPECT_EQ(2, successes);
    {
      SpinWLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      auto *cache = primary_ctx->hotspot_redo_cache_.cache_;
      auto &slot = cache->hotspot_cache_[0];
      EXPECT_EQ(0, slot.need_remove_cnt_);
      EXPECT_EQ(0, secondary_ctx[0]->mt_ctx_.get_main_list_length());
      EXPECT_EQ(TxRedoFlushStatus::NORMAL_START, secondary_ctx[0]->redo_flush_status_);
      EXPECT_EQ(OB_STATE_NOT_MATCH, cache->handle_commit_response_(0, SCN::min_scn(), false));
      EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT, slot.get_resp_status());
      EXPECT_EQ(OB_EAGAIN, cache->response_scheduler(OB_SUCCESS, SCN::min_scn(), false));
      EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT, slot.get_resp_status());
      EXPECT_EQ(OB_INVALID_ARGUMENT, cache->last_response_succ_ret_code_);
      {
        CtxLockGuard ctx_guard(secondary_ctx[0]->lock_);
        secondary_ctx[0]->redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED;
      }
      int64_t need_remove = -1;
      EXPECT_EQ(OB_SUCCESS, cache->remove_synced_hotspot_redo_(0, need_remove));
      EXPECT_EQ(0, need_remove);
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[0]->redo_flush_status_);
      auto &member = cache->hotspot_cache_[1];
      {
        CtxLockGuard ctx_guard(secondary_ctx[1]->lock_);
        secondary_ctx[1]->redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED;
      }
      member.set_resp_status(TxSecondaryRespStatus::RESP_SKIPPED_BY_PRIMARY);
      EXPECT_EQ(OB_SUCCESS, cache->set_secondary_terminal_redo_status_(1));
      EXPECT_EQ(OB_SUCCESS, cache->set_secondary_terminal_redo_status_(1));
      member.set_resp_status(TxSecondaryRespStatus::RESP_COMMITTED);
      EXPECT_EQ(OB_SUCCESS, cache->set_secondary_terminal_redo_status_(1));
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, secondary_ctx[1]->redo_flush_status_);
      // Restore only fixture-injected state before the normal commit path.
      member.set_resp_status(TxSecondaryRespStatus::RESP_NOT_SENT);
      CtxLockGuard ctx_guard(secondary_ctx[1]->lock_);
      secondary_ctx[1]->redo_flush_status_ = TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED;
    }
  }
  SCN submitted_scns[2];
  {
    SpinRLockGuard cache_guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    auto *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_NE(nullptr, cache);
    ASSERT_EQ(2, cache->hotspot_cache_count_);
    for (int i = 0; i < 2; ++i) {
      const auto &slot = cache->hotspot_cache_[i];
      EXPECT_EQ(secondaries[i]->tx_id_, slot.tx_id_);
      submitted_scns[i] = slot.max_submitted_scn_;
      EXPECT_TRUE(slot.seq_remapped_);
      if (i < empty_members) {
        EXPECT_EQ(0, slot.unsynced_node_cnt_);
        EXPECT_EQ(0, slot.need_remove_cnt_);
        EXPECT_FALSE(slot.max_submitted_scn_.is_valid());
      } else {
        EXPECT_EQ(0, slot.unsynced_node_cnt_);
        EXPECT_TRUE(slot.max_submitted_scn_.is_valid());
        if (6 == failure_mode || (partial_member && 0 == i)) {
          EXPECT_EQ(0, slot.need_remove_cnt_);
          EXPECT_GE(slot.max_synced_scn_, slot.max_submitted_scn_);
        } else {
          EXPECT_GT(slot.need_remove_cnt_, 0);
          EXPECT_LT(slot.max_synced_scn_, slot.max_submitted_scn_);
        }
        EXPECT_LE(slot.remapped_min_seq_, slot.remapped_max_seq_);
      }
    }
  }

  for (int i = 0; i < 2; ++i) {
    {
      CtxLockGuard guard(secondary_ctx[i]->lock_);
      const bool synced = 6 == failure_mode || i < empty_members || (partial_member && 0 == i);
      EXPECT_EQ(synced ? TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED
                      : TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED,
                secondary_ctx[i]->redo_flush_status_);
      EXPECT_FALSE(secondary_ctx[i]->is_exiting_);
      EXPECT_TRUE(secondary_ctx[i]->can_not_submit_log_for_hotspot_());
      EXPECT_EQ(!synced, secondary_ctx[i]->is_in_active_aggregation_());
    }
    const int64_t lsn_before = n1->fake_tx_log_adapter_->lsn_;
    const int64_t ref_before = secondary_ctx[i]->get_ref();
    ObTxCommitCallback *role_callbacks = nullptr;
    ASSERT_EQ(OB_SUCCESS, secondary_ctx[i]->switch_to_follower_gracefully(role_callbacks));
    EXPECT_EQ(nullptr, role_callbacks);
    EXPECT_EQ(lsn_before, n1->fake_tx_log_adapter_->lsn_);
    EXPECT_EQ(ref_before, secondary_ctx[i]->get_ref());
    {
      CtxLockGuard guard(secondary_ctx[i]->lock_);
      EXPECT_TRUE(secondary_ctx[i]->is_follower_());
      EXPECT_FALSE(secondary_ctx[i]->is_exiting_);
      EXPECT_EQ(0, secondary_ctx[i]->busy_cbs_.get_size());
    }
  }
  // Direct manager handoff is the tx-layer assertion. It deliberately does
  // not claim to exercise RoleChangeService / ApplyService's outer drain.
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  EXPECT_FALSE(mgr->is_master());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());

  n1->fake_tx_log_adapter_->clear_pause();
  n1->fake_tx_log_adapter_->wakeup();
  const int64_t sync_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  bool all_synced = false;
  while (!all_synced && ObTimeUtility::current_monotonic_time() < sync_deadline) {
    all_synced = true;
    for (int i = 0; i < 2; ++i) {
      CtxLockGuard guard(secondary_ctx[i]->lock_);
      all_synced = all_synced
          && TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED == secondary_ctx[i]->redo_flush_status_;
    }
    if (!all_synced) {
      ob_usleep(1_ms);
    }
  }
  ASSERT_TRUE(all_synced);
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(OB_SUCCESS, secondary_ctx[i]->set_secondary_hotspot_redo_status_by_exit_state(
        TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED));
    EXPECT_EQ(OB_SUCCESS, secondary_ctx[i]->set_secondary_hotspot_redo_status_by_exit_state(
        TxRedoFlushStatus::SECONDARY_MIGRATING));
    EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_ctx[i]->redo_flush_status_);
  }
  ASSERT_TRUE(wait_apply());
  EXPECT_EQ(0, n1->fake_tx_log_adapter_->get_inflight_cnt());
  {
    SpinRLockGuard cache_guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    auto *cache = primary_ctx->hotspot_redo_cache_.cache_;
    ASSERT_NE(nullptr, cache);
    for (int i = 0; i < 2; ++i) {
      EXPECT_EQ(0, cache->hotspot_cache_[i].need_remove_cnt_);
      EXPECT_EQ(submitted_scns[i], cache->hotspot_cache_[i].max_submitted_scn_);
      if (i >= empty_members) {
        EXPECT_GE(cache->hotspot_cache_[i].max_synced_scn_, submitted_scns[i]);
      }
      EXPECT_TRUE(secondary_ctx[i]->is_follower_());
      EXPECT_FALSE(secondary_ctx[i]->is_exiting_);
    }
  }
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  ASSERT_TRUE(wait_apply());
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary, n1->ts_after_ms(5000)));
  for (int i = 0; i < 2; ++i) {
    const int commit_ret = n1->commit_tx(*secondaries[i], n1->ts_after_ms(5000));
    EXPECT_TRUE(OB_SUCCESS == commit_ret || OB_TRANS_COMMITED == commit_ret) << commit_ret;
    EXPECT_EQ(ObTxDesc::State::COMMITTED, secondaries[i]->state_);
  }
  n1->wait_all_msg_consumed();
  ASSERT_TRUE(wait_apply());
  const int64_t response_cleanup_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while (0 != primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count()
         && ObTimeUtility::current_monotonic_time() < response_cleanup_deadline) {
    ob_usleep(1_ms);
  }
  EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(secondary_ctx[i]->is_exiting_);
    if (6 == failure_mode) {
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, secondary_ctx[i]->redo_flush_status_);
      EXPECT_EQ(OB_SUCCESS, secondary_ctx[i]->set_secondary_hotspot_redo_status_by_exit_state(
          TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED));
      EXPECT_EQ(OB_SUCCESS, secondary_ctx[i]->set_secondary_hotspot_redo_status_by_exit_state(
          TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED, secondary_ctx[i]->redo_flush_status_);
    }
    EXPECT_EQ(1, secondary_ctx[i]->get_ref()); // Only this test's lookup remains.
    ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(secondary_ctx[i]));
    secondary_ctx[i] = nullptr;
  }
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard0.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard1.release());
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(primary_ctx));
  primary_ctx = nullptr;
  EXPECT_EQ(0, mgr->get_tx_ctx_count());
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_real_redo_follower_sync)
{
  run_secondary_submitted_case(0);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_one_empty_member)
{
  run_secondary_submitted_case(1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_member_progress_and_submit_retry)
{
  run_secondary_submitted_case(0, true);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_all_empty_members)
{
  run_secondary_submitted_case(2);
}

#ifdef ENABLE_DEBUG_LOG
TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_publication_abort_blocks_primary_success)
{
  run_secondary_submitted_case(0, true, true);
}
#endif

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_abort_keeps_member_until_group_cleanup)
{
  run_secondary_submitted_case(0, false, false, 1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_abort_after_primary_state_log_keeps_commit)
{
  run_secondary_submitted_case(0, false, false, 8);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_prepare_rejects_aborted_member)
{
  run_secondary_submitted_case(0, false, false, 7);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_synced_publication_retry_and_terminal_idempotency)
{
  run_secondary_submitted_case(0, false, false, 6);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_log_sync_failure_cleanup)
{
  run_secondary_submitted_case(0, false, false, 2);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_sync_failure_after_member_abort)
{
  run_secondary_submitted_case(0, false, false, 5);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_empty_initialization_is_idempotent)
{
  run_secondary_submitted_case(2, true);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_empty_initialization_abort_blocks_success)
{
  run_secondary_submitted_case(2, true, false, 3);
}

TEST_F(ObTestHotspotTxLeaderSwitch, secondary_submitted_partial_member_submit_retry)
{
  run_secondary_submitted_case(0, true, false, 4);
}

// Q01/Q02: exercise both linearization orders of the joint blocked/count CAS,
// including the low-63-bit capacity boundary and underflow protection.
TEST_F(ObTestHotspotTxLeaderSwitch, q01_q02_joint_hotspot_admission_state)
{
  ObLSTxCtxMgr mgr;
  mgr.tx_aggregation_state_.value_ = 0;

  ASSERT_EQ(OB_SUCCESS, mgr.acquire_hotspot_aggregation());
  ASSERT_EQ(1, mgr.get_hotspot_aggregation_count());
  mgr.block_hotspot_aggregation_();
  ASSERT_TRUE(mgr.is_hotspot_aggregation_blocked());
  ASSERT_EQ(1, mgr.get_hotspot_aggregation_count());
  ASSERT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, mgr.acquire_hotspot_aggregation());
  ASSERT_EQ(OB_SUCCESS, mgr.release_hotspot_aggregation());
  ASSERT_EQ(0, mgr.get_hotspot_aggregation_count());
  ASSERT_EQ(OB_ERR_UNEXPECTED, mgr.release_hotspot_aggregation());

  mgr.tx_aggregation_state_.value_ = 0;
  mgr.block_hotspot_aggregation_();
  ASSERT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, mgr.acquire_hotspot_aggregation());
  ASSERT_EQ(0, mgr.get_hotspot_aggregation_count());

  mgr.tx_aggregation_state_.value_ = ObLSTxCtxMgr::HOTSPOT_AGGREGATION_MAX_COUNT;
  ASSERT_EQ(OB_SIZE_OVERFLOW, mgr.acquire_hotspot_aggregation());
  ASSERT_EQ(ObLSTxCtxMgr::HOTSPOT_AGGREGATION_MAX_COUNT,
            mgr.get_hotspot_aggregation_count());
}

TEST_F(ObTestHotspotTxLeaderSwitch, hotspot_aggregation_count_calibration_requires_stable_scan)
{
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_calibration_self_addr = GCONF.self_addr_;
  NAMED_DEFER(restore_calibration_self_addr,
              GCONF.self_addr_ = saved_calibration_self_addr);
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard p1_guard = n1->get_tx_guard();
  ObTxDescGuard s1_guard = n1->get_tx_guard();
  ObTxDescGuard p2_guard = n1->get_tx_guard();
  ObTxDescGuard s2_guard = n1->get_tx_guard();
  ObTxDesc &p1 = p1_guard.get_tx_desc();
  ObTxDesc &s1 = s1_guard.get_tx_desc();
  ObTxDesc &p2 = p2_guard.get_tx_desc();
  ObTxDesc &s2 = s2_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(p1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(s1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(p2, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(s2, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(p1, 9701, 9801));
  ASSERT_EQ(OB_SUCCESS, n1->write(s1, 9702, 9802));
  ASSERT_EQ(OB_SUCCESS, n1->write(p2, 9703, 9803));
  ASSERT_EQ(OB_SUCCESS, n1->write(s2, 9704, 9804));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members1;
  ObAggregatedTxIDArray members2;
  ASSERT_EQ(OB_SUCCESS, members1.push_back(s1.tx_id_));
  ASSERT_EQ(OB_SUCCESS, members2.push_back(s2.tx_id_));
  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
  NAMED_DEFER(unblock_calibration_redo,
              n1->fake_tx_log_adapter_->clear_block_log_type());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, p1.tx_id_, members1,
                ObTimeUtility::fast_current_time() + 20 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, p2.tx_id_, members2,
                ObTimeUtility::fast_current_time() + 20 * 1000));

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  NAMED_DEFER(revert_calibration_mgr,
              if (nullptr != mgr) {
                (void)n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr);
              });
  ObPartTransCtx *p1_ctx = nullptr;
  ObPartTransCtx *p2_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(p1.tx_id_, false, p1_ctx));
  NAMED_DEFER(revert_calibration_p1,
              if (nullptr != p1_ctx) {
                (void)mgr->revert_tx_ctx(p1_ctx);
              });
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(p2.tx_id_, false, p2_ctx));
  NAMED_DEFER(revert_calibration_p2,
              if (nullptr != p2_ctx) {
                (void)mgr->revert_tx_ctx(p2_ctx);
              });
  ASSERT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, p1_ctx->redo_flush_status_);
  ASSERT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, p2_ctx->redo_flush_status_);
  ASSERT_EQ(2, mgr->get_hotspot_aggregation_count());
  mgr->block_hotspot_aggregation_();
  ASSERT_TRUE(mgr->is_hotspot_aggregation_blocked());

  auto set_blocked_count = [&](const uint64_t count) {
    ObLSTxCtxMgr::HotspotAggregationState state(
        ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_));
    state.blocked_ = true;
    state.count_ = count;
    ATOMIC_STORE(&mgr->tx_aggregation_state_.value_, state.value_);
  };
  auto accelerate_until_count = [&](const uint64_t expected_count) {
    int ret = OB_SUCCESS;
    const int64_t deadline = ObTimeUtility::current_monotonic_time() + 5_s;
    do {
      ret = mgr->accelerate_hotspot_aggregations_(deadline);
      if (OB_SUCCESS == ret && expected_count != mgr->get_hotspot_aggregation_count()) {
        ob_usleep(1_ms);
      }
    } while (OB_SUCCESS == ret && expected_count != mgr->get_hotspot_aggregation_count()
             && ObTimeUtility::current_monotonic_time() < deadline);
    return OB_SUCCESS != ret || expected_count == mgr->get_hotspot_aggregation_count()
        ? ret : OB_TIMEOUT;
  };
  set_blocked_count(3);
  ASSERT_EQ(OB_SUCCESS, accelerate_until_count(2));
  EXPECT_EQ(2, mgr->get_hotspot_aggregation_count());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());
  set_blocked_count(1);
  ASSERT_EQ(OB_SUCCESS, accelerate_until_count(2));
  EXPECT_EQ(2, mgr->get_hotspot_aggregation_count());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());

  auto calibrate_from_standard_scan = [&](const int64_t deadline_us) {
    int ret = OB_SUCCESS;
    const uint64_t expected_state = ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_);
    const int64_t total_before_scan = ATOMIC_LOAD(&mgr->total_tx_ctx_count_);
    AccelerateHotspotAggregationFunctor fn(deadline_us);
    if (OB_FAIL(mgr->ls_tx_ctx_map_.for_each(fn))) {
      ret = OB_SUCCESS == fn.get_ret() ? ret : fn.get_ret();
    } else if (fn.get_skipped_count() > 0
               || expected_state != ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_)) {
      ret = OB_EAGAIN;
    } else {
      ret = mgr->calibrate_hotspot_aggregation_count_(
          expected_state, total_before_scan, fn.get_visited_count(),
          fn.get_primary_count(), deadline_us);
    }
    return ret;
  };

  // A busy P makes the calibration snapshot incomplete and must preserve the
  // whole-word count. The thread guard releases the ctx before joining.
  set_blocked_count(3);
  std::atomic<bool> busy_locked(false);
  std::atomic<bool> release_busy(false);
  std::thread busy_holder([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    CtxLockGuard guard(p1_ctx->lock_);
    busy_locked.store(true);
    while (!release_busy.load()) {
      ob_usleep(100);
    }
  });
  NAMED_DEFER(busy_holder_cleanup,
              release_busy.store(true);
              if (busy_holder.joinable()) {
                busy_holder.join();
              });
  const int64_t busy_deadline = ObTimeUtility::current_time() + 5_s;
  while (!busy_locked.load() && ObTimeUtility::current_time() < busy_deadline) {
    ob_usleep(100);
  }
  ASSERT_TRUE(busy_locked.load());
  EXPECT_EQ(OB_EAGAIN, calibrate_from_standard_scan(
                            ObTimeUtility::current_monotonic_time() + 5_s));
  EXPECT_EQ(3, mgr->get_hotspot_aggregation_count());
  release_busy.store(true);
  busy_holder.join();

  EXPECT_EQ(OB_TIMEOUT,
            calibrate_from_standard_scan(ObTimeUtility::current_monotonic_time()));
  EXPECT_EQ(3, mgr->get_hotspot_aggregation_count());

  // Keep an ordinary ctx alive after removing it from the map. The physical
  // total then exceeds the visited count, so completeness rejects calibration.
  ObTxDescGuard outside_guard = n1->get_tx_guard();
  ObTxDesc &outside = outside_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(outside, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(outside, 9705, 9805));
  ObPartTransCtx *outside_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(outside.tx_id_, false, outside_ctx));
  NAMED_DEFER(revert_outside_ctx,
              if (nullptr != outside_ctx) {
                (void)mgr->revert_tx_ctx(outside_ctx);
              });
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(outside, OB_SUCCESS));
  ObPartTransCtx *map_ctx = nullptr;
  int map_ret = OB_SUCCESS;
  const int64_t map_remove_deadline = ObTimeUtility::current_time() + 5_s;
  do {
    map_ret = mgr->get_tx_ctx_directly_from_hash_map(outside.tx_id_, map_ctx);
    if (OB_SUCCESS == map_ret && nullptr != map_ctx) {
      ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(map_ctx));
      map_ctx = nullptr;
      ob_usleep(1_ms);
    }
  } while (OB_SUCCESS == map_ret && ObTimeUtility::current_time() < map_remove_deadline);
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, map_ret);
  EXPECT_TRUE(outside_ctx->is_exiting());
  EXPECT_EQ(OB_EAGAIN, calibrate_from_standard_scan(
                            ObTimeUtility::current_monotonic_time() + 5_s));
  EXPECT_EQ(3, mgr->get_hotspot_aggregation_count());
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(outside_ctx));
  outside_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, outside_guard.release());

  // Calibration uses the exact pre-scan word once. A change after a complete
  // scan must make that CAS fail without adopting the newer count.
  set_blocked_count(3);
  const int64_t stable_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  uint64_t stable_expected = 0;
  int64_t stable_total = 0;
  int64_t stable_visited = 0;
  int64_t stable_primary = 0;
  bool stable_scan_complete = false;
  do {
    stable_expected = ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_);
    stable_total = ATOMIC_LOAD(&mgr->total_tx_ctx_count_);
    AccelerateHotspotAggregationFunctor stable_fn(stable_deadline);
    const int scan_ret = mgr->ls_tx_ctx_map_.for_each(stable_fn);
    stable_visited = stable_fn.get_visited_count();
    stable_primary = stable_fn.get_primary_count();
    stable_scan_complete = OB_SUCCESS == scan_ret && OB_SUCCESS == stable_fn.get_ret()
        && 0 == stable_fn.get_skipped_count()
        && stable_expected == ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_)
        && stable_total == ATOMIC_LOAD(&mgr->total_tx_ctx_count_)
        && stable_total == mgr->ls_tx_ctx_map_.count()
        && stable_total == stable_visited;
    if (!stable_scan_complete) {
      ob_usleep(1_ms);
    }
  } while (!stable_scan_complete
           && ObTimeUtility::current_monotonic_time() < stable_deadline);
  ASSERT_TRUE(stable_scan_complete);
  ASSERT_EQ(2, stable_primary);
  set_blocked_count(4);
  EXPECT_EQ(OB_EAGAIN,
            mgr->calibrate_hotspot_aggregation_count_(
                stable_expected, stable_total, stable_visited,
                stable_primary, stable_deadline));
  EXPECT_EQ(4, mgr->get_hotspot_aggregation_count());

  // An open gate or a zero expected count is outside the repair contract.
  mgr->unblock_hotspot_aggregation_();
  uint64_t rejected_state = ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_);
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            mgr->calibrate_hotspot_aggregation_count_(
                rejected_state, ATOMIC_LOAD(&mgr->total_tx_ctx_count_),
                stable_visited, stable_primary,
                ObTimeUtility::current_monotonic_time() + 5_s));
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  set_blocked_count(0);
  rejected_state = ATOMIC_LOAD(&mgr->tx_aggregation_state_.value_);
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            mgr->calibrate_hotspot_aggregation_count_(
                rejected_state, ATOMIC_LOAD(&mgr->total_tx_ctx_count_),
                stable_visited, stable_primary,
                ObTimeUtility::current_monotonic_time() + 5_s));
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());

  set_blocked_count(3);
  ASSERT_EQ(OB_SUCCESS, accelerate_until_count(2));
  EXPECT_EQ(2, mgr->get_hotspot_aggregation_count());
  n1->fake_tx_log_adapter_->clear_block_log_type();
  set_errsim_retry_timeout(300 * 1000);
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_FALSE(mgr->is_master());
  const auto aggregation_finished = [](const TxRedoFlushStatus status) {
    return TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED == status
        || TxRedoFlushStatus::PRIMARY_COMPLETED == status;
  };
  EXPECT_TRUE(aggregation_finished(p1_ctx->redo_flush_status_));
  EXPECT_TRUE(aggregation_finished(p2_ctx->redo_flush_status_));
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(p1_ctx));
  p1_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(p2_ctx));
  p2_ctx = nullptr;
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(p1, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(s1, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(p2, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(s2, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, p1_guard.release());
  EXPECT_EQ(OB_SUCCESS, s1_guard.release());
  EXPECT_EQ(OB_SUCCESS, p2_guard.release());
  EXPECT_EQ(OB_SUCCESS, s2_guard.release());
}

// Q06/Q07 protocol and local-provenance contract. The P identity and zero-row
// qualification are local descriptor state; only the optional P identity is
// appended to the extensible commit request. Fixed V1 bytes must remain exact.
TEST_F(ObTestHotspotTxLeaderSwitch, q06_q07_provenance_lifecycle_and_commit_message)
{
  const ObTransID primary_id(7001);
  ObTxDesc desc;
  EXPECT_FALSE(desc.hotspot_primary_tx_id_.is_valid());
  EXPECT_FALSE(desc.rollback_on_no_affected_rows_);
  desc.hotspot_primary_tx_id_ = primary_id;
  desc.rollback_on_no_affected_rows_ = true;
  EXPECT_EQ(primary_id, desc.hotspot_primary_tx_id_);
  EXPECT_TRUE(desc.rollback_on_no_affected_rows_);
  ASSERT_EQ(OB_SUCCESS, desc.switch_to_idle());
  EXPECT_FALSE(desc.hotspot_primary_tx_id_.is_valid());
  EXPECT_FALSE(desc.rollback_on_no_affected_rows_);
  desc.hotspot_primary_tx_id_ = primary_id;
  desc.rollback_on_no_affected_rows_ = true;
  desc.reset();
  EXPECT_FALSE(desc.hotspot_primary_tx_id_.is_valid());
  EXPECT_FALSE(desc.rollback_on_no_affected_rows_);

  auto init_msg = [&](ObTxCommitMsg &msg, const uint64_t cluster_version) {
    msg.cluster_version_ = cluster_version;
    msg.tenant_id_ = 1001;
    msg.tx_id_ = ObTransID(7002);
    msg.receiver_ = ObLSID(1);
    msg.sender_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888);
    msg.sender_ = share::SCHEDULER_LS;
    msg.request_id_ = 3;
    msg.timestamp_ = 4;
    msg.epoch_ = 5;
    msg.cluster_id_ = 1;
    msg.expire_ts_ = INT64_MAX;
    ASSERT_EQ(OB_SUCCESS, msg.parts_.push_back(ObLSID(2)));
    msg.app_trace_info_ = ObString::make_string("old-extensible-info");
    msg.app_trace_id_ = ObString::make_string("old-extensible-id");
    msg.commit_start_scn_.set_min();
    msg.hotspot_primary_tx_id_ = primary_id;
  };

  char old_with_primary[4096];
  char old_without_primary[4096];
  int64_t old_with_pos = 0;
  int64_t old_without_pos = 0;
  ObTxCommitMsg old_msg;
  ObTxCommitMsg old_baseline;
  init_msg(old_msg, CLUSTER_VERSION_4_1_0_1);
  init_msg(old_baseline, CLUSTER_VERSION_4_1_0_1);
  old_baseline.hotspot_primary_tx_id_.reset();
  ASSERT_EQ(OB_SUCCESS, old_msg.serialize(old_with_primary, sizeof(old_with_primary), old_with_pos));
  ASSERT_EQ(OB_SUCCESS,
            old_baseline.serialize(old_without_primary, sizeof(old_without_primary), old_without_pos));
  EXPECT_EQ(old_without_pos, old_with_pos);
  EXPECT_EQ(0, MEMCMP(old_with_primary, old_without_primary, old_with_pos));
  ObTxCommitMsg old_decoded;
  int64_t old_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_decoded.deserialize(old_with_primary, old_with_pos, old_decode_pos));
  EXPECT_FALSE(old_decoded.hotspot_primary_tx_id_.is_valid());

  char current_buf[4096];
  int64_t current_pos = 0;
  ObTxCommitMsg current_msg;
  init_msg(current_msg, DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, current_msg.serialize(current_buf, sizeof(current_buf), current_pos));
  ObTxCommitMsg current_decoded;
  int64_t current_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            current_decoded.deserialize(current_buf, current_pos, current_decode_pos));
  EXPECT_EQ(primary_id, current_decoded.hotspot_primary_tx_id_);

  char old_extensible_buf[4096];
  int64_t old_extensible_pos = 0;
  ObTxCommitMsg old_extensible_source;
  init_msg(old_extensible_source, DATA_CURRENT_VERSION);
  ASSERT_EQ(OB_SUCCESS, old_extensible_source.ObTxMsg::serialize(
                            old_extensible_buf, sizeof(old_extensible_buf), old_extensible_pos));
  OldExtensibleCommitMsgBox old_extensible_source_box(old_extensible_source);
  ASSERT_EQ(OB_SUCCESS, old_extensible_source_box.serialize(
                            old_extensible_buf, sizeof(old_extensible_buf), old_extensible_pos));
  ObTxCommitMsg missing_tail_reader;
  int64_t missing_tail_pos = 0;
  ASSERT_EQ(OB_SUCCESS, missing_tail_reader.deserialize(
                            old_extensible_buf, old_extensible_pos, missing_tail_pos));
  EXPECT_EQ(old_extensible_pos, missing_tail_pos);
  EXPECT_FALSE(missing_tail_reader.hotspot_primary_tx_id_.is_valid());
  EXPECT_EQ(old_extensible_source.expire_ts_, missing_tail_reader.expire_ts_);
  ASSERT_EQ(1, missing_tail_reader.parts_.count());
  EXPECT_EQ(ObLSID(2), missing_tail_reader.parts_.at(0));
  EXPECT_EQ(old_extensible_source.app_trace_info_, missing_tail_reader.app_trace_info_);
  EXPECT_EQ(old_extensible_source.app_trace_id_, missing_tail_reader.app_trace_id_);

  ObTxCommitMsg old_schema_reader;
  int64_t old_schema_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            old_schema_reader.ObTxMsg::deserialize(current_buf, current_pos, old_schema_pos));
  OldExtensibleCommitMsgBox old_schema_reader_box(old_schema_reader);
  ASSERT_EQ(OB_SUCCESS,
            old_schema_reader_box.deserialize(current_buf, current_pos, old_schema_pos));
  EXPECT_EQ(current_pos, old_schema_pos);
  EXPECT_EQ(current_msg.expire_ts_, old_schema_reader.expire_ts_);
  ASSERT_EQ(1, old_schema_reader.parts_.count());
  EXPECT_EQ(ObLSID(2), old_schema_reader.parts_.at(0));
  EXPECT_EQ(current_msg.app_trace_info_, old_schema_reader.app_trace_info_);
  EXPECT_EQ(current_msg.app_trace_id_, old_schema_reader.app_trace_id_);
  RecordProperty("q06_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q06_secondary_tx_id", std::to_string(current_msg.tx_id_.get_id()));
  RecordProperty("q07_fixed_v1_bytes", std::to_string(old_with_pos));
  RecordProperty("q07_extensible_bytes", std::to_string(current_pos));
  RecordProperty("q07_old_extensible_bytes", std::to_string(old_extensible_pos));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q06/Q07 protocol observations",
            K(primary_id), "secondary_tx_id", current_msg.tx_id_,
            K(old_with_pos), K(current_pos), K(current_decode_pos));
}

// Q08: conversion is published before both synchronous and asynchronous callbacks.
// Only qualified successful S results convert; unrelated failures keep their result.
TEST_F(ObTestHotspotTxLeaderSwitch, q08_zero_row_result_conversion_and_callback_once)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  const ObTransID primary_id(8001);

  ObTxDescGuard eligible_guard = n1->get_tx_guard();
  ObTxDesc &eligible = eligible_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(eligible, tx_param));
  const ObTransID eligible_id = eligible.tx_id_;
  eligible.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback eligible_cb;
  const int64_t eligible_submit_mts = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS,
            n1->submit_commit_tx(eligible, n1->ts_after_ms(500), eligible_cb, nullptr));
  EXPECT_EQ(1, eligible_cb.count_.load());
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, eligible_cb.result_.load());
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, eligible.state_);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, eligible.commit_out_);
  EXPECT_FALSE(eligible.commit_version_.is_valid());
  EXPECT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(eligible_id, OB_SUCCESS, SCN::min_scn()));
  EXPECT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(eligible_id, OB_TRANS_TIMEOUT, SCN()));
  EXPECT_EQ(1, eligible_cb.count_.load());

  ObTxDescGuard ordinary_guard = n1->get_tx_guard();
  ObTxDesc &ordinary = ordinary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(ordinary, tx_param));
  const ObTransID ordinary_id = ordinary.tx_id_;
  ordinary.set_hotspot_group_commit_info(primary_id, false);
  CountingCommitCallback ordinary_cb;
  ASSERT_EQ(OB_SUCCESS,
            n1->submit_commit_tx(ordinary, n1->ts_after_ms(500), ordinary_cb, nullptr));
  EXPECT_EQ(1, ordinary_cb.count_.load());
  EXPECT_EQ(OB_SUCCESS, ordinary_cb.result_.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, ordinary.state_);
  EXPECT_EQ(OB_SUCCESS, ordinary.commit_out_);

  ObTxDescGuard savepoint_guard = n1->get_tx_guard();
  ObTxDesc &savepoint_member = savepoint_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(savepoint_member, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(savepoint_member, 9801, 9901));
  const ObString savepoint_name = ObString::make_string("q08_sp");
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.create_explicit_savepoint(savepoint_member, savepoint_name, 7, true));
  ASSERT_EQ(OB_SUCCESS, n1->write(savepoint_member, 9802, 9902));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.rollback_to_explicit_savepoint(
                savepoint_member, savepoint_name, n1->ts_after_ms(5000), 7));
  EXPECT_FALSE(savepoint_member.need_rollback_on_no_affected_rows());
  CountingCommitCallback savepoint_cb;
  ASSERT_EQ(OB_SUCCESS,
            n1->submit_commit_tx(
                savepoint_member, n1->ts_after_ms(5000), savepoint_cb, nullptr));
  const int64_t savepoint_commit_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while (0 == savepoint_cb.count_.load()
         && ObTimeUtility::current_monotonic_time() < savepoint_commit_deadline) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    ob_usleep(1_ms);
  }
  EXPECT_EQ(1, savepoint_cb.count_.load());
  EXPECT_EQ(OB_SUCCESS, savepoint_cb.result_.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, savepoint_member.state_);

  ObTxDescGuard failure_guard = n1->get_tx_guard();
  ObTxDesc &failure = failure_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(failure, tx_param));
  const ObTransID failure_id = failure.tx_id_;
  failure.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback failure_cb;
  {
    ObSpinLockGuard guard(failure.lock_);
    failure.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, failure.set_commit_cb(&failure_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(failure_id, OB_TRANS_TIMEOUT, SCN()));
  EXPECT_EQ(1, failure_cb.count_.load());
  EXPECT_EQ(OB_TRANS_TIMEOUT, failure_cb.result_.load());
  EXPECT_EQ(ObTxDesc::State::COMMIT_TIMEOUT, failure.state_);
  EXPECT_EQ(OB_TRANS_TIMEOUT, failure.commit_out_);

  ObTxDescGuard concurrent_guard = n1->get_tx_guard();
  ObTxDesc &concurrent = concurrent_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(concurrent, tx_param));
  const ObTransID concurrent_id = concurrent.tx_id_;
  concurrent.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback concurrent_cb;
  {
    ObSpinLockGuard guard(concurrent.lock_);
    concurrent.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, concurrent.set_commit_cb(&concurrent_cb));
  }
  std::thread result1([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    (void)n1->txs_.handle_tx_commit_result(concurrent_id, OB_SUCCESS, SCN::min_scn());
  });
  std::thread result2([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    (void)n1->txs_.handle_tx_commit_result(concurrent_id, OB_SUCCESS, SCN::min_scn());
  });
  result1.join();
  result2.join();
  EXPECT_EQ(1, concurrent_cb.count_.load());
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, concurrent.state_);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, concurrent.commit_out_);

  ObTxDescGuard missing_first_guard = n1->get_tx_guard();
  ObTxDesc &missing_first = missing_first_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(missing_first, tx_param));
  const ObTransID missing_first_id = missing_first.tx_id_;
  CountingCommitCallback missing_first_cb;
  {
    ObSpinLockGuard guard(missing_first.lock_);
    missing_first.state_ = ObTxDesc::State::IN_TERMINATE;
    missing_first.commit_times_ = 1;
    ASSERT_EQ(OB_SUCCESS, missing_first.set_commit_cb(&missing_first_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(
                missing_first_id, OB_TRANS_CTX_NOT_EXIST, SCN()));
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, missing_first.state_);
  EXPECT_EQ(OB_TRANS_KILLED, missing_first.commit_out_);
  EXPECT_EQ(1, missing_first_cb.count_.load());

  ObTxDescGuard missing_retried_guard = n1->get_tx_guard();
  ObTxDesc &missing_retried = missing_retried_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(missing_retried, tx_param));
  const ObTransID missing_retried_id = missing_retried.tx_id_;
  CountingCommitCallback missing_retried_cb;
  {
    ObSpinLockGuard guard(missing_retried.lock_);
    missing_retried.state_ = ObTxDesc::State::IN_TERMINATE;
    missing_retried.commit_times_ = 2;
    ASSERT_EQ(OB_SUCCESS, missing_retried.set_commit_cb(&missing_retried_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(
                missing_retried_id, OB_TRANS_CTX_NOT_EXIST, SCN()));
  EXPECT_EQ(ObTxDesc::State::COMMIT_UNKNOWN, missing_retried.state_);
  EXPECT_EQ(OB_TRANS_UNKNOWN, missing_retried.commit_out_);
  EXPECT_EQ(1, missing_retried_cb.count_.load());

  ObTxDescGuard already_committed_guard = n1->get_tx_guard();
  ObTxDesc &already_committed = already_committed_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(already_committed, tx_param));
  const ObTransID already_committed_id = already_committed.tx_id_;
  already_committed.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback already_committed_cb;
  {
    ObSpinLockGuard guard(already_committed.lock_);
    already_committed.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, already_committed.set_commit_cb(&already_committed_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(
                already_committed_id, OB_TRANS_COMMITED, SCN::min_scn()));
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, already_committed.state_);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, already_committed.commit_out_);
  EXPECT_FALSE(already_committed.commit_version_.is_valid());
  EXPECT_EQ(1, already_committed_cb.count_.load());
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, already_committed_cb.result_.load());

  ObTxDescGuard retry_guard = n1->get_tx_guard();
  ObTxDesc &retry = retry_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(retry, tx_param));
  const ObTransID retry_id = retry.tx_id_;
  retry.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback retry_cb;
  {
    ObSpinLockGuard guard(retry.lock_);
    retry.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, retry.set_commit_cb(&retry_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(retry_id, OB_EAGAIN, SCN()));
  EXPECT_EQ(ObTxDesc::State::IN_TERMINATE, retry.state_);
  EXPECT_EQ(0, retry_cb.count_.load());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(retry_id, OB_TRANS_KILLED, SCN()));
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, retry.state_);
  EXPECT_EQ(OB_TRANS_KILLED, retry.commit_out_);
  EXPECT_EQ(1, retry_cb.count_.load());

  ObTxDescGuard other_error_guard = n1->get_tx_guard();
  ObTxDesc &other_error = other_error_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(other_error, tx_param));
  const ObTransID other_error_id = other_error.tx_id_;
  other_error.set_hotspot_group_commit_info(primary_id, true);
  CountingCommitCallback other_error_cb;
  {
    ObSpinLockGuard guard(other_error.lock_);
    other_error.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, other_error.set_commit_cb(&other_error_cb));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(other_error_id, OB_IO_ERROR, SCN()));
  EXPECT_EQ(ObTxDesc::State::IN_TERMINATE, other_error.state_);
  EXPECT_EQ(0, other_error_cb.count_.load());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(other_error_id, OB_TRANS_KILLED, SCN()));
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, other_error.state_);
  EXPECT_EQ(OB_TRANS_KILLED, other_error.commit_out_);
  EXPECT_EQ(1, other_error_cb.count_.load());

  ObTransID removed_id;
  {
    ObTxDescGuard removed_guard = n1->get_tx_guard();
    ObTxDesc &removed = removed_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(removed, tx_param));
    removed_id = removed.tx_id_;
    CountingCommitCallback removed_cb;
    {
      ObSpinLockGuard guard(removed.lock_);
      removed.state_ = ObTxDesc::State::IN_TERMINATE;
      ASSERT_EQ(OB_SUCCESS, removed.set_commit_cb(&removed_cb));
    }
    ASSERT_EQ(OB_SUCCESS,
              n1->txs_.handle_tx_commit_result(removed_id, OB_TRANS_KILLED, SCN()));
    EXPECT_EQ(1, removed_cb.count_.load());
    ASSERT_EQ(OB_SUCCESS, removed_guard.release());
  }
  const int removed_late_ret =
      n1->txs_.handle_tx_commit_result(removed_id, OB_SUCCESS, SCN::min_scn());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, removed_late_ret);

  ObTxDescGuard reused_guard = n1->get_tx_guard();
  ObTxDesc &reused = reused_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(reused, tx_param));
  const ObTransID reused_old_id = reused.tx_id_;
  reused.set_hotspot_group_commit_info(primary_id, true);
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(reused));
  {
    ObSpinLockGuard guard(reused.lock_);
    ASSERT_TRUE(reused.is_terminated());
    ASSERT_FALSE(reused.flags_.RELEASED_);
    ASSERT_EQ(OB_SUCCESS, n1->txs_.start_epoch_(reused));
  }
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(reused, tx_param));
  const ObTransID reused_id = reused.tx_id_;
  ASSERT_NE(reused_old_id, reused_id);
  ASSERT_FALSE(reused.get_hotspot_primary_tx_id().is_valid());
  ASSERT_FALSE(reused.need_rollback_on_no_affected_rows());
  CountingCommitCallback reused_cb;
  {
    ObSpinLockGuard guard(reused.lock_);
    reused.state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, reused.set_commit_cb(&reused_cb));
  }
  const int reused_old_result_ret =
      n1->txs_.handle_tx_commit_result(reused_old_id, OB_SUCCESS, SCN::min_scn());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, reused_old_result_ret);
  EXPECT_EQ(reused_id, reused.tx_id_);
  EXPECT_EQ(ObTxDesc::State::IN_TERMINATE, reused.state_);
  EXPECT_EQ(0, reused_cb.count_.load());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.handle_tx_commit_result(reused_id, OB_TRANS_KILLED, SCN()));
  EXPECT_EQ(1, reused_cb.count_.load());
  ASSERT_EQ(OB_SUCCESS, reused_guard.release());

  RecordProperty("q08_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("q08_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q08_eligible_tx_id", std::to_string(eligible_id.get_id()));
  RecordProperty("q08_eligible_result", std::to_string(eligible_cb.result_.load()));
  RecordProperty("q08_eligible_state", std::to_string(static_cast<int>(eligible.state_)));
  RecordProperty("q08_eligible_version",
                 std::to_string(eligible.commit_version_.get_val_for_tx(true)));
  RecordProperty("q08_eligible_callback_count", std::to_string(eligible_cb.count_.load()));
  RecordProperty("q08_eligible_submit_mts", std::to_string(eligible_submit_mts));
  RecordProperty("q08_eligible_callback_mts", std::to_string(eligible_cb.callback_mts_.load()));
  RecordProperty("q08_ordinary_tx_id", std::to_string(ordinary_id.get_id()));
  RecordProperty("q08_savepoint_member_result", std::to_string(savepoint_cb.result_.load()));
  RecordProperty("q08_savepoint_member_qualified",
                 std::to_string(savepoint_member.need_rollback_on_no_affected_rows()));
  RecordProperty("q08_failure_tx_id", std::to_string(failure_id.get_id()));
  RecordProperty("q08_failure_result", std::to_string(failure_cb.result_.load()));
  RecordProperty("q08_failure_callback_count", std::to_string(failure_cb.count_.load()));
  RecordProperty("q08_concurrent_callback_count", std::to_string(concurrent_cb.count_.load()));
  RecordProperty("q08_missing_first_result", std::to_string(missing_first.commit_out_));
  RecordProperty("q08_missing_retried_result", std::to_string(missing_retried.commit_out_));
  RecordProperty("q08_already_committed_result",
                 std::to_string(already_committed.commit_out_));
  RecordProperty("q08_retry_final_result", std::to_string(retry.commit_out_));
  RecordProperty("q08_other_error_final_result", std::to_string(other_error.commit_out_));
  RecordProperty("q08_removed_late_result", std::to_string(removed_late_ret));
  RecordProperty("q08_descriptor_storage_reused", "1");
  RecordProperty("q08_reused_old_result", std::to_string(reused_old_result_ret));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q08 result observations",
            "ls_id", n1->ls_id_, K(primary_id), K(eligible_id), K(ordinary_id), K(failure_id),
            "eligible_result", eligible_cb.result_.load(),
            "eligible_state", static_cast<int>(eligible.state_),
            "eligible_version", eligible.commit_version_,
            "eligible_callback_count", eligible_cb.count_.load(),
            K(eligible_submit_mts), "eligible_callback_mts", eligible_cb.callback_mts_.load(),
            "ordinary_result", ordinary_cb.result_.load(),
            "failure_result", failure_cb.result_.load(),
            "failure_callback_count", failure_cb.count_.load(),
            "concurrent_callback_count", concurrent_cb.count_.load(),
            "missing_first_result", missing_first.commit_out_,
            "missing_retried_result", missing_retried.commit_out_,
            "already_committed_result", already_committed.commit_out_,
            "retry_final_result", retry.commit_out_,
            "other_error_final_result", other_error.commit_out_);

  ASSERT_EQ(OB_SUCCESS, eligible_guard.release());
  ASSERT_EQ(OB_SUCCESS, ordinary_guard.release());
  ASSERT_EQ(OB_SUCCESS, savepoint_guard.release());
  ASSERT_EQ(OB_SUCCESS, failure_guard.release());
  ASSERT_EQ(OB_SUCCESS, concurrent_guard.release());
  ASSERT_EQ(OB_SUCCESS, missing_first_guard.release());
  ASSERT_EQ(OB_SUCCESS, missing_retried_guard.release());
  ASSERT_EQ(OB_SUCCESS, already_committed_guard.release());
  ASSERT_EQ(OB_SUCCESS, retry_guard.release());
  ASSERT_EQ(OB_SUCCESS, other_error_guard.release());
}

// A real committed P can later become unqueryable. The fake read control below
// simulates an absent P table result; it does not exercise on-disk compaction.
TEST_F(ObTestHotspotTxLeaderSwitch, missing_primary_result_preserves_commit_unknown)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9821, 9921));
  const ObTransID primary_id = primary.tx_id_;
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary, n1->ts_after_ms(5000)));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();
  ASSERT_EQ(ObTxDesc::State::COMMITTED, primary.state_);
  ObPartTransCtx *primary_ctx = nullptr;
  NAMED_DEFER(revert_unexpected_primary_ctx,
              if (nullptr != primary_ctx) { n1->txs_.revert_tx_ctx_(primary_ctx); });
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST,
            n1->txs_.get_tx_ctx_(n1->ls_id_, primary_id, primary_ctx));
  ASSERT_EQ(nullptr, primary_ctx);
  int64_t primary_state = ObTxData::RUNNING;
  SCN primary_version;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.get_tx_state_from_tx_table_(
      n1->ls_id_, primary_id, primary_state, primary_version));
  ASSERT_EQ(ObTxData::COMMIT, primary_state);

  ObFakeTxDataTable &table = n1->fake_tx_table_.tx_data_table_;
  const ObTransID saved_error_id = table.fake_read_error_tx_id_;
  const int saved_error = table.fake_read_error_;
  NAMED_DEFER(restore_primary_missing_control,
              table.fake_read_error_tx_id_ = saved_error_id;
              table.fake_read_error_ = saved_error);
  table.fake_read_error_tx_id_ = primary_id;
  table.fake_read_error_ = OB_TRANS_CTX_NOT_EXIST;
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->txs_.get_tx_state_from_tx_table_(
      n1->ls_id_, primary_id, primary_state, primary_version));
  ObTxCommitParts empty_parts;
  for (int64_t qualified = 0; qualified < 2; ++qualified) {
    CountingCommitCallback local_cb;
    ObTxDescGuard local_guard = n1->get_tx_guard();
    ObTxDesc &local = local_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(local, tx_param));
    ASSERT_EQ(OB_SUCCESS, local.parts_.assign(primary.parts_));
    ASSERT_FALSE(local.is_all_parts_without_valid_write());
    local.set_hotspot_group_commit_info(primary_id, qualified != 0);
    SCN commit_version;
    EXPECT_EQ(OB_TRANS_UNKNOWN, n1->txs_.local_ls_commit_tx_(
        local.tx_id_, n1->ls_id_, empty_parts, n1->ts_after_ms(5000), ObString(),
        1, SCN::max_scn(), ObTxPart::EPOCH_UNKNOWN, commit_version,
        n1->addr_, ObString(), primary_id));
    EXPECT_EQ(OB_TRANS_UNKNOWN,
              n1->submit_commit_tx(local, n1->ts_after_ms(5000), local_cb, nullptr));
    EXPECT_EQ(ObTxDesc::State::COMMIT_UNKNOWN, local.state_);
    EXPECT_EQ(OB_TRANS_UNKNOWN, local.commit_out_);
    EXPECT_EQ(0, local_cb.count_.load());
    EXPECT_FALSE(local.commit_version_.is_valid());

    CountingCommitCallback remote_cb;
    ObTxDescGuard remote_guard = n1->get_tx_guard();
    ObTxDesc &remote = remote_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(remote, tx_param));
    ASSERT_NE(local.tx_id_, remote.tx_id_);
    ASSERT_EQ(OB_SUCCESS, remote.parts_.assign(primary.parts_));
    ASSERT_FALSE(remote.is_all_parts_without_valid_write());
    remote.set_hotspot_group_commit_info(primary_id, qualified != 0);
    {
      ObSpinLockGuard guard(remote.lock_);
      remote.state_ = ObTxDesc::State::IN_TERMINATE;
      remote.commit_times_ = 1;
      ASSERT_EQ(OB_SUCCESS, remote.set_commit_cb(&remote_cb));
    }
    ObTxCommitMsg request;
    request.tx_id_ = remote.tx_id_;
    request.receiver_ = n1->ls_id_;
    request.sender_addr_ = n1->addr_;
    request.expire_ts_ = n1->ts_after_ms(5000);
    request.commit_start_scn_.set_max();
    request.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    request.hotspot_primary_tx_id_ = primary_id;
    ObTransRpcResult result;
    EXPECT_EQ(OB_TRANS_UNKNOWN, n1->txs_.handle_trans_commit_request(request, result));
    EXPECT_EQ(OB_TRANS_UNKNOWN, result.get_status());
    ObTxCommitRespMsg response;
    response.tx_id_ = remote.tx_id_;
    response.ret_ = result.get_status();
    response.commit_version_ = result.private_data_;
    EXPECT_EQ(OB_SUCCESS, n1->txs_.handle_trans_commit_response(response, result));
    EXPECT_EQ(ObTxDesc::State::COMMIT_UNKNOWN, remote.state_);
    EXPECT_EQ(OB_TRANS_UNKNOWN, remote.commit_out_);
    EXPECT_EQ(1, remote_cb.count_.load());
    EXPECT_EQ(OB_TRANS_UNKNOWN, remote_cb.result_.load());
    EXPECT_FALSE(remote.commit_version_.is_valid());
    EXPECT_EQ(OB_SUCCESS, n1->txs_.handle_trans_commit_response(response, result));
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    EXPECT_EQ(0, local_cb.count_.load());
    EXPECT_EQ(1, remote_cb.count_.load());
  }
  RecordProperty("primary_missing_simulation", "fake P-only table read returns CTX_NOT_EXIST");
  RecordProperty("committed_primary_tx_id", std::to_string(primary_id.get_id()));
}

// Q06/Q07: when S is genuinely absent, commit retry follows the stored P across
// active/committed/aborted states. An existing S context always wins over P.
TEST_F(ObTestHotspotTxLeaderSwitch, q06_q07_missing_secondary_primary_fallback_matrix)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxCommitParts empty_parts;
  const ObTransID missing_secondary_id(INT64_MAX - 8001);
  SCN commit_version;

  const ObTransID txdata_running_primary_id(INT64_MAX - 8010);
  ObTxDataGuard running_data_guard;
  ASSERT_EQ(OB_SUCCESS,
            n1->fake_tx_table_.tx_data_table_.alloc_tx_data(
                running_data_guard, false, INT64_MAX));
  ObTxData *running_data = const_cast<ObTxData *>(running_data_guard.tx_data());
  ASSERT_NE(nullptr, running_data);
  running_data->tx_id_ = txdata_running_primary_id;
  running_data->state_ = ObTxData::RUNNING;
  ObTxData *running_data_to_insert = running_data;
  ASSERT_EQ(OB_SUCCESS,
            n1->fake_tx_table_.tx_data_table_.insert(running_data_to_insert));
  const int txdata_running_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8011), n1->ls_id_, empty_parts, n1->ts_after_ms(500),
      ObString(), 10, SCN::max_scn(), ObTxPart::EPOCH_UNKNOWN, commit_version,
      n1->addr_, ObString(), txdata_running_primary_id);
  EXPECT_EQ(OB_EAGAIN, txdata_running_ret);

  ObTxTableGuard cached_guard;
  ASSERT_EQ(OB_SUCCESS, n1->fake_ls_.tx_table_.get_tx_table_guard(cached_guard));
  cached_guard.get_mini_cache().set(*running_data);
  int64_t cached_state = ObTxData::ABORT;
  SCN cached_version;
  ObReadTxDataArg cached_arg(
      txdata_running_primary_id, cached_guard.get_epoch(), cached_guard.get_mini_cache());
  GetTxStateWithSCNFunctor cached_fn(SCN::max_scn(), cached_state, cached_version);
  cached_fn.set_may_exist_undecided_state_in_tx_data_table();
  EXPECT_EQ(OB_SUCCESS, cached_guard.check_with_tx_data(cached_arg, cached_fn));
  EXPECT_EQ(ObTxData::RUNNING, cached_state);

  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9801, 9901));
  const ObTransID primary_id = primary.tx_id_;
  const int64_t active_query_mts = ObTimeUtility::current_monotonic_time();
  {
    ObFakeTxDataTable &fake_tx_data_table = n1->fake_tx_table_.tx_data_table_;
    const ObTransID saved_read_error_tx_id = fake_tx_data_table.fake_read_error_tx_id_;
    const int saved_read_error = fake_tx_data_table.fake_read_error_;
    NAMED_DEFER(restore_active_primary_read_control,
                fake_tx_data_table.fake_read_error_tx_id_ = saved_read_error_tx_id;
                fake_tx_data_table.fake_read_error_ = saved_read_error);
    fake_tx_data_table.fake_read_error_tx_id_ = primary_id;
    fake_tx_data_table.fake_read_error_ = OB_IO_ERROR;
    EXPECT_EQ(OB_EAGAIN,
              n1->txs_.local_ls_commit_tx_(missing_secondary_id,
                                           n1->ls_id_,
                                           empty_parts,
                                           n1->ts_after_ms(500),
                                           ObString(),
                                           1,
                                           SCN::max_scn(),
                                           ObTxPart::EPOCH_UNKNOWN,
                                           commit_version,
                                           n1->addr_,
                                           ObString(),
                                           primary_id));
  }

  ObTxCommitMsg remote_msg;
  remote_msg.tx_id_ = missing_secondary_id;
  remote_msg.receiver_ = n1->ls_id_;
  remote_msg.sender_addr_ = n1->addr_;
  remote_msg.expire_ts_ = n1->ts_after_ms(500);
  remote_msg.commit_start_scn_.set_max();
  remote_msg.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  remote_msg.hotspot_primary_tx_id_ = primary_id;
  ObTransRpcResult remote_result;
  EXPECT_EQ(OB_EAGAIN, n1->txs_.handle_trans_commit_request(remote_msg, remote_result));

  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary, n1->ts_after_ms(500)));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();
  const int committed_fallback_ret = n1->txs_.local_ls_commit_tx_(
      missing_secondary_id,
      n1->ls_id_,
      empty_parts,
      n1->ts_after_ms(500),
      ObString(),
      2,
      SCN::max_scn(),
      ObTxPart::EPOCH_UNKNOWN,
      commit_version,
      n1->addr_,
      ObString(),
      primary_id);
  EXPECT_EQ(OB_TRANS_COMMITED, committed_fallback_ret);

  // Exercise the public local-submit contract when S has no ctx. Copy only the
  // participant metadata to model the scheduler retained after a handoff; the
  // P result above comes from a real commit. SQL qualification is covered by
  // q08_sql_partial_rollback_publishes_qualification separately.
  for (int64_t qualified = 0; qualified < 2; ++qualified) {
    ObTxDescGuard missing_guard = n1->get_tx_guard();
    ObTxDesc &missing = missing_guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(missing, tx_param));
    ASSERT_EQ(OB_SUCCESS, missing.parts_.assign(primary.parts_));
    ASSERT_FALSE(missing.is_all_parts_without_valid_write());
    missing.set_hotspot_group_commit_info(primary_id, qualified != 0);
    CountingCommitCallback missing_cb;
    EXPECT_EQ(OB_SUCCESS,
              n1->submit_commit_tx(missing, n1->ts_after_ms(500), missing_cb, nullptr));
    EXPECT_EQ(1, missing_cb.count_.load());
    EXPECT_EQ(qualified ? OB_ROLLBACK_ON_NO_AFFECTED_ROWS : OB_SUCCESS,
              missing_cb.result_.load());
    EXPECT_EQ(qualified ? ObTxDesc::State::ROLLED_BACK : ObTxDesc::State::COMMITTED,
              missing.state_);
    EXPECT_EQ(!qualified, missing.commit_version_.is_valid());
  }

  ObTxDescGuard aborted_primary_guard = n1->get_tx_guard();
  ObTxDesc &aborted_primary = aborted_primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(aborted_primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(aborted_primary, 9802, 9902));
  const ObTransID aborted_primary_id = aborted_primary.tx_id_;
  // Persist redo so rollback produces a queryable ABORT record. A transaction
  // aborted only in memory has neither P ctx nor P table and is now UNKNOWN.
  {
    ObPartTransCtx *aborted_ctx = nullptr;
    NAMED_DEFER(revert_aborted_primary_ctx,
                if (nullptr != aborted_ctx) { n1->txs_.revert_tx_ctx_(aborted_ctx); });
    ASSERT_EQ(OB_SUCCESS, n1->txs_.get_tx_ctx_(n1->ls_id_, aborted_primary_id, aborted_ctx));
    ASSERT_NE(nullptr, aborted_ctx);
    ASSERT_EQ(OB_SUCCESS, aborted_ctx->submit_redo_log_for_freeze(UINT32_MAX));
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    ASSERT_TRUE(aborted_ctx->has_persisted_log_());
  }
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(aborted_primary));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();
  int64_t aborted_table_state = ObTxData::RUNNING;
  SCN aborted_table_version;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.get_tx_state_from_tx_table_(
      n1->ls_id_, aborted_primary_id, aborted_table_state, aborted_table_version));
  ASSERT_EQ(ObTxData::ABORT, aborted_table_state);
  const int aborted_fallback_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8002),
      n1->ls_id_,
      empty_parts,
      n1->ts_after_ms(500),
      ObString(),
      3,
      SCN::max_scn(),
      ObTxPart::EPOCH_UNKNOWN,
      commit_version,
      n1->addr_,
      ObString(),
      aborted_primary_id);
  EXPECT_EQ(OB_TRANS_KILLED, aborted_fallback_ret);

  ObTxDescGuard existing_secondary_guard = n1->get_tx_guard();
  ObTxDesc &existing_secondary = existing_secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(existing_secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(existing_secondary, 9803, 9903));
  const ObTransID existing_secondary_id = existing_secondary.tx_id_;
  existing_secondary.set_hotspot_group_commit_info(aborted_primary_id, false);
  const int secondary_priority_ret = n1->commit_tx(existing_secondary, n1->ts_after_ms(500));
  EXPECT_TRUE(OB_SUCCESS == secondary_priority_ret || OB_TRANS_COMMITED == secondary_priority_ret);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, existing_secondary.state_);

  const int both_missing_serving_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8003),
      n1->ls_id_,
      empty_parts,
      n1->ts_after_ms(500),
      ObString(),
      4,
      SCN::max_scn(),
      ObTxPart::EPOCH_UNKNOWN,
      commit_version,
      n1->addr_,
      ObString(),
      ObTransID(INT64_MAX - 8004));
  EXPECT_EQ(OB_TRANS_UNKNOWN, both_missing_serving_ret);

  ObFakeTxDataTable &fake_tx_data_table = n1->fake_tx_table_.tx_data_table_;
  fake_tx_data_table.fake_recycled_scn_enabled_ = true;
  ASSERT_EQ(OB_SUCCESS, fake_tx_data_table.fake_recycled_scn_.convert_for_tx(100));
  NAMED_DEFER(reset_fake_txdata_controls,
              fake_tx_data_table.fake_recycled_scn_enabled_ = false;
              fake_tx_data_table.fake_read_error_tx_id_ = ObTransID();
              fake_tx_data_table.fake_read_error_ = OB_SUCCESS);
  // A valid P id bypasses S tx-data: poison S lookup while committed P wins.
  fake_tx_data_table.fake_read_error_tx_id_ = missing_secondary_id;
  fake_tx_data_table.fake_read_error_ = OB_IO_ERROR;
  const int skipped_secondary_read_ret = n1->txs_.local_ls_commit_tx_(
      missing_secondary_id, n1->ls_id_, empty_parts, n1->ts_after_ms(500),
      ObString(), 40, SCN::max_scn(), ObTxPart::EPOCH_UNKNOWN, commit_version,
      n1->addr_, ObString(), primary_id);
  EXPECT_EQ(OB_TRANS_COMMITED, skipped_secondary_read_ret);
  fake_tx_data_table.fake_read_error_tx_id_ = ObTransID();
  fake_tx_data_table.fake_read_error_ = OB_SUCCESS;

  SCN after_recycle_scn;
  SCN at_recycle_scn;
  ASSERT_EQ(OB_SUCCESS, after_recycle_scn.convert_for_tx(101));
  ASSERT_EQ(OB_SUCCESS, at_recycle_scn.convert_for_tx(100));
  const int missing_after_recycle_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8013), n1->ls_id_, empty_parts, n1->ts_after_ms(500),
      ObString(), 41, after_recycle_scn, ObTxPart::EPOCH_UNKNOWN, commit_version,
      n1->addr_, ObString(), ObTransID(INT64_MAX - 8014));
  const int missing_at_recycle_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8015), n1->ls_id_, empty_parts, n1->ts_after_ms(500),
      ObString(), 42, at_recycle_scn, ObTxPart::EPOCH_UNKNOWN, commit_version,
      n1->addr_, ObString(), ObTransID(INT64_MAX - 8016));
  EXPECT_EQ(OB_TRANS_UNKNOWN, missing_after_recycle_ret);
  EXPECT_EQ(OB_TRANS_UNKNOWN, missing_at_recycle_ret);

  // Ordinary missing transactions retain the existing recycle boundary rule.
  for (int64_t same_primary = 0; same_primary < 2; ++same_primary) {
    const ObTransID ordinary_id(INT64_MAX - 8020 - same_primary);
    const ObTransID ordinary_primary = same_primary ? ordinary_id : ObTransID();
    EXPECT_EQ(OB_TRANS_KILLED, n1->txs_.local_ls_commit_tx_(
        ordinary_id, n1->ls_id_, empty_parts, n1->ts_after_ms(500), ObString(),
        45, after_recycle_scn, ObTxPart::EPOCH_UNKNOWN, commit_version,
        n1->addr_, ObString(), ordinary_primary));
    EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, n1->txs_.local_ls_commit_tx_(
        ordinary_id, n1->ls_id_, empty_parts, n1->ts_after_ms(500), ObString(),
        46, at_recycle_scn, ObTxPart::EPOCH_UNKNOWN, commit_version,
        n1->addr_, ObString(), ordinary_primary));
  }

  fake_tx_data_table.fake_read_error_tx_id_ = txdata_running_primary_id;
  fake_tx_data_table.fake_read_error_ = OB_IO_ERROR;
  const int primary_read_error_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8017), n1->ls_id_, empty_parts, n1->ts_after_ms(500),
      ObString(), 43, SCN::max_scn(), ObTxPart::EPOCH_UNKNOWN, commit_version,
      n1->addr_, ObString(), txdata_running_primary_id);
  EXPECT_EQ(OB_IO_ERROR, primary_read_error_ret);
  remote_msg.tx_id_ = ObTransID(INT64_MAX - 8018);
  remote_msg.hotspot_primary_tx_id_ = txdata_running_primary_id;
  remote_msg.request_id_ = 44;
  const int remote_primary_read_error_ret =
      n1->txs_.handle_trans_commit_request(remote_msg, remote_result);
  EXPECT_EQ(OB_IO_ERROR, remote_primary_read_error_ret);
  fake_tx_data_table.fake_read_error_tx_id_ = ObTransID();
  fake_tx_data_table.fake_read_error_ = OB_SUCCESS;

  ObLSTxCtxMgr *ls_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_mgr->switch_to_follower_gracefully());
  const int missing_role_ret = n1->txs_.local_ls_commit_tx_(
      ObTransID(INT64_MAX - 8005),
      n1->ls_id_,
      empty_parts,
      n1->ts_after_ms(500),
      ObString(),
      5,
      SCN::max_scn(),
      ObTxPart::EPOCH_UNKNOWN,
      commit_version,
      n1->addr_,
      ObString(),
      ObTransID(INT64_MAX - 8006));
  EXPECT_EQ(OB_NOT_MASTER, missing_role_ret);
  remote_msg.tx_id_ = ObTransID(INT64_MAX - 8009);
  remote_msg.hotspot_primary_tx_id_ = ObTransID(INT64_MAX - 8012);
  remote_msg.request_id_ = 51;
  EXPECT_EQ(OB_NOT_MASTER,
            n1->txs_.handle_trans_commit_request(remote_msg, remote_result));
  ASSERT_EQ(OB_SUCCESS, ls_mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_mgr));

  RecordProperty("q06_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("q06_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q06_missing_secondary_tx_id", std::to_string(missing_secondary_id.get_id()));
  RecordProperty("q06_active_query_mts", std::to_string(active_query_mts));
  RecordProperty("q06_active_result", std::to_string(OB_EAGAIN));
  RecordProperty("q06_txdata_running_result", std::to_string(txdata_running_ret));
  RecordProperty("q06_cached_running_state", std::to_string(cached_state));
  RecordProperty("q06_committed_result", std::to_string(committed_fallback_ret));
  RecordProperty("q06_aborted_result", std::to_string(aborted_fallback_ret));
  RecordProperty("q07_existing_secondary_tx_id", std::to_string(existing_secondary_id.get_id()));
  RecordProperty("q07_existing_secondary_result", std::to_string(secondary_priority_ret));
  RecordProperty("q07_both_missing_serving_result", std::to_string(both_missing_serving_ret));
  RecordProperty("q07_skipped_secondary_read_result",
                 std::to_string(skipped_secondary_read_ret));
  RecordProperty("q07_missing_after_recycle_result", std::to_string(missing_after_recycle_ret));
  RecordProperty("q07_missing_at_recycle_result", std::to_string(missing_at_recycle_ret));
  RecordProperty("q07_primary_read_error_result", std::to_string(primary_read_error_ret));
  RecordProperty("q07_remote_primary_read_error_result",
                 std::to_string(remote_primary_read_error_ret));
  RecordProperty("q07_missing_role_result", std::to_string(missing_role_ret));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q06/Q07 fallback observations",
            "ls_id", n1->ls_id_, K(primary_id), K(missing_secondary_id), K(active_query_mts),
            K(committed_fallback_ret), K(aborted_primary_id), K(aborted_fallback_ret),
            K(txdata_running_ret), K(cached_state), K(existing_secondary_id), K(secondary_priority_ret),
            K(both_missing_serving_ret), K(skipped_secondary_read_ret), K(missing_after_recycle_ret),
            K(missing_at_recycle_ret), K(primary_read_error_ret),
            K(remote_primary_read_error_ret), K(missing_role_ret), K(commit_version));

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, aborted_primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, existing_secondary_guard.release());
}

// T012: a statement timeout does not own aggregation completion. Once the blocked
// hotspot redo can progress, the existing async submit/callback chain publishes
// success and releases admission without another validation call.
TEST_F(ObTestHotspotTxLeaderSwitch, q03_async_completion_after_validation_timeout)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9811, 9911));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9812, 9912));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
  NAMED_DEFER(unblock_hotspot_redo, n1->fake_tx_log_adapter_->clear_block_log_type());
  const int64_t validation_start_mts = ObTimeUtility::current_monotonic_time();
  const int validation_ret = n1->txs_.sync_hotspot_legality_validation(
      participant, primary_id, members, ObTimeUtility::fast_current_time() + 20 * 1000);
  const int64_t validation_return_mts = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS, validation_ret);

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary_id, false, primary_ctx));
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());

  n1->fake_tx_log_adapter_->clear_block_log_type();
  bool asynchronously_finished = false;
  int64_t async_finish_mts = 0;
  for (int64_t i = 0; i < 3000 && !asynchronously_finished; ++i) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    {
      CtxLockGuard guard(primary_ctx->lock_);
      asynchronously_finished =
          TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED == primary_ctx->redo_flush_status_
          && 0 == mgr->get_hotspot_aggregation_count();
    }
    if (!asynchronously_finished) {
      usleep(1000);
    } else {
      async_finish_mts = ObTimeUtility::current_monotonic_time();
    }
  }
  EXPECT_TRUE(asynchronously_finished);
  TxRedoFlushStatus terminal_status = TxRedoFlushStatus::NORMAL_START;
  {
    CtxLockGuard guard(primary_ctx->lock_);
    terminal_status = primary_ctx->redo_flush_status_;
  }
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, terminal_status);
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());

  RecordProperty("t012_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("t012_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("t012_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("t012_validation_result", std::to_string(validation_ret));
  RecordProperty("t012_validation_start_mts", std::to_string(validation_start_mts));
  RecordProperty("t012_validation_return_mts", std::to_string(validation_return_mts));
  RecordProperty("t012_async_finish_mts", std::to_string(async_finish_mts));
  RecordProperty("t012_terminal_state",
                 std::to_string(static_cast<int>(terminal_status)));
  RecordProperty("t012_final_admission_count",
                 std::to_string(mgr->get_hotspot_aggregation_count()));
  TRANS_LOG(INFO, "[hotspot leader switch test] T012 async completion observations",
            "ls_id", n1->ls_id_, K(primary_id), K(secondary_id), K(validation_ret),
            K(validation_start_mts), K(validation_return_mts), K(async_finish_mts),
            "terminal_state", to_cstr(terminal_status),
            "admission_count", mgr->get_hotspot_aggregation_count());

  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(primary_ctx));
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  ASSERT_FALSE(mgr->is_master());
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
  ASSERT_EQ(OB_SUCCESS, n1->commit_tx(primary, n1->ts_after_ms(500)));
  const int secondary_commit_ret = n1->commit_tx(secondary, n1->ts_after_ms(500));
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
}

// R01 controlled-timing IT: model already queued self-messages with the real
// pending counters, then run the real failed scheduler-response cleanup while
// retaining a P ctx reference. Pending messages carry only the tx id; after
// cache cleanup their handlers must not recreate aggregation resources.
TEST_F(ObTestHotspotTxLeaderSwitch,
       failure_cleanup_releases_responsibility_before_queued_self_messages_drain)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  const ObAddr saved_self_addr = GCONF.self_addr_;
  GCONF.self_addr_ = n1->addr_;
  NAMED_DEFER(restore_r01_self_addr, GCONF.self_addr_ = saved_self_addr);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9813, 9913));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9814, 9914));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary.tx_id_));
  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
  NAMED_DEFER(unblock_hotspot_redo, n1->fake_tx_log_adapter_->clear_block_log_type());
  n1->add_drop_msg_type(TX_HOTSPOT_DISPATCH_REDO);
  n1->add_drop_msg_type(TX_HOTSPOT_SUBMIT_OTHER_REDO);
  NAMED_DEFER(restore_r01_msg_delivery,
              n1->del_drop_msg_type(TX_HOTSPOT_DISPATCH_REDO);
              n1->del_drop_msg_type(TX_HOTSPOT_SUBMIT_OTHER_REDO));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, primary.tx_id_, members,
                ObTimeUtility::fast_current_time() + 20 * 1000));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  NAMED_DEFER(revert_r01_mgr,
              if (nullptr != mgr) {
                (void)n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr);
              });
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary.tx_id_, false, primary_ctx));
  NAMED_DEFER(revert_r01_primary_ctx,
              if (nullptr != primary_ctx) {
                (void)mgr->revert_tx_ctx(primary_ctx);
              });
  ASSERT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
  ASSERT_EQ(1, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  ASSERT_EQ(1, mgr->get_hotspot_aggregation_count());

  // Keep any real pending count left by the deliberately dropped setup
  // messages. If one message kind was not naturally queued, model exactly one
  // so both late service handlers are covered by the controlled ordering.
  if (0 == primary_ctx->hotspot_redo_cache_.get_pending_dispatch_msg_cnt()) {
    primary_ctx->hotspot_redo_cache_.inc_pending_dispatch_msg_cnt();
  }
  if (0 == primary_ctx->hotspot_redo_cache_.get_pending_submit_other_redo_msg_cnt()) {
    primary_ctx->hotspot_redo_cache_.inc_pending_submit_other_redo_msg_cnt();
  }
  const int64_t queued_dispatch_count =
      primary_ctx->hotspot_redo_cache_.get_pending_dispatch_msg_cnt();
  const int64_t queued_submit_count =
      primary_ctx->hotspot_redo_cache_.get_pending_submit_other_redo_msg_cnt();
  ASSERT_GT(queued_dispatch_count, 0);
  ASSERT_GT(queued_submit_count, 0);
  {
    CtxLockGuard guard(primary_ctx->lock_);
    primary_ctx->sub_state_.set_force_abort();
    bool aggregation_finished = true;
    EXPECT_EQ(OB_SUCCESS,
              primary_ctx->try_finish_hotspot_aggregation_(aggregation_finished));
    EXPECT_FALSE(aggregation_finished);
    EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING,
              primary_ctx->redo_flush_status_);
    EXPECT_TRUE(primary_ctx->can_not_submit_log_for_hotspot_());
    const ObTxLogType terminal_log_types[] = {
        ObTxLogType::TX_ABORT_LOG, ObTxLogType::TX_CLEAR_LOG};
    for (const auto log_type : terminal_log_types) {
      ObTxLogBlock log_block;
      ASSERT_EQ(OB_SUCCESS, log_block.cb_arg_array_.push_back(ObTxCbArg(log_type, nullptr)));
      ObTxLogCb cb;
      ObTxLogCb *log_cb = &cb;
      EXPECT_EQ(OB_NEED_RETRY,
                primary_ctx->submit_log_block_out_(log_block, log_cb, SCN::min_scn(),
                    logservice::ObReplayBarrierType::NO_NEED_BARRIER, 0));
    }
  }
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());

  int pending_abort_ret = OB_SUCCESS;
  {
    ObTxHotspotRedoCacheHandle::ActiveRefGuard active_ref_guard(
        primary_ctx->hotspot_redo_cache_);
    {
      CtxLockGuard guard(primary_ctx->lock_);
      pending_abort_ret = primary_ctx->compensate_abort_log_();
    }
    EXPECT_EQ(OB_EAGAIN, pending_abort_ret);
    EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
    EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
    n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_ABORT_LOG);
  }
  int blocked_abort_ret = OB_SUCCESS;
  {
    CtxLockGuard guard(primary_ctx->lock_);
    blocked_abort_ret = primary_ctx->compensate_abort_log_();
  }
  EXPECT_EQ(OB_EAGAIN, blocked_abort_ret);
  const int64_t cache_count_after_cleanup =
      primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count();
  const ObTxHotspotRedoCacheHandle::HotspotLogCbView cb_view_after_cleanup =
      primary_ctx->hotspot_redo_cache_.get_cb_list_snapshot();
  EXPECT_EQ(0, cache_count_after_cleanup);
  EXPECT_EQ(0, cb_view_after_cleanup.free_cb_count);
  EXPECT_EQ(0, cb_view_after_cleanup.busy_cb_count);
  EXPECT_EQ(0, cb_view_after_cleanup.idle_cb_count);
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_FAILED,
            primary_ctx->redo_flush_status_);
  {
    CtxLockGuard guard(primary_ctx->lock_);
    EXPECT_FALSE(primary_ctx->can_not_submit_log_for_hotspot_());
    EXPECT_FALSE(primary_ctx->is_force_abort_logging_());
  }

  n1->fake_tx_log_adapter_->clear_block_log_type();
  int abort_submit_ret = OB_EAGAIN;
  {
    CtxLockGuard guard(primary_ctx->lock_);
    abort_submit_ret = primary_ctx->compensate_abort_log_();
  }
  EXPECT_EQ(OB_SUCCESS, abort_submit_ret);
  n1->wait_all_redolog_applied();
  bool primary_exiting = false;
  const int64_t abort_apply_deadline = ObTimeUtility::current_time() + 5_s;
  do {
    {
      CtxLockGuard guard(primary_ctx->lock_);
      primary_exiting = primary_ctx->is_exiting_;
    }
    if (!primary_exiting) {
      ob_usleep(1_ms);
    }
  } while (!primary_exiting && ObTimeUtility::current_time() < abort_apply_deadline);
  EXPECT_TRUE(primary_exiting);
  const uint64_t count_before_late_messages = mgr->get_hotspot_aggregation_count();

  ObHotspotDispatchRedoMsg dispatch_msg;
  dispatch_msg.cluster_version_ = DATA_CURRENT_VERSION;
  dispatch_msg.tenant_id_ = 1001;
  dispatch_msg.tx_id_ = primary.tx_id_;
  dispatch_msg.receiver_ = n1->ls_id_;
  dispatch_msg.sender_addr_ = n1->addr_;
  dispatch_msg.sender_ = n1->ls_id_;
  dispatch_msg.cluster_id_ = 1;
  ObTransRpcResult dispatch_result;
  int late_dispatch_ret = OB_SUCCESS;
  for (int64_t i = 0; i < queued_dispatch_count; ++i) {
    late_dispatch_ret =
        n1->txs_.handle_hotspot_dispatch_redo_msg(dispatch_msg, dispatch_result);
  }

  ObHotspotSubmitOtherRedoMsg submit_msg;
  submit_msg.cluster_version_ = DATA_CURRENT_VERSION;
  submit_msg.tenant_id_ = 1001;
  submit_msg.tx_id_ = primary.tx_id_;
  submit_msg.receiver_ = n1->ls_id_;
  submit_msg.sender_addr_ = n1->addr_;
  submit_msg.sender_ = n1->ls_id_;
  submit_msg.cluster_id_ = 1;
  ObTransRpcResult submit_result;
  int late_submit_ret = OB_SUCCESS;
  for (int64_t i = 0; i < queued_submit_count; ++i) {
    late_submit_ret =
        n1->txs_.handle_hotspot_submit_other_redo_msg(submit_msg, submit_result);
  }
  EXPECT_EQ(0, count_before_late_messages);
  EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_pending_dispatch_msg_cnt());
  EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_pending_submit_other_redo_msg_cnt());
  EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  RecordProperty("r01_blocked_abort_result", std::to_string(blocked_abort_ret));
  RecordProperty("r01_abort_submit_result", std::to_string(abort_submit_ret));
  RecordProperty("r01_primary_exiting", std::to_string(primary_exiting));
  RecordProperty("r01_pending_abort_result", std::to_string(pending_abort_ret));
  RecordProperty("r01_late_dispatch_result", std::to_string(late_dispatch_ret));
  RecordProperty("r01_late_submit_result", std::to_string(late_submit_ret));
  RecordProperty("r01_count_before_late_messages",
                 std::to_string(count_before_late_messages));
  RecordProperty("r01_queued_dispatch_count", std::to_string(queued_dispatch_count));
  RecordProperty("r01_queued_submit_count", std::to_string(queued_submit_count));
  n1->fake_tx_log_adapter_->clear_block_log_type();
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary, OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
}

// Q06: after a normal handoff, the new leader has only P recovery state. The
// first S retry observes P running; after the original scheduler commits P, the
// same S identity resolves through P and its data remains committed.
TEST_F(ObTestHotspotTxLeaderSwitch, q06_normal_handoff_secondary_retry_uses_primary)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9821, 9921));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9822, 9922));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  // The graceful handoff appends record/active-info logs after the initial
  // redo drain. Replay that new tail before the target starts serving.
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  ObPartTransCtx *secondary_ctx = nullptr;
  EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST,
            mgr2->get_tx_ctx_directly_from_hash_map(secondary_id, secondary_ctx));
  ObTxCommitMsg retry_msg;
  retry_msg.tx_id_ = secondary_id;
  retry_msg.receiver_ = n2->ls_id_;
  retry_msg.sender_addr_ = n1->addr_;
  retry_msg.expire_ts_ = n1->ts_after_ms(500);
  retry_msg.commit_start_scn_.set_max();
  retry_msg.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  retry_msg.hotspot_primary_tx_id_ = primary_id;
  ObTransRpcResult retry_result;
  const int64_t retry_mts = ObTimeUtility::current_monotonic_time();
  const int running_retry_ret = n2->txs_.handle_trans_commit_request(retry_msg, retry_result);
  EXPECT_EQ(OB_EAGAIN, running_retry_ret);

  const int primary_commit_ret = n1->commit_tx(primary, n1->ts_after_ms(1000));
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();
  const int secondary_commit_ret = n1->commit_tx(secondary, n1->ts_after_ms(1000));
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);

  ObTxDescGuard read_guard = n2->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  int64_t primary_value = 0;
  int64_t secondary_value = 0;
  ASSERT_EQ(OB_SUCCESS, n2->read(read_tx, 9821, primary_value));
  ASSERT_EQ(OB_SUCCESS, n2->read(read_tx, 9822, secondary_value));
  EXPECT_EQ(9921, primary_value);
  EXPECT_EQ(9922, secondary_value);

  RecordProperty("q06_handoff_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("q06_handoff_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q06_handoff_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q06_handoff_retry_mts", std::to_string(retry_mts));
  RecordProperty("q06_handoff_running_result", std::to_string(running_retry_ret));
  RecordProperty("q06_handoff_primary_result", std::to_string(primary_commit_ret));
  RecordProperty("q06_handoff_secondary_result", std::to_string(secondary_commit_ret));
  RecordProperty("q06_handoff_primary_value", std::to_string(primary_value));
  RecordProperty("q06_handoff_secondary_value", std::to_string(secondary_value));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q06 normal handoff observations",
            "ls_id", n1->ls_id_, K(primary_id), K(secondary_id), K(retry_mts),
            K(running_retry_ret), K(primary_commit_ret), K(secondary_commit_ret),
            K(primary_value), K(secondary_value));

  ASSERT_EQ(OB_SUCCESS, read_guard.release());
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

// Q06 recovery gate: takeover is not serving until the real START_WORKING log
// callback runs. The original scheduler must keep retrying during that window
// and complete P/S only after recovery becomes ready.
TEST_F(ObTestHotspotTxLeaderSwitch, q06_takeover_recovery_gate_preserves_original_commit)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9861, 9961));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9862, 9962));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);

  n1->fake_tx_log_adapter_->set_pause();
  NAMED_DEFER(adapter_pause_cleanup,
              n1->fake_tx_log_adapter_->clear_pause();
              n1->fake_tx_log_adapter_->wakeup());
  const int64_t takeover_start_mts = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  EXPECT_FALSE(mgr2->in_leader_serving_state());

  std::atomic<int> primary_commit_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> primary_done(false);
  std::thread primary_committer([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObCurTraceId::init(n1->addr_);
    primary_commit_ret.store(n1->commit_tx(primary, n1->ts_after_ms(20000)));
    primary_done.store(true);
  });
  bool primary_retry_registered = false;
  const int64_t retry_registration_deadline = ObTimeUtility::current_time() + 5_s;
  while (!primary_retry_registered
         && ObTimeUtility::current_time() < retry_registration_deadline) {
    {
      ObSpinLockGuard guard(primary.lock_);
      primary_retry_registered = primary.commit_task_.is_registered();
    }
    if (!primary_retry_registered) {
      ob_usleep(1_ms);
    }
  }
  const bool commit_waited_for_recovery = !primary_done.load();
  EXPECT_FALSE(mgr2->in_leader_serving_state());
  EXPECT_TRUE(primary_retry_registered);
  EXPECT_TRUE(commit_waited_for_recovery);

  const int64_t recovery_release_mts = ObTimeUtility::current_monotonic_time();
  n1->fake_tx_log_adapter_->clear_pause();
  n1->fake_tx_log_adapter_->wakeup();
  const int64_t serving_deadline = ObTimeUtility::current_time() + 10_s;
  while (!mgr2->in_leader_serving_state()
         && ObTimeUtility::current_time() < serving_deadline) {
    ob_usleep(1_ms);
  }
  const int64_t serving_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_TRUE(mgr2->in_leader_serving_state());
  primary_committer.join();
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret.load()
              || OB_TRANS_COMMITED == primary_commit_ret.load());

  const int secondary_commit_ret = n1->commit_tx(secondary, n1->ts_after_ms(5000));
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();
  ObTxDescGuard read_guard = n2->get_tx_guard();
  ObTxDesc &read_tx = read_guard.get_tx_desc();
  int64_t primary_value = 0;
  int64_t secondary_value = 0;
  ASSERT_EQ(OB_SUCCESS, n2->read(read_tx, 9861, primary_value));
  ASSERT_EQ(OB_SUCCESS, n2->read(read_tx, 9862, secondary_value));
  EXPECT_EQ(9961, primary_value);
  EXPECT_EQ(9962, secondary_value);

  RecordProperty("q06_gate_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q06_gate_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q06_gate_takeover_start_mts", std::to_string(takeover_start_mts));
  RecordProperty("q06_gate_recovery_release_mts", std::to_string(recovery_release_mts));
  RecordProperty("q06_gate_serving_mts", std::to_string(serving_mts));
  RecordProperty("q06_gate_commit_waited", std::to_string(commit_waited_for_recovery));
  RecordProperty("q06_gate_retry_registered", std::to_string(primary_retry_registered));
  RecordProperty("q06_gate_primary_result", std::to_string(primary_commit_ret.load()));
  RecordProperty("q06_gate_secondary_result", std::to_string(secondary_commit_ret));
  RecordProperty("q06_gate_primary_value", std::to_string(primary_value));
  RecordProperty("q06_gate_secondary_value", std::to_string(secondary_value));

  ASSERT_EQ(OB_SUCCESS, read_guard.release());
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

// Q09: the original S scheduler is waiting to retry on the new leader while
// the old replica still owns the aggregation membership. Replaying the
// confirmed P COMMIT on the old replica must deliver S's result before that
// retry is allowed to run; no S participant action is fabricated in the test.
TEST_F(ObTestHotspotTxLeaderSwitch, q09_replay_commit_supplements_secondary_result)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9841, 9941));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9842, 9942));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(retry_debug_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  static const char *RETRY_EVENT = "tx_retry_paused";
  static const char *CONTINUE_EVENT = "tx_retry_continue";
  static const char *RESP_EVENT = "tx_resp_paused";
  static const char *RESP_CONTINUE_EVENT = "tx_resp_continue";
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = BEFORE_TX_COMMIT_RETRY_HANDLE;
  pause_action.timeout_ = 20_s;
  pause_action.execute_ = 1;
  pause_action.signal_ = RETRY_EVENT;
  pause_action.wait_ = CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> secondary_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> secondary_done(false);
  std::thread secondary_committer([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObCurTraceId::init(n1->addr_);
    secondary_ret.store(n1->commit_tx(secondary, n1->ts_after_ms(20000)));
    secondary_done.store(true);
  });
  NAMED_DEFER(secondary_committer_cleanup,
              if (secondary_committer.joinable()) {
                ObDebugSyncAction cleanup_resume;
                cleanup_resume.sync_point_ = NOW;
                cleanup_resume.timeout_ = 20_s;
                cleanup_resume.execute_ = 1;
                cleanup_resume.signal_ = RESP_CONTINUE_EVENT;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                cleanup_resume.signal_ = CONTINUE_EVENT;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                secondary_committer.join();
              });
  ObDebugSyncAction wait_retry;
  wait_retry.sync_point_ = NOW;
  wait_retry.timeout_ = 20_s;
  wait_retry.execute_ = 1;
  wait_retry.wait_ = RETRY_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_retry));
  const int64_t retry_paused_mts = ObTimeUtility::current_monotonic_time();

  const int primary_ret = n1->commit_tx(primary, n1->ts_after_ms(20000));
  EXPECT_TRUE(OB_SUCCESS == primary_ret || OB_TRANS_COMMITED == primary_ret);
  ObPartTransCtx *old_secondary_ctx = nullptr;
  const int old_secondary_ctx_ret =
      mgr1->get_tx_ctx_directly_from_hash_map(secondary_id, old_secondary_ctx);
  EXPECT_EQ(OB_SUCCESS, old_secondary_ctx_ret);
  int64_t old_secondary_action = ObPartTransAction::INIT;
  if (OB_SUCCESS == old_secondary_ctx_ret && nullptr != old_secondary_ctx) {
    {
      CtxLockGuard guard(old_secondary_ctx->lock_);
      old_secondary_action = old_secondary_ctx->part_trans_action_;
    }
    EXPECT_EQ(OB_SUCCESS, mgr1->revert_tx_ctx(old_secondary_ctx));
  }
  EXPECT_NE(static_cast<int64_t>(ObPartTransAction::COMMIT), old_secondary_action);
  EXPECT_NE(static_cast<int64_t>(ObPartTransAction::ABORT), old_secondary_action);

  ObDebugSyncAction pause_response;
  pause_response.sync_point_ = BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE;
  pause_response.timeout_ = 20_s;
  pause_response.execute_ = 1;
  pause_response.signal_ = RESP_EVENT;
  pause_response.wait_ = RESP_CONTINUE_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_response));

  ObPartTransCtx *old_primary_ctx = nullptr;
  const int old_primary_ctx_ret =
      mgr1->get_tx_ctx_directly_from_hash_map(primary_id, old_primary_ctx);
  EXPECT_EQ(OB_SUCCESS, old_primary_ctx_ret);
  if (OB_SUCCESS == old_primary_ctx_ret && nullptr != old_primary_ctx) {
    {
      SpinWLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      // Model the enabled-but-not-queued residue of a failed enqueue. The real
      // COMMIT response below must refresh this old role hint before delivery.
      ASSERT_FALSE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
      ASSERT_EQ(OB_SUCCESS,
                old_primary_ctx->hotspot_redo_cache_.resp_task_.enable(
                    OB_NOT_MASTER, SCN()));
    }
    old_primary_ctx->hotspot_redo_cache_.push_response_task(
        OB_SUCCESS, primary.commit_version_);
  }
  ObDebugSyncAction wait_response;
  wait_response.sync_point_ = NOW;
  wait_response.timeout_ = 20_s;
  wait_response.execute_ = 1;
  wait_response.wait_ = RESP_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_response));
  if (nullptr != old_primary_ctx) {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_EQ(old_primary_ctx,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
  }

  ReplayLogEntryFunctor replay_old_leader(n1);
  EXPECT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  if (nullptr != old_primary_ctx) {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_EQ(old_primary_ctx,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
  }
  const int64_t supplement_deadline = ObTimeUtility::current_time() + 10_s;
  while (!secondary_done.load() && ObTimeUtility::current_time() < supplement_deadline) {
    n1->wait_all_msg_consumed();
    ob_usleep(1_ms);
  }
  const int64_t supplement_received_mts = ObTimeUtility::current_monotonic_time();
  const bool received_before_retry = secondary_done.load();
  EXPECT_TRUE(received_before_retry);
  const int old_replica_reelect_ret = mgr1->switch_to_leader();
  EXPECT_EQ(OB_SUCCESS, old_replica_reelect_ret);
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(secondary_done.load());
  const int64_t secondary_delivery_pause_prepare_start_mts =
      ObTimeUtility::current_monotonic_time();
  const int secondary_delivery_pause_switch_ret =
      mgr1->switch_to_follower_gracefully();
  const int64_t secondary_delivery_pause_prepare_done_mts =
      ObTimeUtility::current_monotonic_time();
  EXPECT_EQ(OB_SUCCESS, secondary_delivery_pause_switch_ret);
  EXPECT_TRUE(secondary_done.load());
  EXPECT_EQ(OB_SUCCESS, mgr1->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr1->in_leader_serving_state());
  ObDebugSyncAction resume_response;
  resume_response.sync_point_ = NOW;
  resume_response.timeout_ = 20_s;
  resume_response.execute_ = 1;
  resume_response.signal_ = RESP_CONTINUE_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_response));
  ObDebugSyncAction resume_retry;
  resume_retry.sync_point_ = NOW;
  resume_retry.timeout_ = 20_s;
  resume_retry.execute_ = 1;
  resume_retry.signal_ = CONTINUE_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_retry));
  const int64_t retry_released_mts = ObTimeUtility::current_monotonic_time();
  secondary_committer.join();

  EXPECT_TRUE(received_before_retry);
  EXPECT_TRUE(OB_SUCCESS == secondary_ret.load() || OB_TRANS_COMMITED == secondary_ret.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, primary.state_);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);
  bool response_task_detached = false;
  bool response_task_in_queue = true;
  if (OB_SUCCESS == old_primary_ctx_ret && nullptr != old_primary_ctx) {
    // RPC delivery may finish before the response worker drops its ctx ref.
    const int64_t finish_deadline = ObTimeUtility::current_time() + 10_s;
    do {
      {
        SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
        response_task_detached =
            nullptr == old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_;
        response_task_in_queue = old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue();
      }
      if (!response_task_detached || response_task_in_queue) {
        ob_usleep(1_ms);
      }
    } while ((!response_task_detached || response_task_in_queue)
             && ObTimeUtility::current_time() < finish_deadline);
  }
  EXPECT_TRUE(response_task_detached);
  EXPECT_FALSE(response_task_in_queue);
  if (nullptr != old_primary_ctx) {
    EXPECT_EQ(OB_SUCCESS, mgr1->revert_tx_ctx(old_primary_ctx));
  }
  RecordProperty("q09_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q09_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q09_retry_paused_mts", std::to_string(retry_paused_mts));
  RecordProperty("q09_supplement_received_mts", std::to_string(supplement_received_mts));
  RecordProperty("q09_retry_released_mts", std::to_string(retry_released_mts));
  RecordProperty("q09_old_secondary_action", std::to_string(old_secondary_action));
  RecordProperty("q09_old_replica_reelect_result", std::to_string(old_replica_reelect_ret));
  RecordProperty("q09_secondary_delivery_pause_prepare_start_mts",
                 std::to_string(secondary_delivery_pause_prepare_start_mts));
  RecordProperty("q09_secondary_delivery_pause_prepare_done_mts",
                 std::to_string(secondary_delivery_pause_prepare_done_mts));
  RecordProperty("q09_secondary_delivery_pause_switch_result",
                 std::to_string(secondary_delivery_pause_switch_ret));
  RecordProperty("q09_received_before_retry", std::to_string(received_before_retry));
  RecordProperty("q09_secondary_result", std::to_string(secondary_ret.load()));

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

// SC005 control: without replaying the new leader's COMMIT back to the old
// replica, no supplement can run. The original scheduler still completes the
// same S through its existing retry after the controlled delay is released.
TEST_F(ObTestHotspotTxLeaderSwitch, q09_retry_completes_without_replay_supplement)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9851, 9951));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9852, 9952));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  // The replicas share a tenant id but own different MTL ctx pools. Direct
  // manager calls must run under the tenant instance that owns the manager.
  ObTenantEnv::set_tenant(&n1->tenant_);
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ObTenantEnv::set_tenant(&n2->tenant_);
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(retry_debug_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  static const char *RETRY_EVENT = "tx_no_supplement_retry_paused";
  static const char *CONTINUE_EVENT = "tx_no_supplement_retry_continue";
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = BEFORE_TX_COMMIT_RETRY_HANDLE;
  pause_action.timeout_ = 20_s;
  pause_action.execute_ = 1;
  pause_action.signal_ = RETRY_EVENT;
  pause_action.wait_ = CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> secondary_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> secondary_done(false);
  std::atomic<int64_t> secondary_done_mts(0);
  std::thread secondary_committer([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObCurTraceId::init(n1->addr_);
    secondary_ret.store(n1->commit_tx(secondary, n1->ts_after_ms(20000)));
    secondary_done_mts.store(ObTimeUtility::current_monotonic_time());
    secondary_done.store(true);
  });
  ObDebugSyncAction wait_retry;
  wait_retry.sync_point_ = NOW;
  wait_retry.timeout_ = 20_s;
  wait_retry.execute_ = 1;
  wait_retry.wait_ = RETRY_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_retry));
  const int64_t retry_paused_mts = ObTimeUtility::current_monotonic_time();

  const int primary_ret = n1->commit_tx(primary, n1->ts_after_ms(20000));
  EXPECT_TRUE(OB_SUCCESS == primary_ret || OB_TRANS_COMMITED == primary_ret);
  EXPECT_FALSE(secondary_done.load());
  ObDebugSyncAction resume_retry;
  resume_retry.sync_point_ = NOW;
  resume_retry.timeout_ = 20_s;
  resume_retry.execute_ = 1;
  resume_retry.signal_ = CONTINUE_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_retry));
  const int64_t retry_released_mts = ObTimeUtility::current_monotonic_time();
  secondary_committer.join();

  EXPECT_TRUE(OB_SUCCESS == secondary_ret.load() || OB_TRANS_COMMITED == secondary_ret.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);
  EXPECT_GT(secondary_done_mts.load(), retry_released_mts);
  const ObTxDesc::State secondary_state_before_late_replay = secondary.state_;
  const SCN secondary_version_before_late_replay = secondary.commit_version_;
  const int secondary_result_before_late_replay = secondary.commit_out_;
  ReplayLogEntryFunctor replay_old_leader(n1);
  const int64_t late_replay_mts = ObTimeUtility::current_monotonic_time();
  const int late_replay_ret = n1->fake_tx_log_adapter_->replay_all(replay_old_leader);
  n1->wait_all_msg_consumed();
  ob_usleep(10_ms);
  EXPECT_EQ(OB_SUCCESS, late_replay_ret);
  EXPECT_EQ(secondary_state_before_late_replay, secondary.state_);
  EXPECT_EQ(secondary_version_before_late_replay, secondary.commit_version_);
  EXPECT_EQ(secondary_result_before_late_replay, secondary.commit_out_);
  RecordProperty("q09_control_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q09_control_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q09_control_retry_paused_mts", std::to_string(retry_paused_mts));
  RecordProperty("q09_control_retry_released_mts", std::to_string(retry_released_mts));
  RecordProperty("q09_control_result_received_mts", std::to_string(secondary_done_mts.load()));
  RecordProperty("q09_control_secondary_result", std::to_string(secondary_ret.load()));
  RecordProperty("q09_control_late_replay_mts", std::to_string(late_replay_mts));
  RecordProperty("q09_control_late_replay_result", std::to_string(late_replay_ret));
  RecordProperty("q09_control_late_result_unchanged",
                 std::to_string(secondary_result_before_late_replay == secondary.commit_out_));

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

// A replay supplement may discover the old scheduler only after that RPC route
// has disappeared. The direct post fails once, then an async replay response
// worker delivers the original S result before its scheduler retry is released.
void ObTestHotspotTxLeaderSwitch::run_replay_post_failure(
    const bool fail_enqueue, const int upgrade_phase, const int role_conflict,
    const bool direct_success)
{
  CountingCommitCallback secondary_callback;
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9853, 9953));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9854, 9954));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  static const char *RETRY_EVENT = "tx_post_fail_retry_paused";
  static const char *RETRY_CONTINUE = "tx_post_fail_retry_continue";
  static const char *RESP_EVENT = "tx_post_fail_resp_paused";
  static const char *RESP_CONTINUE = "tx_post_fail_resp_continue";
  static const char *NEXT_EVENT = "tx_replay_upgrade_next_paused";
  static const char *NEXT_CONTINUE = "tx_replay_upgrade_next_continue";
  NAMED_DEFER(debug_cleanup,
              ObDebugSyncAction cleanup_resume;
              cleanup_resume.sync_point_ = NOW;
              cleanup_resume.timeout_ = 20_s;
              cleanup_resume.execute_ = 1;
              cleanup_resume.signal_ = RESP_CONTINUE;
              (void)GDS.set_global_action(false, false, cleanup_resume);
              cleanup_resume.signal_ = NEXT_CONTINUE;
              (void)GDS.set_global_action(false, false, cleanup_resume);
              cleanup_resume.signal_ = RETRY_CONTINUE;
              (void)GDS.set_global_action(false, false, cleanup_resume);
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_retry;
  pause_retry.sync_point_ = BEFORE_TX_COMMIT_RETRY_HANDLE;
  pause_retry.timeout_ = 20_s;
  pause_retry.execute_ = 1;
  pause_retry.signal_ = RETRY_EVENT;
  pause_retry.wait_ = RETRY_CONTINUE;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_retry));
  ObDebugSyncAction pause_response;
  pause_response.sync_point_ = 2 == upgrade_phase
      ? AFTER_SECONDARY_TX_RESPONSE_TASK_TYPE_READ : BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE;
  pause_response.timeout_ = 20_s;
  pause_response.execute_ = 1;
  pause_response.signal_ = RESP_EVENT;
  pause_response.wait_ = RESP_CONTINUE;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_response));

  std::atomic<int> secondary_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> secondary_done(false);
  std::atomic<int64_t> secondary_done_mts(0);
  std::thread secondary_committer([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObCurTraceId::init(n1->addr_);
    const int submit_ret = n1->submit_commit_tx(
        secondary, n1->ts_after_ms(20000), secondary_callback, nullptr);
    const int64_t callback_deadline = ObTimeUtility::current_time() + 20_s;
    while (OB_SUCCESS == submit_ret && 0 == secondary_callback.count_.load()
           && ObTimeUtility::current_time() < callback_deadline) {
      ob_usleep(1_ms);
    }
    secondary_ret.store(OB_SUCCESS != submit_ret ? submit_ret : secondary_callback.result_.load());
    secondary_done_mts.store(ObTimeUtility::current_monotonic_time());
    secondary_done.store(true);
  });
  NAMED_DEFER(post_fail_secondary_thread_cleanup,
              if (secondary_committer.joinable()) {
                ObDebugSyncAction cleanup_resume;
                cleanup_resume.sync_point_ = NOW;
                cleanup_resume.timeout_ = 20_s;
                cleanup_resume.execute_ = 1;
                cleanup_resume.signal_ = RESP_CONTINUE;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                cleanup_resume.signal_ = NEXT_CONTINUE;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                cleanup_resume.signal_ = RETRY_CONTINUE;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                secondary_committer.join();
              });
  ObDebugSyncAction wait_retry;
  wait_retry.sync_point_ = NOW;
  wait_retry.timeout_ = 20_s;
  wait_retry.execute_ = 1;
  wait_retry.wait_ = RETRY_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_retry));
  const int64_t retry_paused_mts = ObTimeUtility::current_monotonic_time();

  const int primary_ret = n1->commit_tx(primary, n1->ts_after_ms(20000));
  EXPECT_TRUE(OB_SUCCESS == primary_ret || OB_TRANS_COMMITED == primary_ret);

  bool route_registered = true;
  NAMED_DEFER(route_cleanup,
              if (!route_registered) {
                (void)bus_.regist(n1->addr_, *n1);
              });
  EXPECT_EQ(OB_SUCCESS, bus_.unregist(n1->addr_));
  route_registered = false;
  ObPartTransCtx *old_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr1->get_tx_ctx_directly_from_hash_map(primary_id, old_primary_ctx));
  oceanbase::common::EventItem reset_enqueue_fail;
  reset_enqueue_fail.error_code_ = 0;
  reset_enqueue_fail.occur_ = 0;
  reset_enqueue_fail.trigger_freq_ = 0;
  reset_enqueue_fail.cond_ = 0;
  NAMED_DEFER(reset_replay_enqueue_errsim,
              (void)oceanbase::common::EventTable::set_event(
                  "ERRSIM_HOTSPOT_RESPONSE_TASK_ENQUEUE_FAIL", reset_enqueue_fail));
  if (fail_enqueue) {
    oceanbase::common::EventItem enqueue_fail;
    enqueue_fail.error_code_ = OB_ALLOCATE_MEMORY_FAILED;
    enqueue_fail.occur_ = 1;
    enqueue_fail.trigger_freq_ = 1;
    enqueue_fail.cond_ = 0;
    ASSERT_EQ(OB_SUCCESS,
              oceanbase::common::EventTable::set_event(
                  "ERRSIM_HOTSPOT_RESPONSE_TASK_ENQUEUE_FAIL", enqueue_fail));
  }
  int queued_result = OB_SUCCESS;
  SCN queued_version = primary.commit_version_;
  if (upgrade_phase > 0) {
    if (1 == role_conflict) {
      // State-bearing real response entry, deliberately invoked on the old
      // follower. This is defensive coverage, not a naturally scheduled timeout.
      CtxLockGuard guard(old_primary_ctx->lock_);
      queued_result = OB_NOT_MASTER;
      queued_version = old_primary_ctx->ctx_tx_data_.get_commit_version();
      const bool timeout_caller_preconditions = !old_primary_ctx->is_sub2pc()
          && !old_primary_ctx->is_follower_()
          && ObPartTransAction::COMMIT == old_primary_ctx->part_trans_action_
          && (old_primary_ctx->is_local_tx_() || old_primary_ctx->is_root());
      EXPECT_FALSE(timeout_caller_preconditions);
      RecordProperty("role_hint_timeout_caller_preconditions", std::to_string(timeout_caller_preconditions));
      RecordProperty("role_hint_part_action", std::to_string(old_primary_ctx->part_trans_action_));
      const int hint_ret = old_primary_ctx->post_tx_commit_resp_(OB_NOT_MASTER);
      RecordProperty("role_hint_response_entry_result", std::to_string(hint_ret));
    } else {
      // Real apply-origin task while the old S action cannot answer COMMIT.
      ASSERT_EQ(OB_SUCCESS, old_primary_ctx->hotspot_redo_cache_.push_response_task(
          OB_SUCCESS, primary.commit_version_));
    }
    ObDebugSyncAction wait_first;
    wait_first.sync_point_ = NOW;
    wait_first.timeout_ = 20_s;
    wait_first.execute_ = 1;
    wait_first.wait_ = RESP_EVENT;
    ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_first));
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_EQ(2 == upgrade_phase ? 1 : 0,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.get_retry_cnt());
    EXPECT_EQ(static_cast<int64_t>(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER),
              old_primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
    EXPECT_EQ(0, secondary_callback.count_.load());
  }
  if (2 == role_conflict) {
    {
      CtxLockGuard guard(old_primary_ctx->lock_);
      EXPECT_TRUE(old_primary_ctx->is_follower_());
      const int hint_ret = old_primary_ctx->post_tx_commit_resp_(OB_NOT_MASTER);
      RecordProperty("late_role_hint_response_entry_result", std::to_string(hint_ret));
    }
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_EQ(queued_result, old_primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_);
    EXPECT_EQ(queued_version, old_primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_);
    EXPECT_EQ(static_cast<int64_t>(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER),
              old_primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
  }
  if (direct_success) {
    EXPECT_EQ(OB_SUCCESS, bus_.regist(n1->addr_, *n1));
    route_registered = true;
  }
  ReplayLogEntryFunctor replay_old_leader(n1);
  const int replay_ret = n1->fake_tx_log_adapter_->replay_all(replay_old_leader);
  EXPECT_EQ(OB_SUCCESS, replay_ret);
  const int64_t failed_post_observed_mts = ObTimeUtility::current_monotonic_time();
  if (direct_success) {
    const int64_t direct_callback_deadline = ObTimeUtility::current_time() + 10_s;
    while (0 == secondary_callback.count_.load()
           && ObTimeUtility::current_time() < direct_callback_deadline) {
      n1->wait_all_msg_consumed();
      ob_usleep(1_ms);
    }
    EXPECT_EQ(1, secondary_callback.count_.load());
  } else {
    EXPECT_FALSE(secondary_done.load());
    EXPECT_EQ(0, secondary_callback.count_.load());
  }
  const auto expected_origin = 1 == role_conflict || direct_success
      ? ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER
      : ObTransRetryTaskType::SECONDARY_TX_REPLAY_RESP_SCHEDULER;
  if (fail_enqueue) {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_FALSE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_FALSE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_enabled());
    EXPECT_EQ(nullptr, old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
  } else {
    ObDebugSyncAction wait_response;
    wait_response.sync_point_ = NOW;
    wait_response.timeout_ = 20_s;
    wait_response.execute_ = 1;
    wait_response.wait_ = RESP_EVENT;
    if (0 == upgrade_phase) {
      EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_response));
    }
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_enabled());
    EXPECT_EQ(expected_origin, old_primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
    EXPECT_EQ(old_primary_ctx,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
    EXPECT_EQ(queued_result, old_primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_);
    EXPECT_EQ(queued_version,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_);
  }
  const int64_t response_ref_before_finish = old_primary_ctx->get_ref();
  old_primary_ctx->hotspot_redo_cache_.finish_response_task(true);
  EXPECT_EQ(response_ref_before_finish, old_primary_ctx->get_ref());
  if (!fail_enqueue) {
    // Repeated replay either fails on the missing route or finds completed
    // cache cleanup; the queued worker keeps its payload and reference.
    old_primary_ctx->supplement_hotspot_response_after_replay_commit();
    EXPECT_EQ(response_ref_before_finish, old_primary_ctx->get_ref());
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_EQ(old_primary_ctx, old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
    EXPECT_EQ(queued_result, old_primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_);
    EXPECT_EQ(queued_version, old_primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_);
  }

  if (!route_registered) {
    EXPECT_EQ(OB_SUCCESS, bus_.regist(n1->addr_, *n1));
    route_registered = true;
  }
  bool replay_worker_completed = false;
  bool response_task_detached = fail_enqueue;
  if (!fail_enqueue) {
    if (upgrade_phase > 0) {
      ObDebugSyncAction pause_next;
      pause_next.sync_point_ = 2 == upgrade_phase
          ? BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE : AFTER_SECONDARY_TX_RESPONSE_TASK_TYPE_READ;
      pause_next.timeout_ = 20_s;
      pause_next.execute_ = 1;
      pause_next.signal_ = NEXT_EVENT;
      pause_next.wait_ = NEXT_CONTINUE;
      EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_next));
    }
    ObDebugSyncAction resume_response;
    resume_response.sync_point_ = NOW;
    resume_response.timeout_ = 20_s;
    resume_response.execute_ = 1;
    resume_response.signal_ = RESP_CONTINUE;
    EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_response));
    if (upgrade_phase > 0) {
      ObDebugSyncAction wait_next;
      wait_next.sync_point_ = NOW;
      wait_next.timeout_ = 20_s;
      wait_next.execute_ = 1;
      wait_next.wait_ = NEXT_EVENT;
      EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_next));
      {
        SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
        EXPECT_EQ(1, old_primary_ctx->hotspot_redo_cache_.resp_task_.get_retry_cnt());
        EXPECT_TRUE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
        EXPECT_EQ(expected_origin, old_primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
        EXPECT_EQ(queued_result, old_primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_);
        EXPECT_EQ(queued_version, old_primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_);
      }
      EXPECT_EQ(direct_success ? 1 : 0, secondary_callback.count_.load());
      EXPECT_EQ(response_ref_before_finish, old_primary_ctx->get_ref());
      RecordProperty("replay_upgrade_phase", std::to_string(upgrade_phase));
      RecordProperty("replay_upgrade_first_attempt_count", "1");
      // Phase 2 reaches BEFORE only after the cached apply attempt returned
      // EAGAIN and ObTransService pushed the same upgraded task again.
      ObDebugSyncAction resume_next;
      resume_next.sync_point_ = NOW;
      resume_next.timeout_ = 20_s;
      resume_next.execute_ = 1;
      resume_next.signal_ = NEXT_CONTINUE;
      EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_next));
    }
    const int64_t worker_deadline = ObTimeUtility::current_time() + 10_s;
    while (1 != role_conflict && !secondary_done.load()
           && ObTimeUtility::current_time() < worker_deadline) {
      n1->wait_all_msg_consumed();
      ob_usleep(1_ms);
    }
    replay_worker_completed = secondary_done.load();
    EXPECT_EQ(1 != role_conflict, replay_worker_completed);
    const int64_t detach_deadline = ObTimeUtility::current_time() + 10_s;
    do {
      {
        SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
        response_task_detached =
            nullptr == old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_
            && !old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue()
            && response_ref_before_finish - 1 == old_primary_ctx->get_ref();
      }
      if (!response_task_detached) {
        ob_usleep(1_ms);
      }
    } while (!response_task_detached && ObTimeUtility::current_time() < detach_deadline);
  } else {
    EXPECT_EQ(0, old_primary_ctx->hotspot_redo_cache_.get_idle_cb_count());
  }
  EXPECT_TRUE(response_task_detached);
  {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_FALSE(old_primary_ctx->hotspot_redo_cache_.resp_task_.is_enabled());
    EXPECT_FALSE(old_primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_.is_valid());
    EXPECT_EQ(0, old_primary_ctx->hotspot_redo_cache_.resp_task_.get_retry_cnt());
  }
  EXPECT_EQ(response_ref_before_finish - (fail_enqueue ? 0 : 1), old_primary_ctx->get_ref());
  const int64_t detached_ref = old_primary_ctx->get_ref();
  old_primary_ctx->hotspot_redo_cache_.finish_response_task(true);
  old_primary_ctx->hotspot_redo_cache_.finish_response_task();
  if (!fail_enqueue && 1 != role_conflict) {
    old_primary_ctx->supplement_hotspot_response_after_replay_commit();
  }
  EXPECT_EQ(detached_ref, old_primary_ctx->get_ref());
  ASSERT_EQ(OB_SUCCESS, mgr1->revert_tx_ctx(old_primary_ctx));

  ObDebugSyncAction resume_retry;
  resume_retry.sync_point_ = NOW;
  resume_retry.timeout_ = 20_s;
  resume_retry.execute_ = 1;
  resume_retry.signal_ = RETRY_CONTINUE;
  const int64_t retry_released_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_retry));
  if (fail_enqueue || 1 == role_conflict) {
    const int64_t retry_deadline = ObTimeUtility::current_time() + 10_s;
    while (!secondary_done.load() && ObTimeUtility::current_time() < retry_deadline) {
      n1->wait_all_msg_consumed();
      ob_usleep(1_ms);
    }
  }
  const bool result_still_complete_after_retry_release = secondary_done.load();
  EXPECT_TRUE(result_still_complete_after_retry_release);
  EXPECT_TRUE(OB_SUCCESS == secondary_ret.load() || OB_TRANS_COMMITED == secondary_ret.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);

  secondary_committer.join();
  n1->wait_all_msg_consumed();
  EXPECT_EQ(1, secondary_callback.count_.load());
  EXPECT_EQ(secondary_ret.load(), secondary_callback.result_.load());

  RecordProperty("q09_post_fail_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q09_post_fail_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q09_post_fail_retry_paused_mts", std::to_string(retry_paused_mts));
  RecordProperty("q09_post_fail_observed_mts", std::to_string(failed_post_observed_mts));
  RecordProperty("q09_post_fail_retry_released_mts", std::to_string(retry_released_mts));
  RecordProperty("q09_post_fail_secondary_done_mts", std::to_string(secondary_done_mts.load()));
  RecordProperty("q09_post_fail_secondary_result", std::to_string(secondary_ret.load()));
  RecordProperty("q09_post_fail_worker_completed_before_retry",
                 std::to_string(replay_worker_completed));
  RecordProperty("q09_post_fail_enqueue_failed", std::to_string(fail_enqueue));
  RecordProperty("queued_role_conflict", std::to_string(role_conflict));
  RecordProperty("queued_payload_result", std::to_string(queued_result));
  RecordProperty("queued_payload_version", std::to_string(queued_version.get_val_for_tx()));
  RecordProperty("replayed_commit_version", std::to_string(primary.commit_version_.get_val_for_tx()));
  RecordProperty("queued_direct_success", std::to_string(direct_success));
  RecordProperty("secondary_callback_count", std::to_string(secondary_callback.count_.load()));

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

TEST_F(ObTestHotspotTxLeaderSwitch, q09_replay_post_failure_handoffs_to_response_worker)
{
  run_replay_post_failure(false /*fail_enqueue*/);
}

TEST_F(ObTestHotspotTxLeaderSwitch, q09_replay_enqueue_failure_falls_back_to_scheduler_retry)
{
  run_replay_post_failure(true /*fail_enqueue*/);
}

TEST_F(ObTestHotspotTxLeaderSwitch, replay_upgrade_before_worker_reads_origin)
{
  run_replay_post_failure(false, 1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, replay_upgrade_after_worker_reads_origin_requeues)
{
  run_replay_post_failure(false, 2);
}

// Controlled calls use real ctx state and real response/worker entry points.
// The old follower does not satisfy the timeout caller's leader prerequisite.
TEST_F(ObTestHotspotTxLeaderSwitch, replay_queued_role_hint_conflict_uses_scheduler_retry)
{
  run_replay_post_failure(false, 1, 1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, replay_queued_success_ignores_later_role_hint)
{
  run_replay_post_failure(false, 1, 2);
}

TEST_F(ObTestHotspotTxLeaderSwitch, replay_direct_success_keeps_queued_task_until_worker_finish)
{
  run_replay_post_failure(false, 1, 0, true);
}

// A replay supplement that arrives before S starts waiting is deliberately
// rejected by the scheduler state check. The original S commit subsequently
// obtains the same confirmed result through the normal retry/query path.
TEST_F(ObTestHotspotTxLeaderSwitch, q11_early_supplement_does_not_replace_retry)
{
  CountingCommitCallback secondary_callback;
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9861, 9961));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9862, 9962));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObLSTxCtxMgr *mgr1 = nullptr;
  ObLSTxCtxMgr *mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr1));
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, mgr2));
  ASSERT_EQ(OB_SUCCESS, mgr1->switch_to_follower_gracefully());
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();

  const int primary_ret = n1->commit_tx(primary, n1->ts_after_ms(20000));
  EXPECT_TRUE(OB_SUCCESS == primary_ret || OB_TRANS_COMMITED == primary_ret);
  EXPECT_LT(secondary.state_, ObTxDesc::State::IN_TERMINATE);
  ObPartTransCtx *old_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr1->get_tx_ctx_directly_from_hash_map(primary_id, old_primary_ctx));
  EXPECT_EQ(OB_INIT_TWICE,
            old_primary_ctx->hotspot_redo_cache_.resp_task_.init(old_primary_ctx));
  EXPECT_TRUE(primary.commit_version_.is_valid());
  const ObTxDesc::State state_before_unconfirmed_supplement = secondary.state_;
  const int result_before_unconfirmed_supplement = secondary.commit_out_;
  ObPartTransCtx *old_secondary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr1->get_tx_ctx_directly_from_hash_map(secondary_id, old_secondary_ctx));
  ASSERT_NE(nullptr, old_secondary_ctx);
  TxRedoFlushStatus secondary_status_before_unconfirmed = TxRedoFlushStatus::NORMAL_START;
  {
    CtxLockGuard guard(old_secondary_ctx->lock_);
    secondary_status_before_unconfirmed = old_secondary_ctx->redo_flush_status_;
  }
  int64_t member_count_before_unconfirmed = 0;
  bool response_sent_before_unconfirmed = true;
  ObPartTransCtx *cached_secondary_before_unconfirmed = nullptr;
  {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    ASSERT_NE(nullptr, old_primary_ctx->hotspot_redo_cache_.cache_);
    member_count_before_unconfirmed =
        old_primary_ctx->hotspot_redo_cache_.cache_->get_hotspot_cache_count();
    ASSERT_GT(member_count_before_unconfirmed, 0);
    response_sent_before_unconfirmed =
        old_primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].has_response_scheduler();
    cached_secondary_before_unconfirmed =
        old_primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].other_ctx_;
  }
  SCN original_commit_version;
  int32_t original_tx_data_state = ObTxCommitData::RUNNING;
  ObTxState downstream_before_unconfirmed = ObTxState::UNKNOWN;
  int set_commit_version_ret = OB_ERR_UNEXPECTED;
  int set_tx_data_state_ret = OB_ERR_UNEXPECTED;
  {
    CtxLockGuard guard(old_primary_ctx->lock_);
    downstream_before_unconfirmed = old_primary_ctx->get_downstream_state();
    original_commit_version = old_primary_ctx->ctx_tx_data_.get_commit_version();
    original_tx_data_state = old_primary_ctx->ctx_tx_data_.get_state();
    set_commit_version_ret =
        old_primary_ctx->ctx_tx_data_.set_commit_version(primary.commit_version_);
    set_tx_data_state_ret =
        old_primary_ctx->ctx_tx_data_.set_state(ObTxCommitData::COMMIT);
  }
  old_primary_ctx->supplement_hotspot_response_after_replay_commit();
  int64_t member_count_after_unconfirmed = 0;
  bool response_sent_after_unconfirmed = true;
  ObPartTransCtx *cached_secondary_after_unconfirmed = nullptr;
  {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    if (nullptr == old_primary_ctx->hotspot_redo_cache_.cache_
        || old_primary_ctx->hotspot_redo_cache_.cache_->get_hotspot_cache_count() <= 0) {
      member_count_after_unconfirmed = -1;
    } else {
      member_count_after_unconfirmed =
          old_primary_ctx->hotspot_redo_cache_.cache_->get_hotspot_cache_count();
      response_sent_after_unconfirmed =
          old_primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].has_response_scheduler();
      cached_secondary_after_unconfirmed =
          old_primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].other_ctx_;
    }
  }
  TxRedoFlushStatus secondary_status_after_unconfirmed = TxRedoFlushStatus::NORMAL_START;
  {
    CtxLockGuard guard(old_secondary_ctx->lock_);
    secondary_status_after_unconfirmed = old_secondary_ctx->redo_flush_status_;
  }
  int restore_tx_data_state_ret = OB_ERR_UNEXPECTED;
  int restore_commit_version_ret = OB_ERR_UNEXPECTED;
  {
    CtxLockGuard guard(old_primary_ctx->lock_);
    restore_tx_data_state_ret =
        old_primary_ctx->ctx_tx_data_.set_state(original_tx_data_state);
    restore_commit_version_ret =
        old_primary_ctx->ctx_tx_data_.set_commit_version(original_commit_version);
  }
  EXPECT_EQ(OB_SUCCESS, set_commit_version_ret);
  EXPECT_EQ(OB_SUCCESS, set_tx_data_state_ret);
  EXPECT_NE(ObTxState::COMMIT, downstream_before_unconfirmed);
  EXPECT_NE(ObTxState::CLEAR, downstream_before_unconfirmed);
  EXPECT_EQ(OB_SUCCESS, restore_tx_data_state_ret);
  EXPECT_EQ(OB_SUCCESS, restore_commit_version_ret);
  EXPECT_EQ(state_before_unconfirmed_supplement, secondary.state_);
  EXPECT_EQ(result_before_unconfirmed_supplement, secondary.commit_out_);
  EXPECT_EQ(member_count_before_unconfirmed, member_count_after_unconfirmed);
  EXPECT_FALSE(response_sent_before_unconfirmed);
  EXPECT_FALSE(response_sent_after_unconfirmed);
  EXPECT_EQ(cached_secondary_before_unconfirmed, cached_secondary_after_unconfirmed);
  EXPECT_EQ(old_secondary_ctx, cached_secondary_after_unconfirmed);
  EXPECT_EQ(secondary_status_before_unconfirmed, secondary_status_after_unconfirmed);
  {
    SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_EQ(old_primary_ctx,
              old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
  }
  EXPECT_EQ(OB_SUCCESS, mgr1->revert_tx_ctx(old_secondary_ctx));
  RecordProperty("q11_unconfirmed_member_count_before",
                 std::to_string(member_count_before_unconfirmed));
  RecordProperty("q11_unconfirmed_member_count_after",
                 std::to_string(member_count_after_unconfirmed));
  RecordProperty("q11_unconfirmed_secondary_status_before",
                 std::to_string(static_cast<int>(secondary_status_before_unconfirmed)));
  RecordProperty("q11_unconfirmed_secondary_status_after",
                 std::to_string(static_cast<int>(secondary_status_after_unconfirmed)));
  RecordProperty("q11_unconfirmed_response_sent_after",
                 std::to_string(response_sent_after_unconfirmed));
  const int64_t early_replay_mts = ObTimeUtility::current_monotonic_time();
  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  bool early_task_in_queue = true;
  bool early_task_enabled = true;
  bool early_task_detached = false;
  for (int64_t i = 0; i < 5000 && !early_task_detached; ++i) {
    {
      SpinRLockGuard guard(old_primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      early_task_in_queue = old_primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue();
      early_task_enabled = old_primary_ctx->hotspot_redo_cache_.resp_task_.is_enabled();
      early_task_detached =
          nullptr == old_primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_;
    }
    if (!early_task_detached) {
      ob_usleep(1_ms);
    }
  }
  EXPECT_FALSE(early_task_in_queue);
  EXPECT_FALSE(early_task_enabled);
  EXPECT_TRUE(early_task_detached);
  n1->wait_all_msg_consumed();
  const ObTxDesc::State state_after_early_supplement = secondary.state_;
  const int result_after_early_supplement = secondary.commit_out_;
  ASSERT_EQ(OB_SUCCESS, mgr1->revert_tx_ctx(old_primary_ctx));
  EXPECT_LT(state_after_early_supplement, ObTxDesc::State::IN_TERMINATE);
  EXPECT_NE(OB_SUCCESS, result_after_early_supplement);

  const int64_t secondary_commit_start_mts = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS, n1->submit_commit_tx(
      secondary, n1->ts_after_ms(20000), secondary_callback, nullptr));
  const int64_t callback_deadline = ObTimeUtility::current_time() + 20_s;
  while (0 == secondary_callback.count_.load() && ObTimeUtility::current_time() < callback_deadline) {
    n1->wait_all_msg_consumed();
    ob_usleep(1_ms);
  }
  EXPECT_EQ(1, secondary_callback.count_.load());
  const int secondary_ret = secondary_callback.result_.load();
  const int64_t secondary_done_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_TRUE(OB_SUCCESS == secondary_ret || OB_TRANS_COMMITED == secondary_ret);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, secondary.state_);
  EXPECT_LT(early_replay_mts, secondary_commit_start_mts);

  RecordProperty("q11_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q11_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q11_early_replay_mts", std::to_string(early_replay_mts));
  RecordProperty("q11_secondary_commit_start_mts", std::to_string(secondary_commit_start_mts));
  RecordProperty("q11_secondary_done_mts", std::to_string(secondary_done_mts));
  RecordProperty("q11_state_after_early_supplement",
                 std::to_string(static_cast<int>(state_after_early_supplement)));
  RecordProperty("q11_result_after_early_supplement",
                 std::to_string(result_after_early_supplement));
  RecordProperty("q11_early_task_in_queue", std::to_string(early_task_in_queue));
  RecordProperty("q11_early_task_enabled", std::to_string(early_task_enabled));
  RecordProperty("q11_early_task_detached", std::to_string(early_task_detached));
  RecordProperty("q11_final_result", std::to_string(secondary_ret));

  n1->wait_all_msg_consumed();
  EXPECT_EQ(1, secondary_callback.count_.load());
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr1));
}

// This phase fixture isolates response-task ownership from 2PC message timing.
// It uses a real hotspot P/S cache, while COMMIT is assigned directly;
// q09 above covers delivery from a replayed COMMIT log.
TEST_F(ObTestHotspotTxLeaderSwitch,
       replay_supplement_preserves_queued_payload_and_finishes_cleanup)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9871, 9971));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9872, 9972));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary.tx_id_));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, primary.tx_id_, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr->get_tx_ctx_directly_from_hash_map(primary.tx_id_, primary_ctx));
  ASSERT_NE(nullptr, primary_ctx);
  ASSERT_FALSE(primary_ctx->is_exiting_);
  ASSERT_TRUE(primary_ctx->hotspot_redo_cache_.has_active_cache());
  ASSERT_EQ(OB_INIT_TWICE,
            primary_ctx->hotspot_redo_cache_.resp_task_.init(primary_ctx));

  SCN fixture_commit_version;
  ASSERT_EQ(OB_SUCCESS, fixture_commit_version.convert_for_tx(9871));
  SCN original_commit_version;
  int32_t original_tx_data_state = ObTxCommitData::RUNNING;
  ObTxState original_downstream_state = ObTxState::UNKNOWN;
  {
    CtxLockGuard guard(primary_ctx->lock_);
    original_commit_version = primary_ctx->ctx_tx_data_.get_commit_version();
    original_tx_data_state = primary_ctx->ctx_tx_data_.get_state();
    original_downstream_state = primary_ctx->get_downstream_state();
    ASSERT_EQ(OB_SUCCESS,
              primary_ctx->ctx_tx_data_.set_commit_version(fixture_commit_version));
    ASSERT_EQ(OB_SUCCESS,
              primary_ctx->ctx_tx_data_.set_state(ObTxCommitData::COMMIT));
    ASSERT_EQ(OB_SUCCESS, primary_ctx->set_downstream_state(ObTxState::COMMIT));
  }

  const int64_t task_ref_before = primary_ctx->get_ref();
  {
    SpinWLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    ASSERT_NE(nullptr, primary_ctx->hotspot_redo_cache_.cache_);
    ASSERT_EQ(1, primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_count_);
    primary_ctx->hotspot_redo_cache_.cache_->hotspot_cache_[0].set_resp_status(
        TxSecondaryRespStatus::RESP_COMMITTED);
  }

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  static const char *RESP_EVENT = "replay_cleanup_worker_paused";
  static const char *RESP_CONTINUE_EVENT = "replay_cleanup_worker_continue";
  NAMED_DEFER(response_debug_cleanup,
              ObDebugSyncAction resume_action;
              resume_action.sync_point_ = NOW;
              resume_action.timeout_ = 20_s;
              resume_action.execute_ = 1;
              resume_action.signal_ = RESP_CONTINUE_EVENT;
              (void)GDS.set_global_action(false, false, resume_action);
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_response;
  pause_response.sync_point_ = BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE;
  pause_response.timeout_ = 20_s;
  pause_response.execute_ = 1;
  pause_response.signal_ = RESP_EVENT;
  pause_response.wait_ = RESP_CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_response));

  bool active_ref_pinned = true;
  ATOMIC_INC(&primary_ctx->hotspot_redo_cache_.active_ref_cnt_);
  NAMED_DEFER(release_fixture_active_ref,
              if (active_ref_pinned) {
                ATOMIC_DEC(&primary_ctx->hotspot_redo_cache_.active_ref_cnt_);
              });
  ASSERT_EQ(OB_SUCCESS,
            primary_ctx->hotspot_redo_cache_.push_response_task(
                OB_SUCCESS, fixture_commit_version));
  ObDebugSyncAction wait_response;
  wait_response.sync_point_ = NOW;
  wait_response.timeout_ = 20_s;
  wait_response.execute_ = 1;
  wait_response.wait_ = RESP_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_response));
  int queued_result = OB_INVALID_ARGUMENT;
  SCN queued_version;
  {
    SpinWLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    ASSERT_TRUE(primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_TRUE(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER
                == primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
    queued_result = primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_;
    queued_version = primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_;
    ASSERT_NE(nullptr, primary_ctx->hotspot_redo_cache_.cache_);
    // Controlled phase fixture: model a completed response whose cache cleanup
    // is still blocked, so replay must bypass the recorded-success enqueue gate.
    primary_ctx->hotspot_redo_cache_.cache_->last_response_succ_ret_code_ = OB_SUCCESS;
  }
  primary_ctx->supplement_hotspot_response_after_replay_commit();
  {
    SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_EQ(primary_ctx,
              primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
    EXPECT_TRUE(primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue());
    EXPECT_TRUE(ObTransRetryTaskType::SECONDARY_TX_REPLAY_RESP_SCHEDULER
                == primary_ctx->hotspot_redo_cache_.resp_task_.get_task_type());
    EXPECT_EQ(queued_result, primary_ctx->hotspot_redo_cache_.resp_task_.tx_result_);
    EXPECT_EQ(queued_version, primary_ctx->hotspot_redo_cache_.resp_task_.commit_version_);
  }
  EXPECT_EQ(task_ref_before, primary_ctx->get_ref());
  EXPECT_GT(primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count(), 0);
  ATOMIC_DEC(&primary_ctx->hotspot_redo_cache_.active_ref_cnt_);
  active_ref_pinned = false;

  ObDebugSyncAction resume_response;
  resume_response.sync_point_ = NOW;
  resume_response.timeout_ = 20_s;
  resume_response.execute_ = 1;
  resume_response.signal_ = RESP_CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_response));
  bool response_task_detached = false;
  const int64_t finish_deadline = ObTimeUtility::current_time() + 10_s;
  do {
    {
      SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
      response_task_detached =
          nullptr == primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_
          && !primary_ctx->hotspot_redo_cache_.resp_task_.is_in_queue();
    }
    if (!response_task_detached) {
      ob_usleep(1_ms);
    }
  } while (!response_task_detached && ObTimeUtility::current_time() < finish_deadline);
  EXPECT_TRUE(response_task_detached);
  EXPECT_FALSE(primary_ctx->is_exiting_);
  EXPECT_EQ(task_ref_before - 1, primary_ctx->get_ref());
  EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());

  primary_ctx->supplement_hotspot_response_after_replay_commit();
  EXPECT_EQ(task_ref_before - 1, primary_ctx->get_ref());
  {
    SpinRLockGuard guard(primary_ctx->hotspot_redo_cache_.hotspot_lock_);
    EXPECT_EQ(nullptr,
              primary_ctx->hotspot_redo_cache_.resp_task_.primary_tx_ctx_);
  }

  {
    CtxLockGuard guard(primary_ctx->lock_);
    ASSERT_EQ(OB_SUCCESS,
              primary_ctx->ctx_tx_data_.set_commit_version(original_commit_version));
    ASSERT_EQ(OB_SUCCESS,
              primary_ctx->ctx_tx_data_.set_state(original_tx_data_state));
    ASSERT_EQ(OB_SUCCESS, primary_ctx->set_downstream_state(original_downstream_state));
  }
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(primary_ctx));
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary, OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(secondary, OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  EXPECT_EQ(0, mgr->get_tx_ctx_count());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

// Original apply behavior remains action-gated. An S that independently
// aborted is not changed to COMMIT and does not receive a fabricated success
// after the P has already accepted its COMMIT log.
TEST_F(ObTestHotspotTxLeaderSwitch, q09_apply_commit_preserves_aborted_secondary)
{
  CountingCommitCallback primary_cb;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9871, 9971));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9872, 9972));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;
  secondary.set_hotspot_group_commit_info(primary_id, false);

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ObPartTransCtx *secondary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr->get_tx_ctx_directly_from_hash_map(secondary_id, secondary_ctx));
  n1->fake_tx_log_adapter_->set_pause();
  NAMED_DEFER(resume_q09_apply,
              n1->fake_tx_log_adapter_->clear_pause();
              n1->fake_tx_log_adapter_->wakeup());
  ASSERT_EQ(OB_SUCCESS, n1->submit_commit_tx(
      primary, n1->ts_after_ms(5000), primary_cb, nullptr));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary_id, false, primary_ctx));
  NAMED_DEFER(revert_q09_primary,
              if (nullptr != primary_ctx) { mgr->revert_tx_ctx(primary_ctx); });
  const int64_t submit_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  bool state_log_submitted = false;
  while (!state_log_submitted && ObTimeUtility::current_monotonic_time() < submit_deadline) {
    {
      CtxLockGuard guard(primary_ctx->lock_);
      state_log_submitted = primary_ctx->sub_state_.is_state_log_submitted();
    }
    if (!state_log_submitted) { ob_usleep(1_ms); }
  }
  ASSERT_TRUE(state_log_submitted);
  // Keep the real map reference across rollback so the independently aborted
  // participant can be inspected after it has been removed from the hash map.
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(secondary));
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, secondary.state_);
  int64_t secondary_action_before_primary_commit = ObPartTransAction::UNKNOWN;
  {
    CtxLockGuard guard(secondary_ctx->lock_);
    secondary_action_before_primary_commit = secondary_ctx->part_trans_action_;
  }
  EXPECT_EQ(static_cast<int64_t>(ObPartTransAction::ABORT),
            secondary_action_before_primary_commit);
  const int secondary_result_before_primary_commit = secondary.commit_out_;

  n1->fake_tx_log_adapter_->clear_pause();
  n1->fake_tx_log_adapter_->wakeup();
  const int64_t commit_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while (0 == primary_cb.count_.load() && ObTimeUtility::current_monotonic_time() < commit_deadline) {
    ob_usleep(1_ms);
  }
  ASSERT_EQ(1, primary_cb.count_.load());
  const int primary_commit_ret = primary_cb.result_.load();
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, secondary.state_);
  EXPECT_EQ(OB_TRANS_ROLLBACKED, secondary.abort_cause_);
  EXPECT_FALSE(secondary.commit_version_.is_valid());
  EXPECT_EQ(secondary_result_before_primary_commit, secondary.commit_out_);
  EXPECT_NE(OB_SUCCESS, secondary.commit_out_);
  int64_t secondary_action_after_primary_commit = ObPartTransAction::UNKNOWN;
  {
    CtxLockGuard guard(secondary_ctx->lock_);
    secondary_action_after_primary_commit = secondary_ctx->part_trans_action_;
  }
  EXPECT_EQ(static_cast<int64_t>(ObPartTransAction::ABORT),
            secondary_action_after_primary_commit);
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(secondary_ctx));
  RecordProperty("q09_apply_abort_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q09_apply_abort_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q09_apply_abort_secondary_action",
                 std::to_string(secondary_action_before_primary_commit));
  RecordProperty("q09_apply_abort_primary_result", std::to_string(primary_commit_ret));
  RecordProperty("q09_apply_abort_secondary_result", std::to_string(secondary.commit_out_));

  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(primary_ctx));
  primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

// Q08 qualification comes from the SQL result path only after the required
// partial rollback has succeeded. A precondition failure must leave the
// started secondary unqualified.
TEST_F(ObTestHotspotTxLeaderSwitch, q08_sql_partial_rollback_publishes_qualification)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  DEFER(delete(n1));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  GCONF.self_addr_ = n1->addr_;

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9831, 9931));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9832, 9932));
  const ObTransID primary_id = primary.tx_id_;
  const ObTransID secondary_id = secondary.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_id));
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_id, members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();
  const int64_t aggregation_complete_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_FALSE(secondary.get_hotspot_primary_tx_id().is_valid());
  ObLSTxCtxMgr *pre_save_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, pre_save_mgr));
  const int64_t pre_identity_prepare_start_mts = ObTimeUtility::current_monotonic_time();
  const int pre_identity_switch_ret = pre_save_mgr->switch_to_follower_gracefully();
  const int64_t pre_identity_prepare_done_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_EQ(OB_SUCCESS, pre_identity_switch_ret);
  EXPECT_EQ(OB_SUCCESS, pre_save_mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(pre_save_mgr->in_leader_serving_state());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(pre_save_mgr));

  sql::ObSQLSessionInfo primary_session(1001);
  sql::ObSQLSessionInfo secondary_session(1001);
  sql::ObSQLSessionInfo dummy_session(1001);
  primary_session.tenant_id_ = primary_session.effective_tenant_id_ = 1001;
  secondary_session.tenant_id_ = secondary_session.effective_tenant_id_ = 1001;
  dummy_session.tenant_id_ = dummy_session.effective_tenant_id_ = 1001;
  const int64_t query_start_time = ObTimeUtility::current_time();
  primary_session.set_query_start_time(query_start_time);
  secondary_session.set_query_start_time(query_start_time);
  primary_session.sys_vars_cache_.set_ob_query_timeout(1000 * 1000 * 1000L);
  secondary_session.sys_vars_cache_.set_ob_query_timeout(1000 * 1000 * 1000L);
  primary_session.get_tx_desc() = &primary;
  secondary_session.get_tx_desc() = &secondary;
  DEFER(primary_session.get_tx_desc() = nullptr;
        secondary_session.get_tx_desc() = nullptr);

  sql::ObGroupCommitSubRequest head;
  sql::ObGroupCommitSubRequest target;
  sql::ObGroupCommitSubRequest dummy;
  obmysql::ObSqlSockSession target_sock(obmysql::global_sm_conn_callback, nullptr);
  head.sess_ = &primary_session;
  head.is_trans_started_ = true;
  target.sess_ = &secondary_session;
  target.req_ = &target_sock.sql_req_;
  target.is_trans_started_ = true;
  dummy.sess_ = &dummy_session;
  dummy.is_trans_started_ = false;
  head.next_ = &target;
  target.next_ = &dummy;
  dummy.next_ = nullptr;

  sql::ObGroupCommitAggInfo agg_info;
  agg_info.inflight_req_handle_.head_ = &head;
  agg_info.inflight_req_handle_.sub_req_cnt_ = 3;
  rpc::ObRequest request(rpc::ObRequest::OB_MYSQL);
  request.set_group_commit_agg_info(&agg_info);

  ObArenaAllocator allocator("SqlQualIT");
  sql::ObResultSet result(primary_session, allocator);
  result.get_exec_context().set_my_session(&primary_session);
  ASSERT_EQ(OB_SUCCESS, result.get_exec_context().create_physical_plan_ctx());
  sql::ObPhysicalPlanCtx *plan_ctx = result.get_exec_context().get_physical_plan_ctx();
  ASSERT_NE(nullptr, plan_ctx);
  plan_ctx->set_timeout_timestamp(ObTimeUtility::current_time() + 1000 * 1000 * 1000L);
  ASSERT_EQ(OB_SUCCESS, plan_ctx->create_implicit_cursor_infos(3));
  for (int64_t i = 0; i < 3; ++i) {
    sql::ObImplicitCursorInfo cursor;
    cursor.stmt_id_ = i;
    cursor.affected_rows_ = (0 == i ? 1 : 0);
    ASSERT_EQ(OB_SUCCESS, plan_ctx->replace_implicit_cursor_info(cursor));
  }
  sql::ObPhysicalPlan plan;
  plan.set_rollback_on_no_affected_rows(true);
  sql::ObPhysicalPlan no_rollback_rule_plan;
  no_rollback_rule_plan.set_rollback_on_no_affected_rows(false);

  result.reset_implicit_cursor_idx();
  const int ineligible_zero_ret =
      result.set_async_cb_ok_packet_param(no_rollback_rule_plan, &request);
  EXPECT_EQ(OB_SUCCESS, ineligible_zero_ret);
  EXPECT_FALSE(target.rollback_on_no_affected_rows_);
  EXPECT_FALSE(dummy.rollback_on_no_affected_rows_);

  const ObAddr saved_self = GCONF.self_addr_;
  GCONF.self_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.9", 8888);
  const int failure_ret = result.set_async_cb_ok_packet_param(plan, &request);
  // Leader-not-local decision: the group rolls back explicitly with the
  // dedicated result code instead of a generic OB_ERR_UNEXPECTED.
  EXPECT_EQ(OB_TRANS_ROLLBACKED, failure_ret);
  EXPECT_FALSE(target.rollback_on_no_affected_rows_);

  GCONF.self_addr_ = saved_self;
  result.reset_implicit_cursor_idx();
  const int success_ret = result.set_async_cb_ok_packet_param(plan, &request);
  EXPECT_EQ(OB_SUCCESS, success_ret);
  EXPECT_TRUE(target.rollback_on_no_affected_rows_);
  EXPECT_TRUE(dummy.rollback_on_no_affected_rows_);

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("30s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(sql_debug_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  static const char *COPIED_EVENT = "gc_secondary_info_copied";
  static const char *CONTINUE_EVENT = "gc_primary_end_continue";
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = BEFORE_GROUP_COMMIT_PRIMARY_END_TRANS;
  pause_action.timeout_ = 10_s;
  pause_action.execute_ = 1;
  pause_action.signal_ = COPIED_EVENT;
  pause_action.wait_ = CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));

  std::atomic<int> end_group_ret(OB_ERR_UNEXPECTED);
  CountingCommitCallback sql_secondary_cb;
  std::thread end_group_thread([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    ObCurTraceId::init(n1->addr_);
    end_group_ret.store(sql::ObSqlTransControl::implicit_end_group_commit_trans(
        agg_info, result.get_exec_context(), false, nullptr, false));
  });
  ObDebugSyncAction wait_copied_action;
  wait_copied_action.sync_point_ = NOW;
  wait_copied_action.timeout_ = 10_s;
  wait_copied_action.execute_ = 1;
  wait_copied_action.wait_ = COPIED_EVENT;
  const int64_t wait_start_mts = ObTimeUtility::current_monotonic_time();
  const int wait_copied_ret = GDS.set_global_action(false, false, wait_copied_action);
  const bool copy_observed = ObTimeUtility::current_monotonic_time() - wait_start_mts < 9_s;
  EXPECT_EQ(OB_SUCCESS, wait_copied_ret);
  EXPECT_TRUE(copy_observed);
  EXPECT_EQ(primary_id, secondary.get_hotspot_primary_tx_id());
  EXPECT_TRUE(secondary.need_rollback_on_no_affected_rows());
  set_errsim_retry_timeout(500 * 1000);
  const int64_t identity_saved_mts = ObTimeUtility::current_monotonic_time();
  const int switch_while_primary_paused_ret = mgr->switch_to_follower_gracefully();
  const int64_t switch_while_primary_paused_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_EQ(OB_SUCCESS, switch_while_primary_paused_ret);
  EXPECT_FALSE(mgr->is_master());
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());

  const int sql_secondary_submit_ret = n1->submit_commit_tx(
      secondary, n1->ts_after_ms(5000), sql_secondary_cb, nullptr);
  const int sql_primary_commit_ret = n1->commit_tx(primary, n1->ts_after_ms(5000));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();
  const int64_t sql_secondary_callback_deadline =
      ObTimeUtility::current_monotonic_time() + 5_s;
  while (0 == sql_secondary_cb.count_.load()
         && ObTimeUtility::current_monotonic_time() < sql_secondary_callback_deadline) {
    ob_usleep(1_ms);
  }
  // Drive the SQL-copied qualification through a real P commit and snapshot S
  // before the resumed SQL end path resets its descriptor.
  const ObTxDesc::State sql_secondary_state = secondary.state_;
  const int sql_secondary_result = secondary.commit_out_;
  const SCN sql_secondary_version = secondary.commit_version_;
  const int sql_secondary_callback_count = sql_secondary_cb.count_.load();
  const int sql_secondary_callback_result = sql_secondary_cb.result_.load();
  EXPECT_EQ(OB_SUCCESS, sql_secondary_submit_ret);
  EXPECT_EQ(OB_SUCCESS, sql_primary_commit_ret);
  EXPECT_EQ(1, sql_secondary_callback_count);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, sql_secondary_callback_result);
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, sql_secondary_state);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, sql_secondary_result);
  EXPECT_FALSE(sql_secondary_version.is_valid());

  ObDebugSyncAction resume_action;
  resume_action.sync_point_ = NOW;
  resume_action.timeout_ = 10_s;
  resume_action.execute_ = 1;
  resume_action.signal_ = CONTINUE_EVENT;
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_action));
  end_group_thread.join();
  const int64_t end_group_thread_done_mts = ObTimeUtility::current_monotonic_time();
  // The real P commit above already completed the transaction. This resumed
  // SQL branch verifies the preserved end call, not the MySQL response path.
  EXPECT_EQ(OB_TRANS_COMMITED, end_group_ret.load());
  EXPECT_GT(end_group_thread_done_mts, switch_while_primary_paused_mts);
  EXPECT_GT(identity_saved_mts, aggregation_complete_mts);

  RecordProperty("q08_sql_primary_tx_id", std::to_string(primary_id.get_id()));
  RecordProperty("q08_sql_secondary_tx_id", std::to_string(secondary_id.get_id()));
  RecordProperty("q08_sql_failure_result", std::to_string(failure_ret));
  RecordProperty("q08_sql_success_result", std::to_string(success_ret));
  RecordProperty("q08_sql_ineligible_zero_result", std::to_string(ineligible_zero_ret));
  RecordProperty("q08_sql_secondary_qualified",
                 std::to_string(target.rollback_on_no_affected_rows_));
  RecordProperty("q08_sql_copy_observed", std::to_string(copy_observed));
  RecordProperty("q08_sql_end_group_result", std::to_string(end_group_ret.load()));
  RecordProperty("q08_sql_identity_saved_mts", std::to_string(identity_saved_mts));
  RecordProperty("q08_sql_aggregation_complete_mts",
                 std::to_string(aggregation_complete_mts));
  RecordProperty("q08_sql_pre_identity_prepare_start_mts",
                 std::to_string(pre_identity_prepare_start_mts));
  RecordProperty("q08_sql_pre_identity_prepare_done_mts",
                 std::to_string(pre_identity_prepare_done_mts));
  RecordProperty("q08_sql_pre_identity_switch_result",
                 std::to_string(pre_identity_switch_ret));
  RecordProperty("q08_sql_switch_while_primary_paused_mts",
                 std::to_string(switch_while_primary_paused_mts));
  RecordProperty("q08_sql_end_group_thread_done_mts",
                 std::to_string(end_group_thread_done_mts));
  RecordProperty("q08_sql_switch_lead_until_group_done_us",
                 std::to_string(end_group_thread_done_mts - switch_while_primary_paused_mts));
  RecordProperty("q08_sql_commit_secondary_submit_result",
                 std::to_string(sql_secondary_submit_ret));
  RecordProperty("q08_sql_commit_primary_result", std::to_string(sql_primary_commit_ret));
  RecordProperty("q08_sql_commit_secondary_callback_count",
                 std::to_string(sql_secondary_callback_count));
  RecordProperty("q08_sql_commit_secondary_callback_result",
                 std::to_string(sql_secondary_callback_result));
  RecordProperty("q08_sql_commit_secondary_state",
                 std::to_string(static_cast<int>(sql_secondary_state)));
  RecordProperty("q08_sql_commit_secondary_result", std::to_string(sql_secondary_result));
  RecordProperty("q08_sql_commit_secondary_version",
                 std::to_string(sql_secondary_version.get_val_for_tx()));
  RecordProperty("q08_sql_secondary_state", std::to_string(static_cast<int>(secondary.state_)));
  RecordProperty("q08_sql_secondary_result", std::to_string(secondary.commit_out_));
  RecordProperty("q08_sql_secondary_version", std::to_string(secondary.commit_version_.get_val_for_tx()));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q08 SQL qualification observations",
            "ls_id", n1->ls_id_, K(primary_id), K(secondary_id), K(failure_ret), K(success_ret),
            K(ineligible_zero_ret),
            "secondary_qualified", target.rollback_on_no_affected_rows_,
            K(copy_observed), "end_group_ret", end_group_ret.load(),
            "secondary_state", secondary.state_, "secondary_result", secondary.commit_out_,
            "secondary_version", secondary.commit_version_);

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, secondary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

// Issue 1 (wall 2): the participant address is local but the primary ctx is
// gone (leader moved and the old primary exited). The service must map the
// missing ctx to the same explicit whole-group rollback result as the
// SQL-layer address mismatch, not a generic internal error.
TEST_F(ObTestHotspotTxLeaderSwitch, hotspot_rollback_to_missing_primary_rolls_back_group)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  // The primary tx id never created a ctx on this LS; the rollback set must be
  // non-empty so the service reaches the ctx lookup branch.
  const ObTransID missing_primary_tx_id(123456789);
  ObAggregatedTxIDArray rollback_tx_ids;
  ASSERT_EQ(OB_SUCCESS, rollback_tx_ids.push_back(ObTransID(987654321)));
  EXPECT_EQ(OB_TRANS_ROLLBACKED,
            n1->txs_.hotspot_rollback_to(participant, missing_primary_tx_id, rollback_tx_ids));
  RecordProperty("wall2_missing_primary_tx_id",
                 std::to_string(missing_primary_tx_id.get_id()));
  RecordProperty("wall2_rollback_member_count",
                 std::to_string(rollback_tx_ids.count()));
}

// Q04/Q05: a real COLLECTING group remains part of the prepare barrier while
// redo is blocked. Prepare timeout cancellation preserves the active aggregation count and reopens
// admission; a later prepare succeeds after normal acceleration can progress.
TEST_F(ObTestHotspotTxLeaderSwitch, q04_q05_prepare_timeout_cancel_and_retry)
{
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_prepare_self_addr = GCONF.self_addr_;
  NAMED_DEFER(restore_prepare_self_addr,
              GCONF.self_addr_ = saved_prepare_self_addr);
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9711, 9811));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9712, 9812));
  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary.tx_id_));
  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
  NAMED_DEFER(unblock_q04_redo, n1->fake_tx_log_adapter_->clear_block_log_type());
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, primary.tx_id_, members,
                ObTimeUtility::fast_current_time() + 20 * 1000));

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  NAMED_DEFER(revert_q04_mgr,
              if (nullptr != mgr) {
                (void)n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr);
              });
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_FALSE(mgr->is_hotspot_aggregation_blocked());
  ASSERT_EQ(1, mgr->get_hotspot_aggregation_count());
  // 30ms prepare budget; acceleration starts and repeats every 3ms.
  set_errsim_retry_timeout(30 * 1000);
  const int64_t start_us = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_TIMEOUT, mgr->switch_to_follower_gracefully());
  ASSERT_GE(ObTimeUtility::current_monotonic_time() - start_us, 20 * 1000);
  ASSERT_TRUE(mgr->is_master());
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_FALSE(mgr->is_hotspot_aggregation_blocked());
  ASSERT_EQ(1, mgr->get_hotspot_aggregation_count());

  ASSERT_EQ(OB_SUCCESS, mgr->acquire_hotspot_aggregation());
  ASSERT_EQ(2, mgr->get_hotspot_aggregation_count());
  ASSERT_EQ(OB_SUCCESS, mgr->release_hotspot_aggregation());
  ASSERT_EQ(1, mgr->get_hotspot_aggregation_count());
  n1->fake_tx_log_adapter_->clear_block_log_type();
  set_errsim_retry_timeout(300 * 1000);
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  ASSERT_EQ(0, mgr->get_hotspot_aggregation_count());
  ASSERT_FALSE(mgr->is_master());
  ASSERT_TRUE(mgr->is_hotspot_aggregation_blocked());
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_FALSE(mgr->is_hotspot_aggregation_blocked());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
  mgr = nullptr;
  const int primary_commit_ret = n1->commit_tx(primary, n1->ts_after_ms(5000));
  const int secondary_commit_ret = n1->commit_tx(secondary, n1->ts_after_ms(5000));
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  EXPECT_EQ(OB_SUCCESS, primary_guard.release());
  EXPECT_EQ(OB_SUCCESS, secondary_guard.release());
}

// Exercise the final submission gate on a pinned, real ctx before and after
// forced revoke. The secondary states are injected only to exhaust the gate
// matrix; this does not model six independent aggregation lifecycles.
TEST_F(ObTestHotspotTxLeaderSwitch, secondary_never_submits_logs_before_or_after_revoke)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  ObTxDescGuard tx_guard = n1->get_tx_guard();
  ObTxDesc &tx = tx_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, 9891, 9991));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  NAMED_DEFER(revert_secondary_gate_mgr,
              (void)n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(tx.tx_id_, false, ctx));
  NAMED_DEFER(revert_secondary_gate_ctx, (void)mgr->revert_tx_ctx(ctx));
  const TxRedoFlushStatus states[] = {
      TxRedoFlushStatus::SECONDARY_PREPARING,
      TxRedoFlushStatus::SECONDARY_MIGRATING,
      TxRedoFlushStatus::SECONDARY_MIGRATE_SUBMITTED,
      TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED,
      TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED,
      TxRedoFlushStatus::SECONDARY_MIGRATE_SUCCEEDED};
  const ObTxLogType log_types[] = {ObTxLogType::TX_REDO_LOG, ObTxLogType::TX_COMMIT_LOG,
                                 ObTxLogType::TX_ABORT_LOG, ObTxLogType::TX_CLEAR_LOG,
                                 ObTxLogType::TX_ACTIVE_INFO_LOG, ObTxLogType::TX_COMMIT_INFO_LOG};
  for (int phase = 0; phase < 2; ++phase) {
    if (1 == phase) {
      ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_forcedly());
    }
    CtxLockGuard guard(ctx->lock_);
    const TxRedoFlushStatus saved_status = ctx->redo_flush_status_;
    const auto saved_sub_state = ctx->sub_state_;
    NAMED_DEFER(restore_secondary_gate_state,
                ctx->redo_flush_status_ = saved_status;
                ctx->sub_state_ = saved_sub_state);
    ctx->sub_state_.set_force_abort();
    for (const auto state : states) {
      ctx->redo_flush_status_ = state;
      EXPECT_EQ(OB_TRANS_HAS_DECIDED, ctx->check_can_submit_redo_());
      for (const auto log_type : log_types) {
        SCOPED_TRACE(::testing::Message() << "phase=" << phase
            << " state=" << static_cast<int>(state)
            << " log_type=" << static_cast<int>(log_type));
        ObTxLogBlock log_block;
        ASSERT_EQ(OB_SUCCESS, log_block.cb_arg_array_.push_back(ObTxCbArg(log_type, nullptr)));
        ASSERT_FALSE(log_block.is_hotspot_redo());
        ObTxLogCb cb;
        ObTxLogCb *log_cb = &cb;
        // No buffer is needed: secondary rejection must precede CB binding
        // and any log adapter call, even for ABORT/CLEAR after force-abort.
        EXPECT_EQ(OB_NEED_RETRY,
                  ctx->submit_log_block_out_(log_block, log_cb, SCN::min_scn(),
                      logservice::ObReplayBarrierType::NO_NEED_BARRIER, 0));
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, tx_guard.release());
}

#ifdef ENABLE_DEBUG_LOG
// Acceleration and timeout share one monotonic prepare budget. A real PREPARING
// group is held before cache construction while a second real group remains
// COLLECTING with redo submission blocked. Only the COLLECTING group receives
// an acceleration dispatch; timeout keeps both groups in the active aggregation
// count for the next preparation.
TEST_F(ObTestHotspotTxLeaderSwitch, q04_t1_promotes_only_collecting_with_shared_t2)
{
  static const char *PREPARING_REACHED = "hs_preparing";
  static const char *PREPARING_CONTINUE = "hs_prepare_go";
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  const ObAddr saved_self_addr = GCONF.self_addr_;
  GCONF.self_addr_ = n1->addr_;
  NAMED_DEFER(restore_self_addr, GCONF.self_addr_ = saved_self_addr);
  ObLogger::get_logger().set_log_level("DEBUG");
  NAMED_DEFER(restore_log_level, ObLogger::get_logger().set_log_level("INFO"));
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard preparing_p_guard = n1->get_tx_guard();
  ObTxDescGuard preparing_s_guard = n1->get_tx_guard();
  ObTxDescGuard collecting_p_guard = n1->get_tx_guard();
  ObTxDescGuard collecting_s_guard = n1->get_tx_guard();
  ObTxDesc &preparing_p = preparing_p_guard.get_tx_desc();
  ObTxDesc &preparing_s = preparing_s_guard.get_tx_desc();
  ObTxDesc &collecting_p = collecting_p_guard.get_tx_desc();
  ObTxDesc &collecting_s = collecting_s_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(preparing_p, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(preparing_s, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(collecting_p, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(collecting_s, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(preparing_p, 9751, 9851));
  ASSERT_EQ(OB_SUCCESS, n1->write(preparing_s, 9752, 9852));
  ASSERT_EQ(OB_SUCCESS, n1->write(collecting_p, 9753, 9853));
  ASSERT_EQ(OB_SUCCESS, n1->write(collecting_s, 9754, 9854));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray preparing_members;
  ObAggregatedTxIDArray collecting_members;
  ASSERT_EQ(OB_SUCCESS, preparing_members.push_back(preparing_s.tx_id_));
  ASSERT_EQ(OB_SUCCESS, collecting_members.push_back(collecting_s.tx_id_));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("15s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(debug_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = AFTER_HOTSPOT_AGGREGATION_ADMISSION;
  pause_action.timeout_ = 15_s;
  pause_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS, pause_action.signal_.assign(ObString::make_string(PREPARING_REACHED)));
  ASSERT_EQ(OB_SUCCESS, pause_action.wait_.assign(ObString::make_string(PREPARING_CONTINUE)));
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> preparing_ret(OB_ERR_UNEXPECTED);
  std::thread preparing_thread([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    preparing_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, preparing_p.tx_id_, preparing_members, n1->ts_after_ms(5000)));
  });
  NAMED_DEFER(preparing_thread_cleanup,
              if (preparing_thread.joinable()) {
                ObDebugSyncAction cleanup_action;
                cleanup_action.sync_point_ = NOW;
                cleanup_action.timeout_ = 15_s;
                cleanup_action.execute_ = 1;
                (void)cleanup_action.signal_.assign(
                    ObString::make_string(PREPARING_CONTINUE));
                (void)GDS.set_global_action(false, false, cleanup_action);
                preparing_thread.join();
              });
  ObDebugSyncAction wait_action;
  wait_action.sync_point_ = NOW;
  wait_action.timeout_ = 15_s;
  wait_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS, wait_action.wait_.assign(ObString::make_string(PREPARING_REACHED)));
  const int preparing_wait_ret = GDS.set_global_action(false, false, wait_action);

  n1->fake_tx_log_adapter_->push_back_block_log_type(ObTxLogType::TX_REDO_LOG);
  NAMED_DEFER(unblock_redo, n1->fake_tx_log_adapter_->clear_block_log_type());
  const int collecting_ret = n1->txs_.sync_hotspot_legality_validation(
      participant, collecting_p.tx_id_, collecting_members,
      ObTimeUtility::fast_current_time() + 20 * 1000);
  ObPartTransCtx *preparing_ctx = nullptr;
  ObPartTransCtx *collecting_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(preparing_p.tx_id_, false, preparing_ctx));
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(collecting_p.tx_id_, false, collecting_ctx));
  TxRedoFlushStatus preparing_status_before = TxRedoFlushStatus::NORMAL_START;
  TxRedoFlushStatus collecting_status_before = TxRedoFlushStatus::NORMAL_START;
  int64_t collecting_dispatch_before = 0;
  {
    CtxLockGuard guard(preparing_ctx->lock_);
    preparing_status_before = preparing_ctx->redo_flush_status_;
  }
  {
    CtxLockGuard guard(collecting_ctx->lock_);
    collecting_status_before = collecting_ctx->redo_flush_status_;
    collecting_dispatch_before = ATOMIC_LOAD(
        &collecting_ctx->hotspot_redo_cache_.cache_->hotspot_stat_.dispatch_msg_sent_cnt_);
  }

  set_errsim_retry_timeout(30 * 1000); // T2=30ms; acceleration starts at 3ms, interval=3ms
  const int64_t first_prepare_start_mts = ObTimeUtility::current_monotonic_time();
  std::atomic<int> first_prepare_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> first_prepare_done(false);
  std::thread first_prepare([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    first_prepare_ret.store(mgr->switch_to_follower_gracefully());
    first_prepare_done.store(true);
  });
  int64_t first_dispatch_observed_mts = 0;
  while (!first_prepare_done.load() && 0 == first_dispatch_observed_mts) {
    const int64_t dispatch_count = ATOMIC_LOAD(
        &collecting_ctx->hotspot_redo_cache_.cache_->hotspot_stat_.dispatch_msg_sent_cnt_);
    if (dispatch_count > collecting_dispatch_before) {
      first_dispatch_observed_mts = ObTimeUtility::current_monotonic_time();
    } else {
      ob_usleep(100);
    }
  }
  first_prepare.join();
  const int64_t first_prepare_cancel_mts = ObTimeUtility::current_monotonic_time();
  TxRedoFlushStatus preparing_status_after = TxRedoFlushStatus::NORMAL_START;
  TxRedoFlushStatus collecting_status_after = TxRedoFlushStatus::NORMAL_START;
  int64_t collecting_dispatch_after = 0;
  {
    CtxLockGuard guard(preparing_ctx->lock_);
    preparing_status_after = preparing_ctx->redo_flush_status_;
  }
  {
    CtxLockGuard guard(collecting_ctx->lock_);
    collecting_status_after = collecting_ctx->redo_flush_status_;
    collecting_dispatch_after = ATOMIC_LOAD(
        &collecting_ctx->hotspot_redo_cache_.cache_->hotspot_stat_.dispatch_msg_sent_cnt_);
  }
  const uint64_t count_after_first_cancel = mgr->get_hotspot_aggregation_count();
  const int64_t second_prepare_start_mts = ObTimeUtility::current_monotonic_time();
  const int second_prepare_ret = mgr->switch_to_follower_gracefully();
  const int64_t second_prepare_cancel_mts = ObTimeUtility::current_monotonic_time();
  const uint64_t count_after_second_cancel = mgr->get_hotspot_aggregation_count();

  EXPECT_EQ(OB_SUCCESS, preparing_wait_ret);
  EXPECT_EQ(OB_SUCCESS, collecting_ret);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_PREPARING, preparing_status_before);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, collecting_status_before);
  EXPECT_EQ(OB_TIMEOUT, first_prepare_ret.load());
  EXPECT_GE(first_prepare_cancel_mts - first_prepare_start_mts, 20 * 1000);
  // Wall clock beyond the 30ms T2: is_pending_() wait (up to 100ms) and 10ms rwlock retries.
  // 500ms is an anti-hang tripwire; shared-budget semantics are covered by the count/status asserts below.
  EXPECT_LT(first_prepare_cancel_mts - first_prepare_start_mts, 500 * 1000);
  EXPECT_GT(first_dispatch_observed_mts, first_prepare_start_mts);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_PREPARING, preparing_status_after);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_COLLECTING, collecting_status_after);
  EXPECT_GT(collecting_dispatch_after, collecting_dispatch_before);
  EXPECT_EQ(2, count_after_first_cancel);
  EXPECT_EQ(OB_TIMEOUT, second_prepare_ret);
  EXPECT_GE(second_prepare_cancel_mts - second_prepare_start_mts, 20 * 1000);
  EXPECT_LT(second_prepare_cancel_mts - second_prepare_start_mts, 500 * 1000);
  EXPECT_EQ(2, count_after_second_cancel);

  RecordProperty("q04_t1_t2_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("q04_t1_t2_preparing_primary", std::to_string(preparing_p.tx_id_.get_id()));
  RecordProperty("q04_t1_t2_collecting_primary", std::to_string(collecting_p.tx_id_.get_id()));
  RecordProperty("q04_t1_t2_first_start_mts", std::to_string(first_prepare_start_mts));
  RecordProperty("q04_t1_t2_first_cancel_mts", std::to_string(first_prepare_cancel_mts));
  RecordProperty("q04_t1_t2_first_dispatch_observed_mts",
                 std::to_string(first_dispatch_observed_mts));
  RecordProperty("q04_t1_t2_second_start_mts", std::to_string(second_prepare_start_mts));
  RecordProperty("q04_t1_t2_second_cancel_mts", std::to_string(second_prepare_cancel_mts));
  RecordProperty("q04_t1_t2_collecting_dispatch_before",
                 std::to_string(collecting_dispatch_before));
  RecordProperty("q04_t1_t2_collecting_dispatch_after",
                 std::to_string(collecting_dispatch_after));
  RecordProperty("q04_t1_t2_count_after_first", std::to_string(count_after_first_cancel));
  RecordProperty("q04_t1_t2_count_after_second", std::to_string(count_after_second_cancel));

  // Keep the same real COLLECTING P blocked for a wider experimental budget.
  // The dedicated acceleration log identifies the eligible P and its attempt
  // timestamp; the short rounds use 30ms/3ms and this wide round uses 300ms/30ms.
  set_errsim_retry_timeout(300 * 1000);
  const int64_t wide_prepare_start_mts = ObTimeUtility::current_monotonic_time();
  const int wide_prepare_ret = mgr->switch_to_follower_gracefully();
  const int64_t wide_prepare_cancel_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_EQ(OB_TIMEOUT, wide_prepare_ret);
  EXPECT_GE(wide_prepare_cancel_mts - wide_prepare_start_mts, 200 * 1000);
  EXPECT_EQ(2, mgr->get_hotspot_aggregation_count());
  RecordProperty("q04_t1_t2_wide_start_mts", std::to_string(wide_prepare_start_mts));
  RecordProperty("q04_t1_t2_wide_cancel_mts", std::to_string(wide_prepare_cancel_mts));
  RecordProperty("q04_t1_t2_wide_result", std::to_string(wide_prepare_ret));

  n1->fake_tx_log_adapter_->clear_block_log_type();
  ObDebugSyncAction resume_action;
  resume_action.sync_point_ = NOW;
  resume_action.timeout_ = 15_s;
  resume_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS, resume_action.signal_.assign(ObString::make_string(PREPARING_CONTINUE)));
  EXPECT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_action));
  preparing_thread.join();
  // Unblocking the fake adapter does not enqueue work by itself. A third real
  // preparation waits for the same active groups while the
  // unblocked normal async chain finishes, then enter the formal revoke.
  set_errsim_retry_timeout(300 * 1000);
  const int64_t third_prepare_start_mts = ObTimeUtility::current_monotonic_time();
  const int third_prepare_ret = mgr->switch_to_follower_gracefully();
  const int64_t third_prepare_done_mts = ObTimeUtility::current_monotonic_time();
  RecordProperty("q04_t1_t2_third_start_mts", std::to_string(third_prepare_start_mts));
  RecordProperty("q04_t1_t2_third_done_mts", std::to_string(third_prepare_done_mts));
  RecordProperty("q04_t1_t2_third_result", std::to_string(third_prepare_ret));
  EXPECT_EQ(OB_SUCCESS, preparing_ret.load());
  EXPECT_EQ(OB_SUCCESS, third_prepare_ret);
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(preparing_ctx));
  EXPECT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(collecting_ctx));
  EXPECT_FALSE(mgr->is_master());
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(preparing_p, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(preparing_s, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(collecting_p, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(collecting_s, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, preparing_p_guard.release());
  EXPECT_EQ(OB_SUCCESS, preparing_s_guard.release());
  EXPECT_EQ(OB_SUCCESS, collecting_p_guard.release());
  EXPECT_EQ(OB_SUCCESS, collecting_s_guard.release());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

TEST_F(ObTestHotspotTxLeaderSwitch, stop_readonly_rejection_does_not_close_hotspot_admission)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ASSERT_EQ(OB_SUCCESS, mgr->start_readonly_request());

  EXPECT_EQ(OB_EAGAIN, mgr->stop(false));
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(OB_SUCCESS, mgr->acquire_hotspot_aggregation());
  EXPECT_EQ(OB_SUCCESS, mgr->release_hotspot_aggregation());

  EXPECT_EQ(OB_SUCCESS, mgr->end_readonly_request());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

TEST_F(ObTestHotspotTxLeaderSwitch, resume_leader_reopens_hotspot_admission_after_start_working)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  ASSERT_TRUE(mgr->is_hotspot_aggregation_blocked());

  n1->fake_tx_log_adapter_->set_pause();
  NAMED_DEFER(resume_adapter_cleanup,
              n1->fake_tx_log_adapter_->clear_pause();
              n1->fake_tx_log_adapter_->wakeup());
  ASSERT_EQ(OB_SUCCESS, mgr->resume_leader());
  EXPECT_FALSE(mgr->in_leader_serving_state());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, mgr->acquire_hotspot_aggregation());

  n1->fake_tx_log_adapter_->clear_pause();
  n1->fake_tx_log_adapter_->wakeup();
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(OB_SUCCESS, mgr->acquire_hotspot_aggregation());
  // Rejected role changes must leave admission and the serving leader's
  // existing aggregation count untouched.
  const int64_t serving_state = mgr->state_;
  EXPECT_EQ(OB_STATE_NOT_MATCH, mgr->resume_leader());
  EXPECT_EQ(OB_STATE_NOT_MATCH, mgr->switch_to_leader());
  EXPECT_EQ(serving_state, mgr->state_);
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, mgr->release_hotspot_aggregation());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

TEST_F(ObTestHotspotTxLeaderSwitch,
       preparing_forced_revoke_cleanup_releases_once_before_next_graceful_prepare)
{
  static const char *PREPARING_REACHED = "fr_prepared";
  static const char *PREPARING_CONTINUE = "fr_continue";
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary = primary_guard.get_tx_desc();
  ObTxDesc &secondary = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary, 9271, 9371));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary, 9272, 9372));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary.tx_id_));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ObPartTransCtx *primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary.tx_id_, false, primary_ctx));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("20s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(preparing_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = AFTER_HOTSPOT_AGGREGATION_ADMISSION;
  pause_action.timeout_ = 15_s;
  pause_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            pause_action.signal_.assign(ObString::make_string(PREPARING_REACHED)));
  ASSERT_EQ(OB_SUCCESS,
            pause_action.wait_.assign(ObString::make_string(PREPARING_CONTINUE)));
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> validation_ret(OB_ERR_UNEXPECTED);
  std::thread validation_thread([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    validation_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, primary.tx_id_, members, n1->ts_after_ms(10000)));
  });
  NAMED_DEFER(validation_thread_cleanup,
              if (validation_thread.joinable()) {
                ObDebugSyncAction resume_action;
                resume_action.sync_point_ = NOW;
                resume_action.timeout_ = 15_s;
                resume_action.execute_ = 1;
                (void)resume_action.signal_.assign(
                    ObString::make_string(PREPARING_CONTINUE));
                (void)GDS.set_global_action(false, false, resume_action);
                validation_thread.join();
              });
  ObDebugSyncAction wait_action;
  wait_action.sync_point_ = NOW;
  wait_action.timeout_ = 15_s;
  wait_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            wait_action.wait_.assign(ObString::make_string(PREPARING_REACHED)));
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, wait_action));

  {
    CtxLockGuard guard(primary_ctx->lock_);
    EXPECT_EQ(TxRedoFlushStatus::PRIMARY_PREPARING, primary_ctx->redo_flush_status_);
    EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  }
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_follower_forcedly());
  // An eligible PREPARING P has no persisted redo and takes the no-log local
  // abort path during forced revoke.
  // Prove kill cleanup, rather than inferring completion from an empty cache.
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  {
    CtxLockGuard guard(primary_ctx->lock_);
    const ObTxHotspotRedoCacheHandle::HotspotLogCbView cb_view =
        primary_ctx->hotspot_redo_cache_.get_cb_list_snapshot();
    EXPECT_TRUE(primary_ctx->is_exiting_);
    EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_FAILED,
              primary_ctx->redo_flush_status_);
    EXPECT_EQ(0, primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
    EXPECT_EQ(0, cb_view.free_cb_count);
    EXPECT_EQ(0, cb_view.busy_cb_count);
    EXPECT_EQ(0, cb_view.idle_cb_count);
  }
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();

  ObDebugSyncAction resume_action;
  resume_action.sync_point_ = NOW;
  resume_action.timeout_ = 15_s;
  resume_action.execute_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            resume_action.signal_.assign(ObString::make_string(PREPARING_CONTINUE)));
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, resume_action));
  validation_thread.join();
  EXPECT_NE(OB_SUCCESS, validation_ret.load());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  {
    CtxLockGuard guard(primary_ctx->lock_);
    EXPECT_TRUE(primary_ctx->is_exiting_);
    EXPECT_NE(TxRedoFlushStatus::PRIMARY_PREPARING, primary_ctx->redo_flush_status_);
    EXPECT_NE(TxRedoFlushStatus::PRIMARY_COLLECTING, primary_ctx->redo_flush_status_);
  }
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());

  EXPECT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(primary_ctx));
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
  EXPECT_EQ(OB_SUCCESS, primary_guard.release());
  EXPECT_EQ(OB_SUCCESS, secondary_guard.release());
}
#endif

// Q02: initialization/member validation failure performs cache/ref cleanup
// before leaving PRIMARY_PREPARING and releasing the manager count.
TEST_F(ObTestHotspotTxLeaderSwitch, q02_validation_failure_releases_once_after_cleanup)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 9241, 9341));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(ObTransID(INT64_MAX - 7)));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ObPartTransCtx *ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr->get_tx_ctx(primary_tx.tx_id_, false, ctx));
  ASSERT_NE(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(
                participant, primary_tx.tx_id_, members, n1->ts_after_ms(100)));

  ASSERT_EQ(0, mgr->get_hotspot_aggregation_count());
  {
    CtxLockGuard guard(ctx->lock_);
    ASSERT_EQ(TxRedoFlushStatus::NORMAL_START, ctx->redo_flush_status_);
  }
  ASSERT_EQ(0, mgr->get_hotspot_aggregation_count());
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(ctx));
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary_tx, OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

#ifdef ENABLE_DEBUG_LOG
// Q03/SC001: once aggregation has published success, its active aggregation
// count has been released even though the validation statement has not returned, the P/S
// identity relationship is still retained, P has not committed, and S has not
// received its scheduler result. A new graceful prepare may pass during that
// interval; the old statement is deliberately released later.
TEST_F(ObTestHotspotTxLeaderSwitch, q03_finished_group_does_not_delay_next_prepare)
{
  static const char *FINISHED_EVENT = "hotspot_group_finished";
  static const char *CONTINUE_EVENT = "hotspot_group_finish_continue";
  static const int64_t DEBUG_SYNC_TIMEOUT_US = 10_s;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ObTxDesc &secondary_tx = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary_tx, tx_param));
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 9251, 9351));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary_tx, snapshot, 9252, 9352));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_tx.tx_id_));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(debug_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = AFTER_HOTSPOT_AGGREGATION_FINISHED;
  pause_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  pause_action.execute_ = 1;
  pause_action.signal_ = FINISHED_EVENT;
  pause_action.wait_ = CONTINUE_EVENT;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> validation_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> validation_done(false);
  std::thread validator([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    validation_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, primary_tx.tx_id_, members, n1->ts_after_ms(500)));
    validation_done.store(true);
  });

  ObDebugSyncAction wait_action;
  wait_action.sync_point_ = NOW;
  wait_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  wait_action.execute_ = 1;
  wait_action.wait_ = FINISHED_EVENT;
  const int64_t wait_start_us = ObTimeUtility::current_monotonic_time();
  const int wait_ret = GDS.set_global_action(false, false, wait_action);
  const bool reached_finish_hook =
      ObTimeUtility::current_monotonic_time() - wait_start_us < 9_s;
  // The finish hook publishes PRIMARY_AGGR_SUCCEEDED and releases the aggregation
  // count before the fake log callback advances S from MIGRATING to SYNCED.
  // Drain that independent async work while the validation statement remains
  // paused, then sample exact states.
  n1->wait_all_redolog_applied();
  n1->wait_all_msg_consumed();

  const uint64_t count_after_finish = mgr->get_hotspot_aggregation_count();
  const bool validation_pending_before_switch = !validation_done.load();
  int primary_ctx_ret = OB_ERR_UNEXPECTED;
  int secondary_ctx_ret = OB_ERR_UNEXPECTED;
  TxRedoFlushStatus primary_status = TxRedoFlushStatus::NORMAL_START;
  TxRedoFlushStatus secondary_status = TxRedoFlushStatus::NORMAL_START;
  int64_t retained_identity_count = -1;
  bool primary_all_redo_flushed = false;
  ObPartTransCtx *primary_ctx = nullptr;
  primary_ctx_ret = mgr->get_tx_ctx_directly_from_hash_map(primary_tx.tx_id_, primary_ctx);
  if (OB_SUCCESS == primary_ctx_ret && nullptr != primary_ctx) {
    {
      CtxLockGuard guard(primary_ctx->lock_);
      primary_status = primary_ctx->redo_flush_status_;
      retained_identity_count = primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count();
      primary_all_redo_flushed = primary_ctx->hotspot_redo_cache_.all_redo_flushed();
    }
    (void)mgr->revert_tx_ctx(primary_ctx);
  }
  ObPartTransCtx *secondary_ctx = nullptr;
  secondary_ctx_ret = mgr->get_tx_ctx_directly_from_hash_map(secondary_tx.tx_id_, secondary_ctx);
  if (OB_SUCCESS == secondary_ctx_ret && nullptr != secondary_ctx) {
    {
      CtxLockGuard guard(secondary_ctx->lock_);
      secondary_status = secondary_ctx->redo_flush_status_;
    }
    (void)mgr->revert_tx_ctx(secondary_ctx);
  }

  set_errsim_retry_timeout(500 * 1000);
  const int switch_ret = mgr->switch_to_follower_gracefully();
  const int64_t new_prepare_allowed_us = ObTimeUtility::current_monotonic_time();
  const bool old_statement_still_pending = !validation_done.load();
  ob_usleep(20_ms);
  ObDebugSyncAction resume_action;
  resume_action.sync_point_ = NOW;
  resume_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  resume_action.execute_ = 1;
  resume_action.signal_ = CONTINUE_EVENT;
  const int resume_ret = GDS.set_global_action(false, false, resume_action);
  validator.join();
  const int64_t old_statement_allowed_us = ObTimeUtility::current_monotonic_time();

  EXPECT_EQ(OB_SUCCESS, wait_ret);
  EXPECT_TRUE(reached_finish_hook);
  EXPECT_EQ(0, count_after_finish);
  EXPECT_TRUE(validation_pending_before_switch);
  EXPECT_EQ(OB_SUCCESS, primary_ctx_ret);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED, primary_status);
  EXPECT_TRUE(primary_all_redo_flushed);
  EXPECT_GT(retained_identity_count, 0);
  EXPECT_EQ(OB_SUCCESS, secondary_ctx_ret);
  EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_SYNCED, secondary_status);
  EXPECT_EQ(OB_SUCCESS, switch_ret);
  EXPECT_TRUE(old_statement_still_pending);
  EXPECT_EQ(OB_SUCCESS, resume_ret);
  EXPECT_EQ(OB_SUCCESS, validation_ret.load());
  EXPECT_GT(old_statement_allowed_us - new_prepare_allowed_us, 0);
  EXPECT_FALSE(mgr->is_master());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());

  int takeover_ret = OB_SUCCESS;
  if (!mgr->is_master()) {
    takeover_ret = mgr->switch_to_leader();
    n1->wait_all_redolog_applied();
  }
  EXPECT_EQ(OB_SUCCESS, takeover_ret);
  const int primary_commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  const int64_t primary_commit_done_mts = ObTimeUtility::current_monotonic_time();
  const int secondary_commit_ret = n1->commit_tx(secondary_tx, n1->ts_after_ms(500));
  const int64_t secondary_result_received_mts = ObTimeUtility::current_monotonic_time();
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  EXPECT_GT(primary_commit_done_mts, new_prepare_allowed_us);
  EXPECT_GE(secondary_result_received_mts, primary_commit_done_mts);
  RecordProperty("q03_ls_id", std::to_string(n1->ls_id_.id()));
  RecordProperty("q03_primary_tx_id", std::to_string(primary_tx.tx_id_.get_id()));
  RecordProperty("q03_secondary_tx_id", std::to_string(secondary_tx.tx_id_.get_id()));
  RecordProperty("q03_new_prepare_allowed_mts", std::to_string(new_prepare_allowed_us));
  RecordProperty("q03_old_statement_allowed_mts", std::to_string(old_statement_allowed_us));
  RecordProperty("q03_primary_commit_done_mts", std::to_string(primary_commit_done_mts));
  RecordProperty("q03_secondary_result_received_mts",
                 std::to_string(secondary_result_received_mts));
  RecordProperty("q03_primary_commit_result", std::to_string(primary_commit_ret));
  RecordProperty("q03_secondary_commit_result", std::to_string(secondary_commit_ret));
  TRANS_LOG(INFO, "[hotspot leader switch test] Q03 strict completion observations",
            "ls_id", n1->ls_id_, "primary_tx_id", primary_tx.tx_id_,
            "secondary_tx_id", secondary_tx.tx_id_, K(new_prepare_allowed_us),
            K(old_statement_allowed_us), K(primary_commit_done_mts),
            K(secondary_result_received_mts), K(primary_commit_ret),
            K(secondary_commit_ret));
  EXPECT_EQ(OB_SUCCESS, primary_guard.release());
  EXPECT_EQ(OB_SUCCESS, secondary_guard.release());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

TEST_F(ObTestHotspotTxLeaderSwitch,
       takeover_reopens_admission_preserving_pending_aggregation_count)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_EQ(OB_SUCCESS, mgr->acquire_hotspot_aggregation());
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_follower_forcedly());
  EXPECT_FALSE(mgr->is_master());
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());

  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(1, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, mgr->release_hotspot_aggregation());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

// Q01 real race: pause after the service has acquired the P map reference and
// incremented admission, then close admission. The old entrant must finish as
// one group; a close-first entrant is rejected without changing the count.
TEST_F(ObTestHotspotTxLeaderSwitch, q01_admission_before_close_real_group_race)
{
  static const char *ADMITTED_EVENT = "hotspot_group_admitted";
  static const char *CONTINUE_EVENT = "hotspot_group_continue";
  static const int64_t DEBUG_SYNC_TIMEOUT_US = 10_s;
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDescGuard secondary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ObTxDesc &secondary_tx = secondary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(secondary_tx, tx_param));
  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 9201, 9301));
  ASSERT_EQ(OB_SUCCESS, n1->write(secondary_tx, snapshot, 9202, 9302));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members;
  ASSERT_EQ(OB_SUCCESS, members.push_back(secondary_tx.tx_id_));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(debug_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  ObDebugSyncAction pause_action;
  pause_action.sync_point_ = AFTER_HOTSPOT_AGGREGATION_ADMISSION;
  pause_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  pause_action.execute_ = 1;
  pause_action.signal_ = ADMITTED_EVENT;
  pause_action.wait_ = CONTINUE_EVENT;
  ASSERT_TRUE(pause_action.is_valid());
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(false, false, pause_action));

  std::atomic<int> validation_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> validation_done(false);
  std::thread validator([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    validation_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, primary_tx.tx_id_, members, n1->ts_after_ms(500)));
    validation_done.store(true);
  });
  NAMED_DEFER(validator_cleanup,
              if (validator.joinable()) {
                ObDebugSyncAction cleanup_resume;
                cleanup_resume.sync_point_ = NOW;
                cleanup_resume.timeout_ = DEBUG_SYNC_TIMEOUT_US;
                cleanup_resume.execute_ = 1;
                cleanup_resume.signal_ = CONTINUE_EVENT;
                (void)GDS.set_global_action(false, false, cleanup_resume);
                validator.join();
              });

  ObDebugSyncAction wait_action;
  wait_action.sync_point_ = NOW;
  wait_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  wait_action.execute_ = 1;
  wait_action.wait_ = ADMITTED_EVENT;
  const int wait_ret = GDS.set_global_action(false, false, wait_action);
  const uint64_t count_while_paused = mgr->get_hotspot_aggregation_count();
  const int repeated_validation_ret = n1->txs_.sync_hotspot_legality_validation(
      participant, primary_tx.tx_id_, members, n1->ts_after_ms(100));
  ObPartTransCtx *paused_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            mgr->get_tx_ctx(primary_tx.tx_id_, false, paused_primary_ctx));
  int64_t paused_cache_count = -1;
  TxRedoFlushStatus paused_status = TxRedoFlushStatus::NORMAL_START;
  {
    CtxLockGuard guard(paused_primary_ctx->lock_);
    paused_status = paused_primary_ctx->redo_flush_status_;
    paused_cache_count = paused_primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count();
  }
  ASSERT_EQ(OB_SUCCESS, mgr->revert_tx_ctx(paused_primary_ctx));
  const uint64_t count_after_repeated_validation =
      mgr->get_hotspot_aggregation_count();
  set_errsim_retry_timeout(500 * 1000);
  std::atomic<int> switch_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> switch_done(false);
  std::thread switcher([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    switch_ret.store(mgr->switch_to_follower_gracefully());
    switch_done.store(true);
  });
  const int64_t block_deadline_us = ObTimeUtility::current_time() + 1_s;
  while (!mgr->is_hotspot_aggregation_blocked()
         && ObTimeUtility::current_time() < block_deadline_us) {
    ob_usleep(1_ms);
  }
  const bool admission_blocked = mgr->is_hotspot_aggregation_blocked();
  ObTxDescGuard ordinary_guard = n1->get_tx_guard();
  ObTxDescGuard rejected_secondary_guard = n1->get_tx_guard();
  ObTxDesc &ordinary_tx = ordinary_guard.get_tx_desc();
  ObTxDesc &rejected_secondary_tx = rejected_secondary_guard.get_tx_desc();
  const int ordinary_start_ret = n1->start_tx(ordinary_tx, tx_param);
  const int rejected_secondary_start_ret =
      n1->start_tx(rejected_secondary_tx, tx_param);
  const int ordinary_write_ret = n1->write(ordinary_tx, snapshot, 9203, 9303);
  const int rejected_secondary_write_ret =
      n1->write(rejected_secondary_tx, snapshot, 9204, 9304);
  ObAggregatedTxIDArray rejected_members;
  EXPECT_EQ(OB_SUCCESS, rejected_members.push_back(rejected_secondary_tx.tx_id_));
  const int rejected_ret = n1->txs_.sync_hotspot_legality_validation(
      participant, ordinary_tx.tx_id_, rejected_members, n1->ts_after_ms(100));
  const uint64_t count_after_reject = mgr->get_hotspot_aggregation_count();
  EXPECT_FALSE(rejected_secondary_tx.get_hotspot_primary_tx_id().is_valid());
  const int ordinary_commit_ret = n1->commit_tx(ordinary_tx, n1->ts_after_ms(500));
  const int rejected_secondary_commit_ret =
      n1->commit_tx(rejected_secondary_tx, n1->ts_after_ms(500));

  ObDebugSyncAction resume_action;
  resume_action.sync_point_ = NOW;
  resume_action.timeout_ = DEBUG_SYNC_TIMEOUT_US;
  resume_action.execute_ = 1;
  resume_action.signal_ = CONTINUE_EVENT;
  const int resume_ret = GDS.set_global_action(false, false, resume_action);
  const int64_t deadline_us = ObTimeUtility::current_time() + 5_s;
  while ((!validation_done.load() || !switch_done.load())
         && ObTimeUtility::current_time() < deadline_us) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    ob_usleep(1_ms);
  }
  validator.join();
  switcher.join();

  EXPECT_EQ(OB_SUCCESS, wait_ret);
  EXPECT_TRUE(admission_blocked);
  EXPECT_EQ(OB_SUCCESS, ordinary_start_ret);
  EXPECT_EQ(OB_SUCCESS, rejected_secondary_start_ret);
  EXPECT_EQ(OB_SUCCESS, ordinary_write_ret);
  EXPECT_EQ(OB_SUCCESS, rejected_secondary_write_ret);
  EXPECT_EQ(OB_SUCCESS, resume_ret);
  EXPECT_EQ(1, count_while_paused);
  EXPECT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, repeated_validation_ret);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_PREPARING, paused_status);
  EXPECT_EQ(0, paused_cache_count);
  EXPECT_EQ(1, count_after_repeated_validation);
  EXPECT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, rejected_ret);
  EXPECT_EQ(1, count_after_reject);
  EXPECT_TRUE(OB_SUCCESS == ordinary_commit_ret || OB_TRANS_COMMITED == ordinary_commit_ret);
  EXPECT_TRUE(OB_SUCCESS == rejected_secondary_commit_ret
              || OB_TRANS_COMMITED == rejected_secondary_commit_ret);
  EXPECT_EQ(OB_SUCCESS, validation_ret.load());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, switch_ret.load());
  EXPECT_FALSE(mgr->is_master());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());

  const int primary_commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  const int secondary_commit_ret = n1->commit_tx(secondary_tx, n1->ts_after_ms(500));
  EXPECT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  EXPECT_TRUE(OB_SUCCESS == secondary_commit_ret || OB_TRANS_COMMITED == secondary_commit_ret);
  RecordProperty("q01_rejected_primary_tx_id", std::to_string(ordinary_tx.tx_id_.get_id()));
  RecordProperty("q01_rejected_secondary_tx_id",
                 std::to_string(rejected_secondary_tx.tx_id_.get_id()));
  RecordProperty("q01_rejected_validation_result", std::to_string(rejected_ret));
  RecordProperty("q01_rejected_primary_commit_result", std::to_string(ordinary_commit_ret));
  RecordProperty("q01_rejected_secondary_commit_result",
                 std::to_string(rejected_secondary_commit_ret));
  EXPECT_EQ(OB_SUCCESS, primary_guard.release());
  EXPECT_EQ(OB_SUCCESS, secondary_guard.release());
  EXPECT_EQ(OB_SUCCESS, ordinary_guard.release());
  EXPECT_EQ(OB_SUCCESS, rejected_secondary_guard.release());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}

// Two independent real groups keep the active aggregation count at two. Releasing
// one completed aggregation must not let the formal role switch pass while the
// other group is still paused at the same admission boundary.
TEST_F(ObTestHotspotTxLeaderSwitch, q01_two_real_groups_release_prepare_one_by_one)
{
  static const char *GROUP_A_REACHED = "hotspot_group_a_admitted";
  static const char *GROUP_A_CONTINUE = "hotspot_group_a_continue";
  static const char *GROUP_B_REACHED = "hotspot_group_b_admitted";
  static const char *GROUP_B_CONTINUE = "hotspot_group_b_continue";
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);
  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;

  ObTxDescGuard p1_guard = n1->get_tx_guard();
  ObTxDescGuard s1_guard = n1->get_tx_guard();
  ObTxDescGuard p2_guard = n1->get_tx_guard();
  ObTxDescGuard s2_guard = n1->get_tx_guard();
  ObTxDesc &p1 = p1_guard.get_tx_desc();
  ObTxDesc &s1 = s1_guard.get_tx_desc();
  ObTxDesc &p2 = p2_guard.get_tx_desc();
  ObTxDesc &s2 = s2_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(p1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(s1, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(p2, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(s2, tx_param));
  ASSERT_EQ(OB_SUCCESS, n1->write(p1, 9211, 9311));
  ASSERT_EQ(OB_SUCCESS, n1->write(s1, 9212, 9312));
  ASSERT_EQ(OB_SUCCESS, n1->write(p2, 9213, 9313));
  ASSERT_EQ(OB_SUCCESS, n1->write(s2, 9214, 9314));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray members1;
  ObAggregatedTxIDArray members2;
  ASSERT_EQ(OB_SUCCESS, members1.push_back(s1.tx_id_));
  ASSERT_EQ(OB_SUCCESS, members2.push_back(s2.tx_id_));
  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));

  const int64_t old_debug_sync_timeout = GCONF.debug_sync_timeout;
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("20s"));
  ObDebugSyncAction reset_action;
  ASSERT_EQ(OB_SUCCESS, GDS.set_global_action(true, false, reset_action));
  NAMED_DEFER(two_group_sync_cleanup,
              (void)GDS.set_global_action(true, false, reset_action);
              GCONF.debug_sync_timeout = old_debug_sync_timeout);
  auto install_admission_pause = [&](const char *reached, const char *resume) {
    int ret = OB_SUCCESS;
    ObDebugSyncAction action;
    action.sync_point_ = AFTER_HOTSPOT_AGGREGATION_ADMISSION;
    action.timeout_ = 15_s;
    action.execute_ = 1;
    if (OB_FAIL(action.signal_.assign(ObString::make_string(reached)))) {
    } else if (OB_FAIL(action.wait_.assign(ObString::make_string(resume)))) {
    } else {
      ret = GDS.set_global_action(false, false, action);
    }
    return ret;
  };
  auto wait_event = [&](const char *event) {
    int ret = OB_SUCCESS;
    ObDebugSyncAction action;
    action.sync_point_ = NOW;
    action.timeout_ = 15_s;
    action.execute_ = 1;
    if (OB_FAIL(action.wait_.assign(ObString::make_string(event)))) {
    } else {
      ret = GDS.set_global_action(false, false, action);
    }
    return ret;
  };
  auto signal_event = [&](const char *event) {
    int ret = OB_SUCCESS;
    ObDebugSyncAction action;
    action.sync_point_ = NOW;
    action.timeout_ = 15_s;
    action.execute_ = 1;
    if (OB_FAIL(action.signal_.assign(ObString::make_string(event)))) {
    } else {
      ret = GDS.set_global_action(false, false, action);
    }
    return ret;
  };

  ASSERT_EQ(OB_SUCCESS, install_admission_pause(GROUP_A_REACHED, GROUP_A_CONTINUE));
  std::atomic<int> group_a_ret(OB_ERR_UNEXPECTED);
  std::atomic<int> group_b_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> group_a_done(false);
  std::atomic<bool> group_b_done(false);
  std::thread group_a([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    group_a_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, p1.tx_id_, members1, n1->ts_after_ms(5000)));
    group_a_done.store(true);
  });
  const int wait_a_ret = wait_event(GROUP_A_REACHED);
  const int install_b_ret = install_admission_pause(GROUP_B_REACHED, GROUP_B_CONTINUE);
  std::thread group_b([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    group_b_ret.store(n1->txs_.sync_hotspot_legality_validation(
        participant, p2.tx_id_, members2, n1->ts_after_ms(5000)));
    group_b_done.store(true);
  });
  const int wait_b_ret = wait_event(GROUP_B_REACHED);
  const uint64_t count_two = mgr->get_hotspot_aggregation_count();

  set_errsim_retry_timeout(2_s);
  std::atomic<int> switch_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> switch_done(false);
  std::thread switcher([&]() {
    ObTenantEnv::set_tenant(&n1->tenant_);
    switch_ret.store(mgr->switch_to_follower_gracefully());
    switch_done.store(true);
  });
  const int64_t blocked_deadline = ObTimeUtility::current_monotonic_time() + 1_s;
  while (!mgr->is_hotspot_aggregation_blocked()
         && ObTimeUtility::current_monotonic_time() < blocked_deadline) {
    ob_usleep(1_ms);
  }
  const int64_t release_a_mts = ObTimeUtility::current_monotonic_time();
  const int resume_a_ret = signal_event(GROUP_A_CONTINUE);
  const int64_t group_a_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while ((!group_a_done.load() || 1 != mgr->get_hotspot_aggregation_count())
         && ObTimeUtility::current_monotonic_time() < group_a_deadline) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    ob_usleep(1_ms);
  }
  const uint64_t count_one = mgr->get_hotspot_aggregation_count();
  const bool switch_done_after_a = switch_done.load();
  const int64_t release_b_mts = ObTimeUtility::current_monotonic_time();
  const int resume_b_ret = signal_event(GROUP_B_CONTINUE);
  const int64_t all_done_deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while ((!group_b_done.load() || !switch_done.load())
         && ObTimeUtility::current_monotonic_time() < all_done_deadline) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    ob_usleep(1_ms);
  }
  group_a.join();
  group_b.join();
  switcher.join();

  EXPECT_EQ(OB_SUCCESS, wait_a_ret);
  EXPECT_EQ(OB_SUCCESS, install_b_ret);
  EXPECT_EQ(OB_SUCCESS, wait_b_ret);
  EXPECT_EQ(2, count_two);
  EXPECT_EQ(OB_SUCCESS, resume_a_ret);
  EXPECT_EQ(1, count_one);
  EXPECT_FALSE(switch_done_after_a);
  EXPECT_EQ(OB_SUCCESS, resume_b_ret);
  EXPECT_EQ(OB_SUCCESS, group_a_ret.load());
  EXPECT_EQ(OB_SUCCESS, group_b_ret.load());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
  EXPECT_EQ(OB_SUCCESS, switch_ret.load());
  EXPECT_FALSE(mgr->is_master());

  RecordProperty("q01_two_groups_ls", std::to_string(n1->ls_id_.id()));
  RecordProperty("q01_group_a_primary", std::to_string(p1.tx_id_.get_id()));
  RecordProperty("q01_group_b_primary", std::to_string(p2.tx_id_.get_id()));
  RecordProperty("q01_count_before_release", std::to_string(count_two));
  RecordProperty("q01_count_after_first_release", std::to_string(count_one));
  RecordProperty("q01_release_a_mts", std::to_string(release_a_mts));
  RecordProperty("q01_release_b_mts", std::to_string(release_b_mts));

  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(p1, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(s1, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(p2, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, n1->commit_tx(s2, n1->ts_after_ms(5000)));
  EXPECT_EQ(OB_SUCCESS, p1_guard.release());
  EXPECT_EQ(OB_SUCCESS, s1_guard.release());
  EXPECT_EQ(OB_SUCCESS, p2_guard.release());
  EXPECT_EQ(OB_SUCCESS, s2_guard.release());
  EXPECT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr));
}
#endif

TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_leader_switch_force_revoke_primary_delay_abort_needs_commit_to_finish)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 200 + i, 300 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 1000, 2000));
  const ObTransID primary_tx_id = primary_tx.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  // Flush once before role switch to ensure part of redo has been submitted.
  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();

  bool has_partial_redo = false;
  auto check_has_redo = [&](const ObTransID &tx_id) {
    ObPartTransCtx *part_ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->get_tx_ctx(tx_id, false /*for_replay*/, part_ctx));
    ASSERT_TRUE(part_ctx != nullptr);
    if (part_ctx->exec_info_.redo_lsns_.count() > 0) {
      has_partial_redo = true;
    }
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(part_ctx));
  };
  check_has_redo(primary_tx.tx_id_);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    check_has_redo(secondary_tx_ids[i]);
  }
  ASSERT_TRUE(has_partial_redo);

  // Aggregation completion is now sufficient for graceful revoke, so a
  // graceful pre-failure is no longer a valid prerequisite for this case.
  // Revoke explicitly to keep testing delayed abort and commit-driven cleanup.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr->is_master());
  // Trigger one more takeover path to drain role-change related async callbacks.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  for (int i = 0; i < 300; i++) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }

  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      // do nothing
    } else {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      {
        CtxLockGuard guard(primary_part_ctx->lock_);
        primary_waiting_delay_abort =
            primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      }
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  // After takeover, primary tx ctx enters delayed rollback. Commit triggers the real rollback
  // and drives the ctx into exiting.
  TRANS_LOG(INFO, "[hotspot leader switch test] before commit_tx",
            K(primary_tx_id), K(primary_tx), "phase", "before_commit_tx");
  const int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  TRANS_LOG(INFO, "[hotspot leader switch test] after commit_tx",
            K(primary_tx_id), K(commit_ret), K(primary_tx), "phase", "after_commit_tx");
  ASSERT_EQ(OB_TRANS_ROLLBACKED, commit_ret);
  n1->wait_all_msg_consumed();
  bool primary_ctx_exiting_after_commit = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      primary_ctx_exiting_after_commit = true;
      break;
    } else {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_ctx_exiting_after_commit = primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(primary_part_ctx));
      if (primary_ctx_exiting_after_commit) {
        break;
      }
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_ctx_exiting_after_commit);
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
  n1->wait_all_msg_consumed();

  auto wait_tx_ctx_exiting = [&](const ObTransID &tx_id) {
    ObPartTransCtx *part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(tx_id, part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      return;
    }
    ASSERT_EQ(OB_SUCCESS, get_ret);
    ASSERT_TRUE(part_ctx != nullptr);
    int i = 0;
    while (!part_ctx->is_exiting_ && i++ < 10000) {
      n1->wait_all_msg_consumed();
      n1->wait_all_redolog_applied();
      usleep(1000);
    }
    ASSERT_TRUE(part_ctx->is_exiting_) << "tx ctx not exiting, tx_id=" << tx_id;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(part_ctx));
  };
  wait_tx_ctx_exiting(primary_tx_id);
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    wait_tx_ctx_exiting(secondary_tx_ids[i]);
  }

  int wait_ret = OB_SUCCESS;
  for (int i = 0; i < 60; i++) {
    wait_ret = n1->wait_all_tx_ctx_is_destoryed();
    if (OB_SUCCESS == wait_ret) {
      break;
    }
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(100 * 1000);
  }
  ASSERT_EQ(OB_SUCCESS, wait_ret);
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
}

TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_force_revoke_new_leader_abort_log_replay_cleans_old_leader_aggregated_ctx)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 400 + i, 500 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 3000, 4000));
  const ObTransID primary_tx_id = primary_tx.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();
  sec_txs[0]->set_hotspot_group_commit_info(primary_tx_id, false);
  ASSERT_EQ(primary_tx_id, sec_txs[0]->get_hotspot_primary_tx_id());

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Model a lost early abort notification so S must converge by querying the
  // real P ABORT result after the forced handoff.
  n1->add_drop_msg_type(KEEPALIVE);
  DEFER(n1->del_drop_msg_type(KEEPALIVE));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());

  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST != get_ret) {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_waiting_delay_abort =
          primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  ObPartTransCtx *new_leader_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->get_tx_ctx(primary_tx_id, false /*for_replay*/, new_leader_primary_ctx));
  ASSERT_TRUE(new_leader_primary_ctx != nullptr);
  {
    CtxLockGuard guard(new_leader_primary_ctx->lock_);
    ASSERT_EQ(OB_SUCCESS, new_leader_primary_ctx->abort_(OB_TRANS_ROLLBACKED));
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(new_leader_primary_ctx));
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary_tx, OB_TRANS_CTX_NOT_EXIST));
  ASSERT_EQ(primary_tx_id, sec_txs[0]->get_hotspot_primary_tx_id());
  ASSERT_FALSE(sec_txs[0]->flags_.PART_ABORTED_);
  const int secondary_retry_ret =
      n1->commit_tx(*sec_txs[0], n1->ts_after_ms(500));
  ASSERT_EQ(OB_TRANS_KILLED, secondary_retry_ret);
  ASSERT_EQ(ObTxDesc::State::ROLLED_BACK, sec_txs[0]->state_);
  ASSERT_EQ(OB_TRANS_KILLED, sec_txs[0]->commit_out_);
  ASSERT_FALSE(sec_txs[0]->commit_version_.is_valid());
  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*sec_txs[1], OB_TRANS_CTX_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());

  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_graceful_switch_commit_on_new_leader_releases_ctx_on_both_replicas)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  ReplayLogEntryFunctor replay_initial(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_initial));

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    sec_txs[i]->set_hotspot_group_commit_info(primary_tx.tx_id_, false);
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 600 + i, 700 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 5000, 6000));

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));
  ObPartTransCtx *primary_part_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr1->get_tx_ctx(primary_tx.tx_id_, false, primary_part_ctx));
  ASSERT_NE(nullptr, primary_part_ctx);
  EXPECT_EQ(TxRedoFlushStatus::PRIMARY_AGGR_SUCCEEDED,
            primary_part_ctx->redo_flush_status_);
  EXPECT_TRUE(primary_part_ctx->hotspot_redo_cache_.all_redo_flushed());
  EXPECT_TRUE(primary_part_ctx->hotspot_redo_cache_.all_redo_synced());
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(primary_part_ctx));
  ASSERT_EQ(0, ls_tx_ctx_mgr1->get_hotspot_aggregation_count());

  // Graceful switch happens only after primary/secondary hotspot redo migration is finished.
  // Do not commit/abort the transaction before role switch.
  ObTenantEnv::set_tenant(&n1->tenant_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_gracefully());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ObTenantEnv::set_tenant(&n2->tenant_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());
  ASSERT_TRUE(ls_tx_ctx_mgr2->in_leader_serving_state());

  const int primary_commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(5000));
  ASSERT_TRUE(OB_SUCCESS == primary_commit_ret || OB_TRANS_COMMITED == primary_commit_ret);
  EXPECT_EQ(ObTxDesc::State::COMMITTED, primary_tx.state_);
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    const int commit_ret = n1->commit_tx(*sec_txs[i], n1->ts_after_ms(5000));
    ASSERT_TRUE(commit_ret == OB_SUCCESS || commit_ret == OB_TRANS_COMMITED)
        << "secondary tx commit failed, tx_id=" << secondary_tx_ids[i]
        << ", commit_ret=" << commit_ret;
    EXPECT_EQ(ObTxDesc::State::COMMITTED, sec_txs[i]->state_);
  }

  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());

  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());

  ObTenantEnv::set_tenant(&n2->tenant_);
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ObTenantEnv::set_tenant(&n1->tenant_);
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

// Verify that after force revoke, replay of abort log on old leader can
// clean up hotspot_redo_cache even when log CBs are stranded in free list.
// A modeled busy CB first defers abort replay; after it returns to the free
// list, replaying that same log must release the CBs and let the ctx exit.
TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_force_revoke_replay_abort_releases_free_cbs)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  // Use more secondary txs to increase the chance of CBs in free list
  const int SECONDARY_TX_COUNT = 3;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guards[SECONDARY_TX_COUNT] = {
      n1->get_tx_guard(), n1->get_tx_guard(), n1->get_tx_guard()};
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT];
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    sec_txs[i] = &sec_guards[i].get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 800 + i, 900 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 7000, 8000));
  const ObTransID primary_tx_id = primary_tx.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  // Flush redo so CBs are allocated
  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Force revoke on n1. This may leave CBs in free list if submit fails mid-way.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());
  ASSERT_GT(ls_tx_ctx_mgr1->get_tx_ctx_count(), 0);

  // Replay on n2 to bring it up to date
  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());

  // Wait for new leader to pick up primary tx
  bool primary_waiting_delay_abort = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *primary_part_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(primary_tx_id, primary_part_ctx);
    if (OB_TRANS_CTX_NOT_EXIST != get_ret) {
      ASSERT_EQ(OB_SUCCESS, get_ret);
      ASSERT_TRUE(primary_part_ctx != nullptr);
      primary_waiting_delay_abort =
          primary_part_ctx->sub_state_.is_force_abort() && !primary_part_ctx->is_exiting_;
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(primary_part_ctx));
      if (primary_waiting_delay_abort) {
        break;
      }
    }
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(primary_waiting_delay_abort);

  // New leader aborts primary tx -> writes abort log
  ObPartTransCtx *new_leader_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr2->get_tx_ctx(primary_tx_id, false, new_leader_primary_ctx));
  ASSERT_TRUE(new_leader_primary_ctx != nullptr);
  {
    CtxLockGuard guard(new_leader_primary_ctx->lock_);
    ASSERT_EQ(OB_SUCCESS, new_leader_primary_ctx->abort_(OB_TRANS_ROLLBACKED));
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(new_leader_primary_ctx));
  n2->wait_all_msg_consumed();
  n2->wait_all_redolog_applied();

  // Inject free CBs into old leader's primary ctx to simulate the scenario
  // where hotspot log submit returned OB_NOT_MASTER during leader switch,
  // leaving CBs stranded in free_hotspot_cbs_ with no clog callback.
  // In production this happens when submit_log_block_out_() fails mid-way
  // during switch_to_follower_forcedly.
  ObTxLogCb *pending_cb = nullptr;
  {
    ObPartTransCtx *n1_primary_ctx = nullptr;
    int get_ret = ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(primary_tx_id, n1_primary_ctx);
    ASSERT_EQ(OB_SUCCESS, get_ret);
    ASSERT_TRUE(n1_primary_ctx != nullptr);

    // Allocate CBs from the tx's log cb pool and place them into free list.
    // This mimics the state after on_failure() moves a CB out of busy list
    // but submit had no clog callback to eventually release it.
    ObTxHotspotRedoCacheHandle &cache = n1_primary_ctx->hotspot_redo_cache_;
    {
      SpinWLockGuard guard(cache.hotspot_lock_);
      ASSERT_TRUE(cache.cache_ != nullptr);
      const int INJECT_FREE_CB_COUNT = 2;
      for (int i = 0; i < INJECT_FREE_CB_COUNT; i++) {
        ObTxLogCb *cb = nullptr;
        int alloc_ret = n1_primary_ctx->prepare_log_cb_(false /*need_final_cb*/, cb);
        if (OB_SUCCESS == alloc_ret && cb != nullptr) {
          cb->reuse();
          cache.cache_->free_hotspot_cbs_.add_last(cb);
          cache.cache_->all_log_cb_cnt_++;
        }
      }
      // Model one callback still owned by the pipeline at the first abort
      // replay. It is returned below between the two replay attempts.
      ASSERT_EQ(OB_SUCCESS,
                n1_primary_ctx->prepare_log_cb_(false /*need_final_cb*/, pending_cb));
      ASSERT_TRUE(nullptr != pending_cb);
      pending_cb->reuse();
      cache.cache_->busy_hotspot_cbs_.add_last(pending_cb);
      ++cache.cache_->all_log_cb_cnt_;
      TRANS_LOG(INFO, "[test] injected free CBs into old leader hotspot_redo_cache",
                K(primary_tx_id),
                K(cache.cache_->free_hotspot_cbs_.get_size()),
                K(cache.cache_->busy_hotspot_cbs_.get_size()),
                K(cache.cache_->idle_hotspot_cbs_.get_size()),
                K(cache.cache_->all_log_cb_cnt_));
      ASSERT_GT(cache.cache_->free_hotspot_cbs_.get_size(), 0);
    }
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(n1_primary_ctx));
  }

  // Cleanup must defer replay completion while the modeled callback is busy,
  // keeping both the ctx map entry and writable tx-data for the retry.
  ObPartTransCtx *old_primary_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(primary_tx_id, old_primary_ctx));
  NAMED_DEFER(revert_abort_retry_ctx,
              if (nullptr != old_primary_ctx) {
                (void)ls_tx_ctx_mgr1->revert_tx_ctx(old_primary_ctx);
              });
  // The fake adapter consumes failed entries instead of retrying them. Save
  // the failed log bytes here to exercise a real second replay of that entry.
  PendingAbortReplayFunctor pending_replay(n1);
  ReplayLogEntryFunctor replay_old_leader(n1);
  ASSERT_EQ(OB_EAGAIN, n1->fake_tx_log_adapter_->replay_all(pending_replay));
  ASSERT_FALSE(pending_replay.log_.empty());
  EXPECT_FALSE(old_primary_ctx->is_exiting_);
  EXPECT_FALSE(old_primary_ctx->ctx_tx_data_.is_read_only());
  ObPartTransCtx *retry_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr1->get_tx_ctx_directly_from_hash_map(primary_tx_id, retry_ctx));
  EXPECT_EQ(old_primary_ctx, retry_ctx);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(retry_ctx));
  {
    // Model the pending callback returning to the free list after failure.
    ObTxHotspotRedoCacheHandle &cache = old_primary_ctx->hotspot_redo_cache_;
    SpinWLockGuard guard(cache.hotspot_lock_);
    ASSERT_EQ(1, cache.cache_->busy_hotspot_cbs_.get_size());
    cache.cache_->busy_hotspot_cbs_.remove(pending_cb);
    cache.cache_->free_hotspot_cbs_.add_last(pending_cb);
  }
  // Replay retries the same abort. Free CBs can now be released before exit.
  ASSERT_EQ(OB_SUCCESS,
            n1->replay(pending_replay.log_.data(), pending_replay.log_.size(),
                       pending_replay.lsn_, pending_replay.timestamp_));
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_old_leader));
  EXPECT_TRUE(old_primary_ctx->is_exiting_);
  EXPECT_EQ(0, old_primary_ctx->hotspot_redo_cache_.get_hotspot_cache_count());
  EXPECT_EQ(0, ls_tx_ctx_mgr1->get_hotspot_aggregation_count());
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(old_primary_ctx));
  old_primary_ctx = nullptr;
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Both nodes must destroy all tx ctxs. Before fix, n1 would hang here
  // because clean_hotspot_redo_cache() failed and ctx never exited.
  ASSERT_EQ(OB_SUCCESS, n1->wait_all_tx_ctx_is_destoryed());
  ASSERT_EQ(OB_SUCCESS, n2->wait_all_tx_ctx_is_destoryed());

  ASSERT_EQ(OB_SUCCESS, n1->abort_tx(primary_tx, OB_TRANS_CTX_NOT_EXIST));
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->abort_tx(*sec_txs[i], OB_TRANS_CTX_NOT_EXIST));
  }
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, sec_guards[i].release());
  }

  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

// ============================================================================
// Leader switch during aggregation: primary enters AGGR_FAILED terminal state,
// secondaries are aborted, and all ctxs are eventually cleaned up.
// ============================================================================
TEST_F(ObTestHotspotTxLeaderSwitch,
       hotspot_leader_switch_during_aggregation_aborts_and_cleans_up)
{
  ObTxNode::reset_localtion_adapter();
  START_ONE_TX_NODE(n1);

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;

  ObTxDescGuard primary_guard = n1->get_tx_guard();
  ObTxDesc &primary_tx = primary_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(primary_tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(primary_tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));

  const int SECONDARY_TX_COUNT = 2;
  ObTransID secondary_tx_ids[SECONDARY_TX_COUNT];
  ObTxDescGuard sec_guard0 = n1->get_tx_guard();
  ObTxDescGuard sec_guard1 = n1->get_tx_guard();
  ObTxDesc *sec_txs[SECONDARY_TX_COUNT] = {&sec_guard0.get_tx_desc(), &sec_guard1.get_tx_desc()};

  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(*sec_txs[i], tx_param));
    secondary_tx_ids[i] = sec_txs[i]->tx_id_;
    ASSERT_EQ(OB_SUCCESS, n1->write(*sec_txs[i], snapshot, 5000 + i, 6000 + i));
  }
  ASSERT_EQ(OB_SUCCESS, n1->write(primary_tx, snapshot, 50000, 60000));

  const ObTransID primary_tx_id = primary_tx.tx_id_;

  ObTxPart participant;
  participant.id_ = n1->ls_id_;
  participant.addr_ = n1->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray aggre_members;
  for (int i = 0; i < SECONDARY_TX_COUNT; i++) {
    ASSERT_EQ(OB_SUCCESS, aggre_members.push_back(secondary_tx_ids[i]));
  }

  // Start aggregation
  ASSERT_EQ(OB_SUCCESS,
            n1->txs_.sync_hotspot_legality_validation(participant, primary_tx.tx_id_, aggre_members));
  n1->wait_all_msg_consumed();

  // Force leader switch while aggregation is active
  ObLSTxCtxMgr *ls_tx_ctx_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr));

  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_follower_forcedly());
  ASSERT_FALSE(ls_tx_ctx_mgr->is_master());

  // Wait for async callbacks to drain
  for (int i = 0; i < 200; i++) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }

  // Switch back to leader so ctx cleanup can proceed
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->switch_to_leader());
  for (int i = 0; i < 200; i++) {
    n1->wait_all_msg_consumed();
    n1->wait_all_redolog_applied();
    usleep(1000);
  }

  // Commit should fail (rolled back due to leader switch)
  const int commit_ret = n1->commit_tx(primary_tx, n1->ts_after_ms(500));
  ASSERT_EQ(OB_TRANS_ROLLBACKED, commit_ret)
      << "Primary tx should be rolled back after leader switch during aggregation";

  n1->wait_all_msg_consumed();

  // Wait for primary ctx to be cleaned up
  bool primary_cleaned = false;
  for (int i = 0; i < 300; i++) {
    ObPartTransCtx *pctx = nullptr;
    int get_ret = ls_tx_ctx_mgr->get_tx_ctx_directly_from_hash_map(primary_tx_id, pctx);
    if (OB_TRANS_CTX_NOT_EXIST == get_ret) {
      primary_cleaned = true;
      break;
    }
    if (OB_SUCCESS == get_ret && pctx != nullptr) {
      if (pctx->is_exiting_) {
        primary_cleaned = true;
        ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(pctx));
        break;
      }
      ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->revert_tx_ctx(pctx));
    }
    n1->wait_all_msg_consumed();
    usleep(1000);
  }
  ASSERT_TRUE(primary_cleaned) << "Primary ctx should be cleaned up after leader switch + rollback";

  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, primary_guard.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard0.release());
  ASSERT_EQ(OB_SUCCESS, sec_guard1.release());
  n1->wait_all_msg_consumed();
}

// ============================================================================
// Regression test: replay ctx must not pass hotspot admission after leader switch.
//
// Root cause: after parallel redo replay (since 4.2.4), redo_lsns_ is not
// maintained and redo_flush_status_ stays NORMAL_START. A replayed transaction
// that has already written redo passes the old admission check
// (redo_lsns_.count() == 0 && NORMAL_START) and is erroneously aggregated as
// a hotspot secondary, causing the primary to stall in PRIMARY_COLLECTING.
//
// Fix: add prev_record_lsn_.is_valid() to the admission check. After graceful
// leader switch, submit_redo_active_info_log_() always submits a RecordLog
// before ActiveInfoLog when redo_lsns_ is non-empty, so prev_record_lsn_ is
// guaranteed to be set on the new leader after replay.
//
// Test flow:
//   1. n1 (leader): create a tx, write redo, flush redo.
//      Do NOT start aggregation (otherwise graceful switch would wait/fail).
//   2. Graceful switch: n1 -> follower. ActiveInfoLog persisted.
//   3. n2 (new leader): replay all logs, switch_to_leader.
//   4. On n2: try to use the replayed tx as hotspot secondary.
//      hotspot_legality_validation_() must reject it.
// ============================================================================
TEST_F(ObTestHotspotTxLeaderSwitch,
       replay_ctx_rejected_by_hotspot_admission_after_leader_switch)
{
  ObTxNode::reset_localtion_adapter();
  auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_);
  auto n2 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_);
  DEFER(delete(n1));
  DEFER(delete(n2));
  ASSERT_EQ(OB_SUCCESS, n1->start());
  n2->set_as_follower_replica(*n1);
  ASSERT_EQ(OB_SUCCESS, n2->start());
  // Fake replay drains hash buckets rather than replaying globally by LSN.
  // Consume the initialization START_WORKING barrier before adding tx logs so
  // a later replay cannot clear the transaction's recovered data_complete bit.
  ReplayLogEntryFunctor replay_initial(n2);
  ASSERT_EQ(OB_SUCCESS, n1->fake_tx_log_adapter_->replay_all(replay_initial));

  PREPARE_TX_PARAM(tx_param);
  tx_param.timeout_us_ = 1000 * 1000 * 1000;

  // ---- Phase 1: n1 (old leader) creates a tx and writes redo ----
  // Do NOT start hotspot aggregation -- if aggregation is active,
  // graceful switch will wait/fail (wait_for_primary_aggregation_gracefully_).
  ObTxDescGuard tx_guard = n1->get_tx_guard();
  ObTxDesc &tx = tx_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(tx, tx_param));

  ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS,
            n1->get_read_snapshot(tx, tx_param.isolation_, n1->ts_after_ms(100), snapshot));
  ASSERT_EQ(OB_SUCCESS, n1->write(tx, snapshot, 100, 200));
  const ObTransID tx_id = tx.tx_id_;

  ObLSTxCtxMgr *ls_tx_ctx_mgr1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, ls_tx_ctx_mgr1));

  // Flush redo to ensure logs are persisted.
  // after_submit_log_() will push LSN to redo_lsns_.
  ObTransID fail_tx_id;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->traverse_tx_to_submit_redo_log(fail_tx_id, UINT32_MAX));
  n1->wait_all_msg_consumed();
  n1->wait_all_redolog_applied();

  // Record state before switch
  {
    ObPartTransCtx *ctx = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->get_tx_ctx(tx_id, false, ctx));
    ASSERT_TRUE(ctx != nullptr);
    TRANS_LOG(INFO, "[test] n1 tx ctx before switch",
              K(tx_id),
              "redo_lsns_count", ctx->exec_info_.redo_lsns_.count(),
              "prev_record_lsn", ctx->exec_info_.prev_record_lsn_,
              "redo_flush_status", to_cstr(ctx->redo_flush_status_));
    // redo_lsns_ should be non-empty after redo flush
    ASSERT_GT(ctx->exec_info_.redo_lsns_.count(), 0);
    ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->revert_tx_ctx(ctx));
  }

  // ---- Phase 2: Graceful switch n1 -> follower ----
  // Since no aggregation is active, this should succeed.
  // submit_redo_active_info_log_() will submit a RecordLog first
  // (because redo_lsns_ is non-empty), then ActiveInfoLog.
  // After RecordLog: redo_lsns_ cleared, prev_record_lsn_ set.
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr1->switch_to_follower_gracefully());
  ASSERT_FALSE(ls_tx_ctx_mgr1->is_master());

  // ---- Phase 3: n2 replays and becomes leader ----
  ReplayLogEntryFunctor replay_new_leader(n2);
  ASSERT_EQ(OB_SUCCESS, n2->fake_tx_log_adapter_->replay_all(replay_new_leader));

  ObLSTxCtxMgr *ls_tx_ctx_mgr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n2->ls_id_, ls_tx_ctx_mgr2));
  ObPartTransCtx *pre_takeover_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(tx_id, pre_takeover_ctx));
  ASSERT_NE(nullptr, pre_takeover_ctx);
  {
    CtxLockGuard guard(pre_takeover_ctx->lock_);
    ASSERT_TRUE(pre_takeover_ctx->exec_info_.data_complete_);
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(pre_takeover_ctx));
  ObTxNode::get_location_adapter_().update_localtion(n2->ls_id_, n2->addr_);
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->switch_to_leader());
  n2->wait_all_redolog_applied();
  ASSERT_TRUE(ls_tx_ctx_mgr2->is_master());

  // ---- Phase 4: Verify replay ctx is rejected by hotspot admission ----
  // Wait for tx ctx to appear on n2
  ObPartTransCtx *n2_ctx = nullptr;
  for (int i = 0; i < 300; i++) {
    int get_ret = ls_tx_ctx_mgr2->get_tx_ctx_directly_from_hash_map(tx_id, n2_ctx);
    if (OB_SUCCESS == get_ret && n2_ctx != nullptr) {
      break;
    }
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }
  ASSERT_TRUE(n2_ctx != nullptr) << "tx ctx should exist on new leader after replay";

  // Verify replay ctx state and admission check
  int validation_ret = OB_SUCCESS;
  {
    CtxLockGuard guard(n2_ctx->lock_);
    TRANS_LOG(INFO, "[test] n2 tx ctx after replay",
              K(tx_id),
              "redo_lsns_count", n2_ctx->exec_info_.redo_lsns_.count(),
              "prev_record_lsn", n2_ctx->exec_info_.prev_record_lsn_,
              "redo_flush_status", to_cstr(n2_ctx->redo_flush_status_),
              "ctx_source", n2_ctx->ctx_source_);

    // Precondition assertions: this ctx must present exactly the state the old
    // admission predicate saw (NORMAL_START + empty redo_lsns_), so a regression
    // of the fix would deterministically re-admit it.
    ASSERT_EQ(TxRedoFlushStatus::NORMAL_START, n2_ctx->redo_flush_status_);
    ASSERT_EQ(0, n2_ctx->exec_info_.redo_lsns_.count());
    ASSERT_EQ(OB_SUCCESS, n2_ctx->check_status_());

    // After graceful switch, RecordLog was submitted before ActiveInfoLog,
    // so prev_record_lsn_ is valid after replay even though redo_lsns_ is
    // empty (parallel replay does not maintain it).
    ASSERT_TRUE(n2_ctx->exec_info_.prev_record_lsn_.is_valid())
        << "prev_record_lsn_ must be set after replaying the switch RecordLog";

    // Call hotspot_legality_validation_() -- must reject this ctx.
    // Before fix: passes (redo_lsns_ empty, NORMAL_START) -> wrong!
    // After fix: rejected by the prev_record_lsn_.is_valid() predicate.
    validation_ret = n2_ctx->hotspot_legality_validation_(false /*is_primary*/);
    TRANS_LOG(INFO, "[test] hotspot_legality_validation_ result",
              K(tx_id), K(validation_ret),
              "redo_flush_status", to_cstr(n2_ctx->redo_flush_status_));
  }
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_mgr2->revert_tx_ctx(n2_ctx));

  // Must be rejected specifically by the new redo-history predicate.
  ASSERT_EQ(OB_TX_NOT_SUPPORT_AGGREGATION, validation_ret)
      << "replay ctx with redo history must be rejected by the "
      << "prev_record_lsn_ admission predicate, got ret=" << validation_ret;

  // ---- Cleanup ----
  // Drain in-flight async callbacks. The replayed ctx is not force-aborted
  // here; it is released when the tx guard and tx node are destroyed during
  // test teardown.
  for (int i = 0; i < 300; i++) {
    n2->wait_all_msg_consumed();
    n2->wait_all_redolog_applied();
    usleep(1000);
  }

  ASSERT_EQ(OB_SUCCESS, tx_guard.release());
  ASSERT_EQ(OB_SUCCESS, n2->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr2));
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr1));
}

// Response-cleanup fixtures below own callbacks before nodes, and release all
// descriptor/context guards before destroying either tenant. Assertion failure
// therefore cannot leave a callback or a worker referring to a dead stack frame.
class ResponseCleanupDebugSync
{
public:
  ResponseCleanupDebugSync() : saved_timeout_(GCONF.debug_sync_timeout) {}
  ~ResponseCleanupDebugSync()
  {
    (void)resume("resp_cleanup_worker_continue");
    (void)resume();
    ObDebugSyncAction reset;
    (void)GDS.set_global_action(true, false, reset);
    GCONF.debug_sync_timeout = saved_timeout_;
  }
  int init()
  {
    ObDebugSyncAction reset;
    return GCONF.debug_sync_timeout.set_value("30s")
        ? GDS.set_global_action(true, false, reset) : OB_ERR_UNEXPECTED;
  }
  int pause(const int point, const char *signal = "response_cleanup_paused",
            const char *resume = "response_cleanup_continue")
  {
    ObDebugSyncAction action;
    action.sync_point_ = static_cast<ObDebugSyncPoint>(point);
    action.timeout_ = 20_s;
    action.execute_ = 1;
    action.signal_ = signal;
    action.wait_ = resume;
    return GDS.set_global_action(false, false, action);
  }
  int wait(const char *event = "response_cleanup_paused")
  {
    ObDebugSyncAction action;
    action.sync_point_ = NOW;
    action.timeout_ = 20_s;
    action.execute_ = 1;
    action.wait_ = event;
    return GDS.set_global_action(false, false, action);
  }
  int resume(const char *event = "response_cleanup_continue")
  {
    ObDebugSyncAction action;
    action.sync_point_ = NOW;
    action.timeout_ = 20_s;
    action.execute_ = 1;
    action.signal_ = event;
    return GDS.set_global_action(false, false, action);
  }
private:
  int64_t saved_timeout_;
};

template <typename Predicate>
static bool response_cleanup_wait(Predicate predicate)
{
  const int64_t deadline = ObTimeUtility::current_monotonic_time() + 5_s;
  while (!predicate() && ObTimeUtility::current_monotonic_time() < deadline) {
    ob_usleep(1_ms);
  }
  return predicate();
}

class ResponseCleanupFixture
{
public:
  explicit ResponseCleanupFixture(MsgBus &bus)
      : saved_self_(GCONF.self_addr_), bus_(bus), mgr_(nullptr), new_mgr_(nullptr),
        primary_ctx_(nullptr), secondary_ctx_{nullptr, nullptr, nullptr, nullptr}, count_(0) {}
  ~ResponseCleanupFixture()
  {
    if (node_) {
      ObTenantEnv::set_tenant(&node_->tenant_);
      for (int i = 0; i < count_; ++i) { secondary_guard_[i].reset(); }
      primary_guard_.reset();
      for (int i = 0; i < count_; ++i) {
        if (nullptr != secondary_ctx_[i]) { (void)mgr_->revert_tx_ctx(secondary_ctx_[i]); }
      }
      if (nullptr != primary_ctx_) { (void)mgr_->revert_tx_ctx(primary_ctx_); }
      if (nullptr != mgr_) { (void)node_->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr_); }
    }
    if (new_node_) {
      ObTenantEnv::set_tenant(&new_node_->tenant_);
      if (nullptr != new_mgr_) {
        (void)new_node_->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(new_mgr_);
      }
      new_node_.reset(); // Its fake log adapter is borrowed from node_.
    }
    node_.reset();
    GCONF.self_addr_ = saved_self_;
  }
  int init(const int count = 1, const bool with_follower = false)
  {
    int ret = OB_SUCCESS;
    ObTxNode::reset_localtion_adapter();
    node_.reset(new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus_));
    OZ(node_->start());
    if (OB_SUCC(ret) && with_follower) {
      new_node_.reset(new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.2", 8888), bus_));
      new_node_->set_as_follower_replica(*node_);
      OZ(new_node_->start());
      // Consume the initial START_WORKING barrier before creating P/S. The fake
      // adapter replays hash buckets, not one global LSN-ordered stream.
      ReplayLogEntryFunctor replay_initial(new_node_.get());
      OZ(node_->fake_tx_log_adapter_->replay_all(replay_initial));
    }
    GCONF.self_addr_ = node_->addr_;
    // No keepalive delivery can be responsible for the asserted old-S cleanup.
    node_->add_drop_msg_type(KEEPALIVE);
    PREPARE_TX_PARAM(param);
    param.timeout_us_ = 1000 * 1000 * 1000;
    if (OB_SUCC(ret)) {
      primary_guard_.reset(new ObTxDescGuard(node_->get_tx_guard()));
      OZ(node_->start_tx(primary(), param));
      OZ(node_->write(primary(), 14000, 15000));
    }
    ObAggregatedTxIDArray members;
    for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
      secondary_guard_[i].reset(new ObTxDescGuard(node_->get_tx_guard()));
      ++count_;
      OZ(node_->start_tx(secondary(i), param));
      secondary(i).set_hotspot_group_commit_info(primary().tx_id_, false);
      OZ(node_->write(secondary(i), 14001 + i, 15001 + i));
      OZ(members.push_back(secondary(i).tx_id_));
    }
    ObTxPart participant;
    participant.id_ = node_->ls_id_;
    participant.addr_ = node_->addr_;
    participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
    OZ(node_->txs_.sync_hotspot_legality_validation(participant, primary().tx_id_, members));
    if (OB_SUCC(ret)) {
      node_->wait_all_msg_consumed();
      OZ(wait_apply() ? OB_SUCCESS : OB_EAGAIN);
      ObTenantEnv::set_tenant(&node_->tenant_);
      OZ(node_->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(node_->ls_id_, mgr_));
      OZ(mgr_->get_tx_ctx_directly_from_hash_map(primary().tx_id_, primary_ctx_));
      for (int i = 0; OB_SUCC(ret) && i < count_; ++i) {
        OZ(mgr_->get_tx_ctx_directly_from_hash_map(secondary(i).tx_id_, secondary_ctx_[i]));
      }
    }
    return ret;
  }
  ObTxDesc &primary() { return primary_guard_->get_tx_desc(); }
  ObTxDesc &secondary(const int i = 0) { return secondary_guard_[i]->get_tx_desc(); }
  ObTxHotspotRedoCacheHandle &handle() { return primary_ctx_->hotspot_redo_cache_; }
  bool wait_apply()
  {
    return response_cleanup_wait([&]() { return 0 == node_->fake_tx_log_adapter_->get_inflight_cnt(); });
  }
  int commit_primary()
  {
    int ret = node_->submit_commit_tx(primary(), node_->ts_after_ms(20000), primary_cb_, nullptr);
    if (OB_SUCC(ret) && !response_cleanup_wait([&]() { return 1 == primary_cb_.count_.load(); })) {
      ret = OB_EAGAIN;
    }
    if (OB_SUCC(ret) && !wait_apply()) { ret = OB_EAGAIN; }
    if (OB_SUCC(ret)) {
      // The SP scheduler callback need not publish a descriptor commit_version_.
      // Freeze the actual participant result before entering any cache lock.
      CtxLockGuard guard(primary_ctx_->lock_);
      primary_commit_version_ = primary_ctx_->ctx_tx_data_.get_commit_version();
    }
    return ret;
  }
  SCN commit_version() const { return primary_commit_version_; }
  int handoff()
  {
    int ret = OB_SUCCESS;
    ReplayLogEntryFunctor replay(new_node_.get());
    OZ(node_->fake_tx_log_adapter_->replay_all(replay));
    ObTenantEnv::set_tenant(&node_->tenant_);
    OZ(mgr_->switch_to_follower_gracefully());
    OZ(node_->fake_tx_log_adapter_->replay_all(replay));
    ObTxNode::get_location_adapter_().update_localtion(new_node_->ls_id_, new_node_->addr_);
    ObTenantEnv::set_tenant(&new_node_->tenant_);
    OZ(new_node_->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(new_node_->ls_id_, new_mgr_));
    OZ(new_mgr_->switch_to_leader());
    OZ(wait_apply() ? OB_SUCCESS : OB_EAGAIN);
    ObTenantEnv::set_tenant(&node_->tenant_);
    return ret;
  }
  // White-box scheduler-result setup only. The actual handler and callback are
  // production code; this is not a claim of a real commit/timeout interleaving.
  int complete_secondary(const int i, const int result = OB_SUCCESS)
  {
    int ret = OB_SUCCESS;
    ObTxDesc &desc = secondary(i);
    {
      ObSpinLockGuard guard(desc.lock_);
      desc.state_ = ObTxDesc::State::IN_TERMINATE;
      desc.commit_times_ = 2;
      OZ(desc.set_commit_cb(&secondary_cb_[i]));
    }
    ObTenantEnv::set_tenant(&node_->tenant_);
    OZ(node_->txs_.handle_tx_commit_result(desc.tx_id_, result,
        OB_SUCCESS == result ? SCN::min_scn() : SCN()));
    return ret;
  }
  int remove_redo()
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < count_; ++i) {
      int64_t remaining = -1;
      OZ(handle().remove_synced_hotspot_redo(i, remaining));
      if (OB_SUCC(ret) && 0 != remaining) { ret = OB_EAGAIN; }
    }
    return ret;
  }
  bool wait_detached()
  {
    return response_cleanup_wait([&]() {
      SpinRLockGuard guard(handle().hotspot_lock_);
      return nullptr == handle().resp_task_.primary_tx_ctx_ && !handle().resp_task_.is_in_queue();
    });
  }
  void expect_member_released(const int i)
  {
    ObTenantEnv::set_tenant(&node_->tenant_);
    EXPECT_TRUE(response_cleanup_wait([&]() {
      CtxLockGuard guard(secondary_ctx_[i]->lock_);
      return secondary_ctx_[i]->is_exiting_ && 1 == secondary_ctx_[i]->get_ref();
    })); // Wait out any already-queued callback; this fixture's lookup remains.
    {
      CtxLockGuard guard(secondary_ctx_[i]->lock_);
      EXPECT_TRUE(secondary_ctx_[i]->is_exiting_);
      EXPECT_EQ(1, secondary_ctx_[i]->get_ref());
    }
    ObPartTransCtx *probe = nullptr;
    const ObTransID id = secondary_ctx_[i]->trans_id_;
    const int ret = mgr_->get_tx_ctx_directly_from_hash_map(id, probe);
    EXPECT_EQ(OB_TRANS_CTX_NOT_EXIST, ret);
    if (nullptr != probe) { (void)mgr_->revert_tx_ctx(probe); }
  }
  // Declared first, destroyed last, including on a fatal ASSERT exit.
  CountingCommitCallback primary_cb_;
  CountingCommitCallback secondary_cb_[4];
  SCN primary_commit_version_;
  ObAddr saved_self_;
  MsgBus &bus_;
  std::unique_ptr<ObTxNode> node_;
  std::unique_ptr<ObTxNode> new_node_;
  std::unique_ptr<ObTxDescGuard> primary_guard_;
  std::unique_ptr<ObTxDescGuard> secondary_guard_[4];
  ObLSTxCtxMgr *mgr_;
  ObLSTxCtxMgr *new_mgr_;
  ObPartTransCtx *primary_ctx_;
  ObPartTransCtx *secondary_ctx_[4];
  int count_;
};

// T01/T02: real P commit, natural ordinary-worker enqueue, A -> B, and a
// real S commit request to B (which has no S ctx). No old-A replay or S abort
// appears between scheduler completion and the resource assertions.
static void run_response_cleanup_handoff(MsgBus &bus, const bool remove_desc)
{
  ResponseCleanupFixture f(bus);
  ASSERT_EQ(OB_SUCCESS, f.init(1, true));
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(1, f.primary_cb_.count_.load());
  ASSERT_EQ(OB_SUCCESS, f.primary_cb_.result_.load());
  ASSERT_EQ(OB_SUCCESS, f.handoff());
  const ObTransID secondary_id = f.secondary().tx_id_;
  const ObTransID primary_id = f.primary().tx_id_;
  ASSERT_EQ(&f.node_->txs_, f.primary_ctx_->get_trans_service());
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    ASSERT_EQ(static_cast<int64_t>(ObPartTransAction::START), f.secondary_ctx_[0]->part_trans_action_);
    ASSERT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_TRUE(f.secondary_ctx_[0]->exec_info_.scheduler_ == f.secondary_ctx_[0]->addr_);
    EXPECT_FALSE(f.secondary_ctx_[0]->has_persisted_log_());
    EXPECT_FALSE(f.secondary_ctx_[0]->is_logging_());
  }
  ObPartTransCtx *unexpected_new_secondary = nullptr;
  ObTenantEnv::set_tenant(&f.new_node_->tenant_);
  const int get_new_ret = f.new_mgr_->get_tx_ctx_directly_from_hash_map(
      secondary_id, unexpected_new_secondary);
  if (nullptr != unexpected_new_secondary) {
    (void)f.new_mgr_->revert_tx_ctx(unexpected_new_secondary);
  }
  ASSERT_EQ(OB_TRANS_CTX_NOT_EXIST, get_new_ret);
  ASSERT_EQ(OB_SUCCESS, f.node_->submit_commit_tx(
      f.secondary(), f.node_->ts_after_ms(20000), f.secondary_cb_[0], nullptr));
  ASSERT_TRUE(response_cleanup_wait([&]() { return 1 == f.secondary_cb_[0].count_.load(); }));
  f.node_->wait_all_msg_consumed();
  ASSERT_EQ(OB_SUCCESS, f.secondary_cb_[0].result_.load());
  ASSERT_EQ(ObTxDesc::State::COMMITTED, f.secondary().state_);
  const SCN secondary_version = f.secondary().commit_version_;
  EXPECT_EQ(f.commit_version(), secondary_version);
  EXPECT_FALSE(f.secondary().commit_task_.is_registered());
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    ASSERT_EQ(static_cast<int64_t>(ObPartTransAction::START), f.secondary_ctx_[0]->part_trans_action_);
    ASSERT_FALSE(f.secondary_ctx_[0]->is_exiting_);
  }
  if (remove_desc) {
    ASSERT_EQ(OB_SUCCESS, f.secondary_guard_[0]->release());
    ObTxDesc *probe = nullptr;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, f.node_->txs_.tx_desc_mgr_.get(secondary_id, probe));
    if (nullptr != probe) { f.node_->txs_.tx_desc_mgr_.revert(*probe); }
  }
  bool not_needed = false;
  ASSERT_EQ(OB_SUCCESS, f.node_->txs_.check_hotspot_response_not_needed(
      secondary_id, primary_id, not_needed));
  ASSERT_TRUE(not_needed);
  const int64_t secondary_refs_before_gate = f.secondary_ctx_[0]->get_ref();
  {
    SpinWLockGuard guard(f.handle().hotspot_lock_);
    // Original-gate control, not a run of the old binary: without the Handle's
    // new query phase, a finalized/missing desc still cannot move old START S.
    EXPECT_EQ(OB_EAGAIN, f.handle().cache_->response_scheduler(
        OB_SUCCESS, f.commit_version(), false));
    EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT,
              f.handle().cache_->hotspot_cache_[0].get_resp_status());
  }
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    EXPECT_EQ(static_cast<int64_t>(ObPartTransAction::START), f.secondary_ctx_[0]->part_trans_action_);
    EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_EQ(secondary_refs_before_gate, f.secondary_ctx_[0]->get_ref());
  }
  const int64_t primary_refs_with_task = f.primary_ctx_->get_ref();
  int64_t responses_before = -1;
  {
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    ASSERT_TRUE(f.handle().resp_task_.is_in_queue());
    ASSERT_EQ(f.primary_ctx_, f.handle().resp_task_.primary_tx_ctx_);
    ASSERT_EQ(static_cast<int64_t>(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER),
              f.handle().resp_task_.get_task_type());
    ASSERT_EQ(OB_SUCCESS, f.handle().resp_task_.tx_result_);
    responses_before = f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_;
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached()) << "old START S still owns cache/map/task references";
  EXPECT_EQ(0, f.handle().get_hotspot_cache_count());
  f.expect_member_released(0);
  EXPECT_EQ(primary_refs_with_task - 1, f.primary_ctx_->get_ref());
  const int64_t primary_refs_after = f.primary_ctx_->get_ref();
  f.handle().resp_task_.finish();
  f.handle().resp_task_.finish();
  EXPECT_EQ(primary_refs_after, f.primary_ctx_->get_ref());
  {
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(responses_before, f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_);
  }
  EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
  EXPECT_EQ(OB_SUCCESS, f.secondary_cb_[0].result_.load());
  if (!remove_desc) {
    EXPECT_EQ(ObTxDesc::State::COMMITTED, f.secondary().state_);
    EXPECT_EQ(secondary_version, f.secondary().commit_version_);
    EXPECT_EQ(OB_SUCCESS, f.secondary().commit_out_);
  }
  ::testing::Test::RecordProperty("response_cleanup_interleaving", "real_A_to_B_without_old_replay");
  ::testing::Test::RecordProperty("response_cleanup_removed_desc", remove_desc ? "1" : "0");
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_t01_start_scheduler_committed_no_old_replay)
{
  run_response_cleanup_handoff(bus_, false);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_t02_start_scheduler_released_no_old_replay)
{
  run_response_cleanup_handoff(bus_, true);
}

// T03--T05: deliberately injected descriptor states/types, not cluster races.
// Every successful get must restore exactly the reference baseline; input true
// also proves that each negative/error exit resets the output parameter.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_state_identity_type_and_ref_matrix)
{
  CountingCommitCallback callback; // Outlives the node and every desc guard.
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_self = GCONF.self_addr_;
  DEFER(GCONF.self_addr_ = saved_self);
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;
  PREPARE_TX_PARAM(param);
  param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard desc_guard = n1->get_tx_guard();
  ObTxDesc &desc = desc_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, n1->start_tx(desc, param));
  const ObTransID secondary_id = desc.tx_id_;
  const ObTransID primary_id(16000);
  desc.set_hotspot_group_commit_info(primary_id, false);
  { ObSpinLockGuard guard(desc.lock_); ASSERT_EQ(OB_SUCCESS, desc.set_commit_cb(&callback)); }
  const int64_t refs = desc.get_ref();
  const ObTxDesc::State states[] = {
      ObTxDesc::State::ACTIVE, ObTxDesc::State::IMPLICIT_ACTIVE,
      ObTxDesc::State::IN_TERMINATE, ObTxDesc::State::ROLLBACK_SAVEPOINT,
      ObTxDesc::State::IDLE, ObTxDesc::State::SUB_PREPARED,
      ObTxDesc::State::COMMITTED, ObTxDesc::State::ROLLED_BACK,
      ObTxDesc::State::ABORTED, ObTxDesc::State::COMMIT_TIMEOUT,
      ObTxDesc::State::COMMIT_UNKNOWN};
  for (int i = 0; i < ARRAYSIZEOF(states); ++i) {
    SCOPED_TRACE(i);
    { ObSpinLockGuard guard(desc.lock_); desc.state_ = states[i]; }
    bool not_needed = true;
    ASSERT_EQ(OB_SUCCESS, n1->txs_.check_hotspot_response_not_needed(
        secondary_id, primary_id, not_needed));
    EXPECT_EQ(i >= 6, not_needed);
    EXPECT_EQ(refs, desc.get_ref());
    EXPECT_EQ(states[i], desc.state_);
    EXPECT_EQ(0, callback.count_.load());
  }
  { ObSpinLockGuard guard(desc.lock_); desc.state_ = ObTxDesc::State::COMMITTED; }
  auto expect_denied = [&]() {
    bool not_needed = true;
    EXPECT_EQ(OB_SUCCESS, n1->txs_.check_hotspot_response_not_needed(
        secondary_id, primary_id, not_needed));
    EXPECT_FALSE(not_needed);
    EXPECT_EQ(refs, desc.get_ref());
  };
  const ObTransID wrong_id(16001);
  // S identity replacement must happen after get() pins the old object; changing
  // the hash key before get() would merely create the documented miss case.
  // The start_epoch/pin race below covers that distinct identity check.
  desc.set_hotspot_group_commit_info(wrong_id, false);
  expect_denied();
  desc.set_hotspot_group_commit_info(primary_id, false);
  const uint64_t tenant_id = desc.tenant_id_;
  { ObSpinLockGuard guard(desc.lock_); desc.tenant_id_ = tenant_id + 1; }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.tenant_id_ = tenant_id; }
  const ObAddr addr = desc.addr_;
  { ObSpinLockGuard guard(desc.lock_); desc.addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.3", 8888); }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.addr_ = addr; desc.flags_.SHADOW_ = true; }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.flags_.SHADOW_ = false; desc.flags_.REPLICA_ = true; }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.flags_.REPLICA_ = false; desc.global_tx_type_ = ObGlobalTxType::XA_TRANS; }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.global_tx_type_ = ObGlobalTxType::DBLINK_TRANS; }
  expect_denied();
  {
    ObSpinLockGuard guard(desc.lock_);
    desc.global_tx_type_ = ObGlobalTxType::PLAIN;
    EXPECT_EQ(OB_SUCCESS, desc.xid_.set(ObString::make_string("response_cleanup_xa"),
                                      ObString::make_string("branch"), 1));
  }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.xid_.reset(); }
  // A non-null XA ctx must also be rejected even if the xid/global type is plain.
  { ObSpinLockGuard guard(desc.lock_); desc.xa_ctx_ = reinterpret_cast<ObXACtx *>(&callback); }
  expect_denied();
  { ObSpinLockGuard guard(desc.lock_); desc.xa_ctx_ = nullptr; }
  bool not_needed = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, n1->txs_.check_hotspot_response_not_needed(
      ObTransID(), primary_id, not_needed));
  EXPECT_FALSE(not_needed);
  not_needed = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, n1->txs_.check_hotspot_response_not_needed(
      secondary_id, secondary_id, not_needed));
  EXPECT_FALSE(not_needed);
  // Simulate an actual get error, not the distinct participant-ctx miss code.
  n1->txs_.tx_desc_mgr_.inited_ = false;
  not_needed = true;
  const int get_error = n1->txs_.check_hotspot_response_not_needed(
      secondary_id, primary_id, not_needed);
  n1->txs_.tx_desc_mgr_.inited_ = true;
  EXPECT_EQ(OB_NOT_INIT, get_error);
  EXPECT_FALSE(not_needed);
  EXPECT_EQ(refs, desc.get_ref());
  n1->txs_.tx_desc_mgr_.stoped_ = true;
  not_needed = true;
  const int stopped_error = n1->txs_.check_hotspot_response_not_needed(
      secondary_id, primary_id, not_needed);
  n1->txs_.tx_desc_mgr_.stoped_ = false;
  EXPECT_EQ(OB_IN_STOP_STATE, stopped_error);
  EXPECT_FALSE(not_needed);
  n1->txs_.is_running_ = false;
  not_needed = true;
  const int running_error = n1->txs_.check_hotspot_response_not_needed(
      secondary_id, primary_id, not_needed);
  n1->txs_.is_running_ = true;
  EXPECT_EQ(OB_NOT_RUNNING, running_error);
  EXPECT_FALSE(not_needed);
  EXPECT_EQ(refs, desc.get_ref());
  { ObSpinLockGuard guard(desc.lock_); desc.state_ = ObTxDesc::State::ACTIVE; }
  ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(desc));
  ASSERT_EQ(OB_SUCCESS, desc_guard.release());
  not_needed = false;
  EXPECT_EQ(OB_SUCCESS, n1->txs_.check_hotspot_response_not_needed(
      secondary_id, primary_id, not_needed));
  EXPECT_TRUE(not_needed); // Only after the normally registered identity is removed.
}

// T06: real DescMgr removal/epoch/reuse while a successful get is pinned.
// The pause is before desc.lock, so the main thread can finish the old identity.
static void run_response_query_pin_race(MsgBus &bus, const int mode)
{
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_self = GCONF.self_addr_;
  DEFER(GCONF.self_addr_ = saved_self);
  std::unique_ptr<ObTxNode> node(new ObTxNode(
      1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), bus));
  ASSERT_EQ(OB_SUCCESS, node->start());
  GCONF.self_addr_ = node->addr_;
  const int64_t allocated_before = node->txs_.tx_desc_mgr_.get_alloc_count();
  PREPARE_TX_PARAM(param);
  param.timeout_us_ = 1000 * 1000 * 1000;
  ObTxDescGuard guard = node->get_tx_guard();
  ObTxDesc &desc = guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, node->start_tx(desc, param));
  const ObTransID old_id = desc.tx_id_;
  const ObTransID primary_id(16002);
  desc.set_hotspot_group_commit_info(primary_id, false);
  ASSERT_EQ(OB_SUCCESS, node->rollback_tx(desc));
  const int64_t refs = desc.get_ref();
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(AFTER_HOTSPOT_RESPONSE_DESC_GET));
  std::atomic<int> query_ret(OB_ERR_UNEXPECTED);
  std::atomic<bool> query_bool(true);
  std::thread query([&]() {
    ObTenantEnv::set_tenant(&node->tenant_);
    bool value = true;
    query_ret.store(node->txs_.check_hotspot_response_not_needed(old_id, primary_id, value));
    query_bool.store(value);
  });
  NAMED_DEFER(join_query, (void)sync.resume(); if (query.joinable()) { query.join(); });
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  EXPECT_EQ(refs + 1, desc.get_ref());
  if (0 == mode) {
    ASSERT_EQ(OB_SUCCESS, guard.release()); // Only the query's pin keeps desc alive now.
  } else if (1 == mode) {
    { ObSpinLockGuard lock(desc.lock_); ASSERT_EQ(OB_SUCCESS, node->txs_.start_epoch_(desc)); }
    ASSERT_EQ(OB_SUCCESS, node->start_tx(desc, param));
    ASSERT_NE(old_id, desc.tx_id_);
  } else {
    // reuse_tx waits for the outstanding pin, unlike start_epoch_. Resume the
    // query before joining; no thread (including this one) crosses a test.
    std::atomic<int> reuse_ret(OB_ERR_UNEXPECTED);
    std::thread reuser([&]() {
      ObTenantEnv::set_tenant(&node->tenant_);
      reuse_ret.store(node->txs_.reuse_tx(desc, DATA_CURRENT_VERSION));
    });
    NAMED_DEFER(join_reuser, (void)sync.resume(); if (reuser.joinable()) { reuser.join(); });
    ASSERT_TRUE(response_cleanup_wait([&]() {
      ObSpinLockGuard lock(desc.lock_);
      return desc.flags_.RELEASED_;
    }));
    ASSERT_EQ(OB_SUCCESS, sync.resume());
    query.join();
    reuser.join();
    EXPECT_EQ(OB_SUCCESS, reuse_ret.load());
    ASSERT_EQ(OB_SUCCESS, node->start_tx(desc, param));
    ASSERT_NE(old_id, desc.tx_id_);
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  if (query.joinable()) { query.join(); }
  EXPECT_EQ(OB_SUCCESS, query_ret.load());
  EXPECT_FALSE(query_bool.load()); // A pinned pointer is not an identity proof.
  if (0 == mode) {
    EXPECT_EQ(allocated_before, node->txs_.tx_desc_mgr_.get_alloc_count());
  } else {
    EXPECT_EQ(refs, desc.get_ref());
    EXPECT_EQ(ObTxDesc::State::ACTIVE, desc.state_);
    EXPECT_FALSE(desc.get_hotspot_primary_tx_id().is_valid());
    ASSERT_EQ(OB_SUCCESS, node->rollback_tx(desc));
  }
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_pin_then_normal_release)
{
  run_response_query_pin_race(bus_, 0);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_pin_then_start_epoch)
{
  run_response_query_pin_race(bus_, 1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_pin_then_reuse)
{
  run_response_query_pin_race(bus_, 2);
}

// T03/T13: real handler/callback semantics, with injected IN_TERMINATE setup.
// No timeout/UNKNOWN/zero-row result is inferred from the queued P success.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_terminal_results_are_not_primary_success)
{
  const int results[] = {OB_TRANS_ROLLBACKED, OB_TRANS_TIMEOUT, OB_TRANS_UNKNOWN,
                         OB_ROLLBACK_ON_NO_AFFECTED_ROWS};
  for (int result : results) {
    SCOPED_TRACE(result);
    // MsgBus routes live until the bus is destroyed, not until a node exits.
    MsgBus case_bus;
    ResponseCleanupFixture f(case_bus);
    ASSERT_EQ(OB_SUCCESS, f.init());
    ResponseCleanupDebugSync sync;
    ASSERT_EQ(OB_SUCCESS, sync.init());
    ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
    ASSERT_EQ(OB_SUCCESS, f.commit_primary());
    ASSERT_EQ(OB_SUCCESS, sync.wait());
    if (OB_ROLLBACK_ON_NO_AFFECTED_ROWS == result) {
      f.secondary().set_hotspot_group_commit_info(f.primary().tx_id_, true);
    }
    ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0, result));
    const ObTxDesc::State final_state = f.secondary().state_;
    const SCN final_version = f.secondary().commit_version_;
    ASSERT_EQ(1, f.secondary_cb_[0].count_.load());
    ASSERT_EQ(result, f.secondary_cb_[0].result_.load());
    const int64_t desc_refs = f.secondary().get_ref();
    ASSERT_EQ(OB_SUCCESS, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    {
      SpinRLockGuard guard(f.handle().hotspot_lock_);
      EXPECT_EQ(TxSecondaryRespStatus::RESP_NO_RESPONSE_NEEDED,
                f.handle().cache_->hotspot_cache_[0].get_resp_status());
      EXPECT_EQ(0, f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_);
    }
    EXPECT_EQ(desc_refs, f.secondary().get_ref());
    EXPECT_EQ(final_state, f.secondary().state_);
    EXPECT_EQ(final_version, f.secondary().commit_version_);
    EXPECT_EQ(result, f.secondary().commit_out_);
    EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
    ASSERT_EQ(OB_SUCCESS, sync.resume());
    ASSERT_TRUE(f.wait_detached());
    f.expect_member_released(0);
    EXPECT_EQ(result, f.secondary_cb_[0].result_.load());
  }
}

// T04/T05: public handle retries rather than turning a conservative query/error
// into worker completion. The queued task and both map/cache refs stay owned.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_unfinished_or_query_error_retains_task)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  const int64_t primary_refs = f.primary_ctx_->get_ref();
  const int64_t secondary_refs = f.secondary_ctx_[0]->get_ref();
  const int64_t desc_refs = f.secondary().get_ref();
  const ObTxDesc::State states[] = {ObTxDesc::State::ACTIVE,
      ObTxDesc::State::IMPLICIT_ACTIVE, ObTxDesc::State::IN_TERMINATE};
  for (const auto state : states) {
    { ObSpinLockGuard guard(f.secondary().lock_); f.secondary().state_ = state; }
    EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_EQ(secondary_refs, f.secondary_ctx_[0]->get_ref());
    EXPECT_EQ(primary_refs, f.primary_ctx_->get_ref());
    EXPECT_EQ(desc_refs, f.secondary().get_ref());
    EXPECT_EQ(0, f.secondary_cb_[0].count_.load());
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT,
              f.handle().cache_->hotspot_cache_[0].get_resp_status());
    EXPECT_EQ(f.primary_ctx_, f.handle().resp_task_.primary_tx_ctx_);
    EXPECT_TRUE(f.handle().resp_task_.is_in_queue());
  }
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0));
  // The service error is returned by get(), not by the action gate.
  f.node_->txs_.tx_desc_mgr_.inited_ = false;
  const int ret = f.handle().response_scheduler(OB_SUCCESS, f.commit_version());
  f.node_->txs_.tx_desc_mgr_.inited_ = true;
  EXPECT_EQ(OB_EAGAIN, ret);
  EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
  EXPECT_EQ(secondary_refs, f.secondary_ctx_[0]->get_ref());
  EXPECT_EQ(desc_refs, f.secondary().get_ref());
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  f.expect_member_released(0);
  EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
}

class ResponseCleanupGcFailure
{
public:
  ~ResponseCleanupGcFailure() { (void)set(OB_SUCCESS); }
  int set(const int error)
  {
    common::EventItem item;
    item.error_code_ = error;
    item.occur_ = OB_SUCCESS == error ? 0 : 100;
    item.trigger_freq_ = 0;
    item.cond_ = 0;
    return common::EventTable::set_event("ERRSIM_HOTSPOT_SECONDARY_GC", item);
  }
};

// T08--T11: white-box FAILED/action setup after real redo removal. The injected
// error is at the new GC entry, not a fabricated failure of an MDS callback.
// Existing response records survive repeated GC-entry failures and action drift.
static void run_response_cleanup_failed_gc(MsgBus &bus, const bool commit_action,
                                          const bool remove_desc)
{
  ResponseCleanupFixture f(bus);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0));
  const SCN final_version = f.secondary().commit_version_;
  const ObTransID secondary_id = f.secondary().tx_id_;
  ASSERT_EQ(OB_SUCCESS, f.remove_redo());
  ASSERT_EQ(OB_SUCCESS, f.secondary_ctx_[0]->set_secondary_hotspot_redo_status_by_exit_state(
      TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    ASSERT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    if (commit_action) { f.secondary_ctx_[0]->part_trans_action_ = ObPartTransAction::COMMIT; }
  }
  if (remove_desc) { ASSERT_EQ(OB_SUCCESS, f.secondary_guard_[0]->release()); }
  ResponseCleanupGcFailure failure;
  ASSERT_EQ(OB_SUCCESS, failure.set(OB_ALLOCATE_MEMORY_FAILED));
  const int64_t primary_refs = f.primary_ctx_->get_ref();
  const int64_t secondary_refs = f.secondary_ctx_[0]->get_ref();
  TxSecondaryRespStatus response_status = TxSecondaryRespStatus::RESP_NOT_SENT;
  int64_t sent = -1;
  for (int attempt = 0; attempt < 3; ++attempt) {
    SCOPED_TRACE(attempt);
    EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    {
      SpinRLockGuard guard(f.handle().hotspot_lock_);
      const auto &slot = f.handle().cache_->hotspot_cache_[0];
      if (0 == attempt) {
        response_status = slot.get_resp_status();
        sent = f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_;
        EXPECT_EQ(commit_action ? TxSecondaryRespStatus::RESP_COMMITTED
                               : TxSecondaryRespStatus::RESP_NO_RESPONSE_NEEDED, response_status);
      }
      EXPECT_EQ(response_status, slot.get_resp_status());
      EXPECT_EQ(sent, f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_);
      EXPECT_EQ(f.primary_ctx_, f.handle().resp_task_.primary_tx_ctx_);
      EXPECT_TRUE(f.handle().resp_task_.is_in_queue());
    }
    {
      CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
      EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, f.secondary_ctx_[0]->redo_flush_status_);
      EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
      // Once responsibility is closed, changing START to COMMIT must not resend.
      if (!commit_action) { f.secondary_ctx_[0]->part_trans_action_ = ObPartTransAction::COMMIT; }
    }
    EXPECT_EQ(primary_refs, f.primary_ctx_->get_ref());
    EXPECT_EQ(secondary_refs, f.secondary_ctx_[0]->get_ref());
    EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
  }
  ASSERT_EQ(OB_SUCCESS, failure.set(OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    EXPECT_TRUE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_EQ(TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED, f.secondary_ctx_[0]->redo_flush_status_);
  }
  // COMMIT + FAILED is permitted to send the old GC KILLED notification. Drain
  // it before checking that the finalized scheduler/callback cannot regress.
  f.node_->wait_all_msg_consumed();
  if (!remove_desc) {
    EXPECT_EQ(ObTxDesc::State::COMMITTED, f.secondary().state_);
    EXPECT_EQ(OB_SUCCESS, f.secondary().commit_out_);
    EXPECT_EQ(final_version, f.secondary().commit_version_);
    EXPECT_FALSE(f.secondary().commit_task_.is_registered());
  } else {
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, f.node_->txs_.handle_tx_commit_result(
        secondary_id, OB_TRANS_KILLED, SCN()));
  }
  EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
  EXPECT_EQ(OB_SUCCESS, f.secondary_cb_[0].result_.load());
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  f.expect_member_released(0);
  EXPECT_EQ(primary_refs - 1, f.primary_ctx_->get_ref());
  EXPECT_EQ(0, f.handle().get_hotspot_cache_count());
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_failed_start_gc_entry_retries_without_resend)
{
  run_response_cleanup_failed_gc(bus_, false, false);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_failed_commit_late_killed_preserves_final_result)
{
  run_response_cleanup_failed_gc(bus_, true, false);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_failed_commit_removed_desc_ignores_late_killed)
{
  run_response_cleanup_failed_gc(bus_, true, true);
}

// T12: force the existing RPC fallback and accept its post while dropping its
// delivery. This is a white-box transport ordering, not proof of scheduler receipt.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_failed_post_success_is_not_scheduler_completion)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.remove_redo());
  {
    ObSpinLockGuard guard(f.secondary().lock_);
    f.secondary().state_ = ObTxDesc::State::IN_TERMINATE;
    ASSERT_EQ(OB_SUCCESS, f.secondary().set_commit_cb(&f.secondary_cb_[0]));
  }
  ASSERT_EQ(OB_SUCCESS, f.secondary_ctx_[0]->set_secondary_hotspot_redo_status_by_exit_state(
      TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    f.secondary_ctx_[0]->part_trans_action_ = ObPartTransAction::COMMIT;
    f.secondary_ctx_[0]->commit_cb_.disable(); // Existing branch falls back to RPC.
  }
  f.node_->add_drop_msg_type(TX_COMMIT_RESP);
  DEFER(f.node_->del_drop_msg_type(TX_COMMIT_RESP));
  const int64_t refs = f.secondary_ctx_[0]->get_ref();
  EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
  f.node_->wait_all_msg_consumed();
  EXPECT_EQ(ObTxDesc::State::IN_TERMINATE, f.secondary().state_);
  EXPECT_EQ(0, f.secondary_cb_[0].count_.load());
  EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
  EXPECT_EQ(refs, f.secondary_ctx_[0]->get_ref());
  {
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(TxSecondaryRespStatus::RESP_COMMITTED,
              f.handle().cache_->hotspot_cache_[0].get_resp_status());
    EXPECT_EQ(1, f.handle().cache_->hotspot_stat_.response_msg_sent_cnt_);
    EXPECT_TRUE(f.handle().resp_task_.is_in_queue());
  }
  ASSERT_EQ(OB_SUCCESS, f.node_->txs_.handle_tx_commit_result(
      f.secondary().tx_id_, OB_SUCCESS, f.commit_version()));
  ASSERT_EQ(1, f.secondary_cb_[0].count_.load());
  f.node_->del_drop_msg_type(TX_COMMIT_RESP);
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  f.node_->wait_all_msg_consumed();
  f.expect_member_released(0);
  EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
  EXPECT_EQ(OB_SUCCESS, f.secondary_cb_[0].result_.load());
  EXPECT_EQ(ObTxDesc::State::COMMITTED, f.secondary().state_);
  EXPECT_FALSE(f.secondary().commit_task_.is_registered());
}

// T07/T08: the ordinary worker is held separately from the thread stopped at
// phase B/C. Neither desc pin nor ctx/cache lock may survive to the query hook.
static void run_response_cleanup_query_apply_race(MsgBus &bus, const int mode)
{
  ResponseCleanupFixture f(bus);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE,
      "response_cleanup_worker_paused", "resp_cleanup_worker_continue"));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait("response_cleanup_worker_paused"));
  if (6 != mode) { ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0)); }
  const int64_t desc_refs = f.secondary().get_ref();
  const int64_t primary_refs = f.primary_ctx_->get_ref();
  NAMED_DEFER(restore_snapshot_primary_id,
              if (4 == mode || 7 == mode) {
                SpinWLockGuard guard(f.handle().hotspot_lock_);
                if (nullptr != f.handle().cache_) {
                  f.handle().cache_->primary_tx_id_ = f.primary().tx_id_;
                  if (7 == mode && f.handle().cache_->hotspot_cache_count_ > 0) {
                    f.handle().cache_->hotspot_cache_[0].tx_id_ = f.secondary().tx_id_;
                  }
                }
              });
  ASSERT_EQ(OB_SUCCESS, sync.pause(AFTER_HOTSPOT_RESPONSE_QUERY));
  std::atomic<int> response_ret(OB_ERR_UNEXPECTED);
  std::thread responder([&]() {
    ObTenantEnv::set_tenant(&f.node_->tenant_);
    response_ret.store(f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
  });
  NAMED_DEFER(join_responder, (void)sync.resume(); if (responder.joinable()) { responder.join(); });
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  EXPECT_EQ(desc_refs, f.secondary().get_ref()); // No pin waiting on the cache.
  {
    ObSpinLockGuard guard(f.secondary().lock_); // Also proves desc.lock was dropped.
    EXPECT_EQ(6 == mode ? ObTxDesc::State::ACTIVE : ObTxDesc::State::COMMITTED,
              f.secondary().state_);
  }
  if (0 == mode || 1 == mode) {
    // The production replay supplement clears members while phase C is stopped.
    f.primary_ctx_->supplement_hotspot_response_after_replay_commit();
    ASSERT_EQ(0, f.handle().get_hotspot_cache_count());
    if (1 == mode) { ASSERT_EQ(OB_SUCCESS, f.handle().reuse()); }
  } else if (2 == mode || 3 == mode) {
    CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
    f.secondary_ctx_[0]->part_trans_action_ = 2 == mode
        ? ObPartTransAction::COMMIT : ObPartTransAction::ABORT;
  } else if (5 == mode) {
    // Another public-facade response finishes the same member while the first
    // invocation is unlocked. Its explicit response reason must not be replaced.
    ASSERT_EQ(OB_SUCCESS, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
  } else if (6 == mode) {
    // Phase B returned false (ACTIVE), but the S becomes COMMIT before phase C.
    // Preserve the original branch rather than treating false as a terminal gate.
    ASSERT_EQ(OB_SUCCESS, f.node_->submit_commit_tx(
        f.secondary(), f.node_->ts_after_ms(20000), f.secondary_cb_[0], nullptr));
  } else {
    SpinWLockGuard guard(f.handle().hotspot_lock_);
    // Identity replacement is defensive white-box injection, not a real new group.
    if (7 == mode) {
      f.handle().cache_->hotspot_cache_[0].tx_id_ = ObTransID(16005);
    } else {
      f.handle().cache_->primary_tx_id_ = ObTransID(16003);
    }
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  responder.join();
  if (4 == mode || 7 == mode) {
    EXPECT_EQ(OB_EAGAIN, response_ret.load());
    SpinWLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT,
              f.handle().cache_->hotspot_cache_[0].get_resp_status());
    f.handle().cache_->primary_tx_id_ = f.primary().tx_id_;
    if (7 == mode) { f.handle().cache_->hotspot_cache_[0].tx_id_ = f.secondary().tx_id_; }
  } else if (1 == mode) {
    EXPECT_EQ(OB_NOT_INIT, response_ret.load()); // Core was actually freed/reused.
  } else {
    EXPECT_EQ(OB_SUCCESS, response_ret.load());
    if (2 == mode || 3 == mode || 5 == mode || 6 == mode) {
      SpinRLockGuard guard(f.handle().hotspot_lock_);
      const TxSecondaryRespStatus expected = 3 == mode
          ? TxSecondaryRespStatus::RESP_ABORTED_BY_SESSION
          : (5 == mode ? TxSecondaryRespStatus::RESP_NO_RESPONSE_NEEDED
                       : TxSecondaryRespStatus::RESP_COMMITTED);
      EXPECT_EQ(expected, f.handle().cache_->hotspot_cache_[0].get_resp_status());
    }
  }
  EXPECT_EQ(primary_refs, f.primary_ctx_->get_ref()); // Queued worker still owns it.
  ASSERT_EQ(OB_SUCCESS, sync.resume("resp_cleanup_worker_continue"));
  ASSERT_TRUE(f.wait_detached());
  f.expect_member_released(0);
  EXPECT_EQ(primary_refs - 1, f.primary_ctx_->get_ref());
  EXPECT_EQ(1, f.secondary_cb_[0].count_.load());
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_races_replay_member_cleanup)
{
  run_response_cleanup_query_apply_race(bus_, 0);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_races_actual_core_reuse)
{
  run_response_cleanup_query_apply_race(bus_, 1);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_rechecks_start_to_commit)
{
  run_response_cleanup_query_apply_race(bus_, 2);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_rechecks_start_to_abort)
{
  run_response_cleanup_query_apply_race(bus_, 3);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_rejects_replaced_primary_identity)
{
  run_response_cleanup_query_apply_race(bus_, 4);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_preserves_already_closed_member)
{
  run_response_cleanup_query_apply_race(bus_, 5);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_negative_query_still_rechecks_commit_action)
{
  run_response_cleanup_query_apply_race(bus_, 6);
}

TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_query_rejects_replaced_secondary_identity)
{
  run_response_cleanup_query_apply_race(bus_, 7);
}

// T13: mixed real normal/zero-row delivery plus explicit ABORT/FAILED state
// setup. Only the latter two are white-box; normal and zero-row use submit APIs.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_mixed_members_preserve_zero_row_and_data)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init(4));
  f.secondary(1).set_hotspot_group_commit_info(f.primary().tx_id_, true);
  ObTxPart participant;
  participant.id_ = f.node_->ls_id_;
  participant.addr_ = f.node_->addr_;
  participant.epoch_ = ObTxPart::EPOCH_UNKNOWN;
  ObAggregatedTxIDArray rollback_members;
  ASSERT_EQ(OB_SUCCESS, rollback_members.push_back(f.secondary(1).tx_id_));
  // Use SQL's full rollback path: P must log/filter the remapped range, not
  // merely mark the member and clean its original secondary memtable.
  ASSERT_EQ(OB_SUCCESS, f.node_->txs_.hotspot_rollback_to(
      participant, f.primary().tx_id_, rollback_members));
  f.node_->wait_all_msg_consumed();
  ASSERT_TRUE(f.wait_apply());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, f.node_->submit_commit_tx(
        f.secondary(i), f.node_->ts_after_ms(20000), f.secondary_cb_[i], nullptr));
  }
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(2, OB_TRANS_ROLLBACKED));
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(3));
  ASSERT_EQ(OB_SUCCESS, f.remove_redo());
  {
    CtxLockGuard guard(f.secondary_ctx_[2]->lock_);
    f.secondary_ctx_[2]->part_trans_action_ = ObPartTransAction::ABORT;
  }
  ASSERT_EQ(OB_SUCCESS, f.secondary_ctx_[3]->set_secondary_hotspot_redo_status_by_exit_state(
      TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  for (int i = 0; i < 4; ++i) {
    SCOPED_TRACE(i);
    f.expect_member_released(i);
    EXPECT_EQ(1, f.secondary_cb_[i].count_.load());
  }
  EXPECT_EQ(OB_SUCCESS, f.secondary_cb_[0].result_.load());
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, f.secondary_cb_[1].result_.load());
  EXPECT_EQ(ObTxDesc::State::ROLLED_BACK, f.secondary(1).state_);
  EXPECT_EQ(OB_ROLLBACK_ON_NO_AFFECTED_ROWS, f.secondary(1).commit_out_);
  EXPECT_FALSE(f.secondary(1).commit_version_.is_valid());
  EXPECT_EQ(OB_TRANS_ROLLBACKED, f.secondary_cb_[2].result_.load());
  EXPECT_EQ(OB_SUCCESS, f.secondary_cb_[3].result_.load());
  PREPARE_TX_PARAM(param);
  ObTxDescGuard reader_guard = f.node_->get_tx_guard();
  ObTxDesc &reader = reader_guard.get_tx_desc();
  ASSERT_EQ(OB_SUCCESS, f.node_->start_tx(reader, param));
  int64_t value = 0;
  EXPECT_EQ(OB_SUCCESS, f.node_->read(reader, 14000, value));
  EXPECT_EQ(15000, value);
  EXPECT_EQ(OB_SUCCESS, f.node_->read(reader, 14001, value));
  EXPECT_EQ(15001, value);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, f.node_->read(reader, 14002, value));
  ASSERT_EQ(OB_SUCCESS, f.node_->rollback_tx(reader));
}

// T14: incomplete redo cannot borrow a terminated descriptor's authority to
// release callbacks. Each synthetic publication-window field is restored before
// trying the next condition; no fake callback is invoked or left in a list.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_preserves_redo_and_active_ref_gates)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0));
  ASSERT_EQ(OB_SUCCESS, f.remove_redo());
  const int64_t refs = f.secondary_ctx_[0]->get_ref();
  {
    ObTxHotspotRedoCacheHandle::ActiveRefGuard active(f.handle());
    EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
  }
  for (int condition = 0; condition < 3; ++condition) {
    SCOPED_TRACE(condition);
    SCN submitted;
    SCN synced;
    SCN submitting;
    int64_t need_remove = 0;
    int64_t unsynced = 0;
    {
      SpinWLockGuard guard(f.handle().hotspot_lock_);
      auto &slot = f.handle().cache_->hotspot_cache_[0];
      submitted = slot.max_submitted_scn_;
      synced = slot.max_synced_scn_;
      submitting = slot.redo_submitting_scn_;
      need_remove = slot.need_remove_cnt_;
      unsynced = slot.unsynced_node_cnt_;
      slot.need_remove_cnt_ = slot.unsynced_node_cnt_ = 1;
      if (0 == condition) { slot.redo_submitting_scn_.set_max(); }
      if (1 == condition) { slot.max_synced_scn_.reset(); }
    }
    NAMED_DEFER(restore_redo_window,
                SpinWLockGuard guard(f.handle().hotspot_lock_);
                auto &slot = f.handle().cache_->hotspot_cache_[0];
                slot.max_submitted_scn_ = submitted;
                slot.max_synced_scn_ = synced;
                slot.redo_submitting_scn_ = submitting;
                slot.need_remove_cnt_ = need_remove;
                slot.unsynced_node_cnt_ = unsynced);
    EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_EQ(refs, f.secondary_ctx_[0]->get_ref());
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(TxSecondaryRespStatus::RESP_NOT_SENT,
              f.handle().cache_->hotspot_cache_[0].get_resp_status());
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached()); // No self-pin survives the query phase.
  f.expect_member_released(0);
}

// T14: group CB ownership independently blocks full cache/task cleanup even
// after member GC. Move one real idle CB through busy/free/gap fixture windows.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_preserves_busy_free_and_gap_cb_ownership)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0));
  // Normal aggregation may already have returned every idle CB. Borrow a real
  // primary-owned CB and publish fixture ownership exactly as append_idle does;
  // normal cleanup below returns it through return_redo_log_cb, not a stack CB.
  ObTxLogCb *owned_cb = nullptr;
  {
    CtxLockGuard guard(f.primary_ctx_->lock_);
    ASSERT_EQ(OB_SUCCESS, f.primary_ctx_->get_log_cb_(false, owned_cb));
  }
  {
    SpinWLockGuard guard(f.handle().hotspot_lock_);
    owned_cb->reuse();
    f.handle().cache_->idle_hotspot_cbs_.add_last(owned_cb);
    ++f.handle().cache_->all_log_cb_cnt_;
  }
  const int64_t secondary_refs = f.secondary_ctx_[0]->get_ref();
  const int64_t primary_refs = f.primary_ctx_->get_ref();
  for (int placement = 0; placement < 3; ++placement) {
    SCOPED_TRACE(placement);
    ObTxLogCb *cb = nullptr;
    {
      SpinWLockGuard guard(f.handle().hotspot_lock_);
      auto &core = *f.handle().cache_;
      ASSERT_FALSE(core.idle_hotspot_cbs_.is_empty());
      cb = core.idle_hotspot_cbs_.get_first();
      core.idle_hotspot_cbs_.remove(cb);
      if (0 == placement) { core.busy_hotspot_cbs_.add_last(cb); }
      if (1 == placement) { core.free_hotspot_cbs_.add_last(cb); }
      // placement 2 leaves the owned CB outside every list: a gap.
    }
    NAMED_DEFER(restore_owned_cb,
                SpinWLockGuard guard(f.handle().hotspot_lock_);
                auto &core = *f.handle().cache_;
                if (0 == placement) { core.busy_hotspot_cbs_.remove(cb); }
                if (1 == placement) { core.free_hotspot_cbs_.remove(cb); }
                core.idle_hotspot_cbs_.add_last(cb));
    EXPECT_EQ(OB_EAGAIN, f.primary_ctx_->response_scheduler_for_secondary_tx(
        OB_SUCCESS, f.commit_version()));
    EXPECT_EQ(1, f.handle().get_hotspot_cache_count());
    // Member may have exited, but its cache ref and the P task ref remain.
    EXPECT_GE(f.secondary_ctx_[0]->get_ref(), secondary_refs - 1);
    EXPECT_EQ(primary_refs, f.primary_ctx_->get_ref());
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(f.primary_ctx_, f.handle().resp_task_.primary_tx_ctx_);
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  f.expect_member_released(0);
}

// T16: independent S persisted/logging work is a GC precondition failure, not
// a successful asynchronous abort. Restore the synthetic work before retrying.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_independent_secondary_work_retains_references)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.complete_secondary(0));
  ASSERT_EQ(OB_SUCCESS, f.remove_redo());
  ASSERT_EQ(OB_SUCCESS, f.secondary_ctx_[0]->set_secondary_hotspot_redo_status_by_exit_state(
      TxRedoFlushStatus::SECONDARY_MIGRATE_FAILED));
  const int64_t refs = f.secondary_ctx_[0]->get_ref();
  for (int condition = 0; condition < 2; ++condition) {
    ObTxLogCb cb;
    SCN persisted;
    {
      CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
      persisted = f.secondary_ctx_[0]->exec_info_.max_applying_log_ts_;
      if (0 == condition) { f.secondary_ctx_[0]->exec_info_.max_applying_log_ts_.set_min(); }
      if (1 == condition) { f.secondary_ctx_[0]->busy_cbs_.add_last(&cb); }
    }
    NAMED_DEFER(restore_secondary_work,
                CtxLockGuard guard(f.secondary_ctx_[0]->lock_);
                f.secondary_ctx_[0]->exec_info_.max_applying_log_ts_ = persisted;
                if (1 == condition) { f.secondary_ctx_[0]->busy_cbs_.remove(&cb); });
    EXPECT_EQ(OB_EAGAIN, f.handle().response_scheduler(OB_SUCCESS, f.commit_version()));
    EXPECT_FALSE(f.secondary_ctx_[0]->is_exiting_);
    EXPECT_EQ(refs, f.secondary_ctx_[0]->get_ref());
    EXPECT_EQ(1, f.handle().get_hotspot_cache_count());
  }
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  f.expect_member_released(0);
}

// T15: a queued failure remains a failure even when P subsequently commits.
// The ordinary group-failure branch is intentionally not given the new proof.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_queued_failure_payload_survives_primary_commit)
{
  ResponseCleanupFixture f(bus_);
  ASSERT_EQ(OB_SUCCESS, f.init());
  ResponseCleanupDebugSync sync;
  ASSERT_EQ(OB_SUCCESS, sync.init());
  ASSERT_EQ(OB_SUCCESS, sync.pause(BEFORE_SECONDARY_TX_RESPONSE_TASK_HANDLE));
  ASSERT_EQ(OB_SUCCESS, f.handle().push_response_task(OB_TRANS_KILLED, SCN()));
  ASSERT_EQ(OB_SUCCESS, sync.wait());
  ASSERT_EQ(OB_SUCCESS, f.commit_primary());
  const int64_t primary_refs = f.primary_ctx_->get_ref();
  for (int attempt = 0; attempt < 3; ++attempt) {
    ASSERT_EQ(OB_SUCCESS, f.handle().push_response_task(
        OB_SUCCESS, f.commit_version(), true));
    SpinRLockGuard guard(f.handle().hotspot_lock_);
    EXPECT_EQ(OB_TRANS_KILLED, f.handle().resp_task_.tx_result_);
    EXPECT_FALSE(f.handle().resp_task_.commit_version_.is_valid());
    EXPECT_EQ(static_cast<int64_t>(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER),
              f.handle().resp_task_.get_task_type());
    EXPECT_EQ(primary_refs, f.primary_ctx_->get_ref());
  }
  EXPECT_EQ(0, f.secondary_cb_[0].count_.load());
  ASSERT_EQ(OB_SUCCESS, sync.resume());
  ASSERT_TRUE(f.wait_detached());
  EXPECT_EQ(0, f.handle().get_hotspot_cache_count());
  EXPECT_EQ(primary_refs - 1, f.primary_ctx_->get_ref());
  const int64_t refs_after = f.primary_ctx_->get_ref();
  f.handle().resp_task_.finish();
  f.handle().resp_task_.finish();
  EXPECT_EQ(refs_after, f.primary_ctx_->get_ref());
  EXPECT_EQ(0, f.secondary_cb_[0].count_.load());
  // The pre-existing failure-group policy leaves S to its own session cleanup.
  ASSERT_EQ(OB_SUCCESS, f.node_->rollback_tx(f.secondary()));
}

class ResponseCleanupReenterCallback : public ObITxCallback
{
public:
  ResponseCleanupReenterCallback() : node_(nullptr), guard_(nullptr), reuse_(false),
      count_(0), query_ret_(OB_ERR_UNEXPECTED), cleanup_ret_(OB_ERR_UNEXPECTED), no_response_(false) {}
  void callback(int result) override
  {
    ObTxDesc &desc = guard_->get_tx_desc();
    const ObTransID old_id = desc.tx_id_;
    bool not_needed = false;
    query_ret_ = node_->txs_.check_hotspot_response_not_needed(
        old_id, desc.get_hotspot_primary_tx_id(), not_needed);
    no_response_ = not_needed;
    if (reuse_) {
      cleanup_ret_ = node_->txs_.reuse_tx(desc, DATA_CURRENT_VERSION);
    } else {
      cleanup_ret_ = guard_->release();
    }
    ++count_;
  }
  ObTxNode *node_;
  ObTxDescGuard *guard_;
  bool reuse_;
  int count_;
  int query_ret_;
  int cleanup_ret_;
  bool no_response_;
};

// T06: query inside the real callback, followed by normal release/reuse, catches
// a retained query pin or returning it before unlocking. No test thread escapes.
TEST_F(ObTestHotspotTxLeaderSwitch, response_cleanup_callback_reenters_query_release_and_reuse)
{
  for (int mode = 0; mode < 2; ++mode) {
    MsgBus case_bus;
    ResponseCleanupReenterCallback callback; // Must survive the node on every exit.
    ObTxNode::reset_localtion_adapter();
    const ObAddr saved_self = GCONF.self_addr_;
    DEFER(GCONF.self_addr_ = saved_self);
    auto n1 = new ObTxNode(1, ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 8888), case_bus);
    DEFER(delete(n1));
    ASSERT_EQ(OB_SUCCESS, n1->start());
    GCONF.self_addr_ = n1->addr_;
    PREPARE_TX_PARAM(param);
    ObTxDescGuard guard = n1->get_tx_guard();
    ObTxDesc &desc = guard.get_tx_desc();
    ASSERT_EQ(OB_SUCCESS, n1->start_tx(desc, param));
    const ObTransID old_id = desc.tx_id_;
    const ObTransID primary_id(16004);
    desc.set_hotspot_group_commit_info(primary_id, false);
    callback.node_ = n1;
    callback.guard_ = &guard;
    callback.reuse_ = 1 == mode;
    {
      ObSpinLockGuard lock(desc.lock_);
      desc.state_ = ObTxDesc::State::IN_TERMINATE;
      ASSERT_EQ(OB_SUCCESS, desc.set_commit_cb(&callback));
    }
    ASSERT_EQ(OB_SUCCESS, n1->txs_.handle_tx_commit_result(old_id, OB_SUCCESS, SCN::min_scn()));
    EXPECT_EQ(1, callback.count_);
    EXPECT_EQ(OB_SUCCESS, callback.query_ret_);
    EXPECT_TRUE(callback.no_response_);
    EXPECT_EQ(OB_SUCCESS, callback.cleanup_ret_);
    if (1 == mode) {
      ASSERT_EQ(OB_SUCCESS, n1->start_tx(desc, param));
      ASSERT_NE(old_id, desc.tx_id_);
      bool not_needed = true;
      ASSERT_EQ(OB_SUCCESS, n1->txs_.check_hotspot_response_not_needed(
          desc.tx_id_, primary_id, not_needed));
      EXPECT_FALSE(not_needed);
      EXPECT_EQ(2, desc.get_ref()); // New map registration + caller, no leaked pin.
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, n1->txs_.handle_tx_commit_result(
          old_id, OB_TRANS_KILLED, SCN()));
      EXPECT_EQ(ObTxDesc::State::ACTIVE, desc.state_);
      EXPECT_EQ(1, callback.count_);
      ASSERT_EQ(OB_SUCCESS, n1->rollback_tx(desc));
    }
  }
}

// Farm 5046001390: after leader takeover but before the GTS wait finishes the
// LS is already master yet still not serving. Such a leader owns no in-flight
// hotspot aggregation, so the graceful switch must remain possible. Gating the
// prepare/formal revoke checks on is_leader_serving_ made
// switch_to_follower_gracefully() return OB_NOT_MASTER (ret=-4038 while the
// state was still L_WORKING) and stalled the leader switch for minutes.
TEST_F(ObTestHotspotTxLeaderSwitch, revoke_while_master_but_not_serving)
{
  ObTxNode::reset_localtion_adapter();
  const ObAddr saved_self_addr = GCONF.self_addr_;
  NAMED_DEFER(restore_self_addr, GCONF.self_addr_ = saved_self_addr);
  START_ONE_TX_NODE(n1);
  GCONF.self_addr_ = n1->addr_;

  ObLSTxCtxMgr *mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, n1->txs_.tx_ctx_mgr_.get_ls_tx_ctx_mgr(n1->ls_id_, mgr));
  ASSERT_NE(nullptr, mgr);
  NAMED_DEFER(revert_mgr, if (nullptr != mgr) {
    (void)n1->txs_.tx_ctx_mgr_.revert_ls_tx_ctx_mgr(mgr);
    mgr = nullptr;
  });

  ASSERT_TRUE(mgr->is_master_());
  ASSERT_TRUE(mgr->in_leader_serving_state());
  ASSERT_FALSE(mgr->is_hotspot_aggregation_blocked());
  ASSERT_EQ(0, mgr->get_hotspot_aggregation_count());

  // Reproduce the farm window: master, but the serving flag is not set yet and
  // there is no in-flight aggregation to protect.
  mgr->is_leader_serving_ = false;
  set_errsim_retry_timeout(300 * 1000);
  EXPECT_EQ(OB_SUCCESS, mgr->switch_to_follower_gracefully());
  EXPECT_FALSE(mgr->is_master_());
  EXPECT_TRUE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());

  ASSERT_EQ(OB_SUCCESS, mgr->switch_to_leader());
  n1->wait_all_redolog_applied();
  EXPECT_TRUE(mgr->in_leader_serving_state());
  EXPECT_FALSE(mgr->is_hotspot_aggregation_blocked());
  EXPECT_EQ(0, mgr->get_hotspot_aggregation_count());
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  char log_path[256];
  snprintf(log_path, sizeof(log_path), "test_hotspot_tx_leader_switch_%d.log",
           static_cast<int>(getpid()));
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name(log_path, true, false, log_path, log_path, log_path);
  logger.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
