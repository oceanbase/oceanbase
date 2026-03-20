#include <gtest/gtest.h>
#include <atomic>
#include <cstdlib>
#include <string>
#include <cstdio>
#include <utility>

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"

#define private public
#include "logservice/restoreservice/ob_log_restore_handler.h"
#include "logservice/transportservice/ob_log_transport_task_queue.h"
#undef private

#include "logservice/ipalf/ipalf_handle.h"
#include "unittest/logservice/transport_task_queue_test_utils.h"

namespace oceanbase
{
namespace logservice
{
namespace unittest
{

using namespace common;
using namespace palf;

#ifndef OB_LOG_RESTORE_QUEUE_TEST_INFRA
#define SKIP_IF_NO_TEST_INFRA() do { fprintf(stderr, "[SKIP] OB_LOG_RESTORE_QUEUE_TEST_INFRA not enabled\n"); return; } while (0)
#else
#define SKIP_IF_NO_TEST_INFRA() do {} while (0)
#endif

class FakePalfHandle final : public ipalf::IPalfHandle
{
public:
  FakePalfHandle()
    : end_lsn_(LSN(0)),
      end_log_id_(OB_INVALID_LOG_ID),
      role_(FOLLOWER),
      proposal_id_(0),
      pending_state_(false),
      valid_(true)
  {}

  void set_end_lsn(const LSN &lsn) { end_lsn_ = lsn; }
  void set_end_log_id(const int64_t log_id) { end_log_id_ = log_id; }
  void set_role(const ObRole &role, const int64_t proposal_id, const bool pending_state = false)
  {
    role_ = role;
    proposal_id_ = proposal_id;
    pending_state_ = pending_state;
  }
  void set_valid(const bool valid) { valid_ = valid; }

  bool is_valid() const override { return valid_; }
  bool operator==(const IPalfHandle &rhs) const override { return this == &rhs; }

  int append(const PalfAppendOptions &opts,
             const void *buffer,
             const int64_t nbytes,
             const share::SCN &ref_scn,
             logservice::AppendCb *cb,
             LSN &lsn,
             share::SCN &scn) override
  {
    UNUSED(opts);
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(ref_scn);
    UNUSED(cb);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_NOT_SUPPORTED;
  }

  int raw_write(const PalfAppendOptions &opts,
                const LSN &lsn,
                const void *buffer,
                const int64_t nbytes) override
  {
    UNUSED(opts);
    UNUSED(lsn);
    UNUSED(buffer);
    UNUSED(nbytes);
    return OB_NOT_SUPPORTED;
  }

  int raw_read(const LSN &lsn,
               void *buffer,
               const int64_t nbytes,
               int64_t &read_size,
               LogIOContext &io_ctx) override
  {
    UNUSED(lsn);
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(read_size);
    UNUSED(io_ctx);
    return OB_NOT_SUPPORTED;
  }

  int seek(const LSN &lsn, PalfBufferIterator &iter) override
  {
    UNUSED(lsn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const LSN &lsn, PalfGroupBufferIterator &iter) override
  {
    UNUSED(lsn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const LSN &lsn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter) override
  {
    UNUSED(lsn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const LSN &lsn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter) override
  {
    UNUSED(lsn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const share::SCN &scn, PalfGroupBufferIterator &iter) override
  {
    UNUSED(scn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const share::SCN &scn, PalfBufferIterator &iter) override
  {
    UNUSED(scn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::ILogEntry> &iter) override
  {
    UNUSED(scn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int seek(const share::SCN &scn, ipalf::IPalfIterator<ipalf::IGroupEntry> &iter) override
  {
    UNUSED(scn);
    UNUSED(iter);
    return OB_NOT_SUPPORTED;
  }

  int locate_by_scn_coarsely(const share::SCN &scn, LSN &result_lsn) override
  {
    UNUSED(scn);
    UNUSED(result_lsn);
    return OB_NOT_SUPPORTED;
  }

  int locate_by_lsn_coarsely(const LSN &lsn, share::SCN &result_scn) override
  {
    UNUSED(lsn);
    UNUSED(result_scn);
    return OB_NOT_SUPPORTED;
  }

  int enable_sync() override { return OB_NOT_SUPPORTED; }
  int disable_sync() override { return OB_NOT_SUPPORTED; }
  bool is_sync_enabled() const override { return false; }
  int advance_base_lsn(const LSN &lsn) override { UNUSED(lsn); return OB_NOT_SUPPORTED; }
  int flashback(const int64_t mode_version, const share::SCN &flashback_scn, const int64_t timeout_us) override
  {
    UNUSED(mode_version);
    UNUSED(flashback_scn);
    UNUSED(timeout_us);
    return OB_NOT_SUPPORTED;
  }

  int get_begin_lsn(LSN &lsn) const override { UNUSED(lsn); return OB_NOT_SUPPORTED; }
  int get_begin_scn(share::SCN &scn) const override { UNUSED(scn); return OB_NOT_SUPPORTED; }
  int get_base_lsn(LSN &lsn) const override { UNUSED(lsn); return OB_NOT_SUPPORTED; }
  int get_base_info(const LSN &lsn, PalfBaseInfo &palf_base_info) override
  {
    UNUSED(lsn);
    UNUSED(palf_base_info);
    return OB_NOT_SUPPORTED;
  }

  int get_end_lsn(LSN &lsn) const override
  {
    lsn = end_lsn_;
    return OB_SUCCESS;
  }

  int get_end_scn(share::SCN &scn) const override
  {
    UNUSED(scn);
    return OB_NOT_SUPPORTED;
  }

  int get_max_lsn(LSN &lsn) const override
  {
    lsn = end_lsn_;
    return OB_SUCCESS;
  }

  int get_max_scn(share::SCN &scn) const override
  {
    UNUSED(scn);
    return OB_NOT_SUPPORTED;
  }

  int get_max_log_id(int64_t &log_id) const override
  {
    log_id = end_log_id_;
    return OB_SUCCESS;
  }

  int get_end_log_id(int64_t &log_id) const override
  {
    log_id = end_log_id_;
    return OB_SUCCESS;
  }

  int get_role(common::ObRole &role, int64_t &proposal_id, bool &is_pending_state) const override
  {
    role = role_;
    proposal_id = proposal_id_;
    is_pending_state = pending_state_;
    return OB_SUCCESS;
  }

  int get_role_and_sync_mode(common::ObRole &role,
                             int64_t &proposal_id,
                             ipalf::SyncMode &sync_mode,
                             bool &is_pending_state) const override
  {
    role = role_;
    proposal_id = proposal_id_;
    sync_mode = ipalf::SyncMode::ASYNC;
    is_pending_state = pending_state_;
    return OB_SUCCESS;
  }

  int get_palf_id(int64_t &palf_id) const override { UNUSED(palf_id); return OB_NOT_SUPPORTED; }
  int get_palf_epoch(int64_t &palf_epoch) const override { UNUSED(palf_epoch); return OB_NOT_SUPPORTED; }
  int get_election_leader(common::ObAddr &addr) const override { UNUSED(addr); return OB_NOT_SUPPORTED; }

  int advance_election_epoch_and_downgrade_priority(const int64_t proposal_id,
                                                    const int64_t downgrade_priority_time_us,
                                                    const char *reason) override
  {
    UNUSED(proposal_id);
    UNUSED(downgrade_priority_time_us);
    UNUSED(reason);
    return OB_NOT_SUPPORTED;
  }

  int change_leader_to(const common::ObAddr &dst_addr) override
  {
    UNUSED(dst_addr);
    return OB_NOT_SUPPORTED;
  }

  int change_access_mode(const int64_t proposal_id,
                         const int64_t mode_version,
                         const ipalf::AccessMode &access_mode,
                         const share::SCN &ref_scn) override
  {
    UNUSED(proposal_id);
    UNUSED(mode_version);
    UNUSED(access_mode);
    UNUSED(ref_scn);
    return OB_NOT_SUPPORTED;
  }

  int get_access_mode(int64_t &mode_version, ipalf::AccessMode &access_mode) const override
  {
    UNUSED(mode_version);
    UNUSED(access_mode);
    return OB_NOT_SUPPORTED;
  }

  int get_access_mode(ipalf::AccessMode &access_mode) const override
  {
    UNUSED(access_mode);
    return OB_NOT_SUPPORTED;
  }

  int get_access_mode_version(int64_t &mode_version) const override
  {
    UNUSED(mode_version);
    return OB_NOT_SUPPORTED;
  }

  int get_access_mode_ref_scn(int64_t &mode_version,
                              ipalf::AccessMode &access_mode,
                              share::SCN &ref_scn) const override
  {
    UNUSED(mode_version);
    UNUSED(access_mode);
    UNUSED(ref_scn);
    return OB_NOT_SUPPORTED;
  }

  int change_sync_mode(const int64_t proposal_id,
                       const int64_t mode_version,
                       const ipalf::SyncMode &sync_mode,
                       int64_t &new_mode_version,
                       int64_t &out_proposal_id) override
  {
    UNUSED(proposal_id);
    UNUSED(mode_version);
    UNUSED(sync_mode);
    UNUSED(new_mode_version);
    UNUSED(out_proposal_id);
    return OB_NOT_SUPPORTED;
  }

  int get_sync_mode(int64_t &mode_version, ipalf::SyncMode &sync_mode) const override
  {
    UNUSED(mode_version);
    UNUSED(sync_mode);
    return OB_NOT_SUPPORTED;
  }

  int get_sync_mode(ipalf::SyncMode &sync_mode) const override
  {
    UNUSED(sync_mode);
    return OB_NOT_SUPPORTED;
  }

  int get_sync_mode_version(int64_t &mode_version) const override
  {
    UNUSED(mode_version);
    return OB_NOT_SUPPORTED;
  }

  int register_file_size_cb(palf::PalfFSCb *fs_cb) override
  {
    UNUSED(fs_cb);
    return OB_NOT_SUPPORTED;
  }

  int unregister_file_size_cb() override { return OB_NOT_SUPPORTED; }

  int register_role_change_cb(palf::PalfRoleChangeCb *rc_cb) override
  {
    UNUSED(rc_cb);
    return OB_NOT_SUPPORTED;
  }

  int unregister_role_change_cb() override { return OB_NOT_SUPPORTED; }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int register_refresh_priority_cb() override { return OB_NOT_SUPPORTED; }
  int unregister_refresh_priority_cb() override { return OB_NOT_SUPPORTED; }
  int set_allow_election_without_memlist(const bool allow_election_without_memlist) override
  {
    UNUSED(allow_election_without_memlist);
    return OB_NOT_SUPPORTED;
  }
#endif

  int set_election_priority(palf::election::ElectionPriority *priority) override
  {
    UNUSED(priority);
    return OB_NOT_SUPPORTED;
  }

  int reset_election_priority() override { return OB_NOT_SUPPORTED; }
  int set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb) override
  {
    UNUSED(lc_cb);
    return OB_NOT_SUPPORTED;
  }

  int reset_location_cache_cb() override { return OB_NOT_SUPPORTED; }
  int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb) override
  {
    UNUSED(locality_cb);
    return OB_NOT_SUPPORTED;
  }

  int reset_locality_cb() override { return OB_NOT_SUPPORTED; }
  int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker) override
  {
    UNUSED(reconfig_checker);
    return OB_NOT_SUPPORTED;
  }

  int reset_reconfig_checker_cb() override { return OB_NOT_SUPPORTED; }
  int stat(palf::PalfStat &palf_stat) const override { UNUSED(palf_stat); return OB_NOT_SUPPORTED; }
  int diagnose(palf::PalfDiagnoseInfo &diagnose_info) const override { UNUSED(diagnose_info); return OB_NOT_SUPPORTED; }

#ifdef OB_BUILD_ARBITRATION
  int get_arbitration_member(common::ObMember &arb_member) const override
  {
    UNUSED(arb_member);
    return OB_NOT_SUPPORTED;
  }
#endif

private:
  LSN end_lsn_;
  int64_t end_log_id_;
  ObRole role_;
  int64_t proposal_id_;
  bool pending_state_;
  bool valid_;
};

static share::SCN build_scn()
{
  share::SCN scn = share::SCN::base_scn();
  if (!scn.is_valid()) {
    scn.set_base();
  }
  return scn;
}

static int build_task_with_lsn(const int64_t log_id,
                               const int64_t payload_len,
                               const LSN &start_lsn,
                               ObLogTransportReq *&req,
                               GroupEntryBuffer &buffer)
{
  int ret = OB_SUCCESS;
  share::SCN scn = build_scn();
  if (OB_FAIL(build_group_entry_buffer(log_id, scn, payload_len, buffer))) {
    // do nothing
  } else {
    const int64_t data_size = buffer.size_;
    char *data = buffer.release();
    LSN end_lsn = start_lsn + data_size;
    ret = build_transport_req(start_lsn, end_lsn, scn, data, data_size, req);
  }
  return ret;
}

static void setup_handler(ObLogRestoreHandler &handler,
                          ObILogAllocator *alloc_mgr,
                          ipalf::IPalfHandle *palf_handle,
                          const ObRole &role,
                          const int64_t proposal_id)
{
  handler.id_ = 1;
  handler.palf_handle_ = palf_handle;
  handler.palf_env_ = nullptr;
  handler.role_ = role;
  handler.proposal_id_ = proposal_id;
  handler.is_inited_ = true;
  handler.is_in_stop_state_ = false;
  ASSERT_EQ(OB_SUCCESS, handler.transport_task_queue_.init(alloc_mgr,
                                                          &handler,
                                                          ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
}

class TestLogRestoreHandler : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObTenantMutilAllocatorMgr::get_instance().init();
    ASSERT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(OB_SERVER_TENANT_ID, alloc_mgr_));
  }

  ObILogAllocator *alloc_mgr_ = nullptr;
};

TEST_F(TestLogRestoreHandler, init_invalid_args)
{
  ObLogRestoreHandler handler;
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.init(1, nullptr));
}

TEST_F(TestLogRestoreHandler, stop_destroy_clear_queue)
{
  ObLogRestoreHandler handler;
  handler.is_inited_ = true;
  handler.is_in_stop_state_ = false;
  handler.palf_handle_ = nullptr;
  handler.palf_env_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, handler.transport_task_queue_.init(alloc_mgr_,
                                                          &handler,
                                                          ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task_with_lsn(2, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ObLogTransportTaskHolder task_holder;
  ASSERT_EQ(OB_SUCCESS, ObLogTransportTaskHolder::owned(guard.get(), task_holder));
  guard.release();
  ASSERT_EQ(OB_SUCCESS,
            handler.transport_task_queue_.push(std::move(task_holder), 1));
  EXPECT_GT(handler.transport_task_queue_.cached_bytes_, 0);

  EXPECT_EQ(OB_SUCCESS, handler.stop());
  EXPECT_TRUE(handler.is_in_stop_state_);
  handler.destroy();
  EXPECT_EQ(0, handler.transport_task_queue_.cached_bytes_);
  EXPECT_FALSE(handler.is_inited_);
}

TEST_F(TestLogRestoreHandler, switch_role_clears_queue)
{
  ObLogRestoreHandler handler;
  handler.is_inited_ = true;
  handler.is_in_stop_state_ = false;
  handler.palf_handle_ = nullptr;
  handler.palf_env_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, handler.transport_task_queue_.init(alloc_mgr_,
                                                          &handler,
                                                          ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task_with_lsn(2, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ObLogTransportTaskHolder task_holder;
  ASSERT_EQ(OB_SUCCESS, ObLogTransportTaskHolder::owned(guard.get(), task_holder));
  guard.release();
  ASSERT_EQ(OB_SUCCESS,
            handler.transport_task_queue_.push(std::move(task_holder), 1));
  EXPECT_GT(handler.transport_task_queue_.cached_bytes_, 0);

  handler.switch_role(FOLLOWER, 2);
  handler.switch_role(LEADER, 3);
  EXPECT_EQ(0, handler.transport_task_queue_.cached_bytes_);
}

TEST_F(TestLogRestoreHandler, submit_transport_task_behaviors)
{
  ObLogRestoreHandler handler;
  FakePalfHandle palf_handle;
  palf_handle.set_end_lsn(LSN(1000));
  palf_handle.set_end_log_id(1);
  palf_handle.set_role(LEADER, 1);

  setup_handler(handler, alloc_mgr_, &palf_handle, LEADER, 1);

  ObLogTransportReq invalid_req;
  invalid_req.log_size_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, handler.submit_transport_task(invalid_req));

  GroupEntryBuffer stale_buf;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, stale_buf));
  ObLogTransportReq stale_req;
  stale_req.standby_cluster_id_ = 1;
  stale_req.standby_tenant_id_ = 1;
  stale_req.ls_id_ = share::ObLSID(1);
  stale_req.start_lsn_ = LSN(10);
  stale_req.end_lsn_ = LSN(100);
  stale_req.scn_ = build_scn();
  stale_req.log_data_ = stale_buf.buf_;
  stale_req.log_size_ = stale_buf.size_;
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, handler.submit_transport_task(stale_req));
  EXPECT_EQ(0, handler.transport_task_queue_.cached_bytes_);

  handler.transport_task_queue_.max_cached_bytes_ = ObLogTransportTaskQueue::DEFAULT_MAX_CACHED_BYTES;
  GroupEntryBuffer overflow_buf;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 64, overflow_buf));
  ObLogTransportReq overflow_req;
  overflow_req.standby_cluster_id_ = 1;
  overflow_req.standby_tenant_id_ = 1;
  overflow_req.ls_id_ = share::ObLSID(1);
  overflow_req.start_lsn_ = LSN(1000 + ObLogTransportTaskQueue::DEFAULT_MAX_CACHED_BYTES);
  overflow_req.end_lsn_ = overflow_req.start_lsn_ + overflow_buf.size_;
  overflow_req.scn_ = build_scn();
  overflow_req.log_data_ = overflow_buf.buf_;
  overflow_req.log_size_ = overflow_buf.size_;
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, handler.submit_transport_task(overflow_req));

  handler.transport_task_queue_.max_cached_bytes_ = ObLogTransportTaskQueue::DEFAULT_MAX_CACHED_BYTES;
  GroupEntryBuffer dup_buf1;
  GroupEntryBuffer dup_buf2;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, dup_buf1));
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, dup_buf2));
  ObLogTransportReq dup_req1;
  dup_req1.standby_cluster_id_ = 1;
  dup_req1.standby_tenant_id_ = 1;
  dup_req1.ls_id_ = share::ObLSID(1);
  dup_req1.start_lsn_ = LSN(2000);
  dup_req1.end_lsn_ = dup_req1.start_lsn_ + dup_buf1.size_;
  dup_req1.scn_ = build_scn();
  dup_req1.log_data_ = dup_buf1.buf_;
  dup_req1.log_size_ = dup_buf1.size_;
  EXPECT_EQ(OB_SUCCESS, handler.submit_transport_task(dup_req1));

  ObLogTransportReq dup_req2 = dup_req1;
  dup_req2.log_data_ = dup_buf2.buf_;
  EXPECT_EQ(OB_SUCCESS, handler.submit_transport_task(dup_req2));

  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;
  handler.transport_task_queue_.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, drop_duplicate);
}

TEST_F(TestLogRestoreHandler, submit_transport_task_deep_copy_test)
{
  ObLogRestoreHandler handler;
  FakePalfHandle palf_handle;
  palf_handle.set_end_lsn(LSN(1000));
  palf_handle.set_end_log_id(1);
  palf_handle.set_role(LEADER, 1);

  setup_handler(handler, alloc_mgr_, &palf_handle, LEADER, 1);

  const int64_t log_id = 2;
  GroupEntryBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(log_id, build_scn(), 32, buffer));

  ObLogTransportReq req;
  req.standby_cluster_id_ = 1;
  req.standby_tenant_id_ = 1;
  req.ls_id_ = share::ObLSID(1);
  req.start_lsn_ = LSN(2000);
  req.end_lsn_ = req.start_lsn_ + buffer.size_;
  req.scn_ = build_scn();
  req.log_data_ = buffer.buf_;
  req.log_size_ = buffer.size_;

  std::string original(buffer.buf_, buffer.size_);
  ASSERT_EQ(OB_SUCCESS, handler.submit_transport_task(req));

  memset(buffer.buf_, 'b', buffer.size_);

  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, handler.transport_task_queue_.sw_.get(log_id, slot));
  {
    ObByteLockGuard slot_guard(slot->slot_lock_);
    ASSERT_NE(nullptr, slot->task_holder_.ptr());
    EXPECT_NE(slot->task_holder_.ptr()->log_data_, buffer.buf_);
    EXPECT_EQ(buffer.size_, slot->task_holder_.ptr()->log_size_);
    EXPECT_EQ(0, memcmp(slot->task_holder_.ptr()->log_data_, original.data(), original.size()));
  }
  EXPECT_EQ(OB_SUCCESS, handler.transport_task_queue_.sw_.revert(log_id));
}

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogRestoreHandler, process_transport_tasks_with_raw_write_hook)
{
  ObLogRestoreHandler handler;
  FakePalfHandle palf_handle;
  palf_handle.set_end_lsn(LSN(100));
  palf_handle.set_end_log_id(1);
  palf_handle.set_role(LEADER, 1);

  setup_handler(handler, alloc_mgr_, &palf_handle, LEADER, 1);
  SKIP_IF_NO_TEST_INFRA();

  std::atomic<int64_t> call_count(0);
  handler.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  GroupEntryBuffer buffer;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, buffer));
  ObLogTransportReq req;
  req.standby_cluster_id_ = 1;
  req.standby_tenant_id_ = 1;
  req.ls_id_ = share::ObLSID(1);
  req.start_lsn_ = LSN(100);
  req.end_lsn_ = req.start_lsn_ + buffer.size_;
  req.scn_ = build_scn();
  req.log_data_ = buffer.buf_;
  req.log_size_ = buffer.size_;
  ASSERT_EQ(OB_SUCCESS, handler.submit_transport_task(req));

  EXPECT_EQ(OB_SUCCESS, handler.process_transport_tasks(10));
  EXPECT_EQ(1, call_count.load());

  handler.role_ = FOLLOWER;
  EXPECT_EQ(OB_NOT_MASTER, handler.process_transport_tasks(10));
}
#else
TEST_F(TestLogRestoreHandler, process_transport_tasks_with_raw_write_hook)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogRestoreHandler, print_stat_reports_drop_stats)
{
  ObLogRestoreHandler handler;
  FakePalfHandle palf_handle;
  palf_handle.set_end_lsn(LSN(0));
  palf_handle.set_end_log_id(1);
  palf_handle.set_role(LEADER, 1);

  setup_handler(handler, alloc_mgr_, &palf_handle, LEADER, 1);
  SKIP_IF_NO_TEST_INFRA();

  // Create a known drop counter (duplicate): push same log_id twice.
  GroupEntryBuffer dup_buf1;
  GroupEntryBuffer dup_buf2;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, dup_buf1));
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, dup_buf2));

  ObLogTransportReq dup_req1;
  dup_req1.standby_cluster_id_ = 1;
  dup_req1.standby_tenant_id_ = 1;
  dup_req1.ls_id_ = share::ObLSID(1);
  dup_req1.start_lsn_ = LSN(100);
  dup_req1.end_lsn_ = dup_req1.start_lsn_ + dup_buf1.size_;
  dup_req1.scn_ = build_scn();
  dup_req1.log_data_ = dup_buf1.buf_;
  dup_req1.log_size_ = dup_buf1.size_;
  ASSERT_EQ(OB_SUCCESS, handler.submit_transport_task(dup_req1));

  ObLogTransportReq dup_req2 = dup_req1;
  dup_req2.log_data_ = dup_buf2.buf_;
  ASSERT_EQ(OB_SUCCESS, handler.submit_transport_task(dup_req2));

  // Verify drop stats via get_drop_stats() directly.
  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;
  handler.transport_task_queue_.get_drop_stats(drop_stale,
                                              drop_duplicate,
                                              reject_out_of_window,
                                              parse_fail);
  EXPECT_EQ(0, drop_stale);
  EXPECT_EQ(1, drop_duplicate);
  EXPECT_EQ(0, reject_out_of_window);
  EXPECT_EQ(0, parse_fail);
}
#else
TEST_F(TestLogRestoreHandler, print_stat_reports_drop_stats)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ob_log_restore_handler.log");
  OB_LOGGER.set_file_name("test_ob_log_restore_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  const uint64_t tenant_id = 1001;
  const uint64_t server_tenant_id = OB_SERVER_TENANT_ID;
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(tenant_id, 0)) {
    malloc->create_and_add_tenant_allocator(tenant_id);
  }
  if (NULL == malloc->get_tenant_ctx_allocator(server_tenant_id, 0)) {
    malloc->create_and_add_tenant_allocator(server_tenant_id);
  }
  static oceanbase::share::ObTenantBase tenant_base(tenant_id);
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  ODV_MGR.set_mock_data_version(DATA_CURRENT_VERSION);
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
