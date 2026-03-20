#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstdint>

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_cluster_version.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/ob_debug_sync.h"
#include "lib/allocator/ob_malloc.h"

#define private public
#include "logservice/transportservice/ob_log_transport_task_queue.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"
#undef private

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

static void enable_debug_sync_for_unittest()
{
  // ObDebugSync requires a non-null rpc proxy even for local actions.
  // For local-only actions in unittest, a non-null dummy pointer is enough.
  GDS.set_rpc_proxy(reinterpret_cast<oceanbase::obrpc::ObCommonRpcProxy *>(
      static_cast<intptr_t>(1)));
  // Debug sync points take effect only when debug_sync_timeout > 0.
  // Keep it enabled for deterministic concurrency scheduling in tests.
  GCONF.debug_sync_timeout.set_value("1000000"); // 1s
}

static int set_debug_sync_actions(const std::vector<const char *> &actions)
{
  int ret = OB_SUCCESS;
  ObMalloc allocator;
  ObDSSessionActions sa;
  if (OB_FAIL(sa.init(1024, allocator))) {
    // do nothing
  } else {
    const bool is_global = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(actions.size()); ++i) {
      ret = GDS.add_debug_sync(actions[i], is_global, sa);
    }
    if (OB_SUCC(ret)) {
      ret = GDS.set_thread_local_actions(sa);
    }
  }
  return ret;
}

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

static int build_task(const int64_t log_id,
                      const int64_t payload_len,
                      const LSN &start_lsn,
                      ObLogTransportReq *&req,
                      GroupEntryBuffer &buffer)
{
  return build_task_with_lsn(log_id, payload_len, start_lsn, req, buffer);
}

static bool task_stored_in_slot(ObLogTransportTaskQueue &queue,
                                const int64_t log_id,
                                const ObLogTransportReq *task)
{
  bool stored = false;
  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  if (OB_SUCCESS == queue.sw_.get(log_id, slot)) {
    stored = (slot->task_holder_.ptr() == task);
    queue.sw_.revert(log_id);
  }
  return stored;
}

static int push_owned_task(ObLogTransportTaskQueue &queue,
                           TransportReqGuard &guard,
                           const int64_t end_log_id)
{
  int ret = OB_SUCCESS;
  ObLogTransportTaskHolder task_holder;
  if (OB_FAIL(ObLogTransportTaskHolder::owned(guard.get(), task_holder))) {
    // do nothing
  } else {
    guard.release();
    ret = queue.push(std::move(task_holder), end_log_id);
  }
  return ret;
}

static int push_borrowed_task(ObLogTransportTaskQueue &queue,
                              const ObLogTransportReq *task,
                              const int64_t end_log_id)
{
  int ret = OB_SUCCESS;
  ObLogTransportTaskHolder task_holder;
  if (OB_FAIL(ObLogTransportTaskHolder::borrowed(task, task_holder))) {
    // do nothing
  } else {
    ret = queue.push(std::move(task_holder), end_log_id);
  }
  return ret;
}

class TestLogTransportTaskQueue : public ::testing::Test
{
protected:
  void SetUp() override
  {
    ObTenantMutilAllocatorMgr::get_instance().init();
    ASSERT_EQ(OB_SUCCESS, TMA_MGR_INSTANCE.get_tenant_log_allocator(OB_SERVER_TENANT_ID, alloc_mgr_));
  }

  ObILogAllocator *alloc_mgr_ = nullptr;
};

TEST_F(TestLogTransportTaskQueue, init_destroy_clear_state)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif

  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.init(nullptr, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.init(alloc_mgr_, nullptr, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  EXPECT_TRUE(queue.is_inited_);
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_EQ(OB_INIT_TWICE, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(11, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 10));
  EXPECT_EQ(palf::FIRST_VALID_LOG_ID, queue.sw_.get_begin_sn());
  EXPECT_GT(queue.cached_bytes_, 0);
  EXPECT_FALSE(queue.get_first_slot_info_().has_task_);

  queue.clear();
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_FALSE(queue.get_first_slot_info_().has_task_);

  queue.destroy();
  EXPECT_FALSE(queue.is_inited_);
  EXPECT_EQ(0, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, clear_resets_begin_sn_to_initial)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  const int64_t initial_begin_sn = queue.sw_.get_begin_sn();

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(5, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 4));
  int64_t processed = 0;
  ASSERT_EQ(OB_SUCCESS, queue.process(1, 4, processed, 1));
  const int64_t begin_sn_before_clear = queue.sw_.get_begin_sn();
  ASSERT_GT(begin_sn_before_clear, initial_begin_sn);

  queue.clear();
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(initial_begin_sn, slot));
  EXPECT_EQ(nullptr, slot->task_holder_.ptr());
  queue.sw_.revert(initial_begin_sn);

  if (begin_sn_before_clear != initial_begin_sn) {
    int ret = queue.sw_.get(begin_sn_before_clear, slot);
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(nullptr, slot->task_holder_.ptr());
      queue.sw_.revert(begin_sn_before_clear);
    } else {
      EXPECT_TRUE(OB_ERR_OUT_OF_LOWER_BOUND == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret);
    }
  }
}

TEST_F(TestLogTransportTaskQueue, clear_resets_begin_and_process_advances_to_end_log_id_plus_one)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  const int64_t initial_begin_sn = queue.sw_.get_begin_sn();

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(11, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 10));
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  queue.clear();
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  int64_t processed = 0;
  const int64_t end_log_id = 20;
  EXPECT_EQ(OB_SUCCESS, queue.process(1, end_log_id, processed, 1));
  EXPECT_EQ(0, processed);
  EXPECT_EQ(end_log_id + 1, queue.sw_.get_begin_sn());
}

TEST_F(TestLogTransportTaskQueue, destroy_with_inflight_tasks)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(6, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 5));
  EXPECT_GT(queue.cached_bytes_, 0);

  queue.destroy();
  EXPECT_FALSE(queue.is_inited_);
  EXPECT_EQ(0, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, slot_reset_and_revert_guard)
{
  ObLogTransportTaskQueue::TransportTaskSlot slot;
  EXPECT_FALSE(slot.can_be_slid());

  int64_t cached_bytes = 0;
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(1, 16, start_lsn, req, buffer));
  ASSERT_EQ(OB_SUCCESS, ObLogTransportTaskHolder::owned(req, slot.task_holder_));
  slot.task_bytes_ = req->log_size_;
  slot.cached_bytes_ptr_ = &cached_bytes;
  cached_bytes = req->log_size_;
  slot.is_submitted_ = true;
  EXPECT_TRUE(slot.can_be_slid());
  slot.reset();
  EXPECT_FALSE(slot.can_be_slid());
  EXPECT_EQ(0, cached_bytes);

  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  const int64_t log_id = queue.sw_.get_begin_sn();
  ObLogTransportTaskQueue::TransportTaskSlot *sw_slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(log_id, sw_slot));
  {
    ObLogTransportTaskQueue::SwRevertGuard guard(queue.sw_, log_id, sw_slot);
  }
  EXPECT_EQ(OB_ERR_UNEXPECTED, queue.sw_.revert(log_id));
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(log_id, sw_slot));
}

TEST_F(TestLogTransportTaskQueue, push_validation)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif

  EXPECT_EQ(OB_INVALID_ARGUMENT, push_borrowed_task(queue, nullptr, 1));
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  ObLogTransportReq invalid_req;
  invalid_req.log_size_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, push_borrowed_task(queue, &invalid_req, 1));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  EXPECT_EQ(OB_INVALID_ARGUMENT, push_borrowed_task(queue, guard.get(), OB_INVALID_LOG_ID));
  EXPECT_EQ(OB_SUCCESS, push_borrowed_task(queue, guard.get(), 0));
  EXPECT_GT(queue.cached_bytes_, 0);
}

TEST_F(TestLogTransportTaskQueue, self_heal_recovers_push_after_sw_destroy)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);

  queue.sw_.destroy();
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 1));
  EXPECT_TRUE(task_stored_in_slot(queue, 2, guard.get()));
  queue.clear();
}

TEST_F(TestLogTransportTaskQueue, process_validation_and_self_heal_after_sw_destroy)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  int64_t processed = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.process(INVALID_PROPOSAL_ID, 1, processed, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.process(-2, 1, processed, 1));
  EXPECT_EQ(OB_SUCCESS, queue.process(1, 0, processed, 1));
  EXPECT_EQ(0, processed);
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.process(1, 1, processed, 0));

  queue.sw_.destroy();
  EXPECT_EQ(OB_SUCCESS, queue.process(1, 20, processed, 1));
  EXPECT_EQ(0, processed);
  EXPECT_EQ(21, queue.sw_.get_begin_sn());
}

TEST_F(TestLogTransportTaskQueue, push_size_overflow)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  queue.max_cached_bytes_ = 8;

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 64, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  EXPECT_EQ(OB_SUCCESS, push_borrowed_task(queue, guard.get(), 1));
  EXPECT_GT(queue.cached_bytes_, queue.max_cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, push_borrowed_deep_copy)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);
  ObLogTransportTaskHolder task_holder;
  ASSERT_EQ(OB_SUCCESS, ObLogTransportTaskHolder::borrowed(guard.get(), task_holder));
  common::ObMemAttr attr(MTL_ID(), "StandbyTpTask");
  ASSERT_EQ(OB_SUCCESS, task_holder.ensure_owned(attr));
  ASSERT_EQ(OB_SUCCESS, queue.push(std::move(task_holder), 1));

  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(2, slot));
  ASSERT_NE(nullptr, slot->task_holder_.ptr());
  EXPECT_NE(slot->task_holder_.ptr(), guard.get());
  ASSERT_NE(nullptr, slot->task_holder_.ptr()->log_data_);
  ASSERT_NE(nullptr, guard.get()->log_data_);
  EXPECT_NE(slot->task_holder_.ptr()->log_data_, guard.get()->log_data_);
  EXPECT_EQ(0, MEMCMP(slot->task_holder_.ptr()->log_data_, guard.get()->log_data_, guard.get()->log_size_));
  queue.sw_.revert(2);
}

TEST_F(TestLogTransportTaskQueue, push_parse_failures)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;

  GroupEntryBuffer bad_crc;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(2, build_scn(), 32, bad_crc, true));
  ObLogTransportReq *req = nullptr;
  const int64_t bad_crc_size = bad_crc.size_;
  ASSERT_EQ(OB_SUCCESS, build_transport_req(LSN(200), LSN(200) + bad_crc_size,
                                            build_scn(), bad_crc.release(), bad_crc_size, req));
  TransportReqGuard guard(req);
  EXPECT_NE(OB_SUCCESS, push_borrowed_task(queue, guard.get(), 1));
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, parse_fail);

  GroupEntryBuffer truncated;
  ASSERT_EQ(OB_SUCCESS, build_group_entry_buffer(3, build_scn(), 32, truncated, false, 8));
  ObLogTransportReq *req2 = nullptr;
  const int64_t truncated_size = truncated.size_;
  ASSERT_EQ(OB_SUCCESS, build_transport_req(LSN(300), LSN(300) + truncated_size,
                                            build_scn(), truncated.release(), truncated_size, req2));
  TransportReqGuard guard2(req2);
  EXPECT_NE(OB_SUCCESS, push_borrowed_task(queue, guard2.get(), 1));
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(2, parse_fail);
}

TEST_F(TestLogTransportTaskQueue, base_init_and_advance)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  const int64_t initial_begin_sn = queue.sw_.get_begin_sn();

  GroupEntryBuffer buffer1;
  ObLogTransportReq *req1 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(11, 32, start_lsn, req1, buffer1));
  start_lsn = start_lsn + req1->log_size_;
  TransportReqGuard guard1(req1);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard1, 10));
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  GroupEntryBuffer buffer2;
  ObLogTransportReq *req2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(12, 32, start_lsn, req2, buffer2));
  start_lsn = start_lsn + req2->log_size_;
  TransportReqGuard guard2(req2);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 5));
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  GroupEntryBuffer buffer3;
  ObLogTransportReq *req3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(21, 32, start_lsn, req3, buffer3));
  TransportReqGuard guard3(req3);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard3, 20));
  EXPECT_EQ(initial_begin_sn, queue.sw_.get_begin_sn());

  int64_t processed = 0;
  ASSERT_EQ(OB_SUCCESS, queue.process(1, 10, processed, 10));
  EXPECT_EQ(2, processed);
  EXPECT_EQ(13, queue.sw_.get_begin_sn());

  processed = 0;
  ASSERT_EQ(OB_SUCCESS, queue.process(1, 20, processed, 10));
  EXPECT_EQ(1, processed);
  EXPECT_EQ(22, queue.sw_.get_begin_sn());
  queue.clear();
}

TEST_F(TestLogTransportTaskQueue, push_stale_and_out_of_window)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  int64_t processed = 0;
  ASSERT_EQ(OB_SUCCESS, queue.process(1, 10, processed, 1));
  EXPECT_EQ(0, processed);
  EXPECT_EQ(11, queue.sw_.get_begin_sn());

  GroupEntryBuffer stale;
  ObLogTransportReq *req = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(9, 32, start_lsn, req, stale));
  start_lsn = start_lsn + req->log_size_;
  TransportReqGuard guard(req);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, push_owned_task(queue, guard, 10));
  if (!task_stored_in_slot(queue, 9, guard.get())) {
    // dropped without ownership transfer
  }

  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, drop_stale);

  const int64_t out_log_id = queue.sw_.get_end_sn();
  GroupEntryBuffer out_buf;
  ObLogTransportReq *req2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(out_log_id, 32, start_lsn, req2, out_buf));
  TransportReqGuard guard2(req2);
  EXPECT_EQ(OB_ERR_OUT_OF_UPPER_BOUND, push_owned_task(queue, guard2, 10));
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, reject_out_of_window);
}

TEST_F(TestLogTransportTaskQueue, push_duplicates_single_and_concurrent)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer first_buf;
  ObLogTransportReq *req1 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req1, first_buf));
  const LSN dup_lsn = start_lsn;
  start_lsn = start_lsn + req1->log_size_;
  TransportReqGuard guard1(req1);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard1, 1));
  EXPECT_TRUE(task_stored_in_slot(queue, 2, guard1.get()));

  GroupEntryBuffer dup_buf;
  ObLogTransportReq *req2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, dup_lsn, req2, dup_buf));
  TransportReqGuard guard2(req2);
  ASSERT_EQ(OB_ENTRY_EXIST, push_owned_task(queue, guard2, 1));

  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, drop_duplicate);
  EXPECT_EQ(guard1.get()->log_size_, queue.cached_bytes_);

  SimpleBarrier barrier(2);
  GroupEntryBuffer concurrent_buf1;
  GroupEntryBuffer concurrent_buf2;
  ObLogTransportReq *concurrent_req1 = nullptr;
  ObLogTransportReq *concurrent_req2 = nullptr;
  const LSN concurrent_start_lsn = start_lsn;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, concurrent_start_lsn, concurrent_req1, concurrent_buf1));
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, concurrent_start_lsn, concurrent_req2, concurrent_buf2));
  TransportReqGuard concurrent_guard1(concurrent_req1);
  TransportReqGuard concurrent_guard2(concurrent_req2);
  std::atomic<int> concurrent_ret1(OB_SUCCESS);
  std::atomic<int> concurrent_ret2(OB_SUCCESS);

  std::thread t1([&]() {
    barrier.wait();
    concurrent_ret1 = push_owned_task(queue, concurrent_guard1, 1);
  });
  std::thread t2([&]() {
    barrier.wait();
    concurrent_ret2 = push_owned_task(queue, concurrent_guard2, 1);
  });
  t1.join();
  t2.join();
  EXPECT_TRUE((OB_SUCCESS == concurrent_ret1.load() && OB_ENTRY_EXIST == concurrent_ret2.load())
              || (OB_SUCCESS == concurrent_ret2.load() && OB_ENTRY_EXIST == concurrent_ret1.load()));

  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(2, drop_duplicate);

  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(3, slot));
  const ObLogTransportReq *stored = slot->task_holder_.ptr();
  queue.sw_.revert(3);
  if (stored == concurrent_guard1.get()) {
    concurrent_guard1.release();
  } else if (stored == concurrent_guard2.get()) {
    concurrent_guard2.release();
  }
  queue.clear();
}

TEST_F(TestLogTransportTaskQueue, reserve_overflow_no_longer_evicts)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buf2;
  GroupEntryBuffer buf4;
  GroupEntryBuffer buf5;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req4 = nullptr;
  ObLogTransportReq *req5 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, 32, start_lsn, req4, buf4));
  start_lsn = start_lsn + req4->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(5, 32, start_lsn, req5, buf5));
  start_lsn = start_lsn + req5->log_size_;
  const int64_t bytes = req2->log_size_;
  queue.max_cached_bytes_ = bytes * 3;

  TransportReqGuard guard2(req2);
  TransportReqGuard guard4(req4);
  TransportReqGuard guard5(req5);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard4, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard5, 1));
  EXPECT_EQ(bytes * 3, queue.cached_bytes_);

  GroupEntryBuffer overflow_buf;
  ObLogTransportReq *overflow_req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(6, 32, start_lsn, overflow_req, overflow_buf));
  start_lsn = start_lsn + overflow_req->log_size_;
  TransportReqGuard overflow_guard(overflow_req);
  EXPECT_EQ(OB_SUCCESS, push_owned_task(queue, overflow_guard, 1));
  EXPECT_GT(queue.cached_bytes_, queue.max_cached_bytes_);
  ObLogTransportTaskQueue::TransportTaskSlot *overflow_slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(6, overflow_slot));
  EXPECT_NE(nullptr, overflow_slot->task_holder_.ptr());
  queue.sw_.revert(6);

  GroupEntryBuffer evict_buf;
  ObLogTransportReq *evict_req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, start_lsn, evict_req, evict_buf));
  TransportReqGuard evict_guard(evict_req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, evict_guard, 1));

  EXPECT_GT(queue.cached_bytes_, queue.max_cached_bytes_);
  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(5, slot));
  EXPECT_NE(nullptr, slot->task_holder_.ptr());
  queue.sw_.revert(5);
  queue.clear();
}

TEST_F(TestLogTransportTaskQueue, reserve_insufficient_bytes_is_allowed)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buf2;
  GroupEntryBuffer buf4;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req4 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 1, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, 1, start_lsn, req4, buf4));
  start_lsn = start_lsn + req4->log_size_;
  const int64_t small_bytes = req2->log_size_;
  queue.max_cached_bytes_ = small_bytes * 2;

  TransportReqGuard guard2(req2);
  TransportReqGuard guard4(req4);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard4, 1));
  EXPECT_EQ(small_bytes * 2, queue.cached_bytes_);

  GroupEntryBuffer large_buf;
  ObLogTransportReq *req3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, small_bytes, start_lsn, req3, large_buf));
  ASSERT_GT(req3->log_size_, small_bytes);
  ASSERT_LE(req3->log_size_, queue.max_cached_bytes_);

  TransportReqGuard guard3(req3);
  EXPECT_EQ(OB_SUCCESS, push_owned_task(queue, guard3, 1));
  EXPECT_GT(queue.cached_bytes_, queue.max_cached_bytes_);
  ObLogTransportTaskQueue::TransportTaskSlot *slot3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(3, slot3));
  EXPECT_NE(nullptr, slot3->task_holder_.ptr());
  queue.sw_.revert(3);

  ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
  ASSERT_EQ(OB_SUCCESS, queue.sw_.get(4, slot));
  EXPECT_NE(nullptr, slot->task_holder_.ptr());
  queue.sw_.revert(4);
  queue.clear();
}

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, process_batch_happy_path)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  int64_t processed = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.process(1, OB_INVALID_LOG_ID, processed, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.process(1, OB_INVALID_LOG_ID, processed, 1));

  std::atomic<int64_t> call_count(0);
  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  GroupEntryBuffer buf2;
  GroupEntryBuffer buf3;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req3 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, start_lsn, req3, buf3));
  TransportReqGuard guard2(req2);
  TransportReqGuard guard3(req3);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard3, 1));

  EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 1));
  EXPECT_EQ(1, processed);
  EXPECT_EQ(1, call_count.load());

  EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 1));
  EXPECT_EQ(1, processed);
  EXPECT_EQ(2, call_count.load());
  EXPECT_EQ(0, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, process_gating_by_current_end_lsn)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  std::atomic<int64_t> call_count(0);
  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  GroupEntryBuffer buf2;
  GroupEntryBuffer buf3;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req3 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, start_lsn, req3, buf3));
  TransportReqGuard guard2(req2);
  TransportReqGuard guard3(req3);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard3, 1));

  const int64_t cached_bytes_before_process = queue.cached_bytes_;
  ASSERT_GT(cached_bytes_before_process, 0);

  int64_t processed = 0;
  EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 1));
  EXPECT_EQ(1, processed);
  EXPECT_EQ(1, call_count.load());
  EXPECT_LT(queue.cached_bytes_, cached_bytes_before_process);
}
#else
TEST_F(TestLogTransportTaskQueue, process_batch_happy_path)
{
  SKIP_IF_NO_TEST_INFRA();
}

TEST_F(TestLogTransportTaskQueue, process_gating_by_current_end_lsn)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, process_gap_and_current_end_lsn)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  std::atomic<int64_t> call_count(0);
  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  GroupEntryBuffer gap_buf;
  ObLogTransportReq *gap_req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, start_lsn, gap_req, gap_buf));
  TransportReqGuard gap_guard(gap_req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, gap_guard, 1));

  int64_t processed = 0;
  EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 10));
  EXPECT_EQ(0, processed);
  EXPECT_EQ(0, call_count.load());

  queue.clear();
  GroupEntryBuffer gated_buf;
  ObLogTransportReq *gated_req = nullptr;
  const LSN gated_start_lsn(5000);
  ASSERT_EQ(OB_SUCCESS, build_task_with_lsn(2, 32, gated_start_lsn, gated_req, gated_buf));
  TransportReqGuard gated_guard(gated_req);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, gated_guard, 1));

  EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 10));
  EXPECT_EQ(1, processed);
  EXPECT_EQ(1, call_count.load());
  queue.clear();
}
#else
TEST_F(TestLogTransportTaskQueue, process_gap_and_current_end_lsn)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, process_raw_write_errors)
{
  SKIP_IF_NO_TEST_INFRA();

  // round1: callback returns OB_ERR_OUT_OF_LOWER_BOUND
  {
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

    std::atomic<int64_t> call_count(0);
    queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
      call_count++;
      return OB_ERR_OUT_OF_LOWER_BOUND;
    });

    GroupEntryBuffer buf2;
    ObLogTransportReq *req2 = nullptr;
    LSN start_lsn(1000);
    ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
    TransportReqGuard guard2(req2);
    ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));

    int64_t processed = 0;
    EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 10));
    EXPECT_EQ(0, processed);
    EXPECT_EQ(1, call_count.load());

    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    ASSERT_EQ(OB_SUCCESS, queue.sw_.get(2, slot));
    EXPECT_NE(nullptr, slot->task_holder_.ptr());
    queue.sw_.revert(2);
  }

  // round2: callback returns OB_ERR_UNEXPECTED
  {
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
    queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
      return OB_ERR_UNEXPECTED;
    });

    GroupEntryBuffer buf2;
    ObLogTransportReq *req2 = nullptr;
    LSN start_lsn(1000);
    ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
    TransportReqGuard guard2(req2);
    ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));

    int64_t processed = 0;
    EXPECT_EQ(OB_SUCCESS, queue.process(1, 1, processed, 10));
    EXPECT_EQ(0, processed);
    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    ASSERT_EQ(OB_SUCCESS, queue.sw_.get(2, slot));
    EXPECT_NE(nullptr, slot->task_holder_.ptr());
    queue.sw_.revert(2);
  }
}
#else
TEST_F(TestLogTransportTaskQueue, process_raw_write_errors)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, concurrent_push_process_cached_bytes)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    return OB_SUCCESS;
  });

  SimpleBarrier barrier(2);
  std::atomic<bool> done(false);

  std::thread pusher([&]() {
    barrier.wait();
    LSN start_lsn(1000);
    for (int64_t log_id = 2; log_id < 10; ++log_id) {
      GroupEntryBuffer buffer;
      ObLogTransportReq *req = nullptr;
      if (OB_SUCCESS == build_task(log_id, 16, start_lsn, req, buffer)) {
        start_lsn = start_lsn + req->log_size_;
        TransportReqGuard guard(req);
        (void)push_owned_task(queue, guard, 1);
      }
    }
    done = true;
  });

  std::thread processor([&]() {
    barrier.wait();
    int64_t processed = 0;
    while (!done.load()) {
      queue.process(1, 1, processed, 1);
    }
  });

  pusher.join();
  processor.join();

  EXPECT_GE(queue.cached_bytes_, 0);
  queue.clear();
  EXPECT_EQ(0, queue.cached_bytes_);
}
#else
TEST_F(TestLogTransportTaskQueue, concurrent_push_process_cached_bytes)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, concurrent_push_evict_cached_bytes)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    return OB_SUCCESS;
  });

  const int64_t payload_len = 16;
  LSN start_lsn(1000);
  GroupEntryBuffer buf2;
  GroupEntryBuffer buf4;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req4 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(2, payload_len, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, payload_len, start_lsn, req4, buf4));
  start_lsn = start_lsn + req4->log_size_;

  const int64_t bytes = req2->log_size_;
  queue.max_cached_bytes_ = bytes * 2;

  TransportReqGuard guard2(req2);
  TransportReqGuard guard4(req4);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard4, 1));
  EXPECT_EQ(queue.max_cached_bytes_, queue.cached_bytes_);

  SimpleBarrier barrier(2);
  GroupEntryBuffer buf3;
  GroupEntryBuffer buf4_new;
  ObLogTransportReq *req3 = nullptr;
  ObLogTransportReq *req4_new = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, payload_len, start_lsn, req3, buf3));
  start_lsn = start_lsn + req3->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, payload_len, start_lsn, req4_new, buf4_new));
  TransportReqGuard guard3(req3);
  TransportReqGuard guard4_new(req4_new);

  std::thread evicting_pusher([&]() {
    barrier.wait();
    (void)push_owned_task(queue, guard3, 1);
    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    if (OB_SUCCESS == queue.sw_.get(3, slot)) {
      if (slot->task_holder_.ptr() == guard3.get()) {
        guard3.release();
      }
      queue.sw_.revert(3);
    }
  });

  std::thread duplicate_pusher([&]() {
    barrier.wait();
    (void)push_owned_task(queue, guard4_new, 1);
    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    if (OB_SUCCESS == queue.sw_.get(4, slot)) {
      if (slot->task_holder_.ptr() == guard4_new.get()) {
        guard4_new.release();
      }
      queue.sw_.revert(4);
    }
  });

  evicting_pusher.join();
  duplicate_pusher.join();

  EXPECT_GE(queue.cached_bytes_, 0);
  EXPECT_GE(queue.cached_bytes_, queue.max_cached_bytes_);
  queue.clear();
  EXPECT_EQ(0, queue.cached_bytes_);
}
#else
TEST_F(TestLogTransportTaskQueue, concurrent_push_evict_cached_bytes)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, concurrent_process_process)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  std::atomic<int64_t> call_count(0);
  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  const int64_t payload_len = 16;
  LSN start_lsn(1000);
  const int64_t task_count = 5;
  for (int64_t log_id = 2; log_id < 2 + task_count; ++log_id) {
    GroupEntryBuffer buffer;
    ObLogTransportReq *req = nullptr;
    ASSERT_EQ(OB_SUCCESS, build_task(log_id, payload_len, start_lsn, req, buffer));
    start_lsn = start_lsn + req->log_size_;
    TransportReqGuard guard(req);
    ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard, 1));
  }

  SimpleBarrier barrier(2);
  std::atomic<bool> done(false);
  auto processor = [&]() {
    barrier.wait();
    for (int64_t i = 0; i < task_count * 8 && !done.load(); ++i) {
      int64_t processed = 0;
      queue.process(1, 1, processed, 1);
      if (call_count.load() >= task_count) {
        done = true;
      } else if (processed <= 0) {
        std::this_thread::yield();
      }
    }
  };

  std::thread t1(processor);
  std::thread t2(processor);
  t1.join();
  t2.join();

  EXPECT_EQ(task_count, call_count.load());
  EXPECT_EQ(0, queue.cached_bytes_);
  queue.clear();
}
#else
TEST_F(TestLogTransportTaskQueue, concurrent_process_process)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

TEST_F(TestLogTransportTaskQueue, debug_sync_advance_base_makes_pending_push_stale)
{
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);

  std::atomic<int> push_ret(OB_SUCCESS);
  std::atomic<int> process_ret(OB_SUCCESS);

  std::thread t_push([&]() {
    push_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_PUSH_BEFORE_SW_GET wait_for adv timeout 1000000 execute 1",
    });
    if (OB_SUCCESS == push_ret.load()) {
      push_ret = push_owned_task(queue, guard, /*end_log_id*/ 1);
    }
  });

  std::thread t_process([&]() {
    process_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_TRY_INIT_BASE_AFTER_TRUNCATE_RESET signal adv execute 1",
    });
    if (OB_SUCCESS == process_ret.load()) {
      int64_t processed = 0;
      process_ret = queue.process(/*proposal_id*/ 1, /*end_log_id*/ 100, processed, /*batch_size*/ 1);
    }
  });

  t_push.join();
  t_process.join();

  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, push_ret.load());
  EXPECT_EQ(OB_SUCCESS, process_ret.load());

  int64_t drop_stale = 0;
  int64_t drop_duplicate = 0;
  int64_t reject_out_of_window = 0;
  int64_t parse_fail = 0;
  queue.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
  EXPECT_EQ(1, drop_stale);
  EXPECT_EQ(0, drop_duplicate);
  EXPECT_EQ(0, reject_out_of_window);
  EXPECT_EQ(0, parse_fail);
  EXPECT_EQ(0, queue.cached_bytes_);
  queue.clear();
}

TEST_F(TestLogTransportTaskQueue, debug_sync_push_reserve_and_process_advance_base_no_hang_with_watchdog)
{
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  auto run_one_case = [&](const char *push_wait_action,
                          const char *process_wait_action,
                          const char *process_signal_action) -> bool {
    struct PushProcessCaseCtx
    {
      PushProcessCaseCtx()
        : req_(nullptr),
          push_ret_(OB_SUCCESS),
          process_ret_(OB_SUCCESS),
          push_done_(false),
          process_done_(false)
      {}
      GroupEntryBuffer buffer_;
      ObLogTransportReq *req_;
      std::unique_ptr<TransportReqGuard> guard_;
      ObLogTransportTaskQueue queue_;
      ObLogRestoreHandler handler_;
      std::atomic<int> push_ret_;
      std::atomic<int> process_ret_;
      std::atomic<bool> push_done_;
      std::atomic<bool> process_done_;
    };

    auto ctx = std::make_shared<PushProcessCaseCtx>();
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
    ctx->queue_.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
    if (OB_SUCCESS != ctx->queue_.init(alloc_mgr_, &ctx->handler_, ObLogTransportTaskQueue::MAX_QUEUE_SIZE)) {
      return false;
    }
    if (OB_SUCCESS != build_task(/*log_id*/ 2, /*payload_len*/ 32, LSN(1000), ctx->req_, ctx->buffer_)) {
      return false;
    }
    ctx->guard_.reset(new TransportReqGuard(ctx->req_));

    std::thread t_push([ctx, push_wait_action]() {
      ctx->push_ret_ = set_debug_sync_actions({push_wait_action});
      if (OB_SUCCESS == ctx->push_ret_.load()) {
        ctx->push_ret_ = push_owned_task(ctx->queue_, *ctx->guard_, /*end_log_id*/ 1);
      }
      ctx->push_done_.store(true);
    });

    std::thread t_process([ctx, process_wait_action, process_signal_action]() {
      ctx->process_ret_ = set_debug_sync_actions({process_wait_action, process_signal_action});
      if (OB_SUCCESS == ctx->process_ret_.load()) {
        int64_t processed = 0;
        ctx->process_ret_ = ctx->queue_.process(/*proposal_id*/ 1,
                                                /*end_log_id*/ 100,
                                                processed,
                                                /*batch_size*/ 1);
      }
      ctx->process_done_.store(true);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!(ctx->push_done_.load() && ctx->process_done_.load())
           && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!(ctx->push_done_.load() && ctx->process_done_.load())) {
      ADD_FAILURE() << "watchdog timeout: push/process did not finish in 5s";
      if (t_push.joinable()) {
        t_push.detach();
      }
      if (t_process.joinable()) {
        t_process.detach();
      }
      return false;
    }

    t_push.join();
    t_process.join();

    const int push_ret = ctx->push_ret_.load();
    EXPECT_TRUE(OB_SUCCESS == push_ret
                || OB_ERR_OUT_OF_LOWER_BOUND == push_ret)
        << "unexpected push ret=" << push_ret;
    EXPECT_EQ(OB_SUCCESS, ctx->process_ret_.load());
    EXPECT_GE(ctx->queue_.cached_bytes_, 0);

    int64_t drop_stale = 0;
    int64_t drop_duplicate = 0;
    int64_t reject_out_of_window = 0;
    int64_t parse_fail = 0;
    ctx->queue_.get_drop_stats(drop_stale, drop_duplicate, reject_out_of_window, parse_fail);
    EXPECT_GE(drop_stale, 0);
    EXPECT_GE(drop_duplicate, 0);
    EXPECT_GE(reject_out_of_window, 0);
    EXPECT_EQ(0, parse_fail);
    ctx->queue_.clear();
    return true;
  };

  ASSERT_TRUE(run_one_case(
      "LOG_TP_QUEUE_PUSH_BEFORE_SET_SLOT signal reserve_after_ready wait_for reserve_after_advanced timeout 1000000 execute 1",
      "LOG_TP_QUEUE_TRY_INIT_BASE_BEFORE_TRUNCATE_RESET wait_for reserve_after_ready timeout 1000000 execute 1",
      "LOG_TP_QUEUE_TRY_INIT_BASE_AFTER_TRUNCATE_RESET signal reserve_after_advanced execute 1"));
}

TEST_F(TestLogTransportTaskQueue, debug_sync_push_evict_slow_path_and_process_advance_base_no_hang_with_watchdog)
{
  // Eviction path was removed; keep this case as a no-op placeholder.
  return;
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  struct PushEvictProcessCaseCtx
  {
    PushEvictProcessCaseCtx()
      : req2_(nullptr),
        req3_(nullptr),
        req4_(nullptr),
        push_ret_(OB_SUCCESS),
        process_ret_(OB_SUCCESS),
        push_done_(false),
        process_done_(false)
    {}
    GroupEntryBuffer buf2_;
    GroupEntryBuffer buf3_;
    GroupEntryBuffer buf4_;
    ObLogTransportReq *req2_;
    ObLogTransportReq *req3_;
    ObLogTransportReq *req4_;
    std::unique_ptr<TransportReqGuard> guard2_;
    std::unique_ptr<TransportReqGuard> guard3_;
    std::unique_ptr<TransportReqGuard> guard4_;
    ObLogTransportTaskQueue queue_;
    ObLogRestoreHandler handler_;
    std::atomic<int> push_ret_;
    std::atomic<int> process_ret_;
    std::atomic<bool> push_done_;
    std::atomic<bool> process_done_;
  };

  auto ctx = std::make_shared<PushEvictProcessCaseCtx>();
  ASSERT_EQ(OB_SUCCESS, ctx->queue_.init(alloc_mgr_, &ctx->handler_, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(/*log_id*/ 2, /*payload_len*/ 128, start_lsn, ctx->req2_, ctx->buf2_));
  start_lsn = start_lsn + ctx->req2_->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(/*log_id*/ 3, /*payload_len*/ 128, start_lsn, ctx->req3_, ctx->buf3_));
  start_lsn = start_lsn + ctx->req3_->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(/*log_id*/ 4, /*payload_len*/ 128, start_lsn, ctx->req4_, ctx->buf4_));

  const int64_t task_bytes = ctx->req2_->log_size_;
  ctx->queue_.max_cached_bytes_ = task_bytes * 2;

  ctx->guard2_.reset(new TransportReqGuard(ctx->req2_));
  ctx->guard3_.reset(new TransportReqGuard(ctx->req3_));
  ctx->guard4_.reset(new TransportReqGuard(ctx->req4_));

  // Fill memory with (2,4), then push(3) must enter try_reserve_with_evict_ slow path.
  ASSERT_EQ(OB_SUCCESS, push_owned_task(ctx->queue_, *ctx->guard2_, /*end_log_id*/ 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(ctx->queue_, *ctx->guard4_, /*end_log_id*/ 1));
  ASSERT_EQ(task_bytes * 2, ctx->queue_.cached_bytes_);

  std::thread t_push([ctx]() {
    ctx->push_ret_ = set_debug_sync_actions({
        "LOG_TP_QUEUE_EVICT_IN_RANGE_BEFORE_SW_GET signal evict_ready wait_for evict_advanced timeout 1000000 execute 1",
    });
    if (OB_SUCCESS == ctx->push_ret_.load()) {
      ctx->push_ret_ = push_owned_task(ctx->queue_, *ctx->guard3_, /*end_log_id*/ 1);
    }
    ctx->push_done_.store(true);
  });

  std::thread t_process([ctx]() {
    ctx->process_ret_ = set_debug_sync_actions({
        "LOG_TP_QUEUE_TRY_INIT_BASE_BEFORE_TRUNCATE_RESET wait_for evict_ready timeout 1000000 execute 1",
        "LOG_TP_QUEUE_TRY_INIT_BASE_AFTER_TRUNCATE_RESET signal evict_advanced execute 1",
    });
    if (OB_SUCCESS == ctx->process_ret_.load()) {
      int64_t processed = 0;
      ctx->process_ret_ = ctx->queue_.process(/*proposal_id*/ 1,
                                              /*end_log_id*/ 100,
                                              processed,
                                              /*batch_size*/ 1);
    }
    ctx->process_done_.store(true);
  });

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!(ctx->push_done_.load() && ctx->process_done_.load())
         && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  if (!(ctx->push_done_.load() && ctx->process_done_.load())) {
    ADD_FAILURE() << "watchdog timeout: evict push/process did not finish in 5s";
    if (t_push.joinable()) {
      t_push.detach();
    }
    if (t_process.joinable()) {
      t_process.detach();
    }
    return;
  }

  t_push.join();
  t_process.join();

  const int push_ret = ctx->push_ret_.load();
  EXPECT_TRUE(OB_SUCCESS == push_ret
              || OB_ERR_OUT_OF_LOWER_BOUND == push_ret)
      << "unexpected push ret=" << push_ret;
  EXPECT_EQ(OB_SUCCESS, ctx->process_ret_.load());
  EXPECT_GE(ctx->queue_.cached_bytes_, 0);
  EXPECT_LE(ctx->queue_.cached_bytes_, ctx->queue_.max_cached_bytes_);

  ctx->queue_.clear();
}

TEST_F(TestLogTransportTaskQueue, debug_sync_push_then_clear_truncate_ordered_by_lock)
{
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  const LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req, buffer));
  TransportReqGuard guard(req);

  std::atomic<int> push_ret(OB_SUCCESS);
  std::atomic<int> clear_setup_ret(OB_SUCCESS);

  std::thread t_push([&]() {
    push_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_PUSH_AFTER_QUEUE_LOCK wait_for clear_ready timeout 1000000 execute 1",
        "LOG_TP_QUEUE_PUSH_AFTER_SET_SLOT signal pushed execute 1",
    });
    if (OB_SUCCESS == push_ret.load()) {
      push_ret = push_owned_task(queue, guard, /*end_log_id*/ 1);
    }
  });

  std::thread t_clear([&]() {
    clear_setup_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_CLEAR_BEFORE_QUEUE_LOCK signal clear_ready wait_for pushed timeout 1000000 execute 1",
    });
    if (OB_SUCCESS == clear_setup_ret.load()) {
      queue.clear();
    }
  });

  t_push.join();
  t_clear.join();

  EXPECT_EQ(OB_SUCCESS, push_ret.load());
  EXPECT_EQ(OB_SUCCESS, clear_setup_ret.load());
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_FALSE(queue.get_first_slot_info_().has_task_);
}

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, debug_sync_pull_waits_and_push_fills_next_slot)
{
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  std::atomic<int64_t> call_count(0);
  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    call_count++;
    return OB_SUCCESS;
  });

  // Push log_id=2 first.
  GroupEntryBuffer buf2;
  ObLogTransportReq *req2 = nullptr;
  LSN start_lsn(1000);
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  TransportReqGuard guard2(req2);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, /*end_log_id*/ 1));

  // Prepare log_id=3 for concurrent push.
  GroupEntryBuffer buf3;
  ObLogTransportReq *req3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, start_lsn, req3, buf3));
  TransportReqGuard guard3(req3);

  std::atomic<int> push_ret(OB_SUCCESS);
  std::atomic<int> process_ret(OB_SUCCESS);

  std::thread t_process([&]() {
    process_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_SLIDING_CB_BEFORE_RAW_WRITE signal ready wait_for pushed3 timeout 1000000 execute 1",
    });
    if (OB_SUCCESS == process_ret.load()) {
      int64_t processed = 0;
      process_ret = queue.process(/*proposal_id*/ 1, /*end_log_id*/ 1, processed, /*batch_size*/ 2);
    }
  });

  std::thread t_push([&]() {
    push_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_PUSH_BEFORE_SW_GET wait_for ready timeout 1000000 execute 1",
        "LOG_TP_QUEUE_PUSH_AFTER_SET_SLOT signal pushed3 execute 1",
    });
    if (OB_SUCCESS == push_ret.load()) {
      push_ret = push_owned_task(queue, guard3, /*end_log_id*/ 1);
    }
  });

  t_process.join();
  t_push.join();

  EXPECT_EQ(OB_SUCCESS, process_ret.load());
  EXPECT_EQ(OB_SUCCESS, push_ret.load());
  EXPECT_EQ(2, call_count.load());
  EXPECT_EQ(0, queue.cached_bytes_);
  queue.clear();
}
#else
TEST_F(TestLogTransportTaskQueue, debug_sync_pull_waits_and_push_fills_next_slot)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

TEST_F(TestLogTransportTaskQueue, debug_sync_evict_reset_interleaves_with_concurrent_push_on_same_slot)
{
  // Eviction interleaving is obsolete after eviction path removal.
  return;
  enable_debug_sync_for_unittest();
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));

  // Build tasks with different sizes to:
  // - fill memory nearly full with (2,4)
  // - force push(3) to evict slot(4)
  // - still allow a smaller re-push(4) after eviction
  LSN start_lsn(1000);
  GroupEntryBuffer buf2;
  GroupEntryBuffer buf3;
  GroupEntryBuffer buf4;
  GroupEntryBuffer buf4_new;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req3 = nullptr;
  ObLogTransportReq *req4 = nullptr;
  ObLogTransportReq *req4_new = nullptr;

  ASSERT_EQ(OB_SUCCESS, build_task(2, /*payload_len*/ 128, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(3, /*payload_len*/ 64, start_lsn, req3, buf3));
  start_lsn = start_lsn + req3->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, /*payload_len*/ 128, start_lsn, req4, buf4));
  // small re-push for log_id=4
  ASSERT_EQ(OB_SUCCESS, build_task(4, /*payload_len*/ 32, start_lsn, req4_new, buf4_new));

  const int64_t bytes2 = req2->log_size_;
  const int64_t bytes3 = req3->log_size_;
  const int64_t bytes4 = req4->log_size_;
  const int64_t bytes4_new = req4_new->log_size_;
  // Leave (bytes3 - 1) slack so reserving bytes3 fails by 1 and triggers eviction.
  queue.max_cached_bytes_ = bytes2 + bytes4 + bytes3 - 1;

  TransportReqGuard guard2(req2);
  TransportReqGuard guard4(req4);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, /*end_log_id*/ 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard4, /*end_log_id*/ 1));
  ASSERT_EQ(bytes2 + bytes4, queue.cached_bytes_);

  TransportReqGuard guard3(req3);
  TransportReqGuard guard4n(req4_new);

  std::atomic<int> push3_ret(OB_SUCCESS);
  std::atomic<int> push4_ret(OB_SUCCESS);
  std::atomic<bool> stored4n(false);

  std::thread t_push3([&]() {
    // During eviction of slot(4), hold the slot lock and wait until push(4) is ready to contend.
    push3_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_EVICT_IN_RANGE_BEFORE_SLOT_RESET signal ev1 wait_for ds1 timeout 1000000 execute 1",
    });
    if (OB_SUCCESS == push3_ret.load()) {
      push3_ret = push_owned_task(queue, guard3, /*end_log_id*/ 1);
    }
  });

  std::thread t_push4([&]() {
    // Wait until evict holds slot(4), then signal "ds1" right before trying to take slot_lock_.
    push4_ret = set_debug_sync_actions({
        "LOG_TP_QUEUE_PUSH_AFTER_SW_GET wait_for ev1 timeout 1000000 execute 1",
        "LOG_TP_QUEUE_PUSH_BEFORE_DUP_CHECK signal ds1 execute 1",
    });
    if (OB_SUCCESS == push4_ret.load()) {
      push4_ret = push_owned_task(queue, guard4n, /*end_log_id*/ 1);
      if (OB_SUCCESS == push4_ret.load()) {
        stored4n = task_stored_in_slot(queue, 4, guard4n.get());
      }
    }
  });

  t_push3.join();
  t_push4.join();

  EXPECT_EQ(OB_SUCCESS, push3_ret.load());
  EXPECT_EQ(OB_SUCCESS, push4_ret.load());
  EXPECT_TRUE(stored4n.load());
  EXPECT_GE(queue.cached_bytes_, 0);
  EXPECT_LE(queue.cached_bytes_, queue.max_cached_bytes_);
  // Sanity: should be able to fit (2,3,4_new) after evict.
  EXPECT_LE(bytes2 + bytes3 + bytes4_new, queue.max_cached_bytes_);
  queue.clear();
}

#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
TEST_F(TestLogTransportTaskQueue, concurrent_push_process_evict)
{
  ObLogTransportTaskQueue queue;
  ObLogRestoreHandler handler;
#ifdef OB_LOG_RESTORE_QUEUE_TEST_INFRA
  queue.set_raw_write_test_hook([](int64_t, const LSN &, const share::SCN &, const char *, int64_t) { return OB_SUCCESS; });
#endif
  ASSERT_EQ(OB_SUCCESS, queue.init(alloc_mgr_, &handler, ObLogTransportTaskQueue::MAX_QUEUE_SIZE));
  SKIP_IF_NO_TEST_INFRA();

  queue.set_raw_write_test_hook([&](const int64_t, const LSN &, const share::SCN &, const char *, const int64_t) {
    return OB_SUCCESS;
  });

  const int64_t payload_len = 16;
  LSN start_lsn(1000);
  GroupEntryBuffer buf2;
  GroupEntryBuffer buf4;
  ObLogTransportReq *req2 = nullptr;
  ObLogTransportReq *req4 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(2, payload_len, start_lsn, req2, buf2));
  start_lsn = start_lsn + req2->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, payload_len, start_lsn, req4, buf4));
  start_lsn = start_lsn + req4->log_size_;

  const int64_t bytes = req2->log_size_;
  queue.max_cached_bytes_ = bytes * 2;

  TransportReqGuard guard2(req2);
  TransportReqGuard guard4(req4);
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard2, 1));
  ASSERT_EQ(OB_SUCCESS, push_owned_task(queue, guard4, 1));

  SimpleBarrier barrier(3);
  std::atomic<bool> evict_done(false);
  std::atomic<int64_t> processed_total(0);

  GroupEntryBuffer buf3;
  GroupEntryBuffer buf4_new;
  ObLogTransportReq *req3 = nullptr;
  ObLogTransportReq *req4_new = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, payload_len, start_lsn, req3, buf3));
  start_lsn = start_lsn + req3->log_size_;
  ASSERT_EQ(OB_SUCCESS, build_task(4, payload_len, start_lsn, req4_new, buf4_new));
  TransportReqGuard guard3(req3);
  TransportReqGuard guard4_new(req4_new);
  std::thread evicting_pusher([&]() {
    barrier.wait();
    (void)push_owned_task(queue, guard3, 1);
    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    if (OB_SUCCESS == queue.sw_.get(3, slot)) {
      if (slot->task_holder_.ptr() == guard3.get()) {
        guard3.release();
      }
      queue.sw_.revert(3);
    }
    evict_done = true;
  });

  std::thread duplicate_pusher([&]() {
    barrier.wait();
    (void)push_owned_task(queue, guard4_new, 1);
    ObLogTransportTaskQueue::TransportTaskSlot *slot = nullptr;
    if (OB_SUCCESS == queue.sw_.get(4, slot)) {
      if (slot->task_holder_.ptr() == guard4_new.get()) {
        guard4_new.release();
      }
      queue.sw_.revert(4);
    }
  });

  std::thread processor([&]() {
    barrier.wait();
    while (!evict_done.load()) {
      std::this_thread::yield();
    }
    for (int64_t i = 0; i < 10; ++i) {
      int64_t processed = 0;
      queue.process(1, 1, processed, 1);
      if (processed > 0) {
        processed_total.fetch_add(processed);
      }
    }
  });

  evicting_pusher.join();
  duplicate_pusher.join();
  processor.join();

  EXPECT_GE(queue.cached_bytes_, 0);
  EXPECT_GE(processed_total.load(), 1);
  queue.clear();
  EXPECT_EQ(0, queue.cached_bytes_);
}
#else
TEST_F(TestLogTransportTaskQueue, concurrent_push_process_evict)
{
  SKIP_IF_NO_TEST_INFRA();
}
#endif

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ob_log_transport_task_queue.log");
  OB_LOGGER.set_file_name("test_ob_log_transport_task_queue.log", true);
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
