#include <gtest/gtest.h>
#include <atomic>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_tenant_data_version_mgr.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"

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

static share::SCN build_scn()
{
  share::SCN scn = share::SCN::base_scn();
  if (!scn.is_valid()) {
    scn.set_base();
  }
  return scn;
}

static int build_task(const int64_t log_id,
                      const int64_t payload_len,
                      const LSN &start_lsn,
                      ObLogTransportReq *&req,
                      GroupEntryBuffer &buffer)
{
  int ret = OB_SUCCESS;
  share::SCN scn = build_scn();
  if (OB_FAIL(build_group_entry_buffer(log_id, scn, payload_len, buffer))) {
  } else {
    const int64_t data_size = buffer.size_;
    char *data = buffer.release();
    ret = build_transport_req(start_lsn, start_lsn + data_size, scn, data, data_size, req);
  }
  return ret;
}

static int seed_next_submit_lsn(ObLogTransportTaskQueue &queue, const LSN &lsn)
{
  queue.switch_to_leader(lsn);
  return OB_SUCCESS;
}

static int init_queue(ObLogTransportTaskQueue &queue,
                      const int64_t queue_size = ObLogTransportTaskQueue::MAX_QUEUE_SIZE)
{
  return queue.init(1, queue_size);
}

static int push_task(ObLogTransportTaskQueue &queue, const ObLogTransportReq *task)
{
  return queue.push(task);
}

static bool task_exists(ObLogTransportTaskQueue &queue, const LSN &start_lsn)
{
  ObLogTransportTaskHandle handle;
  return OB_SUCCESS == queue.task_map_.get(ObLogTransportTaskQueue::LSNWarp(start_lsn), handle)
      && handle.is_valid();
}

class TestLogTransportTaskQueue : public ::testing::Test
{
protected:
  void SetUp() override
  {
    (void)ObTenantMutilAllocatorMgr::get_instance().init();
  }
};

TEST_F(TestLogTransportTaskQueue, init_destroy_clear_state)
{
  ObLogTransportTaskQueue queue;

  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.init(1, 3));
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  EXPECT_TRUE(queue.is_inited_);
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_EQ(OB_INIT_TWICE, init_queue(queue));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(11, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));
  EXPECT_GT(queue.cached_bytes_, 0);
  EXPECT_TRUE(task_exists(queue, LSN(100)));

  queue.switch_to_follower();
  EXPECT_EQ(0, queue.cached_bytes_);
  EXPECT_FALSE(queue.next_submit_lsn_.is_valid());
  EXPECT_EQ(0, queue.task_map_.count());

  queue.stop();
  EXPECT_EQ(OB_IN_STOP_STATE, push_task(queue, guard.get()));
  queue.destroy();
  EXPECT_FALSE(queue.is_inited_);
  EXPECT_EQ(0, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, push_requires_seeded_submit_lsn)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);

  EXPECT_EQ(OB_STATE_NOT_MATCH, push_task(queue, guard.get()));
  EXPECT_EQ(0, queue.cached_bytes_);

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  EXPECT_EQ(OB_SUCCESS, push_task(queue, guard.get()));
  EXPECT_EQ(guard.get()->log_size_, queue.cached_bytes_);
  EXPECT_TRUE(task_exists(queue, LSN(100)));
}

TEST_F(TestLogTransportTaskQueue, push_stale_duplicate_and_invalid_task)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));

  ObLogTransportReq invalid_req;
  invalid_req.log_size_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, push_task(queue, &invalid_req));

  GroupEntryBuffer stale_buf;
  ObLogTransportReq *stale_req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(10), stale_req, stale_buf));
  TransportReqGuard stale_guard(stale_req);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, push_task(queue, stale_guard.get()));

  GroupEntryBuffer first_buf;
  GroupEntryBuffer dup_buf;
  ObLogTransportReq *first_req = nullptr;
  ObLogTransportReq *dup_req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, LSN(100), first_req, first_buf));
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, LSN(100), dup_req, dup_buf));
  TransportReqGuard first_guard(first_req);
  TransportReqGuard dup_guard(dup_req);

  EXPECT_EQ(OB_SUCCESS, push_task(queue, first_guard.get()));
  EXPECT_EQ(OB_SUCCESS, push_task(queue, dup_guard.get()));
  EXPECT_EQ(1, queue.drop_duplicate_cnt_);
  EXPECT_EQ(first_guard.get()->log_size_, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, deep_copy_keeps_original_buffer_independent)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));

  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(3, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  std::string original(guard.get()->log_data_, guard.get()->log_size_);

  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));
  memset(const_cast<char *>(guard.get()->log_data_), 'b', guard.get()->log_size_);

  ObLogTransportTaskHandle handle;
  ASSERT_EQ(OB_SUCCESS, queue.task_map_.get(ObLogTransportTaskQueue::LSNWarp(LSN(100)), handle));
  ASSERT_TRUE(handle.is_valid());
  ASSERT_NE(nullptr, handle.task());
  EXPECT_NE(handle.task()->log_data_, guard.get()->log_data_);
  EXPECT_EQ(original.size(), handle.task()->log_size_);
  EXPECT_EQ(0, memcmp(handle.task()->log_data_, original.data(), original.size()));
}

TEST_F(TestLogTransportTaskQueue, front_success_contiguous_tasks_until_gap)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));

  std::vector<LSN> front_lsn;

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buf1;
  GroupEntryBuffer buf2;
  ObLogTransportReq *req1 = nullptr;
  ObLogTransportReq *req2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req1, buf1));
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, req1->end_lsn_, req2, buf2));
  TransportReqGuard guard1(req1);
  TransportReqGuard guard2(req2);

  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard1.get()));
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard2.get()));

  ObLogTransportTaskHandle handle1;
  ASSERT_EQ(OB_SUCCESS, queue.front(handle1));
  ASSERT_TRUE(handle1.is_valid());
  front_lsn.push_back(handle1.task()->start_lsn_);
  ASSERT_EQ(OB_SUCCESS, queue.success(handle1));

  ObLogTransportTaskHandle handle2;
  ASSERT_EQ(OB_SUCCESS, queue.front(handle2));
  ASSERT_TRUE(handle2.is_valid());
  front_lsn.push_back(handle2.task()->start_lsn_);
  ASSERT_EQ(OB_SUCCESS, queue.success(handle2));

  ObLogTransportTaskHandle gap_handle;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.front(gap_handle));
  EXPECT_EQ(2, queue.total_processed_cnt_);
  EXPECT_EQ(2, queue.total_success_cnt_);
  EXPECT_EQ(req2->end_lsn_, queue.next_submit_lsn_.atomic_load());
  EXPECT_FALSE(task_exists(queue, req1->start_lsn_));
  EXPECT_FALSE(task_exists(queue, req2->start_lsn_));
  EXPECT_EQ(0, queue.cached_bytes_);
  ASSERT_EQ(2, front_lsn.size());
  EXPECT_EQ(req1->start_lsn_, front_lsn[0]);
  EXPECT_EQ(req2->start_lsn_, front_lsn[1]);
}

TEST_F(TestLogTransportTaskQueue, front_stops_on_gap)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(200), req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));

  ObLogTransportTaskHandle handle;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, queue.front(handle));
  EXPECT_EQ(0, queue.total_processed_cnt_);
  EXPECT_TRUE(task_exists(queue, LSN(200)));
}

TEST_F(TestLogTransportTaskQueue, front_removes_tasks_below_end_lsn)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer stale_buf;
  GroupEntryBuffer next_buf;
  ObLogTransportReq *stale_req = nullptr;
  ObLogTransportReq *next_req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(150), stale_req, stale_buf));
  ASSERT_EQ(OB_SUCCESS, build_task(2, 32, LSN(300), next_req, next_buf));
  TransportReqGuard stale_guard(stale_req);
  TransportReqGuard next_guard(next_req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, stale_guard.get()));
  ASSERT_EQ(OB_SUCCESS, push_task(queue, next_guard.get()));
  ASSERT_GT(queue.cached_bytes_, 0);

  ObLogTransportTaskHandle handle;
  ASSERT_EQ(OB_SUCCESS, queue.update_end_lsn(LSN(300)));
  ASSERT_EQ(OB_SUCCESS, queue.front(handle));
  ASSERT_TRUE(handle.is_valid());
  EXPECT_EQ(next_req->start_lsn_, handle.task()->start_lsn_);
  ASSERT_EQ(OB_SUCCESS, queue.success(handle));
  EXPECT_EQ(1, queue.total_processed_cnt_);
  EXPECT_EQ(1, queue.total_success_cnt_);
  EXPECT_FALSE(task_exists(queue, LSN(150)));
  EXPECT_FALSE(task_exists(queue, LSN(300)));
  EXPECT_EQ(0, queue.cached_bytes_);
}

TEST_F(TestLogTransportTaskQueue, update_end_lsn_validation_and_noop)
{
  ObLogTransportTaskQueue queue;
  EXPECT_EQ(OB_NOT_INIT, queue.update_end_lsn(LSN(100)));

  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.update_end_lsn(LSN()));
  EXPECT_EQ(OB_NOT_MASTER, queue.update_end_lsn(LSN(100)));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));

  ASSERT_EQ(OB_SUCCESS, queue.update_end_lsn(LSN(100)));
  EXPECT_EQ(req->start_lsn_, queue.next_submit_lsn_.atomic_load());
  EXPECT_TRUE(task_exists(queue, req->start_lsn_));
  EXPECT_EQ(req->log_size_, queue.cached_bytes_);

  queue.stop();
  EXPECT_EQ(OB_IN_STOP_STATE, queue.update_end_lsn(req->end_lsn_));
}

TEST_F(TestLogTransportTaskQueue, failure_preserves_task)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));

  ObLogTransportTaskHandle handle;
  ASSERT_EQ(OB_SUCCESS, queue.front(handle));
  ASSERT_TRUE(handle.is_valid());
  EXPECT_EQ(OB_SUCCESS, queue.failure(handle));
  EXPECT_EQ(1, queue.total_processed_cnt_);
  EXPECT_EQ(0, queue.total_success_cnt_);
  EXPECT_TRUE(task_exists(queue, LSN(100)));
  EXPECT_EQ(req->start_lsn_, queue.next_submit_lsn_.atomic_load());
}

TEST_F(TestLogTransportTaskQueue, front_success_failure_argument_validation_and_stop)
{
  ObLogTransportTaskQueue queue;
  ObLogTransportTaskHandle handle;

  EXPECT_EQ(OB_NOT_INIT, queue.front(handle));
  EXPECT_EQ(OB_NOT_INIT, queue.success(handle));
  EXPECT_EQ(OB_NOT_INIT, queue.failure(handle));

  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  EXPECT_EQ(OB_NOT_MASTER, queue.front(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.success(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.failure(handle));
  EXPECT_EQ(OB_NOT_MASTER, queue.front(handle));

  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));
  GroupEntryBuffer buffer;
  ObLogTransportReq *req = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req, buffer));
  TransportReqGuard guard(req);
  ASSERT_EQ(OB_SUCCESS, push_task(queue, guard.get()));
  ASSERT_EQ(OB_SUCCESS, queue.front(handle));
  EXPECT_EQ(OB_INVALID_ARGUMENT, queue.front(handle));

  queue.stop();
  ObLogTransportTaskHandle stopped_handle;
  EXPECT_EQ(OB_IN_STOP_STATE, queue.front(stopped_handle));
  EXPECT_EQ(OB_IN_STOP_STATE, queue.success(handle));
  EXPECT_EQ(OB_IN_STOP_STATE, queue.failure(handle));
}

TEST_F(TestLogTransportTaskQueue, concurrent_duplicate_push)
{
  ObLogTransportTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, init_queue(queue));
  ASSERT_EQ(OB_SUCCESS, seed_next_submit_lsn(queue, LSN(100)));

  GroupEntryBuffer buf1;
  GroupEntryBuffer buf2;
  ObLogTransportReq *req1 = nullptr;
  ObLogTransportReq *req2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req1, buf1));
  ASSERT_EQ(OB_SUCCESS, build_task(1, 32, LSN(100), req2, buf2));
  TransportReqGuard guard1(req1);
  TransportReqGuard guard2(req2);
  SimpleBarrier barrier(2);
  std::atomic<int> ret1(OB_SUCCESS);
  std::atomic<int> ret2(OB_SUCCESS);

  std::thread t1([&]() {
    barrier.wait();
    ret1 = push_task(queue, guard1.get());
  });
  std::thread t2([&]() {
    barrier.wait();
    ret2 = push_task(queue, guard2.get());
  });
  t1.join();
  t2.join();

  EXPECT_EQ(OB_SUCCESS, ret1.load());
  EXPECT_EQ(OB_SUCCESS, ret2.load());
  EXPECT_EQ(1, queue.drop_duplicate_cnt_);
  EXPECT_EQ(1, queue.task_map_.count());
}

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
