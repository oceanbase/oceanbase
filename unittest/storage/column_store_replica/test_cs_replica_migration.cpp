/**
 * Copyright (c) 2024 OceanBase
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
#include <cstdio>
#include <memory>
#include <tuple>
#include <unordered_set>
#include <unistd.h>

#define protected public
#define private public

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/ob_malloc_callback.h"
#include "lib/utility/utility.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "src/storage/high_availability/ob_cs_replica_migration.h"
#include "storage/tx_storage/ob_ls_service.h"

#undef private
#undef protected


namespace oceanbase
{
namespace storage
{
namespace unittest
{

class TestCSReplicaMigration: public ::testing::Test
{
public:
  TestCSReplicaMigration();
  virtual ~TestCSReplicaMigration() = default;
public:
  static int mock_tablet_id_array(
      int64_t start_tablet_id,
      int64_t count,
      ObIArray<ObLogicTabletID> &tablet_id_array);
};

int TestCSReplicaMigration::mock_tablet_id_array(
    int64_t start_tablet_id,
    int64_t count,
    ObIArray<ObLogicTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    ObLogicTabletID logic_tablet_id;
    if (OB_FAIL(logic_tablet_id.init(ObTabletID(start_tablet_id + i), 0))) {
      LOG_WARN("failed to init logic tablet id", K(ret));
    } else if (OB_FAIL(tablet_id_array.push_back(logic_tablet_id))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

class A
{
public:
  A(int a) : a_(a) {
    LOG_INFO("print a", K_(a));
  }
  TO_STRING_KV(K_(a));
public:
  int a_;
};

class B : public A
{
public:
  B(int a, int b) : A(a), b_(b) {
    LOG_INFO("print b", K_(b));
  }
  INHERIT_TO_STRING_KV("A", A, K_(b));
public:
  int b_;
};

TEST(TestCSReplicaMigration, test_group_co_ctx_pretty_print)
{
  int ret = OB_SUCCESS;
  A a(1);
  B b(1, 2);
  LOG_INFO("print a", K(a));
  LOG_INFO("print b", K(b));

  ObHATabletGroupCOConvertCtx ctx;
  ObHATabletGroupCtx base_ctx(ObHATabletGroupCtx::TabletGroupCtxType::NORMAL_TYPE);
  ObSEArray<ObLogicTabletID, 8> tablet_id_array;
  ASSERT_EQ(OB_SUCCESS,TestCSReplicaMigration::mock_tablet_id_array(200001, 100, tablet_id_array));
  ASSERT_EQ(OB_SUCCESS, ctx.init(tablet_id_array));
  ASSERT_EQ(OB_SUCCESS, base_ctx.init(tablet_id_array));
  LOG_INFO("print ctx", K(ctx));
  LOG_INFO("print base_ctx", K(base_ctx));
}

TEST(TestCSReplicaMigration, test_ha_tablet_group_mgr)
{
  int ret = OB_SUCCESS;
  ObHATabletGroupMgr mgr;
  ObSEArray<ObLogicTabletID, 10> tablet_id_array[3];
  ASSERT_EQ(OB_SUCCESS, mgr.init());
  for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, TestCSReplicaMigration::mock_tablet_id_array(200001 + i * 10000, 100, tablet_id_array[i]));
    ASSERT_EQ(OB_SUCCESS, mgr.build_tablet_group_ctx(tablet_id_array[i], ObHATabletGroupCtx::TabletGroupCtxType::CS_REPLICA_TYPE));
  }
  LOG_INFO("print mgr", K(mgr));
}

// These tests exercise the common F/R/C scheduling path, not data copying or RPC.
// In particular, schedule_parallel_create_tablets_ must transfer the real reader
// before any of the failures below reach generate_parallel_create_tablets_task_.
namespace
{
using namespace common;
using namespace share;

class MigrationOwnershipAllocator final : public ObIAllocator
{
public:
  MigrationOwnershipAllocator()
    : attempts_(0), allocations_(0), frees_(0), duplicate_frees_(0), fail_at_(0)
  {}
  ~MigrationOwnershipAllocator() override
  {
    EXPECT_TRUE(live_.empty());
    EXPECT_EQ(0, duplicate_frees_);
  }
  void *alloc(const int64_t size) override
  {
    ++attempts_;
    void *ptr = nullptr;
    if (attempts_ != fail_at_) {
      ptr = ob_malloc(size, ObMemAttr(OB_SERVER_TENANT_ID, "MigOwnAllocUT"));
      if (nullptr != ptr) {
        EXPECT_TRUE(live_.insert(ptr).second);
        ++allocations_;
      }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      if (1 != live_.erase(ptr)) {
        ++duplicate_frees_;
        ADD_FAILURE() << "unowned or already freed allocation: " << ptr;
      } else {
        ++frees_;
        ob_free(ptr);
      }
    }
  }
  void fail_after(const int64_t attempts) { fail_at_ = attempts_ + attempts; }
  void disable_failure() { fail_at_ = 0; }
  int64_t outstanding() const { return static_cast<int64_t>(live_.size()); }
  void expect_all_freed() const
  {
    EXPECT_EQ(0, outstanding());
    EXPECT_EQ(allocations_, frees_);
    EXPECT_EQ(0, duplicate_frees_);
  }
  int64_t attempts_;
  int64_t allocations_;
  int64_t frees_;
  int64_t duplicate_frees_;
private:
  int64_t fail_at_;
  std::unordered_set<void *> live_;
};

const char READER_BODY_LABEL[] = "MigReaderOwnUT";

// get_label_usage() reads an asynchronous MemoryDump snapshot, not live allocator
// counters. Observe real ob_malloc/ob_free events instead; no MemoryDump thread or
// ERRSIM is needed. The callback must not allocate, log, or invoke gtest assertions.
class ReaderBodyAllocationObserver final : public lib::ObMallocCallback
{
public:
  ReaderBodyAllocationObserver()
    : allocations_(0), frees_(0), live_bytes_(0), unexpected_events_(0), live_(nullptr)
  {}
  void operator()(const ObMemAttr &attr, int64_t used, const lib::AObject &obj) override
  {
    if (OB_SERVER_TENANT_ID == attr.tenant_id_ && attr.label_ == lib::ObLabel(READER_BODY_LABEL)) {
      if (used > 0) {
        ++allocations_;
        if (nullptr != live_) {
          ++unexpected_events_;
        }
        live_ = obj.data_;
      } else if (used < 0) {
        ++frees_;
        if (live_ != obj.data_) {
          ++unexpected_events_;
        }
        live_ = nullptr;
      } else {
        ++unexpected_events_;
      }
      live_bytes_ += used;
    }
  }
  void expect_live(const ObCopyLSViewInfoObReader *reader) const
  {
    EXPECT_NE(nullptr, reader);
    EXPECT_GT(allocations_, 0); // a missing callback must never pass as 0 == 0
    EXPECT_EQ(allocations_, frees_ + 1);
    EXPECT_EQ(static_cast<const void *>(reader), live_);
    EXPECT_EQ(static_cast<int64_t>(sizeof(ObCopyLSViewInfoObReader)), live_bytes_);
    EXPECT_EQ(0, unexpected_events_);
  }
  void expect_all_freed() const
  {
    EXPECT_GT(allocations_, 0);
    EXPECT_EQ(allocations_, frees_);
    EXPECT_EQ(nullptr, live_);
    EXPECT_EQ(0, live_bytes_);
    EXPECT_EQ(0, unexpected_events_);
  }
private:
  int64_t allocations_;
  int64_t frees_;
  int64_t live_bytes_;
  int64_t unexpected_events_;
  const void *live_;
};

enum class ParallelCreateFailure
{
  ALLOC_CREATE_TASK,
  INIT_CREATE_TASK,
  ALLOC_FINISH_TASK,
  ADD_CHILD,
  ADD_TASK,
};

class FailingStartMigrationTask final : public ObStartMigrationTask
{
public:
  FailingStartMigrationTask() : fail_child_(false), cancel_before_add_task_(false), child_calls_(0) {}
  bool fail_child_;
  bool cancel_before_add_task_;
  int64_t child_calls_;
protected:
  int add_child_without_lock(ObINodeWithChild &child) override
  {
    ++child_calls_;
    int ret = OB_SUCCESS;
    if (fail_child_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(ObINodeWithChild::add_child_without_lock(child))) {
    } else if (cancel_before_add_task_) {
      // Both tasks have already been allocated and initialized. The second edge
      // is still added by production code; the real add_task(finish_task) then
      // rejects the inactive DAG with OB_CANCELED (a non-retryable error).
      get_dag()->set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
    }
    return ret;
  }
};

using MigrationOwnershipParam = std::tuple<ObReplicaType, ParallelCreateFailure>;

class TestParallelCreateTabletsOwnership : public ::testing::TestWithParam<MigrationOwnershipParam>
{
public:
  TestParallelCreateTabletsOwnership()
    : tenant_(OB_SYS_TENANT_ID), old_tenant_(nullptr), old_throttle_(nullptr),
      ls_(nullptr), ls_in_map_(false)
  {}
  void SetUp() override
  {
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    old_tenant_ = MTL_CTX();
    old_throttle_ = GCTX.bandwidth_throttle_;
    ObTenantEnv::set_tenant(&tenant_);
    ASSERT_EQ(OB_SUCCESS, ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_.id()));
    ASSERT_EQ(OB_SUCCESS, tenant_.init());
    GCTX.bandwidth_throttle_ = &throttle_;
    ls_service_.reset(new ObLSService());
    tenant_.set(ls_service_.get());
    ObTenantEnv::set_tenant(&tenant_); // refresh the thread-local module snapshot
    // Only the in-memory lookup dependency is needed by the real create-task
    // init. Do not start an LS, scheduler, storage service, or network endpoint.
    ASSERT_EQ(OB_SUCCESS, ls_service_->ls_map_.init(tenant_.id(), &ls_allocator_));
    ls_service_->is_inited_ = true;
    void *buf = ls_allocator_.alloc(sizeof(ObLS));
    ASSERT_NE(nullptr, buf);
    ls_ = new (buf) ObLS();
    ls_->ls_meta_.ls_id_ = ObLSID(1001);
    ls_->ls_meta_.replica_type_ = std::get<0>(GetParam());
    ASSERT_EQ(OB_SUCCESS, ls_service_->ls_map_.add_ls(*ls_));
    ls_in_map_ = true;
  }
  void TearDown() override
  {
    if (nullptr != ls_ && !ls_in_map_) {
      ls_->~ObLS();
      ls_allocator_.free(ls_);
    }
    if (nullptr != ls_service_) {
      ls_service_->is_stopped_ = true;
      ls_service_.reset(); // the real LS map releases its reference and the LS
    }
    ls_ = nullptr;
    ls_allocator_.expect_all_freed();
    tenant_.set(static_cast<ObLSService *>(nullptr));
    ObTenantEnv::set_tenant(&tenant_);
    GCTX.bandwidth_throttle_ = old_throttle_;
    tenant_.destroy();
    ObTenantEnv::set_tenant(old_tenant_);
  }

  void prepare_ctx(ObMigrationCtx &ctx)
  {
    ObAddr src;
    ObAddr dst;
    ASSERT_TRUE(src.set_ip_addr("127.0.0.1", 12001));
    ASSERT_TRUE(dst.set_ip_addr("127.0.0.1", 12002));
    ctx.tenant_id_ = tenant_.id();
    ctx.arg_.ls_id_ = ls_->get_ls_id();
    ctx.arg_.type_ = ObMigrationOpType::MIGRATE_LS_OP;
    ctx.arg_.cluster_id_ = 1;
    ctx.arg_.paxos_replica_number_ = 3;
    ctx.arg_.src_ = ObReplicaMember(src, 1, REPLICA_TYPE_FULL);
    ctx.arg_.dst_ = ObReplicaMember(dst, 1, std::get<0>(GetParam()));
    ctx.task_id_.init(dst);
    ctx.minor_src_.src_addr_ = src;
    ctx.minor_src_.cluster_id_ = 1;
    ctx.src_ls_rebuild_seq_ = 0;
    ASSERT_TRUE(ctx.is_valid());
  }

  ObCopyLSViewInfoObReader *make_reader(MigrationOwnershipAllocator &pages)
  {
    void *buf = ob_malloc(sizeof(ObCopyLSViewInfoObReader),
        ObMemAttr(OB_SERVER_TENANT_ID, READER_BODY_LABEL));
    if (nullptr == buf) {
      ADD_FAILURE() << "failed to allocate reader";
      return nullptr;
    }
    ObCopyLSViewInfoObReader *reader = new (buf) ObCopyLSViewInfoObReader();
    // Preserve the concrete reader and its real destructors. Count both arenas'
    // backing pages, and separately observe the reader body's malloc/free events.
    reader->allocator_.get_arena().page_allocator_.set_allocator(&pages);
    reader->rpc_reader_.allocator_.get_arena().page_allocator_.set_allocator(&pages);
    if (nullptr == reader->allocator_.alloc(128)
        || OB_SUCCESS != reader->rpc_reader_.init(throttle_)) {
      ADD_FAILURE() << "failed to prepare reader-owned buffers";
      reader->~ObCopyLSViewInfoObReader();
      ob_free(reader);
      reader = nullptr;
    }
    return reader;
  }

  void expect_reader_released(const ReaderBodyAllocationObserver &body, const MigrationOwnershipAllocator &pages)
  {
    pages.expect_all_freed();
    body.expect_all_freed();
  }

  void run_failure_case(const bool try_retry)
  {
    const ParallelCreateFailure failure = std::get<1>(GetParam());
    SCOPED_TRACE(::testing::Message() << "replica=" << std::get<0>(GetParam())
        << " failure=" << static_cast<int>(failure) << " retry=" << try_retry);
    MigrationOwnershipAllocator task_allocator;
    MigrationOwnershipAllocator reader_pages;
    ReaderBodyAllocationObserver reader_body;
    // Keep the coroutine-local observer installed until DAG-net/ctx destruction.
    lib::ObMallocCallbackGuard reader_body_guard(reader_body);
    {
      ObMigrationDagNet dag_net;
      ASSERT_EQ(OB_SUCCESS, dag_net.alloc_migration_ctx_());
      ObMigrationCtx &ctx = *dag_net.get_migration_ctx();
      bool reader_released_by_retry = false;
      {
        ObStartMigrationDag dag;
        ASSERT_EQ(OB_SUCCESS, dag.basic_init(task_allocator));
        dag.set_dag_net(dag_net);
        ASSERT_EQ(OB_SUCCESS, dag.init(&dag_net));
        FailingStartMigrationTask *start = nullptr;
        ASSERT_EQ(OB_SUCCESS, dag.alloc_task(start));
        ASSERT_EQ(OB_SUCCESS, start->init());
        ASSERT_EQ(OB_SUCCESS, dag.add_task(*start));
        ASSERT_NO_FATAL_FAILURE(prepare_ctx(ctx));

        int expected_ret = OB_ALLOCATE_MEMORY_FAILED;
        int64_t expected_tasks = 3;
        switch (failure) {
          case ParallelCreateFailure::ALLOC_CREATE_TASK:
            task_allocator.fail_after(1);
            expected_tasks = 1;
            break;
          case ParallelCreateFailure::INIT_CREATE_TASK:
            // A valid but absent LS causes the real create-task init to fail
            // with retryable OB_LS_NOT_EXIST, after reader ownership moved.
            ctx.arg_.ls_id_ = ObLSID(1002);
            expected_ret = OB_LS_NOT_EXIST;
            expected_tasks = 2;
            break;
          case ParallelCreateFailure::ALLOC_FINISH_TASK:
            task_allocator.fail_after(2);
            expected_tasks = 2;
            break;
          case ParallelCreateFailure::ADD_CHILD:
            start->fail_child_ = true;
            break;
          case ParallelCreateFailure::ADD_TASK:
            start->cancel_before_add_task_ = true;
            expected_ret = OB_CANCELED;
            break;
        }
        ObCopyLSViewInfoObReader *reader = make_reader(reader_pages);
        ASSERT_NE(nullptr, reader);
        EXPECT_EQ(2, reader_pages.outstanding());
        reader_body.expect_live(reader);
        const int ret = start->schedule_parallel_create_tablets_(reader);
        // Cleanup only if the ownership-transfer assertion itself fails.
        if (ctx.parallel_create_tablets_info_.copy_ls_view_reader_ != reader) {
          reader->~ObCopyLSViewInfoObReader();
          ob_free(reader);
          FAIL() << "failure did not occur after reader handoff";
        }
        ASSERT_EQ(expected_ret, ret);
        EXPECT_TRUE(ctx.parallel_create_tablets_info_.has_reader());
        EXPECT_EQ(2, reader_pages.outstanding());
        EXPECT_EQ(0, reader_pages.frees_);
        reader_body.expect_live(ctx.parallel_create_tablets_info_.copy_ls_view_reader_);
        ASSERT_EQ(expected_tasks, dag.get_task_list_count());
        EXPECT_EQ(expected_tasks, task_allocator.outstanding());
        EXPECT_EQ(0, task_allocator.frees_);
        EXPECT_EQ(expected_tasks == 3 ? 1 : 0, start->child_calls_);
        if (expected_tasks >= 2) {
          auto *create = static_cast<ObMigrationCreateAllTabletsTask *>(start->get_next());
          EXPECT_EQ(failure != ParallelCreateFailure::INIT_CREATE_TASK, create->is_inited_);
          EXPECT_EQ(ObITask::TASK_STATUS_INITING, create->get_status());
          if (expected_tasks == 3) {
            auto *finish = static_cast<ObMigrationCreateTabletsFinishTask *>(create->get_next());
            EXPECT_TRUE(finish->is_inited_);
            const bool reached_add_task = failure == ParallelCreateFailure::ADD_TASK;
            EXPECT_EQ(reached_add_task ? 1 : 0, start->get_child_nodes().count());
            EXPECT_EQ(reached_add_task ? 1 : 0, create->get_child_nodes().count());
            EXPECT_EQ(reached_add_task ? 1 : 0, finish->get_indegree());
            // add_task changes finish to WAITING before rejecting the failed
            // DAG; create stays INITING because its add_task was not reached.
            EXPECT_EQ(reached_add_task ? ObITask::TASK_STATUS_WAITING : ObITask::TASK_STATUS_INITING,
                finish->get_status());
          }
        }

        if (try_retry) {
          ASSERT_EQ(OB_SUCCESS, ObStorageHADagUtils::deal_with_fo(ret, &dag));
          dag.set_dag_ret(ret);
          dag.set_dag_status(ObIDag::DAG_STATUS_NODE_FAILED);
          if (ParallelCreateFailure::ADD_TASK == failure) {
            // Cancellation is terminal in the real HA retry policy. Do not
            // manufacture a retry by resetting this error to OB_SUCCESS.
            EXPECT_FALSE(dag.check_can_retry());
          } else {
            ASSERT_TRUE(dag.check_can_retry());
            task_allocator.disable_failure();
            // These are the scheduler's real retry cleanup/reinitialization
            // entry points, invoked only after the failed round is quiescent.
            dag.reset_task_list_for_retry();
            EXPECT_EQ(0, dag.get_task_list_count());
            task_allocator.expect_all_freed();
            EXPECT_EQ(2, reader_pages.outstanding());
            reader_body.expect_live(ctx.parallel_create_tablets_info_.copy_ls_view_reader_);
            ASSERT_EQ(OB_SUCCESS, dag.reset_status_for_retry());
            // create_first_task -> ObStartMigrationTask::init -> ctx.reuse
            EXPECT_FALSE(ctx.parallel_create_tablets_info_.has_reader());
            EXPECT_FALSE(ctx.parallel_create_tablets_info_.is_reader_finished());
            expect_reader_released(reader_body, reader_pages);
            ASSERT_EQ(1, dag.get_task_list_count());
            ASSERT_NO_FATAL_FAILURE(prepare_ctx(ctx));
            auto *retry_start = static_cast<ObStartMigrationTask *>(dag.task_list_.get_first());
            reader = make_reader(reader_pages);
            ASSERT_NE(nullptr, reader);
            reader_body.expect_live(reader);
            const int retry_ret = retry_start->schedule_parallel_create_tablets_(reader);
            if (ctx.parallel_create_tablets_info_.copy_ls_view_reader_ != reader) {
              reader->~ObCopyLSViewInfoObReader();
              ob_free(reader);
              FAIL() << "retry did not take ownership of its new reader";
            }
            ASSERT_EQ(OB_SUCCESS, retry_ret);
            ASSERT_EQ(3, dag.get_task_list_count());
            // Run the real finish cleanup without starting data migration.
            ASSERT_EQ(OB_SUCCESS, ctx.set_result(OB_CANCELED, false, dag.get_type()));
            auto *finish = static_cast<ObMigrationCreateTabletsFinishTask *>(dag.task_list_.get_last());
            ASSERT_EQ(OB_SUCCESS, finish->process());
            EXPECT_FALSE(ctx.parallel_create_tablets_info_.has_reader());
            expect_reader_released(reader_body, reader_pages);
            ASSERT_EQ(OB_SUCCESS, finish->process());
            EXPECT_FALSE(ctx.parallel_create_tablets_info_.has_reader());
            expect_reader_released(reader_body, reader_pages);
            // Only after observing production cleanup, test reset/reuse idempotence.
            ctx.parallel_create_tablets_info_.reset();
            ctx.parallel_create_tablets_info_.reuse();
            EXPECT_FALSE(ctx.parallel_create_tablets_info_.has_reader());
            expect_reader_released(reader_body, reader_pages);
            reader_released_by_retry = true;
          }
        }
      } // the real DAG destructor frees even INITING / never-added tasks
      task_allocator.expect_all_freed();
      EXPECT_EQ(1, ls_->get_ref_mgr().get_total_ref_cnt()); // only the LS map owns it
      if (!reader_released_by_retry) {
        EXPECT_TRUE(ctx.parallel_create_tablets_info_.has_reader());
        EXPECT_EQ(2, reader_pages.outstanding());
        reader_body.expect_live(ctx.parallel_create_tablets_info_.copy_ls_view_reader_);
      }
    } // the real DAG-net/ctx destructors release any reader left by failure
    task_allocator.expect_all_freed();
    expect_reader_released(reader_body, reader_pages);
  }

private:
  ObTenantBase tenant_;
  ObTenantBase *old_tenant_;
  ObInOutBandwidthThrottle throttle_;
  ObInOutBandwidthThrottle *old_throttle_;
  MigrationOwnershipAllocator ls_allocator_;
  std::unique_ptr<ObLSService> ls_service_;
  ObLS *ls_;
  bool ls_in_map_;
};

TEST_P(TestParallelCreateTabletsOwnership, failure_keeps_reader_until_dag_net_destruction)
{
  run_failure_case(false);
}

TEST_P(TestParallelCreateTabletsOwnership, retry_policy_and_reader_cleanup)
{
  run_failure_case(true);
}

INSTANTIATE_TEST_CASE_P(FRC, TestParallelCreateTabletsOwnership,
    ::testing::Combine(
        ::testing::Values(REPLICA_TYPE_FULL, REPLICA_TYPE_READONLY, REPLICA_TYPE_COLUMNSTORE),
        ::testing::Values(ParallelCreateFailure::ALLOC_CREATE_TASK,
            ParallelCreateFailure::INIT_CREATE_TASK,
            ParallelCreateFailure::ALLOC_FINISH_TASK,
            ParallelCreateFailure::ADD_CHILD,
            ParallelCreateFailure::ADD_TASK)));
} // namespace

} // end unittest
} // end storage
} // end oceanbase

int main(int argc, char **argv)
{
  char log_path[256];
  std::snprintf(log_path, sizeof(log_path), "test_cs_replica_migration_%d.log", static_cast<int>(getpid()));
  OB_LOGGER.set_file_name(log_path, true, false);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
