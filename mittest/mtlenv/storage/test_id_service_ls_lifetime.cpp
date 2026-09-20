/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGETEST

#include <future>
#include <sstream>
#include <thread>
#include <vector>
#define private public
#define protected public
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_tablet_helper.h"
#include "storage/tx/ob_id_service.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace storage
{

class TestIDServiceLSLifetime : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    ls_service_ = MTL(ObLSService *);
    id_service_ = MTL(ObTimestampService *);
    ASSERT_NE(nullptr, ls_service_);
    ASSERT_NE(nullptr, id_service_);
    ls_service_->break_point = 0;
    id_service_->clear_ls_cache_();
  }

  void TearDown() override
  {
    // Also clean up after a fatal assertion, so a failed test cannot leave a
    // cached reference blocking the tenant's shutdown indefinitely.
    if (nullptr != id_service_) {
      id_service_->clear_ls_cache_();
    }
    if (nullptr != ls_service_) {
      ls_service_->break_point = 0;
      for (const auto &ls_id : created_ls_) {
        EXPECT_EQ(OB_SUCCESS, ls_service_->remove_ls(ls_id));
        EXPECT_TRUE(wait_for_gc(ls_id));
      }
    }
  }

protected:
  int create_ls(const ObLSID &ls_id, ObLSHandle &handle)
  {
    int ret = OB_SUCCESS;
    obrpc::ObCreateLSArg arg;
    created_ls_.push_back(ls_id);
    if (OB_FAIL(gen_create_ls_arg(MTL_ID(), ls_id, arg))) {
    } else if (OB_FAIL(ls_service_->create_ls(arg))) {
    } else {
      ret = ls_service_->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD);
    }
    return ret;
  }

  int cache_ls(ObIDService &service, const ObLSHandle &handle)
  {
    ObIDService::WLockGuard guard(service.ls_cache_lock_);
    return service.cached_ls_handle_.copy_from(handle);
  }

  bool cache_empty(ObIDService &service)
  {
    ObIDService::RLockGuard guard(service.ls_cache_lock_);
    return !service.cached_ls_handle_.is_valid();
  }

  bool wait_for_gc(const ObLSID &ls_id)
  {
    const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000;
    bool waiting = true;
    while (waiting && ObTimeUtility::current_time() < deadline) {
      if (OB_SUCCESS != ls_service_->check_ls_waiting_safe_destroy(ls_id, waiting)) {
        return false;
      }
      if (waiting) {
        ob_usleep(10 * 1000);
      }
    }
    return !waiting;
  }

  ObLSService *ls_service_ = nullptr;
  ObTimestampService *id_service_ = nullptr;
  std::vector<ObLSID> created_ls_;
};

TEST_F(TestIDServiceLSLifetime, handle_copy_and_swap)
{
  ObLSHandle first;
  ObLSHandle second;
  ASSERT_EQ(OB_SUCCESS, create_ls(ObLSID(301), first));
  ASSERT_EQ(OB_SUCCESS, create_ls(ObLSID(302), second));
  ObLS *first_ls = first.get_ls();
  ObLS *second_ls = second.get_ls();
  ObLSHandle copy;
  ObLSHandle empty;
  EXPECT_EQ(OB_INVALID_ARGUMENT, copy.copy_from(empty));
  EXPECT_EQ(OB_INVALID_ARGUMENT, empty.copy_from(empty));
  ASSERT_EQ(OB_SUCCESS, copy.copy_from(first));
  EXPECT_EQ(OB_INVALID_ARGUMENT, copy.copy_from(second));
  EXPECT_EQ(first_ls, copy.get_ls());
  copy.swap(empty);
  EXPECT_FALSE(copy.is_valid());
  EXPECT_EQ(first_ls, empty.get_ls());
  first.swap(second);
  EXPECT_EQ(second_ls, first.get_ls());
  EXPECT_EQ(first_ls, second.get_ls());
  second.swap(second);
  EXPECT_EQ(first_ls, second.get_ls());
  ObLSHandle another_empty;
  copy.swap(another_empty);
  EXPECT_FALSE(copy.is_valid());
  EXPECT_FALSE(another_empty.is_valid());

  ASSERT_EQ(OB_SUCCESS, ls_service_->remove_ls(ObLSID(301)));
  EXPECT_EQ(OB_NOT_RUNNING, copy.copy_from(second));
  EXPECT_FALSE(copy.is_valid());
  // Ownership can still be transferred after the LS rejects new references.
  second.swap(copy);
  EXPECT_FALSE(second.is_valid());
  EXPECT_EQ(first_ls, copy.get_ls());
  empty.reset();
  copy.reset();
  EXPECT_TRUE(wait_for_gc(ObLSID(301)));
}

TEST_F(TestIDServiceLSLifetime, request_handle_survives_gc_invalidation)
{
  const ObLSID ls_id(303);
  ObLSHandle owner;
  ASSERT_EQ(OB_SUCCESS, create_ls(ls_id, owner));
  // The mock tenant omits some SYS_LS modules. Seed its real ID service with a
  // normal LS to exercise cache ownership and the production GC dispatcher.
  ASSERT_EQ(OB_SUCCESS, cache_ls(*id_service_, owner));
  ObLSHandle request;
  ASSERT_EQ(OB_SUCCESS, id_service_->acquire_ls_handle(request));
  ObLS *ls = request.get_ls();
  owner.reset();

  // Deterministically pause the request between acquire and get_role.
  ASSERT_EQ(OB_SUCCESS, ls_service_->remove_ls(ls_id));
  EXPECT_TRUE(cache_empty(*id_service_));
  EXPECT_FALSE(ls->safe_to_destroy());
  ObRole role = INVALID_ROLE;
  int64_t proposal_id = 0;
  EXPECT_EQ(OB_NOT_RUNNING, ls->get_log_handler()->get_role(role, proposal_id));
  EXPECT_EQ(ls_id, ls->get_ls_id());
  ObLSHandle rejected;
  EXPECT_EQ(OB_NOT_RUNNING, rejected.copy_from(request));
  EXPECT_FALSE(rejected.is_valid());
  request.reset();
  EXPECT_TRUE(wait_for_gc(ls_id));
}

TEST_F(TestIDServiceLSLifetime, gc_finishes_without_another_request)
{
  const ObLSID ls_id(304);
  ObLSHandle owner;
  ASSERT_EQ(OB_SUCCESS, create_ls(ls_id, owner));
  ASSERT_EQ(OB_SUCCESS, cache_ls(*id_service_, owner));
  owner.reset();
  ASSERT_EQ(OB_SUCCESS, ls_service_->remove_ls(ls_id));
  EXPECT_TRUE(cache_empty(*id_service_));
  // Do not call acquire/check_leader again: GC must retire the cache itself.
  EXPECT_TRUE(wait_for_gc(ls_id));
}

TEST_F(TestIDServiceLSLifetime, concurrent_request_and_gc)
{
  const ObLSID ls_id(308);
  const uint64_t tenant_id = MTL_ID();
  ObLSHandle owner;
  ASSERT_EQ(OB_SUCCESS, create_ls(ls_id, owner));
  ASSERT_EQ(OB_SUCCESS, cache_ls(*id_service_, owner));
  owner.reset();
  std::promise<int> acquired;
  std::future<int> acquired_result = acquired.get_future();
  std::promise<void> resume;
  std::future<void> resume_request = resume.get_future();
  int request_ret = OB_ERR_UNEXPECTED;
  std::thread request([&]() {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    int ret = guard.switch_to(tenant_id);
    ObLSHandle handle;
    if (OB_SUCC(ret)) {
      ret = id_service_->acquire_ls_handle(handle);
    }
    acquired.set_value(ret);
    if (OB_SUCC(ret)) {
      if (resume_request.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
        ret = OB_TIMEOUT;
      } else {
        ObRole role = INVALID_ROLE;
        int64_t proposal_id = 0;
        ret = handle.get_ls()->get_log_handler()->get_role(role, proposal_id);
      }
    }
    request_ret = ret;
  });
  // Always release/join the worker, including when the acquisition check fails.
  if (acquired_result.wait_for(std::chrono::seconds(30)) == std::future_status::ready) {
    EXPECT_EQ(OB_SUCCESS, acquired_result.get());
    EXPECT_EQ(OB_SUCCESS, ls_service_->remove_ls(ls_id));
    EXPECT_TRUE(cache_empty(*id_service_));
  } else {
    ADD_FAILURE() << "request did not acquire the LS before the deadline";
  }
  resume.set_value();
  request.join();
  EXPECT_EQ(OB_NOT_RUNNING, request_ret);
  EXPECT_TRUE(wait_for_gc(ls_id));
}

TEST_F(TestIDServiceLSLifetime, rollback_after_map_publication_invalidates_cache)
{
  const ObLSID ls_id(309);
  obrpc::ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ObLS *ls = nullptr;
  ObMajorMVMergeInfo major_merge_info;
  ASSERT_EQ(OB_SUCCESS, ls_service_->inner_create_ls_(
      ls_id, ObMigrationStatus::OB_MIGRATION_STATUS_NONE, ObLSRestoreStatus(ObLSRestoreStatus::NONE),
      SCN::base_scn(), major_merge_info, ObLSStoreType::OB_LS_STORE_NORMAL,
      REPLICA_TYPE_FULL, arg.get_data_version(), ls));
  const int add_ret = ls_service_->add_ls_to_map_(ls);
  if (OB_SUCCESS != add_ret) {
    EXPECT_EQ(OB_SUCCESS, ls_service_->inner_del_ls_(ls));
  }
  ASSERT_EQ(OB_SUCCESS, add_ret);
  created_ls_.push_back(ls_id);
  ObLSHandle request;
  const int get_ret = ls_service_->get_ls(ls_id, request, ObLSGetMod::STORAGE_MOD);
  const int cache_ret = OB_SUCCESS == get_ret ? cache_ls(*id_service_, request) : get_ret;
  auto state = ObLSService::ObLSCreateState::CREATE_STATE_ADDED_TO_MAP;
  // Always roll back this partial LS, even if an acquisition assertion fails.
  ls_service_->del_ls_after_create_ls_failed_(state, ls);
  ASSERT_EQ(OB_SUCCESS, get_ret);
  ASSERT_EQ(OB_SUCCESS, cache_ret);
  EXPECT_TRUE(cache_empty(*id_service_));
  bool exists = true;
  ASSERT_EQ(OB_SUCCESS, ls_service_->check_ls_exist(ls_id, exists));
  EXPECT_FALSE(exists);
  // The rollback's cleanup handle is gone, but this request still owns the LS.
  EXPECT_EQ(ls_id, request.get_ls()->get_ls_id());
  request.reset();
  EXPECT_TRUE(wait_for_gc(ls_id));
}

TEST_F(TestIDServiceLSLifetime, deleted_cache_rejects_acquisition)
{
  const ObLSID ls_id(305);
  ObLSHandle owner;
  ASSERT_EQ(OB_SUCCESS, create_ls(ls_id, owner));
  ObIDService service;
  ASSERT_EQ(OB_SUCCESS, cache_ls(service, owner));
  // This isolated service is intentionally outside the MTL dispatcher,
  // representing the interval between set_delete and cache invalidation.
  ASSERT_EQ(OB_SUCCESS, ls_service_->remove_ls(ls_id));
  ObLSHandle request;
  EXPECT_EQ(OB_NOT_RUNNING, service.acquire_ls_handle(request));
  EXPECT_FALSE(request.is_valid());
  bool leader = true;
  EXPECT_EQ(OB_NOT_RUNNING, service.check_leader(leader));
  EXPECT_FALSE(owner.get_ls()->safe_to_destroy());
  service.invalidate_ls(owner.get_ls());
  EXPECT_TRUE(cache_empty(service));
  owner.reset();
  EXPECT_TRUE(wait_for_gc(ls_id));
}

TEST_F(TestIDServiceLSLifetime, invalidation_matches_instance_and_is_idempotent)
{
  ObLSHandle old_ls;
  ObLSHandle new_ls;
  ASSERT_EQ(OB_SUCCESS, create_ls(ObLSID(306), old_ls));
  ASSERT_EQ(OB_SUCCESS, create_ls(ObLSID(307), new_ls));
  ObIDService service;
  ASSERT_EQ(OB_SUCCESS, cache_ls(service, old_ls));
  service.invalidate_ls(old_ls.get_ls());
  service.invalidate_ls(old_ls.get_ls());
  EXPECT_TRUE(cache_empty(service));
  ASSERT_EQ(OB_SUCCESS, cache_ls(service, new_ls));
  service.invalidate_ls(old_ls.get_ls());
  service.invalidate_ls(nullptr);
  ObLSHandle request;
  ASSERT_EQ(OB_SUCCESS, service.acquire_ls_handle(request));
  EXPECT_EQ(new_ls.get_ls(), request.get_ls());
  EXPECT_EQ(OB_INVALID_ARGUMENT, service.acquire_ls_handle(request));
  service.invalidate_ls(new_ls.get_ls());
  EXPECT_EQ(new_ls.get_ls(), request.get_ls());
  EXPECT_TRUE(cache_empty(service));
}

TEST_F(TestIDServiceLSLifetime, map_get_ls_preserves_handle_error_semantics)
{
  ObLSHandle owner;
  const ObLSID ls_id(310);
  ASSERT_EQ(OB_SUCCESS, create_ls(ls_id, owner));
  ObLS *ls = owner.get_ls();
  ObLSHandle reused;
  ASSERT_EQ(OB_SUCCESS, reused.copy_from(owner));
  ObLSMap uninitialized_map;
  EXPECT_EQ(OB_NOT_INIT, uninitialized_map.get_ls(ls_id, reused, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(ls, reused.get_ls());
  EXPECT_EQ(OB_LS_NOT_EXIST,
      ls_service_->get_ls(ObLSID(999999), reused, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(ls, reused.get_ls());
  ASSERT_EQ(OB_SUCCESS, ls_service_->get_ls(ls_id, reused, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(ls, reused.get_ls());
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ls_service_->get_ls(ls_id, reused, ObLSGetMod::INVALID_MOD));
  EXPECT_FALSE(reused.is_valid());
}

TEST_F(TestIDServiceLSLifetime, map_get_ls_frees_old_ls_outside_bucket)
{
  class CheckingAllocator : public ObIAllocator
  {
  public:
    explicit CheckingAllocator(ObIAllocator &allocator) : allocator_(allocator) {}
    void *alloc(const int64_t size) override { return allocator_.alloc(size); }
    void *alloc(const int64_t size, const ObMemAttr &attr) override
    { return allocator_.alloc(size, attr); }
    void free(void *ptr) override
    {
      if (ptr == watched_ls_) {
        freed_ = true;
        bucket_unlocked_ = OB_SUCCESS == bucket_->try_wrlock();
        if (bucket_unlocked_) {
          bucket_->wrunlock();
        }
      }
      allocator_.free(ptr);
    }
    ObIAllocator &allocator_;
    ObQSyncLock *bucket_ = nullptr;
    void *watched_ls_ = nullptr;
    bool freed_ = false;
    bool bucket_unlocked_ = false;
  };

  // Use an isolated map so no background thread can contend for the checked
  // bucket. Check both successful replacement and failed reference acquisition.
  for (int i = 0; i < 2; ++i) {
    CheckingAllocator allocator(ls_service_->ls_allocator_);
    ObLSMap ls_map;
    ASSERT_EQ(OB_SUCCESS, ls_map.init(MTL_ID(), &allocator));
    const ObLSID old_id(311);
    const ObLSID new_id(312);
    for (const auto &ls_id : {old_id, new_id}) {
      obrpc::ObCreateLSArg arg;
      ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
      ObLS *ls = nullptr;
      ObMajorMVMergeInfo major_merge_info;
      ASSERT_EQ(OB_SUCCESS, ls_service_->inner_create_ls_(
          ls_id, ObMigrationStatus::OB_MIGRATION_STATUS_NONE, ObLSRestoreStatus(ObLSRestoreStatus::NONE),
          SCN::base_scn(), major_merge_info, ObLSStoreType::OB_LS_STORE_NORMAL,
          REPLICA_TYPE_FULL, arg.get_data_version(), ls));
      const int add_ret = ls_map.add_ls(*ls);
      if (OB_SUCCESS != add_ret) {
        EXPECT_EQ(OB_SUCCESS, ls_service_->inner_del_ls_(ls));
      }
      ASSERT_EQ(OB_SUCCESS, add_ret);
    }
    ObLSHandle reused;
    ASSERT_EQ(OB_SUCCESS, ls_map.get_ls(old_id, reused, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ls_map.del_ls(old_id));
    ASSERT_EQ(1, reused.get_ls()->get_ref_mgr().get_total_ref_cnt());
    allocator.watched_ls_ = reused.get_ls();
    allocator.bucket_ = &ls_map.buckets_lock_[new_id.hash() % ObLSMap::BUCKETS_CNT];
    const ObLSGetMod mod = 0 == i ? ObLSGetMod::STORAGE_MOD : ObLSGetMod::INVALID_MOD;
    EXPECT_EQ(0 == i ? OB_SUCCESS : OB_INVALID_ARGUMENT, ls_map.get_ls(new_id, reused, mod));
    EXPECT_TRUE(allocator.freed_);
    EXPECT_TRUE(allocator.bucket_unlocked_);
    EXPECT_EQ(0 == i, reused.is_valid());
  }
}

TEST_F(TestIDServiceLSLifetime, missing_ls_leaves_no_cache)
{
  ObIDService service;
  service.service_type_ = ObIDService::TimestampService;
  ObLSHandle request;
  ASSERT_EQ(OB_LS_NOT_EXIST, service.acquire_ls_handle(request));
  EXPECT_FALSE(request.is_valid());
  EXPECT_TRUE(cache_empty(service));
  bool leader = true;
  EXPECT_EQ(OB_LS_NOT_EXIST, service.check_leader(leader));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_id_service_ls_lifetime.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
