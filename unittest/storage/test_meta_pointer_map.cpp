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

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_meta_pointer_map.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

int64_t ObTenantMetaMemMgr::cal_adaptive_bucket_num()
{
  return 1000;
}

class DeterministicSyncPoint
{
public:
  DeterministicSyncPoint() : reached_(false), released_(false) {}

  void arrive_and_wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    reached_ = true;
    cond_.notify_all();
    cond_.wait_for(lock, std::chrono::seconds(30), [this]() { return released_; });
  }

  bool wait_until_reached()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cond_.wait_for(lock, std::chrono::seconds(10), [this]() { return reached_; });
  }

  void release()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cond_.notify_all();
  }

private:
  std::mutex mutex_;
  std::condition_variable cond_;
  bool reached_;
  bool released_;
};

class TrackingAllocator final : public common::ObIAllocator
{
public:
  TrackingAllocator()
    : allocator_(common::ObMemAttr(OB_SERVER_TENANT_ID, "ExtLoadTest")),
      alloc_count_(0),
      free_count_(0),
      block_on_alloc_(0),
      blocked_(false),
      released_(false),
      double_free_(false),
      live_ptrs_()
  {}

  virtual void *alloc(const int64_t size) override
  {
    return alloc(size, common::ObMemAttr(OB_SERVER_TENANT_ID, "ExtLoadTest"));
  }

  virtual void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  {
    std::unique_lock<std::mutex> lock(mutex_);
    const int64_t alloc_index = ++alloc_count_;
    if (block_on_alloc_ == alloc_index) {
      blocked_ = true;
      cond_.notify_all();
      cond_.wait_for(lock, std::chrono::seconds(30), [this]() { return released_; });
    }
    lock.unlock();
    void *ptr = allocator_.alloc(size, attr);
    lock.lock();
    if (OB_NOT_NULL(ptr)) {
      live_ptrs_.insert(ptr);
    }
    return ptr;
  }

  virtual void free(void *ptr) override
  {
    if (OB_NOT_NULL(ptr)) {
      bool should_free = false;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (1 != live_ptrs_.erase(ptr)) {
          double_free_ = true;
        } else {
          ++free_count_;
          should_free = true;
        }
      }
      if (should_free) {
        allocator_.free(ptr);
      }
    }
  }

  void block_on_alloc(const int64_t alloc_index)
  {
    std::lock_guard<std::mutex> lock(mutex_);
    block_on_alloc_ = alloc_index;
  }

  bool wait_until_blocked()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    return cond_.wait_for(lock, std::chrono::seconds(10), [this]() { return blocked_; });
  }

  void release()
  {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    cond_.notify_all();
  }

  int64_t alloc_count() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return alloc_count_;
  }

  int64_t free_count() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return free_count_;
  }

  int64_t live_count() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return live_ptrs_.size();
  }

  bool has_double_free() const
  {
    std::lock_guard<std::mutex> lock(mutex_);
    return double_free_;
  }

private:
  common::ObMalloc allocator_;
  mutable std::mutex mutex_;
  std::condition_variable cond_;
  int64_t alloc_count_;
  int64_t free_count_;
  int64_t block_on_alloc_;
  bool blocked_;
  bool released_;
  bool double_free_;
  std::set<void *> live_ptrs_;
};

void PrepareEmptyShellTablet(ObTablet *tablet, const ObMetaDiskAddr &load_addr)
{
  tablet->table_store_addr_.addr_.set_none_addr();
  tablet->storage_schema_addr_.addr_.set_none_addr();
  tablet->mds_data_.tablet_status_.uncommitted_kv_.addr_.set_none_addr();
  tablet->mds_data_.tablet_status_.committed_kv_.addr_.set_none_addr();
  tablet->mds_data_.aux_tablet_info_.uncommitted_kv_.addr_.set_none_addr();
  tablet->mds_data_.aux_tablet_info_.committed_kv_.addr_.set_none_addr();
  tablet->mds_data_.medium_info_list_.addr_.set_none_addr();
  tablet->mds_data_.auto_inc_seq_.addr_.set_none_addr();
  tablet->tablet_meta_.has_next_tablet_ = false;
  tablet->tablet_addr_ = load_addr;
}

struct ExternalLoadTestContext
{
  static const int64_t MAX_LOAD_COUNT = 8;

  ExternalLoadTestContext()
    : tablet_(nullptr),
      call_count_(0),
      ref_cnt_(),
      load_ret_(),
      loaded_tablet_(),
      loaded_pointer_(),
      loaded_addr_(),
      replace_pointer_(),
      replacement_addr_(),
      after_load_sync_(nullptr),
      sync_load_index_(-1),
      partial_macro_ref_failure_(false),
      first_macro_addr_(),
      failing_macro_addr_()
  {
    for (int64_t i = 0; i < MAX_LOAD_COUNT; ++i) {
      ref_cnt_[i] = 0;
      load_ret_[i] = OB_SUCCESS;
      loaded_tablet_[i] = nullptr;
      loaded_pointer_[i] = nullptr;
      replace_pointer_[i] = false;
    }
  }

  void prepare_loaded_tablet(ObTablet *tablet, const ObMetaDiskAddr &load_addr)
  {
    PrepareEmptyShellTablet(tablet, load_addr);
    if (partial_macro_ref_failure_) {
      tablet->mds_data_.tablet_status_.uncommitted_kv_.addr_ = first_macro_addr_;
      tablet->mds_data_.tablet_status_.committed_kv_.addr_ = failing_macro_addr_;
    }
  }

  int replace_pointer(
      ObMetaPointerMap<ObTabletMapKey, ObTablet> *map,
      const ObTabletMapKey &key,
      ObMetaPointer<ObTablet> *meta_pointer,
      const ObMetaDiskAddr &new_addr)
  {
    int ret = OB_SUCCESS;
    ObTabletPointer *tablet_pointer = static_cast<ObTabletPointer *>(meta_pointer);
    ObTabletPointer replacement(tablet_pointer->ls_handle_, tablet_pointer->memtable_mgr_handle_);
    replacement.set_addr_with_reset_obj(new_addr);
    if (OB_NOT_NULL(tablet_pointer->obj_.pool_)) {
      replacement.set_obj_pool(*tablet_pointer->obj_.pool_);
    }
    if (OB_FAIL(map->inner_erase(key))) {
      STORAGE_LOG(WARN, "failed to erase pointer before replacement", K(ret), K(key));
    } else if (OB_FAIL(map->set(key, replacement))) {
      STORAGE_LOG(WARN, "failed to install replacement pointer", K(ret), K(key));
    }
    return ret;
  }

  int on_load(
      ObMetaPointerMap<ObTabletMapKey, ObTablet> *map,
      const ObTabletMapKey &key,
      ObMetaPointer<ObTablet> *meta_pointer,
      ObMetaDiskAddr &load_addr,
      ObTablet *loaded_tablet)
  {
    int ret = OB_SUCCESS;
    const int64_t index = call_count_++;
    if (OB_ISNULL(map) || OB_ISNULL(meta_pointer) || OB_ISNULL(loaded_tablet)
        || index >= MAX_LOAD_COUNT) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      uint64_t hash_val = 0;
      if (OB_FAIL(map->hash_func_(key, hash_val))) {
        STORAGE_LOG(WARN, "failed to hash key", K(ret), K(key));
      } else {
        {
          common::ObBucketHashRLockGuard lock_guard(map->bucket_lock_, hash_val);
          load_addr = meta_pointer->get_addr();
        }
        loaded_tablet_[index] = loaded_tablet;
        loaded_pointer_[index] = meta_pointer;
        loaded_addr_[index] = load_addr;
        ref_cnt_[index] = OB_ISNULL(tablet_) ? 0 : tablet_->get_ref();
        prepare_loaded_tablet(loaded_tablet, load_addr);
        if (replace_pointer_[index]
            && OB_FAIL(replace_pointer(map, key, meta_pointer, replacement_addr_[index]))) {
          STORAGE_LOG(WARN, "failed to replace pointer during external load", K(ret), K(index));
        } else if (OB_NOT_NULL(after_load_sync_) && sync_load_index_ == index) {
          after_load_sync_->arrive_and_wait();
        }
      }
      if (OB_SUCC(ret)) {
        ret = load_ret_[index];
      }
    }
    return ret;
  }

  ObTablet *tablet_;
  int64_t call_count_;
  int64_t ref_cnt_[MAX_LOAD_COUNT];
  int load_ret_[MAX_LOAD_COUNT];
  ObTablet *loaded_tablet_[MAX_LOAD_COUNT];
  ObMetaPointer<ObTablet> *loaded_pointer_[MAX_LOAD_COUNT];
  ObMetaDiskAddr loaded_addr_[MAX_LOAD_COUNT];
  bool replace_pointer_[MAX_LOAD_COUNT];
  ObMetaDiskAddr replacement_addr_[MAX_LOAD_COUNT];
  DeterministicSyncPoint *after_load_sync_;
  int64_t sync_load_index_;
  bool partial_macro_ref_failure_;
  ObMetaDiskAddr first_macro_addr_;
  ObMetaDiskAddr failing_macro_addr_;
};

static ExternalLoadTestContext *g_external_load_test_ctx = nullptr;

enum class ExternalLoadWindow
{
  AFTER_LOAD_BEFORE_VALIDATE,
  DURING_POST_WORK
};

enum class ExternalLoadMutation
{
  CAS,
  WASH,
  ERASE
};

template<>
int ObMetaPointerMap<ObTabletMapKey, ObTablet>::load_meta_obj(
    const ObTabletMapKey &key,
    ObMetaPointer<ObTablet> *meta_pointer,
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &load_addr,
    ObTablet *t)
{
  UNUSED(allocator);
  return OB_ISNULL(g_external_load_test_ctx)
      ? OB_SUCCESS
      : g_external_load_test_ctx->on_load(this, key, meta_pointer, load_addr, t);
}

class TestMetaPointerMap : public ::testing::Test
{
public:
  TestMetaPointerMap();
  virtual ~TestMetaPointerMap() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  void FakeLs(ObLS &ls);
  void PrepareInMemoryTablet(
      ObLS &ls,
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &disk_addr,
      ObTabletHandle &tablet_handle);
  void AcquireTabletHandle(const ObMetaDiskAddr &disk_addr, ObTabletHandle &tablet_handle);
  void SetDiskAddr(const int64_t block_id, ObMetaDiskAddr &disk_addr);
  void SetFileAddr(const int64_t file_id, ObMetaDiskAddr &disk_addr);
  int InitBlockManagerForTest();
  int GetMacroBlockRef(const ObMetaDiskAddr &addr, int64_t &ref_cnt);
  void HoldTabletMacroRef(
      const ObTabletMapKey &key,
      const ObMetaDiskAddr &disk_addr,
      ObTabletHandle &tablet_handle);
  void ReleaseExternalTablet(ObTabletHandle &tablet_handle);
  void DrainTabletGcQueue();
  void SetAssignPointerHandleErrsim(const int error_code, const int64_t occur);
  void RunExternalLoadMutationCase(
      const ExternalLoadWindow window,
      const ExternalLoadMutation mutation,
      const int64_t case_id);

private:
  static constexpr uint64_t TEST_TENANT_ID = OB_SERVER_TENANT_ID;
  ObMetaPointerMap<ObTabletMapKey, ObTablet> tablet_map_;
  common::ObArenaAllocator allocator_;
  ObTenantBase tenant_base_;
  bool block_manager_inited_by_test_;
};

TestMetaPointerMap::TestMetaPointerMap()
  : tablet_map_(),
    tenant_base_(TEST_TENANT_ID),
    block_manager_inited_by_test_(false)
{
}

void TestMetaPointerMap::SetUp()
{
  g_external_load_test_ctx = nullptr;
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "TabletMap");
  int ret = tablet_map_.init(1000L, attr, 15 * 1024L * 1024L * 1024L, 8 * 1024L * 1024L,
          common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, t3m->init());

  tenant_base_.set(t3m);
  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
}

void TestMetaPointerMap::TearDown()
{
  g_external_load_test_ctx = nullptr;
  SetAssignPointerHandleErrsim(0, 0);
  tablet_map_.destroy();
  if (block_manager_inited_by_test_) {
    OB_SERVER_BLOCK_MGR.destroy();
    block_manager_inited_by_test_ = false;
  }
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  t3m->stop();
  t3m->wait();
  t3m->destroy();
  tenant_base_.destroy();
}

void TestMetaPointerMap::FakeLs(ObLS &ls)
{
  ls.ls_meta_.tenant_id_ = 1;
  ls.ls_meta_.ls_id_.id_ = 1001;
  ls.ls_meta_.gc_state_ = logservice::LSGCState::NORMAL;
  ls.ls_meta_.migration_status_ = ObMigrationStatus::OB_MIGRATION_STATUS_NONE;
  ls.ls_meta_.restore_status_ = ObLSRestoreStatus::RESTORE_NONE;
  ls.ls_meta_.rebuild_seq_ = 0;
}

void TestMetaPointerMap::SetDiskAddr(const int64_t block_id, ObMetaDiskAddr &disk_addr)
{
  disk_addr.first_id_ = block_id;
  disk_addr.second_id_ = 2;
  disk_addr.offset_ = 0;
  disk_addr.size_ = 4096;
  disk_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
}

void TestMetaPointerMap::SetFileAddr(const int64_t file_id, ObMetaDiskAddr &disk_addr)
{
  ASSERT_EQ(OB_SUCCESS, disk_addr.set_file_addr(file_id, 0, 4096));
}

void TestMetaPointerMap::AcquireTabletHandle(
    const ObMetaDiskAddr &disk_addr,
    ObTabletHandle &tablet_handle)
{
  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  ObMetaObj<ObTablet> tablet_obj;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer));
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, tablet_obj.ptr_);
  tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  tablet_handle.set_obj(tablet_obj);
  tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
  tablet_handle.get_obj()->set_tablet_addr(disk_addr);
}

void TestMetaPointerMap::PrepareInMemoryTablet(
    ObLS &ls,
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &disk_addr,
    ObTabletHandle &tablet_handle)
{
  FakeLs(ls);
  observer::ObIMetaReport *fake_reporter = reinterpret_cast<observer::ObIMetaReport *>(0xff);
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet_svr()->init(&ls, fake_reporter));

  ObMemtableMgrHandle memtable_mgr_hdl;
  ObDDLKvMgrHandle ddl_kv_mgr_hdl;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->acquire_tablet_memtable_mgr(memtable_mgr_hdl));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl));

  ObLSHandle ls_handle;
  ls_handle.ls_ = &ls;
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(none_addr);
  ASSERT_EQ(OB_SUCCESS, tablet_map_.set(key, tablet_ptr));

  AcquireTabletHandle(disk_addr, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, tablet_map_.compare_and_swap_addr_and_object(key, tablet_handle, tablet_handle));
}

int TestMetaPointerMap::InitBlockManagerForTest()
{
  int ret = OB_SUCCESS;
  if (OB_SERVER_BLOCK_MGR.is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.bucket_lock_.init(
      1024, common::ObLatchIds::BLOCK_MANAGER_LOCK))) {
    STORAGE_LOG(WARN, "failed to initialize block manager bucket lock", K(ret));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.block_map_.init(
      common::ObMemAttr(TEST_TENANT_ID, "BlockRefTest")))) {
    STORAGE_LOG(WARN, "failed to initialize block manager map", K(ret));
  } else {
    OB_SERVER_BLOCK_MGR.is_inited_ = true;
    block_manager_inited_by_test_ = true;
  }
  if (OB_FAIL(ret) && !OB_SERVER_BLOCK_MGR.is_inited_) {
    OB_SERVER_BLOCK_MGR.destroy();
  }
  return ret;
}

int TestMetaPointerMap::GetMacroBlockRef(const ObMetaDiskAddr &addr, int64_t &ref_cnt)
{
  int ret = OB_SUCCESS;
  ObBlockManager::BlockInfo block_info;
  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.block_map_.get(addr.block_id(), block_info))) {
    STORAGE_LOG(WARN, "failed to get macro block ref count", K(ret), K(addr));
  } else {
    ref_cnt = block_info.ref_cnt_;
  }
  return ret;
}

void TestMetaPointerMap::HoldTabletMacroRef(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &disk_addr,
    ObTabletHandle &tablet_handle)
{
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  PrepareEmptyShellTablet(tablet, disk_addr);
  tablet->tablet_meta_.ls_id_ = key.ls_id_;
  tablet->tablet_meta_.tablet_id_ = key.tablet_id_;
  tablet->is_inited_ = true;
  ObMetaPointerHandle<ObTabletMapKey, ObTablet> pointer_handle(tablet_map_);
  ASSERT_EQ(OB_SUCCESS, tablet_map_.get(key, pointer_handle));
  ASSERT_EQ(OB_SUCCESS, tablet->assign_pointer_handle(pointer_handle));
  ASSERT_EQ(OB_SUCCESS, tablet->inc_macro_ref_cnt());
  ASSERT_TRUE(tablet->hold_ref_cnt_);
}

void TestMetaPointerMap::ReleaseExternalTablet(ObTabletHandle &tablet_handle)
{
  if (tablet_handle.is_valid()) {
    ASSERT_NE(nullptr, tablet_handle.get_allocator());
    tablet_handle.disallow_copy_and_assign();
    tablet_handle.get_obj()->set_allocator(tablet_handle.get_allocator());
    tablet_handle.reset();
  }
}

void TestMetaPointerMap::DrainTabletGcQueue()
{
  bool all_tablet_cleaned = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->gc_tablets_in_queue(all_tablet_cleaned));
  ASSERT_TRUE(all_tablet_cleaned);
}

void TestMetaPointerMap::SetAssignPointerHandleErrsim(
    const int error_code,
    const int64_t occur)
{
  common::EventItem item;
  item.error_code_ = error_code;
  item.occur_ = occur;
  item.trigger_freq_ = 0;
  item.cond_ = 0;
  const int ret = common::EventTable::set_event(
      "EN_TABLET_ASSIGN_POINTER_HANDLE_FAILED", item);
  if (0 != error_code || 0 != occur) {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_holds_in_memory_tablet_guard)
{
  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));
  ObMetaDiskAddr disk_addr;
  SetDiskAddr(1, disk_addr);
  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, disk_addr, tablet_handle);

  const int64_t baseline_ref_cnt = tablet_handle.get_obj()->get_ref();
  ASSERT_EQ(INT64_MIN, tablet_handle.get_obj()->get_wash_score());
  ExternalLoadTestContext ctx;
  ctx.tablet_ = tablet_handle.get_obj();
  ctx.load_ret_[0] = OB_EAGAIN;
  g_external_load_test_ctx = &ctx;

  const int64_t min_expected_wash_score =
      common::ObClockGenerator::getClock() * 1000 - INT64_MAX;
  ObTabletHandle copied_handle;
  ASSERT_EQ(OB_EAGAIN, tablet_map_.get_meta_obj_with_external_memory(
      key, allocator_, copied_handle, true/*force_alloc_new*/));
  const int64_t max_expected_wash_score =
      common::ObClockGenerator::getClock() * 1000 - INT64_MAX;
  ASSERT_EQ(1, ctx.call_count_);
  ASSERT_EQ(baseline_ref_cnt + 1, ctx.ref_cnt_[0]);
  ASSERT_EQ(baseline_ref_cnt, tablet_handle.get_obj()->get_ref());
  ASSERT_GE(tablet_handle.get_obj()->get_wash_score(), min_expected_wash_score);
  ASSERT_LE(tablet_handle.get_obj()->get_wash_score(), max_expected_wash_score);
  ASSERT_FALSE(copied_handle.is_valid());
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_waits_for_in_memory_tablet_persistence)
{
  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));
  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, none_addr, tablet_handle);

  ExternalLoadTestContext ctx;
  ctx.tablet_ = tablet_handle.get_obj();
  g_external_load_test_ctx = &ctx;
  const int64_t allocator_used = allocator_.used();

  ObTabletHandle copied_handle;
  ASSERT_EQ(OB_EAGAIN, tablet_map_.get_meta_obj_with_external_memory(
      key, allocator_, copied_handle, true/*force_alloc_new*/));
  ASSERT_FALSE(copied_handle.is_valid());
  ASSERT_EQ(0, ctx.call_count_);
  ASSERT_EQ(allocator_used, allocator_.used());
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_protects_replaced_tablet_and_drops_stale_guard)
{
  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));
  ObMetaDiskAddr old_addr;
  ObMetaDiskAddr middle_addr;
  ObMetaDiskAddr final_addr;
  SetFileAddr(1, old_addr);
  SetFileAddr(2, middle_addr);
  SetFileAddr(3, final_addr);
  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, old_addr, tablet_handle);

  const int64_t baseline_ref_cnt = tablet_handle.get_obj()->get_ref();
  ASSERT_EQ(INT64_MIN, tablet_handle.get_obj()->get_wash_score());
  ExternalLoadTestContext ctx;
  ctx.tablet_ = tablet_handle.get_obj();
  ctx.replace_pointer_[0] = true;
  ctx.replacement_addr_[0] = middle_addr;
  ctx.replace_pointer_[1] = true;
  ctx.replacement_addr_[1] = final_addr;
  g_external_load_test_ctx = &ctx;

  const int64_t min_expected_wash_score =
      common::ObClockGenerator::getClock() * 1000 - INT64_MAX;
  ObTabletHandle copied_handle;
  ASSERT_EQ(OB_SUCCESS, tablet_map_.get_meta_obj_with_external_memory(
      key, allocator_, copied_handle, true/*force_alloc_new*/));
  const int64_t max_expected_wash_score =
      common::ObClockGenerator::getClock() * 1000 - INT64_MAX;
  ASSERT_EQ(3, ctx.call_count_);
  ASSERT_EQ(baseline_ref_cnt + 1, ctx.ref_cnt_[0]);
  ASSERT_EQ(baseline_ref_cnt - 1, ctx.ref_cnt_[1]);
  ASSERT_EQ(baseline_ref_cnt - 1, ctx.ref_cnt_[2]);
  ASSERT_EQ(baseline_ref_cnt - 1, tablet_handle.get_obj()->get_ref());
  ASSERT_GE(tablet_handle.get_obj()->get_wash_score(), min_expected_wash_score);
  ASSERT_LE(tablet_handle.get_obj()->get_wash_score(), max_expected_wash_score);
  ASSERT_NE(ctx.loaded_pointer_[0], ctx.loaded_pointer_[1]);
  ASSERT_NE(ctx.loaded_pointer_[1], ctx.loaded_pointer_[2]);
  ASSERT_EQ(old_addr, ctx.loaded_addr_[0]);
  ASSERT_EQ(middle_addr, ctx.loaded_addr_[1]);
  ASSERT_EQ(final_addr, ctx.loaded_addr_[2]);
  ASSERT_TRUE(copied_handle.is_valid());
  ASSERT_EQ(ctx.loaded_tablet_[2], copied_handle.get_obj());
  ASSERT_EQ(final_addr, copied_handle.get_obj()->get_tablet_addr());
  ASSERT_TRUE(copied_handle.get_obj()->get_pointer_handle().is_valid());
  ASSERT_EQ(ctx.loaded_pointer_[2],
      copied_handle.get_obj()->get_pointer_handle().get_resource_ptr());
  ASSERT_EQ(final_addr,
      copied_handle.get_obj()->get_pointer_handle().get_resource_ptr()->get_addr());
  ReleaseExternalTablet(copied_handle);
}

void TestMetaPointerMap::RunExternalLoadMutationCase(
    const ExternalLoadWindow window,
    const ExternalLoadMutation mutation,
    const int64_t case_id)
{
  SCOPED_TRACE(::testing::Message()
      << "window=" << static_cast<int>(window)
      << ", mutation=" << static_cast<int>(mutation));

  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(1000 + case_id));
  ObMetaDiskAddr old_addr;
  ObMetaDiskAddr new_addr;
  SetDiskAddr(100 + case_id * 2, old_addr);
  SetDiskAddr(101 + case_id * 2, new_addr);
  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, old_addr, tablet_handle);
  HoldTabletMacroRef(key, old_addr, tablet_handle);
  ObTablet *old_tablet = tablet_handle.get_obj();
  ObTabletHandle replacement_handle;
  if (ExternalLoadMutation::CAS == mutation) {
    AcquireTabletHandle(new_addr, replacement_handle);
  }

  int64_t macro_ref_cnt = -1;
  EXPECT_EQ(OB_SUCCESS, GetMacroBlockRef(old_addr, macro_ref_cnt));
  ASSERT_EQ(1, macro_ref_cnt);
  DeterministicSyncPoint after_load_sync;
  ExternalLoadTestContext ctx;
  ctx.tablet_ = old_tablet;
  if (ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE == window) {
    ctx.after_load_sync_ = &after_load_sync;
    ctx.sync_load_index_ = 0;
  }
  g_external_load_test_ctx = &ctx;

  TrackingAllocator tracking_allocator;
  if (ExternalLoadWindow::DURING_POST_WORK == window) {
    // The first backing allocation stores ObTablet.  With a deliberately
    // small arena page, the second one is the table-store allocation made
    // from inside deserialize_post_work().
    tracking_allocator.block_on_alloc(2);
  }
  common::ObArenaAllocator external_allocator(tracking_allocator, 64, true);
  int load_ret = OB_ERROR;
  ObTabletHandle copied_handle;
  std::thread loader([&]() {
    ObTenantEnv::set_tenant(&tenant_base_);
    load_ret = tablet_map_.get_meta_obj_with_external_memory(
        key, external_allocator, copied_handle, true/*force_alloc_new*/);
  });

  const bool loader_reached_window =
      ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE == window
      ? after_load_sync.wait_until_reached()
      : tracking_allocator.wait_until_blocked();
  if (!loader_reached_window) {
    after_load_sync.release();
    tracking_allocator.release();
    loader.join();
    ADD_FAILURE() << "loader did not reach the requested synchronization window";
    ReleaseExternalTablet(copied_handle);
    replacement_handle.reset();
    ObTabletHandle erase_guard;
    (void)tablet_map_.erase(key, erase_guard);
    tablet_handle.reset();
    erase_guard.reset();
    DrainTabletGcQueue();
    external_allocator.reset();
    return;
  }

  int mutation_ret = OB_ERROR;
  void *washed_obj = nullptr;
  DeterministicSyncPoint mutation_done;
  std::thread mutator([&]() {
    ObTenantEnv::set_tenant(&tenant_base_);
    if (ExternalLoadMutation::CAS == mutation) {
      mutation_ret = tablet_map_.compare_and_swap_addr_and_object(
          key, tablet_handle, replacement_handle);
      tablet_handle.reset();
    } else if (ExternalLoadMutation::WASH == mutation) {
      // Leave the map reference and tmp_guard as the only two owners.  Wash
      // must be rejected specifically because tmp_guard still protects the
      // disk image being copied.
      tablet_handle.reset();
      ObTabletHandle wash_guard;
      mutation_ret = tablet_map_.wash_meta_obj(key, wash_guard, washed_obj);
      wash_guard.reset();
    } else {
      tablet_handle.reset();
      ObTabletHandle erase_guard;
      mutation_ret = tablet_map_.erase(key, erase_guard);
      erase_guard.reset();
    }
    mutation_done.arrive_and_wait();
  });

  const bool mutation_completed_while_paused = mutation_done.wait_until_reached();
  const int64_t ref_cnt_while_paused = old_tablet->get_ref();
  EXPECT_EQ(OB_SUCCESS, GetMacroBlockRef(old_addr, macro_ref_cnt));
  EXPECT_EQ(1, macro_ref_cnt);
  mutation_done.release();
  after_load_sync.release();
  tracking_allocator.release();
  mutator.join();
  loader.join();

  EXPECT_TRUE(mutation_completed_while_paused)
      << "the competing operation was blocked by a bucket lock";
  EXPECT_EQ(OB_SUCCESS, mutation_ret);
  EXPECT_EQ(old_addr, ctx.loaded_addr_[0]);
  if (ExternalLoadMutation::WASH == mutation) {
    EXPECT_EQ(nullptr, washed_obj);
    EXPECT_EQ(2, ref_cnt_while_paused);
  } else {
    // CAS and erase remove the map reference and the mutator drops its local
    // handle.  The object and its disk macro are now owned only by tmp_guard.
    EXPECT_EQ(1, ref_cnt_while_paused);
  }

  if (ExternalLoadMutation::ERASE == mutation
      && ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE == window) {
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, load_ret);
    EXPECT_FALSE(copied_handle.is_valid());
  } else {
    EXPECT_EQ(OB_SUCCESS, load_ret);
    EXPECT_TRUE(copied_handle.is_valid());
    if (copied_handle.is_valid()) {
      const ObMetaDiskAddr &expected_addr =
          ExternalLoadMutation::CAS == mutation
              && ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE == window
          ? new_addr : old_addr;
      EXPECT_EQ(expected_addr, copied_handle.get_obj()->get_tablet_addr());
      EXPECT_TRUE(copied_handle.get_obj()->get_pointer_handle().is_valid());
    }
  }
  const int64_t expected_load_count =
      ExternalLoadMutation::CAS == mutation
          && ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE == window
      ? 2 : 1;
  EXPECT_EQ(expected_load_count, ctx.call_count_);

  const bool copied_old_disk_image = copied_handle.is_valid()
      && copied_handle.get_obj()->get_tablet_addr() == old_addr;
  EXPECT_EQ(OB_SUCCESS, GetMacroBlockRef(old_addr, macro_ref_cnt));
  EXPECT_EQ(copied_old_disk_image ? 2 : 1, macro_ref_cnt);
  if (copied_handle.is_valid()) {
    EXPECT_TRUE(copied_handle.get_obj()->hold_ref_cnt_);
  }
  ReleaseExternalTablet(copied_handle);
  EXPECT_EQ(OB_SUCCESS, GetMacroBlockRef(old_addr, macro_ref_cnt));
  EXPECT_EQ(1, macro_ref_cnt);

  replacement_handle.reset();
  if (ExternalLoadMutation::ERASE != mutation) {
    ObTabletHandle erase_guard;
    EXPECT_EQ(OB_SUCCESS, tablet_map_.erase(key, erase_guard));
    erase_guard.reset();
  }
  DrainTabletGcQueue();
  EXPECT_EQ(OB_SUCCESS, GetMacroBlockRef(old_addr, macro_ref_cnt));
  EXPECT_EQ(0, macro_ref_cnt);
  EXPECT_GT(tracking_allocator.live_count(), 0);
  external_allocator.reset();
  EXPECT_EQ(tracking_allocator.alloc_count(), tracking_allocator.free_count());
  EXPECT_EQ(0, tracking_allocator.live_count());
  EXPECT_FALSE(tracking_allocator.has_double_free());
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_guard_covers_mutations_after_load)
{
  ASSERT_EQ(OB_SUCCESS, InitBlockManagerForTest());
  RunExternalLoadMutationCase(
      ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE, ExternalLoadMutation::CAS, 1);
  RunExternalLoadMutationCase(
      ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE, ExternalLoadMutation::WASH, 2);
  RunExternalLoadMutationCase(
      ExternalLoadWindow::AFTER_LOAD_BEFORE_VALIDATE, ExternalLoadMutation::ERASE, 3);
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_guard_covers_mutations_during_post_work)
{
  ASSERT_EQ(OB_SUCCESS, InitBlockManagerForTest());
  RunExternalLoadMutationCase(
      ExternalLoadWindow::DURING_POST_WORK, ExternalLoadMutation::CAS, 4);
  RunExternalLoadMutationCase(
      ExternalLoadWindow::DURING_POST_WORK, ExternalLoadMutation::WASH, 5);
  RunExternalLoadMutationCase(
      ExternalLoadWindow::DURING_POST_WORK, ExternalLoadMutation::ERASE, 6);
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_rolls_back_partial_macro_refs_once)
{
  ASSERT_EQ(OB_SUCCESS, InitBlockManagerForTest());
  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(201));
  ObMetaDiskAddr disk_addr;
  ObMetaDiskAddr first_macro_addr;
  ObMetaDiskAddr failing_macro_addr;
  SetFileAddr(201, disk_addr);
  SetDiskAddr(701, first_macro_addr);
  SetDiskAddr(702, failing_macro_addr);
  // ObMetaDiskAddr accepts this representation, while MacroBlockId rejects it.
  // That lets inner_inc_macro_ref_cnt() fail after the preceding address was
  // incremented and exercise its real partial rollback path.
  failing_macro_addr.third_id_ = 1;
  ASSERT_TRUE(failing_macro_addr.is_valid());
  ASSERT_FALSE(failing_macro_addr.block_id().is_valid());

  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, disk_addr, tablet_handle);
  const int64_t baseline_ref_cnt = tablet_handle.get_obj()->get_ref();
  ExternalLoadTestContext ctx;
  ctx.tablet_ = tablet_handle.get_obj();
  ctx.partial_macro_ref_failure_ = true;
  ctx.first_macro_addr_ = first_macro_addr;
  ctx.failing_macro_addr_ = failing_macro_addr;
  g_external_load_test_ctx = &ctx;

  TrackingAllocator tracking_allocator;
  common::ObArenaAllocator external_allocator(tracking_allocator, 64, true);
  ObTabletHandle copied_handle;
  ASSERT_EQ(OB_INVALID_ARGUMENT, tablet_map_.get_meta_obj_with_external_memory(
      key, external_allocator, copied_handle, true/*force_alloc_new*/));
  ASSERT_EQ(1, ctx.call_count_);
  ASSERT_EQ(baseline_ref_cnt + 1, ctx.ref_cnt_[0]);
  ASSERT_EQ(baseline_ref_cnt, tablet_handle.get_obj()->get_ref());
  ASSERT_FALSE(copied_handle.is_valid());

  int64_t macro_ref_cnt = -1;
  ASSERT_EQ(OB_SUCCESS, GetMacroBlockRef(first_macro_addr, macro_ref_cnt));
  ASSERT_EQ(0, macro_ref_cnt);
  ASSERT_GE(tracking_allocator.alloc_count(), 2);
  ASSERT_GT(tracking_allocator.live_count(), 0);
  external_allocator.reset();
  ASSERT_EQ(tracking_allocator.alloc_count(), tracking_allocator.free_count());
  ASSERT_EQ(0, tracking_allocator.live_count());
  ASSERT_FALSE(tracking_allocator.has_double_free());
}

TEST_F(TestMetaPointerMap, test_force_alloc_new_cleans_up_after_assign_pointer_handle_failure)
{
  ASSERT_EQ(OB_SUCCESS, InitBlockManagerForTest());
  ObLS fake_ls;
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(202));
  ObMetaDiskAddr disk_addr;
  SetDiskAddr(703, disk_addr);
  ObTabletHandle tablet_handle;
  PrepareInMemoryTablet(fake_ls, key, disk_addr, tablet_handle);
  const int64_t baseline_ref_cnt = tablet_handle.get_obj()->get_ref();
  ExternalLoadTestContext ctx;
  ctx.tablet_ = tablet_handle.get_obj();
  g_external_load_test_ctx = &ctx;
  SetAssignPointerHandleErrsim(OB_ERR_UNEXPECTED, 1);

  TrackingAllocator tracking_allocator;
  common::ObArenaAllocator external_allocator(tracking_allocator, 64, true);
  ObTabletHandle copied_handle;
  ASSERT_EQ(OB_ERR_UNEXPECTED, tablet_map_.get_meta_obj_with_external_memory(
      key, external_allocator, copied_handle, true/*force_alloc_new*/));
  ASSERT_EQ(1, ctx.call_count_);
  ASSERT_EQ(baseline_ref_cnt + 1, ctx.ref_cnt_[0]);
  ASSERT_EQ(baseline_ref_cnt, tablet_handle.get_obj()->get_ref());
  ASSERT_FALSE(copied_handle.is_valid());

  int64_t macro_ref_cnt = -1;
  ASSERT_EQ(OB_SUCCESS, GetMacroBlockRef(disk_addr, macro_ref_cnt));
  ASSERT_EQ(0, macro_ref_cnt);
  ASSERT_GE(tracking_allocator.alloc_count(), 2);
  ASSERT_GT(tracking_allocator.live_count(), 0);
  external_allocator.reset();
  ASSERT_EQ(tracking_allocator.alloc_count(), tracking_allocator.free_count());
  ASSERT_EQ(0, tracking_allocator.live_count());
  ASSERT_FALSE(tracking_allocator.has_double_free());
}

class CalculateSize final
{
public:
  explicit CalculateSize(int64_t &size);
  ~CalculateSize() = default;

  int operator()(common::hash::HashMapPair<ObTabletMapKey, ObTablet *> &entry);

private:
  int64_t &size_;
};

CalculateSize::CalculateSize(int64_t &size)
  : size_(size)
{
}

int CalculateSize::operator()(common::hash::HashMapPair<ObTabletMapKey, ObTablet *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(entry.second)) {
    size_++;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer is invalid", K(ret), KP(entry.second));
  }
  return ret;
}

TEST_F(TestMetaPointerMap, test_meta_pointer_handle)
{
  ObLS fake_ls;
  FakeLs(fake_ls);

  observer::ObIMetaReport *fake_reporter = (observer::ObIMetaReport *)0xff;

  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls, fake_reporter);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMemtableMgrHandle memtable_mgr_hdl;
  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_memtable_mgr(memtable_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ls_handle.ls_ = &fake_ls;
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr phy_addr;
  phy_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(phy_addr);
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));

  ret = tablet_map_.set(key, tablet_ptr);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ObMetaPointerHandle<ObTabletMapKey, ObTablet> ptr_handle_1(tablet_map_);
  ObMetaPointerHandle<ObTabletMapKey, ObTablet> ptr_handle_2(tablet_map_);

  ret = tablet_map_.get(key, ptr_handle_1);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(2, ptr_handle_1.ptr_->get_ref_cnt());

  ret = tablet_map_.get(key, ptr_handle_2);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(3, ptr_handle_2.ptr_->get_ref_cnt());

  ret = ptr_handle_2.assign(ptr_handle_1);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(3, ptr_handle_2.ptr_->get_ref_cnt());

  ptr_handle_1.reset();
  ASSERT_EQ(2, ptr_handle_2.ptr_->get_ref_cnt());

  ret = tablet_map_.get(key, ptr_handle_1);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(3, ptr_handle_1.ptr_->get_ref_cnt());

  ret = tablet_map_.get(key, ptr_handle_1);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(3, ptr_handle_1.ptr_->get_ref_cnt());

  ptr_handle_2.reset();
  ASSERT_EQ(2, ptr_handle_1.ptr_->get_ref_cnt());

  ObResourceValueStore<ObMetaPointer<ObTablet>> *tmp = ptr_handle_1.ptr_;
  ptr_handle_1.reset();
  ASSERT_EQ(1, tmp->get_ref_cnt());
}

TEST_F(TestMetaPointerMap, test_meta_pointer_map)
{
  ObLS fake_ls;
  FakeLs(fake_ls);
  observer::ObIMetaReport *fake_reporter = (observer::ObIMetaReport *)0xff;

  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls, fake_reporter);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMemtableMgrHandle memtable_mgr_hdl;
  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_memtable_mgr(memtable_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ls_handle.ls_ = &fake_ls;
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr phy_addr;
  phy_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(phy_addr);
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));

  ret = tablet_map_.set(key, tablet_ptr);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ret = tablet_map_.set(key, tablet_ptr);
  ASSERT_EQ(common::OB_HASH_EXIST, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ObTabletHandle handle;
  ret = tablet_map_.get_meta_obj(key, handle);
  ASSERT_EQ(common::OB_ITEM_NOT_SETTED, ret);
  ASSERT_TRUE(!handle.is_valid());
  ASSERT_EQ(nullptr, handle.get_obj());

  handle.reset();

  ObMetaObj<ObTablet> old_tablet_obj;
  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer);
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, old_tablet_obj.ptr_);
  old_tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  handle.set_obj(old_tablet_obj);

  /**
  ret = tablet_map_.set_meta_obj(key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  */

  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
  ObTabletHandle new_tablet_handle;
  new_tablet_handle.reset();
  new_tablet_handle.set_obj(old_tablet_obj);
  new_tablet_handle.get_obj()->set_tablet_addr(phy_addr);
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, new_tablet_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMetaObj<ObTablet> tablet_obj;
  MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer);
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, tablet_obj.ptr_);
  tablet_obj.ptr_->tablet_addr_ = phy_addr;
  tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  ObTabletHandle tablet_handle;
  tablet_handle.set_obj(tablet_obj);
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, tablet_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ret = tablet_map_.get_meta_obj(key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(handle.is_valid());
  ASSERT_EQ(tablet_obj.ptr_, handle.get_obj());

  ObTabletHandle tmp_handle;
  ret = tablet_map_.erase(key, tmp_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, tablet_map_.map_.size());
}

TEST_F(TestMetaPointerMap, test_erase_and_load_concurrency)
{
  ObLS fake_ls;
  FakeLs(fake_ls);
  observer::ObIMetaReport *fake_reporter = (observer::ObIMetaReport *)0xff;

  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls, fake_reporter);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMemtableMgrHandle memtable_mgr_hdl;
  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_memtable_mgr(memtable_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = MTL(ObTenantMetaMemMgr*)->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObLSHandle ls_handle;
  ls_handle.ls_ = &fake_ls;
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr phy_addr;
  phy_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(phy_addr);
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(101));

  ret = tablet_map_.set(key, tablet_ptr);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ret = tablet_map_.set(key, tablet_ptr);
  ASSERT_EQ(common::OB_HASH_EXIST, ret);
  ASSERT_EQ(1, tablet_map_.map_.size());

  ObTabletHandle handle;
  ret = tablet_map_.get_meta_obj(key, handle);
  ASSERT_EQ(common::OB_ITEM_NOT_SETTED, ret);
  ASSERT_TRUE(!handle.is_valid());
  ASSERT_EQ(nullptr, handle.get_obj());

  handle.reset();

  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  ObMetaObj<ObTablet> old_tablet_obj;
  MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer);
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, old_tablet_obj.ptr_);
  old_tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  handle.set_obj(old_tablet_obj);

  /**
  ret = tablet_map_.set_meta_obj(key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  */

  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
  ObTabletHandle new_tablet_handle;
  new_tablet_handle.reset();
  new_tablet_handle.set_obj(old_tablet_obj);
  new_tablet_handle.get_obj()->set_tablet_addr(phy_addr);
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, new_tablet_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMetaPointerHandle<ObTabletMapKey, ObTablet> ptr_hdl(tablet_map_);
  ret = tablet_map_.get(key, ptr_hdl);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(ptr_hdl.get_resource_ptr()->is_in_memory());
  ptr_hdl.get_resource_ptr()->reset_obj();

//  ret = tablet_map_.erase(key);
//  ASSERT_EQ(common::OB_SUCCESS, ret);
//  ASSERT_EQ(0, tablet_map_.map_.size());
//
//  ret = tablet_map_.load_and_hook_meta_obj(key, ptr_hdl, handle);
//  ASSERT_EQ(common::OB_ENTRY_NOT_EXIST, ret);
}

class TestMetaDiskAddr : public ::testing::Test
{
public:
  TestMetaDiskAddr() = default;
  virtual ~TestMetaDiskAddr() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
};

void TestMetaDiskAddr::SetUp()
{
}

void TestMetaDiskAddr::TearDown()
{
}

TEST_F(TestMetaDiskAddr, test_meta_disk_address)
{
  int64_t file_id = -1;
  int64_t offset = 0;
  int64_t size = 0;
  MacroBlockId macro_id;
  ObMetaDiskAddr none_addr;
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_block_addr(macro_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_file_addr(file_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_mem_addr(offset, size));

  ASSERT_TRUE(!none_addr.is_valid());
  none_addr.set_none_addr();
  ASSERT_TRUE(none_addr.is_valid());
  ASSERT_EQ(ObMetaDiskAddr::DiskType::NONE, none_addr.type_);
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_block_addr(macro_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_file_addr(file_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, none_addr.get_mem_addr(offset, size));

  ObMetaDiskAddr file_addr;
  ASSERT_TRUE(!file_addr.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, file_addr.set_file_addr(-1, 0, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, file_addr.set_file_addr(1, -1, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, file_addr.set_file_addr(1, ObMetaDiskAddr::MAX_OFFSET + 10, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, file_addr.set_file_addr(1, 0, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, file_addr.set_file_addr(1, ObMetaDiskAddr::MAX_OFFSET + 10, ObMetaDiskAddr::MAX_SIZE + sizeof(ObTablet)));
  ASSERT_EQ(OB_SUCCESS, file_addr.set_file_addr(1, 0, sizeof(ObTablet)));
  ASSERT_TRUE(file_addr.is_valid());
  ASSERT_EQ(ObMetaDiskAddr::DiskType::FILE, file_addr.type_);
  ASSERT_EQ(1, file_addr.file_id_);
  ASSERT_EQ(0, file_addr.offset_);
  ASSERT_EQ(sizeof(ObTablet), file_addr.size_);
  ASSERT_EQ(OB_NOT_SUPPORTED, file_addr.get_block_addr(macro_id, offset, size));
  ASSERT_EQ(OB_SUCCESS, file_addr.get_file_addr(file_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, file_addr.get_mem_addr(offset, size));

  ObMetaDiskAddr block_addr;
  ASSERT_TRUE(!block_addr.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, 0, sizeof(ObTablet)));
  macro_id.block_index_ = 100;
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, -1, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, ObMetaDiskAddr::MAX_OFFSET + 10, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, 0, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, ObMetaDiskAddr::MAX_OFFSET + 10, ObMetaDiskAddr::MAX_SIZE + sizeof(ObTablet)));
  ASSERT_EQ(OB_SUCCESS, block_addr.set_block_addr(macro_id, 0, sizeof(ObTablet)));
  ASSERT_TRUE(block_addr.is_valid());
  ASSERT_EQ(ObMetaDiskAddr::DiskType::BLOCK, block_addr.type_);
  ASSERT_EQ(macro_id.first_id_, block_addr.first_id_);
  ASSERT_EQ(macro_id.second_id_, block_addr.second_id_);
  ASSERT_EQ(macro_id.third_id_, block_addr.third_id_);
  ASSERT_EQ(0, block_addr.offset_);
  ASSERT_EQ(sizeof(ObTablet), block_addr.size_);
  ASSERT_EQ(OB_SUCCESS, block_addr.get_block_addr(macro_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, block_addr.get_file_addr(file_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, block_addr.get_mem_addr(offset, size));

  ObMetaDiskAddr mem_addr;
  ASSERT_TRUE(!mem_addr.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, mem_addr.set_mem_addr(ObMetaDiskAddr::MAX_OFFSET + 10, sizeof(ObTablet)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mem_addr.set_mem_addr(0, -1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, mem_addr.set_mem_addr(ObMetaDiskAddr::MAX_OFFSET + 10, ObMetaDiskAddr::MAX_SIZE + sizeof(ObTablet)));
  ASSERT_EQ(OB_SUCCESS, mem_addr.set_mem_addr(0, 0));
  ASSERT_EQ(OB_SUCCESS, mem_addr.set_mem_addr(0, sizeof(ObTablet)));
  ASSERT_TRUE(mem_addr.is_valid());
  ASSERT_EQ(ObMetaDiskAddr::DiskType::MEM, mem_addr.type_);
  ASSERT_EQ(0, mem_addr.offset_);
  ASSERT_EQ(sizeof(ObTablet), mem_addr.size_);
  ASSERT_EQ(OB_NOT_SUPPORTED, mem_addr.get_block_addr(macro_id, offset, size));
  ASSERT_EQ(OB_NOT_SUPPORTED, mem_addr.get_file_addr(file_id, offset, size));
  ASSERT_EQ(OB_SUCCESS, mem_addr.get_mem_addr(offset, size));
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_meta_pointer_map.log*");
  OB_LOGGER.set_file_name("test_meta_pointer_map.log", true);
  OB_LOGGER.set_log_level("INFO");
  signal(49, SIG_IGN);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
