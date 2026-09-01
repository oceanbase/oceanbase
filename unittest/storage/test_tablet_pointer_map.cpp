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

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/multi_data_source/ob_tablet_truncate_mds_ctx.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "storage/schema_utils.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet_truncate_mds_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

class TestReplayEnvironmentGuard final
{
public:
  TestReplayEnvironmentGuard(ObTenantBase &tenant_base, ObLSService &ls_service, ObLS &ls)
    : tenant_base_(tenant_base),
      ls_service_(ls_service),
      ls_(ls),
      old_ls_inited_(ls.is_inited_),
      ls_ref_held_(false),
      ls_added_(false),
      service_installed_(false)
  {}

  ~TestReplayEnvironmentGuard()
  {
    if (ls_added_) {
      (void) ls_service_.get_ls_map()->del_ls(ls_.get_ls_id());
    }
    if (ls_ref_held_) {
      (void) ls_.get_ref_mgr().dec(ObLSGetMod::TXSTORAGE_MOD);
    }
    ls_.is_inited_ = old_ls_inited_;
    if (service_installed_) {
      shared_mem_alloc_mgr_.destroy();
      tenant_base_.set(static_cast<ObLSService *>(nullptr));
      tenant_base_.set(static_cast<share::ObSharedMemAllocMgr *>(nullptr));
      tenant_base_.set(static_cast<mds::ObTenantMdsService *>(nullptr));
      ObTenantEnv::set_tenant(&tenant_base_);
    }
    ls_service_.is_stopped_ = true;
  }

  int init()
  {
    int ret = OB_SUCCESS;
    ls_service_.is_inited_ = true;
    ls_service_.is_stopped_ = true;
    if (OB_FAIL(ls_service_.get_ls_map()->init(
            OB_SERVER_TENANT_ID, lib::ObMallocAllocator::get_instance()))) {
    } else {
      tenant_base_.set(&ls_service_);
      tenant_base_.set(&shared_mem_alloc_mgr_);
      tenant_base_.set(&mds_service_);
      ObTenantEnv::set_tenant(&tenant_base_);
      service_installed_ = true;
      if (OB_FAIL(shared_mem_alloc_mgr_.init())) {
      } else {
        ls_.is_inited_ = true;
        if (OB_FAIL(ls_.get_ref_mgr().inc(ObLSGetMod::TXSTORAGE_MOD))) {
        } else {
          ls_ref_held_ = true;
          if (OB_FAIL(ls_service_.get_ls_map()->add_ls(ls_))) {
          } else {
            ls_added_ = true;
          }
        }
      }
    }
    return ret;
  }

private:
  ObTenantBase &tenant_base_;
  ObLSService &ls_service_;
  ObLS &ls_;
  share::ObSharedMemAllocMgr shared_mem_alloc_mgr_;
  mds::ObTenantMdsService mds_service_;
  const bool old_ls_inited_;
  bool ls_ref_held_;
  bool ls_added_;
  bool service_installed_;
};

class TestNamedEventGuard final
{
public:
  explicit TestNamedEventGuard(const char *event_name) : event_name_(event_name) {}
  ~TestNamedEventGuard() { reset(); }

  int set_once(const int error_code)
  {
    common::EventItem item;
    item.error_code_ = error_code;
    item.occur_ = 1;
    item.trigger_freq_ = 1;
    return common::EventTable::set_event(event_name_, item);
  }

  void reset()
  {
    common::EventItem item;
    (void) common::EventTable::set_event(event_name_, item);
  }

private:
  const char *event_name_;
};

int64_t ObTenantMetaMemMgr::cal_adaptive_bucket_num()
{
  return 1000;
}

int ObTenantMetaMemMgr::fetch_tenant_config()
{
  return OB_SUCCESS;
}

int ObTabletPointerMap::load_meta_obj(
    const ObTabletMapKey &key,
    ObTabletPointer *meta_pointer,
    common::ObArenaAllocator &allocator,
    ObMetaDiskAddr &load_addr,
    ObTablet *t)
{
  UNUSEDx(key, meta_pointer, allocator, load_addr, t);
  return OB_SUCCESS;
}

class TestMetaPointerMap : public ::testing::Test
{
public:
  TestMetaPointerMap();
  virtual ~TestMetaPointerMap() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  void FakeLs(ObLS &ls);

private:
  static constexpr uint64_t TEST_TENANT_ID = OB_SERVER_TENANT_ID;
  ObTabletPointerMap tablet_map_;
  common::ObArenaAllocator allocator_;
  ObTenantBase tenant_base_;
};

TestMetaPointerMap::TestMetaPointerMap()
  : tablet_map_(),
    tenant_base_(TEST_TENANT_ID)
{
}

void TestMetaPointerMap::SetUp()
{
  lib::ObMemAttr attr(OB_SERVER_TENANT_ID, "TabletMap");
  int ret = tablet_map_.init(1000L, attr, 15 * 1024L * 1024L * 1024L, 8 * 1024L * 1024L,
          common::OB_MALLOC_NORMAL_BLOCK_SIZE);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObTenantMetaMemMgr *t3m = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, t3m->init());

  ObTabletMemtableMgrPool *pool = OB_NEW(ObTabletMemtableMgrPool, ObModIds::TEST);
  tenant_base_.set(t3m);
  tenant_base_.set(pool);
  ObTenantEnv::set_tenant(&tenant_base_);
  ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
}

void TestMetaPointerMap::TearDown()
{
  tablet_map_.destroy();
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
  ls.ls_meta_.restore_status_ = ObLSRestoreStatus::NONE;
  ls.ls_meta_.rebuild_seq_ = 0;
  ls.ls_meta_.store_format_ = common::ObLSStoreType::OB_LS_STORE_NORMAL;
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

  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ObTabletMemtableMgr *ptr = MTL(ObTabletMemtableMgrPool*)->acquire();
  OB_ASSERT(NULL != ptr);
  ObMemtableMgrHandle memtable_mgr_hdl(ptr, MTL(ObTabletMemtableMgrPool*));

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

  ObTabletPointerHandle ptr_handle_1(tablet_map_);
  ObTabletPointerHandle ptr_handle_2(tablet_map_);

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

  ObResourceValueStore<ObTabletPointer> *tmp = ptr_handle_1.ptr_;
  ptr_handle_1.reset();
  ASSERT_EQ(1, tmp->get_ref_cnt());
}

TEST_F(TestMetaPointerMap, test_meta_pointer_map)
{
  ObLS fake_ls;
  FakeLs(fake_ls);
  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ObTabletMemtableMgr *ptr = MTL(ObTabletMemtableMgrPool*)->acquire();
  OB_ASSERT(NULL != ptr);
  ObMemtableMgrHandle memtable_mgr_hdl(ptr, MTL(ObTabletMemtableMgrPool*));

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
  handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, old_tablet_obj);

  /**
  ret = tablet_map_.set_meta_obj(key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  */

  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;

  old_tablet_obj.ptr_->is_inited_ = true;
  old_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test
  ObTabletPointerHandle ptr_handle(tablet_map_);
  ret = tablet_map_.get(key, ptr_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = handle.get_obj()->assign_pointer_handle(ptr_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ObUpdateTabletPointerParam param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.resident_info_.addr_ = phy_addr;
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMetaObj<ObTablet> tablet_obj;
  MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer);
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, tablet_obj.ptr_);
  tablet_obj.ptr_->tablet_addr_ = phy_addr;
  tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  ObTabletHandle tablet_handle;
  tablet_handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, tablet_obj);

  tablet_obj.ptr_->is_inited_ = true;
  tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.resident_info_.addr_ = phy_addr;
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, tablet_handle, param);
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

TEST_F(TestMetaPointerMap, test_acquire_full_truncate_tablet_gets_current_old_handle)
{
  ObLS fake_ls;
  FakeLs(fake_ls);
  ObLSService ls_service;
  TestReplayEnvironmentGuard replay_environment_guard(
      tenant_base_, ls_service, fake_ls);
  ASSERT_EQ(common::OB_SUCCESS, replay_environment_guard.init());
  ObMdsSchemaHelper::get_instance().init();
  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObDDLKvMgrHandle ddl_kv_mgr_hdl;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ASSERT_NE(nullptr, t3m);

  ObTabletMemtableMgr *ptr = MTL(ObTabletMemtableMgrPool*)->acquire();
  ASSERT_NE(nullptr, ptr);
  ObMemtableMgrHandle memtable_mgr_hdl(ptr, MTL(ObTabletMemtableMgrPool*));
  ASSERT_EQ(common::OB_SUCCESS, t3m->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl));

  const ObTabletMapKey key(ObLSID(1001), ObTabletID(102));
  ObLSHandle ls_handle;
  ASSERT_EQ(common::OB_SUCCESS,
            ls_service.get_ls(key.ls_id_, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(none_addr);
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.set(key, tablet_ptr));

  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  ObMetaObj<ObTablet> old_tablet_obj;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_buffer_pool_.acquire(tablet_buffer));
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, old_tablet_obj.ptr_);
  old_tablet_obj.pool_ = &t3m->tablet_buffer_pool_;
  ObTabletHandle old_handle;
  old_handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, old_tablet_obj);

  ObMetaDiskAddr phy_addr;
  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;

  old_tablet_obj.ptr_->is_inited_ = true;
  old_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr();
  ObTabletPointerHandle ptr_handle(t3m->tablet_map_);
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get(key, ptr_handle));
  ASSERT_EQ(common::OB_SUCCESS, old_handle.get_obj()->assign_pointer_handle(ptr_handle));
  ObUpdateTabletPointerParam param;
  ASSERT_EQ(common::OB_SUCCESS, old_handle.get_obj()->get_updating_tablet_pointer_param(param));
  param.resident_info_.addr_ = phy_addr;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->tablet_map_.compare_and_swap_addr_and_object(
                key, old_handle, old_handle, param));

  // Verify that the fixture permits acquiring a truncate tablet for the current object.
  ObTabletHandle pending_handle;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->acquire_full_truncate_tablet(
                WashTabletPriority::WTP_HIGH, key, old_handle, pending_handle));
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->try_release_pending_truncate_tablet(key, false/*restore_tablet_read_flag*/, "unit_test_current_object"));
  pending_handle.reset();

  ObMetaObj<ObTablet> current_tablet_obj;
  tablet_buffer = nullptr;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_buffer_pool_.acquire(tablet_buffer));
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, current_tablet_obj.ptr_);
  current_tablet_obj.pool_ = &t3m->tablet_buffer_pool_;
  ObTabletHandle current_handle;
  current_handle.set_obj(
      ObTabletHandle::ObTabletHdlType::FROM_T3M, current_tablet_obj);
  current_tablet_obj.ptr_->is_inited_ = true;
  current_tablet_obj.ptr_->tablet_addr_ = phy_addr;
  current_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr();
  current_tablet_obj.ptr_->tablet_meta_.ls_id_ = key.ls_id_;
  current_tablet_obj.ptr_->tablet_meta_.tablet_id_ = key.tablet_id_;
  current_tablet_obj.ptr_->tablet_meta_.data_tablet_id_ = key.tablet_id_;
  current_tablet_obj.ptr_->tablet_meta_.mds_checkpoint_scn_.set_min();
  ASSERT_EQ(common::OB_SUCCESS,
            current_tablet_obj.ptr_->tablet_meta_.ha_status_.init_status());
  ASSERT_EQ(common::OB_SUCCESS,
            current_handle.get_obj()->assign_pointer_handle(ptr_handle));
  ASSERT_EQ(common::OB_SUCCESS,
            old_handle.get_obj()->get_updating_tablet_pointer_param(param));
  param.resident_info_.addr_ = phy_addr;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->tablet_map_.compare_and_swap_addr_and_object(
                key, old_handle, current_handle, param));

  ObTabletHandle actual_handle;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get_meta_obj(key, actual_handle));
  ASSERT_EQ(current_handle.get_obj(), actual_handle.get_obj());
  ASSERT_TRUE(old_handle.is_valid());
  ObTablet *stale_old_tablet = old_handle.get_obj();
  ASSERT_NE(nullptr, stale_old_tablet);
  ASSERT_NE(current_handle.get_obj(), stale_old_tablet);

  ret = t3m->acquire_full_truncate_tablet(
      WashTabletPriority::WTP_HIGH, key, old_handle, pending_handle);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  if (common::OB_SUCCESS == ret) {
    EXPECT_EQ(current_handle.get_obj(), old_handle.get_obj());
    ObPendingTruncateTabletMap::MapValue pending_value;
    ASSERT_EQ(common::OB_SUCCESS,
              t3m->get_pending_truncate_tablet(key, pending_value));
    EXPECT_EQ(current_handle.get_obj(), pending_value.old_handle_.get_obj());
    EXPECT_EQ(pending_handle.get_obj(), pending_value.new_handle_.get_obj());
  }

  bool pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  if (common::OB_SUCCESS == ret) {
    EXPECT_TRUE(pending_exists);
    ASSERT_EQ(common::OB_SUCCESS,
              t3m->try_release_pending_truncate_tablet(key, false/*restore_tablet_read_flag*/, "unit_test_current_old_object"));
  } else {
    EXPECT_FALSE(pending_exists);
  }
  pending_handle.reset();
  pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  EXPECT_FALSE(pending_exists);

  actual_handle.reset();
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get_meta_obj(key, actual_handle));
  ASSERT_EQ(current_handle.get_obj(), actual_handle.get_obj());

  ObTabletTruncateMdsUserData committed_truncate;
  ASSERT_EQ(common::OB_SUCCESS, committed_truncate.truncate_commit_scn_.convert_for_tx(200));
  committed_truncate.truncate_commit_version_ = 200;
  committed_truncate.schema_version_ = 100;
  actual_handle.get_obj()->tablet_truncate_cache_.set_value(committed_truncate);
  ASSERT_EQ(common::OB_SUCCESS,
            actual_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200));

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  ASSERT_EQ(common::OB_SUCCESS,
            create_tablet_schema.init(schema_allocator,
                                      table_schema,
                                      lib::Worker::CompatMode::MYSQL,
                                      false/*skip_column_info*/,
                                      DATA_VERSION_4_3_0_0));

  // A replay covered by a newer committed truncate must be acknowledged as a no-op.
  share::SCN replay_scn;
  ASSERT_EQ(common::OB_SUCCESS, replay_scn.convert_for_tx(100));
  bool need_skip_replay = false;
  bool need_replay_mds_only = false;
  ret = tablet_svr->start_tablet_truncate_mds(key.tablet_id_,
                                               create_tablet_schema,
                                               create_tablet_schema.get_schema_version(),
                                               true/*is_replay*/,
                                               replay_scn,
                                               need_skip_replay,
                                               need_replay_mds_only);
  EXPECT_EQ(common::OB_SUCCESS, ret);
  EXPECT_TRUE(need_skip_replay);
  EXPECT_FALSE(need_replay_mds_only);
  pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  EXPECT_FALSE(pending_exists);

  actual_handle.reset();
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get_meta_obj(key, actual_handle));
  EXPECT_EQ(current_handle.get_obj(), actual_handle.get_obj());
  share::SCN expected_checkpoint_scn;
  ASSERT_EQ(common::OB_SUCCESS, expected_checkpoint_scn.convert_for_tx(200));
  EXPECT_EQ(expected_checkpoint_scn,
            actual_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_);

  // The truncated tablet may be persisted before its truncate MDS data. Exercise
  // the complete helper/executor path to verify that replay backfills only MDS.
  actual_handle.get_obj()->tablet_truncate_cache_.set_empty();
  const ObMetaDiskAddr expected_table_store_addr =
      actual_handle.get_obj()->table_store_addr_.addr_;
  const ObTabletTableStore *expected_table_store =
      actual_handle.get_obj()->table_store_addr_.get_ptr();
  ObTabletTruncateMdsArg replay_arg;
  ASSERT_EQ(common::OB_SUCCESS,
            replay_arg.init(key.ls_id_,
                            key.tablet_id_,
                            lib::Worker::CompatMode::MYSQL,
                            table_schema,
                            table_schema.get_schema_version()));
  replay_arg.truncate_data_.truncate_commit_scn_ = replay_scn;
  replay_arg.truncate_data_.truncate_commit_version_ = replay_scn.get_val_for_tx();

  // The executor must skip an MDS record already covered by the MDS checkpoint.
  actual_handle.get_obj()->tablet_meta_.mds_checkpoint_scn_ = replay_scn;
  mds::ObTabletTruncateMdsCtx checkpoint_ctx(
      mds::MdsWriter(mds::WriterType::TRANSACTION, 1));
  ASSERT_EQ(common::OB_SUCCESS,
            ObTabletTruncateMdsHelper::replay_process(
                replay_arg, replay_scn, checkpoint_ctx));
  share::SCN actual_truncate_scn;
  int64_t actual_truncate_version = OB_INVALID_VERSION;
  ASSERT_EQ(common::OB_SUCCESS,
            actual_handle.get_obj()->get_tablet_truncate_scn_and_version(
                actual_truncate_scn, actual_truncate_version));
  EXPECT_EQ(share::SCN::min_scn(), actual_truncate_scn);
  EXPECT_EQ(0, actual_truncate_version);

  // A transient MDS write failure must propagate and leave replay retryable.
  actual_handle.get_obj()->tablet_meta_.mds_checkpoint_scn_.set_min();
  actual_handle.get_obj()->tablet_truncate_cache_.set_empty();
  mds::ObTabletTruncateMdsCtx replay_ctx(
      mds::MdsWriter(mds::WriterType::TRANSACTION, 2));
  TestNamedEventGuard replay_mds_failure(
      "EN_REPLAY_SET_TABLET_TRUNCATE_MDS_DATA_FAILED");
  ASSERT_EQ(common::OB_SUCCESS, replay_mds_failure.set_once(common::OB_EAGAIN));
  EXPECT_EQ(common::OB_EAGAIN,
            ObTabletTruncateMdsHelper::replay_process(
                replay_arg, replay_scn, replay_ctx));
  pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  EXPECT_FALSE(pending_exists);

  replay_mds_failure.reset();
  ASSERT_EQ(common::OB_SUCCESS,
            ObTabletTruncateMdsHelper::replay_process(
                replay_arg, replay_scn, replay_ctx));
  replay_ctx.on_commit(replay_scn, replay_scn);

  actual_handle.reset();
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get_meta_obj(key, actual_handle));
  EXPECT_EQ(current_handle.get_obj(), actual_handle.get_obj());
  EXPECT_EQ(expected_table_store_addr,
            actual_handle.get_obj()->table_store_addr_.addr_);
  EXPECT_EQ(expected_table_store,
            actual_handle.get_obj()->table_store_addr_.get_ptr());
  EXPECT_EQ(expected_checkpoint_scn,
            actual_handle.get_obj()->tablet_meta_.clog_checkpoint_scn_);
  ASSERT_EQ(common::OB_SUCCESS,
            actual_handle.get_obj()->get_tablet_truncate_scn_and_version(
                actual_truncate_scn, actual_truncate_version));
  EXPECT_EQ(replay_scn, actual_truncate_scn);
  EXPECT_EQ(replay_scn.get_val_for_tx(), actual_truncate_version);
  pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  EXPECT_FALSE(pending_exists);

  ObTabletHandle erased_handle;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.erase(key, erased_handle));
}

TEST_F(TestMetaPointerMap, test_transfer_tablet_enables_memtable_truncate_filter)
{
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ASSERT_NE(nullptr, t3m);

  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_buffer_pool_.acquire(tablet_buffer));
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObj<ObTablet> tablet_obj;
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, tablet_obj.ptr_);
  tablet_obj.pool_ = &t3m->tablet_buffer_pool_;

  ObTabletHandle tablet_handle;
  tablet_handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, tablet_obj);
  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  tablet->is_inited_ = true;
  tablet->table_store_addr_.addr_.set_none_addr();
  tablet->tablet_meta_.need_memtable_filter_after_truncate_tablet_ = false;

  share::SCN transfer_start_scn;
  ASSERT_EQ(common::OB_SUCCESS, transfer_start_scn.convert_for_tx(100));
  ASSERT_EQ(common::OB_SUCCESS,
            tablet->tablet_meta_.transfer_info_.init(
                share::ObLSID(1002), transfer_start_scn, 1, share::SCN::min_scn()));

  ObTabletTruncateMdsUserData truncate_data;
  ASSERT_EQ(common::OB_SUCCESS, truncate_data.truncate_commit_scn_.convert_for_tx(200));
  truncate_data.truncate_commit_version_ = 200;
  truncate_data.schema_version_ = 1;
  tablet->tablet_truncate_cache_.set_value(truncate_data);

  ObTableIterParam iter_param;
  iter_param.set_tablet_handle(&tablet_handle);
  memtable::ObMvccAccessCtx mvcc_ctx;
  memtable::ObMemtable memtable;
  ASSERT_EQ(common::OB_SUCCESS,
            memtable.setup_truncate_filter_on_mvcc_acc_ctx(iter_param, mvcc_ctx));
  EXPECT_TRUE(mvcc_ctx.need_memtable_filter_after_truncate_tablet_);
  EXPECT_EQ(truncate_data.truncate_commit_scn_, mvcc_ctx.truncate_commit_scn_);
  EXPECT_EQ(truncate_data.truncate_commit_version_, mvcc_ctx.truncate_commit_version_);
}

TEST_F(TestMetaPointerMap, test_start_truncate_replay_skips_empty_shell_tablet)
{
  ObLS fake_ls;
  FakeLs(fake_ls);
  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  ASSERT_EQ(common::OB_SUCCESS, tablet_svr->init(&fake_ls));

  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ASSERT_NE(nullptr, t3m);
  ObDDLKvMgrHandle ddl_kv_mgr_hdl;
  ASSERT_EQ(common::OB_SUCCESS, t3m->acquire_tablet_ddl_kv_mgr(ddl_kv_mgr_hdl));

  ObTabletMemtableMgr *memtable_mgr = MTL(ObTabletMemtableMgrPool*)->acquire();
  ASSERT_NE(nullptr, memtable_mgr);
  ObMemtableMgrHandle memtable_mgr_hdl(memtable_mgr, MTL(ObTabletMemtableMgrPool*));
  ObLSHandle ls_handle;
  ls_handle.ls_ = &fake_ls;
  ObTabletPointer tablet_ptr(ls_handle, memtable_mgr_hdl);
  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  tablet_ptr.set_addr_with_reset_obj(none_addr);
  const ObTabletMapKey key(ObLSID(1001), ObTabletID(103));
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.set(key, tablet_ptr));

  ObTenantMetaMemMgr::ObNormalTabletBuffer *tablet_buffer = nullptr;
  ObMetaObj<ObTablet> empty_shell_obj;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_buffer_pool_.acquire(tablet_buffer));
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, empty_shell_obj.ptr_);
  empty_shell_obj.pool_ = &t3m->tablet_buffer_pool_;
  ObTabletHandle empty_shell_handle;
  empty_shell_handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, empty_shell_obj);

  ObMetaDiskAddr phy_addr;
  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
  empty_shell_obj.ptr_->is_inited_ = true;
  empty_shell_obj.ptr_->tablet_meta_.is_empty_shell_ = true;
  empty_shell_obj.ptr_->table_store_addr_.addr_.set_none_addr();
  ObTabletPointerHandle ptr_handle(t3m->tablet_map_);
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.get(key, ptr_handle));
  ASSERT_EQ(common::OB_SUCCESS, empty_shell_handle.get_obj()->assign_pointer_handle(ptr_handle));
  ObUpdateTabletPointerParam param;
  ASSERT_EQ(common::OB_SUCCESS, empty_shell_handle.get_obj()->get_updating_tablet_pointer_param(param));
  param.resident_info_.addr_ = phy_addr;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->tablet_map_.compare_and_swap_addr_and_object(
                key, empty_shell_handle, empty_shell_handle, param));

  share::schema::ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  ASSERT_EQ(common::OB_SUCCESS,
            create_tablet_schema.init(schema_allocator,
                                      table_schema,
                                      lib::Worker::CompatMode::MYSQL,
                                      false/*skip_column_info*/,
                                      DATA_VERSION_4_3_0_0));

  share::SCN replay_scn;
  ASSERT_EQ(common::OB_SUCCESS, replay_scn.convert_for_tx(100));
  bool need_skip_replay = false;
  bool need_replay_mds_only = false;
  EXPECT_EQ(common::OB_NO_NEED_UPDATE,
            tablet_svr->start_tablet_truncate_mds(
                key.tablet_id_,
                create_tablet_schema,
                table_schema.get_schema_version(),
                true/*is_replay*/,
                replay_scn,
                need_skip_replay,
                need_replay_mds_only));
  EXPECT_FALSE(need_skip_replay);
  EXPECT_FALSE(need_replay_mds_only);
  bool pending_exists = false;
  ASSERT_EQ(common::OB_SUCCESS,
            t3m->pending_truncate_tablet_map_.has_exist(key, pending_exists));
  EXPECT_FALSE(pending_exists);

  ObTabletHandle erased_handle;
  ASSERT_EQ(common::OB_SUCCESS, t3m->tablet_map_.erase(key, erased_handle));
}

TEST_F(TestMetaPointerMap, test_erase_and_load_concurrency)
{
  ObLS fake_ls;
  FakeLs(fake_ls);

  ObLSTabletService *tablet_svr = fake_ls.get_tablet_svr();
  int ret = tablet_svr->init(&fake_ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObDDLKvMgrHandle ddl_kv_mgr_hdl;

  ObTabletMemtableMgr *ptr = MTL(ObTabletMemtableMgrPool*)->acquire();
  OB_ASSERT(NULL != ptr);
  ObMemtableMgrHandle memtable_mgr_hdl(ptr, MTL(ObTabletMemtableMgrPool*));

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
  handle.set_obj(ObTabletHandle::ObTabletHdlType::FROM_T3M, old_tablet_obj);

  /**
  ret = tablet_map_.set_meta_obj(key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  */

  phy_addr.first_id_ = 1;
  phy_addr.second_id_ = 2;
  phy_addr.offset_ = 0;
  phy_addr.size_ = 4096;
  phy_addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;

  old_tablet_obj.ptr_->is_inited_ = true;
  old_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test

  ObTabletPointerHandle ptr_handle(tablet_map_);
  ret = tablet_map_.get(key, ptr_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = handle.get_obj()->assign_pointer_handle(ptr_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ObUpdateTabletPointerParam param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.resident_info_.addr_ = phy_addr;
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObTabletPointerHandle ptr_hdl(tablet_map_);
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
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, 0, sizeof(ObTablet), ObMetaDiskAddr::DiskType::BLOCK));
  macro_id.block_index_ = 100;
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, -1, sizeof(ObTablet), ObMetaDiskAddr::DiskType::BLOCK));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, ObMetaDiskAddr::MAX_OFFSET + 10, sizeof(ObTablet), ObMetaDiskAddr::DiskType::BLOCK));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, 0, -1, ObMetaDiskAddr::DiskType::BLOCK));
  ASSERT_EQ(OB_INVALID_ARGUMENT, block_addr.set_block_addr(macro_id, ObMetaDiskAddr::MAX_OFFSET + 10, ObMetaDiskAddr::MAX_SIZE + sizeof(ObTablet), ObMetaDiskAddr::DiskType::BLOCK));
  ASSERT_EQ(OB_SUCCESS, block_addr.set_block_addr(macro_id, 0, sizeof(ObTablet), ObMetaDiskAddr::DiskType::BLOCK));
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
