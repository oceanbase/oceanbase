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

#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_pointer_map.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

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

  old_tablet_obj.ptr_->is_inited_ = true;
  old_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test
  ObUpdateTabletPointerParam param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.tablet_addr_ = phy_addr;
  ret = tablet_map_.compare_and_swap_addr_and_object(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObMetaObj<ObTablet> tablet_obj;
  MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_.acquire(tablet_buffer);
  ASSERT_NE(nullptr, tablet_buffer);
  ObMetaObjBufferHelper::new_meta_obj(tablet_buffer, tablet_obj.ptr_);
  tablet_obj.ptr_->tablet_addr_ = phy_addr;
  tablet_obj.pool_ = &MTL(ObTenantMetaMemMgr*)->tablet_buffer_pool_;
  ObTabletHandle tablet_handle;
  tablet_handle.set_obj(tablet_obj);

  tablet_obj.ptr_->is_inited_ = true;
  tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.tablet_addr_ = phy_addr;
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

  old_tablet_obj.ptr_->is_inited_ = true;
  old_tablet_obj.ptr_->table_store_addr_.addr_.set_none_addr(); // mock empty_shell to pass test

  ObUpdateTabletPointerParam param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  param.tablet_addr_ = phy_addr;
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
