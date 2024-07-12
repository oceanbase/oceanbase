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

#include "lib/alloc/memory_dump.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_leak_checker.h"
#include "storage/ls/ob_ls.h"
#include "storage/schema_utils.h"
#include "storage/mock_ob_log_handler.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablet/ob_tablet_table_store_flag.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tablet/ob_tablet_status.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_dml_common.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"

namespace oceanbase
{
using namespace share;

namespace memtable {

int ObMemtable::batch_remove_unused_callback_for_uncommited_txn(const ObLSID, const memtable::ObMemtableSet *)
{
  int ret = OB_SUCCESS;
  return ret;
}

}  // namespace memtable

namespace storage
{

int ObIMemtable::get_ls_id(share::ObLSID &ls_id)
{
  ls_id = share::ObLSID(1001);
  return OB_SUCCESS;
}

int ObTablet::check_and_set_initial_state()
{
  return OB_SUCCESS;
}

int ObTabletPersister::acquire_tablet(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    const bool try_smaller_pool,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;
  UNUSED(try_smaller_pool);
  if (new_handle.is_valid()) {
    // nothing to do
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
    if (OB_FAIL(t3m->acquire_tablet_from_pool(type, WashTabletPriority::WTP_LOW, key, new_handle))) {
      LOG_WARN("fail to acquire tablet from pool", K(ret), K(key), K(type));
    }
  }
  return ret;
}

class TestTenantMetaMemMgr : public ::testing::Test
{
public:
  TestTenantMetaMemMgr();
  virtual ~TestTenantMetaMemMgr() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_data_schema(common::ObArenaAllocator &allocator, ObCreateTabletSchema &create_tablet_schema);
  void prepare_create_sstable_param();
  void gc_all_tablets();
public:
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 3;
  static const int64_t TEST_COLUMN_CNT = 6;
  static const uint64_t TEST_TENANT_ID = 1;
  static const uint64_t TEST_ANOTHER_TENANT_ID = 2;
  static const int64_t TEST_LS_ID = 101;

public:
  const uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTenantMetaMemMgr t3m_;
  ObTabletCreateSSTableParam param_;
  share::schema::ObTableSchema table_schema_;
  common::ObArenaAllocator allocator_;
};

// record the old_t3m just for ls remove
ObTenantMetaMemMgr *old_t3m = nullptr;
TestTenantMetaMemMgr::TestTenantMetaMemMgr()
  : tenant_id_(TEST_TENANT_ID),
    ls_id_(TEST_LS_ID),
    t3m_(TEST_TENANT_ID),
    param_(),
    table_schema_(),
    allocator_()
{
}

void TestTenantMetaMemMgr::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  ObClockGenerator::init();

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  old_t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTenantMetaMemMgr::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  int ret = OB_SUCCESS;
  ret = t3m_.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(ObTabletHandleIndexMap::get_instance()->init(), OB_SUCCESS);

  ObTenantBase *tenant_base = MTL_CTX();
  tenant_base->set(&t3m_);
  TestSchemaUtils::prepare_data_schema(table_schema_);
  prepare_create_sstable_param();
}

void TestTenantMetaMemMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTenantMetaMemMgr::TearDown()
{
  table_schema_.reset();
  t3m_.stop();
  t3m_.wait();
  t3m_.destroy();

  ObTabletHandleIndexMap::get_instance()->reset();

  // return to the old t3m to make ls destroy success.
  ObTenantBase *tenant_base = MTL_CTX();
  tenant_base->set(old_t3m);
}

void TestTenantMetaMemMgr::prepare_data_schema(
  common::ObArenaAllocator &allocator, ObCreateTabletSchema &create_tablet_schema
)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  const uint64_t table_id = 219039915101;
  int64_t micro_block_size = 16 * 1024;
  ObColumnSchemaV2 column;

  table_schema.reset();
  ret = table_schema.set_table_name("test_ls_tablet_service_data_table");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(TEST_TENANT_ID);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  const int64_t column_ids[] = {16,17,20,21,22,23,24,29};
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = ObIntType;
    const int64_t column_id = column_ids[i];

    if (i == 1) {
      obj_type = ObVarcharType;
    }
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    LOG_INFO("add column", K(i), K(column));
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  LOG_INFO("dump data table schema", LITERAL_K(TEST_ROWKEY_COLUMN_CNT), K(table_schema));

  ret = create_tablet_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL,
        false/*skip_column_info*/, ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTenantMetaMemMgr::prepare_create_sstable_param()
{
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param_.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param_.table_key_.tablet_id_ = 1;
  param_.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param_.table_key_.version_range_.snapshot_version_ = 0;
  param_.schema_version_ = table_schema_.get_schema_version();
  param_.create_snapshot_version_ = 0;
  param_.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param_.progressive_merge_step_ = 0;
  param_.table_mode_ = table_schema_.get_table_mode_struct();
  param_.index_type_ = table_schema_.get_index_type();
  param_.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
          + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param_.root_block_addr_.set_none_addr();
  param_.data_block_macro_meta_addr_.set_none_addr();
  param_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param_.data_index_tree_height_ = 0;
  param_.index_blocks_cnt_ = 0;
  param_.data_blocks_cnt_ = 0;
  param_.micro_block_cnt_ = 0;
  param_.use_old_macro_block_count_ = 0;
  param_.column_cnt_ = table_schema_.get_column_count() + multi_version_col_cnt;
  param_.data_checksum_ = 0;
  param_.occupy_size_ = 0;
  param_.ddl_scn_.set_min();
  param_.filled_tx_scn_.set_min();
  param_.original_size_ = 0;
  param_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param_.encrypt_id_ = 0;
  param_.master_key_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param_.column_cnt_, param_.column_checksums_));
}

void TestTenantMetaMemMgr::gc_all_tablets()
{
  bool all_tablet_cleaned = false;
  while (!all_tablet_cleaned) {
    t3m_.gc_tablets_in_queue(all_tablet_cleaned); // do not use MTL t3m
  }
}

class TestConcurrentT3M : public share::ObThreadPool
{
public:
  TestConcurrentT3M(
      const int64_t thread_cnt,
      ObTenantMetaMemMgr &t3m_,
      ObTenantBase *tenant_base,
      common::ObArenaAllocator &allocator);
  virtual ~TestConcurrentT3M();
  virtual void run1();

  static const int64_t TABLET_CNT_PER_THREAD = 1000;

private:
  int64_t thread_cnt_;
  share::ObLSID ls_id_;
  ObTenantMetaMemMgr &t3m_;
  ObTenantBase *tenant_base_;
  common::SpinRWLock lock_;
  common::ObArenaAllocator &allocator_;
};

TestConcurrentT3M::TestConcurrentT3M(
    const int64_t thread_cnt,
    ObTenantMetaMemMgr &t3m,
    ObTenantBase *tenant_base,
    common::ObArenaAllocator &allocator)
  : thread_cnt_(thread_cnt),
    ls_id_(TestTenantMetaMemMgr::TEST_LS_ID),
    t3m_(t3m),
    tenant_base_(tenant_base),
    lock_(),
    allocator_(allocator)
{
  set_thread_count(static_cast<int32_t>(thread_cnt_));
}

TestConcurrentT3M::~TestConcurrentT3M()
{
}

void TestConcurrentT3M::run1()
{
  int ret = OB_SUCCESS;
  const int N = 131;
  ObLSHandle ls_handle;
  ObTabletHandle handle;
  ObTabletMapKey key;
  key.ls_id_ = ls_id_;

  share::ObTenantEnv::set_tenant(tenant_base_);

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  int count = TABLET_CNT_PER_THREAD;
  while (count-- > 0) {
    ObTablet *tablet = nullptr;
    key.tablet_id_ = thread_idx_ * TABLET_CNT_PER_THREAD * 10 + count + 1;
    {
      SpinWLockGuard guard(lock_);
      int ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
      handle.t3m_ = &t3m_; // temporary code, use local t3m
      ASSERT_EQ(common::OB_SUCCESS, ret);
      ASSERT_TRUE(handle.is_valid());
    }
    tablet = handle.get_obj();
    ASSERT_TRUE(nullptr != tablet);
    ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

    ObTabletHandle tmp_handle;
    ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, tmp_handle);
    ASSERT_EQ(common::OB_ITEM_NOT_SETTED, ret);
    ASSERT_TRUE(!tmp_handle.is_valid());

    tablet = handle.get_obj();
    ASSERT_TRUE(nullptr != tablet);
    ASSERT_TRUE(nullptr != handle.allocator_);
    if (0 == count % N) { /* mock empty tablet to del successfully */
      tablet->table_store_addr_.addr_.set_none_addr();
      tablet->storage_schema_addr_.addr_.set_none_addr();
      tablet->rowkey_read_info_ = nullptr;
    }

    ObMetaDiskAddr addr;
    addr.first_id_ = 1;
    addr.second_id_ = 2;
    addr.offset_ = 0;
    addr.size_ = 4096;
    addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
    handle.get_obj()->set_tablet_addr(addr);

    handle.get_obj()->is_inited_ = true; // to pass test
    handle.get_obj()->table_store_addr_.addr_.set_none_addr();
    ObUpdateTabletPointerParam param;
    ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ret = t3m_.compare_and_swap_tablet(key, handle, handle, param);
    ASSERT_EQ(common::OB_SUCCESS, ret);

    ObTabletPointerHandle ptr_hdl(t3m_.tablet_map_);
    ret = t3m_.tablet_map_.map_.get_refactored(key, ptr_hdl.ptr_);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ret = t3m_.tablet_map_.inc_handle_ref(ptr_hdl.ptr_);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    ASSERT_TRUE(addr == ptr_hdl.ptr_->ptr_->phy_addr_);
    ASSERT_TRUE(handle.obj_ == ptr_hdl.ptr_->ptr_->obj_.ptr_);
    ASSERT_TRUE(handle.allocator_ == ptr_hdl.ptr_->ptr_->obj_.allocator_);

    handle.reset();

    if (0 == count % N) {
      ret = t3m_.del_tablet(key);
      ASSERT_EQ(common::OB_SUCCESS, ret);
    } else {
      ret = t3m_.tablet_map_.erase(key, tmp_handle);
      ASSERT_EQ(common::OB_SUCCESS, ret);
      tmp_handle.reset();
    }
  }
}

TEST_F(TestTenantMetaMemMgr, test_bucket_cnt)
{
  const int64_t unify_bucket_num = common::hash::cal_next_prime(t3m_.cal_adaptive_bucket_num());
  const int64_t t3m_lock_bkt_cnt = t3m_.bucket_lock_.bucket_cnt_;
  const int64_t t3m_map_bkt_cnt = t3m_.tablet_map_.map_.bucket_count();
  const int64_t t3m_map_lock_bkt_cnt = t3m_.tablet_map_.bucket_lock_.bucket_cnt_;
  ASSERT_NE(unify_bucket_num, 0);
  // ASSERT_EQ(unify_bucket_num, t3m_lock_bkt_cnt);
  ASSERT_EQ(unify_bucket_num, t3m_map_bkt_cnt);
  ASSERT_EQ(unify_bucket_num, t3m_map_lock_bkt_cnt);
}

TEST_F(TestTenantMetaMemMgr, test_sstable)
{
  void *buf = nullptr;
  ObSSTable *sstable = nullptr;
  ObArenaAllocator allocator;
  ASSERT_NE(nullptr, (buf = allocator.alloc(sizeof(ObSSTable))));
  sstable = new (buf) ObSSTable();
  sstable->is_tmp_sstable_ = true;
  ObTableHandleV2 sstable_handle;
  ObTableHandleV2 handle2;
  ASSERT_EQ(OB_SUCCESS, sstable_handle.set_sstable(sstable, &allocator));
  ASSERT_EQ(1, sstable->get_ref());
  handle2 = sstable_handle;
  ASSERT_EQ(2, sstable->get_ref());
  handle2.reset();
  ASSERT_EQ(1, sstable->get_ref());
  sstable_handle.reset();
  ASSERT_EQ(0, sstable->get_ref());
  ASSERT_EQ(false, sstable->is_tmp_sstable_);
}

TEST_F(TestTenantMetaMemMgr, test_memtable)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;

  bool for_inc_direct_load = false;
  ret = t3m_.acquire_data_memtable(handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != handle.table_);
  handle.get_table()->set_table_type(ObITable::TableType::DATA_MEMTABLE);


  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.table_->inc_ref();
  t3m_.release_memtable(static_cast<memtable::ObMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(2, handle.get_table()->get_ref());

  handle.table_->dec_ref();
  t3m_.release_memtable(static_cast<memtable::ObMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.reset();
  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.free_tables_queue_.size());

  bool all_table_cleaned = false; // no use
  ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));
}

TEST_F(TestTenantMetaMemMgr, test_tx_ctx_memtable)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;

  ret = t3m_.acquire_tx_ctx_memtable(handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != handle.table_);
  handle.get_table()->set_table_type(ObITable::TableType::TX_CTX_MEMTABLE);

  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.table_->inc_ref();
  t3m_.release_tx_ctx_memtable_(static_cast<ObTxCtxMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(2, handle.get_table()->get_ref());

  handle.table_->dec_ref();
  t3m_.release_tx_ctx_memtable_(static_cast<ObTxCtxMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.reset();
  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.free_tables_queue_.size());

  bool all_table_cleaned = false; // no use
  ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));
}

TEST_F(TestTenantMetaMemMgr, test_tx_data_memtable)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;

  ret = t3m_.acquire_tx_data_memtable(handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != handle.table_);
  handle.get_table()->set_table_type(ObITable::TableType::TX_DATA_MEMTABLE);

  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.table_->inc_ref();
  t3m_.release_tx_data_memtable_(static_cast<ObTxDataMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(2, handle.get_table()->get_ref());

  handle.table_->dec_ref();
  t3m_.release_tx_data_memtable_(static_cast<ObTxDataMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.reset();
  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.free_tables_queue_.size());

  bool all_table_cleaned = false; // no use
  ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));
}

TEST_F(TestTenantMetaMemMgr, test_lock_memtable)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;

  ret = t3m_.acquire_lock_memtable(handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != handle.table_);
  handle.get_table()->set_table_type(ObITable::TableType::LOCK_MEMTABLE);

  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.table_->inc_ref();
  t3m_.release_lock_memtable_(static_cast<transaction::tablelock::ObLockMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);
  ASSERT_EQ(2, handle.get_table()->get_ref());

  handle.table_->dec_ref();
  t3m_.release_lock_memtable_(static_cast<transaction::tablelock::ObLockMemtable *>(handle.table_));
  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, handle.get_table()->get_ref());

  handle.reset();
  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.free_tables_queue_.size());

  bool all_table_cleaned = false; // no use
  ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));
}

TEST_F(TestTenantMetaMemMgr, test_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(10000001);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObUpdateTabletPointerParam param;
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, handle);
  ASSERT_EQ(common::OB_ENTRY_NOT_EXIST, ret);
  ASSERT_TRUE(!handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr == tablet);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ASSERT_TRUE(handle.calc_wash_score(handle.wash_priority_) > 0);

  ObTabletHandle tmp_handle;
  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, tmp_handle);
  ASSERT_EQ(common::OB_ITEM_NOT_SETTED, ret);
  ASSERT_TRUE(!tmp_handle.is_valid());

  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(nullptr != handle.allocator_);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  ObMetaDiskAddr addr;
  addr.first_id_ = 1;
  addr.second_id_ = 2;
  addr.offset_ = 0;
  addr.size_ = 4096;
  addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
  handle.get_obj()->set_tablet_addr(addr);
  handle.get_obj()->is_inited_ = true; // to pass test
  handle.get_obj()->table_store_addr_.addr_.set_none_addr();

  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  ObTabletPointerHandle ptr_hdl(t3m_.tablet_map_);
  ret = t3m_.tablet_map_.map_.get_refactored(key, ptr_hdl.ptr_);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.tablet_map_.inc_handle_ref(ptr_hdl.ptr_);
  ASSERT_TRUE(addr == ptr_hdl.ptr_->ptr_->phy_addr_);
  ASSERT_TRUE(handle.obj_ == ptr_hdl.ptr_->ptr_->obj_.ptr_);
  ASSERT_TRUE(handle.allocator_ == ptr_hdl.ptr_->ptr_->obj_.allocator_);

  ObMetaDiskAddr old_addr;
  old_addr.set_none_addr();
  ret = t3m_.compare_and_swap_tablet(key, old_addr, addr);
  ASSERT_EQ(common::OB_NOT_THE_OBJECT, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  handle.reset();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());
  /* mock empty tablet to del successfully */
  tablet->table_store_addr_.addr_.set_none_addr();
  tablet->storage_schema_addr_.addr_.set_none_addr();
  tablet->rowkey_read_info_ = nullptr;

  tablet->tablet_meta_.ls_id_ = key.ls_id_;
  tablet->tablet_meta_.tablet_id_ = key.tablet_id_;
  handle.reset();

  void *free_obj = nullptr;
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_ITER_END, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ret = t3m_.del_tablet(key);
  // ASSERT_EQ(common::OB_SUCCESS, ret); assert when master is merged
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  ptr_hdl.reset();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.full_tablet_creator_.get_used_obj_cnt());
  gc_all_tablets();
}

TEST_F(TestTenantMetaMemMgr, test_wash_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1000000001);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ObSSTable sstable;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> total_write_ctxs;
  checkpoint::ObCheckpointExecutor ckpt_executor;
  checkpoint::ObDataCheckpoint data_checkpoint;
  ObLS ls;
  ObLSTxService ls_tx_service(&ls);
  ObLSWRSHandler ls_loop_worker;
  ObLSTabletService ls_tablet_svr;
  MockObLogHandler log_handler;
  ObFreezer freezer;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  prepare_data_schema(schema_allocator, create_tablet_schema);

  ret = freezer.init(&ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);


  share::SCN create_scn;
  create_scn.convert_from_ts(ObTimeUtility::fast_current_time());

  ObTabletID empty_tablet_id;
  ret = tablet->init_for_first_time_creation(allocator_, ls_id_, tablet_id, tablet_id,
      create_scn, create_scn.get_val_for_tx(), create_tablet_schema, true/*need_create_empty_major_sstable*/, &freezer);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet->get_ref());
  ObTabletPersister persister;
  ObSArray<MacroBlockId> shared_meta_id_arr;

  ObTabletHandle new_handle;
  ASSERT_EQ(common::OB_SUCCESS, t3m_.acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_and_fill_tablet(
      *tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage));

  ObMetaDiskAddr addr = new_handle.get_obj()->get_tablet_addr();
  ObUpdateTabletPointerParam param;
  ret = new_handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, new_handle, new_handle, param);
  tablet = new_handle.get_obj();
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  void *free_obj = nullptr;
  new_handle.reset();
  ASSERT_EQ(1, tablet->get_ref());
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(free_obj, ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char *>(tablet)));
  t3m_.tablet_buffer_pool_.free_obj(free_obj);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  ret = t3m_.compare_and_swap_tablet(key, addr, none_addr);
  ASSERT_EQ(common::OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  handle.reset();
  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  gc_all_tablets();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_wash_inner_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(300);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ObSSTable sstable;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> total_write_ctxs;
  checkpoint::ObCheckpointExecutor ckpt_executor;
  checkpoint::ObDataCheckpoint data_checkpoint;
  ObLS ls;
  ObLSTxService ls_tx_service(&ls);
  ObLSWRSHandler ls_loop_worker;
  ObLSTabletService ls_tablet_svr;
  MockObLogHandler log_handler;
  ObFreezer freezer;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  prepare_data_schema(schema_allocator, create_tablet_schema);

  ret = freezer.init(&ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);


  share::SCN create_scn;
  create_scn.convert_from_ts(ObTimeUtility::fast_current_time());

  ObTabletID empty_tablet_id;
  bool make_empty_co_sstable = true;
  ret = tablet->init_for_first_time_creation(allocator_, ls_id_, tablet_id, tablet_id,
      create_scn, create_scn.get_val_for_tx(), create_tablet_schema,
      make_empty_co_sstable/*need_create_empty_major_sstable*/, &freezer);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet->get_ref());

  ObTabletHandle new_handle;
  ObTabletPersister persister;
  ObSArray<MacroBlockId> shared_meta_id_arr;
  ASSERT_EQ(common::OB_SUCCESS, t3m_.acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_and_fill_tablet(
      *tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage));

  ObMetaDiskAddr addr = new_handle.get_obj()->get_tablet_addr();

  ObUpdateTabletPointerParam param;
  ret = new_handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, new_handle, new_handle, param);
  tablet = new_handle.get_obj();
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  // test tiny tablet
  ASSERT_EQ(new_handle.get_obj()->allocator_, nullptr);
  ObArenaAllocator tmp_allocator;
  ObTabletHandle tmp_handle;
  ret = t3m_.get_tablet_with_allocator(WashTabletPriority::WTP_LOW, key, tmp_allocator, tmp_handle, true/*force_alloc_new*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(tmp_handle.get_obj(), new_handle.get_obj()); // get tablet allocated by allocator
  ASSERT_TRUE(tmp_handle.get_obj()->get_tablet_addr().is_block());
  ASSERT_EQ(tmp_handle.allocator_, tmp_handle.get_obj()->allocator_);
  ASSERT_FALSE(tmp_handle.allow_copy_and_assign_);
  tmp_handle.reset();

  void *free_obj = nullptr;
  new_handle.reset();
  ASSERT_EQ(1, tablet->get_ref());
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(free_obj, ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char *>(tablet)));
  t3m_.tablet_buffer_pool_.free_obj(free_obj);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ObMetaDiskAddr mem_addr;
  ret = mem_addr.set_mem_addr(0, 0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, addr, mem_addr);
  ASSERT_EQ(common::OB_INVALID_ARGUMENT, ret);

  handle.reset();
  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  gc_all_tablets();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_wash_no_sstable_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(12345678);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  common::ObSEArray<ObSharedBlocksWriteCtx, 16> total_write_ctxs;
  checkpoint::ObCheckpointExecutor ckpt_executor;
  checkpoint::ObDataCheckpoint data_checkpoint;
  ObLS ls;
  ObLSTxService ls_tx_service(&ls);
  ObLSWRSHandler ls_loop_worker;
  ObLSTabletService ls_tablet_svr;
  MockObLogHandler log_handler;
  ObFreezer freezer;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  prepare_data_schema(schema_allocator, create_tablet_schema);

  ret = freezer.init(&ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::SCN create_scn;
  create_scn.convert_from_ts(ObTimeUtility::fast_current_time());

  ObTabletID empty_tablet_id;
  bool make_empty_co_sstable = false;
  ret = tablet->init_for_first_time_creation(allocator_, ls_id_, tablet_id, tablet_id,
      create_scn, create_scn.get_val_for_tx(), create_tablet_schema,
      make_empty_co_sstable/*need_create_empty_major_sstable*/, &freezer);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet->get_ref());

  ObTabletHandle new_handle;
  ObTabletPersister persister;
  ObSArray<MacroBlockId> shared_meta_id_arr;
  ASSERT_EQ(common::OB_SUCCESS, t3m_.acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_and_fill_tablet(
      *tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage));

  ObUpdateTabletPointerParam param;
  ret = new_handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, new_handle, new_handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  tablet = new_handle.get_obj();
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  void *free_obj = nullptr;
  new_handle.reset();
  ASSERT_EQ(1, tablet->get_ref());
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  t3m_.tablet_buffer_pool_.free_obj(free_obj);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  handle.reset();
  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  gc_all_tablets();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_get_tablet_with_allocator)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1000000001);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ObSSTable sstable;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> total_write_ctxs;
  checkpoint::ObCheckpointExecutor ckpt_executor;
  checkpoint::ObDataCheckpoint data_checkpoint;
  ObLS ls;
  ObLSTxService ls_tx_service(&ls);
  ObLSWRSHandler ls_loop_worker;
  ObLSTabletService ls_tablet_svr;
  MockObLogHandler log_handler;
  ObFreezer freezer;
  ObTabletSpaceUsage space_usage;
  ObTabletMacroInfo tablet_macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  prepare_data_schema(schema_allocator, create_tablet_schema);

  ret = freezer.init(&ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObTabletCreateSSTableParam param;
  ret = ObTabletCreateDeleteHelper::build_create_sstable_param(create_tablet_schema, tablet_id, 100, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ret = ObTabletCreateDeleteHelper::create_sstable(param, allocator_, sstable);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  share::SCN create_scn;
  create_scn.convert_from_ts(ObTimeUtility::fast_current_time());

  ObTabletID empty_tablet_id;
  bool make_empty_co_sstable = true;
  ret = tablet->init_for_first_time_creation(allocator_, ls_id_, tablet_id, tablet_id,
      create_scn, create_scn.get_val_for_tx(), create_tablet_schema,
      make_empty_co_sstable/*need_create_empty_major_sstable*/, &freezer);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet->get_ref());

  ObTabletHandle new_handle;
  ObTabletPersister persister;
  ObSArray<MacroBlockId> shared_meta_id_arr;
  ASSERT_EQ(common::OB_SUCCESS, t3m_.acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, new_handle));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_and_fill_tablet(
      *tablet, linked_writer, total_write_ctxs, new_handle, space_usage, tablet_macro_info, shared_meta_id_arr));
  ASSERT_EQ(common::OB_SUCCESS, persister.persist_aggregated_meta(tablet_macro_info, new_handle, space_usage));

  ObUpdateTabletPointerParam update_pointer_param;
  ret = new_handle.get_obj()->get_updating_tablet_pointer_param(update_pointer_param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, new_handle, new_handle, update_pointer_param);
  tablet = new_handle.get_obj();
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_buffer_pool_.inner_used_num_);

  void *free_obj = nullptr;
  new_handle.reset();
  ASSERT_EQ(1, tablet->get_ref());
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(free_obj, ObMetaObjBufferHelper::get_meta_obj_buffer_ptr(reinterpret_cast<char *>(tablet)));
  t3m_.tablet_buffer_pool_.free_obj(free_obj);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  common::ObArenaAllocator allocator;
  ASSERT_EQ(common::OB_SUCCESS, t3m_.get_tablet_with_allocator(WashTabletPriority::WTP_HIGH, key, allocator, handle));
  ASSERT_TRUE(handle.is_valid());

  handle.reset();
  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  gc_all_tablets();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_wash_mem_tablet)
{
  return;
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(1000000001);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ObSSTable sstable;
  checkpoint::ObCheckpointExecutor ckpt_executor;
  checkpoint::ObDataCheckpoint data_checkpoint;
  ObLS ls;
  ObLSTxService ls_tx_service(&ls);
  ObLSWRSHandler ls_loop_worker;
  ObLSTabletService ls_tablet_svr;
  MockObLogHandler log_handler;
  ObFreezer freezer;
  ObTableSchema table_schema;
  ObTabletCreateSSTableParam param;
  ObArenaAllocator schema_allocator;
  ObCreateTabletSchema create_tablet_schema;
  prepare_data_schema(schema_allocator, create_tablet_schema);

  ret = freezer.init(&ls);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param.table_key_.version_range_.snapshot_version_ = 1;
  param.schema_version_ = table_schema.get_schema_version();
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
  param.progressive_merge_step_ = 0;
  param.table_mode_ = table_schema.get_table_mode_struct();
  param.index_type_ = table_schema.get_index_type();
  param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.column_cnt_ = table_schema.get_column_count() + multi_version_col_cnt;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  ASSERT_EQ(OB_SUCCESS, sstable.init(param, &allocator_));

  share::SCN create_scn;
  create_scn.convert_from_ts(ObTimeUtility::fast_current_time());

  ObTabletID empty_tablet_id;
  bool make_empty_co_sstable = false;
  ret = tablet->init_for_first_time_creation(allocator_, ls_id_, tablet_id, tablet_id,
      create_scn, create_scn.get_val_for_tx(), create_tablet_schema,
      make_empty_co_sstable/*need_create_empty_major_sstable*/, &freezer);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, tablet->get_ref());

  ObMetaDiskAddr addr;
  addr.offset_ = 0;
  addr.size_ = 4096;
  addr.type_ = ObMetaDiskAddr::DiskType::MEM;
  handle.get_obj()->set_tablet_addr(addr);

  ObUpdateTabletPointerParam update_pointer_param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(update_pointer_param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, handle, handle, update_pointer_param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  void *free_obj = nullptr;
  handle.reset();
  ASSERT_EQ(1, tablet->get_ref());
  ret = t3m_.try_wash_tablet(typeid(ObTenantMetaMemMgr::ObNormalTabletBuffer), free_obj);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(nullptr, free_obj); // memory address tablet can't be washed.
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ObMetaDiskAddr none_addr;
  none_addr.set_none_addr();
  ret = t3m_.compare_and_swap_tablet(key, addr, none_addr);
  ASSERT_EQ(common::OB_INVALID_ARGUMENT, ret);

  handle.reset();
  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  gc_all_tablets();
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_replace_tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(101);
  const ObTabletMapKey key(ls_id_, tablet_id);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle handle;

  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);

  handle.reset();
  ASSERT_TRUE(!handle.is_valid());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());

  ret = t3m_.del_tablet(key);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());

  ret = t3m_.create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle);
  handle.t3m_ = &t3m_; // temporary code, use local t3m
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  ObTabletHandle tmp_handle;
  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, tmp_handle);
  ASSERT_EQ(common::OB_ITEM_NOT_SETTED, ret);
  ASSERT_TRUE(!tmp_handle.is_valid());

  handle.get_obj()->is_inited_ = true; // to pass test
  handle.get_obj()->table_store_addr_.addr_.set_none_addr();

  ObUpdateTabletPointerParam param;
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  // test full tablet
  ASSERT_EQ(handle.get_obj()->allocator_, handle.allocator_);
  ASSERT_NE(handle.allocator_, nullptr);
  ObArenaAllocator tmp_allocator;
  tmp_handle.reset();
  ret = t3m_.get_tablet_with_allocator(WashTabletPriority::WTP_LOW, key, tmp_allocator, tmp_handle, false/*force_alloc_new*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_handle.get_obj(), handle.get_obj()); // get full tablet
  ASSERT_TRUE(tmp_handle.get_obj()->get_tablet_addr().is_memory());
  ASSERT_EQ(handle.allocator_, tmp_handle.allocator_);
  ASSERT_TRUE(tmp_handle.allow_copy_and_assign_);
  tmp_handle.reset();

  ObMetaDiskAddr addr;
  addr.first_id_ = 1;
  addr.second_id_ = 2;
  addr.offset_ = 0;
  addr.size_ = 4096;
  addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;
  handle.get_obj()->set_tablet_addr(addr);
  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ObTabletPointerHandle ptr_hdl(t3m_.tablet_map_);
  ret = t3m_.tablet_map_.map_.get_refactored(key, ptr_hdl.ptr_);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.tablet_map_.inc_handle_ref(ptr_hdl.ptr_);
  ASSERT_TRUE(addr == ptr_hdl.ptr_->ptr_->phy_addr_);
  ASSERT_TRUE(handle.obj_ == ptr_hdl.ptr_->ptr_->obj_.ptr_);
  ASSERT_TRUE(handle.allocator_ == ptr_hdl.ptr_->ptr_->obj_.allocator_);

  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_TRUE(tablet->pointer_hdl_.is_valid());

  handle.reset();
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(0, t3m_.tablet_buffer_pool_.inner_used_num_);

  ObTabletHandle old_handle;
  ret = t3m_.get_tablet(WashTabletPriority::WTP_HIGH, key, old_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(old_handle.is_valid());

  ret = t3m_.acquire_tablet_from_pool(ObTabletPoolType::TP_NORMAL, WashTabletPriority::WTP_HIGH, key, handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_buffer_pool_.inner_used_num_);
  ASSERT_TRUE(handle.is_valid());

  tablet = handle.get_obj();
  ASSERT_TRUE(nullptr != tablet);

  addr.first_id_ = 1;
  addr.second_id_ = 4;
  addr.offset_ = 0;
  addr.size_ = 4096;
  addr.type_ = ObMetaDiskAddr::DiskType::BLOCK;

  tablet->set_tablet_addr(addr);
  tablet->is_inited_ = true; // to pass test
  tablet->table_store_addr_.addr_.set_none_addr();

  ret = handle.get_obj()->get_updating_tablet_pointer_param(param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ret = t3m_.compare_and_swap_tablet(key, old_handle, handle, param);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(1, t3m_.tablet_map_.map_.size());
  ASSERT_EQ(1, t3m_.tablet_buffer_pool_.inner_used_num_);

  old_handle.reset();
  ASSERT_EQ(1, t3m_.tablet_buffer_pool_.inner_used_num_);

  handle.reset();
  ASSERT_EQ(1, t3m_.tablet_buffer_pool_.inner_used_num_);

  ret = t3m_.tablet_map_.erase(key, tmp_handle);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_EQ(0, t3m_.tablet_map_.map_.size());
  tmp_handle.reset();

  gc_all_tablets();
}

TEST_F(TestTenantMetaMemMgr, test_multi_tablet)
{
  const int64_t thread_cnt = 16;
  ObTenantBase *tenant_base = MTL_CTX();
  TestConcurrentT3M multi_thread(thread_cnt, t3m_, tenant_base, allocator_);

  int ret = multi_thread.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  multi_thread.wait();

  gc_all_tablets();
}

TEST_F(TestTenantMetaMemMgr, test_tablet_wash_priority)
{
  ObTabletHandle handle;

  ASSERT_TRUE(handle.calc_wash_score(handle.wash_priority_) == INT64_MIN);

  handle.set_wash_priority(WashTabletPriority::WTP_HIGH);
  ASSERT_TRUE(handle.calc_wash_score(handle.wash_priority_) > 0);

  handle.set_wash_priority(WashTabletPriority::WTP_LOW);
  ASSERT_TRUE(handle.calc_wash_score(handle.wash_priority_) < 0);

  ObTabletHandle high_priority_handle;
  high_priority_handle.set_wash_priority(WashTabletPriority::WTP_HIGH);

  ASSERT_TRUE(high_priority_handle.calc_wash_score(high_priority_handle.wash_priority_) > handle.calc_wash_score(handle.wash_priority_));

  ObTabletHandle handle1(handle);
  ASSERT_TRUE(handle1.calc_wash_score(handle1.wash_priority_) < 0);

  ObTabletHandle handle2;
  ASSERT_TRUE(handle2.calc_wash_score(handle2.wash_priority_) == INT64_MIN);
  handle2 = high_priority_handle;
  ASSERT_TRUE(handle2.calc_wash_score(handle2.wash_priority_) > 0);
}

TEST_F(TestTenantMetaMemMgr, test_table_gc)
{
  // check the pool_arr of t3m
  ObITable::TableType type;
  for (type = ObITable::TableType::DATA_MEMTABLE; type <= ObITable::TableType::LOCK_MEMTABLE; type = (ObITable::TableType)(type + 1)) {
    ASSERT_NE(nullptr, t3m_.pool_arr_[static_cast<int>(type)]);
  }
  for (type = ObITable::TableType::MAJOR_SSTABLE; type < ObITable::TableType::MAX_TABLE_TYPE; type = (ObITable::TableType)(type + 1)) {
    if (type != ObITable::TableType::DDL_MEM_SSTABLE) {
      ASSERT_EQ(nullptr, t3m_.pool_arr_[static_cast<int>(type)]);
    }
  }
  // check the count of tables in every pool before acquirement
  ASSERT_EQ(0, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.lock_memtable_pool_.inner_used_num_);

  // acquire tables and set table_type
  ObTableHandleV2 memtable_handle;
  ObTableHandleV2 tx_data_memtable_handle;
  ObTableHandleV2 tx_ctx_memtable_handle;
  ObTableHandleV2 lock_memtable_handle;
  ASSERT_EQ(OB_SUCCESS, t3m_.acquire_data_memtable(memtable_handle));
  ASSERT_EQ(OB_SUCCESS, t3m_.acquire_tx_data_memtable(tx_data_memtable_handle));
  ASSERT_EQ(OB_SUCCESS, t3m_.acquire_tx_ctx_memtable(tx_ctx_memtable_handle));
  ASSERT_EQ(OB_SUCCESS, t3m_.acquire_lock_memtable(lock_memtable_handle));
  memtable_handle.table_->set_table_type(ObITable::TableType::DATA_MEMTABLE);
  tx_data_memtable_handle.table_->set_table_type(ObITable::TableType::TX_DATA_MEMTABLE);
  tx_ctx_memtable_handle.table_->set_table_type(ObITable::TableType::TX_CTX_MEMTABLE);
  lock_memtable_handle.table_->set_table_type(ObITable::TableType::LOCK_MEMTABLE);

  // check the count of tables in every pool before reset
  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);

  // reset all handles
  memtable_handle.reset();
  tx_data_memtable_handle.reset();
  tx_ctx_memtable_handle.reset();
  lock_memtable_handle.reset();

  // check the count of tables in every pool after reset
  ASSERT_EQ(1, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(1, t3m_.lock_memtable_pool_.inner_used_num_);

  // check the count of tables in free queue
  ASSERT_EQ(4, t3m_.free_tables_queue_.size());

  // recycle all tables
  bool all_table_cleaned = false; // no use
  ASSERT_EQ(OB_SUCCESS, t3m_.gc_tables_in_queue(all_table_cleaned));

  // check the count after gc
  ASSERT_EQ(0, t3m_.memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.tx_data_memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.tx_ctx_memtable_pool_.inner_used_num_);
  ASSERT_EQ(0, t3m_.lock_memtable_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaMemMgr, test_heap)
{
  int ret = OB_SUCCESS;
  int cmp_ret = OB_SUCCESS;
  ObTenantMetaMemMgr::HeapCompare heap_compare(cmp_ret);
  ObArenaAllocator allocator;
  ObTenantMetaMemMgr::Heap heap(heap_compare, &allocator);

  ObTenantMetaMemMgr::CandidateTabletInfo info_1;
  info_1.ls_id_ = 1;
  info_1.tablet_id_ = 1;
  info_1.wash_score_ = 200;

  ObTenantMetaMemMgr::CandidateTabletInfo info_2;
  info_1.ls_id_ = 2;
  info_1.tablet_id_ = 2;
  info_1.wash_score_ = 100;

  ObTenantMetaMemMgr::CandidateTabletInfo tmp;

  ret = heap.push(info_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, heap.count());

  tmp = heap.top();
  ASSERT_EQ(tmp.ls_id_, info_1.ls_id_);
  ASSERT_EQ(tmp.tablet_id_, info_1.tablet_id_);
  ASSERT_EQ(tmp.wash_score_, info_1.wash_score_);

  ret = heap.push(info_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, heap.count());

  tmp = heap.top();
  ASSERT_EQ(tmp.ls_id_, info_2.ls_id_);
  ASSERT_EQ(tmp.tablet_id_, info_2.tablet_id_);
  ASSERT_EQ(tmp.wash_score_, info_2.wash_score_);

  ret = heap.pop();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, heap.count());

  tmp = heap.top();
  ASSERT_EQ(tmp.ls_id_, info_1.ls_id_);
  ASSERT_EQ(tmp.tablet_id_, info_1.tablet_id_);
  ASSERT_EQ(tmp.wash_score_, info_1.wash_score_);

  ret = heap.pop();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, heap.count());
}

TEST_F(TestTenantMetaMemMgr, test_leak_checker)
{
  auto t3m = MTL(ObTenantMetaMemMgr*);
  auto before_enabled = t3m->is_tablet_leak_checker_enabled_;
  t3m->last_access_tenant_config_ts_ = common::ObClockGenerator::getClock();
  t3m->is_tablet_leak_checker_enabled_ = true;

  struct GetRefMapSizeFunc
  {
    int operator()()
    {
      int res = 0;
      for (int i = 0; i < ObTabletHandleIndexMap::REF_ARRAY_SIZE; ++i) {
        int val = 0;
        for (int j = 0; j < ObTabletHandleIndexMap::REF_BUCKET_SIZE; ++j) {
          val += ATOMIC_LOAD(&(
              MTL(ObTenantMetaMemMgr *)->leak_checker_
                  .tb_ref_bucket_[i + j * ObTabletHandleIndexMap::REF_ARRAY_SIZE]));
        }
        if (val != 0) {
          res++;
        }
      }
      return res;
    }
  };
  GetRefMapSizeFunc functor;
  int32_t initial_size = functor();

  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));

  // Construct tablet handle, did not increase ref cnt
  ObTabletHandle tb_handle_1;
  int32_t index_1 = tb_handle_1.index_;
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 0);
  ASSERT_EQ(functor(), initial_size);
  // Set tablet object, increase ref cnt
  const ObTabletID tablet_id_1(3000);
  MTL(ObTenantMetaMemMgr *)->create_msd_tablet(WashTabletPriority::WTP_HIGH,
                                               ObTabletMapKey(ls_id_, tablet_id_1),
                                               ls_handle,
                                               tb_handle_1);
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 1);
  ASSERT_EQ(functor(), initial_size + 1);
  LOG_INFO("test_leak_checker, should be log pinned tablet info here (maybe n lines)");
  MTL(ObTenantMetaMemMgr *)->leak_checker_.dump_pinned_tablet_info();
  // Reset tablet handle, decrease ref cnt
  tb_handle_1.reset();
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 0);
  ASSERT_EQ(functor(), initial_size);
  LOG_INFO("test_leak_checker, should be log pinned tablet info here (maybe n-1 lines)");
  MTL(ObTenantMetaMemMgr *)->leak_checker_.dump_pinned_tablet_info();

  // Set tablet at one t3m, reset at another t3m
  ObTabletHandle tb_handle_2;
  int32_t index_2 = tb_handle_2.index_;
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_2], 0);
  ASSERT_EQ(functor(), initial_size);
  const ObTabletID tablet_id_2(3001);
  MTL(ObTenantMetaMemMgr *)->create_msd_tablet(WashTabletPriority::WTP_HIGH,
                                                ObTabletMapKey(ls_id_, tablet_id_2),
                                                ls_handle,
                                                tb_handle_2);
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_2], 1);
  ASSERT_EQ(functor(), initial_size + 1);
  LOG_INFO("test_leak_checker, should be log pinned tablet info here (maybe n lines)");
  MTL(ObTenantMetaMemMgr *)->leak_checker_.dump_pinned_tablet_info();
  // Change default t3m
  ObTenantMetaMemMgr *true_t3m = MTL(ObTenantMetaMemMgr*);
  ObTenantMetaMemMgr another_t3m(TEST_ANOTHER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, another_t3m.init());
  share::ObTenantEnv::get_tenant_local()->set(&another_t3m);
  ASSERT_EQ(MTL(ObTenantMetaMemMgr*), &another_t3m);
  ASSERT_EQ(functor(), 0);
  tb_handle_2.reset();
  LOG_INFO("test_leak_checker, should be log pinned tablet info here (maybe n-1 lines)");
  MTL(ObTenantMetaMemMgr *)->leak_checker_.dump_pinned_tablet_info();

  // Change default t3m back
  share::ObTenantEnv::get_tenant_local()->set(true_t3m);
  // Delete useless tablet
  MTL(ObTenantMetaMemMgr *)->del_tablet(ObTabletMapKey(ls_id_, tablet_id_1));
  MTL(ObTenantMetaMemMgr *)->del_tablet(ObTabletMapKey(ls_id_, tablet_id_2));

  t3m->is_tablet_leak_checker_enabled_ = before_enabled;
}

TEST_F(TestTenantMetaMemMgr, test_leak_checker_copy)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));

  auto t3m = MTL(ObTenantMetaMemMgr*);
  auto before_enabled = t3m->is_tablet_leak_checker_enabled_;
  t3m->last_access_tenant_config_ts_ = common::ObClockGenerator::getClock();
  t3m->is_tablet_leak_checker_enabled_ = true;

  {
    ObTabletHandle tb_handle_1;
    int32_t index_1 = tb_handle_1.index_;
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 0);
    const ObTabletID tablet_id_1(3000);
    ASSERT_EQ(OB_SUCCESS,
              MTL(ObTenantMetaMemMgr *)->create_msd_tablet(WashTabletPriority::WTP_HIGH,
                                      ObTabletMapKey(ls_id_, tablet_id_1),
                                      ls_handle, tb_handle_1));
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 1);
    ObTabletHandle tb_handle_2;
    tb_handle_2 = tb_handle_1;
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[tb_handle_2.index_], 1);
    MTL(ObTenantMetaMemMgr *)->del_tablet(ObTabletMapKey(ls_id_, tablet_id_1));
  }

  t3m->leak_checker_.dump_pinned_tablet_info();
  t3m->is_tablet_leak_checker_enabled_ = before_enabled;
}

TEST_F(TestTenantMetaMemMgr, test_leak_checker_switch)
{
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));

  auto t3m = MTL(ObTenantMetaMemMgr*);
  auto before_enabled = t3m->is_tablet_leak_checker_enabled_;

  {
    // new handle_1 with leak_checker enabled
    t3m->last_access_tenant_config_ts_ = common::ObClockGenerator::getClock();
    t3m->is_tablet_leak_checker_enabled_ = true;
    ObTabletHandle tb_handle_1;
    int32_t index_1 = tb_handle_1.index_;
    const ObTabletID tablet_id_1(3000);
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 0);
    ASSERT_EQ(OB_SUCCESS,
              MTL(ObTenantMetaMemMgr *)->create_msd_tablet(WashTabletPriority::WTP_HIGH,
                                      ObTabletMapKey(ls_id_, tablet_id_1),
                                      ls_handle, tb_handle_1));
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 1);

    // new handle_2 with leak_checker disabled
    t3m->last_access_tenant_config_ts_ = common::ObClockGenerator::getClock();
    t3m->is_tablet_leak_checker_enabled_ = false;
    ObTabletHandle tb_handle_2;
    int32_t index_2 = tb_handle_2.index_;
    const ObTabletID tablet_id_2(5000);
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_2], 0);
    ASSERT_EQ(OB_SUCCESS,
              MTL(ObTenantMetaMemMgr *)->create_msd_tablet(WashTabletPriority::WTP_HIGH,
                                      ObTabletMapKey(ls_id_, tablet_id_2),
                                      ls_handle, tb_handle_2));
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_2], 0);

    // destroy handle_1 with leak_checker disabled, dec ref failed (switch-off)
    tb_handle_1.reset();
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_1], 1);

    // destroy handle_2 with leak_checker enabled, dec ref failed (invalid index)
    t3m->last_access_tenant_config_ts_ = common::ObClockGenerator::getClock();
    t3m->is_tablet_leak_checker_enabled_ = true;
    tb_handle_2.reset();
    ASSERT_EQ(MTL(ObTenantMetaMemMgr*)->leak_checker_.tb_ref_bucket_[index_2], 0);

    MTL(ObTenantMetaMemMgr *)->del_tablet(ObTabletMapKey(ls_id_, tablet_id_1));
    MTL(ObTenantMetaMemMgr *)->del_tablet(ObTabletMapKey(ls_id_, tablet_id_2));
  }

  t3m->is_tablet_leak_checker_enabled_ = before_enabled;
}

TEST_F(TestTenantMetaMemMgr, test_tablet_gc_queue)
{
  ObTenantMetaMemMgr::TabletGCQueue gc_queue;
  ASSERT_EQ(0, gc_queue.count());
  ASSERT_EQ(nullptr, gc_queue.gc_head_);
  ASSERT_EQ(nullptr, gc_queue.gc_tail_);

  ObTablet *tablet = new ObTablet();
  ASSERT_EQ(OB_SUCCESS, gc_queue.push(tablet));
  ASSERT_EQ(1, gc_queue.count());

  ASSERT_EQ(tablet, gc_queue.pop());
  ASSERT_TRUE(gc_queue.is_empty());
  delete tablet;

  ASSERT_EQ(OB_INVALID_ARGUMENT, gc_queue.push(nullptr));
  ASSERT_TRUE(gc_queue.is_empty());

  const int64_t tablet_cnt = 1000;
  for (int i = 0; i < tablet_cnt; ++i) {
    tablet = new ObTablet();
    ASSERT_EQ(OB_SUCCESS, gc_queue.push(tablet));
    ASSERT_EQ(i + 1, gc_queue.count());
    ASSERT_EQ(OB_INVALID_ARGUMENT, gc_queue.push(nullptr));
    ASSERT_EQ(i + 1, gc_queue.count());
  }
  ASSERT_EQ(tablet_cnt, gc_queue.count());
  for (int j = 0; j < tablet_cnt; ++j) {
    ASSERT_NE(nullptr, tablet = gc_queue.pop());
    ASSERT_EQ(tablet_cnt - 1 - j, gc_queue.count());
    delete tablet;
  }
  ASSERT_TRUE(gc_queue.is_empty());
}

TEST_F(TestTenantMetaMemMgr, test_show_limit)
{
  auto *calculator = MTL(ObTenantMetaMemMgr*)->get_t3m_limit_calculator();
  ASSERT_NE(nullptr, calculator);

  const int64_t DEFAULT_TABLET_CNT_PER_GB = ObTenantMetaMemMgr::DEFAULT_TABLET_CNT_PER_GB;

  const int64_t before_tenant_mem = lib::get_tenant_memory_limit(MTL_ID());
  const int64_t this_case_tenant_mem = 1 * 1024 * 1024 * 1024;  /* 1GB */
  lib::set_tenant_memory_limit(MTL_ID(), this_case_tenant_mem);

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t config_tablet_per_gb = tenant_config.is_valid() ?
                                          tenant_config->_max_tablet_cnt_per_gb :
                                          DEFAULT_TABLET_CNT_PER_GB;

  share::ObResoureConstraintValue result;
  ASSERT_EQ(OB_SUCCESS, calculator->get_resource_constraint_value(result));
  int64_t config_res = 0;
  result.get_type_value(CONFIGURATION_CONSTRAINT, config_res);
  int64_t memory_res = 0;
  result.get_type_value(MEMORY_CONSTRAINT, memory_res);
  ASSERT_EQ(20000, config_res);
  ASSERT_EQ(20480, memory_res);

  lib::set_tenant_memory_limit(MTL_ID(), before_tenant_mem);
}

TEST_F(TestTenantMetaMemMgr, test_normal_tablet_buffer_fragment)
{
  static const int64_t tablet_cnt = 155000;
  ObTabletHandle *tablets = new ObTabletHandle[tablet_cnt];
  const int64_t before_tenant_mem = lib::get_tenant_memory_limit(MTL_ID());
  const int64_t this_case_tenant_mem = 3 * 1024 * 1024 * 1024L;  /* 3GB */
  lib::set_tenant_memory_limit(MTL_ID(), this_case_tenant_mem);
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    ObTabletHandle tablet_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr *)->acquire_tablet(ObTabletPoolType::TP_NORMAL, tablets[i]));
  }
  ObMallocAllocator::get_instance()->print_tenant_memory_usage(MTL_ID());
  ObMemoryDump::get_instance().init();
  auto task = ObMemoryDump::get_instance().alloc_task();
  task->type_ = STAT_LABEL;
  ObMemoryDump::get_instance().push(task);
  usleep(1000000);
  ObTenantCtxAllocatorGuard ta = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(MTL_ID(), ObCtxIds::META_OBJ_CTX_ID);
  double fragment_rate = 1.0 * (ta->get_hold() - ta->get_used()) / ta->get_hold();
  std::cout << "hold: " << ta->get_hold() << " used: " << ta->get_used() << " limit: " << ta->get_limit() << " fragment_rate: " << fragment_rate << std::endl;
  ASSERT_TRUE(fragment_rate < 0.04);
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    tablets[i].reset();
  }
  delete [] tablets;
  lib::set_tenant_memory_limit(MTL_ID(), before_tenant_mem);
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_meta_mem_mgr.log*");
  OB_LOGGER.set_file_name("test_tenant_meta_mem_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
