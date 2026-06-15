// owner: yeqiyi.yqy
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

#include "storage/test_dml_common.h"

#define protected public
#define private public

#include "lib/alloc/memory_dump.h"
#include "storage/schema_utils.h"
#include "storage/mock_ob_log_handler.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/test_tablet_helper.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"


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

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, expr)
#define ASSERT_FAIL(expr) ASSERT_NE(OB_SUCCESS, expr)

const auto get_tablet_from_ls = std::bind(
  &ObLS::get_tablet,
  std::placeholders::_1,
  std::placeholders::_2,
  std::placeholders::_3,
  ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US * 10,
  ObMDSGetTabletMode::READ_WITHOUT_CHECK);

class TestTabletPersister : public ::testing::Test
{
public:
  TestTabletPersister();
  virtual ~TestTabletPersister() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void prepare_data_schema(common::ObArenaAllocator &allocator, ObCreateTabletSchema &create_tablet_schema);
  void prepare_create_sstable_param();
  void gc_all_tablets();
  int create_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      bool persist_tablet,
      const ObTabletStatus::Status tablet_status = ObTabletStatus::NORMAL);
  int create_empty_shell(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id);
  int get_ls(const int64_t ls_id, ObLSHandle &ls_handle)
  {
    return MTL(ObLSService*)->get_ls(ObLSID(ls_id), ls_handle, ObLSGetMod::STORAGE_MOD);
  }


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

int TestTabletPersister::create_tablet(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    bool persist_tablet,
    const ObTabletStatus::Status tablet_status)
{
  int ret = OB_SUCCESS;
  ObLS *ls = ls_handle.get_ls();
  if (nullptr == ls) {
      return OB_INVALID_ARGUMENT;
  }
  tablet_handle.reset();

  ObArenaAllocator allocator;
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator, tablet_status);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  ret = get_tablet_from_ls(ls_handle.get_ls(), tablet_id, tablet_handle);
  if (OB_SUCCESS != ret) {
    return ret;
  }
  ObTablet *tablet = tablet_handle.get_obj();
  if (nullptr == tablet) {
    return OB_INVALID_ARGUMENT;
  }
  if (persist_tablet) {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      return ret;
    }
    const ObTabletPersisterParam persist_param(data_version, ls->get_ls_id(), ls->get_ls_epoch(), tablet_id,
        tablet->get_transfer_seq(), 0);
    ObTabletHandle tmp_handle;
    ret = ObTabletPersister::persist_and_transform_tablet(persist_param, *tablet, tmp_handle);
    if (OB_SUCCESS != ret) {
        return ret;
    }
    ObUpdateTabletPointerParam param;
    ret = tmp_handle.get_obj()->get_updating_tablet_pointer_param(param);
    if (OB_SUCCESS != ret) {
        return ret;
    }
    ret = MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(ObTabletMapKey(ls->get_ls_id(), tablet_id), tablet_handle, tmp_handle, param);
  }
  if (OB_SUCCESS == ret) {
    ObTabletHandle tmp_handle;
    ret = get_tablet_from_ls(ls, tablet_id, tmp_handle);
    if (OB_SUCCESS != ret) {
        return ret;
    }
  }
  return ret;
}

int TestTabletPersister::create_empty_shell(
    ObLSHandle &ls_handle,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObLS *ls = ls_handle.get_ls();
  if (nullptr == ls) {
    return OB_INVALID_ARGUMENT;
  }
  {
    ObTabletHandle tablet_handle;
    ret = create_tablet(ls_handle, tablet_id, tablet_handle, false, ObTabletStatus::DELETED);
    if (OB_SUCCESS != ret) {
        return ret;
    }
  }
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    return ret;
  }
  ret = ls->get_tablet_svr()->update_tablet_to_empty_shell(data_version, tablet_id);
  if (OB_SUCCESS == ret) {
    ObTabletHandle tablet_handle;
    ret = get_tablet_from_ls(ls, tablet_id, tablet_handle);
    if (OB_SUCCESS != ret) {
        return ret;
    }
  }
  return ret;
}

// record the old_t3m just for ls remove
ObTenantMetaMemMgr *old_t3m = nullptr;

TestTabletPersister::TestTabletPersister()
  : tenant_id_(TEST_TENANT_ID),
    ls_id_(TEST_LS_ID),
    t3m_(TEST_TENANT_ID),
    param_(),
    table_schema_(),
    allocator_()
{
}

void TestTabletPersister::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ObClockGenerator::init();

  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  old_t3m = MTL(ObTenantMetaMemMgr *);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletPersister::SetUp()
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

void TestTabletPersister::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);
  MockTenantModuleEnv::get_instance().destroy();
}

void TestTabletPersister::TearDown()
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

void TestTabletPersister::prepare_data_schema(
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
  table_schema.set_micro_index_clustered(false);
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
        false/*skip_column_info*/, DATA_VERSION_4_3_0_0);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestTabletPersister::prepare_create_sstable_param()
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
  param_.rec_scn_.set_min();
  param_.original_size_ = 0;
  param_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param_.encrypt_id_ = 0;
  param_.master_key_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param_.column_cnt_, param_.column_checksums_));
}

void TestTabletPersister::gc_all_tablets()
{
  bool all_tablet_cleaned = false;
  while (!all_tablet_cleaned) {
    t3m_.gc_tablets_in_queue(all_tablet_cleaned); // do not use MTL t3m
  }
}

TEST_F(TestTabletPersister, test_uncopyable_item_array)
{
  static int64_t ITEM_CNT = 0;
  struct UncopyableItem final
  {
  public:
    UncopyableItem(int &a, const std::string &b)
      : a_(a), b_(b)
    {
      ++ITEM_CNT;
    }
    ~UncopyableItem()
    {
      --ITEM_CNT;
    }

    std::string to_string() const
    {
      return "{a:" + std::to_string(a_) + ", b:\'" + b_ + "\'}";
    }

  public:
    static void print_arr(const ObUncopyableItemArray<UncopyableItem> &arr)
    {
      printf("[");
      for (int64_t i = 0; i < arr.count(); ++i) {
        if (i > 0) {
          printf(", ");
        }
        printf("%s", arr.at(i).to_string().c_str());
      }
      printf("]\n");
    }

  public:
    int &a_;
    std::string b_;
  };

  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "Mittest"));
  {
    ObUncopyableItemArray<UncopyableItem> arr(allocator, 0);
    int a = 0;
    ASSERT_FAIL(arr.emplace_back(a, "a"));
  }
  {
    ObUncopyableItemArray<UncopyableItem> arr(allocator, 5);

    ASSERT_EQ(0, arr.count());
    ASSERT_EQ(5, arr.capacity());

    std::vector<int> nums{100, 200, 3000};

    ASSERT_SUCC(arr.emplace_back(nums[0], "nums[0]"));
    ASSERT_SUCC(arr.emplace_back(nums[1], "nums[1]"));
    ASSERT_SUCC(arr.emplace_back(nums[2], "nums[2]"));
    int tmp = 0;

    ASSERT_EQ((size_t)arr.count(), nums.size());
    ASSERT_EQ(ITEM_CNT, arr.count());
    UncopyableItem::print_arr(arr);
    for (int64_t i = 0; i < arr.count(); ++i) {
      ASSERT_EQ(&arr.at(i).a_, &nums[i]);
      std::string str = "nums[" + std::to_string(i) + "]";
      ASSERT_EQ(arr.at(i).b_, str);
      arr.at(i).a_ = i;
      arr.at(i).b_ = "arr[" + std::to_string(i) + "]";
    }
    UncopyableItem::print_arr(arr);
  }
  ASSERT_EQ(ITEM_CNT, 0);
}

void check_macro_info(ObArenaAllocator &allocator, const ObTablet &tablet)
{
  ObTabletMacroInfo *macro_info = nullptr;
  ASSERT_SUCC(tablet.load_macro_info(0, allocator, macro_info));

  ObMacroInfoIterator iter;
  ASSERT_SUCC(iter.init(ObTabletMacroType::MAX, *macro_info));


  int ret = OB_SUCCESS;

  auto check_macro_ref = [](const MacroBlockId &macro_id)->bool
  {
    int ret = OB_SUCCESS;
    bool b_ret = false;
    ObBlockManager::BlockInfo block_info;
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get from block map", K(ret), K(macro_id));
      } else {
        LOG_WARN("block not exists", K(ret), K(macro_id));
      }
    } else if (block_info.ref_cnt_ < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block ref cnt is unexpected", K(ret), K(macro_id), K(block_info));
    } else {
      // ok
      b_ret = true;
      LOG_INFO("get block info succeed", K(ret), K(macro_id), K(block_info));
    }
    return b_ret;
  };

  const ObMetaDiskAddr &addr = tablet.get_tablet_addr();
  MacroBlockId macro_id;
  ASSERT_SUCC(addr.get_macro_block_id(macro_id));
  ASSERT_TRUE(check_macro_ref(macro_id));

  ObTabletBlockInfo block_info;
  while (OB_SUCC(ret)) {
    block_info.reset();
    if (OB_FAIL(iter.get_next(block_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (!check_macro_ref(block_info.macro_id())) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  ASSERT_SUCC(ret);
  macro_info->~ObTabletMacroInfo();
}

void check_tablet_persist(const ObTablet &tablet)
{
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "Mittest"));
  check_macro_info(allocator, tablet);

  // aggregated meta
  ObTablet tmp_tablet;
  {
    char *buf = nullptr;
    int64_t buf_len = 0;
    int64_t pos = 0;
    const ObMetaDiskAddr &addr = tablet.get_tablet_addr();
    ASSERT_TRUE(addr.is_disked()) << ObCStringHelper().convert(addr);
    ASSERT_NE(nullptr, MTL(ObTenantStorageMetaService *));
    ASSERT_SUCC(MTL(ObTenantStorageMetaService *)->read_from_disk(addr, 0, allocator, buf, buf_len));
    tmp_tablet.set_tablet_addr(addr);
    ASSERT_SUCC(tmp_tablet.deserialize_for_replay(allocator, buf, buf_len, pos));
  }
  ASSERT_TRUE(tmp_tablet.is_valid());

  if (!tmp_tablet.is_empty_shell()) {
    ObStorageSchema *storage_schema_ptr = nullptr;
    ASSERT_SUCC(tmp_tablet.load_storage_schema(allocator, storage_schema_ptr));
    ASSERT_NE(nullptr, storage_schema_ptr);
    ASSERT_TRUE(storage_schema_ptr->is_valid());

    ObTabletMemberWrapper<ObTabletTableStore> wrapper;
    ASSERT_SUCC(tmp_tablet.fetch_table_store(wrapper));
    ASSERT_NE(nullptr, wrapper.get_member());
    ASSERT_TRUE(wrapper.get_member()->is_valid());
  }
}

#define GEN_PERSISTER_PARAM(param_name, data_ver, tablet_ptr)\
  int32_t __tmp_priv_trans_epoch = 0;\
  ASSERT_SUCC(tablet_ptr->get_private_transfer_epoch(__tmp_priv_trans_epoch));\
  ObTabletPersisterParam param_name(data_ver, ls_id_, 0, tablet_ptr->get_tablet_id(), __tmp_priv_trans_epoch, 0);\

static int64_t CUR_TABLET_ID = 2000010;

TEST_F(TestTabletPersister, test_persist_tablet_basic)
{
  ObLSHandle ls_handle;
  ASSERT_SUCC(get_ls(ls_id_.id(), ls_handle));
  ASSERT_TRUE(ls_handle.is_valid());
  uint64_t data_version = 0;
  ASSERT_SUCC(GET_MIN_DATA_VERSION(MTL_ID(), data_version));

  // case1: test normal tablet persistence
  {
    ObTabletHandle tablet_handle;
    ASSERT_SUCC(create_tablet(ls_handle, ObTabletID(CUR_TABLET_ID++), tablet_handle, false));
    const ObTablet *tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
    GEN_PERSISTER_PARAM(param, data_version, tablet);
    ObTabletHandle new_handle;
    ASSERT_SUCC(ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_handle));
    ASSERT_TRUE(new_handle.is_valid());
    check_tablet_persist(*new_handle.get_obj());
  }
  // case2: test empty shell persistence
  {
    ObTabletHandle tablet_handle;
    const ObTabletID tablet_id(CUR_TABLET_ID++);
    ASSERT_SUCC(create_empty_shell(ls_handle, tablet_id));
    ASSERT_SUCC(get_tablet_from_ls(ls_handle.get_ls(), tablet_id, tablet_handle));
    const ObTablet *tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
    ASSERT_TRUE(tablet->is_empty_shell());
    GEN_PERSISTER_PARAM(param, data_version, tablet);
    ObTabletHandle new_handle;
    ASSERT_SUCC(ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_handle));
    ASSERT_TRUE(new_handle.is_valid());
    check_tablet_persist(*new_handle.get_obj());
  }
}

TEST_F(TestTabletPersister, test_batch_tablet_persist)
{
  ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "Mittest"));
  const int64_t tablet_cnt = 256;

  ObLSHandle ls_handle;
  ASSERT_SUCC(get_ls(ls_id_.id(), ls_handle));
  ASSERT_TRUE(ls_handle.is_valid());
  uint64_t data_version = 0;
  ASSERT_SUCC(GET_MIN_DATA_VERSION(MTL_ID(), data_version));

  ObTabletHandle tablet_handles[tablet_cnt];
  ObUncopyableItemArray<ObTabletPersisterParam> params(allocator, tablet_cnt);
  for (int64_t i = 0; i < tablet_cnt; ++i) {
    ASSERT_SUCC(create_tablet(ls_handle, ObTabletID(CUR_TABLET_ID++), tablet_handles[i], false));
    const ObTablet *tablet = tablet_handles[i].get_obj();
    int32_t priv_transfer_epoch = 0;
    ASSERT_SUCC(tablet->get_private_transfer_epoch(priv_transfer_epoch));
    ASSERT_SUCC(params.emplace_back(data_version, ls_id_, 0, tablet->get_tablet_id(), priv_transfer_epoch, 0));
  }

  {
    int64_t timestamp = ObTimeUtility::current_time_us();
    ObTabletHandle new_handles[tablet_cnt];
    ASSERT_SUCC(ObTabletPersister::batch_persist_and_transform_tablets(params, tablet_handles, tablet_cnt, new_handles));
    fprintf(stdout, "batch persist %d tablets cost: %lldus\n", tablet_cnt, ObTimeUtility::current_time_us() - timestamp);
    // check results
    for (int64_t i = 0; i < tablet_cnt; ++i) {
      const ObTabletHandle &new_handle = new_handles[i];
      ASSERT_TRUE(new_handle.is_valid());
      check_tablet_persist(*new_handle.get_obj());
    }
  }

  {
    int64_t timestamp = ObTimeUtility::current_time_us();
    ObSArray<ObTabletHandle> new_handles;
    new_handles.set_attr(lib::ObMemAttr(MTL_ID(), "Mittest"));
    for (int64_t i = 0; i < tablet_cnt; ++i) {
      const ObTabletHandle &tablet_handle = tablet_handles[i];
      ObTabletHandle new_handle;
      GEN_PERSISTER_PARAM(param, data_version, tablet_handle.get_obj());
      ASSERT_SUCC(ObTabletPersister::persist_and_transform_tablet(param, *tablet_handle.get_obj(), new_handle));
      ASSERT_SUCC(new_handles.push_back(new_handle));
    }
    fprintf(stdout, "persist %d tablets cost: %lldus\n", tablet_cnt, ObTimeUtility::current_time_us() - timestamp);
    // check results
    for (int64_t i = 0; i < tablet_cnt; ++i) {
      const ObTabletHandle &new_handle = new_handles.at(i);
      ASSERT_TRUE(new_handle.is_valid());
      check_tablet_persist(*new_handle.get_obj());
    }
  }
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_persister.log*");
  OB_LOGGER.set_file_name("test_tablet_persister.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
