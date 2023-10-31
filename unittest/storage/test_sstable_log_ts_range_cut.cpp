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
#include "storage/blocksstable/ob_data_buffer.h"
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/ob_i_table.h"
#include "storage/ob_storage_struct.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace omt;
using namespace share;
namespace unittest
{

class TestMockSSTable
{
public:
  static void generate_mock_sstable(
      const int64_t start_log_ts,
      const int64_t end_log_ts,
      ObSSTable &sstable,
      blocksstable::ObSSTableMeta &meta)
  {
    meta.basic_meta_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    meta.basic_meta_.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    meta.basic_meta_.status_ = SSTABLE_READY_FOR_READ;
    meta.data_root_info_.addr_.set_none_addr();
    meta.macro_info_.macro_meta_info_.addr_.set_none_addr();
    meta.basic_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    meta.is_inited_ = true;

    sstable.key_.table_type_ = ObITable::MINOR_SSTABLE;
    sstable.key_.tablet_id_ = 1;
    sstable.key_.scn_range_.start_scn_.convert_for_gts(start_log_ts);
    sstable.key_.scn_range_.end_scn_.convert_for_gts(end_log_ts);
    sstable.meta_ = &meta;
    sstable.valid_for_reading_ = true;
  }
};


class TestSSTableScnRangeCut : public ::testing::Test
{
public:
  TestSSTableScnRangeCut()
    : tenant_id_(1),
      t3m_(nullptr),
      tenant_base_(1)
  { }
  ~TestSSTableScnRangeCut() {}
  void SetUp()
  {
    t3m_ = OB_NEW(ObTenantMetaMemMgr, ObModIds::TEST, 1);
    t3m_->init();
    tenant_base_.set(t3m_);

    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  }
  void TearDown()
  {
    t3m_->~ObTenantMetaMemMgr();
    t3m_ = nullptr;
    tenant_base_.destroy();
    ObTenantEnv::set_tenant(nullptr);
  }
private:
  const uint64_t tenant_id_;
  ObTenantMetaMemMgr *t3m_;
  ObTenantBase tenant_base_;
  ObTabletTableStore tablet_table_store_;
  DISALLOW_COPY_AND_ASSIGN(TestSSTableScnRangeCut);
};

//normal condition
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [100, 200)
//sstable3 log_ts: [200,300)

TEST_F(TestSSTableScnRangeCut, sstable_scn_range_no_cross_and_continue)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  blocksstable::ObSSTableMeta meta1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1, meta1);

  ObSSTable sstable2;
  blocksstable::ObSSTableMeta meta2;
  TestMockSSTable::generate_mock_sstable(100, 200, sstable2, meta2);

  ObSSTable sstable3;
  blocksstable::ObSSTableMeta meta3;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable3, meta3);
  ObArray<ObITable *> minor_sstables;
  ObArray<ObITable *> cut_minor_sstables;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_scn_range_(allocator, minor_sstables, cut_minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, cut_minor_sstables.at(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, cut_minor_sstables.at(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, cut_minor_sstables.at(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, cut_minor_sstables.at(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, cut_minor_sstables.at(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(300, cut_minor_sstables.at(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());
}


//sstable log ts is not continue with other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [200, 300)
//sstable3 log_ts: [300,500)

TEST_F(TestSSTableScnRangeCut, sstable_scn_range_is_not_continue)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  blocksstable::ObSSTableMeta meta1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1, meta1);

  ObSSTable sstable2;
  blocksstable::ObSSTableMeta meta2;
  TestMockSSTable::generate_mock_sstable(200, 300, sstable2, meta2);

  ObSSTable sstable3;
  blocksstable::ObSSTableMeta meta3;
  TestMockSSTable::generate_mock_sstable(300, 500, sstable3, meta3);

  ObArray<ObITable *> minor_sstables;
  ObArray<ObITable *> cut_minor_sstables;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_scn_range_(allocator, minor_sstables, cut_minor_sstables);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}


//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [0, 200)
//sstable3 log_ts: [200,500)

TEST_F(TestSSTableScnRangeCut, sstable_scn_range_contain)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  blocksstable::ObSSTableMeta meta1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1, meta1);

  ObSSTable sstable2;
  blocksstable::ObSSTableMeta meta2;
  TestMockSSTable::generate_mock_sstable(0, 200, sstable2, meta2);

  ObSSTable sstable3;
  blocksstable::ObSSTableMeta meta3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3, meta3);

  ObArray<ObITable *> minor_sstables;
  ObArray<ObITable *> cut_minor_sstables;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_scn_range_(allocator, minor_sstables, cut_minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, cut_minor_sstables.at(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, cut_minor_sstables.at(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, cut_minor_sstables.at(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, cut_minor_sstables.at(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, cut_minor_sstables.at(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, cut_minor_sstables.at(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

}

//sstable log ts contain other sstable log ts
//sstable1 log_ts: [0,100)
//sstable2 log_ts: [50, 200)
//sstable3 log_ts: [200,500)

TEST_F(TestSSTableScnRangeCut, sstable_scn_range_has_overlap)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletTableStore tablet_table_store;

  ObSSTable sstable1;
  blocksstable::ObSSTableMeta meta1;
  TestMockSSTable::generate_mock_sstable(0, 100, sstable1, meta1);

  ObSSTable sstable2;
  blocksstable::ObSSTableMeta meta2;
  TestMockSSTable::generate_mock_sstable(50, 200, sstable2, meta2);

  ObSSTable sstable3;
  blocksstable::ObSSTableMeta meta3;
  TestMockSSTable::generate_mock_sstable(200, 500, sstable3, meta3);

  ObArray<ObITable *> minor_sstables;
  ObArray<ObITable *> cut_minor_sstables;

  ret = minor_sstables.push_back(&sstable1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = minor_sstables.push_back(&sstable3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_table_store.cut_ha_sstable_scn_range_(allocator, minor_sstables, cut_minor_sstables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, cut_minor_sstables.at(0)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(100, cut_minor_sstables.at(0)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(100, cut_minor_sstables.at(1)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, cut_minor_sstables.at(1)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());

  ASSERT_EQ(200, cut_minor_sstables.at(2)->key_.scn_range_.start_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(500, cut_minor_sstables.at(2)->key_.scn_range_.end_scn_.get_val_for_inner_table_field());
}


}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_log_ts_range_cut.log*");
  OB_LOGGER.set_file_name("test_sstable_log_ts_range_cut.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40L << 30);
  return RUN_ALL_TESTS();
}
