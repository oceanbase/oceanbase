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
#define protected public
#define private public

#include "storage/blocksstable/ob_macro_block.h"
#include "storage/test_schema_prepare.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace compaction;

namespace unittest
{
static ObSimpleMemLimitGetter getter;
class TestObDataStoreDesc : public blocksstable::TestDataFilePrepare
{
public:
  TestObDataStoreDesc()
    : blocksstable::TestDataFilePrepare(&getter, "test_data_store_desc"),
      mock_ls_id_(1),
      mock_tablet_id_(1)
  {}
  ~TestObDataStoreDesc() = default;
  share::ObLSID mock_ls_id_;
  ObTabletID mock_tablet_id_;
};

TEST_F(TestObDataStoreDesc, test_static_desc)
{
  ObStaticDataStoreDesc static_desc;
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, 5);
  table_schema.compressor_type_ = ObCompressorType::ZSTD_1_3_8_COMPRESSOR;

  const int64_t snapshot = 10000;
  share::SCN scn;
  scn.convert_for_tx(100);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            static_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MINI_MERGE, snapshot, share::SCN::invalid_scn(), 1/*cluster_version*/));
  ASSERT_EQ(OB_SUCCESS, static_desc.init(table_schema, mock_ls_id_, mock_tablet_id_, MINI_MERGE, snapshot, scn, 1/*cluster_version*/));
  ASSERT_TRUE(static_desc.is_valid());

  ASSERT_EQ(static_desc.is_ddl_, false);
  ASSERT_EQ(static_desc.merge_type_, MINI_MERGE);
  ASSERT_EQ(static_desc.ls_id_, mock_ls_id_);
  ASSERT_EQ(static_desc.tablet_id_, mock_tablet_id_);
  ASSERT_EQ(static_desc.compressor_type_, ObStaticDataStoreDesc::DEFAULT_MINOR_COMPRESSOR_TYPE);
  ASSERT_EQ(static_desc.schema_version_, table_schema.schema_version_);
  ASSERT_EQ(static_desc.snapshot_version_, snapshot);
  ASSERT_EQ(static_desc.end_scn_, scn);

  static_desc.reset();
  ASSERT_FALSE(static_desc.is_valid());

  ObStaticDataStoreDesc static_desc2(true/*is_ddl*/);
  ASSERT_EQ(OB_SUCCESS,
            static_desc2.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MAJOR_MERGE, snapshot, scn, DATA_VERSION_4_2_0_0));
  ASSERT_TRUE(static_desc2.is_valid());

  ASSERT_EQ(static_desc2.is_ddl_, true);
  ASSERT_EQ(static_desc2.merge_type_, MAJOR_MERGE);
  ASSERT_EQ(static_desc2.ls_id_, mock_ls_id_);
  ASSERT_EQ(static_desc2.tablet_id_, mock_tablet_id_);
  ASSERT_EQ(static_desc2.compressor_type_, ObCompressorType::ZSTD_1_3_8_COMPRESSOR);
  ASSERT_EQ(static_desc2.schema_version_, table_schema.schema_version_);
  ASSERT_EQ(static_desc2.snapshot_version_, snapshot);
  ASSERT_EQ(static_desc2.end_scn_.val_, snapshot);

  ObStaticDataStoreDesc static_desc3(true/*is_ddl*/);
  ASSERT_EQ(OB_SUCCESS, static_desc3.assign(static_desc2));
  ASSERT_TRUE(static_desc3.is_valid());
}

TEST_F(TestObDataStoreDesc, test_col_desc)
{
  const int64_t rowkey_cnt = 3;
  const int64_t col_cnt = 5;
  const int64_t mv_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ObColDataStoreDesc col_desc;
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, rowkey_cnt, col_cnt);

  ASSERT_FALSE(col_desc.is_valid());
  ASSERT_EQ(OB_SUCCESS, col_desc.init(true/*is_major*/, table_schema, 0/*table_cg_idx*/, DATA_VERSION_4_3_2_0));
  ASSERT_TRUE(col_desc.is_valid());

  ASSERT_EQ(true, col_desc.is_row_store_);
  ASSERT_EQ(true, col_desc.default_col_checksum_array_valid_);
  ASSERT_EQ(0, col_desc.table_cg_idx_);
  ASSERT_EQ(rowkey_cnt, col_desc.schema_rowkey_col_cnt_);
  ASSERT_EQ(rowkey_cnt + mv_rowkey_cnt, col_desc.rowkey_column_count_);
  ASSERT_EQ(col_cnt + mv_rowkey_cnt, col_desc.row_column_count_);
  ASSERT_EQ(col_desc.full_stored_col_cnt_, col_desc.row_column_count_);
  ASSERT_EQ(col_desc.row_column_count_, col_desc.col_default_checksum_array_.count());
  ASSERT_EQ(col_desc.row_column_count_, col_desc.col_desc_array_.count());
}

TEST_F(TestObDataStoreDesc, test_whole_data_desc)
{
  const int64_t snapshot = 1;
  ObWholeDataStoreDesc whole_desc;
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, 5);
  ASSERT_EQ(OB_SUCCESS,
            whole_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                            MAJOR_MERGE, snapshot, DATA_VERSION_4_2_0_0,
                            share::SCN::invalid_scn()));
  ASSERT_TRUE(whole_desc.is_valid());

  // point to other static desc member
  ObStaticDataStoreDesc static_desc;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            static_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MINI_MERGE, snapshot,
                             share::SCN::invalid_scn(), 0/*cluster_version*/));
  ASSERT_EQ(OB_SUCCESS,
            static_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MAJOR_MERGE, snapshot,
                             share::SCN::invalid_scn(), DATA_VERSION_4_2_0_0));
  whole_desc.desc_.static_desc_ = &static_desc;
  ASSERT_FALSE(whole_desc.is_valid());
}

TEST_F(TestObDataStoreDesc, gen_index_desc)
{
  ObWholeDataStoreDesc data_desc;
  ObWholeDataStoreDesc index_desc;
  ObTableSchema table_schema;
  TestSchemaPrepare::prepare_schema(table_schema, 5);

  const int64_t snapshot = 10000;
  share::SCN scn;
  scn.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS,
            data_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MAJOR_MERGE, snapshot, 1/*clsuter_version*/));
  ASSERT_TRUE(data_desc.is_valid());
  const ObDataStoreDesc &data_store_desc = data_desc.get_desc();

  ASSERT_EQ(OB_SUCCESS, index_desc.gen_index_store_desc(data_store_desc));

  const ObDataStoreDesc &index_data_desc = index_desc.get_desc();
  ASSERT_EQ(index_data_desc.get_row_column_count(), data_store_desc.get_rowkey_column_count() + 1);
  ASSERT_EQ(index_data_desc.get_col_desc_array().count(), data_store_desc.get_rowkey_column_count() + 1);
}

TEST_F(TestObDataStoreDesc, test_cg)
{
  ObArenaAllocator tmp_allocator;
  ObStaticDataStoreDesc static_desc;
  const int64_t rowkey_cnt = 1;
  const int64_t column_cnt = 5;
  const int64_t cg_cnt = column_cnt + 1;
  const int64_t mv_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  ObWholeDataStoreDesc data_desc[cg_cnt];
  ObWholeDataStoreDesc index_desc[cg_cnt];
  ObTableSchema table_schema;
  ObStorageSchema storage_schema;
  TestSchemaPrepare::prepare_schema(table_schema, rowkey_cnt, column_cnt);
  TestSchemaPrepare::add_all_and_each_column_group(tmp_allocator, table_schema);
  ASSERT_EQ(OB_SUCCESS, storage_schema.init(tmp_allocator, table_schema, Worker::CompatMode::MYSQL));

  const int64_t snapshot = 10000;
  share::SCN scn;
  scn.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS,
            static_desc.init(table_schema, mock_ls_id_, mock_tablet_id_,
                             MAJOR_MERGE, snapshot, share::SCN::invalid_scn(), DATA_VERSION_4_3_2_0/*cluster_version*/));
  ASSERT_TRUE(static_desc.is_valid());

  const ObIArray<ObStorageColumnGroupSchema> &column_groups = storage_schema.get_column_groups();
  for (int64_t i = 0 ; i < column_groups.count(); ++i) {
    const ObStorageColumnGroupSchema &column_group = column_groups.at(i);
    const ObDataStoreDesc &data_store_desc = data_desc[i].get_desc();
    if (column_group.is_all_column_group()) {
      continue;
    }
    ASSERT_EQ(OB_SUCCESS, data_desc[i].init(static_desc, storage_schema, &column_group, i));

    // check single cg default skip index
    ASSERT_EQ(3, data_desc[i].col_desc_.agg_meta_array_.count());
    ASSERT_EQ(ObSkipIndexColType::SK_IDX_MIN, data_desc[i].col_desc_.agg_meta_array_.at(0).col_type_);
    ASSERT_EQ(ObSkipIndexColType::SK_IDX_MAX, data_desc[i].col_desc_.agg_meta_array_.at(1).col_type_);
    ASSERT_EQ(ObSkipIndexColType::SK_IDX_NULL_COUNT, data_desc[i].col_desc_.agg_meta_array_.at(2).col_type_);

    ASSERT_EQ(data_store_desc.get_row_column_count(), column_group.column_cnt_);
    ASSERT_EQ(data_store_desc.get_full_stored_col_cnt(), column_group.column_cnt_);
    ASSERT_EQ(data_store_desc.get_schema_rowkey_col_cnt(), column_group.schema_rowkey_column_cnt_);
    ASSERT_EQ(data_store_desc.get_rowkey_column_count(), column_group.rowkey_column_cnt_);

    COMMON_LOG(INFO, "prepare desc", K(i), K(column_group), K(data_store_desc));
    ASSERT_EQ(data_store_desc.get_table_cg_idx(), i);
    ASSERT_EQ(data_store_desc.get_is_row_store(), column_group.is_all_column_group() || column_group.is_rowkey_column_group());
    ASSERT_EQ(data_store_desc.get_col_desc_array().count(), data_store_desc.get_row_column_count());

    ASSERT_EQ(OB_SUCCESS, index_desc[i].gen_index_store_desc(data_store_desc));
    const ObDataStoreDesc &index_store_desc = index_desc[i].get_desc();
    ASSERT_EQ(index_store_desc.get_schema_rowkey_col_cnt(), 0);
    ASSERT_EQ(index_store_desc.get_rowkey_column_count(), 1);
    ASSERT_EQ(index_store_desc.get_row_column_count(), data_store_desc.get_row_column_count() + 1);
    ASSERT_EQ(index_store_desc.get_col_desc_array().count(), index_store_desc.get_row_column_count());
  } // end of for
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_data_store_desc.log*");
  OB_LOGGER.set_file_name("test_data_store_desc.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}