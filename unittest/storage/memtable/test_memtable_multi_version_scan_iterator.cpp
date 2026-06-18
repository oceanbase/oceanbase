/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_read_row_util.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace blocksstable;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace share;

static const int64_t SCHEMA_COLUMN_CNT = 3;
static const int64_t SCHEMA_ROWKEY_CNT = 1;
static const int64_t OUTPUT_COLUMN_CNT = SCHEMA_COLUMN_CNT + 2;
static const int64_t OUTPUT_C1_COL_IDX = SCHEMA_ROWKEY_CNT + 2;
static const int64_t OUTPUT_C2_COL_IDX = SCHEMA_ROWKEY_CNT + 3;

struct VersionRowSpec
{
  int64_t trans_version_;
  int64_t c1_;
  ObMergeEngineType merge_engine_type_;
  bool is_delete_insert_update_ = false;
  int64_t delete_c1_ = 0;
};

static int prepare_col_descs(ObIArray<ObColDesc> &schema_cols,
                             ObIArray<ObColDesc> &output_cols,
                             ObIArray<int32_t> &output_cols_index)
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc;
  col_desc.col_type_.set_int();
  col_desc.col_order_ = ObOrderType::ASC;

  col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
  if (OB_FAIL(schema_cols.push_back(col_desc)) || OB_FAIL(output_cols.push_back(col_desc)) ||
      OB_FAIL(output_cols_index.push_back(0))) {
    STORAGE_LOG(WARN, "fail to push rowkey column", K(ret));
  } else {
    col_desc.col_id_ = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
    if (OB_FAIL(output_cols.push_back(col_desc)) || OB_FAIL(output_cols_index.push_back(OB_INVALID_INDEX))) {
      STORAGE_LOG(WARN, "fail to push trans version column", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    col_desc.col_id_ = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
    if (OB_FAIL(output_cols.push_back(col_desc)) || OB_FAIL(output_cols_index.push_back(OB_INVALID_INDEX))) {
      STORAGE_LOG(WARN, "fail to push sql sequence column", K(ret));
    }
  }

  for (int64_t i = 1; OB_SUCC(ret) && i < SCHEMA_COLUMN_CNT; ++i) {
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    if (OB_FAIL(schema_cols.push_back(col_desc)) ||
        OB_FAIL(output_cols.push_back(col_desc)) ||
        OB_FAIL(output_cols_index.push_back(i))) {
      STORAGE_LOG(WARN, "fail to push data column", K(ret), K(i));
    }
  }

  return ret;
}

static int build_row(ObIAllocator &allocator,
                     const int64_t key,
                     const int64_t c1,
                     const int64_t c2,
                     const ObDmlFlag dml_flag,
                     const ObMergeEngineType merge_engine_type,
                     ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(row.init(allocator, SCHEMA_COLUMN_CNT))) {
    STORAGE_LOG(WARN, "fail to init datum row", K(ret));
  } else {
    row.count_ = SCHEMA_COLUMN_CNT;
    row.storage_datums_[0].set_int(key);
    row.storage_datums_[1].set_int(c1);
    row.storage_datums_[2].set_int(c2);
    row.row_flag_.set_flag(dml_flag);
    row.merge_engine_type_ = merge_engine_type;
  }
  return ret;
}

static int build_trans_node(ObIAllocator &allocator,
                            ObCompatRowWriter &row_writer,
                            const ObIArray<ObColDesc> &schema_cols,
                            const ObDatumRow &row,
                            const int64_t trans_version,
                            ObMvccTransNode *&node)
{
  int ret = OB_SUCCESS;
  char *row_buf = nullptr;
  int64_t row_len = 0;
  ObMemtableData data;
  SCN scn;

  if (OB_FAIL(row_writer.write(SCHEMA_ROWKEY_CNT, row, nullptr, &schema_cols, row_buf, row_len))) {
    STORAGE_LOG(WARN, "fail to write datum row", K(ret), K(row));
  } else {
    data.set(row.row_flag_.get_dml_flag(), row_len, row_buf);
    if (OB_ISNULL(node = static_cast<ObMvccTransNode *>(allocator.alloc(sizeof(ObMvccTransNode) + data.dup_size())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc trans node", K(ret));
    } else if (FALSE_IT(new (node) ObMvccTransNode())) {
    } else if (OB_FAIL(ObMemtableDataHeader::build(reinterpret_cast<ObMemtableDataHeader *>(node->buf_), &data))) {
      STORAGE_LOG(WARN, "fail to build memtable data", K(ret));
    } else if (OB_FAIL(scn.convert_for_tx(trans_version))) {
      STORAGE_LOG(WARN, "fail to convert trans version", K(ret), K(trans_version));
    } else {
      node->trans_commit(scn, scn);
    }
  }

  return ret;
}

class ObMockMemtableMultiVersionScanIterator : public ObMemtableMultiVersionScanIterator
{
public:
  ObMockMemtableMultiVersionScanIterator()
    : ObMemtableMultiVersionScanIterator()
  {}

  int init_for_test(
      ObIAllocator &allocator,
      ObTableAccessContext &context,
      const ObITableReadInfo &read_info,
      const ObMemtableKey &key,
      ObMultiVersionValueIterator &value_iter,
      const ScanState scan_state = SCAN_COMPACT_ROW)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(bitmap_.init(read_info.get_request_count(), read_info.get_rowkey_count()))) {
      STORAGE_LOG(WARN, "fail to init nop bitmap", K(ret));
    } else if (OB_FAIL(row_.init(allocator, OUTPUT_COLUMN_CNT))) {
      STORAGE_LOG(WARN, "fail to init row", K(ret));
    } else {
      is_inited_ = true;
      context_ = &context;
      read_info_ = &read_info;
      key_ = &key;
      value_iter_ = &value_iter;
      scan_state_ = scan_state;
      trans_version_col_idx_ = SCHEMA_ROWKEY_CNT;
      sql_sequence_col_idx_ = SCHEMA_ROWKEY_CNT + 1;
    }
    return ret;
  }

  void set_mock_uncommitted_row(const int64_t c1, const int64_t c2, const ObMergeEngineType merge_engine_type)
  {
    has_mock_uncommitted_row_ = true;
    mock_uncommitted_c1_ = c1;
    mock_uncommitted_c2_ = c2;
    mock_uncommitted_merge_engine_type_ = merge_engine_type;
  }

protected:
  virtual int iterate_uncommitted_row(const ObStoreRowkey &key, ObDatumRow &row) override
  {
    int ret = OB_SUCCESS;
    if (!has_mock_uncommitted_row_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected uncommitted row request", K(ret));
    } else if (OB_FAIL(ObReadRow::iterate_row_key(key, row))) {
      STORAGE_LOG(WARN, "fail to iterate rowkey", K(ret), K(key));
    } else {
      row.count_ = OUTPUT_COLUMN_CNT;
      row.storage_datums_[SCHEMA_ROWKEY_CNT].set_int(INT64_MIN);
      row.storage_datums_[SCHEMA_ROWKEY_CNT + 1].set_int(-1);
      row.storage_datums_[OUTPUT_C1_COL_IDX].set_int(mock_uncommitted_c1_);
      row.storage_datums_[OUTPUT_C2_COL_IDX].set_int(mock_uncommitted_c2_);
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.merge_engine_type_ = mock_uncommitted_merge_engine_type_;
    }
    return ret;
  }

private:
  bool has_mock_uncommitted_row_ = false;
  int64_t mock_uncommitted_c1_ = 0;
  int64_t mock_uncommitted_c2_ = 0;
  ObMergeEngineType mock_uncommitted_merge_engine_type_ = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
};

static void prepare_read_info(ObIAllocator &allocator,
                              const ObIArray<ObColDesc> &output_cols,
                              const ObIArray<int32_t> &output_cols_index,
                              const int64_t micro_block_format_version,
                              ObReadInfoStruct &read_info)
{
  read_info.init_basic_info(SCHEMA_COLUMN_CNT,
                            SCHEMA_ROWKEY_CNT,
                            false,
                            false,
                            false,
                            ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE,
                            false,
                            micro_block_format_version,
                            false,
                            false);
  ASSERT_EQ(OB_SUCCESS, read_info.prepare_arrays(allocator, output_cols, output_cols.count()));
  for (int64_t i = 0; i < output_cols_index.count(); ++i) {
    read_info.cols_index_.array_[i] = output_cols_index.at(i);
    read_info.memtable_cols_index_.array_[i] = output_cols_index.at(i);
  }
  ASSERT_EQ(OB_SUCCESS, read_info.datum_utils_.init(output_cols, SCHEMA_ROWKEY_CNT, false, allocator, false));
  read_info.is_inited_ = true;
}

static void run_committed_chain_case(ObIAllocator &allocator,
                                     ObCompatRowWriter &row_writer,
                                     const ObIArray<ObColDesc> &schema_cols,
                                     const ObITableReadInfo &read_info,
                                     const int64_t rowkey,
                                     const VersionRowSpec *specs,
                                     const int64_t spec_count)
{
  ASSERT_GT(spec_count, 1);
  ObTableAccessContext context;
  ObObj rowkey_obj;
  ObStoreRowkey store_rowkey;
  ObMemtableKey memtable_key;
  ObMultiVersionValueIterator value_iter;
  ObMockMemtableMultiVersionScanIterator iter;
  ObDatumRow datum_rows[8];
  ObMvccTransNode *nodes[8] = {nullptr};
  int64_t node_count = 0;
  const ObDatumRow *row = nullptr;

  ASSERT_LE(spec_count, 4);
  rowkey_obj.set_int(rowkey);
  ASSERT_EQ(OB_SUCCESS, store_rowkey.assign(&rowkey_obj, 1));
  ASSERT_EQ(OB_SUCCESS, memtable_key.encode(&store_rowkey));

  for (int64_t i = 0; i < spec_count; ++i) {
    if (specs[i].is_delete_insert_update_) {
      ASSERT_EQ(ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, specs[i].merge_engine_type_);
      ASSERT_LT(node_count + 1, ARRAYSIZEOF(nodes));
      ASSERT_EQ(OB_SUCCESS, build_row(allocator,
                                      rowkey,
                                      specs[i].c1_,
                                      specs[i].c1_ * 10,
                                      ObDmlFlag::DF_INSERT,
                                      specs[i].merge_engine_type_,
                                      datum_rows[node_count]));
      ASSERT_EQ(OB_SUCCESS, build_trans_node(allocator,
                                             row_writer,
                                             schema_cols,
                                             datum_rows[node_count],
                                             specs[i].trans_version_,
                                             nodes[node_count]));
      if (node_count > 0) {
        nodes[node_count - 1]->prev_ = nodes[node_count];
      }
      ++node_count;

      ASSERT_EQ(OB_SUCCESS, build_row(allocator,
                                      rowkey,
                                      specs[i].delete_c1_,
                                      specs[i].delete_c1_ * 10,
                                      ObDmlFlag::DF_DELETE,
                                      specs[i].merge_engine_type_,
                                      datum_rows[node_count]));
      ASSERT_EQ(OB_SUCCESS, build_trans_node(allocator,
                                             row_writer,
                                             schema_cols,
                                             datum_rows[node_count],
                                             specs[i].trans_version_,
                                             nodes[node_count]));
      nodes[node_count - 1]->prev_ = nodes[node_count];
      ++node_count;
    } else {
      ASSERT_LT(node_count, ARRAYSIZEOF(nodes));
      ASSERT_EQ(OB_SUCCESS, build_row(allocator,
                                      rowkey,
                                      specs[i].c1_,
                                      specs[i].c1_ * 10,
                                      ObDmlFlag::DF_INSERT,
                                      specs[i].merge_engine_type_,
                                      datum_rows[node_count]));
      ASSERT_EQ(OB_SUCCESS, build_trans_node(allocator,
                                             row_writer,
                                             schema_cols,
                                             datum_rows[node_count],
                                             specs[i].trans_version_,
                                             nodes[node_count]));
      if (node_count > 0) {
        nodes[node_count - 1]->prev_ = nodes[node_count];
      }
      ++node_count;
    }
  }

  context.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID);
  context.trans_version_range_.base_version_ = 0;
  context.trans_version_range_.multi_version_start_ = 0;
  context.trans_version_range_.snapshot_version_ = specs[0].trans_version_;
  value_iter.is_inited_ = true;
  value_iter.version_range_ = context.trans_version_range_;
  value_iter.version_iter_ = nodes[0];
  ASSERT_EQ(OB_SUCCESS, value_iter.init_multi_version_iter());
  ASSERT_TRUE(value_iter.has_multi_commit_trans());

  ASSERT_EQ(OB_SUCCESS, iter.init_for_test(allocator, context, read_info, memtable_key, value_iter));
  ASSERT_EQ(OB_SUCCESS, iter.inner_get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_TRUE(row->is_compacted_multi_version_row());
  ASSERT_TRUE(row->is_first_multi_version_row());
  ASSERT_EQ(specs[0].c1_, row->storage_datums_[OUTPUT_C1_COL_IDX].get_int());
  ASSERT_EQ(specs[0].merge_engine_type_, row->merge_engine_type_);

  for (int64_t i = 0; i < spec_count; ++i) {
    row = nullptr;
    ASSERT_EQ(OB_SUCCESS, iter.inner_get_next_row(row));
    ASSERT_NE(nullptr, row);
    ASSERT_TRUE(row->row_flag_.is_insert());
    ASSERT_FALSE(row->is_first_multi_version_row());
    ASSERT_EQ(specs[i].c1_, row->storage_datums_[OUTPUT_C1_COL_IDX].get_int());
    ASSERT_EQ(specs[i].c1_ * 10, row->storage_datums_[OUTPUT_C2_COL_IDX].get_int());
    ASSERT_EQ(specs[i].merge_engine_type_, row->merge_engine_type_);
    if (specs[i].is_delete_insert_update_) {
      ASSERT_EQ(-specs[i].trans_version_, row->storage_datums_[SCHEMA_ROWKEY_CNT].get_int());
      row = nullptr;
      ASSERT_EQ(OB_SUCCESS, iter.inner_get_next_row(row));
      ASSERT_NE(nullptr, row);
      ASSERT_TRUE(row->row_flag_.is_delete());
      ASSERT_EQ(specs[i].delete_c1_, row->storage_datums_[OUTPUT_C1_COL_IDX].get_int());
      ASSERT_EQ(specs[i].delete_c1_ * 10, row->storage_datums_[OUTPUT_C2_COL_IDX].get_int());
      ASSERT_EQ(-specs[i].trans_version_, row->storage_datums_[SCHEMA_ROWKEY_CNT].get_int());
      ASSERT_EQ(specs[i].merge_engine_type_, row->merge_engine_type_);
    }
  }
  ASSERT_EQ(ObMemtableMultiVersionScanIterator::SCAN_END, iter.scan_state_);
}

static void run_uncommitted_row_case(ObIAllocator &allocator,
                                     const ObITableReadInfo &read_info,
                                     const int64_t rowkey,
                                     const int64_t c1,
                                     const ObMergeEngineType merge_engine_type)
{
  ObTableAccessContext context;
  ObObj rowkey_obj;
  ObStoreRowkey store_rowkey;
  ObMemtableKey memtable_key;
  ObMultiVersionValueIterator value_iter;
  ObMockMemtableMultiVersionScanIterator iter;
  const ObDatumRow *row = nullptr;

  rowkey_obj.set_int(rowkey);
  ASSERT_EQ(OB_SUCCESS, store_rowkey.assign(&rowkey_obj, 1));
  ASSERT_EQ(OB_SUCCESS, memtable_key.encode(&store_rowkey));

  context.tablet_id_ = ObTabletID(ObTabletID::MIN_USER_TABLET_ID);
  context.trans_version_range_.base_version_ = 0;
  context.trans_version_range_.multi_version_start_ = 0;
  context.trans_version_range_.snapshot_version_ = INT64_MAX;
  value_iter.is_inited_ = true;
  value_iter.version_iter_ = nullptr;

  iter.set_mock_uncommitted_row(c1, c1 * 10, merge_engine_type);
  ASSERT_EQ(OB_SUCCESS, iter.init_for_test(allocator,
                                          context,
                                          read_info,
                                          memtable_key,
                                          value_iter,
                                          ObMemtableMultiVersionScanIterator::SCAN_UNCOMMITTED_ROW));
  ASSERT_EQ(OB_SUCCESS, iter.inner_get_next_row(row));
  ASSERT_NE(nullptr, row);
  ASSERT_TRUE(row->is_uncommitted_row());
  ASSERT_TRUE(row->is_last_multi_version_row());
  ASSERT_TRUE(row->row_flag_.is_insert());
  ASSERT_EQ(c1, row->storage_datums_[OUTPUT_C1_COL_IDX].get_int());
  ASSERT_EQ(c1 * 10, row->storage_datums_[OUTPUT_C2_COL_IDX].get_int());
  ASSERT_EQ(merge_engine_type, row->merge_engine_type_);
  ASSERT_EQ(ObMemtableMultiVersionScanIterator::SCAN_END, iter.scan_state_);
}

static void run_cross_engine_rows_case(const int64_t micro_block_format_version)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObSEArray<ObColDesc, 4> schema_cols;
  ObSEArray<ObColDesc, 8> output_cols;
  ObSEArray<int32_t, 8> output_cols_index;
  ObReadInfoStruct read_info;
  ObCompatRowWriter row_writer;
  const VersionRowSpec partial_delete_partial[] = {
    {300, 100, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE},
    {200, 200, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, true, 190},
    {100, 300, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE},
  };
  const VersionRowSpec partial_partial_delete[] = {
    {300, 400, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE},
    {200, 500, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE},
    {100, 600, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, true, 590},
  };
  const VersionRowSpec delete_partial_delete[] = {
    {300, 700, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, true, 690},
    {200, 800, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE},
    {100, 900, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT, true, 890},
  };

  ASSERT_EQ(OB_SUCCESS, prepare_col_descs(schema_cols, output_cols, output_cols_index));
  prepare_read_info(allocator, output_cols, output_cols_index, micro_block_format_version, read_info);
  ASSERT_EQ(OB_SUCCESS, row_writer.init(micro_block_format_version));

  run_uncommitted_row_case(allocator, read_info, 1, 10, ObMergeEngineType::OB_MERGE_ENGINE_PARTIAL_UPDATE);
  run_uncommitted_row_case(allocator, read_info, 2, 20, ObMergeEngineType::OB_MERGE_ENGINE_DELETE_INSERT);
  run_committed_chain_case(allocator,
                           row_writer,
                           schema_cols,
                           read_info,
                           3,
                           partial_delete_partial,
                           ARRAYSIZEOF(partial_delete_partial));
  run_committed_chain_case(allocator,
                           row_writer,
                           schema_cols,
                           read_info,
                           4,
                           partial_partial_delete,
                           ARRAYSIZEOF(partial_partial_delete));
  run_committed_chain_case(allocator,
                           row_writer,
                           schema_cols,
                           read_info,
                           5,
                           delete_partial_delete,
                           ARRAYSIZEOF(delete_partial_delete));
}

TEST(ObMemtableMultiVersionScanIteratorTest, cross_engine_rows)
{
  run_cross_engine_rows_case(ObMicroBlockFormatVersionHelper::LATEST_VERSION);
}

TEST(ObMemtableMultiVersionScanIteratorTest, cross_engine_rows_v0)
{
  ASSERT_EQ(FLAT_ROW_STORE,
            ObMicroBlockFormatVersionHelper::decide_flat_format(ObMicroBlockFormatVersionHelper::DEFAULT_VERSION));
  run_cross_engine_rows_case(ObMicroBlockFormatVersionHelper::DEFAULT_VERSION);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_memtable_multi_version_scan_iterator.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
