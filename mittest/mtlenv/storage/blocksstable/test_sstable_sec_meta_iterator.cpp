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
#define private public
#define protected public

#include "storage/ob_partition_range_spliter.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
class TestSSTableSecMetaIterator : public TestIndexBlockDataPrepare
{
public:
  TestSSTableSecMetaIterator();
  virtual ~TestSSTableSecMetaIterator() {}
  virtual void SetUp();
  virtual void TearDown();
};

TestSSTableSecMetaIterator::TestSSTableSecMetaIterator()
  : TestIndexBlockDataPrepare(
      "Test sstable secondary meta iterator",
      compaction::MINI_MERGE,
      false,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65535 * 4)
{
}

void TestSSTableSecMetaIterator::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestSSTableSecMetaIterator::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestSSTableSecMetaIterator, test_basic)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  uint64_t tenant_id = MTL_ID();
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);

  // Iterate sample
  const int64_t skip_cnt = 3;
  meta_iter.reset();
  int64_t iter_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_,
      false, 3));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got skip next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  int64_t target_scan_cnt = data_macro_metas.count() == 0 ? 0
      : 1 + (data_macro_metas.count() - 1) / skip_cnt;
  ASSERT_EQ(iter_cnt, target_scan_cnt);


  // Iterate part
  meta_iter.reset();
  const int64_t pivot_rowkey_idx = 0;
  ObDatumRowkey pivot_rowkey;
  iter_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(pivot_rowkey_idx).get_rowkey(pivot_rowkey));

  range.reset();
  range.start_key_.set_min_rowkey();
  range.end_key_ = pivot_rowkey;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  STORAGE_LOG(INFO, "chaser debug start", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, pivot_rowkey_idx + 1);

  meta_iter.reset();
  range.reset();
  range.start_key_ = pivot_rowkey;
  range.end_key_.set_max_rowkey();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, data_macro_metas.count() - 1);

  // iter range lower than first macro block endkey
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(min_row_seed_ - 1, row));
  meta_iter.reset();
  range.reset();
  range.start_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.end_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));
  ASSERT_EQ(OB_SUCCESS, meta_iter.get_next(data_macro_meta));
  ASSERT_EQ(OB_ITER_END, meta_iter.get_next(data_macro_meta));

  // iter range larger than sstable endkey
  meta_iter.reset();
  range.reset();
  row.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(max_row_seed_ + 1, row));
  range.start_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.end_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      sstable_,
      index_read_info,
      allocator_));

  // Dual iter part
  ObDualMacroMetaIterator dual_iter;
  ObMacroBlockDesc macro_desc;
  const int64_t pivot_rowkey_idx_2 = 3;
  ObDatumRowkey pivot_rowkey_2;
  iter_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(pivot_rowkey_idx_2).get_rowkey(pivot_rowkey_2));

  range.reset();
  range.start_key_ = pivot_rowkey;
  range.end_key_ = pivot_rowkey_2;
  range.border_flag_.unset_inclusive_start();
  // range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  macro_desc.macro_meta_ = &data_macro_meta;
  STORAGE_LOG(INFO, "show query range", K(range));
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  // range.border_flag_.set_inclusive_start();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  dual_iter.reset();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  dual_iter.reset();
  range.end_key_.set_max_rowkey();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(INFO, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ObDatumRowkey endkey;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_last_rowkey(allocator_, endkey));
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

TEST_F(TestSSTableSecMetaIterator, test_ddl_kv)
{
  ObArray<ObDataMacroBlockMeta> data_macro_metas;
  uint64_t tenant_id = MTL_ID();
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObSSTableSecMetaIterator meta_iter;
  ObDataMacroBlockMeta data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  STORAGE_LOG(INFO, "qilu debug range1", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ObDataMacroBlockMeta *deep_copy_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, data_macro_meta.deep_copy(deep_copy_meta, allocator_));
      ASSERT_EQ(OB_SUCCESS, data_macro_metas.push_back(
          *(deep_copy_meta)));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_TRUE(data_macro_metas.count() > 0);

  // Iterate sample
  const int64_t skip_cnt = 3;
  meta_iter.reset();
  int64_t iter_cnt = 0;
  STORAGE_LOG(INFO, "qilu debug range2", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_,
      false, 3));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    STORAGE_LOG(DEBUG, "Got skip next data macro block meta", K(tmp_ret), K(data_macro_meta));
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  int64_t target_scan_cnt = data_macro_metas.count() == 0 ? 0
      : 1 + (data_macro_metas.count() - 1) / skip_cnt;
  ASSERT_EQ(iter_cnt, target_scan_cnt);


  // Iterate part
  meta_iter.reset();
  const int64_t pivot_rowkey_idx = 0;
  ObDatumRowkey pivot_rowkey;
  iter_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(pivot_rowkey_idx).get_rowkey(pivot_rowkey));

  range.reset();
  range.start_key_.set_min_rowkey();
  range.end_key_ = pivot_rowkey;
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  STORAGE_LOG(INFO, "qilu debug range3", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_));
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, pivot_rowkey_idx + 1);

  meta_iter.reset();
  range.reset();
  range.start_key_ = pivot_rowkey;
  range.end_key_.set_max_rowkey();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.set_inclusive_end();
  STORAGE_LOG(INFO, "qilu debug range4", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_));
  tmp_ret = OB_SUCCESS;
  iter_cnt = 0;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = meta_iter.get_next(data_macro_meta);
    if (OB_SUCCESS == tmp_ret) {
      ++iter_cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(iter_cnt, data_macro_metas.count() - 1);

  // iter range lower than first macro block endkey
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(min_row_seed_ - 1, row));
  meta_iter.reset();
  range.reset();
  range.start_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.end_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  STORAGE_LOG(INFO, "qilu debug range5", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_));
  ASSERT_EQ(OB_SUCCESS, meta_iter.get_next(data_macro_meta));
  ASSERT_EQ(OB_ITER_END, meta_iter.get_next(data_macro_meta));

  // iter range larger than sstable endkey
  meta_iter.reset();
  range.reset();
  row.reuse();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(max_row_seed_ + 1, row));
  range.start_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.end_key_.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT);
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  STORAGE_LOG(INFO, "qilu debug range6", K(range));
  ASSERT_EQ(OB_SUCCESS, meta_iter.open(
      range,
      ObMacroBlockMetaType::DATA_BLOCK_META,
      ddl_kv_,
      index_read_info,
      allocator_));

  // Dual iter part
  ObDualMacroMetaIterator dual_iter;
  ObMacroBlockDesc macro_desc;
  const int64_t pivot_rowkey_idx_2 = 3;
  ObDatumRowkey pivot_rowkey_2;
  iter_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, data_macro_metas.at(pivot_rowkey_idx_2).get_rowkey(pivot_rowkey_2));

  range.reset();
  range.start_key_ = pivot_rowkey;
  range.end_key_ = pivot_rowkey_2;
  range.border_flag_.unset_inclusive_start();
  // range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  macro_desc.macro_meta_ = &data_macro_meta;
  STORAGE_LOG(INFO, "show query range", K(range));
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  // range.border_flag_.set_inclusive_start();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  dual_iter.reset();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  dual_iter.reset();
  range.end_key_.set_max_rowkey();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    if (OB_SUCCESS == tmp_ret) {
      STORAGE_LOG(INFO, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
    }
  }
  ObDatumRowkey endkey;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_last_rowkey(allocator_, endkey));
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

TEST_F(TestSSTableSecMetaIterator, test_dual_iter)
{
  uint64_t tenant_id = MTL_ID();
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObDualMacroMetaIterator dual_iter;
  ObMacroBlockDesc macro_desc;
  ObDataMacroBlockMeta data_macro_meta;
  macro_desc.macro_meta_ = &data_macro_meta;
  ObDatumRange range;
  range.set_whole_range();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_));
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
    STORAGE_LOG(DEBUG, "Got next data macro block meta", K(tmp_ret), K(macro_desc));
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  dual_iter.reset();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, range, index_read_info, allocator_, true /* reverse */));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);

  // Iterate from half to end;
  const int64_t query_rowkey_seed = max_row_seed_ / 2;
  ObDatumRow gen_query_row;
  ASSERT_EQ(OB_SUCCESS, gen_query_row.init(MAX_TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(query_rowkey_seed, gen_query_row));
  ObDatumRowkey query_rowkey(gen_query_row.storage_datums_, TEST_ROWKEY_COLUMN_CNT + 2);
  ObDatumRange half_query_range;
  half_query_range.start_key_.set_min_rowkey();
  half_query_range.end_key_ = query_rowkey;
  half_query_range.border_flag_.unset_inclusive_start();
  half_query_range.border_flag_.set_inclusive_end();
  dual_iter.reset();
  ASSERT_EQ(OB_SUCCESS, dual_iter.open(sstable_, half_query_range, index_read_info, allocator_));
  tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = dual_iter.get_next_macro_block(macro_desc);
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

TEST_F(TestSSTableSecMetaIterator, test_basic_range_spliter)
{
  const int64_t target_parallel_cnt = 3;
  const ObITableReadInfo &index_read_info = tablet_handle_.get_obj()->get_rowkey_read_info();
  ObPartitionRangeSpliter range_spliter;
  ObRangeSplitInfo range_info;
  ObArray<ObITable *> tables;
  ObArray<ObStoreRange> range_array_1;
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&sstable_));
  ObStoreRange store_range;
  store_range.get_start_key().set_min();
  store_range.get_end_key().set_max();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(
      tables, index_read_info, store_range, range_info));
  ASSERT_EQ(range_info.max_macro_block_count_, sstable_.get_data_macro_block_count());
  // ASSERT_EQ(range_info.total_size_, sstable_.get_meta().basic_meta_.occupy_size_);
  range_info.set_parallel_target(target_parallel_cnt);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_info, allocator_, true, range_array_1));
  ASSERT_EQ(range_array_1.count(), target_parallel_cnt);


  range_info.reset();
  ObArray<ObStoreRange> range_array_2;
  ObStoreRow row;
  ObObj row_cells[MAX_TEST_COLUMN_CNT];
  row.row_val_.assign(row_cells, TEST_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(max_row_seed_ + 1, row));
  store_range.get_end_key().assign(row_cells, TEST_ROWKEY_COLUMN_CNT);
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(
      tables, index_read_info, store_range, range_info));
  ASSERT_EQ(range_info.max_macro_block_count_, sstable_.get_data_macro_block_count());
  // ASSERT_EQ(range_info.total_size_, sstable_.get_meta().basic_meta_.occupy_size_);
  range_info.set_parallel_target(target_parallel_cnt);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_info, allocator_, false, range_array_2));
  ASSERT_EQ(range_array_2.count(), target_parallel_cnt);

  for (int64_t i = 0; i < target_parallel_cnt; ++i) {
    ObStoreRowkey start_schema_key;
    ObStoreRowkey end_schema_key;

    int64_t start_key_cnt = range_array_1.at(i).get_start_key().get_obj_cnt();
    if (!range_array_1.at(i).get_start_key().is_min()) {
      start_key_cnt -= ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    }
    ASSERT_EQ(OB_SUCCESS, start_schema_key.assign(range_array_1.at(i).get_start_key().get_obj_ptr(), start_key_cnt));

    int64_t end_key_cnt = range_array_1.at(i).get_end_key().get_obj_cnt();
    if (!range_array_1.at(i).get_end_key().is_max()) {
      end_key_cnt -= ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    }
    ASSERT_EQ(OB_SUCCESS, end_schema_key.assign(range_array_1.at(i).get_end_key().get_obj_ptr(), end_key_cnt));

    ASSERT_EQ(start_schema_key, range_array_2.at(i).get_start_key());
    if (i != target_parallel_cnt - 1) {
      ASSERT_EQ(end_schema_key, range_array_2.at(i).get_end_key());
    }
  }

  // Try split range larger than sstable endkey
  range_info.reset();
  ObArray<ObStoreRange> range_array;
  store_range.get_start_key().assign(row_cells, TEST_ROWKEY_COLUMN_CNT);
  store_range.get_end_key().set_max();
  store_range.set_left_open();
  store_range.set_right_open();
  ASSERT_EQ(OB_SUCCESS, range_spliter.get_range_split_info(tables, index_read_info, store_range, range_info));
  ASSERT_EQ(range_info.max_macro_block_count_, 0);
  ASSERT_EQ(OB_SUCCESS, range_spliter.split_ranges(range_info, allocator_, false, range_array));
  ASSERT_EQ(1, range_array.count());
}

} // namespace blocksstable
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_sec_meta_iterator.log*");
  OB_LOGGER.set_file_name("test_sstable_sec_meta_iterator.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
