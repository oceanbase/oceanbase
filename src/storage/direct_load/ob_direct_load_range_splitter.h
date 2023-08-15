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
#pragma once

#include "storage/blocksstable/ob_datum_range.h"
#include "storage/direct_load/ob_direct_load_compare.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_range.h"
#include "storage/direct_load/ob_direct_load_rowkey_merger.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
} // namespace blocksstable
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObITableReadInfo;
class ObDirectLoadOriginTable;
class ObDirectLoadSSTable;
class ObDirectLoadMultipleSSTable;

class ObDirectLoadRangeSplitUtils
{
public:
  static int construct_rowkey_iter(ObDirectLoadSSTable *sstable, common::ObIAllocator &allocator,
                                   ObIDirectLoadDatumRowkeyIterator *&rowkey_iter);
  static int construct_rowkey_iter(blocksstable::ObSSTable *sstable,
                                   const blocksstable::ObDatumRange &scan_range,
                                   const storage::ObITableReadInfo &index_read_info,
                                   common::ObIAllocator &allocator,
                                   ObIDirectLoadDatumRowkeyIterator *&rowkey_iter);
  static int construct_rowkey_iter(ObDirectLoadMultipleSSTable *sstable,
                                   const common::ObTabletID &tablet_id,
                                   const ObDirectLoadTableDataDesc &table_data_desc,
                                   const blocksstable::ObStorageDatumUtils *datum_utils,
                                   common::ObIAllocator &allocator,
                                   ObIDirectLoadDatumRowkeyIterator *&rowkey_iter);
  static int construct_multiple_rowkey_iter(ObDirectLoadMultipleSSTable *sstable,
                                            const ObDirectLoadTableDataDesc &table_data_desc,
                                            common::ObIAllocator &allocator,
                                            ObIDirectLoadMultipleDatumRowkeyIterator *&rowkey_iter);
  // append to rowkey_iters
  static int construct_origin_table_rowkey_iters(ObDirectLoadOriginTable *origin_table,
                                                 const blocksstable::ObDatumRange &scan_range,
                                                 common::ObIAllocator &allocator,
                                                 int64_t &total_block_count,
                                                 common::ObIArray<ObIDirectLoadDatumRowkeyIterator *> &rowkey_iters);
};

class ObDirectLoadRowkeyMergeRangeSplitter
{
  typedef ObDirectLoadRowkeyMerger<blocksstable::ObDatumRowkey, ObDirectLoadDatumRowkeyCompare>
    RowkeyMerger;
public:
  ObDirectLoadRowkeyMergeRangeSplitter();
  virtual ~ObDirectLoadRowkeyMergeRangeSplitter();
  int init(const common::ObIArray<ObIDirectLoadDatumRowkeyIterator *> &rowkey_iters,
           int64_t total_rowkey_count, const blocksstable::ObStorageDatumUtils *datum_utils);
  int set_memtable_valid(const common::ObIArray<share::schema::ObColDesc> &col_descs);
  int split_range(common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  int64_t max_range_count, common::ObIAllocator &allocator);
private:
  int check_range_memtable_valid(blocksstable::ObDatumRange &range,
                                 common::ObIAllocator &allocator);
private:
  int64_t total_rowkey_count_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObDirectLoadDatumRowkeyCompare compare_;
  RowkeyMerger rowkey_merger_;
  bool is_memtable_valid_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  bool is_inited_;
};

class ObDirectLoadSSTableRangeSplitter
{
public:
  ObDirectLoadSSTableRangeSplitter();
  ~ObDirectLoadSSTableRangeSplitter();
  int init(const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int split_range(common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  int64_t max_range_count, common::ObIAllocator &allocator);
private:
  int construct_rowkey_iters(const common::ObIArray<ObDirectLoadSSTable *> &sstable_array);
private:
  common::ObArenaAllocator allocator_;
  ObSEArray<ObIDirectLoadDatumRowkeyIterator *, 64> rowkey_iters_;
  int64_t total_block_count_;
  ObDirectLoadRowkeyMergeRangeSplitter rowkey_merge_splitter_;
  bool is_inited_;
};

class ObDirectLoadMergeRangeSplitter
{
public:
  ObDirectLoadMergeRangeSplitter();
  ~ObDirectLoadMergeRangeSplitter();
  int init(ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadSSTable *> &sstable_array,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           const common::ObIArray<share::schema::ObColDesc> &col_descs);
  int split_range(common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  int64_t max_range_count, common::ObIAllocator &allocator);
private:
  int construct_sstable_rowkey_iters(const common::ObIArray<ObDirectLoadSSTable *> &sstable_array);
private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObDatumRange scan_range_;
  ObSEArray<ObIDirectLoadDatumRowkeyIterator *, 64> rowkey_iters_;
  int64_t total_block_count_;
  ObDirectLoadRowkeyMergeRangeSplitter rowkey_merge_splitter_;
  bool is_inited_;
};

class ObDirectLoadMultipleMergeTabletRangeSplitter
{
public:
  ObDirectLoadMultipleMergeTabletRangeSplitter();
  ~ObDirectLoadMultipleMergeTabletRangeSplitter();
  int init(const common::ObTabletID &tablet_id, ObDirectLoadOriginTable *origin_table,
           const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           const common::ObIArray<share::schema::ObColDesc> &col_descs);
  int split_range(common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  int64_t max_range_count, common::ObIAllocator &allocator);
private:
  int construct_sstable_rowkey_iters(
    const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
    const ObDirectLoadTableDataDesc &table_data_desc,
    const blocksstable::ObStorageDatumUtils *datum_utils);
  int get_sstable_index_block_count(ObDirectLoadMultipleSSTable *sstable,
                                    const ObDirectLoadTableDataDesc &table_data_desc,
                                    const blocksstable::ObStorageDatumUtils *datum_utils,
                                    int64_t &block_count);
private:
  common::ObArenaAllocator allocator_;
  common::ObTabletID tablet_id_;
  blocksstable::ObDatumRange scan_range_;
  ObSEArray<ObIDirectLoadDatumRowkeyIterator *, 64> rowkey_iters_;
  int64_t total_block_count_;
  ObDirectLoadRowkeyMergeRangeSplitter rowkey_merge_splitter_;
  bool is_inited_;
};

class ObDirectLoadMultipleMergeRangeSplitter
{
  static const int64_t BLOCK_COUNT_PER_RANGE = 16;
  typedef ObDirectLoadRowkeyMerger<ObDirectLoadMultipleDatumRowkey,
                                   ObDirectLoadMultipleDatumRowkeyCompare>
    MultipleRowkeyMerger;
  typedef ObDirectLoadRowkeyMerger<blocksstable::ObDatumRowkey, ObDirectLoadDatumRowkeyCompare>
    RowkeyMerger;
public:
  ObDirectLoadMultipleMergeRangeSplitter();
  ~ObDirectLoadMultipleMergeRangeSplitter();
  int init(const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           const common::ObIArray<share::schema::ObColDesc> &col_descs);
  int split_range(common::ObTabletID &tablet_id, ObDirectLoadOriginTable *origin_table,
                  int64_t max_range_count,
                  common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  common::ObIAllocator &allocator);
private:
  int construct_rowkey_iters(const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
                             const ObDirectLoadTableDataDesc &table_data_desc,
                             const blocksstable::ObStorageDatumUtils *datum_utils);
  int prepare_range_memtable_readable(blocksstable::ObDatumRange &range,
                                      common::ObIAllocator &allocator);
  int get_rowkeys_by_origin(ObDirectLoadOriginTable *origin_table,
                            common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array,
                            common::ObIAllocator &allocator);
  int get_rowkeys_by_multiple(common::ObTabletID &tablet_id,
                              common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array,
                              common::ObIAllocator &allocator);
  int combine_final_ranges(const common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array1,
                           const common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array2,
                           int64_t max_range_count,
                           common::ObIArray<blocksstable::ObDatumRange> &range_array,
                           common::ObIAllocator &allocator);
private:
  common::ObArenaAllocator allocator_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  common::ObArray<ObIDirectLoadMultipleDatumRowkeyIterator *> rowkey_iters_;
  ObDirectLoadMultipleDatumRowkeyCompare compare_;
  MultipleRowkeyMerger rowkey_merger_;
  common::ObTabletID last_tablet_id_;
  const ObDirectLoadMultipleDatumRowkey *last_rowkey_;
  bool is_inited_;
};

class ObDirectLoadMultipleSSTableRangeSplitter
{
  typedef ObDirectLoadRowkeyMerger<ObDirectLoadMultipleDatumRowkey,
                                   ObDirectLoadMultipleDatumRowkeyCompare>
    RowkeyMerger;
public:
  ObDirectLoadMultipleSSTableRangeSplitter();
  ~ObDirectLoadMultipleSSTableRangeSplitter();
  int init(const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int split_range(common::ObIArray<ObDirectLoadMultipleDatumRange> &range_array,
                  int64_t max_range_count, common::ObIAllocator &allocator);
private:
  int construct_rowkey_iters(const common::ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
                             const ObDirectLoadTableDataDesc &table_data_desc,
                             const blocksstable::ObStorageDatumUtils *datum_utils);
private:
  common::ObArenaAllocator allocator_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  ObSEArray<ObIDirectLoadMultipleDatumRowkeyIterator *, 64> rowkey_iters_;
  int64_t total_block_count_;
  ObDirectLoadMultipleDatumRowkeyCompare compare_;
  RowkeyMerger rowkey_merger_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
