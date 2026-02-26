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
#include "storage/direct_load/ob_direct_load_i_table.h"

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
  static int construct_rowkey_iter(blocksstable::ObSSTable *sstable,
                                   const blocksstable::ObDatumRange &scan_range,
                                   const storage::ObITableReadInfo &index_read_info,
                                   common::ObIAllocator &allocator,
                                   ObIDirectLoadDatumRowkeyIterator *&rowkey_iter);
  static int construct_rowkey_iter(ObDirectLoadMultipleSSTable *sstable,
                                   const ObDirectLoadTableDataDesc &table_data_desc,
                                   common::ObIAllocator &allocator,
                                   ObIDirectLoadDatumRowkeyIterator *&rowkey_iter);
  static int construct_multiple_rowkey_iter(ObDirectLoadMultipleSSTable *sstable,
                                            const ObDirectLoadTableDataDesc &table_data_desc,
                                            common::ObIAllocator &allocator,
                                            ObIDirectLoadMultipleDatumRowkeyIterator *&rowkey_iter);

  static int construct_origin_table_rowkey_iters(
    ObDirectLoadOriginTable *origin_table,
    const blocksstable::ObDatumRange &scan_range,
    common::ObIAllocator &allocator,
    ObDirectLoadDatumRowkeyIteratorGuard &iter_guard);
  static int construct_sstable_rowkey_iters(
    const ObDirectLoadTableHandleArray &sstable_array,
    const ObDirectLoadTableDataDesc &table_data_desc,
    common::ObIAllocator &allocator,
    ObDirectLoadDatumRowkeyIteratorGuard &iter_guard);
  static int construct_sstable_multiple_rowkey_iters(
    const ObDirectLoadTableHandleArray &sstable_array,
    const ObDirectLoadTableDataDesc &table_data_desc,
    common::ObIAllocator &allocator,
    ObDirectLoadMultipleDatumRowkeyIteratorGuard &iter_guard);

  static int prepare_ranges_memtable_readable(
      common::ObIArray<blocksstable::ObDatumRange> &range_array,
      const common::ObIArray<share::schema::ObColDesc> &col_descs,
      common::ObIAllocator &allocator);

  // 固定步长采样
  static int rowkey_fixed_sample(ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
                                 const int64_t step,
                                 common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array,
                                 common::ObIAllocator &allocator);
  // 自适应采样
  static const int64_t DEFAULT_ADAPTIVE_SAMPLE_FACTOR = 8;
  static int rowkey_adaptive_sample(ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
                                    const int64_t sample_num,
                                    common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array,
                                    common::ObIAllocator &allocator);
  // 蓄水池采样
  static const int64_t DEFAULT_RESERVOIR_SAMPLE_NUM = 500;
  static int rowkey_reservoir_sample(ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
                                     const int64_t sample_num,
                                     const blocksstable::ObStorageDatumUtils *datum_utils,
                                     common::ObArray<blocksstable::ObDatumRowkey> &rowkey_array,
                                     common::ObIAllocator &allocator);
};

struct ObDirectLoadSampleInfo
{
public:
  ObDirectLoadSampleInfo();
  ~ObDirectLoadSampleInfo();
  int get_origin_info(ObDirectLoadOriginTable *origin_table);
  int get_sstable_info(const ObDirectLoadTableHandleArray &sstable_array,
                       const ObDirectLoadTableDataDesc &table_data_desc);
  int calc_sample_info(const int64_t parallel, bool based_on_origin = false);
  TO_STRING_KV(K_(origin_sample_num),
               K_(sstable_sample_num),
               K_(origin_rows_per_sample),
               K_(sstable_rows_per_sample),
               K_(origin_step),
               K_(sstable_step));
public:
  int64_t origin_sample_num_;
  int64_t sstable_sample_num_;
  int64_t origin_rows_per_sample_;
  int64_t sstable_rows_per_sample_;
  int64_t origin_step_;
  int64_t sstable_step_;
};

class ObDirectLoadMergeRangeSplitter
{
  typedef ObDirectLoadRowkeyMerger<blocksstable::ObDatumRowkey, ObDirectLoadDatumRowkeyCompare>
    RowkeyMerger;
public:
  ObDirectLoadMergeRangeSplitter();
  ~ObDirectLoadMergeRangeSplitter();
  int init(const common::ObTabletID &tablet_id,
           ObDirectLoadOriginTable *origin_table,
           const ObDirectLoadTableHandleArray &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const int64_t max_range_count);
  int split_range(common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  common::ObIAllocator &allocator);
private:
  int init_origin_rowkey_merger(ObDirectLoadOriginTable *origin_table);
  int init_sstable_rowkey_merger(const ObDirectLoadTableHandleArray &sstable_array,
                                 const ObDirectLoadTableDataDesc &table_data_desc);
  int init_rowkey_merger();
private:
  common::ObArenaAllocator allocator_;
  common::ObTabletID tablet_id_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  int64_t max_range_count_;
  blocksstable::ObDatumRange scan_range_;
  ObDirectLoadSampleInfo sample_info_;
  ObDirectLoadDatumRowkeyCompare compare_;
  ObDirectLoadDatumRowkeyIteratorGuard iter_guard_;
  RowkeyMerger origin_rowkey_merger_;
  RowkeyMerger sstable_rowkey_merger_;
  RowkeyMerger rowkey_merger_;
  int64_t total_sample_count_;
  bool split_one_range_;
  bool is_inited_;
};

class ObDirectLoadMultipleMergeRangeSplitter
{
  typedef ObDirectLoadRowkeyMerger<ObDirectLoadMultipleDatumRowkey,
                                   ObDirectLoadMultipleDatumRowkeyCompare>
    MultipleRowkeyMerger;
  typedef ObDirectLoadRowkeyMerger<blocksstable::ObDatumRowkey, ObDirectLoadDatumRowkeyCompare>
    RowkeyMerger;
public:
  ObDirectLoadMultipleMergeRangeSplitter();
  ~ObDirectLoadMultipleMergeRangeSplitter();
  int init(const ObDirectLoadTableHandleArray &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils,
           const common::ObIArray<share::schema::ObColDesc> &col_descs);
  int split_range(const common::ObTabletID &tablet_id,
                  ObDirectLoadOriginTable *origin_table,
                  int64_t max_range_count,
                  common::ObIArray<blocksstable::ObDatumRange> &range_array,
                  common::ObIAllocator &allocator);
private:
  int init_multiple_sstable_rowkey_merger(const ObDirectLoadTableHandleArray &sstable_array,
                                          const ObDirectLoadTableDataDesc &table_data_desc);
  int get_rowkeys_by_origin(ObDirectLoadOriginTable *origin_table,
                            common::ObArray<blocksstable::ObDatumRowkey> &rowkey_array,
                            common::ObIAllocator &allocator);
  int get_rowkeys_by_sstable(const int64_t max_range_count,
                             common::ObArray<blocksstable::ObDatumRowkey> &rowkey_array,
                             common::ObIAllocator &allocator);
  int combine_final_ranges(const common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array1,
                           const common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array2,
                           int64_t max_range_count,
                           common::ObIArray<blocksstable::ObDatumRange> &range_array,
                           common::ObIAllocator &allocator);
private:
  class TabletRowkeyIter final : public ObIDirectLoadDatumRowkeyIterator
  {
  public:
    TabletRowkeyIter(ObIDirectLoadMultipleDatumRowkeyIterator &multiple_rowkey_iter);
    virtual ~TabletRowkeyIter() = default;
    int set_next_tablet_id(const common::ObTabletID &tablet_id);
    int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
  private:
    ObIDirectLoadMultipleDatumRowkeyIterator &multiple_rowkey_iter_;
    const ObDirectLoadMultipleDatumRowkey *last_multiple_rowkey_;
    common::ObTabletID tablet_id_;
    blocksstable::ObDatumRowkey rowkey_;
    bool iter_end_;
  };
private:
  common::ObArenaAllocator allocator_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  ObDirectLoadSampleInfo sample_info_;
  ObDirectLoadMultipleDatumRowkeyIteratorGuard iter_guard_;
  ObDirectLoadMultipleDatumRowkeyCompare compare_;
  MultipleRowkeyMerger rowkey_merger_;
  TabletRowkeyIter tablet_rowkey_iter_;
  int64_t adaptive_sample_factor_; // 自适应采样系数
  int64_t reservoir_sample_num_;
  bool enable_reservoir_sample_;
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
  int init(const ObDirectLoadTableHandleArray &sstable_array,
           const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int split_range(common::ObIArray<ObDirectLoadMultipleDatumRange> &range_array,
                  int64_t max_range_count,
                  common::ObIAllocator &allocator);
private:
  int init_multiple_sstable_rowkey_merger(const ObDirectLoadTableHandleArray &sstable_array,
                                          const ObDirectLoadTableDataDesc &table_data_desc);
private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadSampleInfo sample_info_;
  ObDirectLoadMultipleDatumRowkeyCompare compare_;
  ObDirectLoadMultipleDatumRowkeyIteratorGuard iter_guard_;
  RowkeyMerger rowkey_merger_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
