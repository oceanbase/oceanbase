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
#define USING_LOG_PREFIX STORAGE

#include "storage/direct_load/ob_direct_load_range_splitter.h"
#include "lib/random/ob_random.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_meta_scanner.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadRangeSplitUtils
 */

int ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
  ObSSTable *sstable,
  const ObDatumRange &scan_range,
  const ObITableReadInfo &index_read_info,
  ObIAllocator &allocator,
  ObIDirectLoadDatumRowkeyIterator *&rowkey_iter)
{
  int ret = OB_SUCCESS;
  ObSSTableSecMetaIterator *macro_meta_iter = nullptr;
  ObDirectLoadMacroBlockEndKeyIterator *end_key_iter = nullptr;
  if (OB_FAIL(sstable->scan_secondary_meta(allocator, scan_range, index_read_info,
                                           ObMacroBlockMetaType::DATA_BLOCK_META,
                                           macro_meta_iter))) {
    LOG_WARN("fail to scan secondary meta", KR(ret));
  } else if (OB_ISNULL(end_key_iter =
                         OB_NEWx(ObDirectLoadMacroBlockEndKeyIterator, (&allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObDirectLoadMacroBlockEndKeyIterator", KR(ret));
  } else if (OB_FAIL(end_key_iter->init(macro_meta_iter))) {
    LOG_WARN("fail to init rowkey iter", KR(ret));
  } else {
    rowkey_iter = end_key_iter;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != macro_meta_iter) {
      macro_meta_iter->~ObSSTableSecMetaIterator();
      allocator.free(macro_meta_iter);
      macro_meta_iter = nullptr;
    }
    if (nullptr != end_key_iter) {
      end_key_iter->~ObDirectLoadMacroBlockEndKeyIterator();
      allocator.free(end_key_iter);
      end_key_iter = nullptr;
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
  ObDirectLoadMultipleSSTable *sstable,
  const ObDirectLoadTableDataDesc &table_data_desc,
  ObIAllocator &allocator,
  ObIDirectLoadDatumRowkeyIterator *&rowkey_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable));
  } else {
    if (ObDirectLoadSampleMode::is_sample_enabled(table_data_desc.sample_mode_)) {
      // 采样模式, 直接从rowkey文件读
      if (OB_FAIL(sstable->scan_whole_rowkey(table_data_desc, allocator, rowkey_iter))) {
        LOG_WARN("fail to scan whole rowkey", KR(ret));
      }
    } else {
      // 非采样模式, 读每个索引块的endkey
      if (OB_FAIL(sstable->scan_whole_index_block_endkey(table_data_desc, allocator, rowkey_iter))) {
        LOG_WARN("fail to scan whole index block endkey", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_multiple_rowkey_iter(
  ObDirectLoadMultipleSSTable *sstable,
  const ObDirectLoadTableDataDesc &table_data_desc,
  ObIAllocator &allocator,
  ObIDirectLoadMultipleDatumRowkeyIterator *&rowkey_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable));
  } else {
    if (ObDirectLoadSampleMode::is_sample_enabled(table_data_desc.sample_mode_)) {
      // 采样模式, 直接从rowkey文件读
      if (OB_FAIL(sstable->scan_whole_rowkey(table_data_desc, allocator, rowkey_iter))) {
        LOG_WARN("fail to scan whole rowkey", KR(ret));
      }
    } else {
      // 非采样模式, 读每个索引块的endkey
      if (OB_FAIL(sstable->scan_whole_index_block_endkey(table_data_desc, allocator, rowkey_iter))) {
        LOG_WARN("fail to scan whole index block endkey", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
  ObDirectLoadOriginTable *origin_table,
  const ObDatumRange &scan_range,
  ObIAllocator &allocator,
  ObDirectLoadDatumRowkeyIteratorGuard &iter_guard)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == origin_table || !origin_table->is_valid() || !scan_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(origin_table), K(scan_range));
  } else {
    const ObITableReadInfo &read_info =
      origin_table->get_tablet_handle().get_obj()->get_rowkey_read_info();
    if (nullptr != origin_table->get_major_sstable()) {
      ObSSTable *major_sstable = origin_table->get_major_sstable();
      ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
      if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
            major_sstable, scan_range, read_info, allocator, rowkey_iter))) {
        LOG_WARN("fail to construct rowkey iter", KR(ret));
      } else if (OB_FAIL(iter_guard.push_back(rowkey_iter))) {
        LOG_WARN("fail to push back rowkey iter", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != rowkey_iter) {
          rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
          allocator.free(rowkey_iter);
          rowkey_iter = nullptr;
        }
      }
    } else {
      const ObIArray<ObSSTable *> &ddl_sstables = origin_table->get_ddl_sstables();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
        ObSSTable *ddl_sstable = ddl_sstables.at(i);
        ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
        if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
              ddl_sstable, scan_range, read_info, allocator, rowkey_iter))) {
          LOG_WARN("fail to construct rowkey iter", KR(ret));
        } else if (OB_FAIL(iter_guard.push_back(rowkey_iter))) {
          LOG_WARN("fail to push back rowkey iter", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != rowkey_iter) {
            rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
            allocator.free(rowkey_iter);
            rowkey_iter = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_sstable_rowkey_iters(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  ObIAllocator &allocator,
  ObDirectLoadDatumRowkeyIteratorGuard &iter_guard)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    const ObDirectLoadTableHandle &table_handle = sstable_array.at(i);
    if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_multiple_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable", KR(ret), K(table_handle));
    } else {
      ObDirectLoadMultipleSSTable *sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_handle.get_table());
      ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
      if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(sstable,
                                                                    table_data_desc,
                                                                    allocator,
                                                                    rowkey_iter))) {
        LOG_WARN("fail to construct rowkey iter", KR(ret));
      } else if (OB_FAIL(iter_guard.push_back(rowkey_iter))) {
        LOG_WARN("fail to push back rowkey iter", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != rowkey_iter) {
          rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
          allocator.free(rowkey_iter);
          rowkey_iter = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_sstable_multiple_rowkey_iters(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  ObIAllocator &allocator,
  ObDirectLoadMultipleDatumRowkeyIteratorGuard &iter_guard)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    const ObDirectLoadTableHandle &table_handle = sstable_array.at(i);
    if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_multiple_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sstable", KR(ret), K(table_handle));
    } else {
      ObDirectLoadMultipleSSTable *sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_handle.get_table());
      ObIDirectLoadMultipleDatumRowkeyIterator *rowkey_iter = nullptr;
      if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_multiple_rowkey_iter(sstable,
                                                                              table_data_desc,
                                                                              allocator,
                                                                              rowkey_iter))) {
        LOG_WARN("fail to construct rowkey iter", KR(ret));
      } else if (OB_FAIL(iter_guard.push_back(rowkey_iter))) {
        LOG_WARN("fail to push back rowkey iter", KR(ret));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != rowkey_iter) {
          rowkey_iter->~ObIDirectLoadMultipleDatumRowkeyIterator();
          allocator.free(rowkey_iter);
          rowkey_iter = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::prepare_ranges_memtable_readable(
  ObIArray<ObDatumRange> &range_array,
  const ObIArray<ObColDesc> &col_descs,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); ++i) {
    ObDatumRange &range = range_array.at(i);
    if (!range.start_key_.is_memtable_valid() &&
        OB_FAIL(range.start_key_.prepare_memtable_readable(col_descs, allocator))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
    } else if (!range.end_key_.is_memtable_valid() &&
               OB_FAIL(range.end_key_.prepare_memtable_readable(col_descs, allocator))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::rowkey_fixed_sample(
  ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
  const int64_t step,
  ObIArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  rowkey_array.reset();
  const ObDatumRowkey *rowkey = nullptr;
  ObDatumRowkey copied_rowkey;
  int count = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(rowkey_iter.get_next_rowkey(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (++count >= step) {
      if (OB_FAIL(rowkey->deep_copy(copied_rowkey, allocator))) {
        LOG_WARN("fail to deep copy rowkey", KR(ret));
      } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        count = 0;
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::rowkey_adaptive_sample(
  ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
  const int64_t sample_num,
  ObIArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  rowkey_array.reset();
  ObArenaAllocator tmp_allocator1("MTL_MMRS_Tmp");
  ObArenaAllocator tmp_allocator2("MTL_MMRS_Tmp");
  ObArray<ObDatumRowkey> rowkey_array1, rowkey_array2;
  tmp_allocator1.set_tenant_id(MTL_ID());
  tmp_allocator2.set_tenant_id(MTL_ID());
  rowkey_array1.set_block_allocator(ModulePageAllocator(tmp_allocator1));
  rowkey_array2.set_block_allocator(ModulePageAllocator(tmp_allocator2));
  ObIAllocator *cur_allocator = &tmp_allocator1, *next_allocator = &tmp_allocator2;
  ObArray<ObDatumRowkey> *cur_rowkey_array = &rowkey_array1, *next_rowkey_array = &rowkey_array2;
  const ObDatumRowkey *rowkey = nullptr;
  ObDatumRowkey copied_rowkey;
  int step = 1;
  int count = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(rowkey_iter.get_next_rowkey(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (++count >= step) {
      if (cur_rowkey_array->count() >= sample_num) {
        // 将所有rowkey两两合并
        for (int64_t i = 0; OB_SUCC(ret) && i < cur_rowkey_array->count() / 2; ++i) {
          const ObDatumRowkey &tmp_rowkey = cur_rowkey_array->at(i * 2 + 1);
          if (OB_FAIL(tmp_rowkey.deep_copy(copied_rowkey, *next_allocator))) {
            LOG_WARN("fail to deep copy rowkey", KR(ret));
          } else if (OB_FAIL(next_rowkey_array->push_back(copied_rowkey))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          step *= 2;
          std::swap(cur_allocator, next_allocator);
          std::swap(cur_rowkey_array, next_rowkey_array);
          next_rowkey_array->reset();
          next_allocator->reuse();
        }
      } else {
        if (OB_FAIL(rowkey->deep_copy(copied_rowkey, *cur_allocator))) {
          LOG_WARN("fail to deep copy rowkey", KR(ret));
        } else if (OB_FAIL(cur_rowkey_array->push_back(copied_rowkey))) {
          LOG_WARN("fail to push back", KR(ret));
        } else {
          count = 0;
        }
      }
    }
  }
  // 拷贝结果
  if (OB_SUCC(ret)) {
    ObDatumRowkey copied_rowkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_rowkey_array->count(); ++i) {
      const ObDatumRowkey &rowkey = cur_rowkey_array->at(i);
      if (OB_FAIL(rowkey.deep_copy(copied_rowkey, allocator))) {
        LOG_WARN("fail to deep copy rowkey", KR(ret));
      } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::rowkey_reservoir_sample(
  ObIDirectLoadDatumRowkeyIterator &rowkey_iter,
  const int64_t sample_num,
  const ObStorageDatumUtils *datum_utils,
  ObArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  rowkey_array.reset();
  const ObDatumRowkey *rowkey = nullptr;
  ObDatumRowkey copied_rowkey;
  int64_t i = 0, idx = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(rowkey_iter.get_next_rowkey(rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      ++i;
      if (i <= sample_num) {
        if (OB_FAIL(rowkey->deep_copy(copied_rowkey, allocator))) {
          LOG_WARN("fail to deep copy rowkey", KR(ret));
        } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } else {
        // TODO: 存在内存膨胀问题
        idx = ObRandom::rand(1, i);
        if (idx <= sample_num) {
          if (OB_FAIL(rowkey->deep_copy(copied_rowkey, allocator))) {
            LOG_WARN("fail to deep copy rowkey", KR(ret));
          } else {
            rowkey_array.at(idx - 1) = copied_rowkey;
          }
        }
      }
    }
  }
  // 对采样结果进行排序
  if (OB_SUCC(ret)) {
    ObDirectLoadDatumRowkeyCompare compare;
    if (OB_FAIL(compare.init(*datum_utils))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else {
      lib::ob_sort(rowkey_array.begin(), rowkey_array.end(), compare);
    }
  }
  return ret;
}

/**
 * ObDirectLoadSampleInfo
 */

ObDirectLoadSampleInfo::ObDirectLoadSampleInfo()
  : origin_sample_num_(0),
    sstable_sample_num_(0),
    origin_rows_per_sample_(0),
    sstable_rows_per_sample_(0),
    origin_step_(0),
    sstable_step_(0)
{
}

ObDirectLoadSampleInfo::~ObDirectLoadSampleInfo()
{
}

int ObDirectLoadSampleInfo::get_origin_info(ObDirectLoadOriginTable *origin_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(origin_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(origin_table));
  } else {
    if (nullptr != origin_table->get_major_sstable()) {
      ObSSTable *major_sstable = origin_table->get_major_sstable();
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(major_sstable->get_meta(meta_handle))) {
        LOG_WARN("fail to get sstable meta", K(ret), KPC(major_sstable));
      } else {
        const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
        origin_sample_num_ = basic_meta.data_macro_block_count_;
        origin_rows_per_sample_ = basic_meta.data_macro_block_count_ > 0
                                    ? basic_meta.row_count_ / basic_meta.data_macro_block_count_
                                    : 0;
      }
    } else {
      int64_t data_macro_block_count = 0;
      int64_t row_count = 0;
      const ObIArray<ObSSTable *> &ddl_sstables = origin_table->get_ddl_sstables();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
        ObSSTable *ddl_sstable = ddl_sstables.at(i);
        ObSSTableMetaHandle meta_handle;
        if (OB_FAIL(ddl_sstable->get_meta(meta_handle))) {
          LOG_WARN("fail to get sstable meta", K(ret), KPC(ddl_sstable));
        } else {
          const ObSSTableBasicMeta &basic_meta = meta_handle.get_sstable_meta().get_basic_meta();
          data_macro_block_count += basic_meta.data_macro_block_count_;
          row_count += basic_meta.row_count_;
        }
      }
      if (OB_SUCC(ret)) {
        origin_sample_num_ = data_macro_block_count;
        origin_rows_per_sample_ =
          data_macro_block_count > 0 ? row_count / data_macro_block_count : 0;
      }
    }
  }
  return ret;
}

int ObDirectLoadSampleInfo::get_sstable_info(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  if (sstable_array.empty()) {
    sstable_sample_num_ = 0;
    sstable_rows_per_sample_ = 0;
  } else {
    int64_t total_row_count = 0;
    int64_t total_rowkey_count = 0;
    int64_t total_index_block_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      const ObDirectLoadTableHandle &table_handle = sstable_array.at(i);
      if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_multiple_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected sstable", KR(ret), K(table_handle));
      } else {
        ObDirectLoadMultipleSSTable *sstable = static_cast<ObDirectLoadMultipleSSTable *>(table_handle.get_table());
        const ObDirectLoadMultipleSSTableMeta &meta = sstable->get_meta();
        total_row_count += meta.row_count_;
        total_rowkey_count += meta.rowkey_count_;
        total_index_block_count += meta.index_block_count_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (ObDirectLoadSampleMode::is_row_sample(table_data_desc.sample_mode_)) {
      // 行采样
      sstable_sample_num_ = total_rowkey_count;
      sstable_rows_per_sample_ = total_rowkey_count > 0 ? table_data_desc.num_per_sample_ : 0;
    } else if (ObDirectLoadSampleMode::is_data_block_sample(table_data_desc.sample_mode_)) {
      // 数据块采样
      sstable_sample_num_ = total_rowkey_count;
      sstable_rows_per_sample_ = total_rowkey_count > 0 ? total_row_count / total_rowkey_count : 0;
    } else {
      // 不采样
      sstable_sample_num_ = total_index_block_count;
      sstable_rows_per_sample_ =
        total_index_block_count > 0 ? total_row_count / total_index_block_count : 0;
    }
  }
  return ret;
}

int ObDirectLoadSampleInfo::calc_sample_info(const int64_t parallel, bool based_on_origin)
{
  int ret = OB_SUCCESS;
  const int64_t max_num = parallel * 8;
  if (origin_rows_per_sample_ == 0 || sstable_rows_per_sample_ == 0) {
    origin_step_ = 1;
    sstable_step_ = 1;
  }
  // 配平采样大小
  else if (based_on_origin || origin_rows_per_sample_ > sstable_rows_per_sample_) {
    origin_step_ = origin_sample_num_ <= max_num ? 1 : ((origin_sample_num_ + max_num - 1) / max_num);
    sstable_step_ = origin_rows_per_sample_ * origin_step_ / sstable_rows_per_sample_;
  } else {
    sstable_step_ = sstable_sample_num_ < max_num ? 1 : ((sstable_sample_num_ + max_num - 1) / max_num);
    origin_step_ = sstable_rows_per_sample_ * sstable_step_ / origin_rows_per_sample_;
  }
  return ret;
}

/**
 * ObDirectLoadMergeRangeSplitter
 */

ObDirectLoadMergeRangeSplitter::ObDirectLoadMergeRangeSplitter()
  : allocator_("TLD_MegRGSplit"), max_range_count_(0), total_sample_count_(0), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMergeRangeSplitter::~ObDirectLoadMergeRangeSplitter()
{
}

int ObDirectLoadMergeRangeSplitter::init(
  const ObTabletID &tablet_id,
  ObDirectLoadOriginTable *origin_table,
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  const ObIArray<ObColDesc> &col_descs,
  const int64_t max_range_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMergeRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(
               !tablet_id.is_valid() || (nullptr != origin_table && !origin_table->is_valid()) ||
               !table_data_desc.is_valid() || nullptr == datum_utils || max_range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KPC(origin_table), K(sstable_array),
             K(table_data_desc), KP(datum_utils), K(max_range_count));
  } else {
    tablet_id_ = tablet_id;
    col_descs_ = &col_descs;
    max_range_count_ = max_range_count;
    scan_range_.set_whole_range();
    if (max_range_count_ == 1) {
      // do nothing
    }
    // init sample_info_
    else if (nullptr != origin_table && OB_FAIL(sample_info_.get_origin_info(origin_table))) {
      LOG_WARN("fail to get origin info", KR(ret));
    } else if (OB_FAIL(sample_info_.get_sstable_info(sstable_array, table_data_desc))) {
      LOG_WARN("fail to get sstable info", KR(ret));
    } else if (OB_FAIL(sample_info_.calc_sample_info(max_range_count))) {
      LOG_WARN("fail to calc sample info", KR(ret));
    }
    // init compare_
    else if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init rowkey compare", KR(ret));
    }
    // init origin_rowkey_merger_
    else if (OB_FAIL(init_origin_rowkey_merger(origin_table))) {
      LOG_WARN("fail to init origin rowkey merger", KR(ret));
    }
    // init sstable_rowkey_merger_
    else if (OB_FAIL(init_sstable_rowkey_merger(sstable_array, table_data_desc))) {
      LOG_WARN("fail to init sstable rowkey merger", KR(ret));
    }
    // init rowkey_merger_
    else if (OB_FAIL(init_rowkey_merger())) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::init_origin_rowkey_merger(ObDirectLoadOriginTable *origin_table)
{
  int ret = OB_SUCCESS;
  iter_guard_.reuse();
  if (sample_info_.origin_sample_num_ == 0) {
  } else if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
               origin_table, scan_range_, allocator_, iter_guard_))) {
    LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
  } else if (OB_FAIL(origin_rowkey_merger_.init(iter_guard_.get_iters(), &compare_,
                                                sample_info_.origin_step_))) {
    LOG_WARN("fail to init merger", KR(ret));
  } else {
    total_sample_count_ += sample_info_.origin_sample_num_ / sample_info_.origin_step_;
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::init_sstable_rowkey_merger(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  iter_guard_.reuse();
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_sstable_rowkey_iters(
        sstable_array, table_data_desc, allocator_, iter_guard_))) {
    LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
  } else if (OB_FAIL(sstable_rowkey_merger_.init(iter_guard_.get_iters(), &compare_,
                                                 sample_info_.sstable_step_))) {
    LOG_WARN("fail to init merger", KR(ret));
  } else {
    total_sample_count_ += sample_info_.sstable_sample_num_ / sample_info_.sstable_step_;
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::init_rowkey_merger()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObIDirectLoadDatumRowkeyIterator *, 2> rowkey_iters;
  if (sample_info_.origin_sample_num_ != 0 &&
      OB_FAIL(rowkey_iters.push_back(&origin_rowkey_merger_))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(rowkey_iters.push_back(&sstable_rowkey_merger_))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(rowkey_merger_.init(rowkey_iters, &compare_))) {
    LOG_WARN("fail to init rowkey merger", KR(ret));
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::split_range(ObIArray<ObDatumRange> &range_array,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeRangeSplitter not init", KR(ret), KP(this));
  } else {
    range_array.reset();
    const int64_t range_count = MIN(total_sample_count_, max_range_count_);
    if (range_count <= 1) {
      ObDatumRange range;
      range.set_whole_range();
      if (OB_FAIL(range_array.push_back(range))) {
        LOG_WARN("fail to push back range", KR(ret));
      }
    } else {
      const int64_t block_count_per_range = total_sample_count_ / range_count;
      int64_t block_count_remainder = total_sample_count_ - block_count_per_range * range_count;
      int64_t block_count_cur_range = block_count_per_range;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      int64_t count = 0;
      const ObDatumRowkey *rowkey = nullptr;
      if (block_count_remainder > 0) {
        block_count_cur_range = block_count_per_range + 1;
        --block_count_remainder;
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_merger_.get_next_rowkey(rowkey))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rowkey", KR(ret));
          } else {
            ret = OB_SUCCESS;
            if (count > 0) {
              range.start_key_ = range.end_key_;
              range.end_key_.set_max_rowkey();
              range.set_right_open();
              if (OB_FAIL(range_array.push_back(range))) {
                LOG_WARN("fail to push back datum ranges", KR(ret));
              }
            } else {
              ObDatumRange &last_range = range_array.at(range_array.count() - 1);
              last_range.end_key_.set_max_rowkey();
              last_range.set_right_open();
            }
            break;
          }
        } else if (++count >= block_count_cur_range) {
          int cmp_ret = 0;
          if (OB_FAIL(compare_.compare(rowkey, &range.end_key_, cmp_ret))) {
            LOG_WARN("fail to compare rowkey", KR(ret));
          } else if (cmp_ret == 0) {
            // next rowkey
          } else {
            range.start_key_ = range.end_key_;
            if (OB_FAIL(rowkey->deep_copy(range.end_key_, allocator))) {
              LOG_WARN("fail to deep copy rowkey", KR(ret), K(rowkey));
            } else if (OB_FAIL(range_array.push_back(range))) {
              LOG_WARN("fail to push back datum ranges", KR(ret));
            } else {
              count = 0;
              if (block_count_remainder > 0) {
                block_count_cur_range = block_count_per_range + 1;
                --block_count_remainder;
              } else {
                block_count_cur_range = block_count_per_range;
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::prepare_ranges_memtable_readable(
          range_array, *col_descs_, allocator))) {
      LOG_WARN("fail to prepare ranges memtable readable", KR(ret));
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleMergeRangeSplitter
 */

ObDirectLoadMultipleMergeRangeSplitter::ObDirectLoadMultipleMergeRangeSplitter()
  : allocator_("TLD_MulMegRS"),
    datum_utils_(nullptr),
    col_descs_(nullptr),
    tablet_rowkey_iter_(rowkey_merger_),
    adaptive_sample_factor_(ObDirectLoadRangeSplitUtils::DEFAULT_ADAPTIVE_SAMPLE_FACTOR),
    reservoir_sample_num_(ObDirectLoadRangeSplitUtils::DEFAULT_RESERVOIR_SAMPLE_NUM),
    enable_reservoir_sample_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleMergeRangeSplitter::~ObDirectLoadMultipleMergeRangeSplitter()
{
}

int ObDirectLoadMultipleMergeRangeSplitter::init(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleMergeRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!table_data_desc.is_valid() || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_data_desc), KP(datum_utils), K(sstable_array));
  } else {
    if (OB_FAIL(sample_info_.get_sstable_info(sstable_array, table_data_desc))) {
      LOG_WARN("fail to get sstable info", KR(ret));
    } else if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(init_multiple_sstable_rowkey_merger(sstable_array, table_data_desc))) {
      LOG_WARN("fail to init multiple sstable rowkey merger", KR(ret));
    } else {
      datum_utils_ = datum_utils;
      col_descs_ = &col_descs;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::init_multiple_sstable_rowkey_merger(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  iter_guard_.reuse();
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_sstable_multiple_rowkey_iters(
        sstable_array, table_data_desc, allocator_, iter_guard_))) {
    LOG_WARN("fail to construct sstable rowkey itres", KR(ret));
  } else if (OB_FAIL(rowkey_merger_.init(iter_guard_.get_iters(), &compare_))) {
    LOG_WARN("fail to init rowkey merger", KR(ret));
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::get_rowkeys_by_origin(
  ObDirectLoadOriginTable *origin_table,
  ObArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDirectLoadDatumRowkeyIteratorGuard iter_guard;
  ObDirectLoadDatumRowkeyCompare compare;
  RowkeyMerger rowkey_merger;
  ObDatumRange scan_range;
  scan_range.set_whole_range();
  const ObDatumRowkey *datum_rowkey = nullptr;
  ObDatumRowkey copied_rowkey;
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
        origin_table, scan_range, allocator, iter_guard))) {
    LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
  } else if (OB_FAIL(compare.init(*datum_utils_))) {
    LOG_WARN("fail to init rowkey compare", KR(ret));
  } else if (OB_FAIL(rowkey_merger.init(iter_guard.get_iters(), &compare, sample_info_.origin_step_))) {
    LOG_WARN("fail to init rowkey merger", KR(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(rowkey_merger.get_next_rowkey(datum_rowkey))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(datum_rowkey->deep_copy(copied_rowkey, allocator))) {
      LOG_WARN("fail to deep copy rowkey", KR(ret));
    } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::get_rowkeys_by_sstable(
  const int64_t max_range_count,
  ObArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (sample_info_.sstable_step_ > 0) {
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::rowkey_fixed_sample(
          tablet_rowkey_iter_, sample_info_.sstable_step_, rowkey_array, allocator))) {
      LOG_WARN("fail to do rowkey fixed sample", KR(ret));
    }
  } else if (enable_reservoir_sample_) {
    // 蓄水池采样
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::rowkey_reservoir_sample(
          tablet_rowkey_iter_, reservoir_sample_num_, datum_utils_, rowkey_array, allocator))) {
      LOG_WARN("fail to do rowkey reservoir sample", KR(ret));
    }
  } else {
    // 自适应采样
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::rowkey_adaptive_sample(
          tablet_rowkey_iter_, max_range_count * adaptive_sample_factor_, rowkey_array,
          allocator))) {
      LOG_WARN("fail to do rowkey adaptive sample", KR(ret));
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::combine_final_ranges(
  const ObIArray<ObDatumRowkey> &rowkey_array1,
  const ObIArray<ObDatumRowkey> &rowkey_array2,
  int64_t max_range_count,
  ObIArray<ObDatumRange> &range_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (rowkey_array1.empty() && rowkey_array2.empty()) {
    ObDatumRange range;
    range.set_whole_range();
    if (OB_FAIL(range_array.push_back(range))) {
      LOG_WARN("fail to push back range", KR(ret));
    }
  } else {
    ObDirectLoadDatumRowkeyArrayIterator rowkey_range_iters[2];
    ObSEArray<ObIDirectLoadDatumRowkeyIterator *, 2> rowkey_iters;
    ObDirectLoadDatumRowkeyCompare compare;
    RowkeyMerger rowkey_merger;
    if (OB_FAIL(rowkey_range_iters[0].init(rowkey_array1))) {
      LOG_WARN("fail to init rowkey range iter", KR(ret));
    } else if (OB_FAIL(rowkey_range_iters[1].init(rowkey_array2))) {
      LOG_WARN("fail to init rowkey range iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters.push_back(&rowkey_range_iters[0]))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(rowkey_iters.push_back(&rowkey_range_iters[1]))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(compare.init(*datum_utils_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(rowkey_merger.init(rowkey_iters, &compare))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else {
      const int64_t rowkey_count_per_range =
        (rowkey_array1.count() + rowkey_array2.count()) / max_range_count;
      int64_t rowkey_count_remainder =
        (rowkey_array1.count() + rowkey_array2.count()) - rowkey_count_per_range * max_range_count;
      int64_t rowkey_count_cur_range = rowkey_count_per_range;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      const ObDatumRowkey *datum_rowkey = nullptr;
      int64_t count = 0;
      if (rowkey_count_remainder > 0) {
        rowkey_count_cur_range = rowkey_count_per_range + 1;
        --rowkey_count_remainder;
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_merger.get_next_rowkey(datum_rowkey))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rowkey", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (++count >= rowkey_count_cur_range) {
          int cmp_ret = 0;
          if (OB_FAIL(datum_rowkey->compare(range.end_key_, *datum_utils_, cmp_ret))) {
            LOG_WARN("fail to compare rowkey", KR(ret));
          } else if (OB_LIKELY(cmp_ret > 0)) {
            range.start_key_ = range.end_key_;
            if (OB_FAIL(datum_rowkey->deep_copy(range.end_key_, allocator))) {
              LOG_WARN("fail to deep copy rowkey", KR(ret));
            } else if (OB_FAIL(range_array.push_back(range))) {
              LOG_WARN("fail to push back range", KR(ret));
            } else {
              count = 0;
              if (rowkey_count_remainder > 0) {
                rowkey_count_cur_range = rowkey_count_per_range + 1;
                --rowkey_count_remainder;
              } else {
                rowkey_count_cur_range = rowkey_count_per_range;
              }
            }
          } else {
            abort_unless(0 == cmp_ret);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (count > 0 || range_array.empty()) {
          range.start_key_ = range.end_key_;
          range.end_key_.set_max_rowkey();
          range.set_right_open();
          if (OB_FAIL(range_array.push_back(range))) {
            LOG_WARN("fail to push back range", KR(ret));
          }
        } else {
          ObDatumRange &last_range = range_array.at(range_array.count() - 1);
          last_range.end_key_.set_max_rowkey();
          last_range.set_right_open();
        }
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::split_range(const ObTabletID &tablet_id,
                                                        ObDirectLoadOriginTable *origin_table,
                                                        int64_t max_range_count,
                                                        ObIArray<ObDatumRange> &range_array,
                                                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  range_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleMergeRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(
      !tablet_id.is_valid() || (nullptr != origin_table && !origin_table->is_valid()) || max_range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KPC(origin_table), K(max_range_count));
  } else if (max_range_count == 1) {
    ObDatumRange range;
    range.set_whole_range();
    if (OB_FAIL(range_array.push_back(range))) {
      LOG_WARN("fail to push back range", KR(ret));
    }
  } else if (OB_FAIL(tablet_rowkey_iter_.set_next_tablet_id(tablet_id))) {
    LOG_WARN("fail to set next tablet id", KR(ret), K(tablet_id));
  } else if (nullptr != origin_table && OB_FAIL(sample_info_.get_origin_info(origin_table))) {
    LOG_WARN("fail to get origin info", KR(ret));
  } else {
    ObArenaAllocator tmp_allocator("MTL_MMRS_Tmp");
    ObArray<ObDatumRowkey> origin_rowkey_array;
    ObArray<ObDatumRowkey> sstable_rowkey_array;
    tmp_allocator.set_tenant_id(MTL_ID());
    origin_rowkey_array.set_block_allocator(ModulePageAllocator(tmp_allocator));
    sstable_rowkey_array.set_block_allocator(ModulePageAllocator(tmp_allocator));
    if (sample_info_.origin_sample_num_ > 0) {
      if (OB_FAIL(sample_info_.calc_sample_info(max_range_count, true /*based_on_origin*/))) {
        LOG_WARN("fail to calc sample info", KR(ret));
      } else if (OB_FAIL(get_rowkeys_by_origin(origin_table, origin_rowkey_array, tmp_allocator))) {
        LOG_WARN("fail to get rowkeys by origin", KR(ret));
      }
    } else {
      sample_info_.sstable_step_ = 0;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                 get_rowkeys_by_sstable(max_range_count, sstable_rowkey_array, tmp_allocator))) {
      LOG_WARN("fail to get rowkeys by sstable", KR(ret));
    } else if (OB_FAIL(combine_final_ranges(origin_rowkey_array, sstable_rowkey_array,
                                            max_range_count, range_array, allocator))) {
      LOG_WARN("fail to combine final range", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::prepare_ranges_memtable_readable(
          range_array, *col_descs_, allocator))) {
      LOG_WARN("fail to prepare ranges memtable readable", KR(ret));
    }
  }
  return ret;
}

/**
 * TabletRowkeyIter
 */

ObDirectLoadMultipleMergeRangeSplitter::TabletRowkeyIter::TabletRowkeyIter(
  ObIDirectLoadMultipleDatumRowkeyIterator &multiple_rowkey_iter)
  : multiple_rowkey_iter_(multiple_rowkey_iter), last_multiple_rowkey_(nullptr), iter_end_(false)
{
}

int ObDirectLoadMultipleMergeRangeSplitter::TabletRowkeyIter::set_next_tablet_id(
  const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet id invalid", KR(ret), K(tablet_id));
  } else if (tablet_id_.is_valid() && OB_UNLIKELY(tablet_id_.compare(tablet_id) >= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet id", KR(ret), K(tablet_id_), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::TabletRowkeyIter::get_next_rowkey(
  const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else {
    if (nullptr == last_multiple_rowkey_ &&
        OB_FAIL(multiple_rowkey_iter_.get_next_rowkey(last_multiple_rowkey_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        iter_end_ = true;
      }
    } else if (OB_ISNULL(last_multiple_rowkey_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected last multiple rowkey is null", KR(ret));
    } else if (OB_UNLIKELY(!tablet_id_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet id invalid", KR(ret));
    } else {
      const int cmp_ret = last_multiple_rowkey_->tablet_id_.compare(tablet_id_);
      // 必须按tablet_id顺序迭代
      if (OB_UNLIKELY(cmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet id", KR(ret), KPC(last_multiple_rowkey_), K(tablet_id_));
      } else if (cmp_ret > 0) {
        // no rowkey in current tablet_id
        ret = OB_ITER_END;
      } else if (OB_FAIL(last_multiple_rowkey_->get_rowkey(rowkey_))) {
        LOG_WARN("fail to get rowkey", KR(ret));
      } else {
        rowkey = &rowkey_;
        last_multiple_rowkey_ = nullptr;
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableRangeSplitter
 */

ObDirectLoadMultipleSSTableRangeSplitter::ObDirectLoadMultipleSSTableRangeSplitter()
  : allocator_("TLD_MulSSTRS"), is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObDirectLoadMultipleSSTableRangeSplitter::~ObDirectLoadMultipleSSTableRangeSplitter()
{
}

int ObDirectLoadMultipleSSTableRangeSplitter::init(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleSSTableRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(sstable_array.empty() || !table_data_desc.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(sstable_array), K(table_data_desc), KP(datum_utils));
  } else {
    if (OB_FAIL(sample_info_.get_sstable_info(sstable_array, table_data_desc))) {
      LOG_WARN("fail to get sstable info", KR(ret));
    } else if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(init_multiple_sstable_rowkey_merger(sstable_array, table_data_desc))) {
      LOG_WARN("fail to init multiple sstable rowkey merger", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableRangeSplitter::init_multiple_sstable_rowkey_merger(
  const ObDirectLoadTableHandleArray &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc)
{
  int ret = OB_SUCCESS;
  iter_guard_.reuse();
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_sstable_multiple_rowkey_iters(
        sstable_array, table_data_desc, allocator_, iter_guard_))) {
    LOG_WARN("fail to construct sstable rowkey itres", KR(ret));
  } else if (OB_FAIL(rowkey_merger_.init(iter_guard_.get_iters(), &compare_))) {
    LOG_WARN("fail to init rowkey merger", KR(ret));
  }
  return ret;
}

int ObDirectLoadMultipleSSTableRangeSplitter::split_range(
  ObIArray<ObDirectLoadMultipleDatumRange> &range_array,
  int64_t max_range_count,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleSSTableRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_range_count));
  } else {
    range_array.reset();
    const int64_t range_count = MIN(sample_info_.sstable_sample_num_, max_range_count);
    if (range_count <= 1) {
      ObDirectLoadMultipleDatumRange range;
      range.set_whole_range();
      if (OB_FAIL(range_array.push_back(range))) {
        LOG_WARN("fail to push back range", KR(ret));
      }
    } else {
      const int64_t block_count_per_range = sample_info_.sstable_sample_num_ / range_count;
      int64_t block_count_remainder = sample_info_.sstable_sample_num_ - block_count_per_range * range_count;
      int64_t block_count_cur_range = block_count_per_range;
      ObDirectLoadMultipleDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      int64_t count = 0;
      const ObDirectLoadMultipleDatumRowkey *rowkey = nullptr;
      if (block_count_remainder > 0) {
        block_count_cur_range = block_count_per_range + 1;
        --block_count_remainder;
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_merger_.get_next_rowkey(rowkey))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rowkey", KR(ret));
          } else {
            ret = OB_SUCCESS;
            if (count > 0) {
              range.start_key_ = range.end_key_;
              range.end_key_.set_max_rowkey();
              range.set_right_open();
              if (OB_FAIL(range_array.push_back(range))) {
                LOG_WARN("fail to push back datum ranges", KR(ret));
              }
            } else {
              ObDirectLoadMultipleDatumRange &last_range = range_array.at(range_array.count() - 1);
              last_range.end_key_.set_max_rowkey();
              last_range.set_right_open();
            }
            break;
          }
        } else if (++count >= block_count_cur_range) {
          int cmp_ret = 0;
          if (OB_FAIL(compare_.compare(*rowkey, range.end_key_, cmp_ret))) {
            LOG_WARN("fail to compare euqal rowkey", KR(ret));
          } else if (cmp_ret == 0) {
            // next rowkey
          } else {
            range.start_key_ = range.end_key_;
            if (OB_FAIL(range.end_key_.deep_copy(*rowkey, allocator))) {
              LOG_WARN("fail to deep copy rowkey", KR(ret), K(rowkey));
            } else if (OB_FAIL(range_array.push_back(range))) {
              LOG_WARN("fail to push back datum ranges", KR(ret));
            } else {
              count = 0;
              if (block_count_remainder > 0) {
                block_count_cur_range = block_count_per_range + 1;
                --block_count_remainder;
              } else {
                block_count_cur_range = block_count_per_range;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
