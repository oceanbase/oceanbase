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
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable.h"
#include "storage/direct_load/ob_direct_load_multiple_sstable_index_block_meta_scanner.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_sstable.h"
#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
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
  ObDirectLoadSSTable *sstable,
  ObIAllocator &allocator,
  ObIDirectLoadDatumRowkeyIterator *&rowkey_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable));
  } else {
    ObDirectLoadIndexBlockMetaIterator *index_block_meta_iter = nullptr;
    ObDirectLoadIndexBlockEndKeyIterator *end_key_iter = nullptr;
    if (OB_FAIL(sstable->scan_index_block_meta(allocator, index_block_meta_iter))) {
      LOG_WARN("fail to scan index block meta", KR(ret));
    } else if (OB_ISNULL(end_key_iter =
                           OB_NEWx(ObDirectLoadIndexBlockEndKeyIterator, (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadIndexBlockEndKeyIterator", KR(ret));
    } else if (OB_FAIL(end_key_iter->init(index_block_meta_iter))) {
      LOG_WARN("fail to init end key iter", KR(ret));
    } else {
      rowkey_iter = end_key_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != index_block_meta_iter) {
        index_block_meta_iter->~ObDirectLoadIndexBlockMetaIterator();
        allocator.free(index_block_meta_iter);
        index_block_meta_iter = nullptr;
      }
      if (nullptr != end_key_iter) {
        end_key_iter->~ObDirectLoadIndexBlockEndKeyIterator();
        allocator.free(end_key_iter);
        end_key_iter = nullptr;
      }
    }
  }
  return ret;
}

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
  const ObTabletID &tablet_id,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  ObIAllocator &allocator,
  ObIDirectLoadDatumRowkeyIterator *&rowkey_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable || !sstable->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(sstable));
  } else {
    ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter = nullptr;
    ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator *end_key_iter = nullptr;
    if (OB_FAIL(sstable->scan_tablet_whole_index_block_meta(tablet_id, table_data_desc, datum_utils,
                                                            allocator, index_block_meta_iter))) {
      LOG_WARN("fail to scan index block meta", KR(ret));
    } else if (OB_ISNULL(end_key_iter =
                           OB_NEWx(ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator,
                                   (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator", KR(ret));
    } else if (OB_FAIL(end_key_iter->init(index_block_meta_iter))) {
      LOG_WARN("fail to init end key iter", KR(ret));
    } else {
      rowkey_iter = end_key_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != index_block_meta_iter) {
        index_block_meta_iter->~ObDirectLoadMultipleSSTableIndexBlockMetaIterator();
        allocator.free(index_block_meta_iter);
        index_block_meta_iter = nullptr;
      }
      if (nullptr != end_key_iter) {
        end_key_iter->~ObDirectLoadMultipleSSTableIndexBlockTabletEndKeyIterator();
        allocator.free(end_key_iter);
        end_key_iter = nullptr;
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
    ObDirectLoadMultipleSSTableIndexBlockMetaIterator *index_block_meta_iter = nullptr;
    ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator *end_key_iter = nullptr;
    if (OB_FAIL(sstable->scan_whole_index_block_meta(table_data_desc, allocator,
                                                     index_block_meta_iter))) {
      LOG_WARN("fail to scan index block meta", KR(ret));
    } else if (OB_ISNULL(end_key_iter = OB_NEWx(ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator,
                                                (&allocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator", KR(ret));
    } else if (OB_FAIL(end_key_iter->init(index_block_meta_iter))) {
      LOG_WARN("fail to init end key iter", KR(ret));
    } else {
      rowkey_iter = end_key_iter;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != index_block_meta_iter) {
        index_block_meta_iter->~ObDirectLoadMultipleSSTableIndexBlockMetaIterator();
        allocator.free(index_block_meta_iter);
        index_block_meta_iter = nullptr;
      }
      if (nullptr != end_key_iter) {
        end_key_iter->~ObDirectLoadMultipleSSTableIndexBlockEndKeyIterator();
        allocator.free(end_key_iter);
        end_key_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
  ObDirectLoadOriginTable *origin_table,
  const ObDatumRange &scan_range,
  ObIAllocator &allocator,
  int64_t &total_block_count,
  ObIArray<ObIDirectLoadDatumRowkeyIterator *> &rowkey_iters)
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
      } else if (OB_FAIL(rowkey_iters.push_back(rowkey_iter))) {
        LOG_WARN("fail to push back rowkey iter", KR(ret));
      } else {
        total_block_count += major_sstable->get_data_macro_block_count();
      }
      if (OB_FAIL(ret)) {
        if (nullptr != rowkey_iter) {
          rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
          allocator.free(rowkey_iter);
          rowkey_iter = nullptr;
        }
      }
    } else {
      const ObIArray<blocksstable::ObSSTable *> &ddl_sstables = origin_table->get_ddl_sstables();
      for (int64_t i = 0; OB_SUCC(ret) && i < ddl_sstables.count(); ++i) {
        ObSSTable *ddl_sstable = ddl_sstables.at(i);
        ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
        if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
              ddl_sstable, scan_range, read_info, allocator, rowkey_iter))) {
          LOG_WARN("fail to construct rowkey iter", KR(ret));
        } else if (OB_FAIL(rowkey_iters.push_back(rowkey_iter))) {
          LOG_WARN("fail to push back rowkey iter", KR(ret));
        } else {
          total_block_count += ddl_sstable->get_data_macro_block_count();
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

/**
 * ObDirectLoadRowkeyMergeRangeSplitter
 */

ObDirectLoadRowkeyMergeRangeSplitter::ObDirectLoadRowkeyMergeRangeSplitter()
  : total_rowkey_count_(0),
    datum_utils_(nullptr),
    is_memtable_valid_(false),
    col_descs_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadRowkeyMergeRangeSplitter::~ObDirectLoadRowkeyMergeRangeSplitter()
{
}

int ObDirectLoadRowkeyMergeRangeSplitter::init(
  const ObIArray<ObIDirectLoadDatumRowkeyIterator *> &rowkey_iters,
  int64_t total_rowkey_count,
  const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadRowkeyMergeRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_iters.empty() || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_iters.count()), K(total_rowkey_count),
             KP(datum_utils));
  } else {
    if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init rowkey compare", KR(ret));
    } else if (OB_FAIL(rowkey_merger_.init(rowkey_iters, &compare_))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else {
      total_rowkey_count_ = total_rowkey_count;
      datum_utils_ = datum_utils;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadRowkeyMergeRangeSplitter::set_memtable_valid(const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadRowkeyMergeRangeSplitter not init", KR(ret), KP(this));
  } else {
    col_descs_ = &col_descs;
    is_memtable_valid_ = true;
  }
  return ret;
}

int ObDirectLoadRowkeyMergeRangeSplitter::check_range_memtable_valid(ObDatumRange &range,
                                                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_memtable_valid_) {
    if (!range.start_key_.is_memtable_valid() &&
        OB_FAIL(range.start_key_.prepare_memtable_readable(*col_descs_, allocator))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
    } else if (!range.end_key_.is_memtable_valid() &&
               OB_FAIL(range.end_key_.prepare_memtable_readable(*col_descs_, allocator))) {
      LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
    }
  }
  return ret;
}

int ObDirectLoadRowkeyMergeRangeSplitter::split_range(ObIArray<ObDatumRange> &range_array,
                                                      int64_t max_range_count,
                                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadRowkeyMergeRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(max_range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(max_range_count));
  } else {
    range_array.reset();
    const int64_t range_count = MIN(total_rowkey_count_, max_range_count);
    if (range_count > 1) {
      const int64_t block_count_per_range = (total_rowkey_count_ + range_count - 1) / range_count;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      int64_t count = 0;
      const ObDatumRowkey *rowkey = nullptr;
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
              if (OB_FAIL(check_range_memtable_valid(range, allocator))) {
                LOG_WARN("fail to check range memtable valid", KR(ret));
              } else if (OB_FAIL(range_array.push_back(range))) {
                LOG_WARN("fail to push back datum ranges", KR(ret));
              }
            } else {
              ObDatumRange &last_range = range_array.at(range_array.count() - 1);
              last_range.end_key_.set_max_rowkey();
              last_range.set_right_open();
            }
            break;
          }
        } else if (++count >= block_count_per_range) {
          bool rowkey_equal = false;
          if (OB_FAIL(rowkey->equal(range.end_key_, *datum_utils_, rowkey_equal))) {
            LOG_WARN("fail to compare euqal rowkey", KR(ret));
          } else if (rowkey_equal) {
            // next rowkey
          } else {
            range.start_key_ = range.end_key_;
            if (OB_FAIL(rowkey->deep_copy(range.end_key_, allocator))) {
              LOG_WARN("fail to deep copy rowkey", KR(ret), K(rowkey));
            } else if (OB_FAIL(check_range_memtable_valid(range, allocator))) {
              LOG_WARN("fail to check range memtable valid", KR(ret));
            } else if (OB_FAIL(range_array.push_back(range))) {
              LOG_WARN("fail to push back datum ranges", KR(ret));
            } else {
              count = 0;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && range_array.empty()) {
      ObDatumRange range;
      range.set_whole_range();
      if (OB_FAIL(check_range_memtable_valid(range, allocator))) {
        LOG_WARN("fail to check range memtable valid", KR(ret));
      } else if (OB_FAIL(range_array.push_back(range))) {
        LOG_WARN("fail to push back range", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObDirectLoadSSTableRangeSplitter
 */

ObDirectLoadSSTableRangeSplitter::ObDirectLoadSSTableRangeSplitter()
  : allocator_("TLD_SSTRGSplit"), total_block_count_(0), is_inited_(false)
{
}

ObDirectLoadSSTableRangeSplitter::~ObDirectLoadSSTableRangeSplitter()
{
  for (int64_t i = 0; i < rowkey_iters_.count(); ++i) {
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = rowkey_iters_.at(i);
    rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
    allocator_.free(rowkey_iter);
  }
  rowkey_iters_.reset();
}

int ObDirectLoadSSTableRangeSplitter::init(const ObIArray<ObDirectLoadSSTable *> &sstable_array,
                                           const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadSSTableRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(sstable_array.empty() || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(sstable_array), KP(datum_utils));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(construct_rowkey_iters(sstable_array))) {
      LOG_WARN("fail to construct rowkey itres", KR(ret));
    } else if (OB_FAIL(
                 rowkey_merge_splitter_.init(rowkey_iters_, total_block_count_, datum_utils))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadSSTableRangeSplitter::construct_rowkey_iters(
  const ObIArray<ObDirectLoadSSTable *> &sstable_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObDirectLoadSSTable *sstable = sstable_array.at(i);
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
    total_block_count_ += sstable->get_meta().index_block_count_;
    if (OB_FAIL(
          ObDirectLoadRangeSplitUtils::construct_rowkey_iter(sstable, allocator_, rowkey_iter))) {
      LOG_WARN("fail to construct rowkey iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters_.push_back(rowkey_iter))) {
      LOG_WARN("fail to push back rowkey iter", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rowkey_iter) {
        rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
        allocator_.free(rowkey_iter);
        rowkey_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadSSTableRangeSplitter::split_range(ObIArray<ObDatumRange> &range_array,
                                                  int64_t max_range_count,
                                                  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadSSTableRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_FAIL(rowkey_merge_splitter_.split_range(range_array, max_range_count, allocator))) {
    LOG_WARN("fail to split range", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadMergeRangeSplitter
 */

ObDirectLoadMergeRangeSplitter::ObDirectLoadMergeRangeSplitter()
  : allocator_("TLD_MegRGSplit"), total_block_count_(0), is_inited_(false)
{
}

ObDirectLoadMergeRangeSplitter::~ObDirectLoadMergeRangeSplitter()
{
  for (int64_t i = 0; i < rowkey_iters_.count(); ++i) {
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = rowkey_iters_.at(i);
    rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
    allocator_.free(rowkey_iter);
  }
  rowkey_iters_.reset();
}

int ObDirectLoadMergeRangeSplitter::init(ObDirectLoadOriginTable *origin_table,
                                         const ObIArray<ObDirectLoadSSTable *> &sstable_array,
                                         const ObStorageDatumUtils *datum_utils,
                                         const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMergeRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == origin_table || !origin_table->is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(origin_table), K(sstable_array), KP(datum_utils));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    scan_range_.set_whole_range();
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
          origin_table, scan_range_, allocator_, total_block_count_, rowkey_iters_))) {
      LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
    } else if (OB_FAIL(construct_sstable_rowkey_iters(sstable_array))) {
      LOG_WARN("fail to construct sstable rowkey itres", KR(ret));
    } else if (OB_FAIL(
                 rowkey_merge_splitter_.init(rowkey_iters_, total_block_count_, datum_utils))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else if (OB_FAIL(rowkey_merge_splitter_.set_memtable_valid(col_descs))) {
      LOG_WARN("fail to set memtable valid", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::construct_sstable_rowkey_iters(
  const ObIArray<ObDirectLoadSSTable *> &sstable_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObDirectLoadSSTable *sstable = sstable_array.at(i);
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
    if (OB_FAIL(
          ObDirectLoadRangeSplitUtils::construct_rowkey_iter(sstable, allocator_, rowkey_iter))) {
      LOG_WARN("fail to construct rowkey iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters_.push_back(rowkey_iter))) {
      LOG_WARN("fail to push back rowkey iter", KR(ret));
    } else {
      total_block_count_ += sstable->get_meta().index_block_count_;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rowkey_iter) {
        rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
        allocator_.free(rowkey_iter);
        rowkey_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMergeRangeSplitter::split_range(ObIArray<ObDatumRange> &range_array,
                                                int64_t max_range_count,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMergeRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_FAIL(rowkey_merge_splitter_.split_range(range_array, max_range_count, allocator))) {
    LOG_WARN("fail to split range", KR(ret));
  }
  return ret;
}

/**
 * ObDirectLoadMultipleMergeTabletRangeSplitter
 */

ObDirectLoadMultipleMergeTabletRangeSplitter::ObDirectLoadMultipleMergeTabletRangeSplitter()
  : allocator_("TLD_MulMegTRS"), total_block_count_(0), is_inited_(false)
{
}

ObDirectLoadMultipleMergeTabletRangeSplitter::~ObDirectLoadMultipleMergeTabletRangeSplitter()
{
  for (int64_t i = 0; i < rowkey_iters_.count(); ++i) {
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = rowkey_iters_.at(i);
    rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
    allocator_.free(rowkey_iter);
  }
  rowkey_iters_.reset();
}

int ObDirectLoadMultipleMergeTabletRangeSplitter::init(
  const ObTabletID &tablet_id,
  ObDirectLoadOriginTable *origin_table,
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleMergeTabletRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == origin_table ||
                         !origin_table->is_valid() || !table_data_desc.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KPC(origin_table), K(sstable_array),
             K(table_data_desc), KP(datum_utils));
  } else {
    tablet_id_ = tablet_id;
    allocator_.set_tenant_id(MTL_ID());
    scan_range_.set_whole_range();
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
          origin_table, scan_range_, allocator_, total_block_count_, rowkey_iters_))) {
      LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
    } else if (OB_FAIL(
                 construct_sstable_rowkey_iters(sstable_array, table_data_desc, datum_utils))) {
      LOG_WARN("fail to construct sstable rowkey itres", KR(ret));
    } else if (OB_FAIL(
                 rowkey_merge_splitter_.init(rowkey_iters_, total_block_count_, datum_utils))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else if (OB_FAIL(rowkey_merge_splitter_.set_memtable_valid(col_descs))) {
      LOG_WARN("fail to set memtable valid", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeTabletRangeSplitter::construct_sstable_rowkey_iters(
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObDirectLoadMultipleSSTable *sstable = sstable_array.at(i);
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
    int64_t block_count = 0;
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
          sstable, tablet_id_, table_data_desc, datum_utils, allocator_, rowkey_iter))) {
      LOG_WARN("fail to construct rowkey iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters_.push_back(rowkey_iter))) {
      LOG_WARN("fail to push back rowkey iter", KR(ret));
    } else if (OB_FAIL(get_sstable_index_block_count(sstable, table_data_desc, datum_utils,
                                                     block_count))) {
      LOG_WARN("fail to get sstable index block count", KR(ret));
    } else {
      total_block_count_ += block_count;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rowkey_iter) {
        rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
        allocator_.free(rowkey_iter);
        rowkey_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeTabletRangeSplitter::get_sstable_index_block_count(
  ObDirectLoadMultipleSSTable *sstable,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  int64_t &block_count)
{
  int ret = OB_SUCCESS;
  block_count = 0;
  const ObDatumRowkey *rowkey = nullptr;
  ObIDirectLoadDatumRowkeyIterator *rowkey_iter = nullptr;
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_rowkey_iter(
        sstable, tablet_id_, table_data_desc, datum_utils, allocator_, rowkey_iter))) {
    LOG_WARN("fail to construct rowkey iter", KR(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(rowkey_iter->get_next_rowkey(rowkey))) {
      if (OB_FAIL(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next rowkey", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      ++block_count;
    }
  }
  if (nullptr != rowkey_iter) {
    rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
    rowkey_iter = nullptr;
  }
  return ret;
}

int ObDirectLoadMultipleMergeTabletRangeSplitter::split_range(ObIArray<ObDatumRange> &range_array,
                                                              int64_t max_range_count,
                                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMultipleMergeTabletRangeSplitter not init", KR(ret), KP(this));
  } else if (OB_FAIL(rowkey_merge_splitter_.split_range(range_array, max_range_count, allocator))) {
    LOG_WARN("fail to split range", KR(ret));
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
    last_rowkey_(nullptr),
    is_inited_(false)
{
}

ObDirectLoadMultipleMergeRangeSplitter::~ObDirectLoadMultipleMergeRangeSplitter()
{
  for (int64_t i = 0; i < rowkey_iters_.count(); ++i) {
    ObIDirectLoadMultipleDatumRowkeyIterator *rowkey_iter = rowkey_iters_.at(i);
    rowkey_iter->~ObIDirectLoadMultipleDatumRowkeyIterator();
    allocator_.free(rowkey_iter);
  }
  rowkey_iters_.reset();
}

int ObDirectLoadMultipleMergeRangeSplitter::init(
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils,
  const ObIArray<ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMultipleMergeRangeSplitter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(sstable_array.empty() || !table_data_desc.is_valid() ||
                         nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(sstable_array), K(table_data_desc), KP(datum_utils));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(construct_rowkey_iters(sstable_array, table_data_desc, datum_utils))) {
      LOG_WARN("fail to construct sstable rowkey itres", KR(ret));
    } else if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(rowkey_merger_.init(rowkey_iters_, &compare_))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else {
      datum_utils_ = datum_utils;
      col_descs_ = &col_descs;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::construct_rowkey_iters(
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObDirectLoadMultipleSSTable *sstable = sstable_array.at(i);
    ObIDirectLoadMultipleDatumRowkeyIterator *rowkey_iter = nullptr;
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_multiple_rowkey_iter(
          sstable, table_data_desc, allocator_, rowkey_iter))) {
      LOG_WARN("fail to construct rowkey iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters_.push_back(rowkey_iter))) {
      LOG_WARN("fail to push back rowkey iter", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rowkey_iter) {
        rowkey_iter->~ObIDirectLoadMultipleDatumRowkeyIterator();
        allocator_.free(rowkey_iter);
        rowkey_iter = nullptr;
      }
    }
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::prepare_range_memtable_readable(
  ObDatumRange &range, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!range.start_key_.is_memtable_valid() &&
      OB_FAIL(range.start_key_.prepare_memtable_readable(*col_descs_, allocator))) {
    LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
  } else if (!range.end_key_.is_memtable_valid() &&
             OB_FAIL(range.end_key_.prepare_memtable_readable(*col_descs_, allocator))) {
    LOG_WARN("fail to prepare memtable readable", KR(ret), K(range));
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::get_rowkeys_by_origin(
  ObDirectLoadOriginTable *origin_table,
  ObIArray<ObDatumRowkey> &rowkey_array,
  ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDirectLoadDatumRowkeyCompare compare;
  RowkeyMerger rowkey_merger;
  ObDatumRange scan_range;
  scan_range.set_whole_range();
  ObArray<ObIDirectLoadDatumRowkeyIterator *> rowkey_iters;
  int64_t unused_total_block_count = 0;
  const ObDatumRowkey *datum_rowkey = nullptr;
  ObDatumRowkey copied_rowkey;
  int64_t count = 0;
  if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_origin_table_rowkey_iters(
        origin_table, scan_range, allocator, unused_total_block_count, rowkey_iters))) {
    LOG_WARN("fail to construct origin table rowkey iters", KR(ret));
  } else if (OB_FAIL(compare.init(*datum_utils_))) {
    LOG_WARN("fail to init rowkey compare", KR(ret));
  } else if (OB_FAIL(rowkey_merger.init(rowkey_iters, &compare))) {
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
    } else if (++count >= BLOCK_COUNT_PER_RANGE) {
      if (OB_FAIL(datum_rowkey->deep_copy(copied_rowkey, allocator))) {
        LOG_WARN("fail to deep copy rowkey", KR(ret));
      } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
        LOG_WARN("fail to push back", KR(ret));
      } else {
        count = 0;
      }
    }
  }
  for (int64_t i = 0; i < rowkey_iters.count(); ++i) {
    ObIDirectLoadDatumRowkeyIterator *rowkey_iter = rowkey_iters.at(i);
    rowkey_iter->~ObIDirectLoadDatumRowkeyIterator();
    allocator.free(rowkey_iter);
    rowkey_iter = nullptr;
  }
  return ret;
}

int ObDirectLoadMultipleMergeRangeSplitter::get_rowkeys_by_multiple(
  ObTabletID &tablet_id, ObIArray<ObDatumRowkey> &rowkey_array, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (nullptr == last_rowkey_ && OB_FAIL(rowkey_merger_.get_next_rowkey(last_rowkey_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next rowkey", KR(ret));
    } else {
      ret = OB_SUCCESS;
      last_rowkey_ = nullptr;
    }
  }
  if (OB_SUCC(ret) && nullptr != last_rowkey_) {
    if (last_rowkey_->tablet_id_.compare(tablet_id) == 0) {
      // the same tablet
      ObDatumRowkey rowkey;
      ObDatumRowkey copied_rowkey;
      int count = 0;
      while (OB_SUCC(ret)) {
        if (++count >= BLOCK_COUNT_PER_RANGE) {
          if (OB_FAIL(last_rowkey_->get_rowkey(rowkey))) {
            LOG_WARN("fail to get rowkey", KR(ret));
          } else if (OB_FAIL(rowkey.deep_copy(copied_rowkey, allocator))) {
            LOG_WARN("fail to deep copy rowkey", KR(ret));
          } else if (OB_FAIL(rowkey_array.push_back(copied_rowkey))) {
            LOG_WARN("fail to push back", KR(ret));
          } else {
            count = 0;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(rowkey_merger_.get_next_rowkey(last_rowkey_))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next rowkey", KR(ret));
            } else {
              ret = OB_SUCCESS;
              last_rowkey_ = nullptr;
              break;
            }
          } else if (last_rowkey_->tablet_id_.compare(tablet_id) != 0) {
            break;
          }
        }
      }
    } else {
      // the different tablet
      abort_unless(last_rowkey_->tablet_id_.compare(tablet_id) > 0);
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
    if (OB_FAIL(prepare_range_memtable_readable(range, allocator))) {
      LOG_WARN("fail to prepare range memtable readable", KR(ret));
    } else if (OB_FAIL(range_array.push_back(range))) {
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
        (rowkey_array1.count() + rowkey_array2.count() + max_range_count - 1) / max_range_count;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      const ObDatumRowkey *datum_rowkey = nullptr;
      int64_t count = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(rowkey_merger.get_next_rowkey(datum_rowkey))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next rowkey", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (++count >= rowkey_count_per_range) {
          int cmp_ret = 0;
          if (OB_FAIL(datum_rowkey->compare(range.end_key_, *datum_utils_, cmp_ret))) {
            LOG_WARN("fail to compare rowkey", KR(ret));
          } else if (OB_LIKELY(cmp_ret > 0)) {
            range.start_key_ = range.end_key_;
            if (OB_FAIL(datum_rowkey->deep_copy(range.end_key_, allocator))) {
              LOG_WARN("fail to deep copy rowkey", KR(ret));
            } else if (OB_FAIL(prepare_range_memtable_readable(range, allocator))) {
              LOG_WARN("fail to prepare range memtable readable", KR(ret));
            } else if (OB_FAIL(range_array.push_back(range))) {
              LOG_WARN("fail to push back range", KR(ret));
            } else {
              count = 0;
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
          if (OB_FAIL(prepare_range_memtable_readable(range, allocator))) {
            LOG_WARN("fail to prepare range memtable readable", KR(ret));
          } else if (OB_FAIL(range_array.push_back(range))) {
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

int ObDirectLoadMultipleMergeRangeSplitter::split_range(ObTabletID &tablet_id,
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
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || nullptr == origin_table ||
                         !origin_table->is_valid() || max_range_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_id), KPC(origin_table), K(max_range_count));
  } else if (last_tablet_id_.is_valid() && OB_UNLIKELY(last_tablet_id_.compare(tablet_id) >= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet id", KR(ret), K(tablet_id), K(last_tablet_id_));
  } else {
    ObArenaAllocator tmp_allocator("MTL_MulMRS_Tmp");
    ObArray<ObDatumRowkey> origin_rowkey_array;
    ObArray<ObDatumRowkey> multiple_rowkey_array;
    tmp_allocator.set_tenant_id(MTL_ID());
    if (OB_FAIL(get_rowkeys_by_origin(origin_table, origin_rowkey_array, tmp_allocator))) {
      LOG_WARN("fail to get rowkeys by origin", KR(ret));
    } else if (OB_FAIL(get_rowkeys_by_multiple(tablet_id, multiple_rowkey_array, tmp_allocator))) {
      LOG_WARN("fail to get rowkeys by multiple", KR(ret));
    } else if (OB_FAIL(combine_final_ranges(origin_rowkey_array, multiple_rowkey_array,
                                            max_range_count, range_array, allocator))) {
      LOG_WARN("fail to combine final range", KR(ret));
    } else {
      last_tablet_id_ = tablet_id;
    }
  }
  return ret;
}

/**
 * ObDirectLoadMultipleSSTableRangeSplitter
 */

ObDirectLoadMultipleSSTableRangeSplitter::ObDirectLoadMultipleSSTableRangeSplitter()
  : allocator_("TLD_MulSSTRS"), datum_utils_(nullptr), total_block_count_(0), is_inited_(false)
{
}

ObDirectLoadMultipleSSTableRangeSplitter::~ObDirectLoadMultipleSSTableRangeSplitter()
{
  for (int64_t i = 0; i < rowkey_iters_.count(); ++i) {
    ObIDirectLoadMultipleDatumRowkeyIterator *rowkey_iter = rowkey_iters_.at(i);
    rowkey_iter->~ObIDirectLoadMultipleDatumRowkeyIterator();
    allocator_.free(rowkey_iter);
  }
  rowkey_iters_.reset();
}

int ObDirectLoadMultipleSSTableRangeSplitter::init(
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
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
    datum_utils_ = datum_utils;
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(construct_rowkey_iters(sstable_array, table_data_desc, datum_utils))) {
      LOG_WARN("fail to construct rowkey iters", KR(ret));
    } else if (OB_FAIL(compare_.init(*datum_utils))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(rowkey_merger_.init(rowkey_iters_, &compare_))) {
      LOG_WARN("fail to init rowkey merger", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDirectLoadMultipleSSTableRangeSplitter::construct_rowkey_iters(
  const ObIArray<ObDirectLoadMultipleSSTable *> &sstable_array,
  const ObDirectLoadTableDataDesc &table_data_desc,
  const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObDirectLoadMultipleSSTable *sstable = sstable_array.at(i);
    ObIDirectLoadMultipleDatumRowkeyIterator *rowkey_iter = nullptr;
    if (OB_FAIL(ObDirectLoadRangeSplitUtils::construct_multiple_rowkey_iter(
          sstable, table_data_desc, allocator_, rowkey_iter))) {
      LOG_WARN("fail to construct rowkey iter", KR(ret));
    } else if (OB_FAIL(rowkey_iters_.push_back(rowkey_iter))) {
      LOG_WARN("fail to push back rowkey iter", KR(ret));
    } else {
      total_block_count_ += sstable->get_meta().index_block_count_;
    }
    if (OB_FAIL(ret)) {
      if (nullptr != rowkey_iter) {
        rowkey_iter->~ObIDirectLoadMultipleDatumRowkeyIterator();
        allocator_.free(rowkey_iter);
        rowkey_iter = nullptr;
      }
    }
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
    const int64_t range_count = MIN(total_block_count_, max_range_count);
    if (range_count <= 1) {
      ObDirectLoadMultipleDatumRange range;
      range.set_whole_range();
      if (OB_FAIL(range_array.push_back(range))) {
        LOG_WARN("fail to push back range", KR(ret));
      }
    } else {
      const int64_t block_count_per_range = (total_block_count_ + range_count - 1) / range_count;
      ObDirectLoadMultipleDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      int64_t count = 0;
      const ObDirectLoadMultipleDatumRowkey *rowkey = nullptr;
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
        } else if (++count >= block_count_per_range) {
          int cmp_ret = 0;
          if (OB_FAIL(rowkey->compare(range.end_key_, *datum_utils_, cmp_ret))) {
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
