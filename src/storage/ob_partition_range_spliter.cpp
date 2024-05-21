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

#include "ob_partition_range_spliter.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/memtable/ob_memtable.h"
#include "compaction/ob_tablet_merge_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "tx/ob_trans_service.h"
#include "access/ob_multiple_scan_merge.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
namespace storage
{

ObEndkeyIterator::ObEndkeyIterator()
  : endkeys_(),
    cur_idx_(0),
    iter_idx_(0),
    is_inited_(false)
{

}

void ObEndkeyIterator::reset()
{
  endkeys_.reset();
  cur_idx_ = 0;
  iter_idx_ = 0;
  is_inited_ = false;
}

int ObEndkeyIterator::open(
    const int64_t skip_cnt,
    const int64_t iter_idx,
    ObSSTable &sstable,
    ObRangeSplitInfo &range_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMacroEndkeyIterator init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(
      iter_idx < 0
      || skip_cnt < 0
      || !sstable.is_valid()
      || !range_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to open ObEndkeyIterator",
        K(ret), K(skip_cnt), K(iter_idx), K(sstable), K(range_info));
  } else {
    ObDatumRowkey sstable_endkey;
    ObArenaAllocator temp_allocator;
    ObDatumRange datum_range;
    const ObITableReadInfo *index_read_info = range_info.index_read_info_;
    const ObStorageDatumUtils &datum_utils = index_read_info->get_datum_utils();
    int cmp_ret = 0;
    // Sample start from a specified point
    if (OB_FAIL(datum_range.from_range(*range_info.store_range_, temp_allocator))) {
      STORAGE_LOG(WARN, "Failed to transfer store range", K(ret), K(range_info));
    } else if (OB_FAIL(sstable.get_last_rowkey(temp_allocator, sstable_endkey))) {
      STORAGE_LOG(WARN, "Failed to get last rowkey from sstable");
    } else if (OB_FAIL(sstable_endkey.compare(datum_range.get_start_key(), datum_utils, cmp_ret))) {
      STORAGE_LOG(WARN, "Failed to compare sstable endkey with range start key",
          K(ret), K(datum_range), K(sstable_endkey), K(datum_utils));
    } else if (cmp_ret < 0) {
      // sstable not in range
      STORAGE_LOG(DEBUG, "Skip empty range", K(ret), K(range_info));
    } else if (OB_FAIL(init_endkeys(skip_cnt, range_info, datum_range, sstable))) {
      STORAGE_LOG(WARN, "Fail to scan secondary meta", K(ret), K(range_info));
    }
    if (OB_SUCC(ret)) {
      cur_idx_ = 0;
      iter_idx_ = iter_idx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObEndkeyIterator::get_endkey_cnt(int64_t &endkey_cnt) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObEndkeyIterator is not init", K(ret));
  } else {
    endkey_cnt = endkeys_.count();
  }

  return ret;
}

int ObEndkeyIterator::get_next_macro_block_endkey(ObMacroEndkey &endkey)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMacroEndkeyIterator is not init", K(ret));
  } else if (cur_idx_ >= endkeys_.count()) {
    ret = OB_ITER_END;
  } else {
    endkey.reset();
    endkey.rowkey_ = &endkeys_[cur_idx_];
    endkey.iter_idx_ = iter_idx_;
    ++cur_idx_;
  }
  return ret;
}

int ObEndkeyIterator::push_rowkey(
    const int64_t rowkey_col_cnt,
    const common::ObIArray<share::schema::ObColDesc> &col_descs,
    const blocksstable::ObDatumRowkey &end_key,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey rowkey;
  ObStoreRowkey endkey;
  ObStoreRowkey newkey;

  if (OB_UNLIKELY(rowkey_col_cnt <= 0 || end_key.datum_cnt_ < rowkey_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected rowkey_col_cnt", K(ret), K(rowkey_col_cnt), K(end_key));
  } else if (OB_FAIL(rowkey.assign(end_key.datums_, rowkey_col_cnt))) {
    STORAGE_LOG(WARN, "Fail to construct src rowkey", K(ret), K(end_key));
  } else if (OB_FAIL(rowkey.to_store_rowkey(col_descs, allocator, endkey))) {
    STORAGE_LOG(WARN, "Fail to transfer store rowkey", K(ret), K(rowkey));
  } else if (OB_UNLIKELY(!endkey.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected invalid endkey", K(ret), K(endkey));
  } else if (OB_FAIL(endkey.deep_copy(newkey, allocator))) {
    STORAGE_LOG(WARN, "fail to deep copy endkey", K(ret), K(endkey));
  } else if (OB_FAIL(endkeys_.push_back(newkey))) {
    STORAGE_LOG(WARN, "Fail to push endkey in searray", K(ret), K(endkey));
  } else {
    endkey.reset();
  }
  return ret;
}


int ObMacroEndkeyIterator::init_endkeys(
      const int64_t skip_cnt,
      const ObRangeSplitInfo &range_info,
      const blocksstable::ObDatumRange &datum_range,
      blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  ObIAllocator *key_allocator = range_info.key_allocator_;
  ObSSTableSecMetaIterator *macro_iter = nullptr;
  const ObITableReadInfo *index_read_info = range_info.index_read_info_;
  ObDataMacroBlockMeta macro_meta;

  if (OB_UNLIKELY(
      skip_cnt <= 0
      || !sstable.is_valid()
      || !range_info.is_valid()
      || nullptr == range_info.key_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to open ObEndkeyIterator",
        K(ret), K(skip_cnt), K(sstable), K(range_info));
  } else if (OB_FAIL(sstable.scan_secondary_meta(*key_allocator, datum_range,
        *index_read_info, DATA_BLOCK_META, macro_iter, false, skip_cnt))) {
      STORAGE_LOG(WARN, "Fail to scan secondary meta", K(ret), K(range_info));
  } else {
    const ObIArray<share::schema::ObColDesc> &col_descs = index_read_info->get_columns_desc();

    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_iter->get_next(macro_meta))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "Fail to get next macro meta", K(ret));
        }
      } else if (OB_UNLIKELY(!macro_meta.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid data macro meta", K(ret), K(macro_meta));
      }  else if (OB_FAIL(push_rowkey(index_read_info->get_schema_rowkey_count(), col_descs, macro_meta.end_key_, *key_allocator))) {
        STORAGE_LOG(WARN, "Fail to push rowkey", K(ret), K(macro_meta));
      }
    }

    if (OB_NOT_NULL(macro_iter)) {
      macro_iter->~ObSSTableSecMetaIterator();
      key_allocator->free(macro_iter);
      macro_iter = nullptr;
    }
  }

  return ret;
}

int ObMicroEndkeyIterator::init_endkeys(
      const int64_t skip_cnt,
      const ObRangeSplitInfo &range_info,
      const blocksstable::ObDatumRange &datum_range,
      blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *index_read_info = range_info.index_read_info_;
  ObIAllocator *key_allocator = range_info.key_allocator_;
  ObIMacroBlockIterator *macro_block_iter = nullptr;

  if (OB_UNLIKELY(
      skip_cnt <= 0
      || !sstable.is_valid()
      || !range_info.is_valid()
      || nullptr == range_info.key_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to open ObEndkeyIterator",
        K(ret), K(sstable), K(range_info));
  } else if (OB_FAIL(sstable.scan_macro_block(datum_range,
                                      *index_read_info,
                                      *key_allocator,
                                      macro_block_iter,
                                      false,
                                      true,
                                      false))) {
    STORAGE_LOG(WARN, "failed to scan macro block", K(ret), K(sstable), K(datum_range), KPC(index_read_info));
  } else if (OB_UNLIKELY(nullptr == macro_block_iter)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected macro block iter", K(ret), KPC(macro_block_iter), K(sstable), K(datum_range));
  } else {
    blocksstable::ObMacroBlockDesc curr_block_desc;
    const ObIArray<share::schema::ObColDesc> &col_descs = index_read_info->get_columns_desc();

    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_block_iter->get_next_macro_block(curr_block_desc))) {
        if (OB_UNLIKELY(ret != OB_ITER_END)) {
          STORAGE_LOG(WARN, "failed to get next macro block", K(ret), KPC(macro_block_iter));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        const ObIArray<ObDatumRowkey> &micro_endkeys = macro_block_iter->get_micro_endkeys();

        for (int64_t i = skip_cnt - 1; OB_SUCC(ret) && i < micro_endkeys.count(); i += skip_cnt) {
          if (OB_FAIL(push_rowkey(index_read_info->get_schema_rowkey_count(), col_descs, micro_endkeys.at(i), *key_allocator))) {
            STORAGE_LOG(WARN, "failed to push rowkey", K(ret), K(i), K(micro_endkeys), K(col_descs), K(index_read_info->get_schema_rowkey_count()));
          }
        }
      }
    }

    if (macro_block_iter != nullptr) {
      macro_block_iter->~ObIMacroBlockIterator();
      key_allocator->free(macro_block_iter);
      macro_block_iter = nullptr;
    }
  }

  return ret;
}

ObPartitionParallelRanger::ObPartitionParallelRanger(ObArenaAllocator &allocator)
  : store_range_(nullptr),
    endkey_iters_(),
    allocator_(allocator),
    comparor_(),
    range_heap_(comparor_),
    last_macro_endkey_(nullptr),
    total_endkey_cnt_(0),
    sample_cnt_(0),
    parallel_target_count_(0),
    is_micro_level_(false),
    col_cnt_(0),
    is_inited_(false)
{
}

ObPartitionParallelRanger::~ObPartitionParallelRanger()
{
  reset();
}

void ObPartitionParallelRanger::reset()
{
  ObEndkeyIterator *endkey_iter = nullptr;
  for (int64_t i = 0; i < endkey_iters_.count(); i++) {
    if (OB_NOT_NULL(endkey_iter = endkey_iters_.at(i)))  {
      endkey_iter->~ObEndkeyIterator();
      allocator_.free(endkey_iter);
    }
  }
  store_range_ = nullptr;
  endkey_iters_.reset();
  range_heap_.reset();
  comparor_.reset();
  last_macro_endkey_ = nullptr;
  total_endkey_cnt_ = 0;
  sample_cnt_ = 0;
  parallel_target_count_ = 0;
  is_micro_level_ = false;
  col_cnt_ = 0;
  is_inited_ = false;
}

int ObPartitionParallelRanger::init(ObRangeSplitInfo &range_info, const bool for_compaction)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is inited twice", K(ret));
  } else if (OB_UNLIKELY(!range_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObPartitionParallelRanger", K(ret), K(range_info));
  } else if (parallel_target_count_ == 1) {
    //  construct single range
  } else if (OB_FAIL(calc_sample_count(for_compaction, range_info))) {
    STORAGE_LOG(WARN, "Failed to calculate sample count", K(ret), K(range_info));
  } else if (sample_cnt_ == 0) {
    // no enough block count, construct single range
  } else if (OB_FAIL(init_macro_iters(range_info))) {
    STORAGE_LOG(WARN, "Failed to init macro iters", K(ret), K(range_info));
  } else if (OB_FAIL(build_parallel_range_heap())) {
    STORAGE_LOG(WARN, "Failed to build parallel range heap", K(ret));
  }
  if (OB_SUCC(ret)) {
    store_range_ = range_info.store_range_;
    parallel_target_count_ = range_info.parallel_target_count_;
    col_cnt_ = store_range_->get_start_key().get_obj_cnt();
    is_inited_ = true;
    STORAGE_LOG(DEBUG, "succ to init partition parallel ranger", K(*this));
  }

  return ret;
}

int ObPartitionParallelRanger::calc_sample_count(const bool for_compaction, ObRangeSplitInfo &range_info)
{
  int ret = OB_SUCCESS;
  // decide sample count due to the largest minor sstable
  if (OB_UNLIKELY(0 == range_info.parallel_target_count_
      || range_info.max_macro_block_count_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid range split info", K(ret), K(range_info));
  } else {
    sample_cnt_ = range_info.max_macro_block_count_ / range_info.parallel_target_count_;
    if (sample_cnt_ > 2) {
      sample_cnt_ /= 2;
    }
    if (sample_cnt_ == 0 && !for_compaction) {
      is_micro_level_ = true;
      sample_cnt_ = range_info.max_estimate_micro_block_cnt_ / range_info.parallel_target_count_;
      if (sample_cnt_ > 2) {
        sample_cnt_ /= 2;
      } else if (sample_cnt_ == 0) {
        sample_cnt_ = 1;
      }
    } else {
      is_micro_level_ = false;
    }
    STORAGE_LOG(DEBUG, "finish calc sample cnt", K(range_info), K_(sample_cnt));
  }

  return ret;
}

int ObPartitionParallelRanger::init_macro_iters(ObRangeSplitInfo &range_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(sample_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected sample count to init macro endkey iters", K(ret), K_(sample_cnt));
  } else {
    ObEndkeyIterator *endkey_iter = nullptr;
    total_endkey_cnt_ = 0;
    int64_t iter_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_info.tables_->count(); i++) {
      int64_t endkey_cnt = 0;
      void *buf = nullptr;
      ObITable *table = nullptr;
      if (OB_ISNULL(table = range_info.tables_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null pointer to table", K(ret), KP(table));
      } else if (OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected table type", K(ret), KPC(table));
      } else if (is_micro_level_ && !table->is_ddl_mem_sstable()) { // ddl kv not support endkey iterator of micro block
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMicroEndkeyIterator)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Failed to alloc memory for endkey iter", K(ret));
        } else {
          endkey_iter = new (buf) ObMicroEndkeyIterator();
        }
      } else {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMacroEndkeyIterator)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Failed to alloc memory for endkey iter", K(ret));
        } else {
          endkey_iter = new (buf) ObMacroEndkeyIterator();
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(endkey_iter->open(
          sample_cnt_, iter_idx, *(static_cast<ObSSTable *>(table)), range_info))) {
        STORAGE_LOG(WARN, "Failed to open endkey iter", K(ret), KP(table), K(sample_cnt_), K(iter_idx));
      } else if (!is_micro_level_ && endkey_iter->is_empty() && sample_cnt_ >= 3) {
        //Open again with a smaller sample_cnt_
        endkey_iter->reset();
        if (OB_FAIL(endkey_iter->open(
          sample_cnt_ / 3, iter_idx, *(static_cast<ObSSTable *>(table)), range_info))) {
          STORAGE_LOG(WARN, "Failed to open endkey iter", K(ret), KP(table), K(sample_cnt_), K(iter_idx));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(endkey_iters_.push_back(endkey_iter))) {
        STORAGE_LOG(WARN, "Failed to push back macro block iter", K(ret));
      } else if (OB_FAIL(endkey_iter->get_endkey_cnt(endkey_cnt))) {
        STORAGE_LOG(WARN, "Failed to get endkey count from ite", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "succ to init sample macro iter",
            KP(table), K_(sample_cnt), K(endkey_cnt), K(iter_idx));
        // last macro block should be ignored
        total_endkey_cnt_ += endkey_cnt;
        iter_idx++;
      }
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < endkey_iters_.count(); i++) {
        if (OB_NOT_NULL(endkey_iter = endkey_iters_.at(i)))  {
          endkey_iter->~ObEndkeyIterator();
          allocator_.free(endkey_iter);
        }
      }
      endkey_iters_.reset();
    }
  }

  return ret;
}

int ObPartitionParallelRanger::build_parallel_range_heap()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(endkey_iters_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build parallel range heap", K(ret), K_(endkey_iters));
  } else {
    ObEndkeyIterator *endkey_iter = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < endkey_iters_.count(); i++) {
      ObMacroEndkey endkey;
      if (OB_ISNULL(endkey_iter = endkey_iters_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro block iter", K(ret), K(i));
      } else if (OB_FAIL(endkey_iter->get_next_macro_block_endkey(endkey))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "Failed to get next macro block endkey", K(ret), K(i));
        }
      } else if (OB_UNLIKELY(!endkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block endkey", K(ret), K(endkey));
      } else  if (OB_FAIL(range_heap_.push(endkey))) {
        STORAGE_LOG(WARN, "Failed to push macro endkey to merge heap", K(ret), K(i), K(endkey));
      } else if (OB_FAIL(comparor_.get_error_code())) {
        STORAGE_LOG(WARN, "Failed to compare macro endkeys", K(ret));
      }
    }
  }

  return ret;
}

int ObPartitionParallelRanger::get_next_macro_endkey(ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is not inited", K(ret), K(*this));
  } else {
    ObMacroEndkey endkey;
    ObEndkeyIterator *endkey_iter = nullptr;
    // already get last row from merge heap
    if (OB_NOT_NULL(last_macro_endkey_) && last_macro_endkey_->is_valid()) {
      if (OB_ISNULL(endkey_iter = endkey_iters_.at(last_macro_endkey_->iter_idx_))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null macro block iter", K(ret), K_(last_macro_endkey));
      } else if (OB_UNLIKELY(last_macro_endkey_->iter_idx_ != endkey_iter->iter_idx_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected iter idx", K(ret), K(*last_macro_endkey_), K(*endkey_iter));
      } else if (OB_FAIL(endkey_iter->get_next_macro_block_endkey(endkey))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Failed to get next macro block endkey", K(ret));
        } else if (OB_FAIL(range_heap_.pop())) {
          STORAGE_LOG(WARN, "Failed to pop the last macro endkey", K(ret));
        } else if (OB_FAIL(comparor_.get_error_code())) {
          STORAGE_LOG(WARN, "Failed compare macro endkeys", K(ret));
        }
      } else if (OB_UNLIKELY(!endkey.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block endkey", K(ret), K(endkey));
      } else if (OB_FAIL(range_heap_.replace_top(endkey))) {
        STORAGE_LOG(WARN, "Failed to replace top of the merge heap", K(ret), K(endkey));
      }
    }
    if (OB_SUCC(ret)) {
      if (range_heap_.empty()) {
        ret = OB_ITER_END;
        last_macro_endkey_ = nullptr;
      } else if (OB_FAIL(range_heap_.top(last_macro_endkey_))) {
        STORAGE_LOG(WARN, "Failed to get top macro endkey from heap", K(ret), KPC(last_macro_endkey_));
      } else if (OB_ISNULL(last_macro_endkey_) || OB_UNLIKELY(!last_macro_endkey_->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro endkey", K(ret), KPC(last_macro_endkey_));
      } else if (OB_FAIL(rowkey.assign(
          last_macro_endkey_->rowkey_->get_obj_ptr(),
          last_macro_endkey_->rowkey_->get_obj_cnt()))) {
        STORAGE_LOG(WARN, "Failed to assign rowkey", K(ret), KPC(last_macro_endkey_));
      }
    }
  }

  return ret;
}

int ObPartitionParallelRanger::check_rowkey_equal(const ObStoreRowkey &rowkey1, const ObStoreRowkey &rowkey2, bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (OB_UNLIKELY(!rowkey1.is_valid() || !rowkey2.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check rowkey equal", K(ret), K(rowkey1), K(rowkey2));
  } else if (rowkey1.is_min()) {
    equal = rowkey2.is_min();
  } else if (rowkey1.is_max()) {
    equal = rowkey2.is_max();
  } else if (0 == rowkey1.compare(rowkey2)) {
    equal = true;
  }
  return ret;
}


int ObPartitionParallelRanger::check_continuous(ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to check continuous for range spliter", K(ret), K(range_array));
  } else {
    bool equal = false;
    ObStoreRowkey macro_endkey = store_range_->get_start_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); i++) {
      if (OB_UNLIKELY(!range_array.at(i).is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid range", K(ret), K(i), K(range_array.at(i)));
      } else if (OB_FAIL(check_rowkey_equal(range_array.at(i).get_start_key(), macro_endkey, equal))) {
        STORAGE_LOG(WARN, "Failed to check rowkey equal", K(ret), K(i));
      } else if (!equal) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected range array which is not continuous", K(ret), K(i),
            K(range_array.at(i)), K(macro_endkey), KPC(store_range_));
      } else {
        macro_endkey = range_array.at(i).get_end_key();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_rowkey_equal(store_range_->get_end_key(), macro_endkey, equal))) {
      STORAGE_LOG(WARN, "Failed to check rowkey equal", K(ret));
    } else if (!equal) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected range array which is not continuous", K(ret), K(macro_endkey), KPC(store_range_));
    }
  }
  return ret;
}

int ObPartitionParallelRanger::split_ranges(
    const bool for_compaction,
    ObIAllocator &allocator,
    ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  ObStoreRange split_range;

  range_array.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionParallelRanger is not init", K(ret));
  } else if (sample_cnt_ == 0 || parallel_target_count_ == 1) {
    if (OB_FAIL(construct_single_range(allocator,
                                       store_range_->get_start_key(),
                                       store_range_->get_end_key(),
                                       store_range_->get_border_flag(),
                                       for_compaction,
                                       split_range))) {
      STORAGE_LOG(WARN, "failed to construct single range", K(ret), KPC(store_range_));
    } else if (OB_FAIL(range_array.push_back(split_range))) {
      STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(split_range));
    } else {
      STORAGE_LOG(DEBUG, "build single range", K(split_range), K_(sample_cnt), K_(parallel_target_count),
                  K_(total_endkey_cnt));
    }
  } else {
    ObStoreRowkey macro_endkey;
    ObStoreRowkey last_macro_endkey = store_range_->get_start_key();
    const int64_t range_skip_cnt = parallel_target_count_ > total_endkey_cnt_ + 1 ?
                        1 : total_endkey_cnt_ / parallel_target_count_;
    const int64_t endkey_left_cnt = parallel_target_count_ > total_endkey_cnt_ + 1 ?
                        0 : total_endkey_cnt_ % parallel_target_count_;
    int64_t iter_cnt = 0;
    int64_t range_skip_revise_cnt = endkey_left_cnt > 0 ? 1 : 0;
    ObBorderFlag border_flag;
    border_flag.set_data(store_range_->get_border_flag().get_data());
    border_flag.set_inclusive_end();

    STORAGE_LOG(DEBUG, "start to iter endkeys to split range", KPC(store_range_), K_(total_endkey_cnt),
                K_(parallel_target_count), K(range_skip_cnt), K(endkey_left_cnt), K(range_skip_revise_cnt));

    while (OB_SUCC(ret) && OB_SUCC(get_next_macro_endkey(macro_endkey))) {
      if (macro_endkey.compare(store_range_->get_end_key()) >= 0) {
        // meet endkey which larger than endkey of the store_range, break
        break;
      } else if (++iter_cnt < range_skip_cnt + range_skip_revise_cnt) {
        // too many split ranges, need skip
      } else if (macro_endkey.compare(last_macro_endkey) <= 0) {
        // duplicate rowkey due to we change last start key with max trans version
        // continue
      } else if (OB_FAIL(construct_single_range(
          allocator, last_macro_endkey, macro_endkey, border_flag, for_compaction, split_range))) {
        STORAGE_LOG(WARN, "failed to construct single range",
            K(ret), K(last_macro_endkey), K(macro_endkey));
      } else if (OB_FAIL(range_array.push_back(split_range))) {
        STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(split_range));
      } else {
        border_flag.unset_inclusive_start();
        split_range.reset();
        range_skip_revise_cnt = range_array.count() < endkey_left_cnt ? 1 : 0;
        iter_cnt = 0;
        last_macro_endkey = macro_endkey;
        if (range_array.count() == parallel_target_count_ - 1) {
          // enough ranges break and add the last range with max value
          break;
        }
      }
    }
    if (OB_FAIL(ret) && OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Failed to get next macro endkey", K(ret), K(macro_endkey));
    } else {
      ret = OB_SUCCESS;
      macro_endkey = store_range_->get_end_key();
      if (!store_range_->get_border_flag().inclusive_end()) {
        border_flag.unset_inclusive_end();
      }
      if (OB_FAIL(construct_single_range(
          allocator, last_macro_endkey, macro_endkey, border_flag, for_compaction, split_range))) {
        STORAGE_LOG(WARN, "Failed to construct single range",
            K(ret), K(last_macro_endkey), K(macro_endkey));
      } else if (OB_FAIL(range_array.push_back(split_range))) {
        STORAGE_LOG(WARN, "Failed to push back merge range", K(ret), K(split_range));
      }
    }

    //check all ranges continuous
    if (OB_SUCC(ret) && OB_FAIL(check_continuous(range_array))) {
      STORAGE_LOG(WARN, "Failed to check range array continuous", K(ret), K(range_array));
    }
  }

  return ret;
}

int ObPartitionParallelRanger::build_bound_rowkey(const bool is_max, common::ObIAllocator &allocator, common::ObStoreRowkey &new_rowkey)
{
  int ret = OB_SUCCESS;
  char *ptr = nullptr;

  if (OB_UNLIKELY(col_cnt_ == 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected col cnt", K(ret));
  } else if (OB_ISNULL(ptr = reinterpret_cast<char *>(allocator.alloc(sizeof(ObObj) * col_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to allocate memory", K(ret), K(sizeof(ObObj) * col_cnt_));
  } else {
    ObObj *obj_ptr = reinterpret_cast<ObObj *>(ptr);
    for (int64_t i = 0; i < col_cnt_; i++) {
      if (is_max) {
        obj_ptr[i].set_max_value();
      } else {
        obj_ptr[i].set_min_value();
      }
    }

    if (OB_FAIL(new_rowkey.assign(obj_ptr, col_cnt_))) {
      STORAGE_LOG(WARN, "Failed to assign new rowkey", K(ret));
    }
  }

  return ret;
}

int ObPartitionParallelRanger::build_new_rowkey(const ObStoreRowkey &rowkey,
                                                const bool for_compaction,
                                                ObIAllocator &allocator,
                                                ObStoreRowkey &new_rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build new rowkey", K(ret), K(rowkey));
  } else {
    const int64_t extra_rowkey_cnt =
        for_compaction ? ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt() : 0;
    const int64_t rowkey_col_cnt = rowkey.get_obj_cnt() + extra_rowkey_cnt;
    const int64_t total_size = rowkey.get_deep_copy_size() + sizeof(ObObj) * extra_rowkey_cnt;
    char *ptr = nullptr;
    if (OB_ISNULL(ptr = reinterpret_cast<char *>(allocator.alloc(total_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to allocate memory", K(ret), K(total_size));
    } else {
      ObObj *obj_ptr = reinterpret_cast<ObObj *>(ptr);
      int64_t pos = sizeof(ObObj) * rowkey_col_cnt;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.get_obj_cnt(); i++) {
        if (OB_FAIL(obj_ptr[i].deep_copy(rowkey.get_obj_ptr()[i], ptr, total_size, pos))) {
          STORAGE_LOG(WARN, "Failed to deep copy object", K(ret), K(i), K(rowkey), K(total_size), K(pos));
        }
      }
      for (int64_t i = rowkey.get_obj_cnt(); OB_SUCC(ret) && i < rowkey_col_cnt; i++) {
        obj_ptr[i].set_max_value();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(new_rowkey.assign(obj_ptr, rowkey_col_cnt))) {
          STORAGE_LOG(WARN, "Failed to assign new rowkey", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPartitionParallelRanger::construct_single_range(ObIAllocator &allocator,
                                                      const ObStoreRowkey &start_key,
                                                      const ObStoreRowkey &end_key,
                                                      const ObBorderFlag  &border_flag,
                                                      const bool for_compaction,
                                                      ObStoreRange &range)
{
  int ret = OB_SUCCESS;

  if (end_key.compare(start_key) < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid keys to construct range", K(ret), K(start_key), K(end_key));
  } else if (start_key.is_min()) {
    if (for_compaction) {
      range.get_start_key().set_min();
    } else if (OB_FAIL(build_bound_rowkey(false /* is max */, allocator, range.get_start_key()))) {
      STORAGE_LOG(WARN, "fail to build bound rowkey", K(ret));
    }
  } else if (OB_FAIL(build_new_rowkey(start_key, for_compaction, allocator, range.get_start_key()))) {
    STORAGE_LOG(WARN, "Failed to deep copy macro start endkey", K(ret), K(start_key));
  }
  if (OB_FAIL(ret)) {
  } else if (end_key.is_max()) {
    if (for_compaction) {
      range.get_end_key().set_max();
    } else if (OB_FAIL(build_bound_rowkey(true /* is max */, allocator, range.get_end_key()))) {
      STORAGE_LOG(WARN, "fail to build bound rowkey", K(ret));
    }
  } else if (OB_FAIL(build_new_rowkey(end_key, for_compaction, allocator, range.get_end_key()))) {
    STORAGE_LOG(WARN, "Failed to deep copy macro end endkey", K(ret), K(start_key));
  }
  if (OB_SUCC(ret)) {
    range.set_border_flag(border_flag);
    if (start_key.is_min()) {
      range.set_left_open();
    }
    if (end_key.is_max()) {
      range.set_right_open();
    }
    if (OB_NOT_NULL(store_range_)) {
      range.set_table_id(store_range_->get_table_id());
    }
  }

  return ret;
}

ObRangeSplitInfo::ObRangeSplitInfo()
  : store_range_(nullptr),
    index_read_info_(nullptr),
    tables_(nullptr),
    total_size_(0),
    parallel_target_count_(1),
    max_macro_block_count_(0),
    max_estimate_micro_block_cnt_(0),
    key_allocator_(nullptr),
    is_sstable_(false)
{}

ObRangeSplitInfo::~ObRangeSplitInfo()
{
  reset();
}

void ObRangeSplitInfo::reset()
{
  store_range_ = nullptr;
  index_read_info_ = nullptr;
  tables_ = nullptr;
  total_size_ = 0;
  parallel_target_count_ = 1;
  max_macro_block_count_ = 0;
  max_estimate_micro_block_cnt_ = 0;
  key_allocator_ = nullptr;
  is_sstable_ = false;
}

ObPartitionRangeSpliter::ObPartitionRangeSpliter()
  : allocator_(),
    parallel_ranger_(allocator_)
{
}

ObPartitionRangeSpliter::~ObPartitionRangeSpliter()
{
  reset();
}

void ObPartitionRangeSpliter::reset()
{
  parallel_ranger_.reset();
  allocator_.reset();
}

int ObPartitionRangeSpliter::get_range_split_info(ObIArray<ObITable *> &tables,
                                                  const ObITableReadInfo &index_read_info,
                                                  const ObStoreRange &store_range,
                                                  ObRangeSplitInfo &range_info)
{
  int ret = OB_SUCCESS;
  range_info.reset();

  if (OB_UNLIKELY(tables.empty() || !store_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObPartitionRangeSpliter", K(ret), K(tables),
                K(store_range));
  } else {
    // build range paras
    range_info.store_range_ = &store_range;
    range_info.tables_ = &tables;
    bool is_sstable = false;
    int64_t size = 0;
    int64_t macro_block_cnt = 0;
    int64_t estimate_micro_block_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      ObITable *table = tables.at(i);
      macro_block_cnt = 0;
      estimate_micro_block_cnt = 0;
      if (OB_ISNULL(table)) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "Invalid null table pointer", K(ret), KP(table));
      } else if (i == 0) {
        is_sstable = table->is_sstable();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(0 != i && !is_sstable)) {
        // Multi memtable
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected multi memtable range info",
          K(ret), K(range_info), KPC(table), K(is_sstable), K(i));
      } else if (OB_UNLIKELY(!is_sstable && table->is_sstable())) {
        // Memtable/sstable mixed up
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected memtable and sstable mixed up",
          K(ret), K(range_info), KPC(table), K(is_sstable), K(i));
      } else if (OB_FAIL(get_single_range_info(
          *range_info.store_range_, index_read_info, table, size, macro_block_cnt, estimate_micro_block_cnt))) {
        STORAGE_LOG(WARN, "Failed to get single range info", K(ret), K(i), KPC(table));
      } else {
        if (table->is_co_sstable()) {
          ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
          if (co_sstable->is_rowkey_cg_base() && !co_sstable->is_cgs_empty_co_table()) {
            size = size * (co_sstable->get_cs_meta().occupy_size_ / co_sstable->get_occupy_size());
          }
        }
        range_info.total_size_ += size;
        range_info.index_read_info_ = &index_read_info;
        range_info.max_macro_block_count_ = MAX(macro_block_cnt, range_info.max_macro_block_count_);
        range_info.max_estimate_micro_block_cnt_ = MAX(estimate_micro_block_cnt, range_info.max_estimate_micro_block_cnt_);
      }
      STORAGE_LOG(DEBUG, "get range para from one table",
          K(ret), K(i), KP(table), K(size), K(macro_block_cnt));
    }
    if (OB_SUCC(ret)) {
      range_info.is_sstable_ = is_sstable;
    }
    STORAGE_LOG(DEBUG, "Get range split info", K(ret), K(range_info));
  }

  return ret;
}

int ObPartitionRangeSpliter::get_single_range_info(const ObStoreRange &store_range,
                                                   const ObITableReadInfo &index_read_info,
                                                   ObITable *table,
                                                   int64_t &total_size,
                                                   int64_t &macro_block_cnt,
                                                   int64_t &estimate_micro_block_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid table pointer", K(ret), KP(table));
  } else if (table->is_data_memtable()) {
    memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
    int64_t row_count = 0;
    if (OB_FAIL(memtable->estimate_phy_size(&store_range.get_start_key(),
                                            &store_range.get_end_key(),
                                            total_size,
                                            row_count))) {
      STORAGE_LOG(WARN, "Failed to get single range info from memtable", K(ret), K(store_range));
    }
  } else if (table->is_sstable()) {
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    if (store_range.is_whole_range()) {
      ObSSTableMetaHandle meta_handle;
      const ObSSTableMeta *sstable_meta = nullptr;
      if (OB_FAIL(sstable->get_meta(meta_handle))) {
        STORAGE_LOG(WARN, "fail to get meta", K(ret));
      } else if (OB_FAIL(meta_handle.get_sstable_meta(sstable_meta))) {
        STORAGE_LOG(WARN, "fail to get sstable meta", K(ret));
      } else {
        total_size = sstable->get_occupy_size();
        macro_block_cnt = sstable->get_data_macro_block_count();
        estimate_micro_block_cnt = sstable_meta->get_data_micro_block_count();
      }
    } else if (0 == sstable->get_data_macro_block_count()) {
      total_size = 0;
      macro_block_cnt = 0;
      estimate_micro_block_cnt = 0;
    } else {
      ObArenaAllocator temp_allocator;
      ObDatumRange datum_range;
      ObDatumRowkey sstable_endkey;
      total_size = 0;
      macro_block_cnt = 0;
      estimate_micro_block_cnt = 0;
      ObSSTableSecMetaIterator *macro_meta_iter = nullptr;
      ObDataMacroBlockMeta macro_meta;
      const ObStorageDatumUtils &datum_utils = index_read_info.get_datum_utils();
      int cmp_ret = 0;
      if (OB_FAIL(datum_range.from_range(store_range, temp_allocator))) {
        STORAGE_LOG(WARN, "Failed to transfer store range", K(ret), K(store_range));
      } else if (OB_FAIL(sstable->get_last_rowkey(temp_allocator, sstable_endkey))) {
        STORAGE_LOG(WARN, "Failed to get last rowkey from sstable");
      } else if (OB_FAIL(sstable_endkey.compare(datum_range.get_start_key(), datum_utils, cmp_ret))) {
        STORAGE_LOG(WARN, "Failed to compare sstable endkey with range start key",
            K(ret), K(datum_range), K(sstable_endkey), K(datum_utils));
      } else if (cmp_ret < 0) {
        // sstable not in range
        STORAGE_LOG(DEBUG, "Skip empty range", K(ret), K(datum_range), KPC(sstable));
      } else if (OB_FAIL(sstable->scan_secondary_meta(
          allocator_,
          datum_range,
          index_read_info,
          ObMacroBlockMetaType::DATA_BLOCK_META,
          macro_meta_iter))) {
        STORAGE_LOG(DEBUG, "Skip empty range", K(ret), K(datum_range), KPC(sstable));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(macro_meta_iter->get_next(macro_meta))) {
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              STORAGE_LOG(WARN, "Fail to get next macro block meta", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            total_size += macro_meta.val_.occupy_size_;
            macro_block_cnt++;
            estimate_micro_block_cnt += macro_meta.val_.micro_block_count_;
          }
        }
        if (OB_NOT_NULL(macro_meta_iter)) {
          macro_meta_iter->~ObSSTableSecMetaIterator();
        }
      }
    }
  } else if (table->is_direct_load_memtable()) {
    // TODO : @suzhi.yt 可能会导致划分range不均衡, 后续实现
    total_size = 0;
    macro_block_cnt = 0;
    estimate_micro_block_cnt = 0;
  }
  return ret;
}

int ObPartitionRangeSpliter::build_single_range(const bool for_compaction,
                                                ObRangeSplitInfo &range_info,
                                                ObIAllocator &allocator,
                                                ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  ObStoreRange dst_range;

  if (OB_FAIL(parallel_ranger_.construct_single_range(allocator,
          range_info.store_range_->get_start_key(),
          range_info.store_range_->get_end_key(),
          range_info.store_range_->get_border_flag(),
          for_compaction,
          dst_range))) {
    STORAGE_LOG(WARN, "failed to construct single range", K(ret), K(range_info));
  } else if (FALSE_IT(dst_range.set_table_id(range_info.store_range_->get_table_id()))) {
  } else if (OB_FAIL(range_array.push_back(dst_range))) {
    STORAGE_LOG(WARN, "failed to push back merge range", K(ret), K(dst_range));
  }

  return ret;
}

int ObPartitionRangeSpliter::split_ranges(ObRangeSplitInfo &range_info,
                                          ObIAllocator &allocator,
                                          const bool for_compaction,
                                          ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  parallel_ranger_.reset();
  range_array.reset();

  if (OB_UNLIKELY(!range_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to split ranges", K(ret), K(range_info));
  } else if (FALSE_IT(parallel_ranger_.set_col_cnt(range_info.store_range_->get_start_key().get_obj_cnt()))) {
  } else if (range_info.parallel_target_count_ == 1
      || range_info.store_range_->is_single_rowkey()) {
    if (OB_FAIL(build_single_range(for_compaction, range_info, allocator, range_array))) {
      STORAGE_LOG(WARN, "Failed to build single range", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "try to make single split range", K(range_info), K(range_array));
    }
  } else if (range_info.is_sstable()) {
    range_info.key_allocator_ = &allocator;
    if (OB_FAIL(parallel_ranger_.init(range_info, for_compaction))) {
      STORAGE_LOG(WARN, "Failed to init parallel ranger", K(ret), K(range_info));
    } else if (OB_FAIL(parallel_ranger_.split_ranges(for_compaction, allocator, range_array))) {
      STORAGE_LOG(WARN, "Failed to split ranges", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "splite ranges with sstable", K(range_info), K(range_array));
    }
  } else if (OB_UNLIKELY(for_compaction)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Split memtable ranges for compaction should not enter here",
        K(ret), K(for_compaction), K(range_info));
  } else if (OB_FAIL(split_ranges_memtable(range_info, allocator, range_array))) {
    STORAGE_LOG(WARN, "Failed to split ranges for memtable", K(ret), K(range_info));
  }


  return ret;
}
int ObPartitionRangeSpliter::split_ranges_memtable(ObRangeSplitInfo &range_info,
                                                   ObIAllocator &allocator,
                                                   ObIArray<ObStoreRange> &range_array)
{
  int ret = OB_SUCCESS;
  ObITable *table = range_info.tables_->at(0);
  if (OB_UNLIKELY(!range_info.is_valid() || range_info.is_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid range info to split ranges for memtable", K(ret));
  } else if (OB_UNLIKELY(range_info.tables_->count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected table count for memtable range info", K(ret), K(range_info));
  } else if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null table", K(ret), KP(table), K(range_info));
  } else if (table->is_data_memtable()) {
    ObSEArray<ObStoreRange, 16> store_ranges;
    memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);

    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected null memtable", K(ret), KP(memtable), K(range_info));
    } else if (OB_FAIL(memtable->get_split_ranges(
                   *range_info.store_range_, range_info.parallel_target_count_, store_ranges))) {
      STORAGE_LOG(WARN, "Failed to get split ranges from memtable", K(ret));
    } else {
      ObStoreRange store_range;
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
        if (OB_FAIL(store_ranges.at(i).deep_copy(allocator, store_range))) {
          STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(store_ranges));
        } else if (FALSE_IT(store_range.set_table_id(range_info.store_range_->get_table_id()))) {
        } else if (OB_FAIL(range_array.push_back(store_range))) {
          STORAGE_LOG(WARN, "Failed to push back store range", K(ret), K(store_range));
        }
      }
    }
    STORAGE_LOG(DEBUG, "splite ranges with memtable", K(range_info), K(range_array));
  } else if (table->is_direct_load_memtable()) {
    // TODO : @suzhi.yt 可能会导致划分range不均衡, 后续实现
    if (OB_FAIL(build_single_range(false/*for compaction*/, range_info, allocator, range_array))) {
      STORAGE_LOG(WARN, "Failed to build single range", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "try to make single split range for memtable", K(range_info), K(range_array));
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_split_tables(ObTableStoreIterator &table_iter,
                                                   ObIArray<ObITable *> &tables)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == table_iter.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get split tables", K(ret), K(table_iter));
  } else {
    int64_t major_size = 0;
    int64_t minor_size = 0;
    int64_t memtable_size = 0;
    ObITable *max_memtable = nullptr;
    ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> minor_sstables;
    ObITable *last_major_sstable = nullptr;
    ObITable *table = nullptr;
    tables.reset();

    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "Fail to get next table", K(ret), K(table_iter));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null table", K(ret), K(table_iter));
      } else if (table->is_major_sstable()) {
        if (table->is_co_sstable()) {
          ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
          if (co_sstable->is_rowkey_cg_base() && !co_sstable->is_cgs_empty_co_table()) {
            major_size = co_sstable->get_cs_meta().occupy_size_;
          } else {
            major_size = co_sstable->get_occupy_size();
          }
        } else {
          major_size = static_cast<ObSSTable *>(table)->get_occupy_size();
        }
        last_major_sstable = table;
      } else if (table->is_minor_sstable()) {
        if (OB_FAIL(minor_sstables.push_back(table))) {
          STORAGE_LOG(WARN, "Fail to cache minor sstables", K(ret), KP(table));
        } else {
          minor_size += static_cast<ObSSTable *>(table)->get_occupy_size();
        }
      } else if (table->is_data_memtable()) {
        int64_t mem_rows = 0;
        int64_t mem_size = 0;
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
        if (OB_FAIL(memtable->estimate_phy_size(nullptr, nullptr, mem_size, mem_rows))) {
          STORAGE_LOG(WARN, "Failed to get estimate size from memtable", K(ret));
        } else {
          memtable_size = MAX(mem_size, memtable_size);
          max_memtable = table;
        }
      } else if (table->is_direct_load_memtable()) {
        // TODO : @suzhi.yt 可能会导致划分range不均衡, 后续实现
      }
    }

    bool split_by_memtable = false;
    if (OB_FAIL(ret)) {
    } else if (memtable_size > (major_size + minor_size) * MEMTABLE_SIZE_AMPLIFICATION_FACTOR) {
      //too big memtable size, use memtable to range split
      split_by_memtable = true;
      if (OB_ISNULL(max_memtable)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "Unexpected null max memtable", K(ret), KP(max_memtable));
      } else if (OB_FAIL(tables.push_back(max_memtable))) {
        STORAGE_LOG(WARN, "Failed to push back max memtable", K(ret));
      } else {
        STORAGE_LOG(DEBUG, "use big memtable to split range", K(memtable_size), K(major_size), K(minor_size));
      }
    } else if (minor_size > MIN_SPLIT_TARGET_SSTABLE_SIZE && minor_size > major_size / 2) {
      // Add all minor sstables
      if (OB_FAIL(tables.reserve(minor_sstables.count() + 1))) {
        STORAGE_LOG(WARN, "Fail to reserve space for sstables", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
        if (OB_FAIL(tables.push_back(minor_sstables.at(i)))) {
          STORAGE_LOG(WARN, "Fail to add minor sstable", K(ret), KPC(table));
        }
      }
    }

    if (OB_FAIL(ret) || split_by_memtable) {
    } else if (major_size > MIN_SPLIT_TARGET_SSTABLE_SIZE) {
      // Add last major sstable
      if (OB_ISNULL(last_major_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Fail to get last major sstable", K(ret));
      } else if (OB_FAIL(tables.push_back(last_major_sstable))) {
        STORAGE_LOG(WARN, "Fail to add last major sstable", K(ret), KPC(last_major_sstable));
      }
    }
    STORAGE_LOG(DEBUG, "get range split tables", K(ret), K(memtable_size), K(major_size), K(minor_size), K(tables));
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_multi_range_size(
    const ObIArray<ObStoreRange> &range_array,
    const ObITableReadInfo &index_read_info,
    ObTableStoreIterator &table_iter,
    int64_t &total_size)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, DEFAULT_STORE_CNT_IN_STORAGE> tables;
  total_size = 0;
  int64_t estimate_size = 0, range_size = 0;

  if (OB_UNLIKELY(0 == table_iter.count() || range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get multi range size", K(ret), K(table_iter),
                K(range_array));
  } else if (OB_FAIL(get_split_tables(table_iter, tables))) {
    STORAGE_LOG(WARN, "Failed to get all sstables", K(ret), K(table_iter));
  } else if (OB_FAIL(try_estimate_range_size(range_array, tables, estimate_size))) {
    STORAGE_LOG(WARN, "fail to estimate range size");
  } else if (tables.empty()) {
    // only small tables, can not support arbitrary range split
    total_size = estimate_size;
  } else {
    RangeSplitInfoArray range_info_array;
    bool all_single_rowkey = false;
    if (OB_FAIL(get_range_split_infos(tables, index_read_info, range_array, range_info_array, range_size, all_single_rowkey))) {
      STORAGE_LOG(WARN, "Failed to get range split info array", K(ret));
    } else {
      total_size = estimate_size + range_size;
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::try_estimate_range_size(
    const common::ObIArray<common::ObStoreRange> &range_array,
    ObIArray<ObITable *> &tables,
    int64_t &total_size)
{
  int ret = OB_SUCCESS;
  total_size = 0;

  if (OB_UNLIKELY(range_array.count() >= RANGE_COUNT_THRESOLD)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObITable * table = tables.at(i);
      if (table->is_sstable()
           && static_cast<const ObSSTable *>(table)->get_data_macro_block_count() * FAST_ESTIMATE_THRESOLD / 100 <= range_array.count()) {
        total_size += static_cast<const ObSSTable *>(table)->get_occupy_size();
        if (OB_FAIL(tables.remove(i))) {
          STORAGE_LOG(WARN, "fail to remove table", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::split_multi_ranges(RangeSplitInfoArray &range_info_array,
                                                     const int64_t expected_task_count,
                                                     const int64_t total_size,
                                                     common::ObIAllocator &allocator,
                                                     ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_info_array.empty() || total_size <= 0 || expected_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to calc parallel target array", K(ret), K(range_info_array),
                K(total_size), K(expected_task_count));
  } else {
    ObArenaAllocator local_allocator;
    const int64_t avg_task_size = MAX(total_size / expected_task_count, 1);
    const int64_t split_task_size = MAX(avg_task_size / 2, 1);
    const int64_t task_size_high_watermark = MAX(avg_task_size *
                                                 SPLIT_TASK_SIZE_HIGH_WATER_MARK_FACTOR / 100, 1);
    const int64_t task_size_low_watermark = max(avg_task_size * SPLIT_TASK_SIZE_LOW_WATER_MARK_FACTOR /
                                                100, 1);
    int64_t sum_size = 0;
    RangeSplitArray range_split_array;
    RangeSplitArray refra_range_split_array;
    int64_t total_size = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_info_array.count(); i++) {
      ObRangeSplitInfo &range_info = range_info_array.at(i);
      int64_t  cur_avg_task_size = 0;
      if (range_info.empty() || range_info.total_size_ < avg_task_size
          || sum_size + range_info.total_size_ < task_size_high_watermark) {
        range_info.set_parallel_target(1);
        cur_avg_task_size = MAX(range_info.total_size_, 1);
      } else {
        int64_t parallel = range_info.total_size_ / split_task_size;
        if (range_info.max_macro_block_count_ > 0 && (range_info.total_size_ / range_info.max_macro_block_count_) < avg_task_size) {
          parallel = MIN(parallel, range_info.max_macro_block_count_);
        }
        range_info.set_parallel_target(parallel);
        cur_avg_task_size = range_info.total_size_ / parallel;
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = OB_E(EventTable::EN_COMPACTION_MEDIUM_INIT_PARALLEL_RANGE) ret;
        if (OB_FAIL(ret)) {
          int64_t parallel = 2;
          range_info.set_parallel_target(parallel);
          cur_avg_task_size = range_info.total_size_ / parallel;
          STORAGE_LOG(INFO, "set EN_COMPACTION_MEDIUM_INIT_PARALLEL_RANGE with errsim", K(range_info));
          ret = OB_SUCCESS;
        }
      }
#endif
      STORAGE_LOG(DEBUG, "start to split range array", K(range_info), K(cur_avg_task_size),
                  K(avg_task_size));
      range_spliter_.reset();
      range_split_array.reset();
      if (OB_FAIL(range_spliter_.split_ranges(range_info, local_allocator, false, range_split_array))) {
        STORAGE_LOG(WARN, "Failed to split ranges", K(ret), K(range_info));
      } else {
        STORAGE_LOG(DEBUG, "get split ranges", K(range_split_array));
        if (range_info.parallel_target_count_ != range_split_array.count()) {
          cur_avg_task_size = range_info.total_size_ / range_split_array.count();
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < range_split_array.count(); i++) {
          const int64_t cur_range_count = multi_range_split_array.count() + 1;
          if (cur_range_count < expected_task_count
            && total_size + sum_size + cur_avg_task_size > cur_range_count * avg_task_size
            && (sum_size >= avg_task_size
              || (sum_size >= task_size_low_watermark
                  && sum_size + cur_avg_task_size >= task_size_high_watermark))) {
            if (OB_FAIL(merge_and_push_range_array(refra_range_split_array, allocator,
                                                   multi_range_split_array))) {
              STORAGE_LOG(WARN, "Failed to merge and push split range array", K(ret), K(refra_range_split_array));
            } else {
              STORAGE_LOG(DEBUG, "succ to build refra split ranges", K(refra_range_split_array), K(sum_size),
                          K(avg_task_size));
              refra_range_split_array.reset();
              total_size += sum_size;
              sum_size = 0;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(refra_range_split_array.push_back(range_split_array.at(i)))) {
            STORAGE_LOG(WARN, "Failed to push back store range", K(ret), K(range_split_array.at(i)));
          } else {
            sum_size += cur_avg_task_size;
          }
        }
      }
    }
    if (OB_SUCC(ret) && refra_range_split_array.count() > 0) {
      if (OB_FAIL(merge_and_push_range_array(refra_range_split_array, allocator,
                                             multi_range_split_array))) {
        STORAGE_LOG(WARN, "Failed to merge and push split range array", K(ret), K(refra_range_split_array));
      } else {
        STORAGE_LOG(DEBUG, "succ to build refra split ranges", K(refra_range_split_array), K(sum_size),
                    K(avg_task_size));
        refra_range_split_array.reset();
      }
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::merge_and_push_range_array(
    const RangeSplitArray &src_range_split_array,
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(src_range_split_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to merge range array", K(ret), K(src_range_split_array));
  } else {
    const ObStoreRange *last_range = nullptr;
    RangeSplitArray dst_range_array;
    ObStoreRange dst_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_range_split_array.count(); i++) {
      const ObStoreRange &cur_range = src_range_split_array.at(i);
      if (OB_NOT_NULL(last_range) && cur_range.get_start_key().compare(last_range->get_end_key()) == 0)  {
        // continous range
        if (!cur_range.get_border_flag().inclusive_end()) {
          // stop find next range
          if (OB_FAIL(cur_range.get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(cur_range));
          } else if (FALSE_IT(dst_range.get_border_flag().unset_inclusive_end())) {
          } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
            STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
          } else {
            dst_range.reset();
            last_range = nullptr;
          }
        } else {
          last_range = &cur_range;
        }
      } else {
        if (OB_NOT_NULL(last_range)) {
          // break range
          if (OB_FAIL(last_range->get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), KPC(last_range));
          } else if (FALSE_IT(dst_range.get_border_flag().set_inclusive_end())) {
          } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
            STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
          } else {
            dst_range.reset();
            last_range = nullptr;
          }
        }
        if (OB_SUCC(ret)) {
          dst_range.set_table_id(cur_range.get_table_id());
          dst_range.set_border_flag(cur_range.get_border_flag());
          if (!cur_range.get_border_flag().inclusive_end()) {
            // only deal with right close | left open situation
            if (OB_FAIL(cur_range.deep_copy(allocator, dst_range))) {
              STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(cur_range));
            } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
              STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
            } else {
              dst_range.reset();
            }
          } else if (OB_FAIL(cur_range.get_start_key().deep_copy(dst_range.get_start_key(), allocator))) {
            STORAGE_LOG(WARN, "Failed to deep copy start key", K(ret), K(cur_range));
          } else {
            last_range = &cur_range;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(last_range)) {
      if (OB_FAIL(last_range->get_end_key().deep_copy(dst_range.get_end_key(), allocator))) {
        STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), KPC(last_range));
      } else if (FALSE_IT(dst_range.get_border_flag().set_inclusive_end())) {
      } else if (OB_FAIL(dst_range_array.push_back(dst_range))) {
        STORAGE_LOG(WARN, "Failed to push back dst range array", K(ret), K(dst_range));
      } else {
        dst_range.reset();
        last_range = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(multi_range_split_array.push_back(dst_range_array))) {
        STORAGE_LOG(WARN, "Failed to push back range split array", K(ret), K(dst_range_array));
      } else {
        STORAGE_LOG(DEBUG, "succ to merge range split array", K(ret), K(src_range_split_array),
                    K(dst_range_array));
      }
    }

  }

  return ret;
}

int ObPartitionMultiRangeSpliter::fast_build_range_array(
    const ObIArray<ObStoreRange> &range_array,
    const int64_t expected_task_cnt,
    ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(range_array.empty() || expected_task_cnt > range_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build single range array", K(ret), K(range_array));
  } else {
    RangeSplitArray range_split_array;
    ObStoreRange store_range;
    int64_t avg_range_cnt = range_array.count() / expected_task_cnt, remain_range_cnt = range_array.count() % expected_task_cnt;

    for (int64_t i =0 ; OB_SUCC(ret) && i < range_array.count(); i++) {
      int64_t task_range_cnt = avg_range_cnt + (multi_range_split_array.count() < remain_range_cnt ? 1 : 0);
      if (OB_FAIL(range_array.at(i).deep_copy(allocator, store_range))) {
        STORAGE_LOG(WARN, "Failed to deep copy store range", K(ret), K(i), K(range_array.at(i)));
      } else if (OB_FAIL(range_split_array.push_back(store_range))) {
        STORAGE_LOG(WARN, "Failed to push back store range", K(ret), K(store_range));
      } else if (range_split_array.count() >=  task_range_cnt) {
        if (OB_FAIL(multi_range_split_array.push_back(range_split_array))) {
          STORAGE_LOG(WARN, "Failed to push range split array", K(ret), K(range_split_array));
        } else {
          range_split_array.reset();
          STORAGE_LOG(DEBUG, "Fast split for single task", K(range_array));
        }
      }

      store_range.reset();
    }

    if (OB_FAIL(ret)) {
    } else if (!range_split_array.empty() || multi_range_split_array.count() != expected_task_cnt) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected multi_range_split_array cnt", K(ret), K(multi_range_split_array.count()), K(expected_task_cnt), K(range_array.count()), K(range_split_array.empty()));
    }
  }

  return ret;
}

int ObPartitionMultiRangeSpliter::get_split_multi_ranges(
    const common::ObIArray<common::ObStoreRange> &range_array,
    const int64_t expected_task_count,
    const ObITableReadInfo &index_read_info,
    ObTableStoreIterator &table_iter,
    common::ObIAllocator &allocator,
    common::ObArrayArray<common::ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, DEFAULT_STORE_CNT_IN_STORAGE> tables;
  int64_t fast_range_array_cnt = 0;
  multi_range_split_array.reset();

  if (OB_UNLIKELY(0 == table_iter.count() || range_array.empty() || expected_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get split multi ranges", K(ret), K(table_iter),
                K(range_array), K(expected_task_count));
  } else if (OB_UNLIKELY(expected_task_count == 1)) {
    STORAGE_LOG(DEBUG, "Unexpected only one split task", K(expected_task_count), K(range_array));
    fast_range_array_cnt = 1;
  } else if (OB_FAIL(get_split_tables(table_iter, tables))) {
    STORAGE_LOG(WARN, "Failed to get split tables", K(ret), K(table_iter));
  } else if (tables.empty()) {
    // only small tables, no need split
    STORAGE_LOG(DEBUG, "empty split tables", K(table_iter));
    fast_range_array_cnt = 1;
  } else {
    RangeSplitInfoArray range_info_array;
    int64_t total_size = 0;
    bool all_single_rowkey = false;
    if (OB_FAIL(get_range_split_infos(tables, index_read_info, range_array, range_info_array, total_size, all_single_rowkey))) {
      STORAGE_LOG(WARN, "Failed to get range split info array", K(ret));
    } else if (total_size == 0) {
      STORAGE_LOG(DEBUG, "too small tables to split range", K(total_size), K(range_info_array));
      fast_range_array_cnt = 1;
    } else if (all_single_rowkey) {
      fast_range_array_cnt = MIN(range_array.count(), expected_task_count);
    } else if (OB_FAIL(split_multi_ranges(range_info_array, expected_task_count, total_size, allocator,
                                          multi_range_split_array))) {
      STORAGE_LOG(WARN, "Failed to split multi ranges", K(ret));
    }
  }

  if (OB_SUCC(ret) && fast_range_array_cnt > 0) {
    if (OB_FAIL(fast_build_range_array(range_array, fast_range_array_cnt, allocator, multi_range_split_array))) {
      STORAGE_LOG(WARN, "Failed to build single range array", K(ret));
    }
  }

  STORAGE_LOG(TRACE, "finish split multi ranges", K(ret), K(expected_task_count), K(range_array), K(multi_range_split_array.count()), K(multi_range_split_array));
  return ret;
}

int ObPartitionMultiRangeSpliter::get_range_split_infos(ObIArray<ObITable *> &tables,
                                                        const ObITableReadInfo &index_read_info,
                                                        const ObIArray<ObStoreRange> &range_array,
                                                        RangeSplitInfoArray &range_info_array,
                                                        int64_t &total_size,
                                                        bool &all_single_rowkey)
{
  int ret = OB_SUCCESS;
  all_single_rowkey = true;

  if (OB_UNLIKELY(tables.empty() || range_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get range split info", K(ret), K(tables), K(range_array));
  } else {
    ObRangeSplitInfo range_info;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_array.count(); i++) {
      if (range_array.at(i).is_single_rowkey()) {
        range_info.store_range_ = &range_array.at(i);
        range_info.tables_ = &tables;
        range_info.index_read_info_ = &index_read_info;
        range_info.total_size_ = DEFAULT_MICRO_BLOCK_SIZE;
        range_info.max_macro_block_count_ = 1;
        range_info.max_estimate_micro_block_cnt_ = 1;
      } else  {
          range_spliter_.reset();
          all_single_rowkey = false;
          if (OB_FAIL(range_spliter_.get_range_split_info(
            tables, index_read_info, range_array.at(i), range_info))) {
            STORAGE_LOG(WARN, "Failed to get range split info", K(ret), K(i), K(range_array.at(i)));
          }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(range_info_array.push_back(range_info))) {
        STORAGE_LOG(WARN, "Failed to push back range info", K(ret), K(range_info));
      } else {
        STORAGE_LOG(DEBUG, "get single range split info", K(range_info));
        total_size += range_info.total_size_;
        range_info.reset();
      }
    }
    STORAGE_LOG(DEBUG, "get total range split info", K(total_size), K(tables), K(range_info_array));
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObPartitionMajorSSTableRangeSpliter::ObPartitionMajorSSTableRangeSpliter()
    : major_sstable_(nullptr),
      index_read_info_(nullptr),
      tablet_size_(-1),
      allocator_(nullptr),
      is_inited_(false)
{
}

ObPartitionMajorSSTableRangeSpliter::~ObPartitionMajorSSTableRangeSpliter()
{
}

int ObPartitionMajorSSTableRangeSpliter::init(const ObITableReadInfo &index_read_info,
                                              ObSSTable *major_sstable, int64_t tablet_size,
                                              ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionMajorSSTableRangeSpliter init twice", KR(ret));
  } else if (OB_ISNULL(major_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument null major sstable", KR(ret), K(major_sstable));
  } else if (OB_UNLIKELY(tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument tablet size", KR(ret), K(tablet_size));
  } else {
    major_sstable_ = major_sstable;
    index_read_info_ = &index_read_info;
    tablet_size_ = tablet_size;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::scan_major_sstable_secondary_meta(
  const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_->scan_secondary_meta(*allocator_, scan_range, *index_read_info_,
                                                  DATA_BLOCK_META, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::split_ranges(ObIArray<ObStoreRange> &result_ranges)
{
  int ret = OB_SUCCESS;
  int64_t parallel_degree = 0;
  result_ranges.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionMajorSSTableRangeSpliter not init", KR(ret));
  } else {
    // 计算parallel_degree
    if (major_sstable_->is_empty() || tablet_size_ == 0) {
      parallel_degree = 1;
    } else {
      const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
      const int64_t occupy_size = macro_block_count * OB_SERVER_BLOCK_MGR.get_macro_block_size();
      parallel_degree = (occupy_size + tablet_size_ - 1) / tablet_size_;
      if (parallel_degree > MAX_MERGE_THREAD) {
        int64_t macro_cnts = (macro_block_count + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
        parallel_degree = (macro_block_count + macro_cnts - 1) / macro_cnts;
      }
    }
    // 根据parallel_degree生成ranges
    if (parallel_degree <= 1) {
      ObStoreRange whole_range;
      whole_range.set_whole_range();
      if (OB_FAIL(result_ranges.push_back(whole_range))) {
        STORAGE_LOG(WARN, "failed to push back merge range to array", KR(ret), K(whole_range));
      }
    } else {
      if (OB_FAIL(generate_ranges_by_macro_block(parallel_degree, result_ranges))) {
        STORAGE_LOG(WARN, "failed to generate ranges by macro block", KR(ret), K(parallel_degree));
      }
    }
  }

  return ret;
}

int ObPartitionMajorSSTableRangeSpliter::generate_ranges_by_macro_block(
  int64_t parallel_degree, ObIArray<ObStoreRange> &result_ranges)
{
  int ret = OB_SUCCESS;
  const ObIArray<share::schema::ObColDesc> &col_descs = index_read_info_->get_columns_desc();
  const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
  const int64_t macro_block_cnt_per_range =
    (macro_block_count + parallel_degree - 1) / parallel_degree;

  ObDataMacroBlockMeta blk_meta;
  ObSSTableSecMetaIterator *meta_iter = nullptr;
  ObDatumRange scan_range;
  scan_range.set_whole_range();
  if (OB_FAIL(scan_major_sstable_secondary_meta(scan_range, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }

  // generate ranges
  ObDatumRowkey endkey;
  ObStoreRange range;
  range.get_end_key().set_min();
  range.set_left_open();
  range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_count;) {
    const int64_t last = i + macro_block_cnt_per_range - 1; // last macro block idx in current range
    range.get_start_key() = range.get_end_key();
    if (last < macro_block_count - 1) {
      // locate to the last macro-block meta in current range
      while (OB_SUCC(meta_iter->get_next(blk_meta)) && i++ < last);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "Failed to get macro block meta", KR(ret), K(i - 1));
      } else if (OB_UNLIKELY(!blk_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block meta", KR(ret), K(i - 1));
      } else if (OB_FAIL(blk_meta.get_rowkey(endkey))) {
        STORAGE_LOG(WARN, "Failed to get rowkey", KR(ret), K(blk_meta));
      } else if (OB_FAIL(endkey.to_store_rowkey(col_descs, *allocator_, range.get_end_key()))) {
        STORAGE_LOG(WARN, "Failed to transfer store rowkey", K(ret), K(endkey));
      }
    } else { // last range
      i = last + 1;
      range.get_end_key().set_max();
      range.set_right_open();
    }
    if (OB_SUCC(ret) && OB_FAIL(result_ranges.push_back(range))) {
      STORAGE_LOG(WARN, "Failed to push range", KR(ret), K(result_ranges), K(range));
    }
  }

  if (OB_NOT_NULL(meta_iter)) {
    meta_iter->~ObSSTableSecMetaIterator();
    meta_iter = nullptr;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::ObIncrementalIterator(compaction::ObTabletMergeCtx &merge_ctx, ObIAllocator &allocator)
    : merge_ctx_(merge_ctx),
      allocator_(allocator),
      tbl_read_info_(),
      iter_(nullptr),
      is_inited_(false)
{
}

ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::~ObIncrementalIterator()
{
  reset();
}

void ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::reset()
{
  rowkey_col_ids_.reset();
  out_cols_project_.reset();
  tbl_read_info_.reset();
  tbl_xs_param_.reset();
  store_ctx_.reset();
  tbl_xs_ctx_.reset();
  tbls_iter_.reset();
  get_tbl_param_.reset();
  range_to_scan_.reset();
  if (iter_ != nullptr) {
    iter_->~ObIStoreRowIterator();
    iter_ = nullptr;
  }
  is_inited_ = false;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIncrementalIterator init twice", KR(ret));
  } else {
    ObMultipleScanMerge *mpl_scan_mrg = nullptr;
    range_to_scan_.set_whole_range();
    if (OB_ISNULL(mpl_scan_mrg = OB_NEWx(ObMultipleScanMerge, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", KR(ret));
    } else if (OB_FAIL(prepare_table_access_param())) {
      STORAGE_LOG(WARN, "Failed to prepare table access param", KR(ret));
    } else if (OB_FAIL(prepare_store_ctx())) {
      STORAGE_LOG(WARN, "Failed to prepare store ctx", KR(ret));
    } else if (OB_FAIL(prepare_table_access_context())) {
      STORAGE_LOG(WARN, "Failed to prepare table access context", KR(ret));
    } else if (OB_FAIL(prepare_get_table_param())) {
      STORAGE_LOG(WARN, "Failed to prepare get table param", KR(ret));
    } else if (OB_FAIL(mpl_scan_mrg->init(tbl_xs_param_, tbl_xs_ctx_, get_tbl_param_))) {
      STORAGE_LOG(WARN, "Failed to init multiple scan merge", KR(ret));
    } else if (OB_FAIL(mpl_scan_mrg->open(range_to_scan_))) {
      STORAGE_LOG(WARN, "Failed to open multiple scan merge", KR(ret));
    } else {
      mpl_scan_mrg->set_iter_del_row(true);
      iter_ = mpl_scan_mrg;
      is_inited_ = true;
    }
    if (OB_FAIL(ret)) {
      if (mpl_scan_mrg != nullptr) {
        mpl_scan_mrg->~ObMultipleScanMerge();
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::get_next_row(
  const ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIncrementalIterator not init", KR(ret));
  } else {
    ret = iter_->get_next_row(row);
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_table_access_param()
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *storage_schema = merge_ctx_.get_schema();
  if (OB_FAIL(storage_schema->get_mulit_version_rowkey_column_ids(rowkey_col_ids_))) {
    STORAGE_LOG(WARN, "Failed to get rowkey column ids", KR(ret));
  } else if (OB_FAIL(tbl_read_info_.init(allocator_, storage_schema->get_column_count(),
                                         storage_schema->get_rowkey_column_num(),
                                         lib::is_oracle_mode(), rowkey_col_ids_))) {
    STORAGE_LOG(WARN, "Failed to init columns info", KR(ret));
  } else if (OB_FAIL(tbl_xs_param_.init_merge_param(
               merge_ctx_.get_tablet_id().id(), merge_ctx_.get_tablet_id(), tbl_read_info_))) {
    STORAGE_LOG(WARN, "Failed to init table access param", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_col_ids_.count(); i++) {
    if (OB_FAIL(out_cols_project_.push_back(static_cast<int32_t>(i)))) {
      STORAGE_LOG(WARN, "Failed to push column project", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    tbl_xs_param_.iter_param_.out_cols_project_ = &out_cols_project_;
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_store_ctx()
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = merge_ctx_.get_ls_id();
  int64_t snapshot = merge_ctx_.get_snapshot();
  SCN scn;
  if (OB_FAIL(scn.convert_for_tx(snapshot))) {
    STORAGE_LOG(WARN, "convert for tx fail", K(ret), K(ls_id), K(snapshot));
  } else if (OB_FAIL(store_ctx_.init_for_read(ls_id,
                                              merge_ctx_.get_tablet_id(),
                                              INT64_MAX,
                                              -1,
                                              scn))) {
    STORAGE_LOG(WARN, "init store ctx fail", K(ret), K(ls_id), K(snapshot));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_table_access_context()
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag(ObQueryFlag::Forward,
                         true  /*daily_merge*/,
                         true  /*optimize*/,
                         true  /*whole_macro_scan*/,
                         false /*full_row*/,
                         false /*index_back*/,
                         false /*query_stat*/
                         );
  ObITable *major_sstable = merge_ctx_.get_tables_handle().get_table(0);
  ObVersionRange scan_version_range = merge_ctx_.static_param_.version_range_;
  scan_version_range.base_version_ = major_sstable->get_snapshot_version();
  if (OB_FAIL(tbl_xs_ctx_.init(query_flag, store_ctx_, allocator_, allocator_, scan_version_range))) {
    STORAGE_LOG(WARN, "Failed to init table access context", KR(ret));
  } else {
    tbl_xs_ctx_.merge_scn_ = merge_ctx_.static_param_.merge_scn_;
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::ObIncrementalIterator::prepare_get_table_param()
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  for (int64_t i = 1; OB_SUCC(ret) && i < merge_ctx_.get_tables_handle().get_count(); i++) {
    table = merge_ctx_.get_tables_handle().get_table(i);
    if (OB_FAIL(tbls_iter_.add_table(table))) {
      STORAGE_LOG(WARN, "Failed to add table to inc handle", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_tbl_param_.tablet_iter_.table_iter()->assign(tbls_iter_))) {
      STORAGE_LOG(WARN, "Failed to assign tablet iterator", KR(ret));
    }
  }
  return ret;
}

ObPartitionIncrementalRangeSpliter::ObPartitionIncrementalRangeSpliter()
    : merge_ctx_(nullptr),
      allocator_(nullptr),
      major_sstable_(nullptr),
      tablet_size_(-1),
      iter_(nullptr),
      default_noisy_row_num_skipped_(DEFAULT_NOISY_ROW_NUM_SKIPPED),
      default_row_num_per_range_(DEFAULT_ROW_NUM_PER_RANGE),
      inc_ranges_(nullptr),
      base_ranges_(nullptr),
      combined_ranges_(nullptr),
      is_inited_(false)
{
}

ObPartitionIncrementalRangeSpliter::~ObPartitionIncrementalRangeSpliter()
{
  if (nullptr != iter_) {
    iter_->~ObIncrementalIterator();
    iter_ = nullptr;
  }
  if (nullptr != combined_ranges_) {
    combined_ranges_->~ObIArray<ObDatumRange>();
    combined_ranges_ = nullptr;
  }
  if (nullptr != base_ranges_) {
    base_ranges_->~ObIArray<ObDatumRange>();
    base_ranges_ = nullptr;
  }
  if (nullptr != inc_ranges_) {
    inc_ranges_->~ObIArray<ObDatumRange>();
    inc_ranges_ = nullptr;
  }
}

int ObPartitionIncrementalRangeSpliter::init(compaction::ObTabletMergeCtx &merge_ctx,
                                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObTablesHandleArray &tables_handle = merge_ctx.get_tables_handle();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter init twice", KR(ret));
  } else if (OB_UNLIKELY(tables_handle.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument tables handle (empty)", KR(ret), K(tables_handle));
  } else if (OB_ISNULL(tables_handle.get_table(0))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "unexpected null first table", KR(ret), K(tables_handle));
  } else if (OB_UNLIKELY(!tables_handle.get_table(0)->is_major_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "first table must be major sstable", KR(ret), K(tables_handle));
  } else {
    merge_ctx_ = &merge_ctx;
    allocator_ = &allocator;
    // TODO(@DanLing) use cg table in co dag_net
    // the first table is ObSSTable in unit test
    major_sstable_ = static_cast<ObSSTable *>(tables_handle.get_table(0));
    tablet_size_ = merge_ctx.get_schema()->get_tablet_size();
    if (OB_UNLIKELY(tablet_size_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument tablet size", KR(ret), K_(tablet_size));
    } else if (OB_FAIL(alloc_ranges())) {
      STORAGE_LOG(WARN, "failed to alloc ranges", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::alloc_ranges()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t ranges_size = sizeof(ObSEArray<ObDatumRange, 64>);
  if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(ranges_size * 3)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to allocate memory", KR(ret), "size", ranges_size * 3);
  } else {
    inc_ranges_ = new (buf) ObSEArray<ObDatumRange, 64>();
    base_ranges_ = new (buf + ranges_size) ObSEArray<ObDatumRange, 64>();
    combined_ranges_ = new (buf + ranges_size * 2) ObSEArray<ObDatumRange, 64>();
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::init_incremental_iter()
{
  int ret = OB_SUCCESS;
  if (nullptr != iter_) {
    iter_->reset();
  } else {
    if (OB_ISNULL(iter_ = OB_NEWx(ObIncrementalIterator, allocator_, *merge_ctx_, *allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(iter_->init())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_major_sstable_end_rowkey(ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(major_sstable_->get_last_rowkey(*allocator_, rowkey))) {
    STORAGE_LOG(WARN, "failed to get major sstable last rowkey", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::scan_major_sstable_secondary_meta(
    const ObDatumRange &scan_range, ObSSTableSecMetaIterator *&meta_iter)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo &rowkey_read_info =
    merge_ctx_->get_tablet()->get_rowkey_read_info();
  if (OB_FAIL(major_sstable_->scan_secondary_meta(*allocator_, scan_range, rowkey_read_info,
                                                  DATA_BLOCK_META, meta_iter))) {
    STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::check_is_incremental(bool &is_incremental)
{
  int ret = OB_SUCCESS;
  const int64_t tables_handle_cnt = merge_ctx_->get_tables_handle().get_count();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter not init", KR(ret));
  } else if (tables_handle_cnt <= 1) {
    // no incremental data
    is_incremental = false;
  } else if (major_sstable_->is_empty()) {
    // no base data
    is_incremental = true;
  } else {
    const ObDatumRow *row = nullptr;
    int64_t row_count = 0;
    is_incremental = false;
    if (OB_FAIL(init_incremental_iter())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    } else {
      // skip a few rows to avoid updated noise
      while (OB_SUCC(ret) && OB_SUCC(iter_->get_next_row(row)) && OB_NOT_NULL(row) &&
            ++row_count <= default_noisy_row_num_skipped_);
      if (OB_ITER_END == ret) {
        STORAGE_LOG(DEBUG, "incremental row num less than skipped num");
        ret = OB_SUCCESS;
      } else if (OB_SUCC(ret) && row_count > default_noisy_row_num_skipped_) {
        // compare with base sstable last rowkey
        int cmp_ret = 0;
        ObDatumRowkey row_rowkey;
        ObDatumRowkey end_rowkey;
        const int64_t rowkey_column_num = merge_ctx_->get_schema()->get_rowkey_column_num();
        const ObStorageDatumUtils &datum_utils =
          merge_ctx_->get_tablet()->get_rowkey_read_info().get_datum_utils();
        if (OB_FAIL(row_rowkey.assign(row->storage_datums_, rowkey_column_num))) {
          STORAGE_LOG(WARN, "failed to assign datum rowkey", KR(ret), K(*row),
                      K(rowkey_column_num));
        } else if (OB_FAIL(get_major_sstable_end_rowkey(end_rowkey))) {
          STORAGE_LOG(WARN, "failed to get base sstable last rowkey", KR(ret), K(*major_sstable_));
        } else if (OB_FAIL(row_rowkey.compare(end_rowkey, datum_utils, cmp_ret))) {
          STORAGE_LOG(WARN, "failed to compare rowkey", KR(ret), K(row_rowkey), K(end_rowkey));
        } else if (cmp_ret > 0) {
          is_incremental = true;
        }
        STORAGE_LOG(DEBUG, "cmp rowkey", KR(ret), K(cmp_ret), K(row_rowkey), K(end_rowkey));
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::split_ranges(ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  result_ranges.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionIncrementalRangeSpliter not init", KR(ret));
  } else if (tablet_size_ == 0) {
    ObDatumRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(result_ranges.push_back(whole_range))) {
      STORAGE_LOG(WARN, "failed to push back merge range to array", KR(ret), K(whole_range));
    }
  } else {
    ObDatumRangeArray *ranges = nullptr;
    if (OB_FAIL(get_ranges_by_inc_data(*inc_ranges_))) {
      STORAGE_LOG(WARN, "failed to get ranges by inc data", KR(ret));
    } else if (merge_ctx_->get_is_full_merge()) {
      if (major_sstable_->is_empty()) {
        ranges = inc_ranges_;
      } else if (OB_FAIL(get_ranges_by_base_sstable(*base_ranges_))) {
        STORAGE_LOG(WARN, "failed to get ranges by base sstable", KR(ret));
      } else if (OB_FAIL(combine_ranges(*base_ranges_, *inc_ranges_, *combined_ranges_))) {
        STORAGE_LOG(WARN, "failed to combine base and inc ranges", KR(ret));
      } else {
        ranges = combined_ranges_;
      }
    } else {
      ranges = inc_ranges_;
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(ranges)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected null ranges", K(ret), K(ranges));
      } else if (OB_FAIL(merge_ranges(*ranges, result_ranges))) {
        STORAGE_LOG(WARN, "failed to merge ranges", KR(ret));
      }
    }

    if (OB_SUCC(ret) && result_ranges.count() > 0) {
      const ObStorageDatumUtils &datum_utils =
        merge_ctx_->get_tablet()->get_rowkey_read_info().get_datum_utils();
      if (OB_FAIL(check_continuous(datum_utils, result_ranges))) {
        STORAGE_LOG(WARN, "failed to check continuous", KR(ret), K(result_ranges));
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_ranges_by_inc_data(ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  const int64_t tables_handle_cnt = merge_ctx_->get_tables_handle().get_count();
  ranges.reset();
  if (OB_UNLIKELY(tablet_size_ <= 0 || tables_handle_cnt <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K_(tablet_size), K(tables_handle_cnt));
  } else {
    if (OB_FAIL(init_incremental_iter())) {
      STORAGE_LOG(WARN, "failed to init incremental iterator", KR(ret));
    } else {
      int64_t num_rows_per_range = default_row_num_per_range_;
      // calculate num_rows_per_range by macro block
      const int64_t macro_block_count = major_sstable_->get_data_macro_block_count();
      const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
      if (macro_block_count * macro_block_size > tablet_size_) {
        const int64_t row_count = major_sstable_->get_row_count();
        const int64_t num_rows_per_macro_block = row_count / macro_block_count;
        num_rows_per_range = num_rows_per_macro_block * tablet_size_ / macro_block_size;
      }

      if (OB_SUCC(ret)) {
        // to avoid block the macro-block, simply make num_rows_per_range >= DEFAULT_NOISY_ROW_NUM_SKIPPED
        if (num_rows_per_range < default_noisy_row_num_skipped_) {
          num_rows_per_range = default_noisy_row_num_skipped_;
        }
      }

      const int64_t rowkey_column_num = merge_ctx_->get_schema()->get_rowkey_column_num();
      int64_t count = 0;
      const ObDatumRow *row = nullptr;
      ObDatumRowkey rowkey;
      ObDatumRange range;
      ObDatumRange multi_version_range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();

      // generate ranges
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter_->get_next_row(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            STORAGE_LOG(WARN, "failed to get nex row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected null row", KR(ret));
        } else if (++count >= num_rows_per_range) {
          count = 0;
          range.start_key_ = range.end_key_;
          if (OB_FAIL(rowkey.assign(row->storage_datums_, rowkey_column_num))) {
            STORAGE_LOG(WARN, "failed to assign datum rowkey", KR(ret), K(*row),
                        K(rowkey_column_num));
          } else if (OB_FAIL(rowkey.deep_copy(range.end_key_, *allocator_))) {
            STORAGE_LOG(WARN, "failed to deep copy datum rowkey", KR(ret), K(rowkey));
          } else if (OB_FAIL(range.to_multi_version_range(*allocator_, multi_version_range))) {
            STORAGE_LOG(WARN, "failed to transfer multi version range", KR(ret), K(range));
          } else if (OB_FAIL(ranges.push_back(multi_version_range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        }
      }

      if (OB_SUCC(ret)) {
        // handle the last range
        if (count > 0) {
          range.start_key_ = range.end_key_;
          range.end_key_.set_max_rowkey();
          range.set_right_open();
          if (OB_FAIL(range.to_multi_version_range(*allocator_, multi_version_range))) {
            STORAGE_LOG(WARN, "failed to transfer multi version range", KR(ret), K(range));
          } else if (OB_FAIL(ranges.push_back(multi_version_range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        } else if (ranges.empty()) {
          range.set_whole_range();
          if (OB_FAIL(ranges.push_back(range))) {
            STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
          }
        } else {
          ObDatumRange &last_range = ranges.at(ranges.count() - 1);
          last_range.end_key_.set_max_rowkey();
          last_range.set_right_open();
        }
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::get_ranges_by_base_sstable(ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  ranges.reset();
  if (OB_UNLIKELY(tablet_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get ranges by base sstable", KR(ret), K_(tablet_size));
  } else {
    ObSSTableSecMetaIterator *meta_iter = nullptr;
    ObDatumRange scan_range;
    scan_range.set_whole_range();
    if (OB_FAIL(scan_major_sstable_secondary_meta(scan_range, meta_iter))) {
      STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), K(*major_sstable_));
    } else {
      const int64_t macro_block_cnt =
        major_sstable_->get_data_macro_block_count();
      const int64_t total_size = macro_block_cnt * OB_SERVER_BLOCK_MGR.get_macro_block_size();
      const int64_t range_cnt = (total_size + tablet_size_ - 1) / tablet_size_;
      const int64_t macro_block_cnt_per_range = (macro_block_cnt + range_cnt - 1) / range_cnt;

      ObDatumRowkey endkey;
      ObDatumRange range;
      range.end_key_.set_min_rowkey();
      range.set_left_open();
      range.set_right_closed();
      ObDataMacroBlockMeta blk_meta;

      // generate ranges
      for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt;) {
        const int64_t last = i + macro_block_cnt_per_range - 1;
        range.start_key_ = range.end_key_;
        if (last < macro_block_cnt - 1) {
          // locate to the last macro-block meta in current range
          while (OB_SUCC(meta_iter->get_next(blk_meta)) && i++ < last);
          if (OB_FAIL(ret)) {
            STORAGE_LOG(WARN, "Failed to get macro block meta", KR(ret), K(i - 1));
          } else if (OB_UNLIKELY(!blk_meta.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "Unexpected invalid macro block meta", KR(ret), K(i - 1));
          } else if (OB_FAIL(blk_meta.get_rowkey(endkey))) {
            STORAGE_LOG(WARN, "Failed to get rowkey", KR(ret), K(blk_meta));
          } else if (OB_FAIL(endkey.deep_copy(range.end_key_, *allocator_))) {
            STORAGE_LOG(WARN, "Failed to transfer store rowkey", K(ret), K(endkey));
          }
        } else { // last range
          i = last + 1;
          range.end_key_.set_max_rowkey();
          range.set_right_open();
        }
        if (OB_SUCC(ret) && OB_FAIL(ranges.push_back(range))) {
          STORAGE_LOG(WARN, "Failed to push range", KR(ret), K(ranges), K(range));
        }
      }
    }
    if (OB_NOT_NULL(meta_iter)) {
      meta_iter->~ObSSTableSecMetaIterator();
      meta_iter = nullptr;
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::combine_ranges(const ObDatumRangeArray &base_ranges,
                                                       const ObDatumRangeArray &inc_ranges,
                                                       ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_ranges.empty() || inc_ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument to combine ranges", KR(ret),
                K(base_ranges), K(inc_ranges));
  } else {
    // incremental data is appended after base sstable
    // so just append the inc ranges to the base ranges
    result_ranges.reset();

    ObDatumRowkey end_rowkey;
    if (OB_FAIL(get_major_sstable_end_rowkey(end_rowkey))) {
      STORAGE_LOG(WARN, "failed to get base sstable last rowkey", KR(ret), K_(major_sstable));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < base_ranges.count(); i++) {
        const ObDatumRange &range = base_ranges.at(i);
        if (OB_FAIL(result_ranges.push_back(range))) {
          STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < inc_ranges.count(); i++) {
        const ObDatumRange &range = inc_ranges.at(i);
        if (OB_FAIL(result_ranges.push_back(range))) {
          STORAGE_LOG(WARN, "failed to push range", KR(ret), K(range));
        }
      }
      if (OB_SUCC(ret)) {
        // base_ranges和inc_ranges交接的地方 (k1, MAX) (MIN, k2] 改成 （k1, endkey] (endkey, k2]
        ObDatumRange &base_last_range = result_ranges.at(base_ranges.count() - 1);
        ObDatumRange &inc_first_range = result_ranges.at(base_ranges.count());
        if (OB_FAIL(end_rowkey.deep_copy(base_last_range.end_key_, *allocator_))) {
          STORAGE_LOG(WARN, "failed to deep copy rowkey", KR(ret), K(end_rowkey),
                      K(base_last_range.get_end_key()));
        } else {
          base_last_range.set_right_closed();
          inc_first_range.start_key_ = base_last_range.end_key_;
        }
      }
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::merge_ranges(const ObDatumRangeArray &ranges,
                                                     ObDatumRangeArray &result_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(ranges));
  } else {
    const int64_t num_ranges_per_thread = (ranges.count() + MAX_MERGE_THREAD - 1) / MAX_MERGE_THREAD;
    ObDatumRange merged_range;
    const ObDatumRange *range;
    merged_range.end_key_.set_min_rowkey();
    merged_range.set_left_open();
    merged_range.set_right_closed();
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); i += num_ranges_per_thread) {
      if (num_ranges_per_thread == 1) {
        range = &ranges.at(i);
      } else {
        int64_t last = i + num_ranges_per_thread - 1;
        last = (last < ranges.count() ? last : ranges.count() - 1);
        const ObDatumRange &first_range = ranges.at(i);
        const ObDatumRange &last_range = ranges.at(last);
        // merged_range.end_key_ should be the same as first_range.start_key_
        merged_range.set_start_key(first_range.get_start_key());
        merged_range.set_end_key(last_range.get_end_key());
        range = &merged_range;
      }

      if (OB_FAIL(result_ranges.push_back(*range))) {
        STORAGE_LOG(WARN, "failed to push range", KR(ret), K(*range));
      }
    }
    if (OB_SUCC(ret)) {
      (result_ranges.at(result_ranges.count() - 1)).set_right_open();
    }
  }
  return ret;
}

int ObPartitionIncrementalRangeSpliter::check_continuous(const ObStorageDatumUtils &datum_utils,
                                                         ObDatumRangeArray &ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.empty())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (empty)", K(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(0).get_start_key().is_min_rowkey() ||
                         !ranges.at(0).is_left_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (first start key not min)", K(ret), K(ranges));
  } else if (OB_UNLIKELY(!ranges.at(ranges.count() - 1).get_end_key().is_max_rowkey() ||
                         !ranges.at(ranges.count() - 1).is_right_open())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected bad ranges (last end key not max)", K(ret), K(ranges));
  } else {
    bool is_equal = false;
    for (int64_t i = 1; OB_SUCC(ret) && i < ranges.count(); i++) {
      if (OB_UNLIKELY(!ranges.at(i - 1).is_right_closed())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (end key not included)", K(ret), K(i), K(ranges));
      } else if (OB_UNLIKELY(!ranges.at(i).is_left_open())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (start key not excluded)", K(ret), K(i), K(ranges));
      } else if (OB_FAIL(ranges.at(i).get_start_key().equal(ranges.at(i - 1).get_end_key(),
                                                            datum_utils, is_equal))) {
        STORAGE_LOG(WARN, "Failed to compare rowkeys", K(ret), K(i), K(ranges));
      } else if (OB_UNLIKELY(!is_equal)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected bad ranges (not contiguous)", K(ret), K(i), K(ranges));
      }
    }
  }
  return ret;
}

}
}
