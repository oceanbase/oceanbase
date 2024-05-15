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

#include "storage/access/ob_sample_iter_helper.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/access/ob_multiple_multi_scan_merge.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase {
namespace storage {

using namespace blocksstable;

int ObGetSampleIterHelper::check_scan_range_count(bool &res, ObIArray<ObDatumRange> &sample_ranges)
{
  int ret = OB_SUCCESS;
  need_scan_multiple_range_ = false;
  if (scan_param_.sample_info_.is_block_sample()) {
    bool retire_to_memtable_row_sample = false;
    if (!get_table_param_.tablet_iter_.table_iter()->is_valid() &&
        OB_FAIL(get_table_param_.tablet_iter_.refresh_read_tables_from_tablet(
            main_table_ctx_.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
            false /*allow_not_ready*/))) {
      STORAGE_LOG(WARN, "Fail to read tables", K(ret));
    } else if (OB_FAIL(can_retire_to_memtable_row_sample_(retire_to_memtable_row_sample, sample_ranges))) {
      STORAGE_LOG(WARN, "Fail to try to retire to row sample", K(ret));
    }

    if (retire_to_memtable_row_sample) {
      need_scan_multiple_range_ = true;
    }
  } else if (scan_param_.sample_info_.is_row_sample()) {
    if (OB_FAIL(sample_ranges.assign(table_scan_range_.get_ranges()))) {
      STORAGE_LOG(WARN,
                  "copy assign from table scan range to sample ranges failed",
                  KR(ret),
                  K(table_scan_range_.get_ranges()));
    } else {
      need_scan_multiple_range_ = true;
    }
  } else {
    // invalid sample type
  }

  if (OB_SUCC(ret)) {
    res = need_scan_multiple_range_;
  }
  return ret;
}

int ObGetSampleIterHelper::can_retire_to_memtable_row_sample_(bool &retire, ObIArray<ObDatumRange> &sample_ranges)
{
  int ret = OB_SUCCESS;

  retire = false;
  if (get_table_param_.is_valid()) {
    int64_t memtable_row_count = 0;
    int64_t sstable_row_count = 0;
    common::ObSEArray<memtable::ObMemtable *, 4> memtables;

    // iter all tables to estimate row count
    get_table_param_.tablet_iter_.table_iter()->resume();
    while (OB_SUCC(ret)) {
      ObSSTableMetaHandle sst_meta_hdl;
      ObITable *table = nullptr;
      if (OB_FAIL(get_table_param_.tablet_iter_.table_iter()->get_next(table))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "Fail to get next table iter", K(ret), K(get_table_param_.tablet_iter_.table_iter()));
        }
      } else if (table->is_data_memtable()) {
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
        if (OB_FAIL(memtables.push_back(memtable))) {
          STORAGE_LOG(WARN, "push back memtable failed", KR(ret));
        } else {
          memtable_row_count += memtable->get_physical_row_cnt();
        }
      } else if (table->is_direct_load_memtable()) {
        ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(table);
        sstable_row_count += ddl_kv->get_row_count();
      } else if (table->is_sstable()) {
        sstable_row_count += static_cast<ObSSTable *>(table)->get_row_count();
      }
    }

    // decide if this block sample need to retire and get sample ranges if need retire
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(get_table_param_.tablet_iter_.table_iter()->resume())) {
    } else if (sstable_row_count < memtable_row_count && memtable_row_count > 0) {
      if (OB_FAIL(get_memtable_sample_ranges_(memtables, sample_ranges))) {
        STORAGE_LOG(WARN, "get memtable sample ranges failed", KR(ret), K(memtables));
      } else {
        retire = true;
      }

      STORAGE_LOG(INFO,
                  "retire to memtable row sample",
                  KR(ret),
                  K(retire),
                  K(sstable_row_count),
                  K(memtable_row_count),
                  K(table_scan_range_));
    }
  }

  return ret;
}

int ObGetSampleIterHelper::get_memtable_sample_ranges_(const ObIArray<memtable::ObMemtable *> &memtables,
                                                       ObIArray<ObDatumRange> &sample_ranges)
{
  int ret = OB_SUCCESS;
  int split_failed_count = 0;
  sample_ranges.reuse();

  // get split ranges from all memtables
  for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); i++) {
    memtable::ObMemtable *memtable = memtables.at(i);
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(memtable)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_TMP_FAIL(memtable->split_ranges_for_sample(table_scan_range_.get_ranges().at(0),
                                                             scan_param_.sample_info_.percent_,
                                                             *(scan_param_.allocator_),
                                                             sample_ranges))) {
      STORAGE_LOG(WARN, "split range failed", KR(tmp_ret));
      split_failed_count++;
    } else {
      // split succeed
    }
  }

  // if we can not split ranges from all memtables, just push a whole range into sample ranges array
  if (split_failed_count == memtables.count()) {
    if (sample_ranges.count() != 0) {
      STORAGE_LOG(WARN, "unexpected sample memtable ranges", K(sample_ranges));
      sample_ranges.reuse();
    }

    blocksstable::ObDatumRange datum_range;
    datum_range.set_whole_range();
    if (OB_FAIL(sample_ranges.push_back(datum_range))) {
      STORAGE_LOG(WARN, "push back datum range to sample memtable ranges failed", KR(ret), K(memtables));
    }
  }
  return ret;
}

#define CONSTRUCT_SAMPLE_ITER(ITER_TYPE, ITER_PTR)                             \
  do {                                                                         \
    void *buf = nullptr;                                                       \
    if (OB_FAIL(ret)) {                                                        \
    } else if (nullptr == ITER_PTR) {                                          \
      if (OB_ISNULL(buf = scan_param_.allocator_->alloc(sizeof(ITER_TYPE)))) { \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                       \
        STORAGE_LOG(WARN, "Fail to allocate memory", K(ret));                  \
      } else {                                                                 \
        ITER_PTR = new (buf) ITER_TYPE(scan_param_.sample_info_);              \
      }                                                                        \
    }                                                                          \
  } while (0)

#define OPEN_SAMPLE_ITER(ITER_TYPE, ITER_PTR)                                    \
  do {                                                                           \
    if (OB_FAIL(ret)) {                                                          \
    } else if (OB_FAIL(static_cast<ITER_TYPE *>(ITER_PTR)->open(*main_iter))) {  \
      STORAGE_LOG(WARN, "failed to open memtable_row_sample_iterator", KR(ret)); \
    } else {                                                                     \
      main_iter = ITER_PTR;                                                      \
    }                                                                            \
  } while (0)

int ObGetSampleIterHelper::get_sample_iter(ObMemtableRowSampleIterator *&sample_iter,
                                           ObQueryRowIterator *&main_iter,
                                           ObMultipleScanMerge *scan_merge)
{
  int ret = OB_SUCCESS;
  CONSTRUCT_SAMPLE_ITER(ObMemtableRowSampleIterator, sample_iter);
  OPEN_SAMPLE_ITER(ObMemtableRowSampleIterator, sample_iter);
  return ret;
}

int ObGetSampleIterHelper::get_sample_iter(ObBlockSampleIterator *&sample_iter,
                                           ObQueryRowIterator *&main_iter,
                                           ObMultipleScanMerge *scan_merge)
{
  int ret = OB_SUCCESS;
  CONSTRUCT_SAMPLE_ITER(ObBlockSampleIterator, sample_iter);
  // TODO : @yuanzhe block sample uses a different initialization logic and different open interface
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(static_cast<ObBlockSampleIterator *>(sample_iter)
                         ->open(*scan_merge,
                                main_table_ctx_,
                                table_scan_range_.get_ranges().at(0),
                                get_table_param_,
                                scan_param_.scan_flag_.is_reverse_scan()))) {
    STORAGE_LOG(WARN, "failed to open block_sample_iterator_", K(ret));
  } else {
    main_iter = sample_iter;
    main_table_ctx_.use_fuse_row_cache_ = false;
  }
  return ret;
}

#undef CONSTRUCT_SAMPLE_ITER
#undef OPEN_SAMPLE_ITER

}  // namespace storage
}  // namespace oceanbase
