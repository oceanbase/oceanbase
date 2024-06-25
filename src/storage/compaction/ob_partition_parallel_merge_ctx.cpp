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

#include "ob_partition_parallel_merge_ctx.h"
#include "storage/memtable/ob_memtable.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/ob_partition_range_spliter.h"
#include "ob_tablet_merge_ctx.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/compaction/ob_compaction_dag_ranker.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;
using namespace compaction;

namespace storage
{

ObParallelMergeCtx::ObParallelMergeCtx(common::ObIAllocator &allocator)
  : allocator_(allocator),
    range_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
    parallel_type_(INVALID_PARALLEL_TYPE),
    concurrent_cnt_(0),
    is_inited_(false)
{
}

ObParallelMergeCtx::~ObParallelMergeCtx()
{
  reset();
}

void ObParallelMergeCtx::reset()
{
  parallel_type_ = INVALID_PARALLEL_TYPE;
  range_array_.reset();
  concurrent_cnt_ = 0;
  is_inited_ = false;
}

bool ObParallelMergeCtx::is_valid() const
{
  bool bret = true;
  if (IS_NOT_INIT || concurrent_cnt_ <= 0 || parallel_type_ >= INVALID_PARALLEL_TYPE) {
    bret = false;
  } else if (range_array_.count() != concurrent_cnt_) {
    bret = false;
  } else if (concurrent_cnt_ > 1 && SERIALIZE_MERGE == parallel_type_) {
    bret = false;
  }
  return bret;
}

int ObParallelMergeCtx::init(compaction::ObBasicTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  int64_t tablet_size = 0;
  bool enable_parallel_minor_merge = false;
  const ObMergeType merge_type = merge_ctx.get_merge_type();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObParallelMergeCtx init twice", K(ret));
  } else if (OB_UNLIKELY(!merge_ctx.is_schema_valid() || merge_ctx.get_tables_handle().empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel merge", K(ret), K(merge_ctx));
  } else if (FALSE_IT(tablet_size = merge_ctx.get_schema()->get_tablet_size())) {
  } else if (!merge_ctx.get_need_parallel_minor_merge()) {
    //TODO(jinyu) backfill need using same code with minor merge in 4.2 RC3.
    enable_parallel_minor_merge = false;
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      enable_parallel_minor_merge = tenant_config->_enable_parallel_minor_merge;
    }
  }

  if (OB_FAIL(ret)) {
  } else if ((tablet_size <= 0
          || (!enable_parallel_minor_merge && !is_major_merge_type(merge_type))
          || (is_mini_merge(merge_type) && MTL(ObTenantCompactionMemPool *)->is_emergency_mode()))) {
    if (OB_FAIL(init_serial_merge())) {
      STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret), K(tablet_size), K(merge_ctx));
    }
  } else if (is_major_or_meta_merge_type(merge_type)) {
    if (OB_FAIL(init_parallel_major_merge(merge_ctx))) {
      STORAGE_LOG(WARN, "Failed to init parallel major merge", K(ret));
    }
  } else if (is_mini_merge(merge_type)) {
    if (OB_FAIL(init_parallel_mini_merge(merge_ctx))) {
      STORAGE_LOG(WARN, "Failed to init parallel setting for mini merge", K(ret));
    }
  } else if (is_minor_merge(merge_type)) {
    if (OB_FAIL(init_parallel_mini_minor_merge(merge_ctx))) {
      STORAGE_LOG(WARN, "Failed to init parallel setting for mini minor merge", K(ret));
    }
  } else {
    // just use serial merge
    if (OB_FAIL(init_serial_merge())) {
      STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret), K(tablet_size), K(merge_ctx));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    STORAGE_LOG(INFO, "Succ to init parallel merge ctx", K(ret),
        K(enable_parallel_minor_merge), K(tablet_size), K(merge_ctx.static_param_), K(concurrent_cnt_));
  }

  return ret;
}

int ObParallelMergeCtx::init(const compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObParallelMergeCtx init twice", K(ret));
  } else if (OB_UNLIKELY(!medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel merge", K(ret), K(medium_info));
  } else {
    ObDatumRange schema_rowkey_range;
    ObDatumRange multi_version_range;
    const compaction::ObParallelMergeInfo &paral_info = medium_info.parallel_merge_info_;
    if (OB_UNLIKELY(!paral_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "parallel info is invalid", KR(ret), K(paral_info));
    } else {
      range_array_.reset();

      schema_rowkey_range.start_key_.set_min_rowkey();
      schema_rowkey_range.end_key_.set_min_rowkey();
      schema_rowkey_range.set_left_open();
      schema_rowkey_range.set_right_closed();
    }

    for (int i = 0; OB_SUCC(ret) && i < paral_info.get_size() + 1; ++i) {
      if (i > 0 && OB_FAIL(schema_rowkey_range.end_key_.deep_copy(schema_rowkey_range.start_key_, allocator_))) { // end_key -> start_key
        STORAGE_LOG(WARN, "failed to deep copy start key", K(ret), K(i), K(medium_info));
      } else if (i < paral_info.get_size()) {
        if (OB_FAIL(paral_info.deep_copy_datum_rowkey(i/*idx*/, allocator_, schema_rowkey_range.end_key_))) {
          STORAGE_LOG(WARN, "failed to deep copy end key", K(ret), K(i), K(medium_info));
        }
      } else { // i == paral_info.get_size()
        schema_rowkey_range.end_key_.set_max_rowkey();
      }
      multi_version_range.reset();
      if (FAILEDx(schema_rowkey_range.to_multi_version_range(allocator_, multi_version_range))) {
        STORAGE_LOG(WARN, "failed to convert multi_version range", K(ret), K(schema_rowkey_range));
      } else if (OB_FAIL(range_array_.push_back(multi_version_range))) {
        STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(multi_version_range));
      }
    }
    if (OB_SUCC(ret)) {
      concurrent_cnt_ = paral_info.get_size() + 1;
      parallel_type_ = PARALLEL_MAJOR;
      is_inited_ = true;
      STORAGE_LOG(INFO, "success to init parallel merge ctx from medium_info", K(ret), KPC(this), K(paral_info));
    }
  }
  return ret;
}


int ObParallelMergeCtx::get_merge_range(const int64_t parallel_idx, ObDatumRange &merge_range)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObParallelMergeCtx is not inited", K(ret), K(*this));
  } else if (parallel_idx >= concurrent_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get parallel mergerange", K(ret), K(parallel_idx),
                K_(concurrent_cnt));
  } else {
    switch (parallel_type_) {
      case PARALLEL_MAJOR:
      case PARALLEL_MINI:
      case PARALLEL_MINOR:
      case SERIALIZE_MERGE:
        merge_range = range_array_.at(parallel_idx);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "Unexpected parallel merge type", K(ret), K(*this));
        break;
    }
  }

  return ret;
}

int ObParallelMergeCtx::init_serial_merge()
{
  int ret = OB_SUCCESS;
  ObDatumRange merge_range;
  merge_range.set_whole_range();
  range_array_.reset();
  if (OB_FAIL(range_array_.push_back(merge_range))) {
    STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(merge_range));
  } else {
    concurrent_cnt_ = 1;
    parallel_type_ = SERIALIZE_MERGE;
  }

  return ret;
}

int ObParallelMergeCtx::init_parallel_major_merge(compaction::ObBasicTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  const ObITable *first_table = nullptr;
  if (OB_UNLIKELY(!is_major_or_meta_merge_type(merge_ctx.get_merge_type()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel major merge", K(ret), K(merge_ctx));
  } else if (OB_UNLIKELY(nullptr == (first_table = merge_ctx.get_tables_handle().get_table(0))
      || !first_table->is_sstable())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected first table", K(ret), K(merge_ctx.get_tables_handle()));
  } else {
    const int64_t tablet_size = merge_ctx.get_schema()->get_tablet_size();
    const ObSSTable *first_sstable = static_cast<const ObSSTable *>(first_table);
    const int64_t macro_block_cnt = first_sstable->get_data_macro_block_count();
    if (OB_FAIL(get_concurrent_cnt(tablet_size, macro_block_cnt, concurrent_cnt_))) {
      STORAGE_LOG(WARN, "failed to get concurrent cnt", K(ret), K(tablet_size), K(concurrent_cnt_),
        KPC(first_sstable));
    } else if (1 >= concurrent_cnt_) {
      if (OB_FAIL(init_serial_merge())) {
        STORAGE_LOG(WARN, "failed to init serial merge", K(ret), KPC(first_sstable));
      }
    } else if (OB_FAIL(get_major_parallel_ranges(
        first_sstable, tablet_size, merge_ctx.get_tablet()->get_rowkey_read_info()))) {
      STORAGE_LOG(WARN, "Failed to get concurrent cnt from first sstable",
          K(ret), K(tablet_size), K_(concurrent_cnt));
      CTX_SET_DIAGNOSE_LOCATION(merge_ctx);
    } else {
      parallel_type_ = PARALLEL_MAJOR;
    }
  }
  return ret;
}

int ObParallelMergeCtx::init_parallel_mini_merge(compaction::ObBasicTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;
  ObIMemtable *memtable = nullptr;
  int64_t total_bytes = 0;
  int64_t total_rows = 0; // placeholder

  if (OB_UNLIKELY(MINI_MERGE != merge_ctx.get_merge_type())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini merge", K(ret), K(merge_ctx));
  } else if (OB_FAIL(merge_ctx.get_tables_handle().get_first_memtable(memtable))) {
    STORAGE_LOG(WARN, "failed to get first memtable", K(ret), "merge tables", merge_ctx.get_tables_handle());
  } else if (memtable->is_data_memtable()) { // only data memtable has mt stat
    total_bytes = static_cast<memtable::ObMemtable *>(memtable)->get_mt_stat().row_size_;
  } else if (OB_FAIL(memtable->estimate_phy_size(nullptr, nullptr, total_bytes, total_rows))) {
    STORAGE_LOG(WARN, "failed to estimate size from memtable", K(ret));
  }

  if (OB_SUCC(ret)) {
    calc_adaptive_parallel_degree(ObDagPrio::DAG_PRIO_COMPACTION_HIGH,
                                  ObCompactionEstimator::MINI_MEM_PER_THREAD,
                                  (total_bytes + ObCompactionEstimator::MINI_PARALLEL_BASE_MEM - 1) / ObCompactionEstimator::MINI_PARALLEL_BASE_MEM,
                                  concurrent_cnt_);

    ObArray<ObStoreRange> store_ranges;
    store_ranges.set_attr(lib::ObMemAttr(MTL_ID(), "TmpMiniRanges", ObCtxIds::MERGE_NORMAL_CTX_ID));

    ObStoreRange input_range;
    input_range.set_whole_range();
    if (concurrent_cnt_ <= 1) {
      if (OB_FAIL(init_serial_merge())) {
        STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
      }
    } else if (OB_FAIL(memtable->get_split_ranges(input_range, concurrent_cnt_, store_ranges))) {
      STORAGE_LOG(WARN, "Failed to get split ranges from memtable", K(ret));
    } else if (OB_UNLIKELY(store_ranges.count() != concurrent_cnt_)) {
      if (1 == store_ranges.count()) {
        if (OB_FAIL(init_serial_merge())) {
          STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected range array and concurrent_cnt", K(ret), K_(concurrent_cnt), K(store_ranges));
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
        ObDatumRange datum_range;
        if (OB_FAIL(datum_range.from_range(store_ranges.at(i), allocator_))) {
          STORAGE_LOG(WARN, "Failed to transfer store range to datum range", K(ret), K(i), K(store_ranges.at(i)));
        } else if (OB_FAIL(range_array_.push_back(datum_range))) {
          STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(datum_range));
        }
      }
      parallel_type_ = PARALLEL_MINI;
      STORAGE_LOG(INFO, "Succ to get parallel mini merge ranges", K(ret),
            K_(concurrent_cnt), K(total_bytes), K_(range_array));
    }
  }
  return ret;
}

int ObParallelMergeCtx::init_parallel_mini_minor_merge(compaction::ObBasicTabletMergeCtx &merge_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_minor_merge(merge_ctx.get_merge_type()))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini minor merge", K(ret), K(merge_ctx));
  } else {
    const ObITableReadInfo &rowkey_read_info = merge_ctx.get_tablet()->get_rowkey_read_info();
    const int64_t tablet_size = merge_ctx.get_schema()->get_tablet_size();
    ObRangeSplitInfo range_info;
    ObSEArray<ObITable *, DEFAULT_STORE_CNT_IN_STORAGE> tables;
    ObSEArray<ObStoreRange, 16> store_ranges;
    ObPartitionRangeSpliter range_spliter;
    ObStoreRange whole_range;
    whole_range.set_whole_range();

    if (OB_FAIL(merge_ctx.get_tables_handle().get_all_minor_sstables(tables))) {
      STORAGE_LOG(WARN, "Failed to get all sstables from merge ctx", K(ret), K(merge_ctx));
    } else if (OB_FAIL(range_spliter.get_range_split_info(tables, rowkey_read_info, whole_range, range_info))) {
      STORAGE_LOG(WARN, "Failed to init range spliter", K(ret));
    } else if (OB_UNLIKELY(tablet_size <= 0 || range_info.total_size_ < 0 || (tables.count() <= 1 && !merge_ctx.static_param_.is_backfill_))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to calc mini minor parallel degree", K(ret), K(tablet_size),
                K(range_info), K(tables.count()), K(merge_ctx));
    } else {
      calc_adaptive_parallel_degree(ObDagPrio::DAG_PRIO_COMPACTION_MID,
                                    ObCompactionEstimator::MINOR_MEM_PER_THREAD,
                                    (range_info.total_size_ / tables.count() + tablet_size - 1) / tablet_size,
                                    range_info.parallel_target_count_);
    }

    if (OB_FAIL(ret)) {
    } else if (range_info.parallel_target_count_ <= 1) {
      if (OB_FAIL(init_serial_merge())) {
        STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
      }
    } else if (OB_FAIL(range_spliter.split_ranges(range_info, allocator_, true, store_ranges))) {
      STORAGE_LOG(WARN, "Failed to split parallel ranges", K(ret));
    } else if (OB_UNLIKELY(store_ranges.count() <= 1)) {
      range_spliter.reset();
      reset();
      if (OB_FAIL(init_serial_merge())) {
        STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
      } else {
        STORAGE_LOG(INFO, "parallel minor merge back to serialize merge");
      }
    } else {
      concurrent_cnt_ = store_ranges.count();
      parallel_type_ = PARALLEL_MINOR;
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
        ObDatumRange datum_range;
        if (OB_FAIL(datum_range.from_range(store_ranges.at(i), allocator_))) {
          STORAGE_LOG(WARN, "Failed to transfer store range to datum range", K(ret), K(i), K(store_ranges.at(i)));
        } else if (OB_FAIL(range_array_.push_back(datum_range))) {
          STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(datum_range));
        }
      }
      STORAGE_LOG(INFO, "Succ to get parallel mini minor merge ranges", K_(concurrent_cnt), K_(range_array));
    }
  }
  return ret;
}

void ObParallelMergeCtx::calc_adaptive_parallel_degree(
    const int64_t prio,
    const int64_t mem_per_thread,
    const int64_t origin_degree,
    int64_t &parallel_degree)
{
  int tmp_ret = OB_SUCCESS;
  int64_t dag_worker_limit = 0;

  if (OB_TMP_FAIL(MTL(ObTenantDagScheduler *)->get_limit(prio, dag_worker_limit))) {
    dag_worker_limit = ObCompactionEstimator::DEFAULT_MERGE_THREAD_CNT;
    STORAGE_LOG_RET(WARN, tmp_ret, "failed to get worker thread cnt, use dfault value", K(prio), K(dag_worker_limit));
  }
  parallel_degree = MIN(MAX(dag_worker_limit, PARALLEL_MERGE_TARGET_TASK_CNT), origin_degree);

  if (parallel_degree <= 2) {
    // do nothing
  } else if (MTL(ObTenantTabletScheduler *)->enable_adaptive_merge_schedule()) {
    int64_t tenant_free_mem_byte = lib::get_tenant_memory_remain(MTL_ID());
    int64_t mem_allow_max_thread_cnt = tenant_free_mem_byte * ADAPTIVE_PERCENT / MAX(mem_per_thread, 1);
    if (mem_allow_max_thread_cnt < parallel_degree) {
      parallel_degree = MAX(parallel_degree / 2, 2); // fix the parallel degree
    }

    STORAGE_LOG(INFO, "[ADAPTIVE_SCHED] calc adaptive parallel degree", K(prio), K(tenant_free_mem_byte), K(mem_per_thread),
                K(dag_worker_limit), K(origin_degree), K(mem_allow_max_thread_cnt), K(parallel_degree));
  }
}


int ObParallelMergeCtx::get_concurrent_cnt(
    const int64_t tablet_size,
    const int64_t macro_block_cnt,
    int64_t &concurrent_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t max_merge_thread = MAX_MERGE_THREAD;
  if (OB_UNLIKELY(tablet_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tablet size is invalid", K(tablet_size), K(ret));
  } else if (0 == tablet_size) {
    concurrent_cnt = 1;
  } else {
    const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
    if (((macro_block_cnt * macro_block_size + tablet_size - 1) / tablet_size) <= max_merge_thread) {
      concurrent_cnt = (macro_block_cnt * macro_block_size + tablet_size - 1) /
                       tablet_size;
      if (0 == concurrent_cnt) {
        concurrent_cnt = 1;
      }
    } else {
      int64_t macro_cnts = (macro_block_cnt + max_merge_thread - 1) / max_merge_thread;
      concurrent_cnt = (macro_block_cnt + macro_cnts - 1) / macro_cnts;
    }
  }
  return ret;
}

int ObParallelMergeCtx::get_major_parallel_ranges(
    const blocksstable::ObSSTable *first_major_sstable,
    const int64_t tablet_size,
    const ObITableReadInfo &rowkey_read_info)
{
  int ret = OB_SUCCESS;
  int64_t macro_block_cnt = 0;
  if (OB_ISNULL(first_major_sstable)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "major sstable is unexpected null", K(ret), KPC(first_major_sstable));
  } else if (OB_UNLIKELY(concurrent_cnt_ <= 1 || concurrent_cnt_ > MAX_MERGE_THREAD)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "concurrent cnt is invalid", K(ret), K_(concurrent_cnt));
  } else {
    const int64_t macro_block_cnt = first_major_sstable->get_data_macro_block_count();
    const int64_t macro_block_cnt_per_range = (macro_block_cnt + concurrent_cnt_ - 1) / concurrent_cnt_;

    ObSSTableMetaHandle sstable_meta_handle;
    ObDatumRowkeyHelper rowkey_helper;
    ObDatumRowkey macro_endkey;
    ObDatumRowkey multi_version_endkey;
    ObDatumRange range;
    range.end_key_.set_min_rowkey();
    range.set_left_open();
    range.set_right_closed();

    blocksstable::ObDataMacroBlockMeta blk_meta;
    blocksstable::ObSSTableSecMetaIterator *meta_iter = nullptr;
    ObDatumRange query_range;
    int64_t schema_rowkey_cnt = 0;
    query_range.set_whole_range();
    if (OB_FAIL(first_major_sstable->scan_secondary_meta(allocator_, query_range,
       rowkey_read_info, DATA_BLOCK_META, meta_iter))) {
      STORAGE_LOG(WARN, "Failed to scan secondary meta", KR(ret), KPC(this));
    } else if (OB_FAIL(first_major_sstable->get_meta(sstable_meta_handle))) {
      STORAGE_LOG(WARN, "failed to get sstable meta handle", K(ret));
    } else if (FALSE_IT(schema_rowkey_cnt = sstable_meta_handle.get_sstable_meta().get_schema_rowkey_column_count())) {
    } else if (OB_FAIL(rowkey_helper.reserve(schema_rowkey_cnt + 1))) {
      STORAGE_LOG(WARN, "Failed to ", K(ret), K(schema_rowkey_cnt));
    } else if (OB_FAIL(multi_version_endkey.assign(rowkey_helper.get_datums(), schema_rowkey_cnt + 1))) {
      STORAGE_LOG(WARN, "Failed to assign datums", K(ret), K(schema_rowkey_cnt));
    }
    // generate ranges
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_cnt;) {
      int64_t last = i + macro_block_cnt_per_range - 1;
      last = (last < macro_block_cnt ? last : macro_block_cnt - 1);
      // locate to the last macro-block meta in current range
      while (OB_SUCC(meta_iter->get_next(blk_meta)) && i++ < last);
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "Failed to get macro block meta", KR(ret), K(i - 1));
      } else if (OB_UNLIKELY(!blk_meta.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected invalid macro block meta", KR(ret), K(i - 1));
      } else if (OB_FAIL(blk_meta.get_rowkey(macro_endkey))) {
        STORAGE_LOG(WARN, "Failed to get rowkey", KR(ret), K(blk_meta));
      } else if (OB_UNLIKELY(macro_endkey.datum_cnt_ < schema_rowkey_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "Unexpected macro endkey", K(ret), K(macro_endkey));
      } else {
        ObStorageDatum *datums = const_cast<ObStorageDatum*>(multi_version_endkey.datums_);
        for (int64_t i = 0; OB_SUCC(ret) && i < schema_rowkey_cnt; i++) {
          datums[i] = macro_endkey.datums_[i];
        }
        datums[schema_rowkey_cnt].set_max();
        multi_version_endkey.datum_cnt_ = schema_rowkey_cnt + 1;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(range.end_key_.deep_copy(range.start_key_, allocator_))) {
        STORAGE_LOG(WARN, "Failed to deep copy rowkey", KR(ret), K(range.get_end_key()), K(range.get_start_key()));
      } else if (OB_FAIL(multi_version_endkey.deep_copy(range.end_key_, allocator_))) {
        STORAGE_LOG(WARN, "Failed to deep copy rowkey", KR(ret), K(multi_version_endkey), K(range.get_end_key()), K(range.get_start_key()));
      } else if (OB_FAIL(range_array_.push_back(range))) {
        STORAGE_LOG(WARN, "Failed to push range", KR(ret), K(range_array_), K(range));
      }
    }

    if (OB_NOT_NULL(meta_iter)) {
      meta_iter->~ObSSTableSecMetaIterator();
      allocator_.free(meta_iter);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(concurrent_cnt_ != range_array_.count())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "range array size is not equal to concurrent_cnt", K(ret), KPC(this));
    } else {
      ObDatumRange &last_range = range_array_.at(range_array_.count() - 1);
      last_range.end_key_.set_max_rowkey();
      last_range.set_right_open();
    }
  }

  return ret;
}

}
}
