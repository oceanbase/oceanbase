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

#include "storage/ob_partition_parallel_merge_ctx.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "ob_partition_range_spliter.h"
namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace share;
using namespace blocksstable;

namespace storage {

ObParallelMergeCtx::ObParallelMergeCtx()
    : parallel_type_(INVALID_PARALLEL_TYPE),
      range_array_(),
      first_sstable_(nullptr),
      concurrent_cnt_(0),
      allocator_(ObModIds::OB_CS_MERGER, OB_MALLOC_NORMAL_BLOCK_SIZE),
      is_inited_(false)
{}

ObParallelMergeCtx::~ObParallelMergeCtx()
{
  reset();
}

void ObParallelMergeCtx::reset()
{
  parallel_type_ = INVALID_PARALLEL_TYPE;
  range_array_.reset();
  first_sstable_ = nullptr;
  concurrent_cnt_ = 0;
  allocator_.reset();
  is_inited_ = false;
}

bool ObParallelMergeCtx::is_valid() const
{
  bool bret = true;
  if (IS_NOT_INIT || concurrent_cnt_ <= 0 || parallel_type_ >= INVALID_PARALLEL_TYPE) {
    bret = false;
  } else if (PARALLEL_MAJOR == parallel_type_) {
    // PARALLEL_MAJOR
    bret = OB_NOT_NULL(first_sstable_);
  } else if (range_array_.count() != concurrent_cnt_) {
    bret = false;
  } else if (concurrent_cnt_ > 1 && SERIALIZE_MERGE == parallel_type_) {
    bret = false;
  }
  return bret;
}

int ObParallelMergeCtx::init(ObSSTableMergeCtx& merge_ctx)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObParallelMergeCtx init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == merge_ctx.table_schema_ || merge_ctx.tables_handle_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel merge", K(ret), K(merge_ctx));
  } else {
    int64_t tablet_size = merge_ctx.table_schema_->get_tablet_size();
    bool enable_parallel_minor_merge = false;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(merge_ctx.table_schema_->get_tenant_id()));
    if (tenant_config.is_valid()) {
      enable_parallel_minor_merge = tenant_config->_enable_parallel_minor_merge;
    }
    if (enable_parallel_minor_merge && tablet_size > 0 && merge_ctx.param_.is_mini_merge()) {
      // TODO use memtable to decide parallel degree
      if (OB_FAIL(init_parallel_mini_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel setting for mini merge", K(ret));
      }
      // TODO  support parallel buffer minor merge in future
    } else if (enable_parallel_minor_merge && tablet_size > 0 && merge_ctx.param_.is_minor_merge()) {
      if (OB_FAIL(init_parallel_mini_minor_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel setting for mini minor merge", K(ret));
      }
    } else if (tablet_size > 0 && merge_ctx.param_.is_major_merge()) {
      if (OB_FAIL(init_parallel_major_merge(merge_ctx))) {
        STORAGE_LOG(WARN, "Failed to init parallel major merge", K(ret));
      }
    } else if (OB_FAIL(init_serial_merge())) {
      STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
      STORAGE_LOG(
          INFO, "Succ to init parallel merge ctx", K(enable_parallel_minor_merge), K(tablet_size), K(merge_ctx.param_));
    }
  }

  return ret;
}

int ObParallelMergeCtx::get_merge_range(
    const int64_t parallel_idx, ObExtStoreRange& merge_range, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObParallelMergeCtx is not inited", K(ret), K(*this));
  } else if (parallel_idx >= concurrent_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to get parallel mergerange", K(ret), K(parallel_idx), K_(concurrent_cnt));
  } else {
    switch (parallel_type_) {
      case PARALLEL_MAJOR:
        if (OB_FAIL(first_sstable_->get_range(parallel_idx, concurrent_cnt_, allocator, merge_range))) {
          STORAGE_LOG(WARN, "Failed to get merge_range from first sstable", K(ret));
        }
        break;
      case PARALLEL_MINI:
      case PARALLEL_MINI_MINOR:
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
  ObExtStoreRange merge_range;
  merge_range.get_range().set_whole_range();
  range_array_.reset();
  if (OB_FAIL(merge_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
    STORAGE_LOG(WARN, "Failed to transform and cut off range", K(ret));
  } else if (OB_FAIL(range_array_.push_back(merge_range))) {
    STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(merge_range));
  } else {
    concurrent_cnt_ = 1;
    parallel_type_ = SERIALIZE_MERGE;
  }

  return ret;
}

int ObParallelMergeCtx::init_parallel_major_merge(ObSSTableMergeCtx& merge_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(MAJOR_MERGE != merge_ctx.param_.schedule_merge_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini minor merge", K(ret), K(merge_ctx));
  } else if (OB_ISNULL(merge_ctx.tables_handle_.get_table(0))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "Unexpected null first table", K(ret), K(merge_ctx.tables_handle_));
  } else if (OB_UNLIKELY(!merge_ctx.tables_handle_.get_table(0)->is_sstable())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "First table must be sstable", K(ret), K(merge_ctx.tables_handle_));
  } else {
    const int64_t tablet_size = merge_ctx.table_schema_->get_tablet_size();
    first_sstable_ = static_cast<ObSSTable*>(merge_ctx.tables_handle_.get_table(0));
    if (OB_FAIL(first_sstable_->get_concurrent_cnt(tablet_size, concurrent_cnt_))) {
      STORAGE_LOG(WARN, "Failed to get concurrent cnt from first sstable", K(ret), K(tablet_size), K_(concurrent_cnt));
    } else {
      parallel_type_ = PARALLEL_MAJOR;
    }
  }

  return ret;
}

int ObParallelMergeCtx::init_parallel_mini_merge(ObSSTableMergeCtx& merge_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(MINI_MERGE != merge_ctx.param_.schedule_merge_type_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini minor merge", K(ret), K(merge_ctx));
  } else {
    const int64_t tablet_size = merge_ctx.table_schema_->get_tablet_size();
    memtable::ObMemtable* memtable = nullptr;
    if (OB_FAIL(merge_ctx.tables_handle_.get_first_memtable(memtable))) {
      STORAGE_LOG(WARN, "failed to get first memtable", K(ret), "merge tables", merge_ctx.tables_handle_);
    } else {
      int64_t total_bytes = 0;
      int64_t total_rows = 0;
      if (OB_FAIL(memtable->estimate_phy_size(
              merge_ctx.table_schema_->get_table_id(), nullptr, nullptr, total_bytes, total_rows))) {
        STORAGE_LOG(WARN, "Failed to get estimate size from memtable", K(ret));
      } else {
        int64_t mini_merge_thread = GCONF._mini_merge_concurrency;
        ObArray<ObStoreRange> store_ranges;
        mini_merge_thread = MAX(mini_merge_thread, PARALLEL_MERGE_TARGET_TASK_CNT);
        concurrent_cnt_ = MIN((total_bytes + tablet_size - 1) / tablet_size, mini_merge_thread);
        if (concurrent_cnt_ <= 1) {
          if (OB_FAIL(init_serial_merge())) {
            STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
          }
        } else if (OB_FAIL(memtable->get_split_ranges(
                       merge_ctx.table_schema_->get_table_id(), nullptr, nullptr, concurrent_cnt_, store_ranges))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            if (OB_FAIL(init_serial_merge())) {
              STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
            }
          } else {
            STORAGE_LOG(WARN, "Failed to get split ranges from memtable", K(ret));
          }
        } else if (OB_UNLIKELY(store_ranges.count() != concurrent_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected range array and concurrent_cnt", K(ret), K_(concurrent_cnt), K(store_ranges));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
            ObExtStoreRange ext_range(store_ranges.at(i));
            if (OB_FAIL(ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
              STORAGE_LOG(WARN, "Failed to transform and cut off range", K(ret));
            } else if (OB_FAIL(range_array_.push_back(ext_range))) {
              STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(ext_range));
            }
          }
          parallel_type_ = PARALLEL_MINI;
          STORAGE_LOG(INFO, "Succ to get parallel mini merge ranges", K_(concurrent_cnt), K_(range_array));
        }
      }
    }
  }

  return ret;
}

int ObParallelMergeCtx::init_parallel_mini_minor_merge(ObSSTableMergeCtx& merge_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!merge_ctx.param_.is_minor_merge())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init parallel mini minor merge", K(ret), K(merge_ctx));
  } else {
    const int64_t tablet_size = merge_ctx.table_schema_->get_tablet_size();
    ObRangeSplitInfo range_info;
    ObArray<ObSSTable*> sstables;
    ObArray<ObStoreRange> store_ranges;
    ObPartitionRangeSpliter range_spliter;
    ObStoreRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(merge_ctx.tables_handle_.get_all_sstables(sstables))) {
      STORAGE_LOG(WARN, "Failed to get all sstables from merge ctx", K(ret), K(merge_ctx));
    } else if (sstables.count() != merge_ctx.tables_handle_.get_count()) {
      if (OB_FAIL(init_serial_merge())) {
        STORAGE_LOG(WARN, "Failed to init serialize merge", K(ret));
      } else if (merge_ctx.param_.schedule_merge_type_ == MINI_MINOR_MERGE) {
        STORAGE_LOG(WARN, "Unexpected tables handle for mini minor merge", K(ret), K(merge_ctx.tables_handle_));
      }
    } else if (OB_FAIL(range_spliter.get_range_split_info(sstables, whole_range, range_info))) {
      STORAGE_LOG(WARN, "Failed to init range spliter", K(ret));
    } else if (OB_FAIL(calc_mini_minor_parallel_degree(
                   tablet_size, range_info.total_size_, sstables.count(), range_info.parallel_target_count_))) {
      STORAGE_LOG(WARN, "Failed to calc mini minor parallel degree", K(ret));
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
      parallel_type_ = PARALLEL_MINI_MINOR;
      STORAGE_LOG(INFO, "Succ to get parallel mini minor merge ranges", K_(concurrent_cnt), K_(range_array));
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ranges.count(); i++) {
        ObExtStoreRange ext_range(store_ranges.at(i));
        if (OB_FAIL(ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
          STORAGE_LOG(WARN, "Failed to transform and cut off range", K(ret));
        } else if (OB_FAIL(range_array_.push_back(ext_range))) {
          STORAGE_LOG(WARN, "Failed to push back merge range to array", K(ret), K(ext_range));
        }
      }
    }
  }
  return ret;
}

int ObParallelMergeCtx::calc_mini_minor_parallel_degree(
    const int64_t tablet_size, const int64_t total_size, const int64_t sstable_count, int64_t& parallel_degree)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(tablet_size == 0 || total_size < 0 || sstable_count <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "Invalid argument to calc mini minor parallel degree",
        K(ret),
        K(tablet_size),
        K(total_size),
        K(sstable_count));
  } else {
    int64_t minor_merge_thread = GCONF.minor_merge_concurrency;
    int64_t avg_sstable_size = total_size / sstable_count;
    parallel_degree = MIN(
        MAX(minor_merge_thread, PARALLEL_MERGE_TARGET_TASK_CNT), (avg_sstable_size + tablet_size - 1) / tablet_size);
  }

  return ret;
}

}  // namespace storage
}  // namespace oceanbase
