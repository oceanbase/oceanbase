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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_empty_insert_tablet_ctx_manager.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

ObTableLoadEmptyInsertTabletCtxManager::ObTableLoadEmptyInsertTabletCtxManager()
  : thread_count_(0),
    idx_(0),
    start_(0),
    is_inited_(false)
{
}

ObTableLoadEmptyInsertTabletCtxManager::~ObTableLoadEmptyInsertTabletCtxManager()
{
}

int ObTableLoadEmptyInsertTabletCtxManager::init(
      const ObIArray<ObTableLoadPartitionId> &partition_ids,
      const ObIArray<ObTableLoadPartitionId> &target_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("empty insert tablet ctx manager init twice", KR(ret));
  } else if (!target_partition_ids.empty()) {
    if (OB_FAIL(ObTableLoadPartitionLocation::init_partition_location(partition_ids,
                                                                      target_partition_ids,
                                                                      partition_location_,
                                                                      target_partition_location_))) {
      LOG_WARN("fail to init partition location", KR(ret));
    } else if (OB_FAIL(partition_location_.get_all_leader_info(all_leader_info_array_))) {
      LOG_WARN("fail to get all origin leader info", KR(ret));
    } else if (OB_FAIL(target_partition_location_.get_all_leader_info(target_all_leader_info_array_))) {
      LOG_WARN("fail to get all target leader info", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadEmptyInsertTabletCtxManager::get_next_task(
      ObAddr &addr,
      ObIArray<table::ObTableLoadLSIdAndPartitionId> &partition_ids,
      ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty insert tablet ctx manager is not init", KR(ret));
  } else {
    ObMutexGuard guard(op_lock_);
    if (all_leader_info_array_.count() == idx_) {
      ret = OB_ITER_END;
    } else {
      const LeaderInfo &leader_info = all_leader_info_array_.at(idx_);
      const LeaderInfo &target_leader_info = target_all_leader_info_array_.at(idx_);
      addr = target_leader_info.addr_;
      for (; OB_SUCC(ret) && start_ < target_leader_info.partition_id_array_.count()
                          && partition_ids.count() < TABLET_COUNT_PER_TASK; ++start_) {
        if (OB_FAIL(partition_ids.push_back(leader_info.partition_id_array_.at(start_)))) {
          LOG_WARN("fail to push back partition ids", KR(ret));
        } else if (OB_FAIL(target_partition_ids.push_back(target_leader_info.partition_id_array_.at(start_)))) {
          LOG_WARN("fail to push back target partition ids", KR(ret));
        }
      }
      if (target_leader_info.partition_id_array_.count() == start_) {
        start_ = 0;
        ++idx_;
      }
    }
  }
  return ret;
}

int ObTableLoadEmptyInsertTabletCtxManager::set_thread_count(const int64_t thread_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty insert tablet ctx manager is not init", KR(ret));
  } else if (thread_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("thread_count is invalid argument", KR(ret), K(thread_count));
  } else {
    thread_count_ = thread_count;
  }
  return ret;
}

int ObTableLoadEmptyInsertTabletCtxManager::handle_thread_finish(bool &is_finish)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty insert tablet ctx is not init", KR(ret));
  } else if (thread_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("thread count is invalid", KR(ret), K(thread_count_));
  } else {
    is_finish = 0 == ATOMIC_AAF(&thread_count_, -1);
  }
  return ret;
}

int ObTableLoadEmptyInsertTabletCtxManager::execute(
      const uint64_t &table_id,
      const ObTableLoadDDLParam &ddl_param,
      const ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_part_ids,
      const ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_part_ids)
{
  int ret = OB_SUCCESS;
  ObTableLoadSchema table_load_schema;
  ObDirectLoadInsertTableParam insert_table_param;
  ObDirectLoadInsertDataTableContext tmp_insert_table_ctx;
  if (OB_FAIL(table_load_schema.init(MTL_ID(), table_id))) {
    LOG_WARN("fail to init table load schema", KR(ret));
  }
  insert_table_param.table_id_ = table_id;
  insert_table_param.schema_version_ = ddl_param.schema_version_;
  insert_table_param.snapshot_version_ = ddl_param.snapshot_version_;
  insert_table_param.ddl_task_id_ = ddl_param.task_id_;
  insert_table_param.data_version_ = ddl_param.data_version_;
  insert_table_param.parallel_ = 1;
  insert_table_param.reserved_parallel_ = 0;
  insert_table_param.rowkey_column_count_ = table_load_schema.rowkey_column_count_;
  insert_table_param.column_count_ = table_load_schema.store_column_count_;
  insert_table_param.lob_inrow_threshold_ = table_load_schema.lob_inrow_threshold_;
  insert_table_param.is_partitioned_table_ = table_load_schema.is_partitioned_table_;
  insert_table_param.is_table_without_pk_ = table_load_schema.is_table_without_pk_;
  insert_table_param.is_table_with_hidden_pk_column_ = table_load_schema.is_table_with_hidden_pk_column_;
  insert_table_param.online_opt_stat_gather_ = false;
  insert_table_param.is_incremental_ = false;
  insert_table_param.datum_utils_ = &(table_load_schema.datum_utils_);
  insert_table_param.col_descs_ = &(table_load_schema.column_descs_);
  insert_table_param.cmp_funcs_ = &(table_load_schema.cmp_funcs_);
  insert_table_param.lob_column_idxs_ = &(table_load_schema.lob_column_idxs_);
  insert_table_param.online_sample_percent_ = 1.0;
  insert_table_param.is_no_logging_ = ddl_param.is_no_logging_;
  insert_table_param.max_batch_size_ = 256;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(tmp_insert_table_ctx.init(insert_table_param,
                                        ls_part_ids,
                                        target_ls_part_ids))) {
    LOG_WARN("fail to init tmp insert table ctx", KR(ret));
  }
  FOREACH_X(it, tmp_insert_table_ctx.get_tablet_ctx_map(), OB_SUCC(ret)) {
    int64_t slice_id = 0;
    ObMacroDataSeq block_start_seq;
    ObDirectLoadInsertTabletContext *insert_tablet_ctx = it->second;
    if (OB_ISNULL(insert_tablet_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert tablet ctx is nullptr", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->open())) {
      LOG_WARN("fail to open tablet ctx", KR(ret));
    } else if (OB_FAIL(insert_tablet_ctx->open_sstable_slice(block_start_seq, 0/*slice_idx*/, slice_id))) {
      LOG_WARN("fail to open sstable slice", KR(ret), K(block_start_seq), K(slice_id));
    } else if (OB_FAIL(insert_tablet_ctx->close_sstable_slice(slice_id, 0/*slice_idx*/))) {
      LOG_WARN("fail to close sstable slice", KR(ret), K(slice_id));
    } else if (OB_FAIL(insert_tablet_ctx->close())) {
      LOG_WARN("fail to close tablet ctx", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
