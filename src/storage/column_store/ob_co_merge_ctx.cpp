//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_co_merge_ctx.h"
#include "storage/column_store/ob_co_merge_dag.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_refresh_tablet_util.h"
#include "storage/compaction/ob_merge_ctx_func.h"
#include "storage/compaction/ob_tenant_compaction_obj_mgr.h"
#endif

namespace oceanbase
{
namespace compaction
{
ObCOTabletMergeCtx::ObCOTabletMergeCtx(
    ObCOMergeDagNet &dag_net,
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
  : ObBasicTabletMergeCtx(param, allocator),
    array_count_(0),
    start_schedule_cg_idx_(0),
    base_rowkey_cg_idx_(-1),
    exe_stat_(),
    dag_net_(dag_net),
    cg_merge_info_array_(nullptr),
    merged_sstable_array_(nullptr),
    cg_schedule_status_array_(nullptr),
    cg_tables_handle_lock_(),
    merged_cg_tables_handle_(MTL_ID()),
    mocked_row_store_cg_(),
    mocked_row_store_table_read_info_(),
    dag_net_merge_history_()
{
}

/*
 * ATTENTION: NEVER USE ANY LOG STREEM VARIABLES IN THIS FUNCTION.
 * Destructor will be called when finish dag net.
 * ObCOMergeDagNet is special, it will be check canceled when ls offine in ObDagNetScheduler::check_ls_compaction_dag_exist_with_cancel.
 * But dag_net is only moved into finished dag net list and delaying freed. So if log streem variables used in this function after ls offine, it will be dangerous
 */
ObCOTabletMergeCtx::~ObCOTabletMergeCtx()
{
  destroy();
}

void ObCOTabletMergeCtx::destroy()
{
  if (OB_NOT_NULL(cg_merge_info_array_)) {
    for (int i = 0; i < array_count_; ++i) {
      if (OB_NOT_NULL(cg_merge_info_array_[i])) {
        LOG_WARN_RET(OB_ERR_UNEXPECTED, "should destroyed before", K(i), K(cg_merge_info_array_[i]));
        cg_merge_info_array_[i]->~ObTabletMergeInfo();
        cg_merge_info_array_[i] = nullptr;
      }
    }
    mem_ctx_.free(cg_merge_info_array_);
    cg_merge_info_array_ = nullptr;
    merged_sstable_array_ = nullptr;
  }
  mocked_row_store_cg_.reset();
  mocked_row_store_table_read_info_.reset();
}

int ObCOTabletMergeCtx::schedule_minor_errsim(bool &schedule_minor) const
{
  int ret = OB_SUCCESS;
  // set schedule_minor
#ifdef ERRSIM
  #define SCHEDULE_MINOR_ERRSIM(tracepoint)                             \
    do {                                                                \
      if (OB_SUCC(ret)) {                                               \
        ret = OB_E((EventTable::tracepoint)) OB_SUCCESS;                \
        if (OB_FAIL(ret)) {                                             \
          ret = OB_SUCCESS;                                             \
          STORAGE_LOG(INFO, "ERRSIM " #tracepoint);                     \
          schedule_minor = get_tables_handle().get_count() > 1;         \
        }                                                               \
      }                                                                 \
    } while(0);

  SCHEDULE_MINOR_ERRSIM(EN_SWAP_TABLET_IN_COMPACTION);
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_SCHEDULE_MINOR_FAIL);
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED);
#endif
  return ret;
}

bool ObCOTabletMergeCtx::is_co_dag_net_failed()
{
  bool need_cancel = false;
  if (DAG_NET_ERROR_COUNT_THREASHOLD <= exe_stat_.period_error_count_) {
    if (EXE_DAG_FINISH_GROWTH_RATIO > (double)exe_stat_.period_finish_cg_count_ / array_count_) {
      need_cancel = true;
    }
    exe_stat_.period_error_count_ = 0;
    exe_stat_.period_finish_cg_count_ = 0;
  }
  if (EXE_DAG_FINISH_UP_RATIO < (double)exe_stat_.finish_cg_count_ / array_count_) {
    need_cancel = DAG_NET_ERROR_COUNT_UP_THREASHOLD < exe_stat_.error_count_;
  }
  if (need_cancel) {
    LOG_WARN_RET(OB_SUCCESS, "dag_report_result: co dag net need cancel", K_(exe_stat));
  }
  return need_cancel;
}

int ObCOTabletMergeCtx::check_need_schedule_minor(bool &schedule_minor) const
{
  schedule_minor = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is invalid", K(ret), K(static_param_));
  } else if (get_schema()->get_column_group_count() > SCHEDULE_MINOR_CG_CNT_THREASHOLD
      && get_tables_handle().get_count() > SCHEDULE_MINOR_TABLE_CNT_THREASHOLD) {
    int64_t minor_table_cnt = 0;
    for (int64_t i = 1; OB_SUCC(ret) && i < get_tables_handle().get_count(); ++i) { // skip major
      ObSSTable *sstable = static_cast<ObSSTable *>(get_tables_handle().get_table(i));
      if (sstable->get_row_count() >= SCHEDULE_MINOR_ROW_CNT_THREASHOLD) {
        ++minor_table_cnt;
      }
    }
    schedule_minor = (minor_table_cnt > 1);
  }
  if (FAILEDx(schedule_minor_errsim(schedule_minor))) {
    LOG_WARN("failed to set schedule_minor in errsim mode", K(ret), K(schedule_minor));
  }
  return ret;
}

int ObCOTabletMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  const int64_t cg_count = get_schema()->get_column_group_count();
  const int64_t alloc_size = cg_count * (sizeof(ObTabletMergeInfo*) + sizeof(ObITable*) + sizeof(CGScheduleStatus));
  void *buf = nullptr;
  if (OB_UNLIKELY(cg_count <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Unexpected cg count for co major", K(ret), K(cg_count), K(static_param_));
  } else if (OB_ISNULL(buf = mem_ctx_.alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(alloc_size));
  } else if (OB_FAIL(init_sstable_merge_history())) {
    LOG_WARN("failed to init merge history", KR(ret));
  } else {
    MEMSET(buf, 0, alloc_size);
    array_count_ = cg_count;
    cg_merge_info_array_ = static_cast<ObTabletMergeInfo **>(buf);
    buf = (void *)(static_cast<char *>(buf) + cg_count * sizeof(ObTabletMergeInfo *));
    merged_sstable_array_ = static_cast<ObITable **>(buf);
    buf = (void *)(static_cast<char *>(buf) + cg_count * sizeof(ObITable*));
    // new alloc cg_schedule_status_array_, every cg marks CG_SCHE_STATUS_IDLE
    cg_schedule_status_array_ = static_cast<CGScheduleStatus *>(buf);
  }
  return ret;
}

int ObCOTabletMergeCtx::prepare_cs_replica_param()
{
  int ret = OB_SUCCESS;
  static_param_.is_cs_replica_ = false;
  ObStorageSchema *schema_on_tablet = nullptr;
  ObSSTable *sstable = nullptr;
  if (static_param_.ls_handle_.get_ls()->is_cs_replica()) {
    if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet handle is invalid", K(ret), K_(tablet_handle));
    } else if (OB_FAIL(static_param_.tablet_schema_guard_.init(tablet_handle_, mem_ctx_))) {
      LOG_WARN("failed to init cs replica schema guard", K(ret), KPC(this));
    } else if (OB_FAIL(static_param_.tablet_schema_guard_.load(schema_on_tablet))) {
      LOG_WARN("failed to load schema on tablet", K(ret));
    } else if (tablet_handle_.get_obj()->is_cs_replica_compat()) {
      static_param_.is_cs_replica_ = true;
    } else if (is_convert_co_major_merge(get_merge_type())) {
      static_param_.is_cs_replica_ = true;
    } else {
      static_param_.is_cs_replica_ = static_param_.get_tablet_id().is_user_tablet()
                                  && schema_on_tablet->is_user_data_table()
                                  && schema_on_tablet->is_row_store();
    }

    if (OB_FAIL(ret) || !static_param_.is_cs_replica_) {
    } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(get_tables_handle().get_table(0)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sstable", K(ret), K(sstable), K(static_param_.tables_handle_));
    } else if (OB_UNLIKELY(!sstable->is_co_sstable())) {
      // may be column store replica rebuild from full/read only row store replica
      if (sstable->is_major_sstable()) {
        static_param_.major_sstable_status_ = ObCOMajorSSTableStatus::COL_REPLICA_MAJOR;
        static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::USE_RS_BUILD_SCHEMA_MATCH_MERGE;
        LOG_INFO("[CS-Replica] Decide rebuild column store from row major for cs replica", K(ret), KPC(sstable), K_(static_param));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first table should be major in cs replica", K(ret), KPC(sstable), K_(static_param));
      }
    } else {
      static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::BUILD_COLUMN_STORE_MERGE;
    }
  }
  LOG_INFO("[CS-Replica] prepare_cs_replica_param", K(ret), "merge_type", get_merge_type(),
           "co_merge_type", ObCOMajorMergePolicy::co_major_merge_type_to_str(static_param_.co_major_merge_type_),
           "is_cs_replica",static_param_.is_cs_replica_, KPC(schema_on_tablet));
  return ret;
}

// major_sstable_status_ is decided in static_param_, according to the storage schema in medium compaction info
int ObCOTabletMergeCtx::check_and_set_build_redundant_row_merge()
{
  int ret = OB_SUCCESS;
  if (static_param_.is_cs_replica_ && static_param_.is_build_redundent_row_store_from_rowkey_cg()) {
    static_param_.co_major_merge_type_ = ObCOMajorMergePolicy::BUILD_REDUNDANT_ROW_STORE_MERGE;
    LOG_INFO("[CS-Replica] Decide build redundant row store from rowkey cg for cs replica", K(ret), K_(static_param));
  }
  return ret;
}

int ObCOTabletMergeCtx::check_convert_co_checksum(const ObSSTable *new_sstable)
{
  int ret = OB_SUCCESS;
  if (is_convert_co_major_merge(get_merge_type())) {
    const ObITable *base_table = get_tables_handle().get_table(0);
    const ObCOSSTableV2 *co_sstable = nullptr;
    const ObSSTable *row_sstable = nullptr;
    ObSEArray<int64_t, 16> row_column_checksums;
    ObSEArray<int64_t, 16> col_column_checksums;
    if (OB_ISNULL(new_sstable) || OB_ISNULL(base_table)
        || OB_UNLIKELY(!new_sstable->is_co_sstable() || !base_table->is_sstable())
        || OB_ISNULL(co_sstable = static_cast<const ObCOSSTableV2 *>(new_sstable))
        || OB_ISNULL(row_sstable = static_cast<const ObSSTable *>(base_table))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid sstable", K(ret), KPC(new_sstable), KPC(co_sstable), KPC(base_table), KPC(row_sstable));
    } else if (OB_FAIL(row_sstable->fill_column_ckm_array(row_column_checksums))) {
      LOG_WARN("failed to fill column ckm array", K(ret), KPC(row_sstable));
    } else if (OB_FAIL(co_sstable->fill_column_ckm_array(*get_schema(), col_column_checksums))) {
      LOG_WARN("failed to fill column ckm array", K(ret), KPC(co_sstable));
    } else if (row_column_checksums.count() != col_column_checksums.count()) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("column count not match", K(ret), "row_column_ckm_cnt", row_column_checksums.count(), "col_column_ckm_cnt", col_column_checksums.count(),
                K(row_column_checksums), K(col_column_checksums), KPC(row_sstable)); // only print partia ObIArray; co_sstable will be printed before.
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_column_checksums.count(); ++i) {
        if (row_column_checksums.at(i) != col_column_checksums.at(i)) {
          ret = OB_CHECKSUM_ERROR;
          LOG_ERROR("column checksum error match", K(ret), "row_ckm", row_column_checksums.at(i) , "col_ckm", col_column_checksums.at(i),
            K(i), "row_column_ckm_cnt", row_column_checksums.count(), "col_column_ckm_cnt", col_column_checksums.count(),
            K(row_column_checksums), K(col_column_checksums), KPC(row_sstable));
        }
      }
    }
  }
  return ret;
}

int ObCOTabletMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_cs_replica_param())) {
    LOG_WARN("failed to prepare cs replica param", K(ret), K_(static_param));
  } else if (is_meta_major_merge(get_merge_type())) {
    if (OB_FAIL(get_meta_compaction_info())) {
      LOG_WARN("failed to get meta compaction info", K(ret), KPC(this));
    }
  } else if (is_convert_co_major_merge(get_merge_type())) {
    if (OB_FAIL(get_convert_compaction_info())) {
      LOG_WARN("failed to get convert compaction info", K(ret), KPC(this));
    }
  } else if (OB_FAIL(get_medium_compaction_info())) {
    // have checked medium info inside
    LOG_WARN("failed to get medium compaction info", K(ret), KPC(this));
  }

  if (FAILEDx(prepare_row_store_cg_schema())) {
    LOG_WARN("failed to init major sstable status", K(ret));
  } else if (OB_FAIL(check_and_set_build_redundant_row_merge())) {
    LOG_WARN("failed to check and set build redundant row merge", K(ret), KPC(this));
  } else {
    LOG_INFO("[CS-Replica] finish prepare schema for co merge", K(ret),
             "is_cs_replica", static_param_.is_cs_replica_, KPC(this));
  }
  return ret;
}

int ObCOTabletMergeCtx::build_ctx(bool &finish_flag)
{
  int ret = OB_SUCCESS;
  int32_t base_rowkey_cg_idx = -1;
  // finish_flag in this function is useless, just for virtual function definition.
  if (OB_FAIL(ObBasicTabletMergeCtx::build_ctx(finish_flag))) {
    LOG_WARN("failed to build basic ctx", KR(ret), "param", get_dag_param(), KPC(this));
  } else if (OB_ISNULL(get_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is null", K(ret), K_(static_param));
  } else if (OB_FAIL(get_schema()->get_base_rowkey_column_group_index(base_rowkey_cg_idx))) {
    LOG_WARN("fail to get base rowkey column group index", K(ret), KPC(get_schema()));
  } else if (FALSE_IT(base_rowkey_cg_idx_ = base_rowkey_cg_idx)) {
  } else if (is_major_merge_type(get_merge_type())) {
    // meta major merge not support row col switch now
    if (is_build_row_store_from_rowkey_cg() || is_build_redundant_row_store_from_rowkey_cg()) {
      if (OB_FAIL(mock_row_store_table_read_info())) {
        STORAGE_LOG(WARN, "fail to init table read info", K(ret));
      }
    }
  }
  return ret;
}

int ObCOTabletMergeCtx::check_merge_ctx_valid()
{
  int ret = OB_SUCCESS;
  const ObMergeType &merge_type = get_merge_type();
  const ObITable *base_table = nullptr;
  const ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!tablet_handle_.is_valid()) || OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet", K(ret), K_(tablet_handle));
  } else if (tablet->is_row_store()
             && OB_UNLIKELY(!is_convert_co_major_merge(merge_type) && !ObCOMajorMergePolicy::is_use_rs_build_schema_match_merge(static_param_.co_major_merge_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only row store tablet need to do convert co major merge", K(ret), K(merge_type), KPC(tablet));
  } else if (OB_ISNULL(base_table = static_param_.tables_handle_.get_table(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base table is null", K(ret), K_(static_param));
  } else if (OB_UNLIKELY(!base_table->is_major_sstable())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid base table type", K(ret), KPC(base_table));
  } else if (OB_UNLIKELY(base_rowkey_cg_idx_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid base rowkey cg idx", K(ret), K_(base_rowkey_cg_idx));
  } else if (ObCOMajorMergePolicy::is_use_rs_build_schema_match_merge(static_param_.co_major_merge_type_)) {
    if (base_table->is_co_sstable()) {
      const ObSSTable *sstable = nullptr;
      const ObCOSSTableV2 *co_sstable = nullptr;
      if (OB_ISNULL(sstable = static_cast<const ObSSTable *>(base_table)) || OB_ISNULL(co_sstable = static_cast<const ObCOSSTableV2 *>(sstable))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sstable type", K(ret), KPC(sstable), KPC(co_sstable));
      } else if (OB_UNLIKELY(!co_sstable->is_row_store_only_co_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only row store only co sstable can do use_rs_build_schema_match_merge", K(ret), KPC(co_sstable));
      }
    }
  }
  return ret;
}

int ObCOTabletMergeCtx::cal_merge_param()
{
  int ret = OB_SUCCESS;
  bool force_full_merge = false;
  const ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type = static_param_.co_major_merge_type_;
  if (OB_UNLIKELY(!ObCOMajorMergePolicy::is_valid_major_merge_type(co_major_merge_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid major merge type", K(ret), K(co_major_merge_type));
  } else if (ObCOMajorMergePolicy::is_build_column_store_merge(co_major_merge_type)) {
  } else if (ObCOMajorMergePolicy::is_build_row_store_merge(co_major_merge_type)) {
    force_full_merge = true;
  } else if (ObCOMajorMergePolicy::is_use_rs_build_schema_match_merge(co_major_merge_type)) {
    force_full_merge = true;
    static_param_.is_rebuild_column_store_ = true;
  } else if (ObCOMajorMergePolicy::is_build_redundent_row_store_merge(co_major_merge_type)) {
    force_full_merge = true;
  }
  if (FAILEDx(ObBasicTabletMergeCtx::cal_major_merge_param(force_full_merge, progressive_merge_mgr_))) {
    LOG_WARN("failed to calc major merge param", KR(ret), K(force_full_merge));
  }
  return ret;
}

int ObCOTabletMergeCtx::collect_running_info()
{
  int ret = OB_SUCCESS;
  const int64_t batch_exe_dag_cnt = dag_net_.get_batch_dag_count();

  if (is_build_row_store() || 1 >= batch_exe_dag_cnt) {
    // no need to report dag net merge history
  } else {
    dag_net_merge_history_.static_info_.shallow_copy(static_history_);
    dag_net_merge_history_.static_info_.is_fake_ = true;
    dag_net_merge_history_.running_info_.merge_start_time_ = dag_net_.get_start_time();
    // add a fake merge info into history with only dag_net time_guard
    (void) ObBasicTabletMergeCtx::add_sstable_merge_info(dag_net_merge_history_,
                                                         dag_net_.get_dag_id(),
                                                         dag_net_.hash(),
                                                         info_collector_.time_guard_,
                                                         nullptr/*sstable*/,
                                                         &static_param_.snapshot_info_,
                                                         0/*start_cg_idx*/,
                                                         array_count_/*end_cg_idx*/,
                                                         batch_exe_dag_cnt);
  }
  return ret;
}

int ObCOTabletMergeCtx::collect_running_info_in_batch(
  const uint32_t start_cg_idx,
  const uint32_t end_cg_idx,
  const ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeHistory &merge_history = cg_merge_info_array_[start_cg_idx]->get_merge_history();
  for (int64_t idx = start_cg_idx + 1; OB_SUCC(ret) && idx < end_cg_idx; ++idx) {
    const ObSSTableMergeHistory &tmp = cg_merge_info_array_[idx]->get_merge_history();
    if (OB_FAIL(merge_history.update_block_info(tmp.block_info_, true/*without_row_cnt*/))) {
      LOG_WARN("failed to update block info", KR(ret), K(tmp));
    }
  }
  if (FAILEDx(dag_net_merge_history_.update_block_info(
      merge_history.block_info_,
      0 == start_cg_idx ? false : true/*without_row_cnt*/))) {
    LOG_WARN("failed to update block info", KR(ret), K(dag_net_merge_history_), K(merge_history));
  } else {
    dag_net_merge_history_.update_execute_time(merge_history.running_info_.execute_time_);
    info_collector_.time_guard_.add_time_guard(time_guard);
  }
  return ret;
}

// for ObCOMergeBatchExeDag
int ObCOTabletMergeCtx::collect_running_info(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const int64_t hash,
    const share::ObDagId &dag_id,
    const ObCompactionTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObITable *new_table = merged_sstable_array_[start_cg_idx];
  if (OB_UNLIKELY(NULL != new_table && !new_table->is_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get new sstable", K(ret), KP(new_table));
  } else if (OB_FAIL(collect_running_info_in_batch(start_cg_idx, end_cg_idx, time_guard))) {
    LOG_WARN("failed to collect running info in batch");
  } else {
    const ObSSTable *new_sstable = static_cast<ObSSTable *>(new_table);
    ObBasicTabletMergeCtx::add_sstable_merge_info(cg_merge_info_array_[start_cg_idx]->get_merge_history(),
          dag_id, hash, time_guard, new_sstable, &static_param_.snapshot_info_, start_cg_idx, end_cg_idx);
  }
  return ret;
}

int ObCOTabletMergeCtx::prepare_index_builder(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool reuse_merge_info)
{
  int ret = OB_SUCCESS;
  ObTabletMergeInfo *temp_array = nullptr;
  int64_t batch_cg_count = 0;
  int64_t alloc_size = 0;
  void *buf = nullptr;

  if (OB_UNLIKELY(start_cg_idx >= end_cg_idx || array_count_ < end_cg_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_cg_idx), K(end_cg_idx), K(array_count_), KPC(this));
  } else if (OB_ISNULL(cg_merge_info_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge info array is null", K(ret), KP(cg_merge_info_array_));
  } else if (OB_UNLIKELY(!is_schema_valid()
      || get_schema()->get_column_group_count() < end_cg_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is invalid", K(ret), K_(static_param));
  } else if (FALSE_IT(batch_cg_count = end_cg_idx - start_cg_idx)) {
  } else if (reuse_merge_info && is_cg_merge_infos_valid(start_cg_idx, end_cg_idx, false/*check_info_ready*/)) {
    // reuse old merge info
  } else if (FALSE_IT(alloc_size = batch_cg_count * sizeof(ObTabletMergeInfo))) {
  } else if (OB_ISNULL(buf = mem_ctx_.local_alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(alloc_size));
  } else {
    temp_array = new (buf) ObTabletMergeInfo[batch_cg_count];
    for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; i++) {
      if (OB_NOT_NULL(cg_merge_info_array_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg merge info should be null", K(ret), K(i), KPC(cg_merge_info_array_[i]), K(reuse_merge_info));
      } else {
        cg_merge_info_array_[i] = &temp_array[i - start_cg_idx];
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_loop_prepare_index_tree(start_cg_idx, end_cg_idx))) {
      LOG_WARN("failed to loop prepare index tree", KR(ret), KP(this), K(start_cg_idx), K(end_cg_idx));
    } else {
      LOG_INFO("success to init merge info array", K(ret), KP(this), K(start_cg_idx), K(end_cg_idx));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    // reuse_merge_info = true in retry progress, release_mem_flag should be false
    if (NULL != cg_merge_info_array_[start_cg_idx]) {
      (void) destroy_merge_info_array(start_cg_idx, end_cg_idx, !reuse_merge_info/*release_mem_flag*/);
    } else {
      mem_ctx_.local_free(buf);
    }
  }
  return ret;
}

bool ObCOTabletMergeCtx::is_cg_merge_infos_valid(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool check_info_ready) const
{
  bool bret = true;

  if (OB_NOT_NULL(cg_merge_info_array_)) {
    ObTabletMergeInfo *info_ptr = nullptr;
    for (int64_t i = start_cg_idx; bret && i < end_cg_idx; ++i) {
      if (OB_ISNULL(info_ptr = cg_merge_info_array_[i])) {
        bret = false;
      } else if (!check_info_ready) {
        // do nothing
      } else if (nullptr == info_ptr->get_index_builder()
             || !info_ptr->get_index_builder()->is_inited()) {
        bret = false;
      }
    }
  } else {
    bret = false;
  }
  return bret;
}

int ObCOTabletMergeCtx::inner_loop_prepare_index_tree(
  const uint32_t start_cg_idx,
  const uint32_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *rowkey_read_info = nullptr;
  const ObITableReadInfo *cg_idx_read_info = nullptr;
  const ObStorageColumnGroupSchema *cg_schema_ptr = nullptr;
  if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(cg_idx_read_info))) {
    LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
  }
  for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; i++) {
    if (OB_FAIL(get_cg_schema_for_merge(i, cg_schema_ptr))) {
      LOG_WARN("fail to get cg schema for merge", K(ret), K(i));
    } else if (OB_ISNULL(cg_merge_info_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null merge info", K(ret), K(i));
    } else if (OB_FAIL(cg_merge_info_array_[i]->init(static_history_))) {
      LOG_WARN("failed to init merge info", K(ret), K(i), KPC(cg_merge_info_array_[i]));
    } else if (cg_schema_ptr->is_all_column_group()) {
      rowkey_read_info = &get_tablet()->get_rowkey_read_info();
    } else {
      rowkey_read_info = cg_idx_read_info;
    }
    if (FAILEDx(build_index_tree(*cg_merge_info_array_[i], rowkey_read_info, cg_schema_ptr, i /*table_cg_idx*/))) {
      LOG_WARN("fail to prepare index tree", K(ret), K(i), KPC(cg_schema_ptr));
    }
  } // end of for
  return ret;
}

int ObCOTabletMergeCtx::create_sstables(const uint32_t start_cg_idx, const uint32_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_cg_idx > end_cg_idx
      || end_cg_idx > array_count_
      || nullptr == cg_merge_info_array_
      || !is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected co merge info array or storage schema", K(ret), K(start_cg_idx),
        K(end_cg_idx), KP(cg_merge_info_array_), K(array_count_), K_(static_param));
  }

  int64_t count = 0;
  int64_t i = start_cg_idx;
  int64_t exist_cg_tables_cnt = 0;
  const ObStorageColumnGroupSchema *cg_schema_ptr = nullptr;
  for ( ; OB_SUCC(ret) && i < end_cg_idx; ++i) {
    ObTableHandleV2 table_handle;
    bool skip_to_create_empty_cg = false;
    if (OB_FAIL(get_cg_schema_for_merge(i, cg_schema_ptr))) {
      LOG_WARN("fail to get cg schema for merge", K(ret), K(i));
    } else if (OB_UNLIKELY(nullptr == cg_merge_info_array_[i] || nullptr == merged_sstable_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge info or table array is null", K(ret), K(i), K(merged_sstable_array_));
    } else if (OB_FAIL(cg_merge_info_array_[i]->create_sstable(*this, table_handle, skip_to_create_empty_cg, cg_schema_ptr, i))) {
      LOG_WARN("fail to create sstable", K(ret), K(i), KPC(this));
    } else if (skip_to_create_empty_cg) {
      // optimization: no need to create empty normal cg
#ifdef ERRSIM
    } else if ((i > start_cg_idx + 2) && OB_FAIL(ret = OB_E(EventTable::EN_COMPACTION_CO_PUSH_TABLES_FAILED) OB_SUCCESS)) {
      LOG_INFO("ERRSIM EN_COMPACTION_CO_PUSH_TABLES_FAILED", K(ret));
      SERVER_EVENT_SYNC_ADD("merge_errsim", "co_push_table_failed", "ret_code", ret);
#endif
    } else if (OB_FAIL(push_table_handle(table_handle, count))) {
      LOG_WARN("failed to add table into tables handle array", K(ret), K(table_handle));
    } else {
      merged_sstable_array_[i] = table_handle.get_table();
      exist_cg_tables_cnt++;
      const ObSSTable *new_sstable = static_cast<ObSSTable *>(table_handle.get_table());
      FLOG_INFO("success to create sstable", KPC(new_sstable), KPC(cg_schema_ptr), "cg_idx", i);
    }
  } // for
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(revert_pushed_table_handle(start_cg_idx, i/*right_border_cg_idx*/, exist_cg_tables_cnt))) {
      LOG_WARN("failed to revert pushed table handle", KR(tmp_ret), K(start_cg_idx), K(i));
      if (OB_TMP_FAIL(dag_net_.set_cancel())) {
        LOG_WARN("failed to set dag net cancel", KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObCOTabletMergeCtx::push_table_handle(ObTableHandleV2 &table_handle, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  ObMutexGuard guard(cg_tables_handle_lock_);
  if (OB_FAIL(merged_cg_tables_handle_.add_table(table_handle))) {
    LOG_WARN("fail to add table", K(ret), K(table_handle));
  } else {
    count = merged_cg_tables_handle_.get_count();
  }
  return ret;
}

// destroyed pushed tables [start_cg_idx, right_border_cg_idx)
int ObCOTabletMergeCtx::revert_pushed_table_handle(
  const int64_t start_cg_idx,
  const int64_t right_border_cg_idx,
  const int64_t exist_cg_tables_cnt)
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray tmp_tables_array(MTL_ID());
  int64_t cg_idx = 0;
  ObMutexGuard guard(cg_tables_handle_lock_);
  for (int64_t idx = 0; OB_SUCC(ret) && idx < merged_cg_tables_handle_.get_count(); ++idx) {
    ObTableHandleV2 tmp_table_handle;
    if (OB_FAIL(merged_cg_tables_handle_.get_table(idx, tmp_table_handle))) {
      LOG_WARN("failed to get table_handle from merged_cg_tables_handle", KR(ret));
    } else if (FALSE_IT(cg_idx = tmp_table_handle.get_table()->get_column_group_id())) {
    } else if (cg_idx < start_cg_idx || start_cg_idx >= right_border_cg_idx) {
      if (OB_FAIL(tmp_tables_array.add_table(tmp_table_handle))) {
        LOG_WARN("failed to add table", KR(ret), K(tmp_table_handle), K(tmp_tables_array));
      }
    }
  } // end of for
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(tmp_tables_array.get_count() + exist_cg_tables_cnt != merged_cg_tables_handle_.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table count is unexpected", KR(ret), K(start_cg_idx), K(right_border_cg_idx), K(tmp_tables_array), K_(merged_cg_tables_handle));
  } else if (OB_FAIL(merged_cg_tables_handle_.assign(tmp_tables_array))) {
    LOG_WARN("failed to assgin tables handle", KR(ret), K(tmp_tables_array), K_(merged_cg_tables_handle));
  } else {
    for (int64_t idx = start_cg_idx; idx < right_border_cg_idx; ++idx) {
      merged_sstable_array_[idx] = nullptr;
    }
    LOG_INFO("success to revert pushed tables handle", KR(ret), K(exist_cg_tables_cnt), K(start_cg_idx),
      K(right_border_cg_idx), K_(merged_cg_tables_handle));
  }
  return ret;
}

void ObCOTabletMergeCtx::destroy_merge_info_array(
    const uint32_t start_cg_idx,
    const uint32_t end_cg_idx,
    const bool release_mem_flag)
{
  if (OB_UNLIKELY(start_cg_idx > end_cg_idx
      || end_cg_idx > array_count_
      || nullptr == cg_merge_info_array_)) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "co merge info array is null", K(start_cg_idx), K(end_cg_idx), KP(cg_merge_info_array_));
  } else {
    LOG_DEBUG("destroy merge info array", KP(this), K(start_cg_idx), K(end_cg_idx),
        KP(cg_merge_info_array_));
    void *cg_array_buf = cg_merge_info_array_[start_cg_idx];
    for (int64_t i = start_cg_idx; i < end_cg_idx; ++i) {
      if (nullptr != cg_merge_info_array_[i]) {
        cg_merge_info_array_[i]->destroy();
        if (release_mem_flag) {
          cg_merge_info_array_[i]->~ObTabletMergeInfo();
          cg_merge_info_array_[i] = nullptr;
        }
      }
    }
    if (release_mem_flag) {
      mem_ctx_.local_free(cg_array_buf);
    }
  }
}

int ObCOTabletMergeCtx::create_sstable(const ObSSTable *&new_sstable)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = get_schema()->get_column_groups();
  const ObTablesHandleArray &cg_tables_handle = merged_cg_tables_handle_;
  ObITable *base_co_table = nullptr;
  ObCOSSTableV2 *co_sstable = nullptr;

  /*
   * There are only 2 case:
   * 1. every cg is not empty, cg_schemas.count() == cg_tables_handle.get_count()
   * 2. some cgs are empty, cg_tables_handle.get_count() == 1 < cg_schemas.count(), only one all cg exists
   */
  if (cg_schemas.count() == cg_tables_handle.get_count()) {
    // no empty cg table skip
    if (OB_FAIL(inner_add_cg_sstables(new_sstable))) {
      LOG_WARN("failed to innner add cg sstables", K(ret), KPC(this));
    }
  } else if (OB_UNLIKELY(cg_tables_handle.get_count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected cg table cnt", K(ret), "cg_schemas_count", cg_schemas.count(),
        "cg_tables_count", cg_tables_handle.get_count());
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else if (FALSE_IT(base_co_table = cg_tables_handle.get_table(0))) {
  } else if (OB_UNLIKELY(NULL == base_co_table || !base_co_table->is_co_sstable()
      || OB_ISNULL(co_sstable = static_cast<ObCOSSTableV2 *>(base_co_table)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected cg sstable", K(ret), KPC(base_co_table), K(cg_tables_handle), K(cg_schemas));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else {
    // only exist one co table here, empty or empty cg sstables
    new_sstable = static_cast<ObSSTable *>(base_co_table);
    LOG_DEBUG("[RowColSwitch] FinsihTask with only one co sstable", KPC(new_sstable));
  }

  if (FAILEDx(check_convert_co_checksum(new_sstable))) {
    LOG_WARN("failed to check convert co checksum", K(ret), KPC(new_sstable));
  }
  return ret;
}

int ObCOTabletMergeCtx::inner_add_cg_sstables(const ObSSTable *&new_sstable)
{
  int ret = OB_SUCCESS;
  new_sstable = nullptr;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = get_schema()->get_column_groups();
  // check whether cg table and cg schema match
  ObCOSSTableV2 *base_co_table = nullptr;
  ObSEArray<ObITable *, 16> cg_sstables;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_schemas.count(); ++idx) {
    const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(idx);
    ObSSTable *table = static_cast<ObSSTable *>(merged_sstable_array_[idx]);

    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is unexpected null", K(ret), K(idx), KPC(this));
    } else if (table->is_co_sstable()) {
      base_co_table = static_cast<ObCOSSTableV2 *>(table);
    } else if (OB_FAIL(cg_sstables.push_back(table))) { // only add cg table
      LOG_WARN("failed to add cg sstable", K(ret));
    }
  } // end for

  // check cksum between co and cg sstables
  const common::ObTabletID &tablet_id = get_tablet_id();
  ObSSTableMetaHandle meta_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(base_co_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("find no base co table", K(ret), KPC(this));
  } else if (OB_FAIL(base_co_table->get_meta(meta_handle))) {
    LOG_WARN("failed to get sstable meta handle", K(ret), KPC(base_co_table));
  } else if (base_co_table->is_all_cg_base() && compaction::is_major_merge_type(static_param_.get_merge_type())
                && OB_FAIL(validate_column_checksums(meta_handle.get_sstable_meta().get_col_checksum(),
                  meta_handle.get_sstable_meta().get_col_checksum_cnt(), cg_schemas))) {
    LOG_ERROR("failed to validate column checksums", K(ret), KPC(base_co_table));
    if (OB_CHECKSUM_ERROR == ret) {
      (void) get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id, true/*found_cksum_error*/);
    }
  } else if (OB_FAIL(base_co_table->fill_cg_sstables(cg_sstables))) {
    LOG_WARN("failed to fill cg sstables to co sstable", K(ret), KPC(base_co_table));
  } else {
    new_sstable = base_co_table;
    LOG_DEBUG("[RowColSwitch] Success to fill cg sstables to co sstable", KPC(new_sstable));
  }
  return ret;
}

int ObCOTabletMergeCtx::validate_column_checksums(
    int64_t *all_column_cksums,
    const int64_t all_column_cnt,
    const common::ObIArray<ObStorageColumnGroupSchema> &cg_schemas)
{
  int ret = OB_SUCCESS;
  ObArray<share::schema::ObColDesc> column_descs;
  if (OB_UNLIKELY(array_count_ != cg_schemas.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table count must be equal to schema count", K(ret), K(cg_schemas.count()), K_(array_count), KPC(this));
  } else if (OB_FAIL(get_schema()->get_multi_version_column_descs(column_descs))) {//temp code
    STORAGE_LOG(WARN, "fail to get_multi_version_column_descs", K(ret));
  }

  ObSSTable * cur_cg_table = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_schemas.count(); ++idx) {
    const ObStorageColumnGroupSchema &cur_cg_schema = cg_schemas.at(idx);
    ObSSTableMetaHandle meta_handle;
    if (cur_cg_schema.is_all_column_group() || cur_cg_schema.is_default_column_group()) {
      continue;
    } else if (OB_ISNULL(cur_cg_table = static_cast<ObSSTable *>(merged_sstable_array_[idx]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null sstable", K(ret), K(idx), KPC(cur_cg_table));
    } else if (OB_FAIL(cur_cg_table->get_meta(meta_handle))) {
      LOG_WARN("failed to get sstable meta handle", K(ret), KPC(cur_cg_table));
    } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_col_checksum_cnt() != cur_cg_schema.column_cnt_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("column group schema not matched sstable!", K(ret), K(cur_cg_schema), KPC(cur_cg_table), K(meta_handle));
    }

    for (uint16_t col_idx = 0; OB_SUCC(ret) && col_idx < cur_cg_schema.column_cnt_; ++col_idx) {
      const int64_t col_seq_idx = cur_cg_schema.get_column_idx(col_idx);
      int64_t *cur_col_cksums = meta_handle.get_sstable_meta().get_col_checksum();
      int64_t cur_col_cksum_cnt = meta_handle.get_sstable_meta().get_col_checksum_cnt();
      if (OB_UNLIKELY(col_seq_idx >= all_column_cnt || col_seq_idx < 0)) {
        ret = OB_CHECKSUM_ERROR;
        LOG_WARN("get unexpected col seq idx", K(ret), K(col_seq_idx), K(all_column_cnt));
      } else if (is_lob_storage(column_descs.at(col_seq_idx).col_type_.get_type())) {//temp code
        continue;
      } else if (cur_col_cksums[col_idx] != all_column_cksums[col_seq_idx]) {
        ret = OB_CHECKSUM_ERROR;
        LOG_ERROR("ERROR! Column Checksum not equal!!!", K(ret), K(col_idx), K(col_seq_idx),
            K(cur_col_cksums[col_idx]), K(all_column_cksums[col_seq_idx]), K(cur_cg_schema));
      }
    }
  }
  return ret;
}

bool ObCOTabletMergeCtx::should_mock_row_store_cg_schema()
{ // need output ALL_CG but schema don't have one
  return is_build_row_store() && !get_schema()->has_all_column_group();
}

int ObCOTabletMergeCtx::prepare_mocked_row_store_cg_schema()
{
  int ret = OB_SUCCESS;
  if (mocked_row_store_cg_.is_inited()) {
  } else if (OB_FAIL(get_schema()->mock_row_store_cg(mocked_row_store_cg_))) {
    LOG_WARN("failed to prepare mocked_row_store_cg_schema", K(ret));
  }
  return ret;
}

int ObCOTabletMergeCtx::get_cg_schema_for_merge(const int64_t idx, const ObStorageColumnGroupSchema *&cg_schema_ptr)
{
  int ret = OB_SUCCESS;
  cg_schema_ptr = nullptr;
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_schemas = get_schema()->get_column_groups();
  if (OB_UNLIKELY(idx < 0 || idx >= cg_schemas.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("idx is out of range", K(ret), K(idx), "count", cg_schemas.count());
  } else {
    cg_schema_ptr = (cg_schemas.at(idx).is_rowkey_column_group() && should_mock_row_store_cg_schema())
                  ? &mocked_row_store_cg_
                  : &cg_schemas.at(idx);
    if (OB_ISNULL(cg_schema_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get cg schema ptr", K(ret));
    } else if (OB_UNLIKELY(!cg_schema_ptr->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid cg schema", K(ret), KPC(cg_schema_ptr), K(idx), K(cg_schemas.at(idx)), KPC(this));
    } else {
      LOG_DEBUG("[RowColSwitch] get cg schema for merge", K(idx), KPC(cg_schema_ptr));
    }
  }
  return ret;
}

int ObCOTabletMergeCtx::prepare_row_store_cg_schema()
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = static_cast<ObSSTable *>(get_tables_handle().get_table(0));
  if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get sstable", K(ret), K(sstable), K(static_param_.tables_handle_));
  } else if (OB_UNLIKELY(!sstable->is_co_sstable())) {
    if (static_param_.is_cs_replica_) {
      // should be cs replica, processed in ObCOTabletMergeCtx::prepare_cs_replica_param
    } else if (sstable->is_major_sstable() && !static_param_.schema_->is_row_store()) {
      static_param_.major_sstable_status_ = ObCOMajorSSTableStatus::DELAYED_TRANSFORM_MAJOR;
      // should be delayed column group transform
      LOG_INFO("set major sstable status with column store schema", K(ret), KPC(sstable), K_(static_param));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first table should be major in co merge", K(ret), KPC(sstable), K_(static_param));
    }
  } else if (OB_FAIL(ObCOMajorMergePolicy::decide_co_major_sstable_status(
                      *static_cast<ObCOSSTableV2 *>(sstable),
                      *get_schema(),
                      static_param_.major_sstable_status_))) {
    LOG_WARN("fail to init major sstable status", K(ret));
  } else if (should_mock_row_store_cg_schema() && OB_FAIL(prepare_mocked_row_store_cg_schema())) {
    LOG_WARN("fail to init mocked row store cg schema", K(ret));
  }
  return ret;
}

int ObCOTabletMergeCtx::construct_column_param(
    const uint64_t column_id,
    const ObStorageColumnSchema *column_schema,
    ObColumnParam &column_param)
{
  int ret = OB_SUCCESS;
  column_param.set_column_id(column_id);
  if (OB_ISNULL(column_schema)) {
    if (OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id
     || OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id) {
      ObObjMeta meta_type;
      meta_type.set_int();
      column_param.set_meta_type(meta_type);
    } else {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(column_id));
    }
  } else if (OB_FAIL(column_schema->construct_column_param(column_param))) {
    STORAGE_LOG(WARN, "fail to construct column param from column schema", K(ret), K(column_id));
  }
  return ret;
}

// mock row_store read info from pure_col schema
int ObCOTabletMergeCtx::mock_row_store_table_read_info()
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *storage_schema = get_schema();
  const ObColDescIArray &all_column_ids = static_param_.multi_version_column_descs_;
  const int64_t column_cnt = all_column_ids.count();
  if (OB_ISNULL(storage_schema)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "storage schema is nullptr", K(ret));
  } else if (OB_UNLIKELY(column_cnt > INT32_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(ERROR, "column count is overflow", K(column_cnt));
  } else {
    static const int64_t COMMON_COLUMN_NUM = 16;
    ObSEArray<ObColumnParam *, COMMON_COLUMN_NUM> tmp_cols;
    ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_cols_index;
    ObSEArray<int32_t, COMMON_COLUMN_NUM> tmp_cg_idxs;
    const int64_t schema_rowkey_cnt = storage_schema->get_rowkey_column_num();
    uint64_t column_id = 0;
    int32_t cg_idx = OB_INVALID_INDEX;
    int32_t multi_version_col_cnt = 0;
    bool is_multi_version_col = false;
    const ObStorageColumnSchema *column_schema  = nullptr;
    ObColumnParam *column = nullptr;
    for (int32_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
      column_id = all_column_ids.at(i).col_id_;
      cg_idx = OB_INVALID_INDEX;
      column_schema = storage_schema->get_column_schema(column_id);
      column = nullptr;
      is_multi_version_col = OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id || OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id;
      if (OB_FAIL(ObTableParam::alloc_column(mem_ctx_.get_allocator(), column))) {
        STORAGE_LOG(WARN, "fail to alloc column", K(ret));
      } else if (OB_FAIL(construct_column_param(column_id, column_schema, *column))) {
        STORAGE_LOG(WARN, "fail to construct column param", K(ret), K(column_id), K(column_schema));
      } else if (OB_FAIL(tmp_cols.push_back(column))) {
        STORAGE_LOG(WARN, "fail to push back column param", K(ret));
      } else if (OB_FAIL(tmp_cols_index.push_back(i))) {
        STORAGE_LOG(WARN, "fail to push back col_index", K(ret), K(i));
      } else if (OB_FAIL(storage_schema->get_column_group_index(column_id, i /* real column idx */, cg_idx))) {
        STORAGE_LOG(WARN, "fail to get column group idx", K(ret), K(i), K(column_id));
      } else if (OB_FAIL(tmp_cg_idxs.push_back(cg_idx))) {
        // even if rowkey cg contains all rowkey column, still need to get column from each cg
        STORAGE_LOG(WARN, "fail to push back cg idx", KR(ret), K(cg_idx));
      }
    } // for

    if (FAILEDx(mocked_row_store_table_read_info_.mock_for_sstable_query(
          mem_ctx_.get_allocator(),
          storage_schema->get_column_count(),
          schema_rowkey_cnt,
          storage_schema->is_oracle_mode(),
          all_column_ids,
          tmp_cols_index,
          tmp_cols,
          tmp_cg_idxs))) {
      STORAGE_LOG(WARN, "fail to init table read info", K(ret), K(all_column_ids), K(tmp_cols_index), K(tmp_cols), K(tmp_cg_idxs));
    }
  }

  LOG_INFO("[RowColSwitch] Generate read info for co merge", K(ret), K(mocked_row_store_table_read_info_));
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE

int ObCOTabletOutputMergeCtx::init_tablet_merge_info()
{
  int ret = OB_SUCCESS;
  const int64_t cg_count = get_schema()->get_column_group_count();
  if (OB_FAIL(ObMergeCtxFunc::init_major_task_checkpoint_mgr(
      get_tablet_id(), get_merge_version(), get_concurrent_cnt(), cg_count,
      get_tables_handle(), get_exec_mode(), *get_ls(), mem_ctx_.get_allocator(),
      task_ckp_mgr_))) {
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to init major checkpoint mgr", KR(ret), KPC(this));
    }
  } else {
    start_schedule_cg_idx_ = task_ckp_mgr_.get_start_schedule_task_idx();
    if (OB_FAIL(ObCOTabletMergeCtx::init_tablet_merge_info())) {
      LOG_WARN("failed to init tablet merge info", KR(ret));
    } else if (OB_FAIL(major_pre_warm_param_.init(get_ls()->get_ls_id(), get_tablet_id()))) {
      LOG_WARN("failed to init pre warm param", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObCOTabletOutputMergeCtx::get_macro_seq_by_stage(
    const ObGetMacroSeqStage stage, int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_ckp_mgr_.get_macro_seq_by_stage(stage, macro_start_seq))) {
    LOG_WARN("failed to get macro seq", KR(ret), K(stage));
  }
  return ret;
}

int ObCOTabletOutputMergeCtx::mark_cg_finish(const int64_t start_cg_idx, const int64_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_ckp_mgr_.mark_cg_finish(get_tablet_id(), get_merge_version(), start_cg_idx, end_cg_idx))) {
    LOG_WARN("failed to mark cg finish", KR(ret));
  }
  return ret;
}

void ObCOTabletOutputMergeCtx::destroy()
{
  task_ckp_mgr_.destroy(mem_ctx_.get_allocator());
  ObCOTabletMergeCtx::destroy();
}

int ObCOTabletOutputMergeCtx::update_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObSSTable *new_sstable = nullptr;
  if (OB_FAIL(pre_warm_writer_.complete())) {
    LOG_WARN("failed to complete pre warm writer", KR(ret), K_(pre_warm_writer));
  } else if (OB_FAIL(ObCOTabletMergeCtx::create_sstable(new_sstable))) {
    LOG_WARN("failed to create sstable", KR(ret));
  } else if (OB_FAIL(ObMergeCtxFunc::upload_tablet_and_write_major_ckm_info(*this, *new_sstable, new_tablet_handle))) {
    LOG_WARN("failed to upload and write major ckm info", KR(ret), KPC(new_sstable));
  }
  return ret;
}

int ObCOTabletOutputMergeCtx::check_medium_info(
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot)
{
  return ObMergeCtxFunc::check_medium_info(*get_ls(), next_medium_info, last_major_snapshot, get_tablet_id());
}

int ObCOTabletOutputMergeCtx::generate_macro_seq_info(
    const int64_t task_idx,
    int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task_ckp_mgr_.get_macro_start_seq(task_idx, macro_start_seq))) {
    LOG_WARN("failed to get macro seq", K(ret), K(task_idx));
  } else {
    LOG_INFO("success to get macro start seq", KR(ret), K(task_idx), K(macro_start_seq));
  }
  return ret;
}


void ObCOTabletOutputMergeCtx::after_update_tablet_for_major()
{
  // do nothing
  int tmp_ret = OB_SUCCESS;
  ObBasicObjHandle<ObCompactionReportObj> report_obj_hdl;

  if (OB_TMP_FAIL(MTL_SVR_OBJ_MGR.get_obj_handle(GCTX.get_server_id(), report_obj_hdl))) {
    LOG_WARN_RET(tmp_ret, "failed to get report obj handle", "cur_svr_id", GCTX.get_server_id());
  } else if (OB_TMP_FAIL(report_obj_hdl.get_obj()->update_exec_tablet(1, true/*is_finish_task*/))) {
    LOG_WARN_RET(tmp_ret, "failed to inc exec tablet", K(get_ls_id()), K(get_tablet_id()));
  }
  // add ckm-report task & update tablet state in update_tablet->download_major_ckm_info
}

/*
 *  ----------------------------------------------ObCOTabletValidateMergeCtx--------------------------------------------------
 */
int ObCOTabletValidateMergeCtx::update_tablet_after_merge()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletHandle new_tablet_handle;
  ObUpdateTableStoreParam param;
  if (OB_FAIL(build_update_table_store_param(nullptr/* new_sstable */, param))) {
    LOG_WARN("failed to build update table store param", KR(ret), K(param));
  } else if (OB_FAIL(param.compaction_info_.major_ckm_info_.assign(major_ckm_info_, &mem_ctx_.get_allocator()))) {
    LOG_WARN("failed to assign major ckm info", KR(ret), K(major_ckm_info_));
  } else if (OB_FAIL(get_ls()->update_tablet_table_store(
      get_tablet_id(), param, new_tablet_handle))) {
    LOG_WARN("failed to update tablet table store", K(ret), K(param), K(new_tablet_handle));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else {
    ObTabletCompactionState tmp_state;
    tmp_state.set_calc_ckm_scn(get_merge_version());
    (void) ObMergeCtxFunc::update_tablet_state(get_ls_id(), get_tablet_id(), tmp_state);
    time_guard_click(ObStorageCompactionTimeGuard::UPDATE_TABLET);
    (void) ObCOTabletMergeCtx::collect_running_info();
  }
  return ret;
}

int ObCOTabletValidateMergeCtx::create_sstables(const uint32_t start_cg_idx, const uint32_t end_cg_idx)
{
  int ret = OB_SUCCESS;
  int64_t macro_start_seq = 0;
  if (OB_UNLIKELY(start_cg_idx > end_cg_idx
      || end_cg_idx > array_count_
      || nullptr == cg_merge_info_array_
      || !is_schema_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected co merge info array or storage schema", K(ret), K(start_cg_idx),
        K(end_cg_idx), KP(cg_merge_info_array_), K(array_count_), K_(static_param));
  } else if (OB_FAIL(get_macro_seq_by_stage(BUILD_INDEX_TREE, macro_start_seq))) {
      LOG_WARN("failed to get macro seq", KR(ret), K(macro_start_seq), "param", get_dag_param());
  } else {
    SMART_VAR(ObSSTableMergeRes, res) {
      for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; ++i) {
        res.reset();
        const ObStorageColumnGroupSchema &cg_schema = get_schema()->get_column_groups().at(i);
        // the start_macro_seq of each cg is same,
        // but the param of build_sstable_merge_res becomes ref instead of const, (which should be IncSeqGenerator finally);
        // avoid the changing of macro_start_seq, we use a tmp_macro_start_seq there.
        int64_t tmp_macro_start_seq = macro_start_seq;
        if (OB_UNLIKELY(nullptr == cg_merge_info_array_[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("merge info or table array is null", K(ret), K(i));
        } else if (OB_FAIL(cg_merge_info_array_[i]->build_sstable_merge_res(static_param_, get_pre_warm_param(), tmp_macro_start_seq, res))) {
          LOG_WARN("fail to create sstable", K(ret), K(i), KPC(this));
        } else if (OB_FAIL(major_ckm_info_.init_from_merge_result(mem_ctx_.get_allocator(), *this, cg_schema, res))) {
          LOG_WARN("failed to init major ckm info", KR(ret));
        } else {
          LOG_INFO("success to init major ckm info", K(ret), K(i), K(cg_schema));
        }
      }
    }
  }
  return ret;
}

int ObCOTabletValidateMergeCtx::check_medium_info(
    const ObMediumCompactionInfo &next_medium_info,
    const int64_t last_major_snapshot)
{
  return ObMergeCtxFunc::check_medium_info(*get_ls(), next_medium_info, last_major_snapshot, get_tablet_id());
}

#endif


} // namespace compaction
} // namespace oceanbase
