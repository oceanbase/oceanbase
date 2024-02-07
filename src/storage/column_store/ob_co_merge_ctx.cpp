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
#include "storage/column_store/ob_co_merge_ctx.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/column_store/ob_co_merge_dag.h"
#include "storage/tablet/ob_tablet.h"

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
    exe_stat_(),
    dag_net_(dag_net),
    cg_merge_info_array_(nullptr),
    merged_sstable_array_(nullptr),
    cg_schedule_status_array_(nullptr),
    cg_tables_handle_lock_(),
    merged_cg_tables_handle_(MTL_ID())
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
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_CO_MERGE_PREPARE_MINOR_FAILED);
  SCHEDULE_MINOR_ERRSIM(EN_COMPACTION_CO_MERGE_SCHEDULE_FAILED);
#endif
  return ret;
}

bool ObCOTabletMergeCtx::is_co_dag_net_failed()
{
  bool need_cancel = false;
  if (DAG_NET_ERROR_COUNT_THREASHOLD == exe_stat_.period_error_count_) {
    if (EXE_DAG_FINISH_GROWTH_RATIO > (double)exe_stat_.period_finish_count_ / array_count_) {
      need_cancel = true;
    }
    exe_stat_.period_error_count_ = 0;
    exe_stat_.period_finish_count_ = 0;
  }
  if (EXE_DAG_FINISH_UP_RATIO < (double)exe_stat_.finish_count_ / array_count_) {
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

int ObCOTabletMergeCtx::init_tablet_merge_info(const bool need_check)
{
  UNUSED(need_check);
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
  } else {
    MEMSET(buf, 0, alloc_size);
    array_count_ = cg_count;
    cg_merge_info_array_ = static_cast<ObTabletMergeInfo **>(buf);
    buf = (void *)(static_cast<char *>(buf) + cg_count * sizeof(ObTabletMergeInfo *));
    merged_sstable_array_ = static_cast<ObITable **>(buf);
    buf = (void *)(static_cast<char *>(buf) + cg_count * sizeof(ObITable*));
    cg_schedule_status_array_ = static_cast<CGScheduleStatus *>(buf);
  }
  return ret;
}

int ObCOTabletMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;

  if (is_meta_major_merge(static_param_.get_merge_type())) {
    if (OB_FAIL(get_meta_compaction_info())) {
      LOG_WARN("failed to get meta compaction info", K(ret), KPC(this));
    }
  } else if (OB_FAIL(get_medium_compaction_info())) {
    // have checked medium info inside
    LOG_WARN("failed to get medium compaction info", K(ret), KPC(this));
  }
  return ret;
}

int ObCOTabletMergeCtx::collect_running_info()
{
  int ret = OB_SUCCESS;
  ObSSTableMergeInfo fake_merge_info;
  fake_merge_info.tenant_id_ = MTL_ID();
  fake_merge_info.ls_id_ = get_ls_id();
  fake_merge_info.is_fake_ = true;
  fake_merge_info.tablet_id_ = get_tablet_id();
  fake_merge_info.merge_start_time_ = dag_net_.get_start_time();
  fake_merge_info.merge_type_ = get_inner_table_merge_type();
  // add a fake merge info into history with only dag_net time_guard
  (void) ObBasicTabletMergeCtx::add_sstable_merge_info(fake_merge_info, dag_net_.get_dag_id(), dag_net_.hash(),
                            info_collector_.time_guard_, nullptr/*sstable*/,
                            &static_param_.snapshot_info_,
                            0/*start_cg_idx*/, array_count_/*end_cg_idx*/);
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
  } else {
    const ObSSTable *new_sstable = static_cast<ObSSTable *>(new_table);
    add_sstable_merge_info(cg_merge_info_array_[start_cg_idx]->get_sstable_merge_info(),
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
      // reuse_merge_info = true in retry progress, release_mem_flag should be false
      (void) destroy_merge_info_array(start_cg_idx, end_cg_idx, !reuse_merge_info/*release_mem_flag*/);
    } else {
      LOG_INFO("success to init merge info array", K(ret), KP(this), K(start_cg_idx), K(end_cg_idx));
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
  const common::ObIArray<ObStorageColumnGroupSchema> &cg_schemas = get_schema()->get_column_groups();
  if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(cg_idx_read_info))) {
    LOG_WARN("failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
  }
  for (int64_t i = start_cg_idx; OB_SUCC(ret) && i < end_cg_idx; i++) {
    const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(i);
    if (OB_ISNULL(cg_merge_info_array_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null merge info", K(ret), K(i));
    } else if (OB_FAIL(cg_merge_info_array_[i]->init(*this, true/*need_check*/, false/*merge_start*/))) {
      LOG_WARN("failed to init merge info", K(ret), K(i), KPC(cg_merge_info_array_[i]));
    } else if (cg_schema.is_all_column_group()) {
      rowkey_read_info = &get_tablet()->get_rowkey_read_info();
    } else {
      rowkey_read_info = cg_idx_read_info;
    }
    if (FAILEDx(build_index_tree(*cg_merge_info_array_[i], rowkey_read_info, &cg_schema, i /*table_cg_idx*/))) {
      LOG_WARN("fail to prepare index tree", K(ret), K(i), K(cg_schema));
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
  for ( ; OB_SUCC(ret) && i < end_cg_idx; ++i) {
    ObTableHandleV2 table_handle;
    const ObStorageColumnGroupSchema &cg_schema = get_schema()->get_column_groups().at(i);
    bool skip_to_create_empty_cg = false;
    if (OB_UNLIKELY(nullptr == cg_merge_info_array_[i] || nullptr == merged_sstable_array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge info or table array is null", K(ret), K(i), K(merged_sstable_array_));
    } else if (OB_FAIL(cg_merge_info_array_[i]->create_sstable(*this, table_handle, skip_to_create_empty_cg, &cg_schema, i))) {
      LOG_WARN("fail to create sstable", K(ret), K(i), KPC(this));
    } else if (skip_to_create_empty_cg) {
      // optimization: no need to create empty normal cg
#ifdef ERRSIM
    } else if ((i > start_cg_idx + 2) && OB_FAIL(ret = OB_E(EventTable::EN_COMPACTION_CO_PUSH_TABLES_FAILED) OB_SUCCESS)) {
      LOG_INFO("ERRSIM EN_COMPACTION_CO_PUSH_TABLES_FAILED", K(ret));
#endif
    } else if (OB_FAIL(push_table_handle(table_handle, count))) {
      LOG_WARN("failed to add table into tables handle array", K(ret), K(table_handle));
    } else {
      merged_sstable_array_[i] = table_handle.get_table();
      exist_cg_tables_cnt++;
      LOG_TRACE("success to create sstable", K(ret), K(i), K(table_handle), "merged_cg_tables_count", count);
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(revert_pushed_table_handle(start_cg_idx, i/*right_border_cg_idx*/, exist_cg_tables_cnt))) {
      LOG_WARN("failed to revert pushed table handle", KR(tmp_ret), K(start_cg_idx), K(i));
      dag_net_.set_cancel();
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
      || !static_cast<ObCOSSTableV2 *>(base_co_table)->is_empty_co_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected cg sstable", K(ret), KPC(base_co_table), K(cg_tables_handle), K(cg_schemas));
    CTX_SET_DIAGNOSE_LOCATION(*this);
  } else {
    // only exist one empty co table here
    new_sstable = static_cast<ObSSTable *>(base_co_table);
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
  } else if (base_co_table->is_all_cg_base() && OB_FAIL(validate_column_checksums(meta_handle.get_sstable_meta().get_col_checksum(),
        meta_handle.get_sstable_meta().get_col_checksum_cnt(), cg_schemas))) {
    LOG_ERROR("failed to validate column checksums", K(ret), KPC(base_co_table));
    if (OB_CHECKSUM_ERROR == ret) {
      (void) get_ls()->get_tablet_svr()->update_tablet_report_status(tablet_id, true/*found_cksum_error*/);
    }
  } else if (OB_FAIL(base_co_table->fill_cg_sstables(cg_sstables))) {
    LOG_WARN("failed to fill cg sstables to co sstable", K(ret), KPC(base_co_table));
  } else {
    new_sstable = base_co_table;
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
    } else if (OB_ISNULL(cur_cg_schema.column_idxs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("normal cg has unexpected null column_idxs", K(ret), K(cur_cg_schema));
    } else if (OB_FAIL(cur_cg_table->get_meta(meta_handle))) {
      LOG_WARN("failed to get sstable meta handle", K(ret), KPC(cur_cg_table));
    } else if (OB_UNLIKELY(meta_handle.get_sstable_meta().get_col_checksum_cnt() != cur_cg_schema.column_cnt_)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("column group schema not matched sstable!", K(ret), K(cur_cg_schema), KPC(cur_cg_table), K(meta_handle));
    }

    for (uint16_t col_idx = 0; OB_SUCC(ret) && col_idx < cur_cg_schema.column_cnt_; ++col_idx) {
      const int64_t col_seq_idx = cur_cg_schema.column_idxs_[col_idx];
      int64_t *cur_col_cksums = meta_handle.get_sstable_meta().get_col_checksum();
      int64_t cur_col_cksum_cnt = meta_handle.get_sstable_meta().get_col_checksum_cnt();
      if (OB_UNLIKELY(col_seq_idx >= all_column_cnt || col_seq_idx < 0)) {
        ret = OB_CHECKSUM_ERROR;
        LOG_WARN("get unexpected col seq idx", K(ret), K(col_seq_idx), K(all_column_cnt));
      } else if (ob_is_large_text(column_descs.at(col_seq_idx).col_type_.get_type())) {//temp code
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

} // namespace compaction
} // namespace oceanbase
