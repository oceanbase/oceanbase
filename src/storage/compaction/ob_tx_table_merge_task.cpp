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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "lib/stat/ob_session_stat.h"
#include "ob_partition_merge_policy.h"
#include "ob_tablet_merge_ctx.h"
#include "ob_tablet_merge_task.h"
#include "ob_tx_table_merge_task.h"

namespace oceanbase
{
using namespace share;
namespace compaction
{

/*
 *  ----------------------------------------------ObTxTableMergeDag--------------------------------------------------
 */

ObTxTableMergeDag::ObTxTableMergeDag()
  : ObBasicTabletMergeDag(ObDagType::DAG_TYPE_TX_TABLE_MERGE),
    compaction_filter_()
{
}

int ObTxTableMergeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTxTableMergePrepareTask *prepare_task = NULL;
  if (OB_FAIL(alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "Fail to alloc task", K(ret));
  } else if (OB_FAIL(prepare_task->init())) {
    STORAGE_LOG(WARN, "failed to init prepare_task", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "Fail to add task", K(ret), K_(ls_id), K_(tablet_id), K_(ctx));
  }
  return ret;
}

int ObTxTableMergeDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTabletMergeDagParam *merge_param = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret), K(param));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input param is null", K(ret), K(param));
  } else if (FALSE_IT(merge_param = static_cast<const ObTabletMergeDagParam *>(param))) {
  } else if (OB_UNLIKELY(!merge_param->tablet_id_.is_special_merge_tablet()
      || !merge_param->is_multi_version_minor_merge())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is not valid", K(ret), KPC(merge_param));
  } else if (OB_FAIL(ObBasicTabletMergeDag::inner_init(*merge_param))) {
    LOG_WARN("failed to init ObTabletMergeDag", K(ret));
  } else if (merge_param->tablet_id_.is_ls_tx_data_tablet() && merge_param->is_minor_merge()) {
    // init compaction filter for minor merge in TxDataTable
    ObTxTableGuard guard;
    int64_t recycle_ts = 0;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx_->ls_handle_.get_ls()->get_tx_table_guard(guard))) {
      LOG_WARN("failed to get tx table", K(tmp_ret), KPC(merge_param));
    } else if (OB_UNLIKELY(!guard.is_valid())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx table guard is invalid", K(tmp_ret), KPC(merge_param), K(guard));
    } else if (OB_TMP_FAIL(guard.get_tx_table()->get_recycle_ts(recycle_ts))) {
      LOG_WARN("failed to get recycle ts", K(tmp_ret), KPC(merge_param));
    } else if (OB_TMP_FAIL(compaction_filter_.init(recycle_ts, ObTxTable::get_filter_col_idx()))) {
      LOG_WARN("failed to get init compaction filter", K(tmp_ret), KPC(merge_param), K(recycle_ts));
    } else {
      ctx_->compaction_filter_ = &compaction_filter_;
      FLOG_INFO("success to init compaction filter", K(tmp_ret), K(recycle_ts));
    }
  }
  return ret;
}

/*
 *  ----------------------------------------------ObTxTableMergePrepareTask--------------------------------------------------
 */

ObTxTableMergePrepareTask::ObTxTableMergePrepareTask()
  : ObTabletMergePrepareTask()
{
}

ObTxTableMergePrepareTask::~ObTxTableMergePrepareTask()
{
}

int ObTxTableMergePrepareTask::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", K(ret));
  } else if (OB_UNLIKELY(ObDagType::DAG_TYPE_TX_TABLE_MERGE != dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    merge_dag_ = static_cast<ObBasicTabletMergeDag *>(dag_);
    if (OB_UNLIKELY(!merge_dag_->get_param().is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("param_ is not valid", K(ret), K(merge_dag_->get_param()));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTxTableMergePrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTabletMergeCtx *ctx = NULL;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (FALSE_IT(ctx = &merge_dag_->get_ctx())) {
  } else if (OB_FAIL(ctx->ls_handle_.get_ls()->get_tablet_svr()->get_tablet(
          ctx->param_.tablet_id_,
          ctx->tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), "ls_id", ctx->param_.ls_id_,
        "tablet_id", ctx->param_.tablet_id_);
  } else if (OB_FAIL(build_merge_ctx())) {
    LOG_WARN("failed to build merge ctx", K(ret), K(ctx->param_));
  } else if (ctx->log_ts_range_.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexcepted empty log ts range in minor merge", K(ret), K(ctx->log_ts_range_));
  } else {
    ctx->merge_log_ts_ = ctx->log_ts_range_.end_log_ts_;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTabletMergePrepareTask::generate_merge_task())) {
    LOG_WARN("Failed to generate_merge_sstable_task", K(ret));
  } else {
    // it doesn't matter if merge_progress failed to init
    // TODO FIX merge progress
//      int tmp_ret = OB_SUCCESS;
//      if (OB_SUCCESS != (tmp_ret = ctx->merge_progress_.init(ctx))) {
//        ctx->merge_progress_.reset();
//        LOG_WARN("Failed to init merge progress", K(tmp_ret));
//      } else {
//        LOG_INFO("succeed to init merge progress", K(tmp_ret), K(ctx->merge_progress_));
//      }
    FLOG_INFO("succeed to generate merge task", "task", *this);
  }

  return ret;
}

int ObTxTableMergePrepareTask::build_merge_ctx()
{
  int ret = OB_SUCCESS;
  ObTabletMergeCtx &ctx = merge_dag_->get_ctx();
  const common::ObTabletID &tablet_id = ctx.param_.tablet_id_;
  ObTablet *tablet = ctx.tablet_handle_.get_obj();
  ObGetMergeTablesParam get_merge_table_param;
  ObGetMergeTablesResult get_merge_table_result;
  get_merge_table_param.merge_type_ = ctx.param_.merge_type_;

  // only ctx.param_ is inited, fill other fields here
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The tablet has not been initialized", K(ret), K(tablet_id));
  } else if (OB_UNLIKELY(!ctx.param_.is_valid() || nullptr == tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx));
  } else if (FALSE_IT(ctx.rebuild_seq_ = ctx.ls_handle_.get_ls()->get_ls_meta().get_rebuild_seq())) {
  } else if (OB_FAIL(ObPartitionMergePolicy::get_merge_tables[ctx.param_.merge_type_](
          get_merge_table_param,
          INT64_MAX/*multi_version_start*/,
          *tablet,
          get_merge_table_result))) {
    // TODO(@DanLin) optimize this interface
    if (OB_NO_NEED_MERGE != ret) {
      LOG_WARN("failed to get merge tables", K(ret), K(ctx), K(get_merge_table_result));
    } else if (is_mini_merge(ctx.param_.merge_type_)) { // OB_NO_NEED_MERGE && mini merge
      int tmp_ret = OB_SUCCESS;
      // then release memtable
      const int64_t clog_checkpoint_log_ts = tablet->get_tablet_meta().clog_checkpoint_ts_;
      if (OB_TMP_FAIL(tablet->release_memtables(clog_checkpoint_log_ts))) {
        LOG_WARN("failed to release memtable", K(tmp_ret), K(clog_checkpoint_log_ts));
      }
    }
  } else if (OB_FAIL(ctx.get_basic_info_from_result(get_merge_table_result))) {
    LOG_WARN("failed to set basic info to ctx", K(ret), K(get_merge_table_result), K(ctx));
  } else if (OB_FAIL(ctx.get_storage_schema_to_merge(get_merge_table_result.handle_, false/*get_schema_on_memtable*/))) {
    LOG_WARN("failed to get storage schema", K(ret), K(get_merge_table_result), K(ctx));
  } else if (OB_UNLIKELY(!ctx.is_schema_valid())) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema of merge ctx is not valid", K(ret), K(ctx));
  } else if (OB_FAIL(ctx.init_parallel_merge())) {
    LOG_WARN("Failed to init parallel merge in sstable merge ctx", K(ret));
  } else {
    ctx.progressive_merge_num_ = 0;
    ctx.is_full_merge_ = true;
    ctx.merge_level_ = MACRO_BLOCK_MERGE_LEVEL;
    ctx.read_base_version_ = 0;
    if (OB_FAIL(ctx.merge_info_.init(ctx))) {
      LOG_WARN("failed to init merge context", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeed to build merge ctx", K(tablet_id), K(ctx));
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
