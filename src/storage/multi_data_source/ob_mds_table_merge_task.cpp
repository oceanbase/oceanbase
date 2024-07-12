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

#include "storage/multi_data_source/ob_mds_table_merge_task.h"
#include "lib/ob_errno.h"
#include "common/ob_smart_var.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag.h"
#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{
namespace mds
{
ObMdsTableMergeTask::ObMdsTableMergeTask()
  : ObITask(ObITaskType::TASK_TYPE_MDS_MINI_MERGE),
    is_inited_(false),
    mds_merge_dag_(nullptr)
{
}

int ObMdsTableMergeTask::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not be null", K(ret));
  } else if (OB_UNLIKELY(ObDagType::ObDagTypeEnum::DAG_TYPE_MDS_MINI_MERGE != dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), KPC_(dag));
  } else {
    ObMdsTableMergeDag *mds_merge_dag = static_cast<ObMdsTableMergeDag *>(dag_);
    const ObTabletMergeDagParam &merge_dag_param = mds_merge_dag->get_param();
    if (OB_UNLIKELY(!merge_dag_param.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_WARN("param is not valid", K(ret), "param", mds_merge_dag->get_param());
    } else {
      mds_merge_dag_ = mds_merge_dag;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObMdsTableMergeTask::process()
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  ObTabletMergeCtx *ctx_ptr = nullptr;
  DEBUG_SYNC(AFTER_EMPTY_SHELL_TABLET_CREATE);
  bool need_schedule_mds_minor = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited), KPC(mds_merge_dag_));
  } else if (OB_ISNULL(mds_merge_dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null dag", K(ret), KPC(mds_merge_dag_));
  } else if (OB_FAIL(mds_merge_dag_->alloc_merge_ctx())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), KPC_(mds_merge_dag), KPC(mds_merge_dag_));
  } else if (OB_ISNULL(ctx_ptr = static_cast<ObTabletMergeCtx *>(mds_merge_dag_->get_ctx()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected null", KR(ret), KPC_(mds_merge_dag), KPC(mds_merge_dag_));
  } else {
    ObLS *ls = nullptr;
    ObTablet *tablet = nullptr;
    ObTabletMergeCtx &ctx = *ctx_ptr;
    ctx.static_param_.start_time_ = common::ObTimeUtility::fast_current_time();
    const share::ObLSID &ls_id = ctx.get_ls_id();
    const common::ObTabletID &tablet_id = ctx.get_tablet_id();
#ifdef ERRSIM
    if (GCONF.errsim_test_tablet_id.get_value() > 0 && tablet_id.id() == GCONF.errsim_test_tablet_id.get_value()) {
      LOG_INFO("test tablet mds dump start", K(ret), K(tablet_id));
      DEBUG_SYNC(BEFORE_DDL_LOB_META_TABLET_MDS_DUMP);
    }
#endif
    const share::SCN &flush_scn = mds_merge_dag_->get_flush_scn();
    ctx.static_param_.scn_range_.end_scn_ = flush_scn;
    ctx.static_param_.version_range_.snapshot_version_ = flush_scn.get_val_for_tx();
    ObTabletHandle new_tablet_handle;
    mds::MdsTableHandle mds_table;
    const int64_t mds_construct_sequence = mds_merge_dag_->get_mds_construct_sequence();
    ObTableHandleV2 table_handle;
    if (OB_FAIL(ctx.get_ls_and_tablet())) {
      LOG_WARN("failed to get ls and tablet", KR(ret), K(ctx), KPC(mds_merge_dag_));
    } else if (OB_ISNULL(ls = ctx.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K(ls_id), "ls_handle", ctx.static_param_.ls_handle_, KPC(mds_merge_dag_));
    } else if (ls->is_offline()) {
      ret = OB_CANCELED;
      LOG_INFO("ls offline, skip merge", K(ret), K(ctx), KPC(mds_merge_dag_));
    } else if (OB_FAIL(ctx.init_tablet_merge_info(false/*need_check*/))) {
      LOG_WARN("failed to init tablet merge info", K(ret), K(ls_id), K(tablet_id), KPC(mds_merge_dag_));
    } else if (OB_ISNULL(tablet = ctx.get_tablet())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet->get_mds_table_for_dump(mds_table))) {
      LOG_WARN("fail to get mds table", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_UNLIKELY(!mds_table.get_mds_table_ptr()->is_construct_sequence_matched(mds_construct_sequence))) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("construct sequence does not match with current mds table", K(ret), K(ls_id), K(tablet_id), K(mds_construct_sequence));
    } else if (ctx.get_tablet()->get_mds_checkpoint_scn() >= flush_scn) {
      need_schedule_mds_minor = false;
      FLOG_INFO("flush scn smaller than mds ckpt scn, only flush nodes of mds table and do not generate mds mini",
          K(ret), K(ls_id), K(tablet_id), K(flush_scn),
          "mds_checkpoint_scn", ctx.get_tablet()->get_mds_checkpoint_scn(),
          KPC(mds_merge_dag_));
      ctx.time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
      share::dag_yield();
    } else if (FALSE_IT(ctx.static_param_.scn_range_.start_scn_ = ctx.get_tablet()->get_mds_checkpoint_scn())) {
    } else if (MDS_FAIL(build_mds_sstable(ctx, mds_construct_sequence, table_handle))) {
      LOG_WARN("fail to build mds sstable", K(ret), K(ls_id), K(tablet_id), KPC(mds_merge_dag_));
    } else if (MDS_FAIL(ls->build_new_tablet_from_mds_table(
        ctx,
        tablet_id,
        table_handle,
        flush_scn,
        new_tablet_handle))) {
      LOG_WARN("failed to build new tablet from mds table", K(ret), K(ctx), K(ls_id), K(tablet_id), K(flush_scn), KPC(mds_merge_dag_));
    } else {
      ctx.time_guard_click(ObStorageCompactionTimeGuard::EXECUTE);
      share::dag_yield();
    }

    // notify flush ret if ret is not OB_NO_NEED_MERGE
    if (mds_table.is_valid() && OB_NO_NEED_MERGE != ret) {
      mds_table.on_flush(flush_scn, ret);
    }
    ctx.time_guard_click(ObStorageCompactionTimeGuard::DAG_FINISH);
    if (OB_SUCC(ret)) {
      ctx.add_sstable_merge_info(ctx.get_merge_info().get_sstable_merge_info(),
                mds_merge_dag_->get_dag_id(), mds_merge_dag_->hash(),
                ctx.info_collector_.time_guard_);
    }
    // ATTENTION! Critical diagnostic log, DO NOT CHANGE!!!
    FLOG_INFO("sstable merge finish", K(ret), "merge_info", ctx_ptr->get_merge_info(),
        "time_guard", ctx_ptr->info_collector_.time_guard_, KPC(mds_merge_dag_));

    // try schedule mds minor after mds mini
    if (OB_SUCC(ret) && need_schedule_mds_minor) {
      try_schedule_compaction_after_mds_mini(ctx, new_tablet_handle);
    }
  }

  return ret;
}

void ObMdsTableMergeTask::try_schedule_compaction_after_mds_mini(compaction::ObTabletMergeCtx &ctx, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ctx.get_ls_id();
  const common::ObTabletID &tablet_id = ctx.get_tablet_id();
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(tablet_handle), KPC(mds_merge_dag_));
  // when restoring, some log stream may be not ready,
  // thus the inner sql in ObTenantFreezeInfoMgr::try_update_info may timeout
  } else if (!MTL(ObTenantTabletScheduler *)->is_restore()) {
    if (0 == ctx.get_merge_info().get_sstable_merge_info().macro_block_count_) {
      // no need to schedule mds minor merge
    } else if (OB_FAIL(ObTenantTabletScheduler::schedule_tablet_minor_merge<ObTabletMergeExecuteDag>(
        compaction::MDS_MINOR_MERGE, ctx.static_param_.ls_handle_, tablet_handle))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule special tablet minor merge which triggle mds",
            K(ret), K(ls_id), K(tablet_id), KPC(mds_merge_dag_));
      }
    } else {
      LOG_INFO("succeed to try schedule mds minor after mds", K(ls_id), K(tablet_id), KPC(mds_merge_dag_));
    }
  }
}

void ObMdsTableMergeTask::set_merge_finish_time(compaction::ObTabletMergeCtx &ctx)
{
  ObSSTableMergeInfo &sstable_merge_info = ctx.get_merge_info().get_sstable_merge_info();
  sstable_merge_info.merge_finish_time_ = ObTimeUtility::fast_current_time();
}

int ObMdsTableMergeTask::build_mds_sstable(
    compaction::ObTabletMergeCtx &ctx,
    const int64_t mds_construct_sequence,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ctx.get_ls_id();
  const common::ObTabletID &tablet_id = ctx.get_tablet_id();

  SMART_VARS_2((ObMdsTableMiniMerger, mds_mini_merger), (ObTabletDumpMds2MiniOperator, op)) {
    if (OB_FAIL(mds_mini_merger.init(ctx, op))) {
      LOG_WARN("fail to init mds mini merger", K(ret), K(ctx), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ctx.get_tablet()->scan_mds_table_with_op(mds_construct_sequence, op))) {
      LOG_WARN("fail to scan mds table with op", K(ret), K(ctx), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(
          ctx.mem_ctx_.get_allocator(), table_handle))) {
      LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
    }
  }

  return ret;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase
