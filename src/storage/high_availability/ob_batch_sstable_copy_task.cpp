/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_batch_sstable_copy_task.h"
#include "ob_physical_copy_ctx.h"
#include "ob_sstable_copy_finish_task.h"
#include "ob_sstable_copy_ops.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_reader.h"
#include "ob_storage_ha_tablet_builder.h"
#include "ob_tablet_copy_finish_task.h"
#include "observer/ob_server.h"
#ifdef ERRSIM
#include "observer/ob_server_event_history_table_operator.h"
#endif
#include "share/ob_cluster_version.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_BATCH_CG_COPY_FORCE_LEGACY_CG);

using namespace blocksstable;

ObBatchSSTableKeysHolder::ObBatchSSTableKeysHolder(const uint64_t tenant_id)
  : pending_keys_(),
    cursor_(0)
{
  pending_keys_.set_attr(common::ObMemAttr(tenant_id, "BatchSSTKeys"));
}

int ObBatchSSTableKeysHolder::assign(
    const common::ObIArray<ObITable::TableKey> &keys)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(pending_keys_.assign(keys))) {
    LOG_WARN("failed to assign batch sstable keys", K(ret), "key_count", keys.count());
  }
  return ret;
}

void ObBatchSSTableKeysHolder::reset()
{
  pending_keys_.reset();
  cursor_ = 0;
}

bool ObBatchSSTableKeysHolder::has_more() const
{
  return cursor_ < pending_keys_.count();
}

int ObBatchSSTableKeysHolder::take_next(
    const int64_t max_count,
    common::ObIArray<ObITable::TableKey> &keys,
    bool &has_more)
{
  int ret = OB_SUCCESS;
  keys.reset();
  has_more = false;
  if (OB_UNLIKELY(max_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid max batch count", K(ret), K(max_count));
  } else {
    const int64_t remaining = pending_keys_.count() - cursor_;
    int64_t take_count = 0;
    if (remaining <= max_count) {
      take_count = remaining;
    } else if (remaining <= 2 * max_count) {
      // Avoid a nearly empty tail task, for example 129 -> 65 + 64.
      take_count = (remaining + 1) / 2;
    } else {
      take_count = max_count;
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < take_count; ++i) {
      if (OB_FAIL(keys.push_back(pending_keys_.at(cursor_ + i)))) {
        LOG_WARN("failed to append batch sstable key", K(ret), K_(cursor), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      cursor_ += take_count;
      has_more = cursor_ < pending_keys_.count();
    }
  }
  return ret;
}

ObBatchSSTableCopyTaskParam::ObBatchSSTableCopyTaskParam()
  : ls_(nullptr),
    tablet_id_(),
    src_info_(),
    src_tablet_meta_(nullptr),
    table_info_mgr_(nullptr),
    copy_sstable_info_mgr_(nullptr),
    tablet_copy_finish_task_(nullptr),
    extra_info_(nullptr),
    ha_svc_ctx_(),
    src_ls_rebuild_seq_(-1)
{
}

bool ObBatchSSTableCopyTaskParam::is_valid() const
{
  return OB_NOT_NULL(ls_)
      && tablet_id_.is_valid()
      && src_info_.is_valid()
      && OB_NOT_NULL(src_tablet_meta_)
      && OB_NOT_NULL(table_info_mgr_)
      && OB_NOT_NULL(copy_sstable_info_mgr_)
      && OB_NOT_NULL(tablet_copy_finish_task_)
      && OB_NOT_NULL(extra_info_)
      && ha_svc_ctx_.is_valid()
      && src_ls_rebuild_seq_ >= 0;
}

ObBatchSSTableCopyTask::ObBatchSSTableCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
    is_inited_(false),
    param_(),
    my_keys_(),
    macro_block_reuse_mgr_()
{
}

ObBatchSSTableCopyTask::~ObBatchSSTableCopyTask()
{
}

int ObBatchSSTableCopyTask::init(
    const ObBatchSSTableCopyTaskParam &param,
    ObBatchSSTableKeysHolder &keys_holder)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("batch sstable copy task init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch sstable copy task param", K(ret), K(param));
  } else if (OB_FAIL(keys_holder.take_next(
                 MAX_SSTABLE_PER_BATCH, my_keys_, has_more))) {
    LOG_WARN("failed to take batch sstable keys", K(ret), K(keys_holder));
  } else if (OB_UNLIKELY(my_keys_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch sstable key slice is empty", K(ret), K(keys_holder));
  } else if (OB_FAIL(macro_block_reuse_mgr_.init())) {
    LOG_WARN("failed to initialize batch macro block reuse manager", K(ret));
  } else {
    param_ = param;
    is_inited_ = true;
  }
  return ret;
}

void ObBatchSSTableCopyTask::fill_copy_ctx_from_param_(
    const ObBatchSSTableCopyTaskParam &param,
    share::ObIDag *dag,
    ObPhysicalCopyCtx &copy_ctx)
{
  copy_ctx.tenant_id_ = MTL_ID();
  copy_ctx.ls_id_ = param.ls_->get_ls_id();
  copy_ctx.tablet_id_ = param.tablet_id_;
  copy_ctx.src_info_ = param.src_info_;
  copy_ctx.ha_svc_ctx_ = param.ha_svc_ctx_;
  copy_ctx.is_leader_restore_ = false;
  copy_ctx.restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
  copy_ctx.ha_dag_ = static_cast<ObStorageHADag *>(dag);
  copy_ctx.need_check_seq_ = true;
  copy_ctx.ls_rebuild_seq_ = param.src_ls_rebuild_seq_;
  copy_ctx.extra_info_ = param.extra_info_;
}

int ObBatchSSTableCopyTask::prepare_sstable_macro_range_info_(
    const ObMigrationSSTableParam &sstable_param,
    const ObCopySSTableMacroRangeInfo &sstable_macro_range_info,
    bool &reuse_supported)
{
  int ret = OB_SUCCESS;
  reuse_supported = false;
  ObArray<ObCopyMacroRangeIdInfo> &range_array =
      const_cast<ObCopySSTableMacroRangeInfo &>(
          sstable_macro_range_info).copy_macro_range_array_;
  bool already_prepared = !range_array.empty();
  for (int64_t i = 0; already_prepared && i < range_array.count(); ++i) {
    already_prepared = range_array.at(i).macro_block_ids_.count()
        == range_array.at(i).range_info_.macro_block_count_;
  }

  if (already_prepared) {
    reuse_supported = true;
  } else {
    for (int64_t i = 0; i < range_array.count(); ++i) {
      range_array.at(i).macro_block_ids_.reset();
    }

    ObCopySSTableMacroIdInfoReaderInitParam init_param;
    init_param.tenant_id_ = MTL_ID();
    init_param.ls_id_ = param_.ls_->get_ls_id();
    init_param.table_key_ = sstable_param.table_key_;
    init_param.src_info_ = param_.src_info_;
    init_param.is_leader_restore_ = false;
    init_param.restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
    init_param.ha_svc_ctx_ = param_.ha_svc_ctx_;
    init_param.need_check_seq_ = true;
    init_param.ls_rebuild_seq_ = param_.src_ls_rebuild_seq_;
    init_param.filled_tx_scn_ = sstable_param.basic_meta_.filled_tx_scn_;

    ObCopySSTableMacroIdInfoObReader reader;
    common::ObArray<ObLogicMacroBlockId> logic_ids;
    if (OB_FAIL(reader.init(init_param))) {
      if (OB_NOT_SUPPORTED == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("source does not support macro logic id RPC; skip batch reuse",
            K(sstable_param.table_key_));
      } else {
        LOG_WARN("failed to initialize macro logic id reader",
            K(ret), K(init_param));
      }
    } else if (OB_FAIL(reader.get_sstable_macro_logic_ids(logic_ids))) {
      LOG_WARN("failed to fetch source macro logic ids",
          K(ret), K(sstable_param.table_key_));
    } else if (logic_ids.empty()) {
      LOG_INFO("source macro logic id list is empty; skip batch reuse",
          K(sstable_param.table_key_));
    } else if (OB_FAIL(ObSSTableCopyFinishTask::
                   fill_logic_macro_info_for_range(
                       logic_ids, sstable_macro_range_info))) {
      LOG_WARN("failed to fill batch macro range logic ids",
          K(ret), K(sstable_param.table_key_));
    } else {
      reuse_supported = true;
    }

    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < range_array.count(); ++i) {
        range_array.at(i).macro_block_ids_.reset();
      }
    }
  }
  return ret;
}

int ObBatchSSTableCopyTask::prepare_macro_block_reuse_(
    const ObPhysicalCopyCtx &copy_ctx,
    const ObMigrationSSTableParam &sstable_param,
    const ObCopySSTableMacroRangeInfo *sstable_macro_range_info,
    const ObTabletHandle &tablet_handle,
    ObTableHandleV2 &split_src_sstable_handle)
{
  int ret = OB_SUCCESS;
  bool reuse_supported = false;
  macro_block_reuse_mgr_.reset();
  if (!ObITable::is_major_sstable(sstable_param.table_key_.table_type_)
      || sstable_param.is_small_sstable_
      || OB_ISNULL(sstable_macro_range_info)
      || sstable_macro_range_info->copy_macro_range_array_.empty()) {
    // Macro reuse is only applicable to non-empty, non-small major SSTables.
  } else if (OB_FAIL(prepare_sstable_macro_range_info_(
                 sstable_param, *sstable_macro_range_info,
                 reuse_supported))) {
    LOG_WARN("failed to prepare batch macro range reuse info",
        K(ret), K(sstable_param.table_key_));
  } else if (!reuse_supported) {
    // Older source: full macro copy remains correct.
  } else if (OB_FAIL(ObSSTableCopyFinishTask::build_sstable_reuse_info(
                 copy_ctx,
                 sstable_param,
                 tablet_handle,
                 *param_.tablet_copy_finish_task_,
                 *param_.ls_,
                 param_.tablet_copy_finish_task_->get_allocator(),
                 macro_block_reuse_mgr_,
                 split_src_sstable_handle))) {
    LOG_WARN("failed to build batch macro reuse info",
        K(ret), K(sstable_param.table_key_));
  }
  return ret;
}

int ObBatchSSTableCopyTask::process()
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = get_dag();
  const int64_t start_us = ObTimeUtility::current_time();
#ifdef ERRSIM
  int64_t batch_reuse_macro_count = 0;
#endif

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("batch sstable copy task is not initialized", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch sstable copy task has no dag", K(ret));
  } else if (OB_UNLIKELY(my_keys_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch sstable copy task has no keys", K(ret));
  } else {
    ObPhysicalCopyCtx copy_ctx;
    ObTabletHandle dest_tablet_handle;
    const ObTablet *cached_dest_tablet = nullptr;
    common::ObArenaAllocator &allocator =
        param_.tablet_copy_finish_task_->get_allocator();
    fill_copy_ctx_from_param_(param_, dag, copy_ctx);
    copy_ctx.macro_block_reuse_mgr_ = &macro_block_reuse_mgr_;

    if (OB_FAIL(param_.ls_->ha_get_tablet(param_.tablet_id_, dest_tablet_handle))) {
      LOG_WARN("failed to cache destination tablet for batch copy",
          K(ret), "tablet_id", param_.tablet_id_);
    } else if (OB_ISNULL(cached_dest_tablet = dest_tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("destination tablet is null for batch copy",
          K(ret), "tablet_id", param_.tablet_id_);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < my_keys_.count(); ++i) {
      const ObITable::TableKey &key = my_keys_.at(i);
      const blocksstable::ObMigrationSSTableParam *src_param = nullptr;
      const ObCopySSTableMacroRangeInfo *range_info = nullptr;
      ObTableHandleV2 table_handle;
      ObTableHandleV2 split_src_sstable_handle;

      if (OB_NOT_NULL(copy_ctx.ha_dag_)
          && OB_NOT_NULL(copy_ctx.ha_dag_->get_ha_dag_net_ctx())
          && copy_ctx.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
        LOG_INFO("ha dag net already failed, skip remaining batch sstable copies",
            "tablet_id", param_.tablet_id_, K(i));
        break;
      } else if (OB_FAIL(param_.table_info_mgr_->get_table_info(
                     param_.tablet_id_, key, src_param))) {
        LOG_WARN("failed to get source sstable info", K(ret),
            "tablet_id", param_.tablet_id_, K(key));
      } else if (OB_ISNULL(src_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("source sstable info is null", K(ret),
            "tablet_id", param_.tablet_id_, K(key));
      } else if (ObSSTableCopyOps::is_sstable_should_rebuild_index(
                     *src_param, false /* is_leader_restore */)
                 && OB_FAIL(param_.copy_sstable_info_mgr_->
                     get_copy_sstable_macro_range_info_ptr(key, range_info))) {
        LOG_WARN("failed to get prefetched macro range info", K(ret), K(key));
      } else if (OB_FAIL(prepare_macro_block_reuse_(
                     copy_ctx,
                     *src_param,
                     range_info,
                     dest_tablet_handle,
                     split_src_sstable_handle))) {
        LOG_WARN("failed to prepare batch macro block reuse",
            K(ret), K(key));
      } else if (OB_FAIL(ObSSTableCopyOps::copy_one_sstable(
                     copy_ctx,
                     *param_.ls_,
                     *param_.src_tablet_meta_,
                     *src_param,
                     range_info,
                     cached_dest_tablet,
                     allocator,
                     table_handle))) {
        LOG_WARN("failed to copy one sstable in batch", K(ret), K(key));
      } else if (OB_FAIL(
                     param_.tablet_copy_finish_task_->add_sstable(table_handle))) {
        LOG_WARN("failed to add batch-copied sstable", K(ret), K(key));
      } else if (OB_FAIL(copy_ctx.extra_info_->update_after_sstable_copy(
                     copy_ctx.table_key_, copy_ctx.get_total_macro_count(),
                     copy_ctx.get_reuse_macro_count(),
                     copy_ctx.macro_block_reuse_mgr_))) {
        LOG_WARN("failed to update batch-copied sstable extra info",
            K(ret), K(key));
      }
#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        batch_reuse_macro_count += copy_ctx.get_reuse_macro_count();
      }
#endif
    }

#ifdef ERRSIM
    if (OB_SUCC(ret) && batch_reuse_macro_count > 0) {
      SERVER_EVENT_ADD("storage_ha", "batch_cg_copy_reuse_macro",
          "tenant_id", MTL_ID(),
          "ls_id", copy_ctx.ls_id_.id(),
          "tablet_id", copy_ctx.tablet_id_.id(),
          "sstable_count", my_keys_.count(),
          "reuse_macro_count", batch_reuse_macro_count);
    }
#endif
  }

  if (OB_TABLET_NOT_EXIST == ret
      && OB_NOT_NULL(param_.tablet_copy_finish_task_)) {
    const int copy_ret = ret;
    int tmp_ret = param_.tablet_copy_finish_task_->set_tablet_status(
        ObCopyTabletStatus::TABLET_NOT_EXIST);
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to mark source tablet as not existent",
          K(ret), K(copy_ret), "tablet_id", param_.tablet_id_);
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("source tablet disappeared during batch sstable copy",
          "tablet_id", param_.tablet_id_);
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(dag)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS !=
        (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, dag))) {
      LOG_WARN("failed to deal with batch sstable copy failure",
          K(ret), K(tmp_ret));
    }
  }

  LOG_INFO("batch sstable copy task finished",
      "tablet_id", param_.tablet_id_,
      "key_count", my_keys_.count(),
      "cost_time_us", ObTimeUtility::current_time() - start_us);
  return ret;
}

ObBatchSSTableCopyPlanParam::ObBatchSSTableCopyPlanParam()
  : tenant_id_(OB_INVALID_ID),
    op_type_(ObMigrationOpType::MAX_LS_OP),
    tablet_id_(),
    table_info_mgr_(nullptr),
    copy_sstable_info_mgr_(nullptr)
{
}

bool ObBatchSSTableCopyPlanParam::is_valid() const
{
  return OB_INVALID_ID != tenant_id_
      && ObMigrationOpType::is_valid(op_type_)
      && tablet_id_.is_valid()
      && OB_NOT_NULL(table_info_mgr_)
      && OB_NOT_NULL(copy_sstable_info_mgr_);
}

void ObBatchSSTableCopyTaskGenerator::check_batch_cg_copy_enabled_(
    const ObBatchSSTableCopyPlanParam &param,
    bool &enabled)
{
  enabled = false;
  uint64_t data_version = 0;
  int tmp_ret = OB_SUCCESS;

  if (GCTX.is_shared_storage_mode()) {
    // The inline copy ops only support shared-nothing migration.
  } else if (ObMigrationOpType::MIGRATE_LS_OP != param.op_type_
      && ObMigrationOpType::ADD_LS_OP != param.op_type_
      && ObMigrationOpType::REBUILD_TABLET_OP != param.op_type_) {
    // Keep all other migration/restore operations on the legacy path.
  } else if (OB_SUCCESS !=
      (tmp_ret = GET_MIN_DATA_VERSION(param.tenant_id_, data_version))) {
    LOG_WARN_RET(tmp_ret,
        "failed to get tenant data version, disable batch CG copy",
        K(tmp_ret), "tenant_id", param.tenant_id_);
  } else {
    enabled = data_version >= MOCK_DATA_VERSION_4_4_2_2;
  }

  // Per driver round and called for every CG tablet; keep at DEBUG to avoid
  // flooding the log on wide column-store clusters.
  LOG_DEBUG("check batch CG sstable copy",
      K(enabled), K(data_version), "op_type", param.op_type_);
}

int ObBatchSSTableCopyTaskGenerator::check_key_batch_eligible_(
    const ObBatchSSTableCopyPlanParam &param,
    const ObITable::TableKey &key,
    bool &is_eligible)
{
  int ret = OB_SUCCESS;
  const blocksstable::ObMigrationSSTableParam *src_param = nullptr;
#ifdef ERRSIM
  // pushes the CG sstable whose column group id is -EN_BATCH_CG_COPY_FORCE_LEGACY_CG onto the
  // legacy per-sstable copy chain, so a mixed batch / legacy driver chain can be tested
  const int errsim_ret = EN_BATCH_CG_COPY_FORCE_LEGACY_CG;
  const bool force_legacy_cg = OB_SUCCESS != errsim_ret
      && key.get_column_group_id() == -errsim_ret;
#else
  const bool force_legacy_cg = false;
#endif
  is_eligible = false;

  if (!key.is_cg_sstable()) {
    // Only CG sstables are batched, a normal sstable keeps its own copy chain.
  } else if (OB_FAIL(param.table_info_mgr_->get_table_info(param.tablet_id_, key, src_param))) {
    LOG_WARN("failed to get CG sstable info", K(ret), K(key));
  } else if (OB_ISNULL(src_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("CG sstable info is null", K(ret), K(key));
  } else if (src_param->basic_meta_.table_shared_flag_.is_shared_macro_blocks()
      || src_param->is_shared_sstable()) {
    // A shared sstable needs the copy start task of the legacy chain.
  } else if (force_legacy_cg) {
    LOG_INFO("[ERRSIM] force CG sstable onto legacy copy path", K(key),
        "data_macro_block_count", src_param->basic_meta_.data_macro_block_count_);
  } else if (src_param->basic_meta_.data_macro_block_count_
      > 2 * ObSSTableCopyOps::MACRO_RANGE_MAX_MACRO_COUNT) {
    // Batch copies an SSTable's ranges serially in one worker, while the
    // legacy path fans them out as parallel ObPhysicalCopyTasks. Keep at
    // most two standard ranges in batch to avoid a long serial-copy tail.
    LOG_DEBUG("CG sstable is too large to batch copy", K(key),
        "data_macro_block_count", src_param->basic_meta_.data_macro_block_count_);
  } else {
    is_eligible = true;
  }
  return ret;
}

int ObBatchSSTableCopyTaskGenerator::plan_batch_copy_unit(
    const ObBatchSSTableCopyPlanParam &param,
    const common::ObIArray<ObITable::TableKey> &copy_table_key_array,
    const int64_t start_index,
    ObISSTableCopyScanPolicy &scan_policy,
    ObSSTableCopyUnit &unit)
{
  int ret = OB_SUCCESS;
  bool enabled = false;
  bool is_src_tablet_exist = true;
  unit.reset();

  if (OB_UNLIKELY(!param.is_valid()
      || start_index < 0
      || start_index >= copy_table_key_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan batch copy unit get invalid argument", K(ret), K(param), K(start_index),
        "key_count", copy_table_key_array.count());
    // The scanner has already checked that the sstable at start_index needs copy, so a
    // non-CG sstable here means this round copies one sstable by the legacy copy chain.
    // Skip the version / tablet checks in that (common) case, they are not free.
  } else if (!copy_table_key_array.at(start_index).is_cg_sstable()) {
    unit.consumed_count_ = 1;
  } else if (FALSE_IT(check_batch_cg_copy_enabled_(param, enabled))) {
  } else if (!enabled) {
    unit.consumed_count_ = 1;
  } else if (OB_FAIL(param.copy_sstable_info_mgr_->check_src_tablet_exist(is_src_tablet_exist))) {
    LOG_WARN("failed to check source tablet status", K(ret), K(param));
  } else if (!is_src_tablet_exist) {
    // The legacy copy chain reports the vanished source tablet to the tablet copy finish task.
    unit.consumed_count_ = 1;
  } else {
    // Collect the run of batch-eligible CG sstables starting at start_index. The run stops at
    // the first sstable that still needs copy but cannot be batched, so that sstable is copied
    // by the legacy chain in the next driver round; the sstables the local replica already
    // contains are consumed here, exactly like the lazy scan would skip them.
    bool is_run_end = false;
    for (int64_t i = start_index;
         OB_SUCC(ret) && !is_run_end && i < copy_table_key_array.count()
             && unit.batch_keys_.count() < ObBatchSSTableCopyTask::MAX_SSTABLE_PER_BATCH;
         ++i) {
      const ObITable::TableKey &key = copy_table_key_array.at(i);
      bool need_copy = true;
      bool is_eligible = false;
      if (OB_FAIL(ObSSTableCopyChainScanner::check_one_key(key, scan_policy, need_copy))) {
        LOG_WARN("failed to check copy table key", K(ret), K(key));
      } else if (!need_copy) {
        unit.consumed_count_ = i + 1 - start_index;
      } else if (OB_FAIL(check_key_batch_eligible_(param, key, is_eligible))) {
        LOG_WARN("failed to check whether sstable can be batch copied", K(ret), K(key));
      } else if (!is_eligible) {
        is_run_end = true;
      } else if (OB_FAIL(unit.batch_keys_.push_back(key))) {
        LOG_WARN("failed to append batch CG key", K(ret), K(key));
      } else {
        unit.consumed_count_ = i + 1 - start_index;
      }
    }

    if (OB_SUCC(ret)) {
      if (unit.batch_keys_.empty()) {
        // the sstable at start_index needs copy but cannot be batched
        unit.reset();
        unit.consumed_count_ = 1;
      } else {
        LOG_INFO("succeed to plan batch CG sstable copy unit", "tablet_id", param.tablet_id_,
            K(start_index), K(unit));
      }
    }
  }
  return ret;
}

int ObBatchSSTableCopyTaskGenerator::generate_batch_copy_task(
    const ObBatchSSTableCopyTaskParam &param,
    const common::ObIArray<ObITable::TableKey> &batch_keys,
    share::ObIDag *dag,
    share::ObITask *parent_task,
    share::ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObBatchSSTableCopyTask *batch_task = nullptr;
  ObBatchSSTableKeysHolder keys_holder(get_ha_mem_tenant_id());

  if (OB_UNLIKELY(!param.is_valid()
      || batch_keys.empty()
      || batch_keys.count() > ObBatchSSTableCopyTask::MAX_SSTABLE_PER_BATCH
      || OB_ISNULL(dag)
      || OB_ISNULL(parent_task)
      || OB_ISNULL(child_task))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch copy task argument", K(ret), K(param),
        "batch_key_count", batch_keys.count(), KP(dag), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(keys_holder.assign(batch_keys))) {
    LOG_WARN("failed to assign batch CG keys", K(ret), "batch_key_count", batch_keys.count());
  } else if (OB_FAIL(dag->alloc_task(batch_task))) {
    LOG_WARN("failed to allocate batch sstable copy task", K(ret));
    // the whole run fits into one task, see ObBatchSSTableKeysHolder::take_next
  } else if (OB_FAIL(batch_task->init(param, keys_holder))) {
    LOG_WARN("failed to initialize batch sstable copy task", K(ret), K(keys_holder));
  } else if (OB_UNLIKELY(keys_holder.has_more())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch sstable copy task did not take all planned keys", K(ret), K(keys_holder));
  } else if (OB_FAIL(parent_task->add_child(*batch_task))) {
    LOG_WARN("failed to add batch sstable copy task as child of parent", K(ret));
  } else if (OB_FAIL(batch_task->add_child(*child_task))) {
    LOG_WARN("failed to add child task of batch sstable copy task", K(ret));
  } else if (OB_FAIL(dag->add_task(*batch_task))) {
    LOG_WARN("failed to add batch sstable copy task to dag", K(ret));
  } else {
    LOG_INFO("succeed to generate batch CG sstable copy task",
        "tablet_id", param.tablet_id_, "batch_key_count", batch_keys.count());
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
