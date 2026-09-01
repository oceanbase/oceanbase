/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_batch_sstable_copy_task.h"
#include "ob_ls_migration.h"
#include "ob_physical_copy_ctx.h"
#include "ob_sstable_copy_ops.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_tablet_builder.h"
#include "ob_tablet_copy_finish_task.h"
#include "observer/ob_server.h"
#include "share/ob_cluster_version.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{

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
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    src_ls_rebuild_seq_(-1),
    macro_block_reuse_mgr_(nullptr)
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
      && OB_NOT_NULL(bandwidth_throttle_)
      && OB_NOT_NULL(svr_rpc_proxy_)
      && src_ls_rebuild_seq_ >= 0;
}

ObBatchSSTableCopyTask::ObBatchSSTableCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
    is_inited_(false),
    param_(),
    my_keys_()
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
  copy_ctx.bandwidth_throttle_ = param.bandwidth_throttle_;
  copy_ctx.svr_rpc_proxy_ = param.svr_rpc_proxy_;
  copy_ctx.is_leader_restore_ = false;
  copy_ctx.restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
  copy_ctx.ha_dag_ = static_cast<ObStorageHADag *>(dag);
  copy_ctx.need_sort_macro_meta_ = false;
  copy_ctx.need_check_seq_ = true;
  copy_ctx.ls_rebuild_seq_ = param.src_ls_rebuild_seq_;
  copy_ctx.macro_block_reuse_mgr_ = param.macro_block_reuse_mgr_;
  copy_ctx.extra_info_ = param.extra_info_;
}

int ObBatchSSTableCopyTask::update_major_sstable_reuse_info_(
    const ObITable::TableKey &table_key,
    const ObTabletHandle &tablet_handle,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  int64_t reuse_info_count = 0;
  blocksstable::ObSSTable *sstable = nullptr;
  ObMacroBlockReuseMgr *reuse_mgr = param_.macro_block_reuse_mgr_;

  if (!ObITable::is_major_sstable(table_key.table_type_)) {
    // Macro-block reuse is only supported for major SSTables.
  } else if (GCTX.is_shared_storage_mode()) {
    LOG_INFO("skip updating batch major reuse info in shared storage mode",
        K(table_key));
  } else if (OB_ISNULL(reuse_mgr)) {
    LOG_WARN("macro block reuse mgr is null, skip updating batch major reuse info",
        K(table_key), KP(reuse_mgr));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid destination tablet handle for batch major reuse info",
        K(ret), K(table_key), K(tablet_handle));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get batch-copied major sstable",
        K(ret), K(table_key), K(table_handle));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch-copied major sstable is null",
        K(ret), K(table_key), K(table_handle));
  } else {
    if (OB_FAIL(reuse_mgr->update_single_reuse_map(
            table_key, tablet_handle, *sstable))) {
      LOG_WARN("failed to update batch major reuse map",
          K(ret), K(table_key), KPC(sstable));
    } else if (OB_FAIL(reuse_mgr->count(reuse_info_count))) {
      LOG_WARN("failed to count batch major reuse info",
          K(ret), K(table_key));
    } else {
      LOG_INFO("succeed updating batch major reuse info",
          K(table_key), K(reuse_info_count));
    }

    // update_single_reuse_map() removes the previous version before building
    // the new map. Match the legacy finish task by discarding all reuse state
    // if publishing or counting the replacement fails.
    if (OB_FAIL(ret)) {
      reuse_mgr->reset();
    }
  }
  return ret;
}

int ObBatchSSTableCopyTask::process()
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = get_dag();
  const int64_t start_us = ObTimeUtility::current_time();

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
      } else if (FALSE_IT(copy_ctx.macro_block_reuse_mgr_ =
                     ObITable::is_major_sstable(key.table_type_)
                         ? param_.macro_block_reuse_mgr_
                         : nullptr)) {
      } else if (ObSSTableCopyOps::is_sstable_should_rebuild_index(
                     *src_param, false /* is_leader_restore */)
                 && OB_FAIL(param_.copy_sstable_info_mgr_->
                     get_copy_sstable_macro_range_info_ptr(key, range_info))) {
        LOG_WARN("failed to get prefetched macro range info", K(ret), K(key));
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
      } else if (OB_FAIL(update_major_sstable_reuse_info_(
                     key, dest_tablet_handle, table_handle))) {
        LOG_WARN("failed to update batch-copied major reuse info",
            K(ret), K(key));
      } else if (OB_FAIL(copy_ctx.extra_info_->update_after_sstable_copy(
                     copy_ctx.table_key_, copy_ctx.total_macro_count_,
                     copy_ctx.reuse_macro_count_,
                     copy_ctx.macro_block_reuse_mgr_))) {
        LOG_WARN("failed to update batch-copied sstable extra info",
            K(ret), K(key));
      }
    }
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

void ObBatchSSTableCopyTaskGenerator::check_batch_cg_copy_enabled_(
    const ObTabletMigrationTask &migration_task,
    bool &enabled)
{
  enabled = false;
  uint64_t data_version = 0;
  int tmp_ret = OB_SUCCESS;
  const ObMigrationCtx *ctx = migration_task.ctx_;

  if (OB_ISNULL(ctx) || GCTX.is_shared_storage_mode()) {
    // The inline copy ops only support shared-nothing migration.
  } else if (ObMigrationOpType::MIGRATE_LS_OP != ctx->arg_.type_
      && ObMigrationOpType::ADD_LS_OP != ctx->arg_.type_
      && ObMigrationOpType::REBUILD_TABLET_OP != ctx->arg_.type_) {
    // Keep all other migration/restore operations on the legacy path.
  } else if (OB_SUCCESS !=
      (tmp_ret = GET_MIN_DATA_VERSION(ctx->tenant_id_, data_version))) {
    LOG_WARN_RET(tmp_ret,
        "failed to get tenant data version, disable batch CG copy",
        K(tmp_ret), "tenant_id", ctx->tenant_id_);
  } else {
    enabled = data_version >= DATA_VERSION_4_4_2_2;
  }

  // Per tablet-stage and called for every CG tablet; keep at DEBUG to avoid
  // flooding the log on wide column-store clusters.
  LOG_DEBUG("check batch CG sstable copy",
      K(enabled), K(data_version),
      "op_type", OB_ISNULL(ctx) ? ObMigrationOpType::MAX_LS_OP : ctx->arg_.type_);
}

int ObBatchSSTableCopyTaskGenerator::classify_copy_keys_(
    ObTabletMigrationTask &migration_task,
    IsRightTypeSSTableFunc is_right_type_sstable,
    common::ObIArray<ObITable::TableKey> &batch_keys,
    common::ObIArray<ObITable::TableKey> &legacy_keys)
{
  int ret = OB_SUCCESS;
  bool enabled = false;
  bool has_cg_key = false;
  bool is_src_tablet_exist = true;
  batch_keys.reset();
  legacy_keys.reset();

  for (int64_t i = 0;
       !has_cg_key && i < migration_task.copy_table_key_array_.count();
       ++i) {
    const ObITable::TableKey &key =
        migration_task.copy_table_key_array_.at(i);
    has_cg_key = key.is_valid()
        && is_right_type_sstable(key.table_type_)
        && key.is_cg_sstable();
  }

  if (has_cg_key) {
    check_batch_cg_copy_enabled_(migration_task, enabled);
  }
  if (enabled
      && OB_FAIL(migration_task.copy_tablet_ctx_->copy_sstable_info_mgr_.
          check_src_tablet_exist(is_src_tablet_exist))) {
    LOG_WARN("failed to check source tablet status", K(ret));
  } else if (!is_src_tablet_exist) {
    enabled = false;
  }

  for (int64_t i = 0;
       OB_SUCC(ret) && i < migration_task.copy_table_key_array_.count();
       ++i) {
    const ObITable::TableKey &key =
        migration_task.copy_table_key_array_.at(i);
    bool need_copy = true;
    const blocksstable::ObMigrationSSTableParam *src_param = nullptr;

    if (!key.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid copy table key", K(ret), K(key));
    } else if (!is_right_type_sstable(key.table_type_)) {
      // Belongs to another stage.
    } else if (OB_FAIL(
                   migration_task.check_need_copy_sstable_(key, need_copy))) {
      LOG_WARN("failed to check whether sstable needs copying", K(ret), K(key));
    } else if (!need_copy) {
      LOG_INFO("local tablet already contains sstable", K(key));
    } else if (enabled && key.is_cg_sstable()) {
      if (OB_FAIL(migration_task.ctx_->ha_table_info_mgr_.get_table_info(
              migration_task.copy_tablet_ctx_->tablet_id_, key, src_param))) {
        LOG_WARN("failed to get CG sstable info", K(ret), K(key));
      } else if (OB_ISNULL(src_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("CG sstable info is null", K(ret), K(key));
      } else if (src_param->basic_meta_.table_shared_flag_.
                     is_shared_macro_blocks()
                 || src_param->is_shared_sstable()) {
        if (OB_FAIL(legacy_keys.push_back(key))) {
          LOG_WARN("failed to append shared CG to legacy keys", K(ret), K(key));
        }
      } else if (src_param->basic_meta_.data_macro_block_count_
                 > 2 * ObSSTableCopyOps::MACRO_RANGE_MAX_MACRO_COUNT) {
        // Batch copies an SSTable's ranges serially in one worker, while the
        // legacy path fans them out as parallel ObPhysicalCopyTasks. Keep at
        // most two standard ranges in batch to avoid a long serial-copy tail.
        if (OB_FAIL(legacy_keys.push_back(key))) {
          LOG_WARN("failed to append large CG to legacy keys", K(ret), K(key),
              "data_macro_block_count",
              src_param->basic_meta_.data_macro_block_count_);
        }
      } else if (OB_FAIL(batch_keys.push_back(key))) {
        LOG_WARN("failed to append batch CG key", K(ret), K(key));
      }
    } else if (OB_FAIL(legacy_keys.push_back(key))) {
      LOG_WARN("failed to append legacy sstable key", K(ret), K(key));
    }
  }
  return ret;
}

int ObBatchSSTableCopyTaskGenerator::build_batch_copy_tasks_(
    ObTabletMigrationTask &migration_task,
    const common::ObIArray<ObITable::TableKey> &batch_keys,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = migration_task.get_dag();
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  ObBatchSSTableCopyTaskParam param;
  ObBatchSSTableKeysHolder keys_holder(get_ha_mem_tenant_id());

  if (batch_keys.empty()) {
    // Nothing to build.
  } else if (OB_ISNULL(dag)
      || OB_ISNULL(parent_task)
      || OB_ISNULL(tablet_copy_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch copy chain argument", K(ret),
        KP(dag), KP(parent_task), KP(tablet_copy_finish_task));
  } else if (FALSE_IT(
                 tablet_migration_dag =
                     static_cast<ObTabletMigrationDag *>(dag))) {
  } else if (OB_FAIL(tablet_migration_dag->get_ls(param.ls_))) {
    LOG_WARN("failed to get migration LS", K(ret));
  } else if (OB_FAIL(migration_task.ctx_->ha_table_info_mgr_.get_tablet_meta(
                 migration_task.copy_tablet_ctx_->tablet_id_,
                 param.src_tablet_meta_))) {
    LOG_WARN("failed to get source tablet meta", K(ret));
  } else if (OB_FAIL(keys_holder.assign(batch_keys))) {
    LOG_WARN("failed to assign batch CG keys", K(ret));
  } else {
    param.tablet_id_ = migration_task.copy_tablet_ctx_->tablet_id_;
    param.src_info_ = migration_task.ctx_->minor_src_;
    param.table_info_mgr_ = &migration_task.ctx_->ha_table_info_mgr_;
    param.copy_sstable_info_mgr_ =
        &migration_task.copy_tablet_ctx_->copy_sstable_info_mgr_;
    param.tablet_copy_finish_task_ = tablet_copy_finish_task;
    param.extra_info_ = &migration_task.copy_tablet_ctx_->extra_info_;
    param.bandwidth_throttle_ = migration_task.bandwidth_throttle_;
    param.svr_rpc_proxy_ = migration_task.svr_rpc_proxy_;
    param.src_ls_rebuild_seq_ = migration_task.ctx_->src_ls_rebuild_seq_;
    param.macro_block_reuse_mgr_ =
        &migration_task.copy_tablet_ctx_->macro_block_reuse_mgr_;

    int64_t task_count = 0;
    while (OB_SUCC(ret) && keys_holder.has_more()) {
      ObBatchSSTableCopyTask *batch_task = nullptr;
      if (OB_FAIL(dag->alloc_task(batch_task))) {
        LOG_WARN("failed to allocate batch sstable copy task",
            K(ret), K(task_count));
      } else if (OB_FAIL(batch_task->init(param, keys_holder))) {
        LOG_WARN("failed to initialize batch sstable copy task",
            K(ret), K(task_count));
      } else if (OB_FAIL(parent_task->add_child(*batch_task))) {
        LOG_WARN("failed to chain batch sstable copy task",
            K(ret), K(task_count));
      } else if (OB_FAIL(dag->add_task(*batch_task))) {
        LOG_WARN("failed to add batch sstable copy task",
            K(ret), K(task_count));
      } else {
        parent_task = batch_task;
        ++task_count;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("built batch CG sstable copy chain",
          "tablet_id", param.tablet_id_,
          "key_count", batch_keys.count(),
          K(task_count));
    }
  }
  return ret;
}

int ObBatchSSTableCopyTaskGenerator::build_legacy_copy_tasks_(
    ObTabletMigrationTask &migration_task,
    const common::ObIArray<ObITable::TableKey> &legacy_keys,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  share::ObIDag *dag = migration_task.get_dag();
  if (legacy_keys.empty()) {
    // Nothing to build.
  } else if (OB_ISNULL(dag)
      || OB_ISNULL(parent_task)
      || OB_ISNULL(tablet_copy_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid legacy copy chain argument", K(ret),
        KP(dag), KP(parent_task), KP(tablet_copy_finish_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < legacy_keys.count(); ++i) {
      const ObITable::TableKey &key = legacy_keys.at(i);
      share::ObFakeTask *wait_finish_task = nullptr;
      if (OB_FAIL(dag->alloc_task(wait_finish_task))) {
        LOG_WARN("failed to allocate legacy wait task", K(ret), K(key));
      } else if (OB_FAIL(migration_task.generate_physical_copy_task_(
                     migration_task.ctx_->minor_src_,
                     key,
                     tablet_copy_finish_task,
                     parent_task,
                     wait_finish_task))) {
        LOG_WARN("failed to generate legacy physical copy task",
            K(ret), K(key));
      } else if (OB_FAIL(dag->add_task(*wait_finish_task))) {
        LOG_WARN("failed to add legacy wait task", K(ret), K(key));
      } else {
        parent_task = wait_finish_task;
        LOG_INFO("generated legacy sstable copy task", K(key));
      }
    }
  }
  return ret;
}

int ObBatchSSTableCopyTaskGenerator::try_generate_copy_tasks(
    ObTabletMigrationTask &migration_task,
    IsRightTypeSSTableFunc is_right_type_sstable,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task,
    bool &is_generated)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObITable::TableKey> batch_keys;
  common::ObArray<ObITable::TableKey> legacy_keys;
  is_generated = false;

  if (!migration_task.is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task is not initialized", K(ret));
  } else if (OB_ISNULL(is_right_type_sstable)
      || OB_ISNULL(tablet_copy_finish_task)
      || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sstable copy generator argument", K(ret),
        KP(is_right_type_sstable),
        KP(tablet_copy_finish_task),
        KP(parent_task));
  } else if (OB_FAIL(classify_copy_keys_(
                 migration_task,
                 is_right_type_sstable,
                 batch_keys,
                 legacy_keys))) {
    LOG_WARN("failed to classify sstable copy keys", K(ret));
  } else if (batch_keys.empty()) {
    // Preserve the original per-SSTable path when this stage has no eligible
    // CG SSTable. Besides reducing behavior changes, this keeps the legacy
    // implementation in ObTabletMigrationTask intact.
  } else if (OB_FAIL(build_batch_copy_tasks_(
                 migration_task,
                 batch_keys,
                 tablet_copy_finish_task,
                 parent_task))) {
    LOG_WARN("failed to build batch CG copy tasks", K(ret));
  } else if (OB_FAIL(build_legacy_copy_tasks_(
                 migration_task,
                 legacy_keys,
                 tablet_copy_finish_task,
                 parent_task))) {
    LOG_WARN("failed to build legacy sstable copy tasks", K(ret));
  } else {
    is_generated = true;
    LOG_INFO("generated staged sstable copy tasks",
        "tablet_id", migration_task.copy_tablet_ctx_->tablet_id_,
        "batch_count", batch_keys.count(),
        "legacy_count", legacy_keys.count());
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
