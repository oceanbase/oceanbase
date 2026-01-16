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

#define USING_LOG_PREFIX STORAGE
#include "ob_sstable_copy_finish_task.h"
#include "lib/ob_define.h"
#include "lib/thread/ob_thread_name.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "src/storage/high_availability/ob_storage_ha_macro_block_writer.h"
#include "storage/high_availability/ob_storage_ha_tablet_builder.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/compaction/ob_shared_storage_compaction_util.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace compaction;
namespace storage
{

//errsim def
ERRSIM_POINT_DEF(PHYSICAL_COPY_TASK_GET_TABLET_FAILED);
ERRSIM_POINT_DEF(SSTABLE_COPY_FINISH_TASK_FAILED);

// ObPhysicalCopyTaskInitParam
ObPhysicalCopyTaskInitParam::ObPhysicalCopyTaskInitParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    src_info_(),
    sstable_param_(nullptr),
    sstable_macro_range_info_(),
    tablet_copy_finish_task_(nullptr),
    ls_(nullptr),
    is_leader_restore_(false),
    restore_action_(ObTabletRestoreAction::RESTORE_NONE),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    need_sort_macro_meta_(true),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    macro_block_reuse_mgr_(nullptr),
    extra_info_(nullptr)
{
}

ObPhysicalCopyTaskInitParam::~ObPhysicalCopyTaskInitParam()
{
}

bool ObPhysicalCopyTaskInitParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID
             && ls_id_.is_valid()
             && tablet_id_.is_valid()
             && OB_NOT_NULL(sstable_param_)
             && sstable_macro_range_info_.is_valid()
             && OB_NOT_NULL(tablet_copy_finish_task_)
             && OB_NOT_NULL(ls_)
             && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);

  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid();
    } else if (OB_ISNULL(restore_base_info_)
               || OB_ISNULL(meta_index_store_)
               || OB_ISNULL(second_meta_index_store_)) {
      bool_ret = false;
    }
  }

  return bool_ret;
}

void ObPhysicalCopyTaskInitParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  src_info_.reset();
  sstable_param_ = nullptr;
  sstable_macro_range_info_.reset();
  tablet_copy_finish_task_ = nullptr;
  ls_ = nullptr;
  is_leader_restore_ = false;
  restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  need_sort_macro_meta_ = true;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  extra_info_ = nullptr;
}


// ObCopiedSSTableCreatorImpl
ObCopiedSSTableCreatorImpl::ObCopiedSSTableCreatorImpl()
  : is_inited_(false),
    ls_id_(),
    tablet_id_(),
    src_sstable_param_(nullptr),
    sstable_index_builder_(nullptr),
    allocator_(nullptr),
    finish_task_(nullptr)
{

}

int ObCopiedSSTableCreatorImpl::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMigrationSSTableParam *src_sstable_param,
    ObSSTableIndexBuilder *sstable_index_builder,
    common::ObArenaAllocator *allocator,
    ObSSTableCopyFinishTask *finish_task)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCopiedSSTableCreatorImpl init twice", K(ret));
  } else if (OB_ISNULL(src_sstable_param)
             || OB_ISNULL(sstable_index_builder)
             || OB_ISNULL(allocator)
             || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), K(ls_id), K(tablet_id), KP(src_sstable_param),
      KP(sstable_index_builder), KP(allocator), KP(finish_task));
  } else if (OB_FAIL(check_sstable_param_for_init_(src_sstable_param))) {
    LOG_WARN("failed to check sstable param", K(ret));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    src_sstable_param_ = src_sstable_param;
    sstable_index_builder_ = sstable_index_builder;
    allocator_ = allocator;
    finish_task_ = finish_task;
    is_inited_ = true;
  }

  return ret;
}

int ObCopiedSSTableCreatorImpl::init_create_sstable_param_(
    ObTabletCreateSSTableParam &param,
    const common::ObIArray<blocksstable::MacroBlockId> &data_block_ids,
    const common::ObIArray<blocksstable::MacroBlockId> &other_block_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param.init_for_ha(*src_sstable_param_, data_block_ids, other_block_ids))) {
    LOG_WARN("fail to init create sstable param", K(ret), KPC(src_sstable_param_));
  }
  return ret;
}

int ObCopiedSSTableCreatorImpl::init_create_sstable_param_(
    const blocksstable::ObSSTableMergeRes &res,
    ObTabletCreateSSTableParam &param) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param.init_for_ha(*src_sstable_param_, res))) {
    LOG_WARN("fail to init create sstable param", K(ret), KPC(src_sstable_param_));
  }
  return ret;
}

int ObCopiedSSTableCreatorImpl::do_create_sstable_(
    const ObTabletCreateSSTableParam &param,
    ObTableHandleV2 &table_handle) const
{
  int ret = OB_SUCCESS;
  if (param.table_key().is_co_sstable()) {
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param,
                                                                          *allocator_,
                                                                          table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(param));
    }
  } else {
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param,
                                                           *allocator_,
                                                           table_handle))) {
      LOG_WARN("failed to create sstable", K(ret), K(param));
    }
  }
#ifdef ERRSIM
  char *origin_thread_name = ob_get_tname_v2();
  int64_t base_snapshot_version = src_sstable_param_->table_key_.get_end_scn().get_val_for_logservice();
  SERVER_EVENT_SYNC_ADD("copy_errsim", "create_sstable",
                        "origin_thread_name", origin_thread_name,
                        "tablet_id", src_sstable_param_->table_key_.tablet_id_.id(),
                        "column_group_idx", src_sstable_param_->table_key_.column_group_idx_,
                        "base_snapshot_version", base_snapshot_version,
                        "co_base_snapshot_version", param.co_base_snapshot_version());
#endif

  LOG_INFO("create sstable", K(ret), KPC_(src_sstable_param), K(param));

  return ret;
}


// ObCopiedEmptySSTableCreator
int ObCopiedEmptySSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObTabletCreateSSTableParam param;
  const int64_t MACRO_BLOCK_CNT = 1;
  common::ObSEArray<blocksstable::MacroBlockId, MACRO_BLOCK_CNT> data_block_ids;
  common::ObSEArray<blocksstable::MacroBlockId, MACRO_BLOCK_CNT> other_block_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopiedEmptySSTableCreator not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(init_create_sstable_param_(param, data_block_ids, other_block_ids))) {
        LOG_WARN("fail to init create sstable param", K(ret));
      } else if (OB_FAIL(do_create_sstable_(param, table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(param));
#ifdef OB_BUILD_SHARED_STORAGE
      } else if (src_sstable_param_->is_shared_sstable()) {
        // empty sstable may be shared sstable.
        int64_t last_meta_macro_seq = 1;
        last_meta_macro_seq = MAX(src_sstable_param_->basic_meta_.root_macro_seq_,
                finish_task_->get_max_next_copy_task_id() * oceanbase::compaction::MACRO_STEP_SIZE);
        if (OB_FAIL(finish_task_->add_sstable(table_handle, last_meta_macro_seq))) {
          LOG_WARN("fail to add sstable", K(ret), K(table_handle));
        }
#endif
      } else if (OB_FAIL(finish_task_->add_sstable(table_handle))) {
        LOG_WARN("fail to add sstable", K(ret), K(table_handle));
      }
    }
  }

  LOG_INFO("create empty sstable", K(ret), K(table_handle));

  return ret;
}

int ObCopiedEmptySSTableCreator::check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const
{
  int ret = OB_SUCCESS;
  if (!src_sstable_param->is_empty_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is not empty", K(ret), KPC(src_sstable_param));
  }

  return ret;
}


// ObCopiedSSTableCreator
int ObCopiedSSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  ObTableHandleV2 table_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopiedSSTableCreator not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(sstable_index_builder_->close(res))) {
        LOG_WARN("failed to close sstable index builder", K(ret), K(tablet_id_), KPC(src_sstable_param_));
      } else if (OB_FAIL(init_create_sstable_param_(res, param))) {
        LOG_WARN("fail to init create sstable param", K(ret), K(res));
      } else if (OB_FAIL(do_create_sstable_(param, table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(param));
      } else if (OB_FAIL(finish_task_->add_sstable(table_handle))) {
        LOG_WARN("fail to add sstable", K(ret), K(table_handle));
      }
    }
  }

  LOG_INFO("create local sstable with index builder", K(ret), K(table_handle));

  return ret;
}

int ObCopiedSSTableCreator::check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const
{
  int ret = OB_SUCCESS;
  if (src_sstable_param->is_empty_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is empty", K(ret), KPC(src_sstable_param));
  } else if (src_sstable_param->basic_meta_.table_shared_flag_.is_shared_macro_blocks()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable has shared macro blocks and it's not shared backup table", K(ret), KPC(src_sstable_param));
  }

  return ret;
}


#ifdef OB_BUILD_SHARED_STORAGE
// ObRestoredSharedSSTableCreator
int ObRestoredSharedSSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  ObTableHandleV2 table_handle;
  ObMacroDataSeq meta_block_seq(1);
  meta_block_seq.macro_data_seq_ = MAX(src_sstable_param_->basic_meta_.root_macro_seq_,
          finish_task_->get_max_next_copy_task_id() * oceanbase::compaction::MACRO_STEP_SIZE);
  share::ObPreWarmerParam pre_warm_param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopiedSSTableCreator not init", K(ret));
  } else {
    meta_block_seq.set_index_block();
    pre_warm_param.init(ls_id_, tablet_id_);
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(sstable_index_builder_->close_with_macro_seq(res,
                                                               meta_block_seq.macro_data_seq_,
                                                               OB_DEFAULT_MACRO_BLOCK_SIZE /*nested_size*/,
                                                               0 /*nested_offset*/,
                                                               pre_warm_param))) {
        LOG_WARN("failed to close sstable index builder", K(ret), K(tablet_id_), KPC(src_sstable_param_));
      } else if (OB_FAIL(init_create_sstable_param_(res, param))) {
        LOG_WARN("fail to init create sstable param", K(ret), K(res));
      } else if (OB_FAIL(do_create_sstable_(param, table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(param));
      } else if (OB_FAIL(finish_task_->add_sstable(table_handle, meta_block_seq.macro_data_seq_))) {
        LOG_WARN("fail to add sstable", K(ret), K(table_handle), K(meta_block_seq));
      }
    }
  }

  LOG_INFO("create restore shared sstable with index builder", K(ret), K(table_handle));

  return ret;
}

int ObRestoredSharedSSTableCreator::check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const
{
  int ret = OB_SUCCESS;
  if (!src_sstable_param->is_shared_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is not shared", K(ret), KPC(src_sstable_param));
  }

  return ret;
}

//TODO(muwei.ym) check it after shared mini/minor with cangdi
// ObCopiedSharedMacroBlocksSSTableCreator
int ObCopiedSharedMacroBlocksSSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  ObTableHandleV2 table_handle;
  ObArray<MacroBlockId> macro_block_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(get_shared_macro_id_list_(macro_block_id_array))) {
        LOG_WARN("failed to get shared ddl sstable macro id list", K(ret));
      } else if (OB_FAIL(sstable_index_builder_->close(res))) {
        LOG_WARN("failed to close sstable index builder", K(ret));
      } else if (OB_FAIL(res.other_block_ids_.assign(macro_block_id_array))) {
        LOG_WARN("failed to assign other block id", K(ret), K(macro_block_id_array));
      } else if (OB_FAIL(init_create_sstable_param_(res, param))) {
        LOG_WARN("fail to init create sstable param", K(ret), K(res));
      } else if (OB_FAIL(do_create_sstable_(param, table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(param));
      } else if (OB_FAIL(finish_task_->add_sstable(table_handle))) {
        LOG_WARN("fail to add sstable", K(ret), K(table_handle));
      }
    }
  }

  LOG_INFO("create shared macro blocks sstable", K(ret), K(table_handle));

  return ret;
}

int ObCopiedSharedMacroBlocksSSTableCreator::check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const
{
  int ret = OB_SUCCESS;
  if (!src_sstable_param->is_shared_macro_blocks_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is not shared macro blocks", K(ret), KPC(src_sstable_param));
  } else if (src_sstable_param->is_shared_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is shared", K(ret), KPC(src_sstable_param));
  } else if (!src_sstable_param->table_key_.is_ddl_dump_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is not ddl dump", K(ret), KPC(src_sstable_param));
  }

  return ret;
}


int ObCopiedSharedMacroBlocksSSTableCreator::get_shared_macro_id_list_(common::ObIArray<MacroBlockId> &macro_block_id_array)
{
  int ret = OB_SUCCESS;
  const common::ObArray<ObCopyMacroRangeIdInfo> *macro_range_array = NULL;

  macro_block_id_array.reset();
  if (OB_FAIL(finish_task_->get_copy_macro_range_array(macro_range_array))) {
    LOG_WARN("failed to get macro range array", K(ret));
  } else if (OB_ISNULL(macro_range_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_range_array is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_range_array->count(); ++i) {
      const ObCopyMacroRangeInfo &macro_range_info = macro_range_array->at(i).range_info_;
      if (OB_FAIL(ObStorageHAUtils::extract_macro_id_from_datum(macro_range_info.start_macro_block_end_key_, macro_block_id_array))) {
        LOG_WARN("failed to extract macro id from datum", K(ret), K(macro_range_info));
      }
    }
  }

  return ret;
}
#endif

// ObSSTableCopyFinishTask
ObSSTableCopyFinishTask::ObSSTableCopyFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL),
    is_inited_(false),
    next_copy_task_id_(0),
    copy_ctx_(),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    sstable_param_(nullptr),
    sstable_macro_range_info_(),
    macro_range_info_index_(0),
    tablet_copy_finish_task_(nullptr),
    ls_(nullptr),
    tablet_service_(nullptr),
    sstable_index_builder_(false /* not use writer buffer*/),
    restore_macro_block_id_mgr_(nullptr),
    macro_block_reuse_mgr_(),
    allocator_(ObMemAttr(MTL_ID(), "SSTCopyFinish")),
    split_src_sstable_handle_()
{
}

ObSSTableCopyFinishTask::~ObSSTableCopyFinishTask()
{
  if (OB_NOT_NULL(restore_macro_block_id_mgr_)) {
    ob_delete(restore_macro_block_id_mgr_);
  }
}

int ObSSTableCopyFinishTask::init(const ObPhysicalCopyTaskInitParam &init_param)
{
  int ret = OB_SUCCESS;
  common::ObInOutBandwidthThrottle *bandwidth_throttle = nullptr;
  ObLSService *ls_service = nullptr;
  ObStorageRpcProxy *svr_rpc_proxy = nullptr;
  ObStorageHADag *ha_dag = nullptr;
  const uint64_t tenant_id = MTL_ID();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable copy finish task init twice", K(ret));
  } else if (!init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical copy finish task init get invalid argument", K(ret), K(init_param));
  } else if (OB_ISNULL(ls_service = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (FALSE_IT(bandwidth_throttle = GCTX.bandwidth_throttle_)) {
  } else if (FALSE_IT(svr_rpc_proxy = ls_service->get_storage_rpc_proxy())) {
  } else if (FALSE_IT(ha_dag = static_cast<ObStorageHADag *>(this->get_dag()))) {
  } else if (OB_FAIL(sstable_macro_range_info_.assign(init_param.sstable_macro_range_info_))) {
    LOG_WARN("failed to assign sstable macro range info", K(ret), K(init_param));
  } else if (OB_FAIL(build_restore_macro_block_id_mgr_(init_param))) {
    LOG_WARN("failed to build restore macro block id mgr", K(ret), K(init_param));
  } else if (OB_FAIL(macro_block_reuse_mgr_.init())) {
    LOG_WARN("failed to init macro block reuse mgr", K(ret));
  } else {
    copy_ctx_.tenant_id_ = init_param.tenant_id_;
    copy_ctx_.ls_id_ = init_param.ls_id_;
    copy_ctx_.tablet_id_ = init_param.tablet_id_;
    copy_ctx_.src_info_ = init_param.src_info_;
    copy_ctx_.bandwidth_throttle_ = bandwidth_throttle;
    copy_ctx_.svr_rpc_proxy_ = svr_rpc_proxy;
    copy_ctx_.is_leader_restore_ = init_param.is_leader_restore_;
    copy_ctx_.restore_action_ = init_param.restore_action_;
    copy_ctx_.restore_base_info_ = init_param.restore_base_info_;
    copy_ctx_.meta_index_store_ = init_param.meta_index_store_;
    copy_ctx_.second_meta_index_store_ = init_param.second_meta_index_store_;
    copy_ctx_.ha_dag_ = ha_dag;
    copy_ctx_.sstable_index_builder_ = &sstable_index_builder_;
    copy_ctx_.restore_macro_block_id_mgr_ = restore_macro_block_id_mgr_;
    copy_ctx_.need_sort_macro_meta_ = init_param.need_sort_macro_meta_;
    copy_ctx_.need_check_seq_ = init_param.need_check_seq_;
    copy_ctx_.ls_rebuild_seq_ = init_param.ls_rebuild_seq_;
    copy_ctx_.table_key_ = init_param.sstable_param_->table_key_;
    copy_ctx_.macro_block_reuse_mgr_ = &macro_block_reuse_mgr_;
    copy_ctx_.extra_info_ = init_param.extra_info_;
    macro_range_info_index_ = 0;
    ls_ = init_param.ls_;
    sstable_param_ = init_param.sstable_param_;
    tablet_copy_finish_task_ = init_param.tablet_copy_finish_task_;
    int64_t cluster_version = 0;
    if (OB_FAIL(get_cluster_version_(init_param, cluster_version))) {
      LOG_WARN("failed to get cluster version", K(ret));
    } else if (OB_FAIL(prepare_sstable_index_builder_(init_param.ls_id_,
        init_param.tablet_id_, init_param.sstable_param_, cluster_version))) {
      LOG_WARN("failed to prepare sstable index builder", K(ret), K(init_param), K(cluster_version));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init ObSSTableCopyFinishTask", K(init_param), K(sstable_macro_range_info_));
    }
  }

#ifdef ERRSIM
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
      ret = PHYSICAL_COPY_TASK_GET_TABLET_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake PHYSICAL_COPY_TASK_GET_TABLET_FAILED", K(ret));
      }
    }
#endif

  return ret;
}

int ObSSTableCopyFinishTask::get_next_macro_block_copy_info(
    ObITable::TableKey &copy_table_key,
    const ObCopyMacroRangeIdInfo *&copy_macro_range_info)
{
  int ret = OB_SUCCESS;
  copy_table_key.reset();
  copy_macro_range_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (macro_range_info_index_ >= sstable_macro_range_info_.copy_macro_range_array_.count()) {
      ret = OB_ITER_END;
    } else {
      copy_table_key = sstable_macro_range_info_.copy_table_key_;
      copy_macro_range_info = &sstable_macro_range_info_.copy_macro_range_array_.at(macro_range_info_index_);
      macro_range_info_index_++;
      LOG_INFO("succeed get macro block copy info", K(copy_table_key), KPC(copy_macro_range_info),
          K(macro_range_info_index_), K(sstable_macro_range_info_));
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::check_is_iter_end(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (macro_range_info_index_ >= sstable_macro_range_info_.copy_macro_range_array_.count()) {
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::add_sstable(ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(tablet_copy_finish_task_->add_sstable(table_handle))) {
    LOG_WARN("failed to add sstable", K(ret), K(table_handle));
  }

  return ret;
}

int ObSSTableCopyFinishTask::add_sstable(ObTableHandleV2 &table_handle, const int64_t last_meta_macro_seq)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(tablet_copy_finish_task_->add_sstable(table_handle, last_meta_macro_seq))) {
    LOG_WARN("failed to add sstable", K(ret), K(table_handle), K(last_meta_macro_seq));
  }

  return ret;
}

int ObSSTableCopyFinishTask::get_copy_macro_range_array(const common::ObArray<ObCopyMacroRangeIdInfo> *&macro_range_array) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    macro_range_array = &sstable_macro_range_info_.copy_macro_range_array_;
  }
  return ret;
}

int64_t ObSSTableCopyFinishTask::get_next_copy_task_id()
{
  return ATOMIC_FAA(&next_copy_task_id_, 1);
}

int64_t ObSSTableCopyFinishTask::get_max_next_copy_task_id()
{
  return ATOMIC_LOAD(&next_copy_task_id_);
}

int ObSSTableCopyFinishTask::add_logic_macro_info_for_range(const ObIArray<ObLogicMacroBlockId> &logic_ids)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    ObArray<ObCopyMacroRangeIdInfo> &range_array = sstable_macro_range_info_.copy_macro_range_array_;
    int64_t macro_idx = 0;
    // for each range
    for (int64_t range_idx = 0; OB_SUCC(ret) && range_idx < range_array.count(); ++range_idx) {
      ObCopyMacroRangeIdInfo &range_id_info = range_array.at(range_idx);
      const ObCopyMacroRangeInfo &range_info = range_id_info.range_info_;
      if (!range_id_info.macro_block_ids_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro block ids should be empty", K(ret), K(range_id_info));
      } else {
        // add logic macro block ids to corresponding range
        for (int64_t idx = 0; OB_SUCC(ret) && idx < range_info.macro_block_count_; ++idx) {
          ObLogicMacroBlockId logic_id;
          if (macro_idx >= logic_ids.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("macro block ids count is not match", K(ret), K(macro_idx), K(logic_ids.count()));
          } else if (FALSE_IT(logic_id = logic_ids.at(macro_idx))) {
          } else if (OB_FAIL(range_id_info.macro_block_ids_.push_back(logic_id))) {
            LOG_WARN("failed to push back logic id", K(ret), K(logic_id));
          } else {
            ++macro_idx;
          }
        }

        if (OB_SUCC(ret)) {
          // validate macro block ids (count and correctness)
          if (range_id_info.macro_block_ids_.count() != range_info.macro_block_count_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("macro block ids count is not match", K(ret), K(range_id_info));
          } else if (range_id_info.macro_block_ids_.at(0) != range_info.start_macro_block_id_
                  || range_id_info.macro_block_ids_.at(range_id_info.macro_block_ids_.count() - 1) != range_info.end_macro_block_id_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("macro block ids is not match", K(ret), K(range_info));
          }
        }
      }
    }
  }

  return ret;
}

int ObSSTableCopyFinishTask::build_sstable_reuse_info()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(build_latest_major_sstable_reuse_info_())) {
    LOG_WARN("failed to build latest major sstable reuse info", K(ret), K(copy_ctx_));
  } else if (OB_FAIL(build_split_src_sstable_reuse_info_())) {
    LOG_WARN("failed to build split src sstable reuse info", K(ret), K(copy_ctx_));
  } else {
    LOG_INFO("succeed build sstable reuse info", K(copy_ctx_));
  }

  return ret;
}

int ObSSTableCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  int tmp_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (copy_ctx_.ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip physical copy finish task", K(copy_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task_->get_tablet_status(status))) {
    LOG_WARN("failed to get tablet status", K(ret), K(copy_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    //do nothing
  } else if (OB_FAIL(create_sstable_())) {
    LOG_WARN("failed to create sstable", K(ret), K(copy_ctx_));
  } else if (OB_FAIL(check_sstable_valid_())) {
    LOG_WARN("failed to check sstable valid", K(ret), K(copy_ctx_));
  } else if (OB_FAIL(update_copy_tablet_record_extra_info_())) {
    LOG_WARN("failed to update copy tablet record extra info", K(ret), K(copy_ctx_));
  } else {
    LOG_INFO("succeed sstable copy finish", K(copy_ctx_));
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    if (GCONF.errsim_migration_tablet_id == copy_ctx_.tablet_id_.id()
      && ObITable::is_major_sstable(copy_ctx_.table_key_.table_type_)
    ) {
      // inject error when finish copying 3rd major sstable
      if (3 == copy_ctx_.extra_info_->get_major_count()) {
        ret = SSTABLE_COPY_FINISH_TASK_FAILED ? : OB_SUCCESS;
        LOG_ERROR("fake SSTABLE_COPY_FINISH_TASK_FAILED", K(ret), K(copy_ctx_));
      }
    }
  }
#endif

  if (OB_FAIL(ret)) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      status = ObCopyTabletStatus::TABLET_NOT_EXIST;
      if (OB_FAIL(tablet_copy_finish_task_->set_tablet_status(status))) {
        LOG_WARN("failed to set copy tablet status", K(ret), K(copy_ctx_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, copy_ctx_.ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(copy_ctx_));
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::update_copy_tablet_record_extra_info_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(copy_ctx_.extra_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy ctx extra info is NULL", K(ret), K(copy_ctx_));
  } else {
    copy_ctx_.extra_info_->add_macro_count(copy_ctx_.total_macro_count_);
    copy_ctx_.extra_info_->add_reuse_macro_count(copy_ctx_.reuse_macro_count_);

    if (!ObITable::is_major_sstable(copy_ctx_.table_key_.table_type_)) {
      // skip
    } else if (OB_FAIL(copy_ctx_.extra_info_->update_max_reuse_mgr_size(&macro_block_reuse_mgr_))) {
      LOG_WARN("failed to update max reuse mgr size", K(ret), K(copy_ctx_));
    } else {
      copy_ctx_.extra_info_->inc_major_count();
      copy_ctx_.extra_info_->add_major_macro_count(copy_ctx_.total_macro_count_);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeed update copy tablet record extra info", K(copy_ctx_));
  }

  return ret;
}

int ObSSTableCopyFinishTask::prepare_data_store_desc_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMigrationSSTableParam *sstable_param,
    const int64_t cluster_version,
    ObWholeDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObMergeType merge_type;
  const ObMigrationTabletParam *src_tablet_param = nullptr;
  const ObStorageSchema *storage_schema = nullptr;
  ObTabletHandle tablet_handle;

  if (OB_UNLIKELY(!tablet_id.is_valid()
                  || cluster_version < 0
                  || NULL == sstable_param
                  || NULL == tablet_copy_finish_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare sstable index builder get invalid argument", K(ret), K(tablet_id), K(cluster_version), KP(sstable_param));
  } else if (FALSE_IT(src_tablet_param = tablet_copy_finish_task_->get_src_tablet_meta())) {
  } else if (OB_UNLIKELY(NULL == src_tablet_param || !src_tablet_param->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected invalid tablet param", K(ret), K(ls_id), K(tablet_id), K(sstable_param), KPC(src_tablet_param));
  } else if (FALSE_IT(storage_schema = &src_tablet_param->storage_schema_)) {
  } else if (sstable_param->table_key_.is_mds_sstable()
      && FALSE_IT(storage_schema = ObMdsSchemaHelper::get_instance().get_storage_schema())) {
  } else if (OB_FAIL(get_merge_type_(sstable_param, merge_type))) {
    LOG_WARN("failed to get merge type", K(ret), KPC(sstable_param));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to do ha get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id));
  } else {
    const uint16_t cg_idx = sstable_param->table_key_.get_column_group_id();
    const ObStorageColumnGroupSchema *cg_schema = nullptr;
    if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected cg idx", K(ret), K(cg_idx), KPC(storage_schema));
    } else {
      cg_schema = &storage_schema->get_column_groups().at(cg_idx);
      if (OB_ISNULL(cg_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get cg schema", K(ret), KPC(storage_schema), K(cg_idx));
      }
    }
    int32_t private_transfer_epoch = -1;
    if (FAILEDx(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get private transfer epoch", K(ret), "tablet_meta", tablet->get_tablet_meta());
    } else if (OB_FAIL(desc.init(
        false/*is ddl*/,
        *storage_schema,
        ls_id,
        tablet_id,
        merge_type,
        tablet->get_snapshot_version(),
        0/*cluster_version*/,
        tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_,
        private_transfer_epoch,
        0/*concurrent_cnt*/,
        tablet->get_reorganization_scn(),
        sstable_param->table_key_.get_end_scn(),
        cg_schema,
        cg_idx,
        sstable_param->basic_meta_.table_shared_flag_.is_shared_sstable()
          ? compaction::ObExecMode::EXEC_MODE_OUTPUT
          : compaction::ObExecMode::EXEC_MODE_LOCAL))) {
      LOG_WARN("failed to init index store desc for column store table", K(ret), K(cg_idx), KPC(sstable_param), K(cg_schema));
    } else {
      /* Since the storage_schema of migration maybe newer or older than the original sstable,
        we always use the col_cnt in sstable_param to re-generate sstable for dst.
        Besides, we fill default chksum array with zeros since there's no need to recalculate*/
      int64_t column_cnt = sstable_param->basic_meta_.column_cnt_;
      if (OB_FAIL(desc.get_col_desc().mock_valid_col_default_checksum_array(column_cnt))) {
        LOG_WARN("fail to mock valid col default checksum array", K(ret));
      } else if (OB_FAIL(desc.get_desc().update_basic_info_from_macro_meta(sstable_param->basic_meta_))) {
        LOG_WARN("failed to update basic info from macro meta", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_cluster_version_(
    const ObPhysicalCopyTaskInitParam &init_param,
    int64_t &cluster_version)
{
  int ret = OB_SUCCESS;
  if (!init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(init_param));
  } else {
    // if restore use backup cluster version
    if (init_param.is_leader_restore_) {
      if (OB_ISNULL(init_param.restore_base_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("restore base info is null", K(ret), K(init_param));
      } else {
        cluster_version = init_param.restore_base_info_->backup_cluster_version_;
      }
    } else {
      cluster_version = static_cast<int64_t>(GET_MIN_CLUSTER_VERSION());
    }
  }
  return ret;
}

bool ObSSTableCopyFinishTask::is_sstable_should_rebuild_index_(const ObMigrationSSTableParam *sstable_param) const
{
  // Non-empty SSTable whose macro blocks should be copied needs rebuild index after
  // macros are copied.
  return !sstable_param->is_empty_sstable()
         && !is_shared_sstable_without_copy_(sstable_param);
}

bool ObSSTableCopyFinishTask::is_shared_sstable_without_copy_(const ObMigrationSSTableParam *sstable_param) const
{
  // Shared SSTable is the SSTable whose macro blocks, including data and index blocks,
  // are all in shared storage or backup storage. During migration or follower restore,
  // macro blocks are no need to be copied.
  return !copy_ctx_.is_leader_restore_ && sstable_param->is_shared_sstable();
}

int ObSSTableCopyFinishTask::prepare_sstable_index_builder_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMigrationSSTableParam *sstable_param,
    const int64_t cluster_version)
{
  int ret = OB_SUCCESS;

  if (!tablet_id.is_valid() || OB_ISNULL(sstable_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare sstable index builder get invalid argument", K(ret), K(tablet_id), KP(sstable_param));
  } else {
    ObWholeDataStoreDesc desc;
    ObSSTableIndexBuilder::ObSpaceOptimizationMode mode = ObSSTableIndexBuilder::DISABLE;

    if (!is_sstable_should_rebuild_index_(sstable_param)) {
      LOG_INFO("sstable is no need build sstable index builder", K(tablet_id), KPC(sstable_param));
    } else if (OB_FAIL(get_space_optimization_mode_(sstable_param, mode))) {
      LOG_WARN("failed to get space optimization mode", K(ret), K(tablet_id), KPC(sstable_param));
    } else if (OB_FAIL(prepare_data_store_desc_(ls_id, tablet_id, sstable_param, cluster_version, desc))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        //overwrite ret
        if (OB_FAIL(tablet_copy_finish_task_->set_tablet_status(ObCopyTabletStatus::TABLET_NOT_EXIST))) {
          LOG_WARN("failed to set tablet status", K(ret), K(tablet_id));
        }
      } else {
        LOG_WARN("failed to prepare data store desc", K(ret), K(tablet_id), K(cluster_version));
      }
    } else if (OB_FAIL(sstable_index_builder_.init(desc.get_desc(), mode))) {
      LOG_WARN("failed to init sstable index builder", K(ret), K(desc), K(mode));
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_merge_type_(
    const ObMigrationSSTableParam *sstable_param,
    ObMergeType &merge_type)
{
  int ret = OB_SUCCESS;
  merge_type = ObMergeType::INVALID_MERGE_TYPE;

  if (OB_ISNULL(sstable_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable should not be NULL", K(ret), KP(sstable_param));
  } else if (sstable_param->table_key_.is_major_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (sstable_param->table_key_.is_minor_sstable()) {
    merge_type = ObMergeType::MINOR_MERGE;
  } else if (sstable_param->table_key_.is_inc_major_type_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE; // inc major and major has the same format, we should use major merge to init data store desc and index builder
  } else if (sstable_param->table_key_.is_ddl_dump_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (sstable_param->table_key_.is_inc_major_ddl_dump_sstable()) {
    merge_type = ObMergeType::MAJOR_MERGE;
  } else if (sstable_param->table_key_.is_mds_sstable()) {
    merge_type = ObMergeType::MDS_MINI_MERGE;
  }
  else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable type is unexpected", K(ret), KPC(sstable_param));
  }
  return ret;
}

int ObSSTableCopyFinishTask::create_sstable_()
{
  int ret = OB_SUCCESS;
  ObCopiedSSTableCreatorImpl *sstable_creator = nullptr;
  if (OB_FAIL(alloc_and_init_sstable_creator_(sstable_creator))) {
    LOG_WARN("failed to get sstable creator", K(ret));
  } else if (OB_FAIL(sstable_creator->create_sstable())) {
    LOG_WARN("failed to create sstable", K(ret));
  }

  if (OB_NOT_NULL(sstable_creator)) {
    free_sstable_creator_(sstable_creator);
  }
  return ret;
}

int ObSSTableCopyFinishTask::build_restore_macro_block_id_mgr_(
    const ObPhysicalCopyTaskInitParam &init_param)
{
  int ret = OB_SUCCESS;
  ObRestoreMacroBlockIdMgr *restore_macro_block_id_mgr = nullptr;

  if (!init_param.is_leader_restore_) {
    restore_macro_block_id_mgr_ = nullptr;
  } else if (ObTabletRestoreAction::is_restore_remote_sstable(init_param.restore_action_)) {
    // restore index/meta tree for backup sstable, macro blocks should be got by iterator.
    restore_macro_block_id_mgr_ = nullptr;
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(init_param.restore_action_)) {
    restore_macro_block_id_mgr_ = nullptr;
  } else {
    void *buf = mtl_malloc(sizeof(ObRestoreMacroBlockIdMgr), "RestoreMacIdMgr");
    ObTabletHandle tablet_handle;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(restore_macro_block_id_mgr = new(buf) ObRestoreMacroBlockIdMgr())) {
    } else if (OB_FAIL(init_param.ls_->ha_get_tablet(init_param.tablet_id_, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(init_param));
    } else if (OB_FAIL(restore_macro_block_id_mgr->init(init_param.tablet_id_,
                                                        tablet_handle,
                                                        init_param.sstable_macro_range_info_.copy_table_key_,
                                                        *init_param.restore_base_info_,
                                                        *init_param.meta_index_store_,
                                                        *init_param.second_meta_index_store_))) {
      LOG_WARN("failed to init restore macro block id mgr", K(ret), K(init_param));
    } else {
      restore_macro_block_id_mgr_ = restore_macro_block_id_mgr;
      restore_macro_block_id_mgr = NULL;
    }

    if (OB_FAIL(ret)) {
      if (NULL != restore_macro_block_id_mgr_) {
        restore_macro_block_id_mgr_->~ObRestoreMacroBlockIdMgr();
        mtl_free(restore_macro_block_id_mgr_);
        restore_macro_block_id_mgr_ = nullptr;
      }
    }
    if (NULL != restore_macro_block_id_mgr) {
      restore_macro_block_id_mgr->~ObRestoreMacroBlockIdMgr();
      mtl_free(restore_macro_block_id_mgr);
      restore_macro_block_id_mgr = nullptr;
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::check_sstable_valid_()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObSSTableMetaHandle sst_meta_hdl;
  ObSSTable *sstable = nullptr;
  ObITable::TableKey table_key = sstable_param_->table_key_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(tablet_copy_finish_task_->get_sstable(table_key, table_handle))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be NULL", K(ret), KP(sstable), KPC(sstable_param_));
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (OB_FAIL(check_sstable_meta_(*sstable_param_, sst_meta_hdl.get_sstable_meta()))) {
    LOG_WARN("failed to check sstable meta", K(ret), K(table_key), KPC(sstable_param_),
      K(sst_meta_hdl.get_sstable_meta()));
  }
  return ret;
}

int ObSSTableCopyFinishTask::check_sstable_meta_(
    const ObMigrationSSTableParam &src_meta,
    const ObSSTableMeta &write_meta)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (!src_meta.is_valid() || !write_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check sstable meta get invalid argument", K(ret), K(src_meta), K(write_meta));
  } else if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(src_meta, write_meta))) {
    LOG_WARN("failed to check sstable meta", K(ret), K(src_meta), K(write_meta));
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_tablet_finish_task(ObTabletCopyFinishTask *&finish_task)
{
  int ret = OB_SUCCESS;
  finish_task = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    finish_task = tablet_copy_finish_task_;
  }
  return ret;
}

int ObSSTableCopyFinishTask::alloc_and_init_sstable_creator_(ObCopiedSSTableCreatorImpl *&sstable_creator)
{
  int ret = OB_SUCCESS;
  ObCopiedSSTableCreatorImpl *tmp_creator = nullptr;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (sstable_param_->is_empty_sstable()) {
    tmp_creator = MTL_NEW(ObCopiedEmptySSTableCreator, "CopySSTCreator");
  } else if (!is_shared_storage) {
    if (sstable_param_->is_shared_sstable() && is_shared_sstable_without_copy_(sstable_param_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported to create shared sstable", K(ret));
    } else {
      tmp_creator = MTL_NEW(ObCopiedSSTableCreator, "CopySSTCreator");
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (sstable_param_->is_shared_sstable()) {
      if (is_shared_sstable_without_copy_(sstable_param_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported to create shared sstable", K(ret));
      } else {
        tmp_creator = MTL_NEW(ObRestoredSharedSSTableCreator, "CopySSTCreator");
      }
    } else if (sstable_param_->is_shared_macro_blocks_sstable()) {
      tmp_creator = MTL_NEW(ObCopiedSharedMacroBlocksSSTableCreator, "CopySSTCreator");
    } else {
      tmp_creator = MTL_NEW(ObCopiedSSTableCreator, "CopySSTCreator");
    }
#endif
  }

  if (OB_ISNULL(tmp_creator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_creator->init(copy_ctx_.ls_id_,
                                       copy_ctx_.tablet_id_,
                                       sstable_param_,
                                       &sstable_index_builder_,
                                       &tablet_copy_finish_task_->get_allocator(),
                                       this))) {
    LOG_WARN("failed to init sstable creator", K(ret));
  } else {
    sstable_creator = tmp_creator;
    tmp_creator = nullptr;
  }

  if (OB_NOT_NULL(tmp_creator)) {
    free_sstable_creator_(tmp_creator);
  }

  return ret;
}

void ObSSTableCopyFinishTask::free_sstable_creator_(ObCopiedSSTableCreatorImpl *&sstable_creator)
{
  MTL_DELETE(ObCopiedSSTableCreatorImpl, "CopySSTCreator", sstable_creator);
}

int ObSSTableCopyFinishTask::get_space_optimization_mode_(
    const ObMigrationSSTableParam *sstable_param,
    ObSSTableIndexBuilder::ObSpaceOptimizationMode &mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sstable_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable_param is null", K(ret));
  } else if (sstable_param->table_key_.is_ddl_sstable() || sstable_param->table_key_.is_inc_major_ddl_sstable()) {
    mode = ObSSTableIndexBuilder::DISABLE;
  } else if (ObTabletRestoreAction::is_restore_remote_sstable(copy_ctx_.restore_action_)) {
    mode = ObSSTableIndexBuilder::DISABLE;
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(copy_ctx_.restore_action_)) {
    mode = ObSSTableIndexBuilder::ENABLE;
  } else if (sstable_param->is_small_sstable_) {
    mode = ObSSTableIndexBuilder::ENABLE;
  } else {
    mode = ObSSTableIndexBuilder::DISABLE;
  }

  return ret;
}

int ObSSTableCopyFinishTask::build_latest_major_sstable_reuse_info_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTableHandleV2 table_handle;
  bool is_exist = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableWrapper sstable_wrapper;
  ObTabletMapKey map_key;
  map_key.ls_id_ = copy_ctx_.ls_id_;
  map_key.tablet_id_ = copy_ctx_.tablet_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet_without_memtables(
      WashTabletPriority::WTP_LOW, map_key, allocator_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(copy_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task_->get_latest_major_sstable(sstable_param_->table_key_, table_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_exist = false;
      LOG_INFO("no major sstable exist, try get major from local", K(ret), KPC(sstable_param_));
    } else {
      LOG_WARN("failed to get lastest major sstable", K(ret), KPC(sstable_param_));
    }
  } else {
    is_exist = true;
  }

  // if didn't copy any major sstable, try find latest major sstable from local
  if (OB_SUCC(ret)) {
    if (!is_exist) {
      ObTablet *tablet = nullptr;
      if (!tablet_handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet handle should not be valid", K(ret), K(tablet_handle));
      } else if (FALSE_IT(tablet = tablet_handle.get_obj())) {
      } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
        LOG_WARN("failed to fetch table store", K(ret), KPC(tablet));
      } else if (OB_FAIL(get_lastest_major_sstable_for_reuse_(copy_ctx_.table_key_, table_store_wrapper, sstable_wrapper))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          LOG_INFO("no major sstable exist in local", K(ret), KPC(sstable_param_));
          ret = OB_SUCCESS;
          is_exist = false;
        } else {
          LOG_WARN("failed to get local lastest major sstable", K(ret), KPC(sstable_param_));
        }
      } else if (!sstable_wrapper.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable wrapper is invalid", K(ret), K(sstable_wrapper), KPC(sstable_param_));
      } else if (OB_FAIL(table_handle.set_sstable_with_tablet(sstable_wrapper.get_sstable()))) {
        // this sstable lifetime is managed by current tablet (tablet_handle is hold by ObTabletFinishCopyTask)
        LOG_WARN("failed to set sstable with tablet", K(ret), K(sstable_wrapper), KPC(sstable_param_));
      } else {
        is_exist = true;
      }
    } else {
      // do nothing
      LOG_INFO("has get latest major sstable from copy finish task, skip get latest major sstable from local",
                K(is_exist), K(copy_ctx_.table_key_));
    }
  }

  if (OB_SUCC(ret) && is_exist) {
    if (!macro_block_reuse_mgr_.is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block reuse mgr do not init", K(ret));
    } else if (OB_FAIL(build_major_sstable_reuse_info_(table_handle, tablet_handle))) {
      LOG_WARN("failed to build major sstable reuse info", K(ret), KPC(sstable_param_));
    } else {
      LOG_INFO("succeed build major sstable reuse info", K(table_handle), K(tablet_handle), KPC(sstable_param_));
    }
  }

  return ret;
}


int ObSSTableCopyFinishTask::build_split_src_sstable_reuse_info_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletHandle split_src_tablet_handle;
  ObTablet *tablet = nullptr;
  ObTablet *split_src_tablet = nullptr;
  ObTabletID src_tablet_id;
  ObITable *sstable = nullptr;
  ObITable::TableKey src_table_key;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableWrapper sstable_wrapper;
  ObTabletMapKey map_key;
  ObTabletMapKey split_src_map_key;
  map_key.ls_id_ = copy_ctx_.ls_id_;
  map_key.tablet_id_ = copy_ctx_.tablet_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (copy_ctx_.is_leader_restore_) {
    // skip, no need to build reuse info for restore
    LOG_INFO("skip build split src sstable reuse info for restore");
  } else if (OB_FAIL(ls_->ha_get_tablet_without_memtables(
      WashTabletPriority::WTP_LOW, map_key, allocator_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(copy_ctx_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle));
  } else if (!tablet->get_tablet_meta().split_info_.can_reuse_macro_block()) {
    // skip, is not reuse macro block split
    LOG_INFO("not reuse macro block split, skip build split src sstable reuse info");
  } else if (FALSE_IT(src_tablet_id = tablet->get_tablet_meta().split_info_.get_split_src_tablet_id())) {
  } else if (!src_tablet_id.is_valid()) {
    ret = OB_SUCCESS;
    LOG_INFO("split src tablet id is invalid, skip reuse", K(src_tablet_id));
  } else if (FALSE_IT(split_src_map_key.ls_id_ = copy_ctx_.ls_id_)) {
  } else if (FALSE_IT(split_src_map_key.tablet_id_ = src_tablet_id)) {
  } else if (OB_FAIL(ls_->ha_get_tablet_without_memtables(
      WashTabletPriority::WTP_LOW, split_src_map_key, allocator_, split_src_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("split src tablet is not exist, maybe GC, skip build reuse info", K(ret), K(copy_ctx_.ls_id_), K(src_tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get split src tablet", K(ret), K(src_tablet_id));
    }
  } else if (FALSE_IT(split_src_tablet = split_src_tablet_handle.get_obj())) {
  } else if (OB_ISNULL(split_src_tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("split src tablet should not be NULL", K(ret), K(src_tablet_id), K(split_src_tablet));
  } else if (OB_FAIL(split_src_tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("failed to fetch table store", K(ret), KPC(split_src_tablet));
  } else if (FALSE_IT(src_table_key = sstable_param_->table_key_)) {
  } else if (FALSE_IT(src_table_key.tablet_id_ = src_tablet_id)) { // modify to split src table key
  } else if (OB_FAIL(get_target_major_sstable_for_reuse_(src_table_key, table_store_wrapper, sstable_wrapper))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("split src sstable is not exist", K(ret), KPC(sstable_param_));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get local lastest major sstable", K(ret), KPC(sstable_param_));
    }
  } else if (!sstable_wrapper.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable wrapper is invalid", K(ret), K(sstable_wrapper), KPC(sstable_param_));
  } else if (OB_ISNULL(sstable = sstable_wrapper.get_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be NULL", K(ret), KP(sstable), KPC(sstable_param_));
  } else if (OB_FAIL(split_src_sstable_handle_.set_sstable(sstable, &allocator_))) {
    // hold this sstable until copy task finish
    LOG_WARN("failed to set sstable with tablet", K(ret), KPC(sstable_param_));
  } else if (OB_FAIL(build_major_sstable_reuse_info_(split_src_sstable_handle_, split_src_tablet_handle))) {
    LOG_WARN("failed to build major sstable reuse info", K(ret), KPC(sstable_param_));
  } else {
    LOG_INFO("succeed build major sstable reuse info", K(split_src_tablet_handle), K(split_src_sstable_handle_), KPC(sstable_param_));
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_lastest_major_sstable_for_reuse_(
      const ObITable::TableKey &table_key,
      const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
      ObSSTableWrapper &latest_major_sstable)
{
  // 1. get local max major sstable snapshot version (and related sstable)
  // 2. iterate these major sstabels' macro blocks (if not co, there is only one major sstable)
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  latest_major_sstable.reset();

  if (!table_store_wrapper.is_valid() || !table_key.is_valid() || !ObITable::is_major_sstable(table_key.table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_store_wrapper), KPC(tablet), K(table_key));
  } else {
    const ObSSTableArray &major_sstables = table_store_wrapper.get_member()->get_major_sstables();
    common::ObArray<ObSSTableWrapper> major_sstable_wrappers;

    if (major_sstables.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("no major sstable, skip", K(ret), K(table_store_wrapper), K(major_sstables), KPC(tablet));
    } else if (OB_FAIL(major_sstables.get_all_table_wrappers(major_sstable_wrappers))) {
      LOG_WARN("failed to get all major sstable wrappers", K(ret), K(major_sstables));
    } else {
      ObSSTableWrapper major_sstable;
      if (OB_FAIL(get_latest_available_major_(major_sstable_wrappers, major_sstable))) {
        // get_major_sstables return major sstables ordered by snapshot version in ascending order
        // get latest major sstable from target tablet's table store
        LOG_WARN("failed to get latest available major sstable", K(ret), K(table_store_wrapper), KPC(tablet));
      } else if (!major_sstable.is_valid()) {
        // skip, first major sstable has backup data, no need to build reuse info
        ret = OB_ENTRY_NOT_EXIST;
        LOG_INFO("first major sstable has backup data, no need to build reuse info", K(ret), K(table_store_wrapper), KPC(tablet));
      } else if (OB_FAIL(get_latest_major_sstable_(table_key, major_sstable, latest_major_sstable))) {
        // get the latest major sstable (if cg, will get the latest cg sstable)
        LOG_WARN("failed to get latest major sstable", K(ret), K(table_key), K(major_sstable));
      } else {
        LOG_INFO("succeed to get local lastest major sstable for reuse", K(ret), K(table_key), K(latest_major_sstable));
      }
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_target_major_sstable_for_reuse_(
  const ObITable::TableKey &table_key,
  const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
  ObSSTableWrapper &target_sstable)
{
  int ret = OB_SUCCESS;
  int64_t major_cnt = 0;

  if (!table_store_wrapper.is_valid() || !table_key.is_valid() || !ObITable::is_major_sstable(table_key.table_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table store wrapper is invalid", K(ret), K(table_key), K(table_store_wrapper));
  } else if (FALSE_IT(major_cnt = table_store_wrapper.get_member()->get_major_sstables().count())) {
  } else if (0 == major_cnt) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("no major sstable, skip", K(ret), K(table_store_wrapper));
  } else {
    common::ObArray<ObSSTable *> major_sstables;
    ObSSTableMetaHandle sst_meta_hdl;
    ObSSTable *sstable;
    if (OB_FAIL(table_store_wrapper.get_member()->get_major_sstables().get_table(table_key, target_sstable))) {
      // get the target major sstable (if cg, will get the target cg sstable) from src tablet table store
      LOG_WARN("failed to get target major sstable wrapper", K(ret), K(table_key));
    } else if (!target_sstable.is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_INFO("no target major sstable, skip", K(ret), K(table_key));
    } else if (OB_ISNULL(sstable = target_sstable.get_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is null", K(ret), KP(sstable));
    } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("failed to get sstable meta", K(ret), KPC(sstable));
    } else if (sst_meta_hdl.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("target major sstable has backup data, skip to build reuse info", K(ret), KPC(sstable));
    } else {
      LOG_INFO("succeed to get target major sstable for reuse", K(ret), K(table_key), K(target_sstable));
    }
  }

  return ret;
}

int ObSSTableCopyFinishTask::get_latest_available_major_(const ObIArray<ObSSTableWrapper> &major_sstables, ObSSTableWrapper &latest_major)
{
  int ret = OB_SUCCESS;
  latest_major.reset();

  // major sstables must be sorted by snapshot version in ascending order
  // get the latest major sstable that has no backup data and all previous major sstables have backup data
  for (int64_t i = 0; OB_SUCC(ret) && i < major_sstables.count(); ++i) {
    const ObSSTableWrapper &cur_major = major_sstables.at(i);
    ObSSTable *sstable = nullptr;
    ObSSTableMetaHandle sst_meta_hdl;

    if (!cur_major.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sstable wrapper", K(i), K(cur_major));
    } else if (FALSE_IT(sstable = cur_major.get_sstable())) {
    } else if (OB_ISNULL(sstable) || !ObITable::is_major_sstable(sstable->get_key().table_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid major sstable", K(ret), K(i), KPC(sstable));
    } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("failed to get sstable meta", K(ret), KPC(sstable));
    } else if (sst_meta_hdl.get_sstable_meta().get_basic_meta().table_backup_flag_.has_backup()) {
      // stop at the first major sstable that has backup data
      break;
    } else {
      latest_major = cur_major;
    }
  }

  return ret;
}

int ObSSTableCopyFinishTask::get_latest_major_sstable_(
    const ObITable::TableKey &target_table_key,
    const ObSSTableWrapper &sstable_wrapper,
    ObSSTableWrapper &target_major_sstable)
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = nullptr;

  if (!target_table_key.is_valid() || !sstable_wrapper.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("latest major sstable is invalid", K(ret), K(sstable));
  } else if (OB_ISNULL(sstable = sstable_wrapper.get_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable is null", K(ret), KP(sstable), K(sstable_wrapper));
  } else {
    ObITable::TableKey table_key = sstable->get_key();
    // table type of the local major sstable which has max snapshot version
    // could be normal major sstable (row store) or co sstable (column store)
    ObITable::TableType table_type = table_key.table_type_;
    ObITable::TableType target_table_type = target_table_key.table_type_;
    if (ObITable::MAJOR_SSTABLE != table_type && ObITable::COLUMN_ORIENTED_SSTABLE != table_type ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table type", K(ret), K(table_type));
    } else if (!ObITable::is_major_sstable(target_table_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid target table type", K(ret), K(target_table_type));
    } else if (ObITable::MAJOR_SSTABLE == target_table_type || ObITable::COLUMN_ORIENTED_SSTABLE == target_table_type) {
      // if target major sstable is row store major/co sstable, the local major sstable is the target sstable
      target_major_sstable = sstable_wrapper;
    } else if (ObITable::COLUMN_ORIENTED_SSTABLE != table_type) {
      // if the target major sstable is cg, the local sstable must be co sstable
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table type is not co", K(ret), K(table_type), K(target_table_key));
    } else {
      const ObCOSSTableV2 *co_sstable = static_cast<const ObCOSSTableV2 *>(sstable);
      if (OB_ISNULL(co_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("co sstable is null", K(ret), KPC(sstable), KP(co_sstable));
      } else if (OB_FAIL(co_sstable->fetch_cg_sstable(target_table_key.column_group_idx_, target_major_sstable))) {
        if (OB_STATE_NOT_MATCH == ret) {
          // overwrite ret
          ret = OB_ENTRY_NOT_EXIST;
          LOG_INFO("latest cg sstable is_cgs_empty_co, won't reuse macro block", K(ret), K(target_table_key));
        }
        LOG_WARN("failed to get cg table", K(ret), K(table_key));
      }
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::build_major_sstable_reuse_info_(
  const storage::ObTableHandleV2 &table_handle,
  const storage::ObTabletHandle &tablet_handle)
{
 int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObDatumRange datum_range;
  const storage::ObITableReadInfo *index_read_info = nullptr;
  const ObSSTable *sstable = nullptr;
  ObSSTableMetaHandle sst_meta_hdl;
  const ObSSTableMeta *sst_meta = nullptr;
  int64_t co_base_snapshot_version = 0;
  int64_t src_co_base_snapshot_version = 0;

  if (!tablet_handle.is_valid() || !table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_handle), K(table_handle));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else if (sstable->get_key().table_type_ != copy_ctx_.table_key_.table_type_
      || sstable->get_key().column_group_idx_ != copy_ctx_.table_key_.column_group_idx_) {
    // skip
    LOG_INFO("table key is not match, skip to build reuse info", K(ret),
      "build_table_key", sstable->get_key(),
      "copy_table_key", copy_ctx_.table_key_);
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta handler", K(ret), K(sstable));
  } else if (OB_FAIL(sst_meta_hdl.get_sstable_meta(sst_meta))) {
    LOG_WARN("failed to get sstable meta", K(ret), K(sst_meta_hdl));
  } else if (FALSE_IT(src_co_base_snapshot_version = sst_meta->get_basic_meta().get_co_base_snapshot_version())) {
  } else if (FALSE_IT(co_base_snapshot_version = sstable_param_->basic_meta_.get_co_base_snapshot_version())) {
  } else if (co_base_snapshot_version != src_co_base_snapshot_version) {
    // skip
    LOG_INFO("co base snapshot version is not equal, skip to build reuse info", K(ret), KPC(sstable),
      K(co_base_snapshot_version), K(src_co_base_snapshot_version));
  } else {
    SMART_VAR(ObSSTableSecMetaIterator, meta_iter) {
      if (FALSE_IT(datum_range.set_whole_range())) {
      } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(sstable, index_read_info))) {
        LOG_WARN("failed to get index read info ", KR(ret), K(sstable));
      } else if (OB_FAIL(meta_iter.open(datum_range,
                                        ObMacroBlockMetaType::DATA_BLOCK_META,
                                        *sstable,
                                        *index_read_info,
                                        allocator))) {
        LOG_WARN("failed to open sec meta iterator", K(ret));
      } else {
        ObDataMacroBlockMeta data_macro_block_meta;
        ObLogicMacroBlockId logic_id;
        MacroBlockId macro_id;

        while (OB_SUCC(ret)) {
          data_macro_block_meta.reset();
          logic_id.reset();
          if (OB_FAIL(meta_iter.get_next(data_macro_block_meta))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next", K(ret));
            }
          } else {
            logic_id = data_macro_block_meta.get_logic_id();
            macro_id = data_macro_block_meta.get_macro_id();
            int64_t data_checksum = data_macro_block_meta.get_meta_val().data_checksum_;

            if (OB_FAIL(macro_block_reuse_mgr_.add_macro_block_reuse_info(logic_id, macro_id, data_checksum))) {
              LOG_WARN("failed to insert reuse info into reuse map", K(ret), K(logic_id), K(macro_id), K(data_checksum));
            }
          }
        }
      }
    }
  }

  return ret;
}

}
}
