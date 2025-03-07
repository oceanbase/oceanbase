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

int ObCopiedSSTableCreatorImpl::init_create_sstable_param_(ObTabletCreateSSTableParam &param) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param.init_for_ha(*src_sstable_param_))) {
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

  LOG_INFO("create sstable", K(ret), KPC_(src_sstable_param), K(param));

  return ret;
}


// ObCopiedEmptySSTableCreator
int ObCopiedEmptySSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObTabletCreateSSTableParam param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopiedEmptySSTableCreator not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(init_create_sstable_param_(param))) {
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
  const common::ObArray<ObCopyMacroRangeInfo> *macro_range_array = NULL;

  macro_block_id_array.reset();
  if (OB_FAIL(finish_task_->get_copy_macro_range_array(macro_range_array))) {
    LOG_WARN("failed to get macro range array", K(ret));
  } else if (OB_ISNULL(macro_range_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro_range_array is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_range_array->count(); ++i) {
      const ObCopyMacroRangeInfo &macro_range_info = macro_range_array->at(i);
      if (OB_FAIL(ObStorageHAUtils::extract_macro_id_from_datum(macro_range_info.start_macro_block_end_key_, macro_block_id_array))) {
        LOG_WARN("failed to extract macro id from datum", K(ret), K(macro_range_info));
      }
    }
  }

  return ret;
}
#endif

// ObCopiedSharedSSTableCreator
int ObCopiedSharedSSTableCreator::create_sstable()
{
  int ret = OB_SUCCESS;
  ObSSTableMergeRes res;
  ObTableHandleV2 table_handle;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopiedSSTableCreator not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      if (OB_FAIL(init_create_sstable_param_(param))) {
        LOG_WARN("fail to init create sstable param", K(ret));
      } else if (OB_FAIL(do_create_sstable_(param, table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(param));
      } else if (OB_FAIL(finish_task_->add_sstable(table_handle))) {
        LOG_WARN("fail to add sstable", K(ret), K(table_handle));
      }
    }
  }
  LOG_INFO("create shared sstable with index builder", K(ret), K(table_handle));
  return ret;
}

int ObCopiedSharedSSTableCreator::check_sstable_param_for_init_(const ObMigrationSSTableParam *src_sstable_param) const
{
  int ret = OB_SUCCESS;
  if (!src_sstable_param->is_shared_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable is not shared", K(ret), KPC(src_sstable_param));
  }

  return ret;
}


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
    restore_macro_block_id_mgr_(nullptr)
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
    copy_ctx_.macro_block_reuse_mgr_ = init_param.macro_block_reuse_mgr_;
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
  return ret;
}

int ObSSTableCopyFinishTask::get_next_macro_block_copy_info(
    ObITable::TableKey &copy_table_key,
    const ObCopyMacroRangeInfo *&copy_macro_range_info)
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

int ObSSTableCopyFinishTask::get_copy_macro_range_array(const common::ObArray<ObCopyMacroRangeInfo> *&macro_range_array) const
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
  } else if (OB_FAIL(update_major_sstable_reuse_info_())) {
    LOG_WARN("failed to update major sstable reuse info", K(ret), K(copy_ctx_));
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


int ObSSTableCopyFinishTask::update_major_sstable_reuse_info_()
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;
  int64_t copy_snapshot_version = 0;
  int64_t reuse_info_count = 0;
  ObTabletHandle tablet_handle;
  ObTableHandleV2 table_handle;
  ObSSTable *sstable = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (!ObITable::is_major_sstable(copy_ctx_.table_key_.table_type_)) {
    // sstable is not major or is meta major, skip reuse
    LOG_INFO("sstable is not major, skip update major sstable reuse info", K(ret), K(copy_ctx_));
  } else if (OB_ISNULL(copy_ctx_.macro_block_reuse_mgr_)) {
    //do nothing
    LOG_WARN("macro block reuse mgr is null, skip update major sstbale reuse info", K(ret), KP(copy_ctx_.macro_block_reuse_mgr_), K(copy_ctx_));
  } else if (OB_FAIL(ls_->ha_get_tablet(copy_ctx_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(copy_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task_->get_sstable(sstable_param_->table_key_, table_handle))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be NULL", K(ret), KP(sstable), K(tablet_handle));
  } else {
    if (OB_FAIL(copy_ctx_.macro_block_reuse_mgr_->update_single_reuse_map(copy_ctx_.table_key_, tablet_handle, *sstable))) {
      LOG_WARN("failed to update single reuse map", K(ret), K(copy_ctx_));
    } else if (OB_FAIL(copy_ctx_.macro_block_reuse_mgr_->count(reuse_info_count))) {
      LOG_WARN("failed to count reuse info", K(ret), K(copy_ctx_));
    } else {
      LOG_INFO("succeed update major sstable reuse info", K(ret), K(copy_ctx_), K(reuse_info_count));
    }

    // if build or count reuse map failed, reset reuse map
    if (OB_FAIL(ret)) {
      copy_ctx_.macro_block_reuse_mgr_->reset();
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
    } else if (OB_NOT_NULL(copy_ctx_.macro_block_reuse_mgr_) &&
         OB_FAIL(copy_ctx_.extra_info_->update_max_reuse_mgr_size(copy_ctx_.macro_block_reuse_mgr_))) {
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
  const uint64_t tenant_id = MTL_ID();

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
  }

#ifdef ERRSIM
    if (OB_SUCC(ret) && is_user_tenant(tenant_id)) {
      ret = PHYSICAL_COPY_TASK_GET_TABLET_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake PHYSICAL_COPY_TASK_GET_TABLET_FAILED", K(ret));
      }
    }
#endif
  if (OB_FAIL(ret)) {
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
    if (FAILEDx(desc.init(
        false/*is ddl*/,
        *storage_schema,
        ls_id,
        tablet_id,
        merge_type,
        tablet->get_snapshot_version(),
        0/*cluster_version*/,
        tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_,
        tablet->get_transfer_seq(),
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
  } else if (sstable_param->table_key_.is_ddl_dump_sstable()) {
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
      tmp_creator = MTL_NEW(ObCopiedSharedSSTableCreator, "CopySSTCreator");
    } else {
      tmp_creator = MTL_NEW(ObCopiedSSTableCreator, "CopySSTCreator");
    }
  } else {
#ifdef OB_BUILD_SHARED_STORAGE
    if (sstable_param_->is_shared_sstable()) {
      if (is_shared_sstable_without_copy_(sstable_param_)) {
        tmp_creator = MTL_NEW(ObCopiedSharedSSTableCreator, "CopySSTCreator");
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
  } else if (sstable_param->table_key_.is_ddl_sstable()) {
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

}
}
