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
#include "storage/high_availability/ob_physical_copy_task.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/high_availability/ob_storage_ha_tablet_builder.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_mds_schema_helper.h"

namespace oceanbase
{
using namespace share;
using namespace compaction;
namespace storage
{

//errsim def
ERRSIM_POINT_DEF(PHYSICAL_COPY_TASK_GET_TABLET_FAILED);

/******************ObPhysicalCopyCtx*********************/
ObPhysicalCopyCtx::ObPhysicalCopyCtx()
  : lock_(),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    src_info_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    is_leader_restore_(false),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    ha_dag_(nullptr),
    sstable_index_builder_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    need_sort_macro_meta_(true),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    table_key_()
{
}

ObPhysicalCopyCtx::~ObPhysicalCopyCtx()
{
}

bool ObPhysicalCopyCtx::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_id_.is_valid()
      && OB_NOT_NULL(bandwidth_throttle_) && OB_NOT_NULL(svr_rpc_proxy_) && OB_NOT_NULL(ha_dag_)
      && OB_NOT_NULL(sstable_index_builder_) && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
      && table_key_.is_valid();
  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid();
    } else if (OB_ISNULL(restore_base_info_) || OB_ISNULL(second_meta_index_store_)
        || OB_ISNULL(restore_macro_block_id_mgr_)) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

void ObPhysicalCopyCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  src_info_.reset();
  bandwidth_throttle_ = nullptr;
  svr_rpc_proxy_ = nullptr;
  is_leader_restore_ = false;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  ha_dag_ = nullptr;
  sstable_index_builder_ = nullptr;
  restore_macro_block_id_mgr_ = nullptr;
  need_sort_macro_meta_ = true;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  table_key_.reset();
}

/******************ObPhysicalCopyTaskInitParam*********************/
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
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    need_sort_macro_meta_(true),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

ObPhysicalCopyTaskInitParam::~ObPhysicalCopyTaskInitParam()
{
}

bool ObPhysicalCopyTaskInitParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_id_.is_valid() && OB_NOT_NULL(sstable_param_)
      && sstable_macro_range_info_.is_valid() && OB_NOT_NULL(tablet_copy_finish_task_) && OB_NOT_NULL(ls_)
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
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  need_sort_macro_meta_ = true;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

/******************ObPhysicalCopyTask*********************/
ObPhysicalCopyTask::ObPhysicalCopyTask()
  : ObITask(TASK_TYPE_MIGRATE_COPY_PHYSICAL),
    is_inited_(false),
    copy_ctx_(nullptr),
    finish_task_(nullptr),
    copy_table_key_(),
    copy_macro_range_info_(),
    task_idx_(0)
{
}

ObPhysicalCopyTask::~ObPhysicalCopyTask()
{
}

int ObPhysicalCopyTask::init(
    ObPhysicalCopyCtx *copy_ctx,
    ObSSTableCopyFinishTask *finish_task,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical copy task init tiwce", K(ret));
  } else if (OB_ISNULL(copy_ctx) || !copy_ctx->is_valid() || OB_ISNULL(finish_task) || task_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical copy task get invalid argument", K(ret), KPC(copy_ctx), KPC(finish_task), K(task_idx));
  } else if (OB_FAIL(build_macro_block_copy_info_(finish_task))) {
    LOG_WARN("failed to build macro block copy info", K(ret), KPC(copy_ctx));
  } else {
    copy_ctx_ = copy_ctx;
    finish_task_ = finish_task;
    task_idx_ = task_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObPhysicalCopyTask::build_macro_block_copy_info_(ObSSTableCopyFinishTask *finish_task)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build macro block copy info get invalid argument", K(ret), KP(finish_task));
  } else if (OB_FAIL(finish_task->get_macro_block_copy_info(copy_table_key_, copy_macro_range_info_))) {
    if (OB_ITER_END == ret) {
    } else {
      LOG_WARN("failed to get macro block copy info", K(ret));
    }
  } else {
    LOG_INFO("succeed get macro block copy info", K(copy_table_key_), K(copy_macro_range_info_));
  }
  return ret;
}

int ObPhysicalCopyTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMacroBlocksWriteCtx copied_ctx;
  int64_t copy_count = 0;
  int64_t reuse_count = 0;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  ObTabletCopyFinishTask *tablet_finish_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (copy_ctx_->ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip physical copy task", KPC(copy_ctx_));
  } else if (OB_FAIL(finish_task_->get_tablet_finish_task(tablet_finish_task))) {
    LOG_WARN("failed to get tablet finish task", K(ret), KPC(copy_ctx_));
  } else if (OB_FAIL(tablet_finish_task->get_tablet_status(status))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(copy_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("tablet is not exist in src, skip physical copy task", KPC(copy_ctx_));
  } else {
    if (copy_ctx_->tablet_id_.is_inner_tablet() || copy_ctx_->tablet_id_.is_ls_inner_tablet()) {
    } else {
      DEBUG_SYNC(FETCH_MACRO_BLOCK);
    }

    if (OB_SUCC(ret) && copy_macro_range_info_->macro_block_count_ > 0) {
      if (OB_FAIL(fetch_macro_block_with_retry_(copied_ctx))) {
        LOG_WARN("failed to fetch major block", K(ret), K(copy_table_key_), KPC(copy_macro_range_info_));
      } else if (copy_macro_range_info_->macro_block_count_ != copied_ctx.get_macro_block_count()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("list count not match", K(ret), KPC(copy_macro_range_info_),
            K(copied_ctx.get_macro_block_count()), K(copied_ctx));
      }
    }
    LOG_INFO("physical copy task finish", K(ret), KPC(copy_macro_range_info_), KPC(copy_ctx_));
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      status = ObCopyTabletStatus::TABLET_NOT_EXIST;
      if (OB_FAIL(tablet_finish_task->set_tablet_status(status))) {
        LOG_WARN("failed to set copy tablet status", K(ret), K(status), KPC(copy_ctx_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, copy_ctx_->ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(copy_ctx_));
    }
  }

  return ret;
}

int ObPhysicalCopyTask::fetch_macro_block_with_retry_(
    ObMacroBlocksWriteCtx &copied_ctx)
{
  int ret = OB_SUCCESS;
  int64_t retry_times = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else {
    while (retry_times < MAX_RETRY_TIMES) {
      if (retry_times > 0) {
        LOG_INFO("retry get major block", K(retry_times));
      }
      if (OB_FAIL(fetch_macro_block_(retry_times, copied_ctx))) {
        STORAGE_LOG(WARN, "failed to fetch major block", K(ret), K(retry_times));
      }

      if (OB_SUCC(ret)) {
        break;
      }

      if (OB_FAIL(ret)) {
        if (OB_TABLET_NOT_EXIST == ret) {
          break;
        } else {
          copied_ctx.clear();
          retry_times++;
          ob_usleep(OB_FETCH_MAJOR_BLOCK_RETRY_INTERVAL);
        }
      }
    }
  }

  return ret;
}

int ObPhysicalCopyTask::fetch_macro_block_(
    const int64_t retry_times,
    ObMacroBlocksWriteCtx &copied_ctx)
{
  int ret = OB_SUCCESS;
  ObStorageHAMacroBlockWriter *writer = NULL;
  ObICopyMacroBlockReader *reader = NULL;
  ObIndexBlockRebuilder index_block_rebuilder;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy physical task do not init", K(ret));
  } else {
    LOG_INFO("init reader", K(copy_table_key_));
    if (OB_UNLIKELY(task_idx_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task_idx_", K(ret), K(task_idx_));
    } else if (OB_FAIL(index_block_rebuilder.init(
            *copy_ctx_->sstable_index_builder_, copy_ctx_->need_sort_macro_meta_, &task_idx_, copy_ctx_->table_key_.is_ddl_merge_sstable()))) {
      LOG_WARN("failed to init index block rebuilder", K(ret), K(copy_table_key_));
    } else if (OB_FAIL(get_macro_block_reader_(reader))) {
      LOG_WARN("fail to get macro block reader", K(ret));
    } else if (OB_FAIL(get_macro_block_writer_(reader, &index_block_rebuilder, writer))) {
      LOG_WARN("failed to get macro block writer", K(ret), K(copy_table_key_));
    } else if (OB_FAIL(writer->process(copied_ctx, *copy_ctx_->ha_dag_->get_ha_dag_net_ctx()))) {
      LOG_WARN("failed to process writer", K(ret), K(copy_table_key_));
    } else if (copy_macro_range_info_->macro_block_count_ != copied_ctx.get_macro_block_count()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("list count not match", K(ret), K(copy_table_key_), KPC(copy_macro_range_info_),
          K(copied_ctx.get_macro_block_count()), K(copied_ctx));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_MIGRATE_FETCH_MACRO_BLOCK) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        if (retry_times == 0) {
        } else {
          ret = OB_SUCCESS;
        }
        STORAGE_LOG(ERROR, "fake EN_MIGRATE_FETCH_MACRO_BLOCK", K(ret));
      }
    }
#endif

    if (FAILEDx(index_block_rebuilder.close())) {
      LOG_WARN("failed to close index block builder", K(ret), K(copied_ctx));
    }

    if (NULL != reader) {
      free_macro_block_reader_(reader);
    }
    if (NULL != writer) {
      free_macro_block_writer_(writer);
    }
  }
  return ret;
}

int ObPhysicalCopyTask::get_macro_block_reader_(
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopyMacroBlockReaderInitParam init_param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (OB_FAIL(build_copy_macro_block_reader_init_param_(init_param))) {
    LOG_WARN("failed to build macro block reader init param", K(ret), KPC(copy_ctx_));
  } else if (copy_ctx_->is_leader_restore_) {
    if (OB_FAIL(get_macro_block_restore_reader_(init_param, reader))) {
      STORAGE_LOG(WARN, "failed to get_macro_block_restore_reader_", K(ret));
    }
  } else {
    if (OB_FAIL(get_macro_block_ob_reader_(init_param, reader))) {
      STORAGE_LOG(WARN, "failed to get_macro_block_ob_reader", K(ret));
    }
  }
  return ret;
}

int ObPhysicalCopyTask::get_macro_block_ob_reader_(
    const ObCopyMacroBlockReaderInitParam &init_param,
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopyMacroBlockObReader *tmp_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (copy_ctx_->is_leader_restore_ || !init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get macro block ob reader get invalid argument", K(ret), K(init_param));
  } else {
    void *buf = mtl_malloc(sizeof(ObCopyMacroBlockObReader), "MacroObReader");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(tmp_reader = new(buf) ObCopyMacroBlockObReader())) {
    } else if (OB_FAIL(tmp_reader->init(init_param))) {
      STORAGE_LOG(WARN, "failed to init ob reader", K(ret));
    } else {
      reader = tmp_reader;
      tmp_reader = NULL;
    }

    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        reader->~ObICopyMacroBlockReader();
        mtl_free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      tmp_reader->~ObCopyMacroBlockObReader();
      mtl_free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObPhysicalCopyTask::get_macro_block_restore_reader_(
    const ObCopyMacroBlockReaderInitParam &init_param,
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopyMacroBlockRestoreReader *tmp_reader = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (!copy_ctx_->is_leader_restore_ || !init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get macro block restore reader get invalid argument", K(ret), KPC(copy_ctx_), K(init_param));
  } else {
    void *buf = mtl_malloc(sizeof(ObCopyMacroBlockRestoreReader), "MacroRestReader");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(tmp_reader = new(buf) ObCopyMacroBlockRestoreReader())) {
    } else if (OB_FAIL(tmp_reader->init(init_param))) {
      STORAGE_LOG(WARN, "failed to init restore reader", K(ret), K(init_param), KPC(copy_ctx_));
    } else {
      reader = tmp_reader;
      tmp_reader = NULL;
    }

    if (OB_FAIL(ret)) {
      if (NULL != reader) {
        reader->~ObICopyMacroBlockReader();
        mtl_free(reader);
        reader = NULL;
      }
    }
    if (NULL != tmp_reader) {
      tmp_reader->~ObCopyMacroBlockRestoreReader();
      mtl_free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObPhysicalCopyTask::get_macro_block_writer_(
    ObICopyMacroBlockReader *reader,
    ObIndexBlockRebuilder *index_block_rebuilder,
    ObStorageHAMacroBlockWriter *&writer)
{
  int ret = OB_SUCCESS;
  ObStorageHAMacroBlockWriter *tmp_writer = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (OB_ISNULL(reader) || OB_ISNULL(index_block_rebuilder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block writer get invalid argument", K(ret), KP(reader), KP(index_block_rebuilder));
  } else {
    void *buf = mtl_malloc(sizeof(ObStorageHAMacroBlockWriter), "MacroObWriter");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(writer = new(buf) ObStorageHAMacroBlockWriter())) {
    } else if (OB_FAIL(writer->init(copy_ctx_->tenant_id_, copy_ctx_->ls_id_, this->get_dag()->get_dag_id(),
                                    reader, index_block_rebuilder))) {
      STORAGE_LOG(WARN, "failed to init ob reader", K(ret), KPC(copy_ctx_));
    }

    if (OB_FAIL(ret)) {
      free_macro_block_writer_(writer);
    }
  }
  return ret;
}

int ObPhysicalCopyTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObPhysicalCopyTask *tmp_next_task = nullptr;
  bool is_iter_end = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (OB_FAIL(finish_task_->check_is_iter_end(is_iter_end))) {
    LOG_WARN("failed to check is iter end", K(ret));
  } else if (is_iter_end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(copy_ctx_, finish_task_, task_idx_ + 1))) {
    LOG_WARN("failed to init next task", K(ret), K(*copy_ctx_));
  } else {
    next_task = tmp_next_task;
  }

  return ret;
}

void ObPhysicalCopyTask::free_macro_block_reader_(ObICopyMacroBlockReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObICopyMacroBlockReader();
    mtl_free(reader);
    reader = nullptr;
  }
}

void ObPhysicalCopyTask::free_macro_block_writer_(ObStorageHAMacroBlockWriter *&writer)
{
  if (OB_ISNULL(writer)) {
  } else {
    writer->~ObStorageHAMacroBlockWriter();
    mtl_free(writer);
    writer = NULL;
  }
}

int ObPhysicalCopyTask::build_copy_macro_block_reader_init_param_(
    ObCopyMacroBlockReaderInitParam &init_param)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (OB_ISNULL(finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish task should not be null", K(ret));
  } else {
    init_param.tenant_id_ = copy_ctx_->tenant_id_;
    init_param.ls_id_ = copy_ctx_->ls_id_;
    init_param.table_key_ = copy_table_key_;
    init_param.is_leader_restore_ = copy_ctx_->is_leader_restore_;
    init_param.src_info_ = copy_ctx_->src_info_;
    init_param.bandwidth_throttle_ = copy_ctx_->bandwidth_throttle_;
    init_param.svr_rpc_proxy_ = copy_ctx_->svr_rpc_proxy_;
    init_param.restore_base_info_ = copy_ctx_->restore_base_info_;
    init_param.meta_index_store_ = copy_ctx_->meta_index_store_;
    init_param.second_meta_index_store_ = copy_ctx_->second_meta_index_store_;
    init_param.restore_macro_block_id_mgr_ = copy_ctx_->restore_macro_block_id_mgr_;
    init_param.copy_macro_range_info_ = copy_macro_range_info_;
    init_param.need_check_seq_ = copy_ctx_->need_check_seq_;
    init_param.ls_rebuild_seq_ = copy_ctx_->ls_rebuild_seq_;
    init_param.backfill_tx_scn_ = finish_task_->get_sstable_param()->basic_meta_.filled_tx_scn_;
    if (!init_param.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy macro block reader init param is invalid", K(ret), K(init_param));
    } else {
      LOG_INFO("succeed init param", KPC(copy_macro_range_info_), K(init_param));
    }
  }
  return ret;
}

int ObPhysicalCopyTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(copy_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy ctx should not be null", K(ret), KPC_(copy_ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "physical_copy_task",
        "tenant_id", copy_ctx_->tenant_id_,
        "ls_id", copy_ctx_->ls_id_.id(),
        "tablet_id", copy_ctx_->tablet_id_.id(),
        "macro_block_count", copy_macro_range_info_->macro_block_count_,
        "src", copy_ctx_->src_info_.src_addr_);
  }
  return ret;
}

/******************ObSSTableCopyFinishTask*********************/
ObSSTableCopyFinishTask::ObSSTableCopyFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL),
    is_inited_(false),
    copy_ctx_(),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    sstable_param_(nullptr),
    sstable_macro_range_info_(),
    macro_range_info_index_(0),
    tablet_copy_finish_task_(nullptr),
    ls_(nullptr),
    tablet_service_(nullptr),
    sstable_index_builder_(),
    restore_macro_block_id_mgr_(nullptr)
{
}

ObSSTableCopyFinishTask::~ObSSTableCopyFinishTask()
{
  if (OB_NOT_NULL(restore_macro_block_id_mgr_)) {
    ob_delete(restore_macro_block_id_mgr_);
  }
}

int ObSSTableCopyFinishTask::init(
    const ObPhysicalCopyTaskInitParam &init_param)
{
  int ret = OB_SUCCESS;
  common::ObInOutBandwidthThrottle *bandwidth_throttle = nullptr;
  ObLSService *ls_service = nullptr;
  ObStorageRpcProxy *svr_rpc_proxy = nullptr;
  ObStorageHADag *ha_dag = nullptr;

  if (is_inited_) {
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
    macro_range_info_index_ = 0;
    ls_ = init_param.ls_;
    sstable_param_ = init_param.sstable_param_;
    tablet_copy_finish_task_ = init_param.tablet_copy_finish_task_;
    if (OB_FAIL(prepare_sstable_index_builder_(init_param.ls_id_,
        init_param.tablet_id_, init_param.sstable_param_))) {
      LOG_WARN("failed to prepare sstable index builder", K(ret), K(init_param));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init ObPhysicalCopyFinishTask", K(init_param), K(sstable_macro_range_info_));
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::get_macro_block_copy_info(
    ObITable::TableKey &copy_table_key,
    const ObCopyMacroRangeInfo *&copy_macro_range_info)
{
  int ret = OB_SUCCESS;
  copy_table_key.reset();
  copy_macro_range_info = nullptr;
  ObMacroBlockCopyInfo macro_block_copy_info;
  ObMigrationFakeBlockID block_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (macro_range_info_index_ == sstable_macro_range_info_.copy_macro_range_array_.count()) {
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

int ObSSTableCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
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
  } else {
    LOG_INFO("succeed physical copy finish", K(copy_ctx_));
  }

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

int ObSSTableCopyFinishTask::check_is_iter_end(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  is_iter_end = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (macro_range_info_index_ == sstable_macro_range_info_.copy_macro_range_array_.count()) {
      is_iter_end = true;
    } else {
      is_iter_end = false;
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::prepare_data_store_desc_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMigrationSSTableParam *sstable_param,
    ObWholeDataStoreDesc &desc)
{
  int ret = OB_SUCCESS;
  desc.reset();
  ObTablet *tablet = nullptr;
  ObMergeType merge_type = ObMergeType::INVALID_MERGE_TYPE;
  const ObMigrationTabletParam *src_tablet_param = nullptr;
  const ObStorageSchema *storage_schema = nullptr;
  ObTabletHandle tablet_handle;
  const uint64_t tenant_id = MTL_ID();

  if (OB_UNLIKELY(!tablet_id.is_valid() || NULL == sstable_param || NULL == tablet_copy_finish_task_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare sstable index builder get invalid argument", K(ret), K(tablet_id), KP(sstable_param));
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
    if (sstable_param->table_key_.is_cg_sstable()) {
      if (OB_UNLIKELY(cg_idx < 0 || cg_idx >= storage_schema->get_column_group_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected cg idx", K(ret), K(cg_idx), KPC(storage_schema));
      } else {
        cg_schema = &storage_schema->get_column_groups().at(cg_idx);
      }
    }
    if (FAILEDx(desc.init(
        *storage_schema,
        ls_id,
        tablet_id,
        merge_type,
        tablet->get_snapshot_version(),
        0/*cluster_version*/,
        sstable_param->table_key_.get_end_scn(),
        cg_schema,
        cg_idx))) {
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

int ObSSTableCopyFinishTask::prepare_sstable_index_builder_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObMigrationSSTableParam *sstable_param)
{
  int ret = OB_SUCCESS;
  ObWholeDataStoreDesc desc;
  const ObSSTableIndexBuilder::ObSpaceOptimizationMode mode =
      (sstable_param->table_key_.is_ddl_sstable() || !sstable_param->is_small_sstable_)
      ? ObSSTableIndexBuilder::DISABLE : ObSSTableIndexBuilder::ENABLE;

  if (!tablet_id.is_valid() || OB_ISNULL(sstable_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare sstable index builder get invalid argument", K(ret), K(tablet_id), KP(sstable_param));
  } else if (0 == sstable_param->basic_meta_.data_macro_block_count_) {
    LOG_INFO("sstable is empty, no need build sstable index builder", K(ret), K(tablet_id), KPC(sstable_param));
  } else if (OB_FAIL(prepare_data_store_desc_(ls_id, tablet_id, sstable_param, desc))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      //overwrite ret
      if (OB_FAIL(tablet_copy_finish_task_->set_tablet_status(ObCopyTabletStatus::TABLET_NOT_EXIST))) {
        LOG_WARN("failed to set tablet status", K(ret), K(tablet_id));
      }
    } else {
      LOG_WARN("failed to prepare data store desc", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(sstable_index_builder_.init(
      desc.get_desc(),
      nullptr, // macro block flush callback, default value is nullptr
      mode))) {
    LOG_WARN("failed to init sstable index builder", K(ret), K(desc));
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
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable type is unexpected", K(ret), KPC(sstable_param));
  }
  return ret;
}

int ObSSTableCopyFinishTask::create_sstable_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (0 == sstable_param_->basic_meta_.data_macro_block_count_) {
    //create empty sstable
    if (OB_FAIL(create_empty_sstable_())) {
      LOG_WARN("failed to create empty sstable", K(ret), KPC(sstable_param_));
    }
  } else {
    if (OB_FAIL(create_sstable_with_index_builder_())) {
      LOG_WARN("failed to create sstable with index builder", K(ret), KPC(sstable_param_));
    }
  }
  return ret;
}

int ObSSTableCopyFinishTask::create_empty_sstable_()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  ObTabletCreateSSTableParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(build_create_sstable_param_(param))) {
    LOG_WARN("failed to build create sstable param", K(ret));
  } else if (param.table_key_.is_co_sstable()) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    common::ObArenaAllocator tmp_allocator; // for storage schema
    ObStorageSchema *storage_schema_ptr = nullptr;
    if (OB_FAIL(ls_->ha_get_tablet(copy_ctx_.tablet_id_, tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(copy_ctx_));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(copy_ctx_));
    } else if (OB_FAIL(tablet->load_storage_schema(tmp_allocator, storage_schema_ptr))) {
      LOG_WARN("failed to load storage_schema", K(ret), KPC(tablet));
    } else if (FALSE_IT(param.column_group_cnt_ = sstable_param_->column_group_cnt_)) {
    } else if (FALSE_IT(param.is_co_table_without_cgs_ = param.table_key_.is_ddl_sstable() ? false : true)) {
    } else if (FALSE_IT(param.full_column_cnt_ = sstable_param_->full_column_cnt_)) {
      LOG_WARN("failed to get_stored_column_count_in_sstable", K(ret), KPC(storage_schema_ptr));
    } else if (FALSE_IT(param.co_base_type_ = sstable_param_->co_base_type_)) {
    } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param,
            tablet_copy_finish_task_->get_allocator(), table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(param), K(copy_ctx_));
    }
    ObTabletObjLoadHelper::free(tmp_allocator, storage_schema_ptr);
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param,
          tablet_copy_finish_task_->get_allocator(), table_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(param), K(copy_ctx_));
  }

  if (FAILEDx(tablet_copy_finish_task_->add_sstable(table_handle))) {
    LOG_WARN("failed to add table handle", K(ret), K(table_handle), K(copy_ctx_));
  }
  return ret;
}

int ObSSTableCopyFinishTask::create_sstable_with_index_builder_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int64_t column_count = 0;
  ObSSTableMergeRes res;
  ObTableHandleV2 table_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else {
    SMART_VAR(ObTabletCreateSSTableParam, param) {
      //TODO(lingchuan) column_count should not be in parameters
      if (OB_FAIL(ls_->ha_get_tablet(copy_ctx_.tablet_id_, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(copy_ctx_));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(copy_ctx_));
      } else if (OB_FAIL(sstable_index_builder_.close(res))) {
        LOG_WARN("failed to close sstable index builder", K(ret), K(copy_ctx_));
      } else if (OB_FAIL(build_create_sstable_param_(tablet, res, param))) {
        LOG_WARN("failed to build create sstable param", K(ret), K(copy_ctx_));
      } else if (param.table_key_.is_co_sstable()) {
        if (sstable_param_->co_base_type_ <= ObCOSSTableBaseType::INVALID_TYPE ||
            sstable_param_->co_base_type_ >= ObCOSSTableBaseType::MAX_TYPE ||
            sstable_param_->column_group_cnt_ <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("co sstable with invalid co sstable param", K(ret), KPC(sstable_param_));
        } else if (FALSE_IT(param.column_group_cnt_ = sstable_param_->column_group_cnt_)) {
        } else if (FALSE_IT(param.full_column_cnt_ = sstable_param_->full_column_cnt_)) {
        } else if (FALSE_IT(param.co_base_type_ = sstable_param_->co_base_type_)) {
        } else if (FALSE_IT(param.is_co_table_without_cgs_ = sstable_param_->is_empty_cg_sstables_)) {
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(param,
              tablet_copy_finish_task_->get_allocator(), table_handle))) {
          LOG_WARN("failed to create co sstable", K(ret), K(copy_ctx_), KPC(sstable_param_));
        }
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param,
              tablet_copy_finish_task_->get_allocator(), table_handle))) {
        LOG_WARN("failed to create sstable", K(ret), K(copy_ctx_), KPC(sstable_param_));
      }

      if (FAILEDx(tablet_copy_finish_task_->add_sstable(table_handle))) {
        LOG_WARN("failed to add table handle", K(ret), K(table_handle), K(copy_ctx_));
      }
    }
  }
  return ret;
}


int ObSSTableCopyFinishTask::build_create_sstable_param_(
    ObTablet *tablet,
    const blocksstable::ObSSTableMergeRes &res,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(tablet) || !res.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build create sstable param get invalid argument", K(ret), KP(tablet), K(res));
  } else if (OB_FAIL(param.init_for_ha(*sstable_param_, res))) {
    LOG_WARN("fail to init create sstable param", K(ret), KPC(sstable_param_), K(res));
  }
  return ret;
}

int ObSSTableCopyFinishTask::build_create_sstable_param_(
    ObTabletCreateSSTableParam &param)
{
  //using sstable meta to create sstable
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (0 != sstable_param_->basic_meta_.data_macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable param has data macro block, can not build sstable from basic meta", K(ret), KPC(sstable_param_));
  } else if (OB_FAIL(param.init_for_ha(*sstable_param_))) {
    LOG_WARN("fail to init create sstable param", K(ret), KPC(sstable_param_));
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
  } else {
    void *buf = mtl_malloc(sizeof(ObRestoreMacroBlockIdMgr), "RestoreMacIdMgr");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(restore_macro_block_id_mgr = new(buf) ObRestoreMacroBlockIdMgr())) {
    } else if (OB_FAIL(restore_macro_block_id_mgr->init(init_param.tablet_id_,
        init_param.sstable_macro_range_info_.copy_table_key_, *init_param.restore_base_info_,
        *init_param.meta_index_store_, *init_param.second_meta_index_store_))) {
      STORAGE_LOG(WARN, "failed to init restore macro block id mgr", K(ret), K(init_param));
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

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy finish task do not init", K(ret));
  } else if (OB_FAIL(tablet_copy_finish_task_->get_sstable(sstable_param_->table_key_, table_handle))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(sstable_param_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be NULL", K(ret), KP(sstable), KPC(sstable_param_));
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta handle", K(ret));
  } else if (OB_FAIL(check_sstable_meta_(*sstable_param_, sst_meta_hdl.get_sstable_meta()))) {
    LOG_WARN("failed to check sstable meta", K(ret), KPC(sstable), KPC(sstable_param_));
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

/******************ObTabletCopyFinishTask*********************/
ObTabletCopyFinishTask::ObTabletCopyFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL),
    is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    tablet_id_(),
    ls_(nullptr),
    reporter_(nullptr),
    ha_dag_(nullptr),
    arena_allocator_("TabCopyFinish"),
    minor_tables_handle_(),
    ddl_tables_handle_(),
    major_tables_handle_(),
    mds_tables_handle_(),
    restore_action_(ObTabletRestoreAction::MAX),
    src_tablet_meta_(nullptr),
    copy_tablet_ctx_(nullptr)


{
}

ObTabletCopyFinishTask::~ObTabletCopyFinishTask()
{
}

int ObTabletCopyFinishTask::init(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    observer::ObIMetaReport *reporter,
    const ObTabletRestoreAction::ACTION &restore_action,
    const ObMigrationTabletParam *src_tablet_meta,
    ObICopyTabletCtx *copy_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet copy finish task init twice", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(ls) || OB_ISNULL(reporter) || OB_ISNULL(src_tablet_meta)
      || !ObTabletRestoreAction::is_valid(restore_action) || OB_ISNULL(copy_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet copy finish task get invalid argument", K(ret), K(tablet_id), KP(ls),
        KP(reporter), KP(src_tablet_meta), K(restore_action));
  } else if (OB_FAIL(copy_tablet_ctx->set_copy_tablet_status(ObCopyTabletStatus::TABLET_EXIST))) {
    LOG_WARN("failed to set copy tablet status", K(ret));
  } else {
    tablet_id_ = tablet_id;
    ls_ = ls;
    reporter_ = reporter;
    ha_dag_ = static_cast<ObStorageHADag *>(this->get_dag());
    restore_action_ = restore_action;
    src_tablet_meta_ = src_tablet_meta;
    copy_tablet_ctx_ = copy_tablet_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  bool only_contain_major = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip physical copy finish task", K(tablet_id_), KPC(ha_dag_));
  } else if (OB_FAIL(get_tablet_status(status))) {
    LOG_WARN("failed to get tablet status", K(ret), K(tablet_id_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet from src do not exist, skip copy finish task", K(tablet_id_), K(status));
  } else if (OB_FAIL(check_log_replay_to_mds_sstable_end_scn_())) {
    LOG_WARN("failed to check log replay to mds sstable end scn", K(ret), K(tablet_id_));
  } else if (OB_FAIL(create_new_table_store_with_major_())) {
    LOG_WARN("failed to create new table store with major", K(ret), K_(tablet_id));
  } else if (OB_FAIL(create_new_table_store_with_minor_())) {
    LOG_WARN("failed to create new table store with minor", K(ret), K_(tablet_id));
  } else if (OB_FAIL(trim_tablet_())) {
    LOG_WARN("failed to trim tablet", K(ret), K_(tablet_id));
  } else if (OB_FAIL(check_tablet_valid_())) {
    LOG_WARN("failed to check tablet valid", K(ret), KPC(this));
  } else if (OB_FAIL(check_finish_copy_tablet_data_valid_())) {
    LOG_WARN("failed to update tablet data status", K(ret), K(tablet_id_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TABLET_NOT_EXIST == ret) {
      FLOG_INFO("tablet is not exist, skip copy tablet", K(tablet_id_));
      //overwrite ret
      ret = OB_SUCCESS;
    }
  }

  SERVER_EVENT_ADD("storage_ha", "tablet_copy_finish_task",
        "tenant_id", MTL_ID(),
        "ls_id", ls_->get_ls_id().id(),
        "tablet_id", tablet_id_.id(),
        "ret", ret,
        "result", ha_dag_->get_ha_dag_net_ctx()->is_failed());

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(tablet_id_));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::add_sstable(ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray *tables_handle_ptr = nullptr;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable get invalid argument", K(ret), K(table_handle));
  } else if (OB_FAIL(get_tables_handle_ptr_(table_handle.get_table()->get_key(), tables_handle_ptr))) {
    LOG_WARN("failed to get tables handle ptr", K(ret), K(table_handle));
  } else if (OB_FAIL(tables_handle_ptr->add_table(table_handle))) {
    LOG_WARN("failed to add table", K(ret), K(table_handle));
  }
  return ret;
}

int ObTabletCopyFinishTask::get_sstable(
    const ObITable::TableKey &table_key,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *meta_mem_mgr = nullptr;
  bool found = false;
  ObTablesHandleArray *tables_handle_ptr = nullptr;
  common::SpinRLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable get invalid argument", K(ret), K(table_key));
  } else if (OB_ISNULL(meta_mem_mgr = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get meta mem mgr from MTL", K(ret));
  } else if (OB_FAIL(get_tables_handle_ptr_(table_key, tables_handle_ptr))) {
    LOG_WARN("failed to get tables handle ptr", K(ret), K(table_key));
  } else {
    ObTableHandleV2 tmp_table_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_ptr->get_count() && !found; ++i) {
      if (OB_FAIL(tables_handle_ptr->get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table handle", K(ret), K(i));
      } else if (tmp_table_handle.get_table()->get_key() == table_key) {
        table_handle = tmp_table_handle;
        found = true;
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not get sstable, unexected", K(ret), K(table_key), K(major_tables_handle_),
          K(minor_tables_handle_), K(ddl_tables_handle_));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::create_new_table_store_with_minor_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_table_with_minor_tables(ls_, tablet_id_,
      src_tablet_meta_, mds_tables_handle_, minor_tables_handle_, ddl_tables_handle_, restore_action_))) {
    LOG_WARN("failed to build table with ddl tables", K(ret));
  }
  return ret;
}

int ObTabletCopyFinishTask::create_new_table_store_with_major_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_ISNULL(src_tablet_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet meta should not be null", K(ret));
  } else if (major_tables_handle_.empty()) {
    // do nothing
  } else if (ObTabletRestoreAction::is_restore_major(restore_action_)) {
    if (1 == major_tables_handle_.get_count()) {
      // do nothing
    } else if (OB_NOT_NULL(major_tables_handle_.get_table(0))
        && major_tables_handle_.get_table(0)->is_column_store_sstable()) {
      // if more than one major table, it should be column store tables
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major tablet should only has one sstable", K(ret),
               "major_sstable_count", major_tables_handle_.get_count(), K(major_tables_handle_));
    }
  }

  if (OB_FAIL(ret) || major_tables_handle_.empty()) {
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_tablet_with_major_tables(ls_, tablet_id_,
      major_tables_handle_, src_tablet_meta_->storage_schema_))) {
    LOG_WARN("failed to build tablet with major tables", K(ret), K(tablet_id_), K(major_tables_handle_), KPC(src_tablet_meta_));
  }
  return ret;
}

int ObTabletCopyFinishTask::check_finish_copy_tablet_data_valid_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
  bool is_logical_sstable_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id_), KP(tablet));
  } else if (tablet->get_tablet_meta().has_next_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet here should only has one", K(ret), KPC(tablet));
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::check_remote_logical_sstable_exist(tablet, is_logical_sstable_exist))) {
    LOG_WARN("failed to check remote logical sstable exist", K(ret), KPC(tablet));
  } else if (is_logical_sstable_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet still has remote logical sstable, unexpected !!!", K(ret), KPC(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &major_sstables = table_store_wrapper.get_member()->get_major_sstables();
    if (OB_SUCC(ret)
        && tablet->get_tablet_meta().table_store_flag_.with_major_sstable()
        && tablet->get_tablet_meta().ha_status_.is_restore_status_full()
        && !tablet->get_tablet_meta().has_transfer_table()
        && major_sstables.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should has major sstable, unexpected", K(ret), KPC(tablet));
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(MTL(observer::ObTabletTableUpdater*))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet table updater should not be null", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(
          ls_->get_ls_id(), tablet_id_))) {
        LOG_WARN("failed to submit tablet update task", K(tmp_ret), KPC(ls_), K(tablet_id_));
      }
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::get_tables_handle_ptr_(
    const ObITable::TableKey &table_key,
    ObTablesHandleArray *&tables_handle_ptr)
{
  int ret = OB_SUCCESS;
  tables_handle_ptr = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tables handle ptr get invalid argument", K(ret), K(table_key));
  } else if (table_key.is_major_sstable()) {
    tables_handle_ptr = &major_tables_handle_;
  } else if (table_key.is_minor_sstable()) {
    tables_handle_ptr = &minor_tables_handle_;
  } else if (table_key.is_ddl_sstable()) {
    tables_handle_ptr = &ddl_tables_handle_;
  } else if (table_key.is_mds_sstable()) {
    tables_handle_ptr = &mds_tables_handle_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tables handle ptr get unexpected table key", K(ret), K(table_key));
  }
  return ret;
}

int ObTabletCopyFinishTask::trim_tablet_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const bool is_rollback = false;
  bool need_merge = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id_));
  } else if (tablet->get_tablet_meta().has_next_tablet_
      && OB_FAIL(ls_->trim_rebuild_tablet(tablet_id_, is_rollback))) {
    LOG_WARN("failed to trim rebuild tablet", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObTabletCopyFinishTask::set_tablet_status(const ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!ObCopyTabletStatus::is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tablet status get invalid argument", K(ret), K(status));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(copy_tablet_ctx_->set_copy_tablet_status(status))) {
      LOG_WARN("failed to set copy tablet status", K(ret));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::check_tablet_valid_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish restore task do not init", K(ret));
  } else if (OB_FAIL(ls_->get_tablet(tablet_id_, tablet_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_id_));
  } else if (OB_FAIL(tablet->check_valid(true/*ignore_ha_status*/))) {
    LOG_WARN("failed to check valid", K(ret), KPC(tablet));
  }
  return ret;
}

int ObTabletCopyFinishTask::get_tablet_status(ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  status = ObCopyTabletStatus::MAX_STATUS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status))) {
      LOG_WARN("failed to get copy tablet status", K(ret));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::check_log_replay_to_mds_sstable_end_scn_()
{
  int ret = OB_SUCCESS;
  SCN max_end_scn(SCN::min_scn());
  SCN current_replay_scn;
  const int64_t total_timeout = 20_min;
  const int64_t wait_replay_timeout = 10_min;
  bool is_ls_deleted = false;
  SCN last_replay_scn;
  share::SCN readable_scn;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t current_ts = 0;
  int64_t last_replay_ts = 0;
  const int64_t CHECK_CONDITION_INTERVAL = 200_ms;
  ObTimeoutCtx timeout_ctx;
  int32_t result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (tablet_id_.is_ls_inner_tablet()) {
    //do nothing
  } else if (OB_FAIL(timeout_ctx.set_timeout(total_timeout))) {
    LOG_WARN("failed to set timeout ctx", K(ret), K(tablet_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mds_tables_handle_.get_count(); ++i) {
      const ObITable *table = mds_tables_handle_.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mds table should not be NULL", K(ret), KP(table), K(mds_tables_handle_));
      } else {
        max_end_scn = SCN::max(table->get_end_scn(), max_end_scn);
      }
    }

    while (OB_SUCC(ret)) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_WAIT_REPLAY_TIMEOUT;
        LOG_WARN("wait log replay to mds sstable end scn already timeout", K(ret));
      } else if (ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
        FLOG_INFO("ha dag net is already failed, skip physical copy finish task", K(tablet_id_), KPC(ha_dag_));
        ret = OB_CANCELED;
      } else if (ls_->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop check log replay scn", K(ret), K(tablet_id_));
      } else if (OB_FAIL(ObStorageHAUtils::check_ls_deleted(ls_->get_ls_id(), is_ls_deleted))) {
        LOG_WARN("failed to get ls status from inner table", K(ret));
      } else if (is_ls_deleted) {
        ret = OB_CANCELED;
        LOG_WARN("ls will be removed, no need run migration", K(ret), KPC(ls_), K(is_ls_deleted));
      } else if (OB_FAIL(ObStorageHAUtils::check_log_status(ls_->get_tenant_id(), ls_->get_ls_id(), result))) {
        LOG_WARN("failed to check log status", K(ret), KPC(ls_), K(tablet_id_));
      } else if (OB_SUCCESS != result) {
        LOG_INFO("can not replay log, it will retry", K(result), KPC(ha_dag_));
        if (OB_FAIL(ha_dag_->get_ha_dag_net_ctx()->set_result(result/*result*/, true/*need_retry*/, this->get_dag()->get_type()))) {
          LOG_WARN("failed to set result", K(ret), KPC(ha_dag_));
        } else {
          ret = result;
          LOG_WARN("log sync or replay error, need retry", K(ret), KPC(ha_dag_));
        }
      } else if (OB_FAIL(ls_->get_max_decided_scn(current_replay_scn))) {
        LOG_WARN("failed to get current replay log ts", K(ret), K(tablet_id_));
      } else if (current_replay_scn >= max_end_scn) {
        break;
      } else {
        current_ts = ObTimeUtility::current_time();
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("replay log is not ready, retry next loop", K(tablet_id_),
              "current_replay_scn", current_replay_scn,
              "mds max end scn", max_end_scn);
        }

        if (current_replay_scn == last_replay_scn) {
          if (current_ts - last_replay_ts > wait_replay_timeout) {
            ret = OB_WAIT_REPLAY_TIMEOUT;
            LOG_WARN("failed to check log replay to mds end scn", K(ret), K(tablet_id_), K(current_replay_scn), K(max_end_scn));
          }
        } else if (last_replay_scn > current_replay_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last end log ts should not smaller than current end log ts", K(ret),
              K(last_replay_scn), K(current_replay_scn));
        } else {
          last_replay_scn = current_replay_scn;
          last_replay_ts = current_ts;
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
          if (OB_FAIL(share::dag_yield())) {
            LOG_WARN("fail to yield dag", KR(ret));
          }
        }
      }
    }
  }

  LOG_INFO("finish check_log_replay_to_mds_sstable_end_scn_",
      K(ret), K(tablet_id_), "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}


}
}

