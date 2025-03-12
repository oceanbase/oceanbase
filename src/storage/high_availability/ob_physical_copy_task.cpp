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
#include "observer/ob_server_event_history_table_operator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif

namespace oceanbase
{
using namespace share;
using namespace compaction;
namespace storage
{
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
    ObSSTableCopyFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("physical copy task init tiwce", K(ret));
  } else if (OB_ISNULL(copy_ctx) || !copy_ctx->is_valid() || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical copy task get invalid argument", K(ret), KPC(copy_ctx), KPC(finish_task));
  } else if (OB_FAIL(build_macro_block_copy_info_(finish_task))) {
    LOG_WARN("failed to build macro block copy info", K(ret), KPC(copy_ctx));
  } else {
    copy_ctx_ = copy_ctx;
    finish_task_ = finish_task;
    task_idx_ = finish_task->get_next_copy_task_id();
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
  } else if (OB_FAIL(finish_task->get_next_macro_block_copy_info(copy_table_key_, copy_macro_range_info_))) {
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
    copy_ctx_->total_macro_count_ += copied_ctx.get_macro_block_count();
    copy_ctx_->reuse_macro_count_ += copied_ctx.use_old_macro_block_count_;
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
            *copy_ctx_->sstable_index_builder_, &task_idx_, copy_ctx_->table_key_))) {
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
    if (OB_FAIL(get_restore_reader_(init_param, reader))) {
      LOG_WARN("failed to get_macro_block_restore_reader_", K(ret));
    }
  } else {
    if (OB_FAIL(get_macro_block_ob_reader_(init_param, reader))) {
      LOG_WARN("failed to get_macro_block_ob_reader", K(ret));
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

int ObPhysicalCopyTask::get_restore_reader_(
    const ObCopyMacroBlockReaderInitParam &init_param,
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical copy task do not init", K(ret));
  } else if (!copy_ctx_->is_leader_restore_ || !init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get macro block restore reader get invalid argument", K(ret), KPC(copy_ctx_), K(init_param));
  } else {
    const bool is_shared_macro_blocks = finish_task_->get_sstable_param()->basic_meta_.table_shared_flag_.is_shared_macro_blocks();
    if (init_param.table_key_.is_ddl_dump_sstable() && is_shared_macro_blocks) {
      if (OB_FAIL(get_ddl_macro_block_restore_reader_(init_param, reader))) {
        LOG_WARN("failed to get ddl macro block restore reader", K(ret), K(init_param));
      }
    } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(copy_ctx_->restore_action_)) {
      // Restore action is to replace remote sstables in table store with local sstables, macro block list
      // can be obtained by remote sstable in table store.
      if (OB_FAIL(get_remote_macro_block_restore_reader_(init_param, reader))) {
        LOG_WARN("failed to get_remote_macro_block_restore_reader_", K(ret));
      }
    } else {
      if (OB_FAIL(get_macro_block_restore_reader_(init_param, reader))) {
        LOG_WARN("failed to get macro block restore reader", K(ret), K(init_param));
      }
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

int ObPhysicalCopyTask::get_ddl_macro_block_restore_reader_(
    const ObCopyMacroBlockReaderInitParam &init_param,
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopyDDLMacroBlockRestoreReader *tmp_reader = NULL;

  reader = nullptr;
  if (!copy_ctx_->is_leader_restore_
      || !init_param.is_valid()
      || !init_param.table_key_.is_ddl_dump_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ddl macro block restore reader get invalid argument", K(ret), KPC(copy_ctx_), K(init_param));
  } else if (OB_ISNULL(tmp_reader = MTL_NEW(ObCopyDDLMacroBlockRestoreReader, "MacroDDLReader"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObCopyDDLMacroBlockRestoreReader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(init_param))) {
    LOG_WARN("failed to init ddl macro block restore reader", K(ret), K(init_param), KPC(copy_ctx_));
  } else {
    reader = tmp_reader;
    tmp_reader = nullptr;
  }

  if (OB_NOT_NULL(tmp_reader)) {
    MTL_DELETE(ObCopyDDLMacroBlockRestoreReader, "MacroDDLReader", tmp_reader);
  }

  return ret;
}

int ObPhysicalCopyTask::get_remote_macro_block_restore_reader_(
    const ObCopyMacroBlockReaderInitParam &init_param,
    ObICopyMacroBlockReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopyRemoteSSTableMacroBlockRestoreReader *tmp_reader = nullptr;

  reader = nullptr;
  if (!copy_ctx_->is_leader_restore_
      || !ObTabletRestoreAction::is_restore_replace_remote_sstable(copy_ctx_->restore_action_)
      || !init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get remote macro block restore reader get invalid argument", K(ret), KPC(copy_ctx_), K(init_param));
  } else if (OB_ISNULL(tmp_reader = MTL_NEW(ObCopyRemoteSSTableMacroBlockRestoreReader, "RMacroRReader"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc ObCopyRemoteSSTableMacroBlockRestoreReader", K(ret));
  } else if (OB_FAIL(tmp_reader->init(init_param))) {
    LOG_WARN("failed to init remote macro block restore reader", K(ret), K(init_param), KPC(copy_ctx_));
  } else {
    reader = tmp_reader;
    tmp_reader = nullptr;
  }

  if (OB_NOT_NULL(tmp_reader)) {
    MTL_DELETE(ObCopyRemoteSSTableMacroBlockRestoreReader, "RMacroRReader", tmp_reader);
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
  const ObMigrationSSTableParam *sstable_param = nullptr;
  const bool is_shared_storage = GCTX.is_shared_storage_mode();
  if (OB_ISNULL(reader) || OB_ISNULL(index_block_rebuilder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block writer get invalid argument", K(ret), KP(reader), KP(index_block_rebuilder));
  } else if (FALSE_IT(sstable_param = finish_task_->get_sstable_param())) {
  } else if (OB_ISNULL(sstable_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src sstable param is null", K(ret), KP(finish_task_));
  } else {
    if (!is_shared_storage) {
      tmp_writer = MTL_NEW(ObStorageHALocalMacroBlockWriter, "HAMacroObWriter");
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      if (sstable_param->is_shared_macro_blocks_sstable()) {
        tmp_writer = MTL_NEW(ObStorageHASharedMacroBlockWriter, "HAMacroObWriter");
      } else {
        tmp_writer = MTL_NEW(ObStorageHALocalMacroBlockWriter, "HAMacroObWriter");
      }
#endif
    }

    if (OB_ISNULL(tmp_writer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (OB_FAIL(tmp_writer->init(copy_ctx_->tenant_id_, copy_ctx_->ls_id_, copy_ctx_->tablet_id_,
        this->get_dag()->get_dag_id(), sstable_param, reader, index_block_rebuilder, copy_ctx_->extra_info_))) {
      STORAGE_LOG(WARN, "failed to init macro block writer", K(ret), KPC(copy_ctx_));
    } else {
      writer = tmp_writer;
      tmp_writer = nullptr;
    }

    if (OB_NOT_NULL(tmp_writer)) {
      free_macro_block_writer_(tmp_writer);
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
  } else if (OB_FAIL(tmp_next_task->init(copy_ctx_, finish_task_))) {
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
  MTL_DELETE(ObStorageHAMacroBlockWriter, "MacroObWriter", writer);
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
    init_param.restore_action_ = copy_ctx_->restore_action_;
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
    init_param.macro_block_reuse_mgr_ = copy_ctx_->macro_block_reuse_mgr_;
    init_param.data_version_ = 0;

    if (OB_FAIL(build_data_version_for_macro_block_reuse_(init_param))) {
      LOG_WARN("failed to check enable macro block reuse", K(ret), K(init_param));
    } else if (!init_param.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy macro block reader init param is invalid", K(ret), K(init_param));
    } else {
      LOG_INFO("succeed init param", KPC(copy_macro_range_info_), K(init_param));
    }
  }
  return ret;
}

int ObPhysicalCopyTask::build_data_version_for_macro_block_reuse_(ObCopyMacroBlockReaderInitParam &init_param)
{
  int ret = OB_SUCCESS;
  int64_t snapshot_version = 0;
  int64_t co_base_snapshot_version = 0;
  int64_t src_co_base_snapshot_version = 0;
  uint64_t compat_version = 0;
  init_param.data_version_ = 0;

  if (OB_ISNULL(copy_ctx_->macro_block_reuse_mgr_)) {
    // skip reuse
    init_param.data_version_ = 0;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(copy_ctx_->tenant_id_, compat_version))) {
    LOG_INFO("failed to get min data version", K(ret), KPC(copy_ctx_));
  } else if (compat_version < DATA_VERSION_4_3_4_0) {
    // when reuse macro block, dst will send data_version to src, but data_version cannot be set to larger than 0 before 4.3.4
    // therefore, if src observer version is less than 4.3.4, skip reuse for compatibility
    init_param.data_version_ = 0;
    LOG_INFO("skip reuse for compatibility", K(compat_version), KPC(copy_ctx_));
  } else if (finish_task_->get_sstable_param()->is_small_sstable_) {
    // skip reuse for small sstable
    init_param.data_version_ = 0;
    LOG_INFO("skip reuse for small sstable", KPC(copy_ctx_));
  } else if (OB_FAIL(copy_ctx_->macro_block_reuse_mgr_->get_major_snapshot_version(copy_ctx_->table_key_, snapshot_version, co_base_snapshot_version))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get reuse major snapshot version", K(ret), KPC(copy_ctx_));
    } else {
      ret = OB_SUCCESS;
      init_param.data_version_ = 0;
      LOG_INFO("major snapshot version not exist, maybe copying first major in this tablet or copying F major to C replica, skip reuse, set data_version_ to 0", K(ret), KPC(copy_ctx_), K(init_param));
    }
  } else if (FALSE_IT(src_co_base_snapshot_version = finish_task_->get_sstable_param()->basic_meta_.get_co_base_snapshot_version())) {
  } else if (co_base_snapshot_version != src_co_base_snapshot_version) {
    // when co_base_snapshot_version not match (dst C's major is converted from different version of src major), skip reuse
    init_param.data_version_ = 0;
    LOG_INFO("co_base_snapshot_version not match, skip reuse, set data_version_ to 0", K(snapshot_version), K(co_base_snapshot_version),
        K(src_co_base_snapshot_version), KPC(copy_ctx_), K(init_param));
  } else {
    init_param.data_version_ = snapshot_version;
    LOG_INFO("succeed get and set reuse major max snapshot version", K(snapshot_version), K(co_base_snapshot_version), K(src_co_base_snapshot_version), KPC(copy_ctx_), K(init_param));
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
        "table_key", copy_table_key_,
        "macro_block_count", copy_macro_range_info_->macro_block_count_,
        "src", copy_ctx_->src_info_.src_addr_);
  }
  return ret;
}

}
}
