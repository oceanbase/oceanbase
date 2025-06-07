/**
 * Copyright (c) 2024 OceanBase
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
#include "storage/high_availability/ob_sstable_copy_start_task.h"

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_GET_MACRO_ID_INFO_READER_FAILED);

ObSSTableCopyStartTask::ObSSTableCopyStartTask()
  : ObITask(TASK_TYPE_MIGRATE_START_PHYSICAL),
    is_inited_(false),
    copy_ctx_(nullptr),
    finish_task_(nullptr),
    reader_(nullptr)
{
}

ObSSTableCopyStartTask::~ObSSTableCopyStartTask()
{
}

int ObSSTableCopyStartTask::init(ObPhysicalCopyCtx *copy_ctx, ObSSTableCopyFinishTask *finish_task)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sstable copy start task init twice", K(ret));
  } else if (OB_ISNULL(copy_ctx) || !copy_ctx->is_valid() || OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable copy start task get invalid argument", K(ret), KPC(copy_ctx), KPC(finish_task));
  } else {
    copy_ctx_ = copy_ctx;
    finish_task_ = finish_task;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableCopyStartTask::process()
{
  // for shared sstable, only get macro block id list from source
  int ret = OB_SUCCESS;
  const ObMigrationSSTableParam *sstable_param = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
  } else if (OB_ISNULL(finish_task_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable copy finish task is null", K(ret));
  } else if (OB_ISNULL(sstable_param = finish_task_->get_sstable_param())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable param should not be NULL", K(ret), KPC(copy_ctx_));
  } else if (finish_task_->get_sstable_param()->basic_meta_.table_shared_flag_.is_shared_macro_blocks()
      || finish_task_->get_sstable_param()->basic_meta_.table_backup_flag_.is_shared_sstable()) {
    if (OB_FAIL(do_with_shared_sstable_())) {
      LOG_WARN("failed to do with shared sstable", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("Only Shared SSTable need build physical macro block id", K(ret), KPC(sstable_param));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, copy_ctx_->ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(copy_ctx_));
    }
  }

  return ret;
}

int ObSSTableCopyStartTask::do_with_shared_sstable_()
{
  int ret = OB_SUCCESS;
  bool is_rpc_not_support = false;
  if (OB_FAIL(prepare_sstable_macro_range_info_(is_rpc_not_support))) {
    LOG_WARN("failed to prepare sstable macro range info", K(ret));
  } else if (is_rpc_not_support) {
    // skip build reuse info (for compatibility)
    LOG_INFO("rpc not support, skip build reuse info", K(ret));
  }
  return ret;
}

int ObSSTableCopyStartTask::prepare_sstable_macro_range_info_(bool &is_rpc_not_support)
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroIdInfo macro_block_id_info;
  is_rpc_not_support = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
  } else if (OB_FAIL(fetch_sstable_macro_id_info_(macro_block_id_info, is_rpc_not_support))) {
    LOG_WARN("failed to fetch sstable logic macro info", K(ret), KPC(copy_ctx_));
  } else if (is_rpc_not_support) {
    // skip
  } else if (OB_FAIL(finish_task_->add_macro_block_id_info(macro_block_id_info))) {
    LOG_WARN("failed to add logic macro info for range", K(ret), KPC(copy_ctx_));
  }
  return ret;
}

int ObSSTableCopyStartTask::fetch_sstable_macro_id_info_(ObCopySSTableMacroIdInfo &macro_block_id_info, bool &is_rpc_not_support)
{
  int ret = OB_SUCCESS;
  ObICopySSTableMacroIdInfoReader *reader = nullptr;
  macro_block_id_info.reset();
  is_rpc_not_support = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
#ifdef ERRSIM
  } else if (OB_SUCCESS != EN_GET_MACRO_ID_INFO_READER_FAILED) {
    ret = EN_GET_MACRO_ID_INFO_READER_FAILED;
    LOG_WARN("[ERRSIM] fake get macro id info reader failed", K(ret));
#endif
  } else if (OB_FAIL(get_macro_id_info_reader_(reader, is_rpc_not_support))) {
    LOG_WARN("failed to get macro id info reader", K(ret));
  } else if (is_rpc_not_support) {
    // skip
  } else if (OB_ISNULL(reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro id info reader is null", K(ret));
  } else if (OB_FAIL(reader->get_sstable_macro_id_info(macro_block_id_info))) {
    LOG_WARN("failed to get logic macro block ids", K(ret));
  } else {
    LOG_INFO("succeed fetch sstable logic macro block ids", K(macro_block_id_info));
  }

  if (nullptr != reader) {
    free_macro_id_info_reader_(reader);
  }

  return ret;
}

int ObSSTableCopyStartTask::get_macro_id_info_reader_(ObICopySSTableMacroIdInfoReader *&reader, bool &is_rpc_not_support)
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroIdInfoReaderInitParam init_param;
  is_rpc_not_support = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
  } else if (OB_FAIL(build_macro_id_info_reader_init_param_(init_param))) {
    LOG_WARN("failed to build macro id info reader init param", K(ret));
  } else {
    if (OB_FAIL(get_macro_id_info_ob_reader_(init_param, reader))) {
      if (OB_NOT_SUPPORTED == ret) {
        // overwrite ret
        ret = OB_SUCCESS;
        is_rpc_not_support = true;
        LOG_INFO("get macro block ob reader not supported", K(ret));
      } else {
        LOG_WARN("failed to get macro block ob reader", K(ret));
      }
    }
  }

  return ret;
}

int ObSSTableCopyStartTask::build_macro_id_info_reader_init_param_(ObCopySSTableMacroIdInfoReaderInitParam &init_param)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
  } else if (OB_ISNULL(finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable copy finish task is null", K(ret));
  } else {
    init_param.tenant_id_ = copy_ctx_->tenant_id_;
    init_param.ls_id_ = copy_ctx_->ls_id_;
    init_param.table_key_ = copy_ctx_->table_key_;
    init_param.src_info_ = copy_ctx_->src_info_;
    init_param.bandwidth_throttle_ = copy_ctx_->bandwidth_throttle_;
    init_param.svr_rpc_proxy_ = copy_ctx_->svr_rpc_proxy_;
    init_param.need_check_seq_ = copy_ctx_->need_check_seq_;
    init_param.ls_rebuild_seq_ = copy_ctx_->ls_rebuild_seq_;
    init_param.filled_tx_scn_ = finish_task_->get_sstable_param()->basic_meta_.filled_tx_scn_;
  }

  return ret;
}

int ObSSTableCopyStartTask::get_macro_id_info_ob_reader_(
    const ObCopySSTableMacroIdInfoReaderInitParam &init_param,
    ObICopySSTableMacroIdInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  ObCopySSTableMacroIdInfoObReader *tmp_reader = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("sstable copy start task do not init", K(ret));
  } else if (copy_ctx_->is_leader_restore_ || !init_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get macro block restore reader get invalid argument", K(ret), KPC(copy_ctx_), K(init_param));
  } else {
    void *buf = mtl_malloc(sizeof(ObCopySSTableMacroIdInfoObReader), "MacroIdObReader");
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (FALSE_IT(tmp_reader = new(buf) ObCopySSTableMacroIdInfoObReader())) {
    } else if (OB_FAIL(tmp_reader->init(init_param))) {
      LOG_WARN("failed to init ob reader", K(ret));
    } else {
      reader = tmp_reader;
      tmp_reader = nullptr;
    }

    if (nullptr != tmp_reader) {
      tmp_reader->~ObCopySSTableMacroIdInfoObReader();
      mtl_free(tmp_reader);
      tmp_reader = nullptr;
    }
  }

  return ret;
}

void ObSSTableCopyStartTask::free_macro_id_info_reader_(ObICopySSTableMacroIdInfoReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObICopySSTableMacroIdInfoReader();
    mtl_free(reader);
    reader = nullptr;
  }
}

} // namespace storage
} // namespace oceanbase
