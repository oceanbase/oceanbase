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

#include "lib/allocator/ob_malloc.h"
#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{

using namespace blocksstable;
using namespace share;
using namespace common;

ObDDLIncStartClogCb::ObDDLIncStartClogCb()
  : is_inited_(false), log_basic_(), scn_(SCN::min_scn())
{
}

int ObDDLIncStartClogCb::init(const ObDDLIncLogBasic& log_basic)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_basic));
  } else {
    log_basic_ = log_basic;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLIncStartClogCb::on_success()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObDDLIncStartClogCb::on_success", 1 * 1000 * 1000); // 1s
  scn_ = __get_scn();
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();

  return OB_SUCCESS;
}

int ObDDLIncStartClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  status_.set_state(STATE_FAILED);
  try_release();

  return OB_SUCCESS;
}

void ObDDLIncStartClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncStartClogCb *cb = this;
    ob_delete(cb);
  }
}

ObDDLIncRedoClogCb::ObDDLIncRedoClogCb()
  : is_inited_(false), ls_id_(), redo_info_(), macro_block_id_(),
    data_buffer_lock_(), is_data_buffer_freed_(false)
{

}

ObDDLIncRedoClogCb::~ObDDLIncRedoClogCb()
{
  int ret = OB_SUCCESS;
  if (macro_block_id_.is_valid() && OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(macro_block_id_))) {
    LOG_ERROR("dec ref failed", K(ret), K(macro_block_id_), K(common::lbt()));
  }
  macro_block_id_.reset();
}

int ObDDLIncRedoClogCb::init(const share::ObLSID &ls_id,
                                const storage::ObDDLMacroBlockRedoInfo &redo_info,
                                const blocksstable::MacroBlockId &macro_block_id,
                                ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !redo_info.is_valid() || !macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(redo_info), K(macro_block_id));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(macro_block_id))) {
    LOG_WARN("inc reference count failed", K(ret), K(macro_block_id));
  } else {
    redo_info_ = redo_info;
    ls_id_ = ls_id;
    macro_block_id_ = macro_block_id;
    tablet_handle_ = tablet_handle;
  }
  return ret;
}

void ObDDLIncRedoClogCb::try_release()
{
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    is_data_buffer_freed_ = true;
  }
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncRedoClogCb *cb = this;
    ob_delete(cb);
  }
}

int ObDDLIncRedoClogCb::on_success()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObDDLIncRedoClogCb::on_success", 1 * 1000 * 1000); // 1s
  ObDDLMacroBlock macro_block;
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    if (is_data_buffer_freed_) {
      LOG_INFO("data buffer is freed, do not need to callback");
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_block_id_))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_block_id_));
    } else {
      macro_block.block_type_ = redo_info_.block_type_;
      macro_block.logic_id_ = redo_info_.logic_id_;
      macro_block.scn_ = __get_scn();
      macro_block.buf_ = redo_info_.data_buffer_.ptr();
      macro_block.size_ = redo_info_.data_buffer_.length();
      macro_block.ddl_start_scn_ = redo_info_.start_scn_;
      macro_block.table_key_ = redo_info_.table_key_;
      macro_block.end_row_id_ = redo_info_.end_row_id_;
      macro_block.trans_id_ = redo_info_.trans_id_;
      const int64_t snapshot_version = redo_info_.table_key_.get_snapshot_version();
      const uint64_t data_format_version = redo_info_.data_format_version_;
      if (OB_FAIL(tablet_handle_.get_obj()->set_macro_block(macro_block, snapshot_version, data_format_version))) {
        LOG_WARN("fail to set macro block", K(ret));
      }
    }
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();

  return OB_SUCCESS; // force return success
}

int ObDDLIncRedoClogCb::on_failure()
{
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

ObDDLIncCommitClogCb::ObDDLIncCommitClogCb()
  : is_inited_(false), ls_id_(), log_basic_(), scn_(SCN::min_scn())
{
}

int ObDDLIncCommitClogCb::init(const share::ObLSID &ls_id, const ObDDLIncLogBasic &log_basic)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(log_basic));
  } else {
    ls_id_ = ls_id;
    log_basic_ = log_basic;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLIncCommitClogCb::on_success()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObDDLIncCommitClogCb::on_success", 1 * 1000 * 1000); // 1s
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(log_basic_.get_tablet_id()));
  } else {
    const bool is_sync = false;
    (void)ls->tablet_freeze(log_basic_.get_tablet_id(), is_sync);
    if (log_basic_.get_lob_meta_tablet_id().is_valid()) {
      (void)ls->tablet_freeze(log_basic_.get_lob_meta_tablet_id(), is_sync);
    }
  }

  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();

  return OB_SUCCESS;
}

int ObDDLIncCommitClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  status_.set_state(STATE_FAILED);
  try_release();

  return OB_SUCCESS;
}

void ObDDLIncCommitClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncCommitClogCb *cb = this;
    ob_delete(cb);
  }
}

} // namespace storage
} // namespace oceanbase
