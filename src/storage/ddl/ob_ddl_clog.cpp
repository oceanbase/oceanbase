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

#include "ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{

using namespace blocksstable;
using namespace share;

namespace storage
{

ObDDLClogCbStatus::ObDDLClogCbStatus()
  : the_other_release_this_(false), state_(ObDDLClogState::STATE_INIT), ret_code_(OB_SUCCESS)
{
}

bool ObDDLClogCbStatus::try_set_release_flag()
{
  return ATOMIC_BCAS(&the_other_release_this_, false, true);
}

ObDDLClogCb::ObDDLClogCb()
  : status_()
{
}

int ObDDLClogCb::on_success()
{
  status_.set_state(STATE_SUCCESS);
  try_release();
  return OB_SUCCESS;
}

int ObDDLClogCb::on_failure()
{
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

void ObDDLClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    op_free(this);
  }
}

ObDDLMacroBlockClogCb::ObDDLMacroBlockClogCb()
  : is_inited_(false), status_(), ls_id_(), redo_info_(), macro_block_id_(),
    arena_("ddl_clog_cb", OB_MALLOC_BIG_BLOCK_SIZE), data_buffer_lock_(), is_data_buffer_freed_(false)
{

}

int ObDDLMacroBlockClogCb::init(const share::ObLSID &ls_id,
                                const blocksstable::ObDDLMacroBlockRedoInfo &redo_info,
                                const blocksstable::MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !redo_info.is_valid() || !macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(redo_info), K(macro_block_id));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = arena_.alloc(redo_info.data_buffer_.length()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(redo_info.data_buffer_.length()));
    } else {
      redo_info_.data_buffer_.assign(const_cast<char *>(redo_info.data_buffer_.ptr()), redo_info.data_buffer_.length());
      redo_info_.block_type_ = redo_info.block_type_;
      redo_info_.logic_id_ = redo_info.logic_id_;
      redo_info_.table_key_ = redo_info.table_key_;
      redo_info_.start_log_ts_ = redo_info.start_log_ts_;
      ls_id_ = ls_id;
      macro_block_id_ = macro_block_id;
    }
  }
  return ret;
}

void ObDDLMacroBlockClogCb::try_release()
{
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    is_data_buffer_freed_ = true;
  }
  if (status_.try_set_release_flag()) {
  } else {
    op_free(this);
  }
}

int ObDDLMacroBlockClogCb::on_success()
{
  int ret = OB_SUCCESS;
  ObDDLMacroBlock macro_block;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    if (is_data_buffer_freed_) {
      LOG_INFO("data buffer is freed, do not need to callback");
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, redo_info_.table_key_.get_tablet_id(), tablet_handle))) {
      LOG_WARN("get tablet handle failed", K(ret), K(redo_info_.table_key_));
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_block_id_))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_block_id_));
    } else {
      macro_block.block_type_ = redo_info_.block_type_;
      macro_block.logic_id_ = redo_info_.logic_id_;
      macro_block.log_ts_ = __get_ts_ns();
      macro_block.buf_ = redo_info_.data_buffer_.ptr();
      macro_block.size_ = redo_info_.data_buffer_.length();
      macro_block.ddl_start_log_ts_ = redo_info_.start_log_ts_;
      if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(tablet_handle.get_obj(), macro_block))) {
        LOG_WARN("set macro block into ddl kv failed", K(ret), K(tablet_handle), K(macro_block));
      }
    }
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();
  return OB_SUCCESS; // force return success
}

int ObDDLMacroBlockClogCb::on_failure()
{
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

DEFINE_SERIALIZE(ObDDLClogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos, static_cast<int64_t>(ddl_clog_type_)))) {
    LOG_WARN("fail to serialize ObDDLClogHeader", K(ret));
  } else {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObDDLClogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  int64_t log_type = 0;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &log_type))) {
    LOG_WARN("fail to deserialize ObDDLClogHeader", K(ret));
  } else {
    ddl_clog_type_ = static_cast<ObDDLClogType>(log_type);
    pos = tmp_pos;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObDDLClogHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(static_cast<int64_t>(ddl_clog_type_));
  return size;
}

ObDDLStartLog::ObDDLStartLog()
  : table_key_(), cluster_version_(0), execution_id_(0)
{
}

int ObDDLStartLog::init(const ObITable::TableKey &table_key, const int64_t cluster_version, const int64_t execution_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_key.is_valid() || execution_id < 0 || cluster_version_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(execution_id), K(cluster_version));
  } else {
    table_key_ = table_key;
    cluster_version_ = cluster_version;
    execution_id_ = execution_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLStartLog, table_key_, cluster_version_, execution_id_);

ObDDLRedoLog::ObDDLRedoLog()
  : redo_info_()
{
}

int ObDDLRedoLog::init(const blocksstable::ObDDLMacroBlockRedoInfo &redo_info)
{
  int ret = OB_SUCCESS;
  if (!redo_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(redo_info));
  } else {
    redo_info_ = redo_info;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLRedoLog, redo_info_);

ObDDLPrepareLog::ObDDLPrepareLog()
  : table_key_(), start_log_ts_(0)
{
}

int ObDDLPrepareLog::init(const ObITable::TableKey &table_key,
                         const int64_t start_log_ts)
{
  int ret = OB_SUCCESS;
  if (!table_key.is_valid() || start_log_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_log_ts));
  } else {
    table_key_ = table_key;
    start_log_ts_ = start_log_ts;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLPrepareLog, table_key_, start_log_ts_);

ObDDLCommitLog::ObDDLCommitLog()
  : table_key_(), start_log_ts_(0), prepare_log_ts_(0)
{
}

int ObDDLCommitLog::init(const ObITable::TableKey &table_key,
                         const int64_t start_log_ts,
                         const int64_t prepare_log_ts)
{
  int ret = OB_SUCCESS;
  if (!table_key.is_valid() || start_log_ts <= 0 || prepare_log_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_log_ts), K(prepare_log_ts));
  } else {
    table_key_ = table_key;
    start_log_ts_ = start_log_ts;
    prepare_log_ts_ = prepare_log_ts;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLCommitLog, table_key_, start_log_ts_, prepare_log_ts_);

ObTabletSchemaVersionChangeLog::ObTabletSchemaVersionChangeLog()
  : tablet_id_(), schema_version_(-1)
{
}

int ObTabletSchemaVersionChangeLog::init(const ObTabletID &tablet_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid() || schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id), K(schema_version));
  } else {
    tablet_id_ = tablet_id;
    schema_version_ = schema_version;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletSchemaVersionChangeLog, tablet_id_, schema_version_);

OB_SERIALIZE_MEMBER(ObDDLBarrierLog, ls_id_, hidden_tablet_ids_);

}
}
