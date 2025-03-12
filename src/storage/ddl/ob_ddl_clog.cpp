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
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
namespace oceanbase
{

using namespace blocksstable;
using namespace share;
using namespace common;

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

ObDDLStartClogCb::ObDDLStartClogCb()
  : is_inited_(false), status_(), lock_tid_(0), direct_load_mgr_handle_()
{
}

int ObDDLStartClogCb::init(const ObITable::TableKey &table_key,
                           const uint64_t data_format_version,
                           const int64_t execution_id,
                           ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                           ObDDLKvMgrHandle &lob_kv_mgr_handle,
                           ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
                           const uint32_t lock_tid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || execution_id < 0 || data_format_version <= 0
          || 0 == lock_tid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(execution_id), K(data_format_version), K(lock_tid));
  } else if (OB_FAIL(direct_load_mgr_handle_.assign(direct_load_mgr_handle))) {
    LOG_WARN("assign direct load mgr handle failed", K(ret));
  } else {
    table_key_ = table_key;
    data_format_version_ = data_format_version;
    execution_id_ = execution_id;
    lock_tid_ = lock_tid;
    ddl_kv_mgr_handle_ = ddl_kv_mgr_handle;
    lob_kv_mgr_handle_ = lob_kv_mgr_handle;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLStartClogCb::on_success()
{
  int ret = OB_SUCCESS;
  const SCN &start_scn = __get_scn();
  bool unused_brand_new = false;
  ObTabletFullDirectLoadMgr *data_direct_load_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(data_direct_load_mgr
      = (direct_load_mgr_handle_.get_full_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(table_key_));
  } else if (OB_FAIL(data_direct_load_mgr->start_nolock(table_key_, start_scn, data_format_version_,
      execution_id_, SCN::min_scn()/*checkpoint_scn*/, ddl_kv_mgr_handle_, lob_kv_mgr_handle_))) {
    LOG_WARN("failed to start ddl in cb", K(ret), K(table_key_), K(start_scn), K(execution_id_));
  }
  if (OB_NOT_NULL(data_direct_load_mgr)) {
    data_direct_load_mgr->unlock(lock_tid_);
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();
  return OB_SUCCESS; // force return success
}

int ObDDLStartClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(direct_load_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(table_key_), K(execution_id_));
  } else {
    direct_load_mgr_handle_.get_full_obj()->unlock(lock_tid_);
  }
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

void ObDDLStartClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    op_free(this);
  }
}

ObDDLMacroBlockClogCb::ObDDLMacroBlockClogCb()
  : is_inited_(false), status_(), ls_id_(), macro_block_id_(),
    data_buffer_lock_(), is_data_buffer_freed_(false), ddl_macro_block_(), snapshot_version_(0), data_format_version_(0), with_cs_replica_(false)
{

}

ObDDLMacroBlockClogCb::~ObDDLMacroBlockClogCb()
{
  int ret = OB_SUCCESS;
  if (macro_block_id_.is_valid() && OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_block_id_))) {
    LOG_ERROR("dec ref failed", K(ret), K(macro_block_id_), K(common::lbt()));
  }
  macro_block_id_.reset();
}

int ObDDLMacroBlockClogCb::init(const share::ObLSID &ls_id,
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
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(macro_block_id))) {
    LOG_WARN("inc reference count failed", K(ret), K(macro_block_id));
  } else {
    ls_id_ = ls_id;
    macro_block_id_ = macro_block_id;
    tablet_handle_ = tablet_handle;
    snapshot_version_ = redo_info.table_key_.get_snapshot_version();
    data_format_version_ = redo_info.data_format_version_;
    with_cs_replica_ = redo_info.with_cs_replica_;
    if (OB_FAIL(ddl_macro_block_.block_handle_.set_block_id(macro_block_id_))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_block_id_));
    } else if (OB_FAIL(ddl_macro_block_.set_data_macro_meta(macro_block_id_,
                                                            redo_info.data_buffer_.ptr(),
                                                            redo_info.data_buffer_.length(),
                                                            redo_info.block_type_))) {
      LOG_WARN("failed to set data macro meta", K(ret), K(redo_info));
    } else {
      ddl_macro_block_.block_type_ = redo_info.block_type_;
      ddl_macro_block_.logic_id_ = redo_info.logic_id_;
      ddl_macro_block_.ddl_start_scn_ = redo_info.start_scn_;
      ddl_macro_block_.table_key_ = redo_info.table_key_;
      ddl_macro_block_.end_row_id_ = redo_info.end_row_id_;
      ddl_macro_block_.merge_slice_idx_ = redo_info.merge_slice_idx_;
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
  bool is_major_sstable_exist = false;
  ObTablet *tablet = nullptr;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr", K(ret));
  } else if (ObDDLUtil::use_idempotent_mode(data_format_version_)) {
    // do nothing, can NOT get direct load mgr
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
        ls_id_, tablet->get_tablet_meta().tablet_id_, true/*is_full_direct_load*/, direct_load_mgr_handle, is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("major sstable already exist", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
    } else {
      LOG_WARN("get tablet mgr failed", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (with_cs_replica_ && ddl_macro_block_.table_key_ .is_column_store_sstable()) {
    LOG_INFO("[CS-Replica] skip replay cs replica redo clog in leader", K(ret), K_(with_cs_replica), K_(ddl_macro_block));
  } else if (FALSE_IT(ddl_macro_block_.scn_ = __get_scn())) {
  } else if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(
      tablet, ddl_macro_block_, snapshot_version_,
      data_format_version_, direct_load_mgr_handle))) {
    LOG_WARN("set macro block into ddl kv failed", K(ret), KPC(tablet), K(ddl_macro_block_),
            K(snapshot_version_), K(data_format_version_));
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

ObDDLCommitClogCb::ObDDLCommitClogCb()
  : is_inited_(false), status_(), ls_id_(), tablet_id_(), start_scn_(SCN::min_scn()), lock_tid_(0), direct_load_mgr_handle_(), lob_direct_load_mgr_handle_()
{

}

int ObDDLCommitClogCb::init(const share::ObLSID &ls_id,
                            const common::ObTabletID &tablet_id,
                            const share::SCN &start_scn,
                            const uint32_t lock_tid,
                            ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
                            ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !start_scn.is_valid_and_not_min()
      || 0 == lock_tid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(start_scn), K(lock_tid));
  } else if (OB_FAIL(direct_load_mgr_handle_.assign(direct_load_mgr_handle))) {
    LOG_WARN("assign handle failed", K(ret));
  } else if (OB_FAIL(lob_direct_load_mgr_handle_.assign(lob_direct_load_mgr_handle))) {
    LOG_WARN("assign handle failed", K(ret));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    start_scn_ = start_scn;
    lock_tid_ = lock_tid;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLCommitClogCb::on_success()
{
  int ret = OB_SUCCESS;
  ObTabletFullDirectLoadMgr *data_direct_load_mgr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(data_direct_load_mgr
      = direct_load_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(tablet_id_));
  } else {
    const SCN commit_scn = __get_scn();
    data_direct_load_mgr->set_commit_scn_nolock(commit_scn);
    if (lob_direct_load_mgr_handle_.is_valid()) {
      lob_direct_load_mgr_handle_.get_full_obj()->set_commit_scn_nolock(commit_scn);
    }
    data_direct_load_mgr->unlock(lock_tid_);
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();
  return OB_SUCCESS; // force return success
}

int ObDDLCommitClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(direct_load_mgr_handle_.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(tablet_id_));
  } else {
    direct_load_mgr_handle_.get_full_obj()->unlock(lock_tid_);
  }
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

void ObDDLCommitClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    op_free(this);
  }
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
  : table_key_(), data_format_version_(0), execution_id_(-1), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_DDL) /*for compatibility*/,
    lob_meta_tablet_id_(ObDDLClog::COMPATIBLE_LOB_META_TABLET_ID)
{
}

int ObDDLStartLog::init(
    const ObITable::TableKey &table_key,
    const uint64_t data_format_version,
    const int64_t execution_id,
    const ObDirectLoadType direct_load_type,
    const ObTabletID &lob_meta_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_key.is_valid() || execution_id < 0 || data_format_version <= 0 || !is_valid_direct_load(direct_load_type)
        || ObDDLClog::COMPATIBLE_LOB_META_TABLET_ID == lob_meta_tablet_id.id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(execution_id), K(data_format_version), K(direct_load_type), K(lob_meta_tablet_id));
  } else {
    table_key_ = table_key;
    data_format_version_ = data_format_version;
    execution_id_ = execution_id;
    direct_load_type_ = direct_load_type;
    lob_meta_tablet_id_ = lob_meta_tablet_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLStartLog, table_key_, data_format_version_, execution_id_, direct_load_type_, lob_meta_tablet_id_);

ObDDLRedoLog::ObDDLRedoLog()
  : redo_info_()
{
}

int ObDDLRedoLog::init(const storage::ObDDLMacroBlockRedoInfo &redo_info)
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

ObDDLCommitLog::ObDDLCommitLog()
  : table_key_(), start_scn_(SCN::min_scn()), lob_meta_tablet_id_(ObDDLClog::COMPATIBLE_LOB_META_TABLET_ID)
{
}

int ObDDLCommitLog::init(const ObITable::TableKey &table_key,
                         const SCN &start_scn,
                         const ObTabletID &lob_meta_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_key.is_valid() || !start_scn.is_valid_and_not_min()
        || ObDDLClog::COMPATIBLE_LOB_META_TABLET_ID == lob_meta_tablet_id.id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_scn), K(lob_meta_tablet_id));
  } else {
    table_key_ = table_key;
    start_scn_ = start_scn;
    lob_meta_tablet_id_ = lob_meta_tablet_id;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLCommitLog, table_key_, start_scn_, lob_meta_tablet_id_);

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
#ifdef OB_BUILD_SHARED_STORAGE
ObDDLFinishLog::ObDDLFinishLog()
  :finish_info_()
{
}

void ObDDLFinishLog::reset()
{
  finish_info_.reset();
}

int ObDDLFinishLog::assign(const storage::ObDDLFinishLogInfo &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(other));
  } else if (OB_FAIL(finish_info_.assign(other))) {
    LOG_WARN("failed to assign value", K(ret));
  }
  return ret;
}

int ObDDLFinishLog::init(int64_t tenant_id,
                         const share::ObLSID ls_id,
                         const ObITable::TableKey &table_key,
                         const char* buf,
                         const int64_t buf_len,
                         const blocksstable::MacroBlockId &macro_block_id,
                         const uint64_t data_format_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid() ||!table_key.is_valid() ||
       nullptr == buf || buf_len <= 0  || !macro_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(ls_id), K(table_key), KP(buf), K(buf_len), K(macro_block_id));
  } else {
    finish_info_.ls_id_ = ls_id;
    finish_info_.table_key_ = table_key;
    finish_info_.data_buffer_.assign_ptr(buf, buf_len);
    finish_info_.macro_block_id_ = macro_block_id;
    finish_info_.data_format_version_ = data_format_version;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObDDLFinishLog, finish_info_);

ObDDLFinishClogCb::ObDDLFinishClogCb():
is_inited_(false), status_(), finish_log_()
{
}

int ObDDLFinishClogCb::init(const ObDDLFinishLog &finish_log)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init finish log cb twice", K(ret));
  } else if (!finish_log.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(finish_log));
  } else if (OB_FAIL(finish_log_.assign(finish_log.get_log_info()))) {
    LOG_WARN("failed to assign value", K(ret), K(finish_log));
  } else {
    is_inited_ = true;
  }
  return ret;
}

/*
 * apply only need to clean up ddl kv·
*/
int ObDDLFinishClogCb::on_success()
{
  /* on success do nothing upload and update tablet meta is call in following action*/
  int ret = OB_SUCCESS;
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();
  return OB_SUCCESS;
}

void ObDDLFinishClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    this->~ObDDLFinishClogCb();
    ob_free(this);
  }
}

int ObDDLFinishClogCb::on_failure()
{
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}
#endif

ObTabletSplitInfo::ObTabletSplitInfo()
  : rowkey_allocator_("SplitRangeClog"),
    table_id_(OB_INVALID_ID), lob_table_id_(OB_INVALID_ID),
    schema_version_(0), task_id_(0),
    source_tablet_id_(), dest_tablets_id_(),
    compaction_scn_(0), data_format_version_(0), consumer_group_id_(0),
    can_reuse_macro_block_(false),
    lob_col_idxs_(), parallel_datum_rowkey_list_()
{
}

int ObTabletSplitInfo::assign(const ObTabletSplitInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(info));
  } else if (OB_FAIL(dest_tablets_id_.assign(info.dest_tablets_id_))) {
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(parallel_datum_rowkey_list_.assign(info.parallel_datum_rowkey_list_))) {
    // shallow copy enough.
    LOG_WARN("assign failed", K(ret));
  } else if (OB_FAIL(lob_col_idxs_.assign(info.lob_col_idxs_))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    table_id_            = info.table_id_;
    lob_table_id_        = info.lob_table_id_;
    schema_version_      = info.schema_version_;
    task_id_             = info.task_id_;
    source_tablet_id_    = info.source_tablet_id_;
    compaction_scn_      = info.compaction_scn_;
    data_format_version_ = info.data_format_version_;
    consumer_group_id_   = info.consumer_group_id_;
    can_reuse_macro_block_ = info.can_reuse_macro_block_;
  }
  return ret;
}

bool ObTabletSplitInfo::is_valid() const
{
  bool is_valid = OB_INVALID_ID != table_id_
      && schema_version_ > 0 && task_id_ > 0
      && source_tablet_id_.is_valid() && dest_tablets_id_.count() > 0
      && compaction_scn_ > 0
      && data_format_version_ > 0 && consumer_group_id_ >= 0
      && parallel_datum_rowkey_list_.count() > 0;
  if (!lob_col_idxs_.empty()) {
    is_valid = is_valid && (OB_INVALID_ID != lob_table_id_);
  }
  return is_valid;
}

int ObTabletSplitStartLog::assign(const ObTabletSplitStartLog &log)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(log));
  } else if (OB_FAIL(basic_info_.assign(log.basic_info_))) {
    LOG_WARN("assign failed", K(ret), K(log));
  }
  return ret;
}

int ObTabletSplitFinishLog::assign(const ObTabletSplitFinishLog &log)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(log));
  } else if (OB_FAIL(basic_info_.assign(log.basic_info_))) {
    LOG_WARN("assign failed", K(ret), K(log));
  }
  return ret;
}

int ObTabletFreezeLog::assign(const ObTabletFreezeLog &log)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(log));
  } else {
    tablet_id_ = log.tablet_id_;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTabletSplitInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, table_id_, lob_table_id_, schema_version_,
    task_id_, source_tablet_id_, dest_tablets_id_,
    compaction_scn_, data_format_version_, consumer_group_id_,
    can_reuse_macro_block_, split_sstable_type_, lob_col_idxs_,
    parallel_datum_rowkey_list_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletSplitInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, table_id_, lob_table_id_, schema_version_,
    task_id_, source_tablet_id_, dest_tablets_id_,
    compaction_scn_, data_format_version_, consumer_group_id_,
    can_reuse_macro_block_, split_sstable_type_, lob_col_idxs_);
  if (FAILEDx(ObSplitUtil::deserializ_parallel_datum_rowkey(
      rowkey_allocator_, buf, data_len, pos, parallel_datum_rowkey_list_))) {
    LOG_WARN("deserialzie parallel info failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletSplitInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, table_id_, lob_table_id_, schema_version_,
    task_id_, source_tablet_id_, dest_tablets_id_,
    compaction_scn_, data_format_version_, consumer_group_id_,
    can_reuse_macro_block_, split_sstable_type_, lob_col_idxs_,
    parallel_datum_rowkey_list_);
  return len;
}

OB_SERIALIZE_MEMBER(ObTabletSplitStartLog, basic_info_);
OB_SERIALIZE_MEMBER(ObTabletSplitFinishLog, basic_info_);
OB_SERIALIZE_MEMBER(ObTabletFreezeLog, tablet_id_);

} // namespace storage
} // namespace oceanbase
