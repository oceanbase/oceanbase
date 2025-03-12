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
#include "ob_lob_access_param.h"
#include "storage/tx_storage/ob_access_service.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace storage
{

ObLobAccessParam::~ObLobAccessParam()
{
  if (OB_NOT_NULL(dml_base_param_)) {
    if (OB_NOT_NULL(dml_base_param_->store_ctx_guard_)) {
      dml_base_param_->store_ctx_guard_->~ObStoreCtxGuard();
    }
    dml_base_param_->~ObDMLBaseParam();
  }
}

int ObLobAccessParam::assign(const ObLobAccessParam& other)
{
  int ret = OB_SUCCESS;
  this->tmp_allocator_ = other.tmp_allocator_;
  this->allocator_ = other.allocator_;
  this->tx_desc_ = other.tx_desc_;
  // use assign
  // this->snapshot_ = other.snapshot_;
  this->tx_id_ = other.tx_id_;
  this->sql_mode_ = other.sql_mode_;
  this->dml_base_param_ = other.dml_base_param_;

  this->tenant_id_ = other.tenant_id_;
  this->src_tenant_id_ = other.src_tenant_id_;
  this->ls_id_ = other.ls_id_;
  this->tablet_id_ = other.tablet_id_;
  this->lob_meta_tablet_id_ = other.lob_meta_tablet_id_;
  this->lob_piece_tablet_id_ = other.lob_piece_tablet_id_;

  this->coll_type_ = other.coll_type_;
  this->lob_locator_ = other.lob_locator_;
  this->lob_common_ = other.lob_common_;
  this->lob_data_ = other.lob_data_;
  this->byte_size_ = other.byte_size_;
  this->handle_size_ = other.handle_size_;

  this->timeout_ = other.timeout_;
  this->fb_snapshot_ = other.fb_snapshot_;

  this->offset_ = other.offset_;
  this->len_ = other.len_;

  this->parent_seq_no_ = other.parent_seq_no_;
  this->seq_no_st_ = other.seq_no_st_;
  this->used_seq_cnt_ = other.used_seq_cnt_;
  this->total_seq_cnt_ = other.total_seq_cnt_;
  this->checksum_ = other.checksum_;
  this->update_len_ = other.update_len_;
  this->op_type_ = other.op_type_;

  this->is_total_quantity_log_ = other.is_total_quantity_log_;
  this->read_latest_ = other.read_latest_;
  this->scan_backward_ = other.scan_backward_;
  this->is_fill_zero_ = other.is_fill_zero_;
  this->from_rpc_ = other.from_rpc_;
  this->inrow_read_nocopy_ = other.inrow_read_nocopy_;
  this->is_store_char_len_ = other.is_store_char_len_;
  this->need_read_latest_ = other.need_read_latest_;

  this->inrow_threshold_ = other.inrow_threshold_;
  this->schema_chunk_size_ = other.schema_chunk_size_;

  this->ext_info_log_ = other.ext_info_log_;
  this->access_ctx_ = other.access_ctx_;
  this->addr_ = other.addr_;
  this->lob_id_geneator_ = other.lob_id_geneator_;

  if (OB_FAIL(this->snapshot_.assign(other.snapshot_))) {
    LOG_WARN("assign snapshot fail", K(ret), K(other));
  }
  return ret;
}

int ObLobAccessParam::prepare()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), KPC(this));
  } else if (OB_FAIL(set_lob_locator(lob_locator_))) {
    LOG_WARN("set_lob_locator fail", K(ret), KPC(this));
  }
  return ret;
}

int ObLobAccessParam::set_lob_locator(common::ObLobLocatorV2 *lob_locator)
{
  int ret = OB_SUCCESS;
  ObString disk_locator;
  if (OB_ISNULL(lob_locator)) {
    // do nothing
  } else if (!lob_locator->has_lob_header()) {
    // do nothing
  } else if (!lob_locator->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob locator is invalid", K(ret), KPC(lob_locator));
  } else if (!(lob_locator->is_lob_disk_locator() || lob_locator->is_persist_lob() || lob_locator->is_full_temp_lob())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob locator type is invalid", K(ret), KPC(lob_locator));
  } else if (OB_FAIL(lob_locator->get_disk_locator(disk_locator))) {
    LOG_WARN("failed to get lob common from lob locator", K(ret), KPC(lob_locator));
  } else {
    lob_common_ = reinterpret_cast<ObLobCommon*>(disk_locator.ptr());
    handle_size_ = disk_locator.length();
    lob_locator_ = lob_locator;
    lob_data_ = lob_common_->is_init_ ? reinterpret_cast<ObLobData*>(lob_common_->buffer_) : nullptr;
  }
  return ret;
}

int64_t ObLobAccessParam::get_schema_chunk_size() const
{
  uint64_t chunk_size = 0;
  if (0 == schema_chunk_size_ || schema_chunk_size_ > ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE) {
    chunk_size = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
  } else {
    chunk_size = schema_chunk_size_;
  }
  return chunk_size;
}

bool ObLobAccessParam::has_store_chunk_size() const
{
  bool bres = false;
  if (OB_ISNULL(lob_common_)) {
  } else if (lob_common_->in_row_ || ! lob_common_->is_init_) {
  } else if (OB_ISNULL(lob_data_)) {
  } else {
    bres = true;
  }
  return bres;
}

// chunk size can be changed online.
// that means lob data that has been writed may have different chunk size with schema
// so here should get chunk size according context
int ObLobAccessParam::get_store_chunk_size(int64_t &chunk_size) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_common_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_common_ is null", KR(ret), KPC(this));
  } else if (lob_common_->in_row_ || ! lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_common_ is not outrow", KR(ret), KPC(lob_common_), KPC(this));
  } else if (OB_ISNULL(lob_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_data_ is null", KR(ret), KPC(lob_common_), KPC(this));
  } else {
    ObLobDataOutRowCtx *outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(lob_data_->buffer_);
    chunk_size = outrow_ctx->get_real_chunk_size();
  }
  return ret;
}

int64_t ObLobAccessParam::get_inrow_threshold() const
{
  int64_t res = inrow_threshold_;
  if (res < OB_MIN_LOB_INROW_THRESHOLD || res > OB_MAX_LOB_INROW_THRESHOLD) {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid inrow threshold, use default inrow threshold", K(res));
    res = OB_DEFAULT_LOB_INROW_THRESHOLD;
  }
  return res;
}

int ObLobAccessParam::is_timeout() const
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  if (cur_time > timeout_) {
    ret = OB_TIMEOUT;
    LOG_WARN("query timeout", K(ret), K(cur_time), K(timeout_));
  }
  return ret;
}

bool ObLobAccessParam::has_single_chunk() const
{
  int ret = OB_SUCCESS;
  bool res = false;
  int64_t chunk_size = 0;
  if (! lib::is_mysql_mode() || byte_size_ <= 0) {
    // skip
  } else if (OB_FAIL(get_store_chunk_size(chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", K(ret), KPC(this));
  } else if (byte_size_ <= chunk_size) {
    res = true;
  }
  return res;
}

bool ObLobAccessParam::enable_block_cache() const
{
  bool res = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    res = false;
  } else {
    res = byte_size_ <= tenant_config->lob_enable_block_cache_threshold;
  }
  return res;
}

// 1. from rpc can not remote again
// 2. lob from other tenant also should read by rpc
bool ObLobAccessParam::is_remote() const  { return ! from_rpc_ && addr_.is_valid() && (MYADDR != addr_ || MTL_ID() != tenant_id_); }

int ObLobAccessParam::check_handle_size() const
{
  int ret = OB_SUCCESS;
  ObLobCommon *lob_common = lob_common_;
  int64_t expected_len = sizeof(ObLobCommon);
  if (lob_common->is_init_) {
    expected_len += sizeof(ObLobData);
  }
  if (!lob_common->in_row_) {
    expected_len += sizeof(ObLobDataOutRowCtx);
  } else {
    expected_len += byte_size_;
  }
  if (handle_size_ < expected_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("handle size is too small", K(ret), K(expected_len), KPC(this));
  } else {
    uint64_t max_handle_lob_len = 64 * 1024L * 1024L;
    if (lob_common->use_big_endian_ == 0 && byte_size_ > max_handle_lob_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unable to process little endian lob with length over 64M",
        K(ret), K(lob_common->use_big_endian_), KPC(this));
    }
  }
  return ret;
}

bool ObLobAccessParam::lob_handle_has_char_len_field() const
{
  bool bret = false;
  if (lob_common_ != nullptr && !lob_common_->in_row_) {
    if (handle_size_ >= ObLobConstants::LOB_OUTROW_FULL_SIZE) {
      bret = true;
    } else {
      LOG_INFO("old old data", KPC(this));
    }
  }
  return bret;
}

bool ObLobAccessParam::lob_handle_has_char_len() const
{
  bool bret = false;
  if (lob_common_ != nullptr && !lob_common_->in_row_ && handle_size_ >= ObLobConstants::LOB_OUTROW_FULL_SIZE) {
    char *ptr = reinterpret_cast<char*>(lob_common_);
    uint64_t *len = reinterpret_cast<uint64_t*>(ptr + ObLobConstants::LOB_WITH_OUTROW_CTX_SIZE);
    if (*len != UINT64_MAX) {
      bret = true;
    }
  }
  return bret;
}

int64_t* ObLobAccessParam::get_char_len_ptr() const
{
  char *ptr = reinterpret_cast<char*>(lob_common_);
  return reinterpret_cast<int64_t*>(ptr + ObLobConstants::LOB_WITH_OUTROW_CTX_SIZE);
}

int ObLobAccessParam::update_handle_data_size(const ObLobMetaInfo *old_info, const ObLobMetaInfo *new_info)
{
  int ret = OB_SUCCESS;
  // update byte size
  if (nullptr != old_info) {
    this->lob_data_->byte_size_ -= old_info->byte_len_;
  }
  if (nullptr != new_info) {
    this->lob_data_->byte_size_ += new_info->byte_len_;
  }
  this->byte_size_ = this->lob_data_->byte_size_;

  // update char_len if has char_len
  int64_t *char_len_ptr = nullptr;
  int64_t char_len = 0;
  if (this->lob_handle_has_char_len()) {
    char_len_ptr = this->get_char_len_ptr();
    if (nullptr != old_info) {
      *char_len_ptr = *char_len_ptr - old_info->char_len_;
    }
    if (nullptr != new_info) {
      *char_len_ptr = *char_len_ptr + new_info->char_len_;
    }

    if (*char_len_ptr < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("char_len is invalid after inc", K(ret), K(*char_len_ptr), KPC(this), KPC(old_info), KPC(new_info));
    } else {
      // just for debug
      char_len = *char_len_ptr;
    }
  }
  LOG_DEBUG("update handle success", K(ret), K_(byte_size), KP(char_len_ptr), K(char_len), K_(lob_data), KPC(old_info), KPC(new_info));
  return ret;
}

/**
 *  insert
 *    modified_len is total insert lob data byte len
 *  delete
 *    modified_len is total insert lob data byte len
 *  update
 *    update will be changed to delete + insert
 *    and insert lob data byte len is stored in update_len_
 *  partial update
 *    partial update may be delete/insert/update lob meta
 *    so modified_len sholud make sure each modify use distinct seq_no
*/
int ObLobAccessParam::init_seq_no(const uint64_t modified_len)
{
  int ret = OB_SUCCESS;
  int64_t need_seq_cnt = 0;
  int64_t store_chunk_size = 0;
  // use store chunk size for erase, append, partial update
  if (this->seq_no_st_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("seq_no has been inited", K(ret), KPC(this));
  } else if (OB_FAIL(get_store_chunk_size(store_chunk_size))) {
    LOG_WARN("get_store_chunk_size fail", K(ret), KPC(this));
  } else if (store_chunk_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store_chunk_size is invalid", K(ret), KPC(this));
  } else {
    // pre-calc seq_no_cnt and init seq_no_st
    // for insert, most oper len/128K + 2
    // for erase, most oper len/128K + 2
    // for append, most oper len/256K + 1
    // for sql update, calc erase+insert

    // calc seq_no count that modified len need
    need_seq_cnt += (modified_len / (store_chunk_size / 2) + 2);

    // update_len_ is only used by update in dml
    // and is new lob data byte len that will be inserted
    need_seq_cnt += (update_len_ / (this->get_schema_chunk_size() / 2) + 2);
  }

  if (OB_SUCC(ret)) {
    if (nullptr != tx_desc_) {
      if (OB_FAIL(tx_desc_->get_and_inc_tx_seq(this->parent_seq_no_.get_branch(),
                                                      need_seq_cnt,
                                                      seq_no_st_))) {
        LOG_WARN("get and inc tx seq failed", K(ret), K(need_seq_cnt), KPC(this));
      }
    } else {
      // do nothing, for direct load has no tx desc, do not use seq no
      LOG_DEBUG("tx_desc is null", KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    used_seq_cnt_ = 0;
    total_seq_cnt_ = need_seq_cnt;
    LOG_DEBUG("init lob seq no success", K_(op_type), K(modified_len), K_(update_len),  K(store_chunk_size), K_(schema_chunk_size), K_(seq_no_st), K_(total_seq_cnt));
  }

  return ret;
}

int ObLobAccessParam::init_out_row_ctx(uint64_t modified_len)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtx *out_row_ctx = nullptr;
  if (OB_ISNULL(out_row_ctx = get_data_outrow_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data outrow ctx is null", K(ret), KPC(this));
  } else if (! this->seq_no_st_.is_valid() && OB_FAIL(init_seq_no(modified_len))) {
    LOG_WARN("init_seq_no fail", K(ret), KPC(this));
  } else {
    // if op is not SQL, it must be partial update
    // so also means not full
    if (ObLobDataOutRowCtx::OpType::SQL == op_type_) {
      out_row_ctx->is_full_ = 1;
    } else {
      out_row_ctx->is_full_ = 0;
    }

    out_row_ctx->seq_no_st_ = this->seq_no_st_.cast_to_int();
    out_row_ctx->offset_ = this->offset_;
    out_row_ctx->check_sum_ = this->checksum_;
    out_row_ctx->seq_no_cnt_ = this->used_seq_cnt_;
    out_row_ctx->del_seq_no_cnt_ = this->used_seq_cnt_; // for sql update, first delete then insert
    out_row_ctx->op_ = static_cast<uint8_t>(op_type_);
    out_row_ctx->modified_len_ = modified_len;
    out_row_ctx->first_meta_offset_ = 0;

    LOG_DEBUG("init lob out_row_ctx success", KPC_(lob_data), KPC(out_row_ctx), K(modified_len), K_(op_type), K_(seq_no_st));
  }
  return ret;
}

int ObLobAccessParam::update_out_row_ctx(const ObLobMetaInfo *old_info, const ObLobMetaInfo& new_info)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtx *out_row_ctx = nullptr;
  if (OB_ISNULL(out_row_ctx = get_data_outrow_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data outrow ctx is null", K(ret), KPC(this));
  } else if (ObLobDataOutRowCtx::OpType::DIFF == out_row_ctx->op_) {
    // when op is DIFF, that is json parital update
    // out_row_ctx will be used to record json diff ext log
    // no need check_sum and modified_len of updated lob data
    // and seq_no_cnt is not needed actually, this is just for debug
    out_row_ctx->seq_no_cnt_ = this->used_seq_cnt_;
    LOG_DEBUG("update outrow ctx success", K_(op_type), KPC_(lob_data), KPC(out_row_ctx), KPC(old_info), K(new_info));
  } else {
    // update seq no
    // it sholud be update when each lob meta table row is modified (insert/delete/update)
    out_row_ctx->seq_no_cnt_ = this->used_seq_cnt_;

    // currently checksum and modified_len is not used by obcdc expect DIFF situation
    // update checksum
    ObBatchChecksum bc;
    if (old_info != nullptr) {
      bc.fill(&out_row_ctx->check_sum_, sizeof(out_row_ctx->check_sum_));
      bc.fill(&old_info->lob_id_, sizeof(old_info->lob_id_));
      bc.fill(old_info->seq_id_.ptr(), old_info->seq_id_.length());
      bc.fill(old_info->lob_data_.ptr(), old_info->lob_data_.length());
      out_row_ctx->check_sum_ = bc.calc();
      bc.reset();
    }
    bc.fill(&out_row_ctx->check_sum_, sizeof(out_row_ctx->check_sum_));
    bc.fill(&new_info.lob_id_, sizeof(new_info.lob_id_));
    bc.fill(new_info.seq_id_.ptr(), new_info.seq_id_.length());
    bc.fill(new_info.lob_data_.ptr(), new_info.lob_data_.length());
    out_row_ctx->check_sum_ = bc.calc();

    // update modified_len
    int64_t old_meta_len = (old_info == nullptr) ? 0 : old_info->byte_len_;
    int64_t new_meta_len = (new_info.byte_len_);
    out_row_ctx->modified_len_ += std::abs(new_meta_len - old_meta_len);

    LOG_DEBUG("update outrow ctx success", K_(op_type), KPC_(lob_data), KPC(out_row_ctx), K(new_meta_len), K(old_meta_len), KPC(old_info), K(new_info));
  }
  return ret;
}

int ObLobAccessParam::get_tx_read_snapshot(ObLobLocatorV2 &locator, transaction::ObTxReadSnapshot &read_snapshot)
{
  int ret = OB_SUCCESS;
  ObMemLobExternHeader *extern_header = nullptr;
  if (! locator.is_persist_lob() || locator.has_inrow_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not outrow persit lob", K(ret), K(locator));
  } else if (OB_FAIL(locator.get_extern_header(extern_header))) {
    LOG_WARN("failed to get extern header", K(ret), K(locator));
  } else if (extern_header->flags_.has_read_snapshot_) {
    ObString read_snapshot_data;
    int64_t read_snapshot_data_pos = 0;
    if (OB_FAIL(locator.get_read_snapshot_data(read_snapshot_data))) {
      LOG_WARN("failed to get read_snapshot_data", K(ret), K(locator));
    } else if (OB_FAIL(read_snapshot.deserialize_for_lob(read_snapshot_data.ptr(), read_snapshot_data.length(), read_snapshot_data_pos))) {
      LOG_WARN("failed to deserialize read_snapshot_data", K(ret), K(locator));
    }
  // for compatibility
  // beacuase old observer (version < 424) does not produce ObTxReadSnapshot
  // so still use has_tx_info when upgrage observer (eg: 423 --> 424)
  } else if (extern_header->flags_.has_tx_info_) {
    ObMemLobTxInfo *tx_info = nullptr;
    ObMemLobLocationInfo *location_info = nullptr;
    if (OB_FAIL(locator.get_tx_info(tx_info))) {
      LOG_WARN("failed to get tx info", K(ret), K(locator));
    } else if (OB_FAIL(locator.get_location_info(location_info))) {
      LOG_WARN("failed to get location info", K(ret), K(locator));
    } else if (OB_FAIL(read_snapshot.build_snapshot_for_lob(
        tx_info->snapshot_version_, tx_info->snapshot_tx_id_, tx_info->snapshot_seq_, share::ObLSID(location_info->ls_id_)))) {
      LOG_WARN("build_snapshot_for_lob fail", K(ret), KPC(tx_info), KPC(location_info), K(locator));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx info not found", K(ret), K(locator));
  }

  if (OB_SUCC(ret) && extern_header->flags_.has_retry_info_) {
    ObMemLobRetryInfo *retry_info = nullptr;
    if (OB_FAIL(locator.get_retry_info(retry_info))) {
      LOG_WARN("failed to get retry info", K(ret), K(locator));
    } else if (retry_info->read_latest_) {
      this->read_latest_ = retry_info->read_latest_;
    }
  }
  return ret;
}

int ObLobAccessParam::set_tx_read_snapshot(ObLobLocatorV2 &locator)
{
  int ret = OB_SUCCESS;
  transaction::ObTxReadSnapshot read_snapshot;
  if (OB_FAIL(get_tx_read_snapshot(locator, read_snapshot))) {
    LOG_WARN("get tx read snapshot fail", K(ret), KPC(this), K(locator));

  // 1. if tx_desc is null , use read snapshot in locator
  // 2. if tx_id in tx_desc is equal to tx_id in locator read snapshot, use read snapshot in locator
  // 3. if tx_id in locator read snapshot is zero and snapshot version is valid, use read snapshot in locator
  } else if (OB_ISNULL(this->tx_desc_)
      || this->tx_desc_->get_tx_id() == read_snapshot.tx_id()  // read in same tx
      || read_snapshot.is_not_in_tx_snapshot()) { // read not in tx
    if (OB_FAIL(this->snapshot_.assign(read_snapshot))) {
      LOG_WARN("assign snapshot fail", K(ret), K(read_snapshot));
    }
  } else {
    // if tx_desc is not null
    //    and tx_id in tx_desc is not equal to tx_id in locator read snapshot
    //    and tx_id in locator read snapshot is not zero,
    // that means lob locator is old but tx_id in tx_desc is zero, that means transaction mab be commited.
    // so return not support because the data of the current transaction is not guaranteed to be read
    // and this situation happens in jdbc read wirte with lob locator
    // for example:
    //      begin;
    //      select lob_col into lob_var from test for update;  // will return lob locator to lob_var
    //      call dbms_lob.write(lob_var, new_data);
    //      commit;                                            // reset tx_desc and tx_id in tx_desc is zero
    //      call dbms_lob.read(lob_var);                       // should return updated data
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("it is not support for reading lob after tranaction commit", K(ret), K(locator), K(read_snapshot), KPC(this));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "it is not support for reading lob after tranaction commit, please re-select lob locator");
  }
  return ret;
}

} // storage
} // oceanbase
