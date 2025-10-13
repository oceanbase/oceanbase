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

#include "ob_ext_info_callback.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_freezer.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
const int64_t ObExtInfoCbRegister::OB_EXT_INFO_LOG_BLOCK_MAX_SIZE = 1 * 1024 * 1024 + 500 * 1024;

DEFINE_GET_SERIALIZE_SIZE(ObExtInfoLogHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i8(type_);
  return size;
}

DEFINE_SERIALIZE(ObExtInfoLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("serialize failed", K(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, type_))) {
    LOG_WARN("serialize failed", K(ret), K(pos), K(buf_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExtInfoLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len || 0 > pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("serialize failed", K(ret), K(pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, reinterpret_cast<int8_t*>(&type_)))) {
    LOG_WARN("serialize failed", K(ret), K(pos), K(data_len));
  } else {
    pos = new_pos;
  }
  return ret;
}

void ObExtInfoLogHeader::init(ObObj &obj, const bool is_update) {
  if (obj.is_lob_storage()) {
    if (ObJsonType == obj.get_type()) {
      if (is_update) {
        type_ = OB_JSON_DIFF_EXT_INFO_LOG;
      } else {
        type_ = OB_VALID_OLD_LOB_VALUE_LOG;
      }
    } else {
      type_ = OB_VALID_OLD_LOB_VALUE_LOG;
    }
  }
}

ObJsonDiffLog::~ObJsonDiffLog()
{}

int ObJsonDiffLog::to_string(ObIAllocator &allocator, ObString &result)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator);
  if (diffs_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json diff is empty", KR(ret));
  } else if (OB_FAIL(buffer.append("{\"version\": 1, "))) {
    LOG_WARN("buffer append fail", KR(ret));
  } else if (OB_FAIL(buffer.append("\"diffs\": ["))) { // diffs begin
    LOG_WARN("buffer append fail", KR(ret));
  } else {
    for (int idx = 0; OB_SUCC(ret) && idx < diffs_.count(); ++idx) {
      if (idx > 0 && OB_FAIL(buffer.append(", "))) {
        LOG_WARN("buffer append fail", KR(ret));
      } else if (OB_FAIL(diffs_[idx].print(buffer))) {
        LOG_WARN("print json diff fail", KR(ret), K(idx));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(buffer.append("]"))) { // diffs end
      LOG_WARN("buffer append fail", KR(ret));
    } else if (OB_FAIL(buffer.append("}"))) {
      LOG_WARN("buffer append fail", KR(ret));
    } else {
      buffer.get_result_string(result);
    }
  }
  return ret;
}

int ObJsonDiffLog::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(diff_header_.deserialize(buf, data_len, pos))) {
    LOG_WARN("deserialize json diff header fail", KR(ret), K(data_len), K(pos));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < diff_header_.cnt_; ++j) {
      ObJsonDiff json_diff;
      if (OB_FAIL(json_diff.deserialize(buf, data_len, pos))) {
        LOG_WARN("deserialize json diff header fail", KR(ret), K(data_len), K(pos));
      } else if (OB_FAIL(diffs_.push_back(json_diff))) {
        LOG_WARN("push fail", KR(ret));
      }
    }
  }
  return ret;
}

ObExtInfoCallback::~ObExtInfoCallback()
{
  if (OB_NOT_NULL(mutator_row_buf_)) {
    allocator_->free(mutator_row_buf_);
    mutator_row_buf_ = nullptr;
    mutator_row_len_ = 0;
  }
}

memtable::MutatorType ObExtInfoCallback::get_mutator_type() const
{
  return memtable::MutatorType::MUTATOR_ROW_EXT_INFO;
}


int ObExtInfoCallback::log_submitted()
{
  return release_resource();
}

int ObExtInfoCallback::release_resource()
{
  int ret = OB_SUCCESS;
  if (nullptr == mutator_row_buf_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is null", K(ret), KPC(this));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), KPC(this));
  } else {
    allocator_->free(mutator_row_buf_);
    mutator_row_buf_ = nullptr;
    mutator_row_len_ = 0;
  }
  return ret;
}

int ObExtInfoCallback::get_redo(memtable::RedoDataNode &redo_node)
{
  int ret = OB_SUCCESS;
  memtable::ObRowData old_row;
  memtable::ObRowData new_row;
  ObTabletID tablet_id;
  if (OB_ISNULL(mutator_row_buf_) || mutator_row_len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mutator_row_buf is empty", K(ret), KP(mutator_row_buf_), K(mutator_row_len_));
  } else {
    new_row.set(mutator_row_buf_, mutator_row_len_);
    redo_node.set(&key_,
                  old_row,
                  new_row,
                  dml_flag_,
                  1, /* modify_count */
                  0, /* acc_checksum */
                  0, /* version */
                  0, /* flag */
                  seq_no_cur_,
                  tablet_id,
                  OB_EXT_INFO_MUTATOR_ROW_COUNT);
    redo_node.set_callback(this);
  }
  return ret;
}

int ObExtInfoCallback::set(
    ObIAllocator &allocator,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObDatumRow datum_row;
  char *buf = nullptr;
  int64_t len = 0;

  dml_flag_ = dml_flag;
  seq_no_cur_ = seq_no_cur;
  lob_id_ = lob_id;
  allocator_ = &(lob_mngr->get_ext_info_log_allocator());

  key_obj_.reset();
  rowkey_.reset();
  key_.reset();
  key_obj_.set_uint64(seq_no_cur_.cast_to_int());
  rowkey_.assign(&key_obj_, OB_EXT_INFO_MUTATOR_ROW_KEY_CNT);
  SMART_VAR(blocksstable::ObRowWriter, row_writer) {
    if (data.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data is empty", K(ret));
    } else if (OB_FAIL(key_.encode(&rowkey_))) {
      LOG_WARN("encode memtable key failed", K(ret), K(rowkey_));
    } else if (OB_NOT_NULL(mutator_row_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mutator_row_buf is not null", K(ret), KP(mutator_row_buf_));
    } else if (OB_FAIL(datum_row.init(allocator, OB_EXT_INFO_MUTATOR_ROW_COUNT))) {
      LOG_WARN("init datum row fail", K(ret));
    } else if (OB_FAIL(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_KEY_IDX].from_obj_enhance(key_obj_))) {
      LOG_WARN("set datum fail", K(ret), K(data));
    } else if (OB_FALSE_IT(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].set_string(data))) {
    } else if (OB_FALSE_IT(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_LOB_ID_IDX].set_string(reinterpret_cast<char*>(&lob_id_), sizeof(lob_id_)))) {
    } else if (OB_FAIL(row_writer.write(OB_EXT_INFO_MUTATOR_ROW_KEY_CNT, datum_row, buf, len))) {
      LOG_WARN("write row data fail", K(ret));
    } else if (OB_ISNULL(mutator_row_buf_ = static_cast<char*>(allocator_->alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mutator_row_buf fail", K(ret), K(len));
    } else {
      MEMCPY(mutator_row_buf_, buf, len);
      mutator_row_len_ = len;
    }
  } // end row_writer
  return ret;
}

ObExtInfoCbRegister::~ObExtInfoCbRegister()
{
  if (OB_NOT_NULL(lob_param_)) {
    lob_param_->~ObLobAccessParam();
    tmp_allocator_.free(lob_param_);
    lob_param_ = nullptr;
  }
  if (OB_NOT_NULL(data_iter_)) {
    // date_iter is alloc from ObLobManager::query
    // that use ob_malloc alloc memory, so here use ob_delete
    data_iter_->reset();
    OB_DELETE(ObLobQueryIter, "unused", data_iter_);
    data_iter_ = nullptr;
  }
}

int ObExtInfoCbRegister::register_cb(
    memtable::ObIMvccCtx *ctx,
    storage::ObStoreCtx &store_ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    ObObj &index_data,
    ObObj &ext_info_data,
    const transaction::ObTxReadSnapshot &snapshot,
    const ObExtInfoLogHeader &header,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ext_info_data_ = ext_info_data;
  timeout_ = timeout;
  header_ = header;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  transaction::ObTxDesc *tx_desc = store_ctx.mvcc_acc_ctx_.tx_desc_;
  const transaction::ObTxSEQ &parent_seq_no = store_ctx.mvcc_acc_ctx_.tx_scn_;
  const share::ObLSID &ls_id = store_ctx.ls_id_;
  ObLobId lob_id;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]get lob manager instance failed.", K(ret));
  } else if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx_desc is null", K(ret));
  } else if (ext_info_data.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is empty", K(ret), K(ext_info_data));
  } else if (OB_ISNULL(mvcc_ctx_ = ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is empty", K(ret), K(ext_info_data));
  } else if (OB_FAIL(get_lob_id(index_data, lob_id))) {
    LOG_WARN("get lob id fail", K(ret), K(header));
  } else if (OB_FAIL(build_data_iter(
        ext_info_data,
        tx_desc,
        parent_seq_no,
        snapshot,
        ls_id,
        tablet_id))) {
    LOG_WARN("build data iter fail", K(ret));
  } else if (FALSE_IT(seq_no_cnt_ = data_size_/OB_EXT_INFO_LOG_BLOCK_MAX_SIZE + 1)) {
  } else if (OB_FAIL(tx_desc->get_and_inc_tx_seq(parent_seq_no.get_branch(),
                                                 seq_no_cnt_,
                                                 seq_no_st_))) {
    LOG_WARN("get and inc tx seq failed", K(ret), K_(seq_no_cnt));
  } else {
    transaction::ObTxSEQ seq_no_cur = seq_no_st_;
    ObString data;
    int cb_cnt = 0;
    while (OB_SUCC(ret) && OB_SUCC(get_data(data))) {
      // need throttle for each append, or may be too fash if has very large lob
      // each data is as most 1.5MB, so not affect performance
      ObLobExtInfoLogThrottleGuard throttle_guard(timeout, &(lob_mngr->get_ext_info_log_throttle_tool()));
      if (OB_FAIL(append_callback_with_retry(store_ctx, dml_flag, seq_no_cur, lob_id, data))) {
        LOG_WARN("set row callback failed", K(ret), K(cb_cnt));
      } else {
        seq_no_cur = seq_no_cur + 1;
        ++cb_cnt;
        LOG_DEBUG("register ext info callback success", K(cb_cnt), K(seq_no_cur));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(seq_no_cnt_ = cb_cnt)) {
    } else if (OB_FAIL(set_index_data(index_data))) {
      LOG_WARN("set_index_data fail", K(ret));
    }
  }
  return ret;
}

int ObExtInfoCbRegister::set_index_data(ObObj &index_data)
{
  int ret = OB_SUCCESS;
  if (is_lob_storage(index_data.get_type())) {
    if (OB_FAIL(set_outrow_ctx_seq_no(index_data))) {
      LOG_WARN("set_outrow_ctx_seq_no fail", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support type", K(ret), K(index_data));
  }
  return ret;
}

int ObExtInfoCbRegister::set_outrow_ctx_seq_no(ObObj& index_data)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 locator;
  ObLobCommon *lob_common = nullptr;
  ObString str_data = index_data.get_string();
  if (str_data.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index lob data is empty", K(ret), K(index_data));
  } else if (OB_ISNULL(lob_common = reinterpret_cast<ObLobCommon*>(str_data.ptr()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob_common is null", K(ret), K(index_data));
  } else if (! lob_common->is_valid() || lob_common->in_row_ || lob_common->is_mem_loc_ || ! lob_common->is_init_  ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob", K(ret), K(*lob_common));
  } else {
    ObLobData *lob_data = reinterpret_cast<ObLobData *>(lob_common->buffer_);
    ObLobDataOutRowCtx *lob_data_outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx *>(lob_data->buffer_);
    lob_data_outrow_ctx->offset_ = 0;
    lob_data_outrow_ctx->seq_no_st_ = seq_no_st_.cast_to_int();
    lob_data_outrow_ctx->seq_no_cnt_ = seq_no_cnt_;
    if (lob_data_outrow_ctx->is_valid_old_value_ext_info_log()) {
      lob_data_outrow_ctx->del_seq_no_cnt_ = seq_no_cnt_;
    }
    lob_data_outrow_ctx->first_meta_offset_ = 0;
    lob_data_outrow_ctx->check_sum_ = 0;
    lob_data_outrow_ctx->modified_len_ = data_size_ + header_.get_serialize_size();
  }
  return ret;
}

int ObExtInfoCbRegister::build_data_iter(
    ObObj &ext_info_data,
    transaction::ObTxDesc *tx_desc,
    const transaction::ObTxSEQ &tx_scn,
    const transaction::ObTxReadSnapshot &snapshot,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager*);
  ObString data = ext_info_data.get_string();
  char *data_buf = nullptr;
  int64_t data_buf_len = 0;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "lob manager is  null", K(ret));
  } else if (! is_lob_storage(ext_info_data.get_type())) {
    if (OB_FAIL(lob_mgr->query(data, data_iter_))) {
      LOG_WARN("build data iter fail", K(ret), K(data));
    } else {
      data_size_ = data.length();
    }
  } else if (OB_FALSE_IT(lob_locator_.assign_buffer(data.ptr(), data.length()))) {
  } else if (! lob_locator_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret), K(lob_locator_));
  } else if (OB_FAIL(lob_locator_.get_lob_data_byte_len(data_size_))) {
    LOG_WARN("get lob data byte len fail", K(ret), K(lob_locator_));
  } else if (OB_ISNULL(lob_param_ = OB_NEWx(ObLobAccessParam, &tmp_allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob param fail", K(ret), "size", sizeof(ObLobAccessParam));
  } else if (OB_FAIL(lob_mgr->build_lob_param(
      *lob_param_,
      tmp_allocator_,
      CS_TYPE_BINARY,
      0,
      data_size_,
      timeout_,
      lob_locator_))) {
    LOG_WARN("build lob param fail", K(ret), K(lob_locator_));
  } else {
    lob_param_->tx_desc_ = tx_desc;
    lob_param_->parent_seq_no_ = tx_scn;
    lob_param_->snapshot_ = snapshot;
    lob_param_->tx_id_ = tx_desc->get_tx_id();
    lob_param_->need_read_latest_ = true;
    lob_param_->no_need_retry_ = true;
    lob_param_->tablet_id_ = tablet_id;
    lob_param_->ls_id_ = ls_id;

    if (OB_FAIL(lob_mgr->query(*lob_param_, data_iter_))) {
      LOG_WARN("build data iter fail", K(ret), K(lob_param_));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(data_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data iter is null", K(ret), K(lob_param_));
  } else if (OB_FALSE_IT(data_buf_len = std::min(data_size_ + header_.get_serialize_size(), OB_EXT_INFO_LOG_BLOCK_MAX_SIZE))) {
  } else if (OB_ISNULL(data_buf = reinterpret_cast<char*>(tmp_allocator_.alloc(data_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob param fail", K(ret), K(data_buf_len));
  } else {
    data_buffer_.assign_buffer(data_buf, data_buf_len);
  }
  return ret;
}

int ObExtInfoCbRegister::get_data(ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data iter is null", K(ret));
  } else if (! header_writed_) {
    char *buf = data_buffer_.ptr();
    int64_t buf_len = data_buffer_.size();
    int64_t pos = 0;
    ObString read_buffer;
    if (OB_FAIL(header_.serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize header fail", KR(ret), K(pos), K(buf_len), KP(buf));
    } else {
      read_buffer.assign_buffer(buf + pos, buf_len - pos);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_iter_->get_next_row(read_buffer))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row fail", K(ret));
      }
    } else {
      data.assign_ptr(buf, read_buffer.length() + 1);
      data_buffer_.set_length(read_buffer.length() + 1);
      header_writed_ = true;
    }
  } else {
    data_buffer_.set_length(0);
    if (OB_FAIL(data_iter_->get_next_row(data_buffer_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row fail", K(ret));
      }
    } else {
      data.assign_ptr(data_buffer_.ptr(), data_buffer_.length());
    }
  }
  return ret;
}

int ObExtInfoCbRegister::get_lob_id(ObObj &index_data, ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  ObObjType type = index_data.get_type();
  if (is_lob_storage(type)) {
    ObLobCommon *lob_common = nullptr;
    ObString str_data = index_data.get_string();
    if (str_data.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index lob data is empty", K(ret), K(index_data));
    } else if (OB_ISNULL(lob_common = reinterpret_cast<ObLobCommon*>(str_data.ptr()))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lob_common is null", K(ret), K(index_data));
    } else if (! lob_common->is_valid() || lob_common->in_row_ || lob_common->is_mem_loc_ || ! lob_common->is_init_  ) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid lob", K(ret), K(*lob_common));
    } else {
      ObLobData *lob_data = reinterpret_cast<ObLobData *>(lob_common->buffer_);
      lob_id = lob_data->id_;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support type", K(ret), K(index_data));
  }
  return ret;
}

int ObExtInfoCbRegister::append_callback_with_retry(
    storage::ObStoreCtx &store_ctx,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data)
{
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_INTERVAL = 10000; //10ms
  static const int64_t tigger_sleep_retry_cnt = 5;
  int64_t retry_cnt = 0;
  do {
    if (retry_cnt > tigger_sleep_retry_cnt) {
      ob_usleep(OB_MIN(SLEEP_INTERVAL * (retry_cnt - tigger_sleep_retry_cnt), THIS_WORKER.get_timeout_remain()));
    }
    ++retry_cnt;
    if (OB_UNLIKELY(THIS_WORKER.is_timeout())) {
      ret = OB_TIMEOUT;
      LOG_WARN("worker timeout", KR(ret), K(THIS_WORKER.get_timeout_ts()), K(retry_cnt));
    } else if (IS_INTERRUPTED()) {
      common::ObInterruptCode code = GET_INTERRUPT_CODE();
      ret = code.code_;
      LOG_WARN("received a interrupt", K(code), K(ret), K(retry_cnt));
    } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
      ret = OB_KILLED_BY_THROTTLING;
      LOG_INFO("retry is interrupted by worker check wait", K(ret), K(retry_cnt));
    } else if (OB_FAIL(append_callback(store_ctx, dml_flag, seq_no_cur, lob_id, data))) {
      LOG_WARN("set row callback failed", K(ret), K(dml_flag), K(seq_no_cur), K(lob_id), K(retry_cnt));
    }
  } while (OB_NEED_RETRY == ret);
  return ret;
}

int ObExtInfoCbRegister::append_callback(
    storage::ObStoreCtx &store_ctx,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_cur,
    const ObLobId &lob_id,
    ObString &data)
{
  int ret = OB_SUCCESS;
  memtable::ObMvccWriteGuard guard(false);
  storage::ObExtInfoCallback *cb = nullptr;
  bool is_during_freeze = false;
  if (OB_FAIL(check_is_during_freeze(is_during_freeze))) {
    LOG_WARN("check is during freeze failed", K(ret));
  } else if (is_during_freeze) {
    ret = OB_NEED_RETRY;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("during freeze, we need wait freeze finished", K(ret));
    }
  } else if (OB_ISNULL(cb = mvcc_ctx_->alloc_ext_info_callback())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc row callback failed", K(ret));
  } else if (OB_FAIL(cb->set(tmp_allocator_, dml_flag, seq_no_cur, lob_id, data))) {
    LOG_WARN("set row callback failed", K(ret), K(*cb));
  } else if (OB_FAIL(guard.write_auth(store_ctx))) {
    LOG_WARN("write_auth fail", K(ret), K(store_ctx));
  } else if (OB_FAIL(mvcc_ctx_->append_callback(cb))) {
    LOG_WARN("register ext info callback failed", K(ret), K(*this),K(*cb));
  } else {
    guard.set_is_lob_ext_info_log(true);
    LOG_DEBUG("append ext info callback success", KPC(cb), K(seq_no_cur));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(cb)) {
    // append callback fail, need free to avoid memory leak
    mvcc_ctx_->free_ext_info_callback(cb);
    LOG_WARN("append fail, free callback", K(ret), K(seq_no_cur));
  }
  return ret;
}

int ObExtInfoCbRegister::check_is_during_freeze(bool &is_during_freeze)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  is_during_freeze = false;
  if (OB_ISNULL(lob_param_)) { // if lob_param_ is null, means this is not outrow lob reading, so no need check
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(lob_param_->ls_id_,
                                                ls_handle,
                                                ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls handle failed", KR(ret), K(lob_param_->ls_id_));
  } else if (ls_handle.get_ls()->is_logonly_replica()) {
    is_during_freeze = false;
    LOG_TRACE("logonly replica do not need to freeze", K(lob_param_->ls_id_));
  } else {
    ObFreezer *freezer = ls_handle.get_ls()->get_freezer();
    is_during_freeze = freezer->is_freeze(freezer->get_freeze_flag());
  }
  return ret;
}

}; // end namespace storage
}; // end namespace oceanbase
