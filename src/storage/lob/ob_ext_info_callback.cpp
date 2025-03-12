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
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/lob/ob_lob_manager.h"

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
    LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, new_pos, type_))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(buf_len));
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
    LOG_ERROR("serialize failed", K(ret), K(pos), K(data_len));
  } else if (OB_FAIL(serialization::decode_i8(buf, data_len, new_pos, reinterpret_cast<int8_t*>(&type_)))) {
    LOG_ERROR("serialize failed", K(ret), K(pos), K(data_len));
  } else {
    pos = new_pos;
  }
  return ret;
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

int ObExtInfoCallback::log_submitted(const SCN scn, storage::ObIMemtable *&last_mt)
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
  blocksstable::ObDatumRow datum_row;
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

int ObExtInfoCbRegister::alloc_seq_no(
    transaction::ObTxDesc *tx_desc,
    const transaction::ObTxSEQ &parent_seq_no,
    const int64_t data_size,
    transaction::ObTxSEQ &seq_no_st,
    int64_t &seq_no_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx desc is null", K(ret));
  } else if (data_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_size is invalid", K(ret), K(data_size));
  } else {
    if (data_size % OB_EXT_INFO_LOG_BLOCK_MAX_SIZE > 0) {
      seq_no_cnt = data_size / OB_EXT_INFO_LOG_BLOCK_MAX_SIZE + 1;
    } else {
      seq_no_cnt = data_size / OB_EXT_INFO_LOG_BLOCK_MAX_SIZE;
    }
    if (OB_FAIL(tx_desc->get_and_inc_tx_seq(parent_seq_no.get_branch(), seq_no_cnt, seq_no_st))) {
      LOG_WARN("alloc seq_no fail", K(ret), K(parent_seq_no), KPC(tx_desc));
    }
  }
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
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    const transaction::ObTxSEQ &seq_no_st,
    const int64_t seq_no_cnt,
    const ObString &index_data,
    const ObObjType index_data_type,
    const ObExtInfoLogHeader &header,
    ObObj &ext_info_data)
{
  int ret = OB_SUCCESS;
  ext_info_data_ = ext_info_data;
  timeout_ = timeout;
  seq_no_st_ = seq_no_st;
  seq_no_cnt_ = seq_no_cnt;
  header_ = header;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobId lob_id;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]get lob manager instance failed.", K(ret));
  } else if (ext_info_data.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is empty", K(ret), K(ext_info_data));
  } else if (OB_ISNULL(mvcc_ctx_ = ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is empty", K(ret), K(ext_info_data));
  } else if (OB_FAIL(get_lob_id(index_data, index_data_type, lob_id))) {
    LOG_WARN("get lob id fail", K(ret), K(index_data_type));
  } else if (OB_FAIL(build_data_iter(ext_info_data))) {
    LOG_WARN("build data iter fail", K(ret));
  } else {
    transaction::ObTxSEQ seq_no_cur = seq_no_st_;
    ObString data;
    int cb_cnt = 0;
    while (OB_SUCC(ret) && OB_SUCC(get_data(data))) {
      storage::ObExtInfoCallback *cb = nullptr;
      if (cb_cnt >= seq_no_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("seq no alloc too small", K(ret), K(seq_no_cnt_), K(cb_cnt), K(data_size_));
      } else if (OB_ISNULL(cb = mvcc_ctx_->alloc_ext_info_callback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc row callback failed", K(ret));
      } else if (OB_FAIL(cb->set(tmp_allocator_, dml_flag, seq_no_cur, lob_id, data))) {
        LOG_WARN("set row callback failed", K(ret), K(*cb));
      } else if (OB_FAIL(mvcc_ctx_->append_callback(cb))) {
        LOG_WARN("register ext info callback failed", K(ret), K(*this),K(*cb));
      } else {
        seq_no_cur = seq_no_cur + 1;
        ++cb_cnt;
        LOG_DEBUG("register ext info callback success", K(*cb));
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(cb)) {
        mvcc_ctx_->free_ext_info_callback(cb);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && cb_cnt != seq_no_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("seq_no used is not match", K(ret), K(seq_no_cnt_), K(cb_cnt), K(data_size_));
    }
  }
  return ret;
}

int ObExtInfoCbRegister::build_data_iter(ObObj &ext_info_data)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mgr = MTL(ObLobManager*);
  ObString data = ext_info_data.get_string();
  ObLobLocatorV2 data_locator;
  char *data_buf = nullptr;
  int64_t data_buf_len = 0;
  if (OB_ISNULL(lob_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "lob manager is  null", K(ret));
  } else if (! is_lob_storage(ext_info_data.get_type())) {
    if (OB_FAIL(lob_mgr->query(data, data_iter_))) {
      LOG_WARN("build data iter fail", K(ret), K(lob_param_));
    } else {
      data_size_ = data.length();
    }
  } else if (OB_FALSE_IT(data_locator.assign_buffer(data.ptr(), data.length()))) {
  } else if (! data_locator.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret), K(data_locator));
  } else if (OB_FAIL(data_locator.get_lob_data_byte_len(data_size_))) {
    LOG_WARN("get lob data byte len fail", K(ret), K(data_locator));
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
      data_locator))) {
    LOG_WARN("build lob param fail", K(ret), K(data_locator));
  } else if (OB_FAIL(lob_mgr->query(*lob_param_, data_iter_))) {
    LOG_WARN("build data iter fail", K(ret), K(lob_param_));
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

int ObExtInfoCbRegister::get_lob_id(const ObString &index_data, const ObObjType type, ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  if (is_lob_storage(type)) {
    ObLobCommon *lob_common = nullptr;
    ObString str_data = index_data;
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

}; // end namespace storage
}; // end namespace oceanbase
