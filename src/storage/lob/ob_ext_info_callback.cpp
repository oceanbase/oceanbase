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
#include "share/ob_lob_access_utils.h"
#include "lib/utility/ob_fast_convert.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_writer.h"

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
    ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObDatumRow datum_row;
  char *buf = nullptr;
  int64_t len = 0;

  dml_flag_ = dml_flag;
  seq_no_cur_ = seq_no_cur;
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
    } else if (OB_FAIL(datum_row.storage_datums_[OB_EXT_INFO_MUTATOR_ROW_VALUE_IDX].from_buf_enhance(data.ptr(), data.length()))) {
      LOG_WARN("set datum fail", K(ret), K(data));
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
    data_iter_->~ObLobQueryIter();
    OB_DELETE(ObLobQueryIter, "unused", data_iter_);
    data_iter_ = nullptr;
  }
}

int ObExtInfoCbRegister::register_cb(
    memtable::ObIMvccCtx *ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    transaction::ObTxDesc *tx_desc,
    transaction::ObTxSEQ &parent_seq_no,
    ObObj &index_data,
    ObObj &ext_info_data)
{
  int ret = OB_SUCCESS;
  ext_info_data_ = ext_info_data;
  timeout_ = timeout;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
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
  } else if (OB_FAIL(init_header(index_data, ext_info_data))) {
    LOG_WARN("init_header_ fail", K(ret), K(ext_info_data));
  } else if (OB_FAIL(build_data_iter(ext_info_data))) {
    LOG_WARN("build data iter fail", K(ret));
  } else if (FALSE_IT(seq_no_cnt_ = data_size_/OB_EXT_INFO_LOG_BLOCK_MAX_SIZE + 1)) {
  } else if (OB_FAIL(tx_desc->get_and_inc_tx_seq(parent_seq_no.get_branch(),
                                                 seq_no_cnt_,
                                                 seq_no_st_))) {
    LOG_WARN("get and inc tx seq failed", K(ret), K_(seq_no_cnt));
  } else {
    transaction::ObTxSEQ seq_no_cur = seq_no_st_;
    ObString data;
    ObSEArray<ObExtInfoCallback*, 1> cb_array;
    int cb_cnt = 0;
    while (OB_SUCC(ret) && OB_SUCC(get_data(data))) {
      storage::ObExtInfoCallback *cb = nullptr;
      if (OB_ISNULL(cb = mvcc_ctx_->alloc_ext_info_callback())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc row callback failed", K(ret));
      } else if (OB_FAIL(cb_array.push_back(cb))) {
        LOG_WARN("push back cb fail", K(ret), K(cb_array));
      } else {
        cb->set(tmp_allocator_, dml_flag, seq_no_cur, data);
        seq_no_cur = seq_no_cur + 1;
        if (OB_FAIL(mvcc_ctx_->append_callback(cb))) {
          LOG_ERROR("register ext info callback failed", K(ret), K(*this),K(*cb));
        } else {
          ++cb_cnt;
          LOG_DEBUG("register ext info callback success", K(*cb));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(cb)) {
        mvcc_ctx_->free_ext_info_callback(cb);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      for(int i = 0; i < cb_cnt; ++i) {
        cb_array[i]->del();
        mvcc_ctx_->free_ext_info_callback(cb_array[i]);
      }
    } else if (OB_FALSE_IT(seq_no_cnt_ = cb_cnt)) {
    } else if (OB_FAIL(set_index_data(index_data))) {
      LOG_WARN("set_index_data fail", K(ret));
    }
  }
  return ret;
}

int ObExtInfoCbRegister::init_header(ObObj& index_data, ObObj &ext_info_data)
{
  int ret = OB_SUCCESS;
  header_.type_ = get_type(index_data.get_type());
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
    lob_data_outrow_ctx->seq_no_st_ = seq_no_st_.cast_to_int();
    lob_data_outrow_ctx->seq_no_cnt_ = seq_no_cnt_;
    lob_data_outrow_ctx->modified_len_ = data_size_ + header_.get_serialize_size();
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

}; // end namespace storage
}; // end namespace oceanbase
