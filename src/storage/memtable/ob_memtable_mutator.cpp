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

#include "storage/memtable/ob_memtable_mutator.h"

#include "lib/utility/serialization.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/ob_tracepoint.h"

#include "storage/memtable/ob_memtable_context.h"  // ObTransRowFlag

namespace oceanbase {
using namespace common;
using namespace share;
using namespace serialization;
using namespace storage;
namespace memtable {
ObMemtableMutatorMeta::ObMemtableMutatorMeta()
    : magic_(MMB_MAGIC),
      meta_crc_(0),
      meta_size_(sizeof(*this)),
      version_(0),
      flags_(ObTransRowFlag::NORMAL_ROW),
      data_crc_(0),
      data_size_(0),
      row_count_(0)
{}

ObMemtableMutatorMeta::~ObMemtableMutatorMeta()
{}

bool ObMemtableMutatorMeta::check_magic()
{
  return MMB_MAGIC == magic_;
}

int ObMemtableMutatorMeta::fill_header(const char* buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    data_size_ = (uint32_t)data_len;
    data_crc_ = (uint32_t)ob_crc64(buf, data_len);
    meta_crc_ = calc_meta_crc((const char*)this);
  }
  return ret;
}

int ObMemtableMutatorMeta::check_data_integrity(const char* buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_crc_ != (uint32_t)ob_crc64(buf, data_len)) {
    ret = OB_INVALID_LOG;
    TRANS_LOG(WARN, "check_data_integrity fail", K(ret));
  }
  return ret;
}

uint32_t ObMemtableMutatorMeta::calc_meta_crc(const char* buf)
{
  int64_t skip_size = ((char*)&meta_size_ - (char*)this);
  return (uint32_t)ob_crc64(buf + skip_size, meta_size_ - skip_size);
}

int ObMemtableMutatorMeta::inc_row_count()
{
  int ret = OB_SUCCESS;
  ++row_count_;
  return ret;
}

int64_t ObMemtableMutatorMeta::get_row_count() const
{
  return row_count_;
}

int64_t ObMemtableMutatorMeta::get_serialize_size() const
{
  return sizeof(*this);
}

int ObMemtableMutatorMeta::serialize(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int32_t meta_size = static_cast<int32_t>(get_serialize_size());
  if (OB_ISNULL(buf) || pos + meta_size > buf_len) {
    TRANS_LOG(WARN, "invalid param", KP(buf), K(meta_size), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf + pos, this, meta_size);
    pos += meta_size;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "serialize fail", "ret", ret, "buf", OB_P(buf), "buf_len", buf_len, "meta_size", meta_size);
  }
  return ret;
}

int ObMemtableMutatorMeta::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  const int32_t min_meta_size = MIN_META_SIZE;
  if (OB_ISNULL(buf) || data_len < 0 || pos + min_meta_size > data_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    MEMCPY(this, buf + pos, min_meta_size);
    if (!check_magic()) {
      ret = OB_INVALID_LOG;
      TRANS_LOG(WARN, "invalid log: check_magic fail", K(*this));
    } else if (calc_meta_crc(buf + pos) != meta_crc_) {
      ret = OB_INVALID_LOG;
      TRANS_LOG(WARN, "invalid log: check_meta_crc fail", K(*this));
    } else if (pos + meta_size_ > data_len) {
      ret = OB_BUF_NOT_ENOUGH;
      TRANS_LOG(WARN, "buf not enough", K(pos), K(meta_size_), K(data_len));
    } else {
      MEMCPY(this, buf + pos, min(sizeof(*this), meta_size_));
      pos += meta_size_;
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN,
        "deserialize fail",
        "ret",
        ret,
        "buf",
        OB_P(buf),
        "data_len",
        data_len,
        "pos",
        pos,
        "meta_size",
        meta_size_);
  }
  return ret;
}

int64_t ObMemtableMutatorMeta::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  common::databuff_printf(buffer,
      length,
      pos,
      "%p data_crc=%x meta_size=%d data_size=%d row_count=%d",
      this,
      data_crc_,
      meta_size_,
      data_size_,
      row_count_);
  return pos;
}

bool ObMemtableMutatorMeta::is_row_start() const
{
  return ObTransRowFlag::is_row_start(flags_);
}

// only meta_crc is newly generated here, other information will keep unchanged
void ObMemtableMutatorMeta::generate_new_header()
{
  meta_crc_ = calc_meta_crc((const char*)this);
}

int ObMemtableMutatorMeta::set_flags(const uint8_t row_flag)
{
  int ret = OB_SUCCESS;

  if (!ObTransRowFlag::is_valid_row_flag(row_flag)) {
    TRANS_LOG(WARN, "invalid argument", K(row_flag));
    ret = OB_INVALID_ARGUMENT;
  } else {
    flags_ = row_flag;
  }

  return ret;
}

ObMemtableMutatorRow::ObMemtableMutatorRow()
    : row_size_(0),
      table_id_(OB_INVALID_ID),
      rowkey_(),
      table_version_(0),
      dml_type_(),
      update_seq_(0),
      acc_checksum_(0),
      version_(0),
      sql_no_(0),
      flag_(0)
{
  rowkey_.assign((ObObj*)obj_array_, OB_MAX_ROWKEY_COLUMN_NUMBER);
}

ObMemtableMutatorRow::ObMemtableMutatorRow(const uint64_t table_id, const ObStoreRowkey& rowkey,
    const int64_t table_version, const ObRowData& new_row, const ObRowData& old_row, const ObRowDml dml_type,
    const uint32_t modify_count, const uint32_t acc_checksum, const int64_t version, const int32_t sql_no,
    const int32_t flag)
    : row_size_(0),
      table_id_(table_id),
      rowkey_(rowkey),
      table_version_(table_version),
      dml_type_(dml_type),
      update_seq_((uint32_t)modify_count),
      new_row_(new_row),
      old_row_(old_row),
      acc_checksum_(acc_checksum),
      version_(version),
      sql_no_(sql_no),
      flag_(flag)
{}

ObMemtableMutatorRow::~ObMemtableMutatorRow()
{}

void ObMemtableMutatorRow::reset()
{
  row_size_ = 0;
  table_id_ = OB_INVALID_ID;
  rowkey_.assign((ObObj*)obj_array_, OB_MAX_ROWKEY_COLUMN_NUMBER);
  table_version_ = 0;
  dml_type_ = storage::T_DML_UNKNOWN;
  update_seq_ = 0;
  new_row_.reset();
  old_row_.reset();
  acc_checksum_ = 0;
  version_ = 0;
  sql_no_ = 0;
  flag_ = 0;
}

int ObMemtableMutatorRow::copy(uint64_t& table_id, ObStoreRowkey& rowkey, int64_t& table_version, ObRowData& new_row,
    ObRowData& old_row, ObRowDml& dml_type, uint32_t& modify_count, uint32_t& acc_checksum, int64_t& version,
    int32_t& sql_no, int32_t& flag) const
{
  int ret = OB_SUCCESS;
  table_id = table_id_;
  rowkey = rowkey_;
  table_version = table_version_;
  new_row = new_row_;
  old_row = old_row_;
  dml_type = dml_type_;
  modify_count = update_seq_;
  acc_checksum = acc_checksum_;
  version = version_;
  sql_no = sql_no_;
  flag = flag_;
  return ret;
}

int ObMemtableMutatorRow::serialize(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos + encoded_length_i32(0);
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_id_))) {
    TRANS_LOG(WARN, "serialize table id failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(rowkey_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_version_)) ||
             OB_FAIL(encode_vi32(buf, buf_len, new_pos, dml_type_)) ||
             OB_FAIL(encode_vi32(buf, buf_len, new_pos, update_seq_)) ||
             OB_FAIL(new_row_.serialize(buf, buf_len, new_pos)) || OB_FAIL(old_row_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(encode_vi32(buf, buf_len, new_pos, acc_checksum_)) ||
             OB_FAIL(encode_vi64(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(encode_vi32(buf, buf_len, new_pos, sql_no_)) ||
             OB_FAIL(encode_vi32(buf, buf_len, new_pos, flag_))) {
    if (OB_BUF_NOT_ENOUGH != ret || buf_len > common::OB_MAX_LOG_ALLOWED_SIZE) {
      TRANS_LOG(INFO, "serialize row fail", K(ret), KP(buf), K(buf_len), K(pos));
    }
  }

  if (OB_SUCC(ret)) {
    row_size_ = (uint32_t)(new_pos - pos);
    if (OB_FAIL(encode_i32(buf, buf_len, pos, row_size_))) {
      TRANS_LOG(WARN, "serialize row fail", K(ret), K(buf_len), K(pos), K(table_id_));
    } else {
      pos = new_pos;
    }
  }
  return ret;
}

int ObMemtableMutatorRow::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const char* data_buf = nullptr;
  int64_t data_len = 0;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos, (int32_t*)&row_size_))) {
    TRANS_LOG(WARN, "deserialize row_size fail", K(ret), K(buf_len), K(new_pos));
  } else if (pos + row_size_ > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "size overflow", K(ret), KP(buf), K(buf_len), K(pos), K_(row_size));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t*)&table_id_))) {
    TRANS_LOG(WARN, "deserialize table id failed", K(ret), K(buf_len), K(new_pos));
  } else {
    int64_t data_pos = new_pos;
    data_buf = buf + data_pos;
    data_len = row_size_ - (data_pos - pos);
    new_pos = 0;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey_.deserialize(data_buf, data_len, new_pos)) ||
          OB_FAIL(decode_vi64(data_buf, data_len, new_pos, &table_version_)) ||
          OB_FAIL(decode_vi32(data_buf, data_len, new_pos, (int32_t*)&dml_type_)) ||
          OB_FAIL(decode_vi32(data_buf, data_len, new_pos, (int32_t*)&update_seq_)) ||
          OB_FAIL(new_row_.deserialize(data_buf, data_len, new_pos)) ||
          OB_FAIL(old_row_.deserialize(data_buf, data_len, new_pos))) {
        TRANS_LOG(WARN, "deserialize row fail", K(ret), K(table_id_));
      } else {
        acc_checksum_ = 0;
        version_ = 0;
        sql_no_ = 0;
        if (new_pos < data_len) {
          if (OB_FAIL(decode_vi32(data_buf, data_len, new_pos, (int32_t*)&acc_checksum_))) {
            TRANS_LOG(WARN, "deserialize acc checksum fail", K(ret), K(table_id_), K(data_len), K(new_pos));
          } else if (OB_FAIL(decode_vi64(data_buf, data_len, new_pos, (int64_t*)&version_))) {
            TRANS_LOG(WARN, "deserialize version fail", K(ret), K(table_id_), K(data_len), K(new_pos));
          } else {
            // do nothing
          }
        }
        if (OB_SUCC(ret) && (new_pos < data_len)) {
          if (OB_FAIL(decode_vi32(data_buf, data_len, new_pos, (int32_t*)&sql_no_))) {
            TRANS_LOG(WARN, "deserialize sql_no fail", K(ret), K(table_id_), K(data_len), K(new_pos));
          }
        }
        if (OB_SUCC(ret) && (new_pos < data_len)) {
          if (OB_FAIL(decode_vi32(data_buf, data_len, new_pos, (int32_t*)&flag_))) {
            TRANS_LOG(WARN, "deserialize flag fail", K(ret), K(table_id_), K(data_len), K(new_pos));
          }
        }
        if (OB_SUCC(ret)) {
          pos += row_size_;
        }
      }
    }
  }
  return ret;
}

int ObMemtableMutatorRow::try_replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(new_tenant_id == extract_tenant_id(table_id_))) {
    // no need update tenant_id, skip
  } else if (OB_UNLIKELY(!is_valid_tenant_id(new_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t old_tid = table_id_;
    uint64_t new_table_id = combine_id(new_tenant_id, table_id_);
    table_id_ = new_table_id;
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
ObMemtableMutatorWriter::ObMemtableMutatorWriter()
{}

ObMemtableMutatorWriter::~ObMemtableMutatorWriter()
{}

int ObMemtableMutatorWriter::set_buffer(char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t meta_size = meta_.get_serialize_size();
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (buf_len < meta_size) {
    ret = OB_BUF_NOT_ENOUGH;
    TRANS_LOG(WARN, "buf not enough", K(buf_len), K(meta_size));
  } else if (!buf_.set_data(buf, buf_len)) {
    TRANS_LOG(WARN, "set_data fail", KP(buf), K(buf_len));
  } else {
    buf_.get_position() = meta_size;
  }
  return ret;
}

int ObMemtableMutatorWriter::append_kv(
    const uint64_t table_id, const ObStoreRowkey& rowkey, const int64_t table_version, const RedoDataNode& redo)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id || table_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObMemtableMutatorRow row(table_id,
        rowkey,
        table_version,
        redo.new_row_,
        redo.old_row_,
        redo.dml_type_,
        redo.modify_count_,
        redo.acc_checksum_,
        redo.version_,
        redo.sql_no_,
        redo.flag_);
    if (OB_ISNULL(buf_.get_data())) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "not init", K(ret));
    } else if (OB_FAIL(row.serialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        // do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(meta_.inc_row_count())) {
    } else {
    }
  }
  if (OB_SUCCESS != ret && OB_BUF_NOT_ENOUGH != ret) {
    TRANS_LOG(WARN, "append_kv fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMemtableMutatorWriter::append_row(ObMemtableMutatorRow& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(row.serialize(buf_.get_data(), buf_.get_capacity(), buf_.get_position()))) {
    if (ret == OB_ALLOCATE_MEMORY_FAILED) {
      // do nothing
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  } else if (OB_FAIL(meta_.inc_row_count())) {
    TRANS_LOG(WARN, "meta inc_row_count failed", K(ret));
  }
  return ret;
}

int ObMemtableMutatorWriter::serialize(const uint8_t row_flag, int64_t& res_len)
{
  int ret = OB_SUCCESS;
  const int64_t meta_size = meta_.get_serialize_size();
  int64_t meta_pos = 0;
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (0 >= meta_.get_row_count()) {
    TRANS_LOG(DEBUG, "no row exist");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!ObTransRowFlag::is_valid_row_flag(row_flag)) {
    TRANS_LOG(WARN, "invalid argument", K(row_flag));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(meta_.set_flags(row_flag))) {
    TRANS_LOG(WARN, "set flags error", K(ret), K(row_flag));
  } else if (OB_FAIL(meta_.fill_header(buf_.get_data() + meta_size, buf_.get_position() - meta_size))) {
  } else if (OB_FAIL(meta_.serialize(buf_.get_data(), meta_size, meta_pos))) {
  } else {
    res_len = buf_.get_position();
  }
  if (OB_FAIL(ret) && OB_ENTRY_NOT_EXIST != ret) {
    TRANS_LOG(WARN, "serialize fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int64_t ObMemtableMutatorWriter::get_serialize_size() const
{
  //@FIXME
  const int64_t SIZE = 2 * OB_MAX_ROW_LENGTH_IN_MEMTABLE;
  return SIZE;
}

int ObMemtableMutatorWriter::handle_big_row_data(const char* in_buf, const int64_t in_buf_len, int64_t& in_buf_pos,
    char* out_buf, const int64_t out_buf_len, int64_t& out_buf_pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_buf) || in_buf_len < 0 || in_buf_pos > in_buf_len || OB_ISNULL(out_buf) || out_buf_len < 0 ||
      out_buf_pos > out_buf_len) {
    TRANS_LOG(WARN, "invalid argument", KP(in_buf), K(in_buf_len), K(in_buf_pos));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t data_len = min(in_buf_len - in_buf_pos, out_buf_len - out_buf_pos);
    MEMCPY(out_buf + out_buf_pos, in_buf + in_buf_pos, data_len);
    in_buf_pos += data_len;
    out_buf_pos += data_len;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
ObMemtableMutatorIterator::ObMemtableMutatorIterator()
{
  big_row_ = false;
  reset();
}

ObMemtableMutatorIterator::~ObMemtableMutatorIterator()
{
  reset();
}

// If leader switch happened before the last log entry of lob row is successfully written,
// there may be memory leak on follower's mutator buf. reset function need to provide some
// basic bottom-line operations.
void ObMemtableMutatorIterator::reset()
{
  if (big_row_) {
    if (NULL != buf_.get_data()) {
      ob_free(buf_.get_data());
    }
  }
  buf_.reset();
  row_.reset();
  big_row_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  big_row_pos_ = 0;
  big_row_last_pos_ = 0;
  need_undo_redo_log_ = false;
}

void ObMemtableMutatorIterator::reset_big_row_args_()
{
  if (NULL != buf_.get_data()) {
    ob_free(buf_.get_data());
  }
  buf_.reset();
  big_row_ = false;
  big_row_pos_ = 0;
  big_row_last_pos_ = 0;
  need_undo_redo_log_ = false;
}

int ObMemtableMutatorIterator::set_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tenant_id_ = tenant_id;
  }

  return ret;
}

int ObMemtableMutatorIterator::undo_redo_log(const int64_t data_len)
{
  UNUSED(data_len);
  int ret = OB_SUCCESS;

  // 1. for a normal row, it's unnecessary since buf_ would be overwritten when retry replaying
  // 2. for a big row, if the first redo log encounters a replay error,
  //    buf_ would be NULL and don't need to handle, if the middle or last redo log
  //    encounters replay error, then the most recent redo log need to be rollbacked
  if (NULL != buf_.get_data()) {
    // if big_row_pos_ is less than or equal to 0,
    // then it's a normal row, and doesn't need special handling
    if (big_row_pos_ > 0) {
      if (big_row_last_pos_ < 0) {
        TRANS_LOG(ERROR, "unexpected data_len", K_(big_row_pos), K(data_len), K_(tenant_id), K_(big_row_last_pos));
        ret = OB_ERR_UNEXPECTED;
      } else {
        big_row_pos_ = big_row_last_pos_;
        buf_.get_position() = big_row_last_pos_;
        need_undo_redo_log_ = false;
      }
    }
  }
  return ret;
}

int ObMemtableMutatorIterator::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  int64_t data_pos = pos;

  if (OB_ISNULL(buf) || (data_len - pos) <= 0 || data_len < 0) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(data_len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(meta_.deserialize(buf, data_len, data_pos))) {
    TRANS_LOG(WARN, "decode meta fail", K(ret), KP(buf), K(data_len), K(data_pos));
    ret = (OB_SUCCESS == ret) ? OB_INVALID_DATA : ret;
  } else if (big_row_) {
    // the second and afterward redo log of big row, no need to copy meta
    const bool is_big_row_end = (ObTransRowFlag::is_big_row_end(meta_.get_flags()));
    big_row_last_pos_ = big_row_pos_;
    {
      const int64_t redo_log_len = (data_len - data_pos);
      if (buf_.get_remain() < redo_log_len) {
        TRANS_LOG(ERROR, "unexpectd data len", K_(big_row_pos), K_(meta), K(data_len));
        ret = OB_BUF_NOT_ENOUGH;
      } else if (big_row_pos_ >= meta_.get_total_size()) {
        TRANS_LOG(ERROR, "big row data already deserialize over", K_(big_row_pos), K_(meta), K(data_len));
        ret = OB_ERR_UNEXPECTED;
      } else {
        MEMCPY(buf_.get_data() + big_row_pos_, buf + data_pos, redo_log_len);
        pos = data_len;  // consume all data
        big_row_pos_ += redo_log_len;
        need_undo_redo_log_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (is_big_row_end) {
        if (big_row_pos_ == meta_.get_total_size()) {
          buf_.get_position() = meta_.get_meta_size();
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "big row data not complete", K_(big_row_pos), K_(meta), K(data_len));
        }
      }
    }
    // big row log replaying, apply for ephemeral big row buffer
  } else if (ObTransRowFlag::is_big_row_start(meta_.get_flags())) {
#ifdef ERRSIM
    ret = E(EventTable::EN_ALLOCATE_DESERIALIZE_LOB_BUF_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "ERRSIM, memory alloc fail", K(ret));
      return ret;
    }
#endif
    if (OB_INVALID_TENANT_ID == tenant_id_) {
      TRANS_LOG(ERROR, "tenant_id_ is invalid", K(tenant_id_));
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int64_t approximate_size = meta_.get_total_size();
      ObMemAttr memattr(tenant_id_, "LOBMutatorBuf");
      void* ptr = NULL;
      if (NULL == (ptr = ob_malloc(approximate_size, memattr))) {
        TRANS_LOG(ERROR, "memory alloc fail", KP(ptr), K(approximate_size));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (!buf_.set_data((char*)ptr, approximate_size)) {
        TRANS_LOG(WARN, "set_data fail", KP(buf), K(pos), K(approximate_size));
      } else {
        buf_.get_limit() = meta_.get_total_size();
        // the first redo log of a big row, need to copy meta data
        MEMCPY(buf_.get_data(), buf + pos, data_len - pos);
        big_row_last_pos_ = big_row_pos_;
        big_row_pos_ += data_len - pos;
        pos = data_len;  // consume all data
        big_row_ = true;
        need_undo_redo_log_ = true;
      }
    }
  } else if (OB_UNLIKELY(!buf_.set_data(const_cast<char*>(buf + pos), meta_.get_total_size()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "set_data fail", KP(buf), K(pos), K(meta_.get_total_size()));
  } else {
    pos += meta_.get_total_size();
    buf_.get_limit() = meta_.get_total_size();
    buf_.get_position() = meta_.get_meta_size();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "deserialize fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMemtableMutatorIterator::get_next_row(uint64_t& table_id, common::ObStoreRowkey& rowkey, int64_t& table_version,
    ObRowData& new_row, storage::ObRowDml& dml_type, uint32_t& modify_count, uint32_t& acc_checksum, int64_t& version,
    int32_t& sql_no, int32_t& flag)
{
  int ret = OB_SUCCESS;
  table_version = -1;
  table_id = OB_INVALID_ID;
  rowkey.reset();
  new_row.reset();
  dml_type = T_DML_UNKNOWN;
  modify_count = 0;
  sql_no = 0;
  flag = 0;

  row_.reset();
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), K(buf_));
  } else if (buf_.get_remain_data_len() <= 0) {
    ret = OB_ITER_END;
    // after replay all data of a big row, release big row buffer immediately
    if (big_row_) {
      reset_big_row_args_();
    }
  } else if (big_row_ && big_row_pos_ < meta_.get_total_size()) {
    // big row data hasn't been completely collected, need to wait other data
    ret = OB_ITER_END;
    TRANS_LOG(INFO, "desrialize big row need continue", K(ret), K_(big_row), K_(big_row_pos), K_(meta), K_(tenant_id));
  } else if (OB_FAIL(row_.deserialize(buf_.get_data(), buf_.get_limit(), buf_.get_position()))) {
    TRANS_LOG(WARN, "deserialize mutator row fail", K(ret));
  } else if (row_.get_tenant_id() != tenant_id_ && OB_FAIL(row_.try_replace_tenant_id(tenant_id_))) {
    TRANS_LOG(WARN, "try_replace_tenant_id fail", K(ret), K(tenant_id_));
  } else {
    ObRowData old_row;
    if (OB_FAIL(row_.copy(table_id,
            rowkey,
            table_version,
            new_row,
            old_row,
            dml_type,
            modify_count,
            acc_checksum,
            version,
            sql_no,
            flag))) {
      TRANS_LOG(WARN,
          "row_.copy fail",
          K(ret),
          K(table_id),
          K(rowkey),
          K(table_version),
          K(new_row),
          K(old_row),
          K(dml_type),
          K(modify_count),
          K(acc_checksum),
          K(version));
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    TRANS_LOG(WARN, "get_next_row fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMemtableMutatorIterator::get_next_row(ObMemtableMutatorRow& row)
{
  int ret = OB_SUCCESS;
  row.reset();

  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(buf_));
  } else if (buf_.get_remain_data_len() <= 0) {
    ret = OB_ITER_END;
    // after replay all data of a big row, release big row buffer immediately
    if (big_row_) {
      reset_big_row_args_();
    }
  } else if (big_row_ && big_row_pos_ < meta_.get_total_size()) {
    // big row data hasn't been completely collected, need to wait other data
    ret = OB_ITER_END;
    TRANS_LOG(INFO, "desrialize big row need continue", K(ret), K_(big_row), K_(big_row_pos), K_(meta), K_(tenant_id));
  } else if (OB_FAIL(row.deserialize(buf_.get_data(), buf_.get_limit(), buf_.get_position()))) {
    TRANS_LOG(WARN, "deserialize mutator row fail", K(ret), K(buf_));
  } else {
    // do nothing
  }
  return ret;
}

int ObMemtableMutatorIterator::try_replace_tenant_id(const uint64_t new_tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(new_tenant_id == tenant_id_)) {
    // no need update tenant_id, skip
  } else if (OB_UNLIKELY(!is_valid_tenant_id(new_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t old_id = tenant_id_;
    tenant_id_ = new_tenant_id;
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      TRANS_LOG(INFO, "replace_tenant_id", K(old_id), K(new_tenant_id));
    }
  }

  return ret;
}

}  // namespace memtable
}  // namespace oceanbase
