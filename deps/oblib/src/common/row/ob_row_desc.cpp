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

#define USING_LOG_PREFIX COMMON

#include "common/row/ob_row_desc.h"

#include "lib/hash_func/murmur_hash.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/utility.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace common
{
using namespace serialization;

uint64_t ObRowDesc::HASH_COLLISIONS_COUNT = 0;

ObRowDesc::ObRowDesc()
    : cells_desc_count_(0), rowkey_cell_count_(0), hash_map_(cells_desc_)
{
  //avoid to call default constructor of Desc
  cells_desc_ = reinterpret_cast<Desc *>(cells_desc_buf_);
}

ObRowDesc::~ObRowDesc()
{
}

int64_t ObRowDesc::get_idx(const uint64_t table_id, const uint64_t column_id) const
{
  int64_t idx = OB_INVALID_INDEX;
  if (OB_LIKELY(cells_desc_count_ > 0 && 0 != table_id && 0 != column_id)) {
    int64_t index = 0;
    if (OB_SUCCESS == hash_map_.get_index(Desc(table_id, column_id),
                                          reinterpret_cast<uint64_t &>(index))) {
      if (OB_LIKELY(index < cells_desc_count_)) {
        idx = index;
      }
    }
  }
  return idx;
}

int ObRowDesc::add_column_desc(const uint64_t table_id, const uint64_t column_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cells_desc_count_ >= MAX_COLUMNS_COUNT)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("too many column for a row", K(table_id), K(column_id));
  } else {
    cells_desc_[cells_desc_count_].table_id_ = table_id;
    cells_desc_[cells_desc_count_].column_id_ = column_id;
    if (OB_FAIL(hash_map_.set_index(cells_desc_count_))) {
      LOG_WARN("failed to insert column desc", K(ret));
    } else {
      ++cells_desc_count_;
    }
  }
  return ret;
}

int64_t ObRowDesc::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_ROWKEY_CELL_NUM, rowkey_cell_count_);
  J_COMMA();
  J_NAME(N_CELL);
  J_COLON();
  J_ARRAY_START();
  for (int64_t i = 0; i < cells_desc_count_; ++i) {
    if (0 != i) {
      J_COMMA();
    }
    J_OW(J_KV(N_TID, cells_desc_[i].table_id_,
              N_CID, cells_desc_[i].column_id_));
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

ObRowDesc &ObRowDesc::operator = (const ObRowDesc &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_WARN("fail to assign row desc", K(ret));
    }
  }
  return *this;
}

ObRowDesc::ObRowDesc(const ObRowDesc &other)
    : cells_desc_count_(0), rowkey_cell_count_(0), hash_map_(cells_desc_)
{
  //avoid to call default constructor of Desc
  cells_desc_ = reinterpret_cast<Desc *>(cells_desc_buf_);
  *this = other;
}

int ObRowDesc::assign(const ObRowDesc &other)
{
  int ret = OB_SUCCESS;

  cells_desc_count_ = other.cells_desc_count_;
  rowkey_cell_count_ = other.rowkey_cell_count_;
  hash_map_.reset();
  MEMCPY(cells_desc_buf_, other.cells_desc_buf_, sizeof(Desc) * cells_desc_count_);

  for (int64_t i = 0; OB_SUCC(ret) && i < cells_desc_count_; i++) {
    if (OB_FAIL(hash_map_.set_index(i))) {
      LOG_WARN("failed to insert column desc", K(ret));
    }
  }

  return ret;
}

/* int ObRowDesc::serialize(char *buf, const int64_t buf_len, int64_t &pos) const */
DEFINE_SERIALIZE(ObRowDesc)
{
  int ret = OB_SUCCESS;
  const int64_t column_cnt = get_column_num();
  const int64_t pos_bk = pos;
  uint64_t tid = 0;
  uint64_t cid = 0;

  ret = encode_vi64(buf, buf_len, pos, column_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    if (OB_FAIL(get_tid_cid(i, tid, cid))) {
      LOG_ERROR("get tid cid error", K(ret));
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, static_cast<int64_t>(tid)))) {
      LOG_ERROR("encode tid error", K(ret));
    } else if (OB_FAIL(encode_vi64(buf, buf_len, pos, static_cast<int64_t>(cid)))) {
      LOG_ERROR("encode cid error", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encode_vi64(buf, buf_len, pos, rowkey_cell_count_))) {
      LOG_WARN("fail to encode", K(ret), K_(rowkey_cell_count));
    }
  }

  if (OB_FAIL(ret)) {
    pos = pos_bk;
  }
  return ret;
}

/* int ObRowDesc::deserialize(const char *buf, const int64_t data_len, int64_t &pos) */
DEFINE_DESERIALIZE(ObRowDesc)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = 0;
  int64_t tid = 0;
  int64_t cid = 0;
  int64_t pos_bk = pos;

  ret = decode_vi64(buf, data_len, pos, &column_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    if (OB_FAIL(decode_vi64(buf, data_len, pos, &tid))) {
      LOG_ERROR("decode tid error", K(ret));
    } else if (OB_FAIL(decode_vi64(buf, data_len, pos, &cid))) {
      LOG_ERROR("decode cid error", K(ret));
    } else if (OB_FAIL(add_column_desc(tid, cid))) {
      LOG_ERROR("add column desc error", K(ret));
    }
    //_OB_LOG(DEBUG, "tid %lu, cid %lu", tid, cid);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(decode_vi64(buf, data_len, pos, &rowkey_cell_count_))) {
      LOG_WARN("fail to decode", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    pos = pos_bk;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRowDesc)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  const int64_t column_cnt = get_column_num();
  uint64_t tid = 0;
  uint64_t cid = 0;

  size += encoded_length_vi64(column_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    if (OB_FAIL(get_tid_cid(i, tid, cid))) {
      LOG_ERROR("get tid cid error", K(ret));
    } else {
      size += encoded_length_vi64(static_cast<int64_t>(tid));
      size += encoded_length_vi64(static_cast<int64_t>(cid));
    }
  }
  size += encoded_length_vi64(rowkey_cell_count_);
  return size;
}

} // end namespace common
} // end namespace oceanbase
