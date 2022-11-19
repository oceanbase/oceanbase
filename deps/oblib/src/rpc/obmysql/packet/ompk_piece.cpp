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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "ompk_piece.h"
#include "lib/oblog/ob_log_module.h"
#include "rpc/obmysql/ob_mysql_util.h"


using namespace oceanbase::obmysql;

int OMPKPiece::serialize(char* buffer, int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, piece_mode_, pos))) {
      LOG_WARN("store failed", KP(buffer), K(length), K_(piece_mode), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, 
                                               length, 
                                               is_null_, 
                                               pos))) {
      LOG_WARN("store is null failed", KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int8(buffer, length, data_length_, pos))) {
      LOG_WARN("store length failed", KP(buffer), K(length), K_(data_length), K(pos));
    } else if (!is_array_) {
      if (length >= pos && data_length_ <= static_cast<uint64_t>(length - pos)) {
        MEMCPY(buffer + pos, data_.ptr(), data_length_);
        pos += data_length_;
      } else {
        // need OB_SIZE_OVERFLOW to extend easy_buf size in  try_encode_with
        ret = OB_SIZE_OVERFLOW;
        LOG_DEBUG("piece size overflow", K(data_length_));
      }
    } else {
      // is_array_ must be true
      // TODO: array type would support long data protocol in the furture
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support array type yet.", K(ret)); 
    }
  }
  return ret;
}

int64_t OMPKPiece::get_serialize_size() const
{
  int64_t len = 0;
  len += 1;                 // piece mode
  len += 8;                 // len
  len += 1;                 // is_null
  if (!is_array_) {
    len += data_.size();              // data
  }
  return len;
}