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

#include "ob_semistruct_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_column_encoding_struct.h"
#include "storage/blocksstable/cs_encoding/ob_cs_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

int ObSemiStructEncodeMetaDesc::deserialize(
    const ObCSColumnHeader &col_header,
    const uint32_t row_cnt,
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos + sizeof(ObSemiStructEncodeHeader) > len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("buf is invalid", K(ret), KP(buf), K(len), K(pos), "size", sizeof(ObSemiStructEncodeHeader));
  } else {
    int64_t offset = pos;
    semistruct_header_ = reinterpret_cast<const ObSemiStructEncodeHeader *>(buf + offset);
    offset += sizeof(ObSemiStructEncodeHeader);
    if (OB_UNLIKELY(semistruct_header_->type_ != ObSemiStructEncodeHeader::Type::JSON)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semistruct_header is incorrect", K(ret), KPC(semistruct_header_));
    } else if (OB_UNLIKELY(pos + semistruct_header_->header_len_ > len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buf is invalid", K(ret), KP(buf), K(len), K(pos), KPC(semistruct_header_));
    } else {
      uint16_t sub_column_cnt = semistruct_header_->column_cnt_;
      sub_col_headers_ = reinterpret_cast<const ObCSColumnHeader*>(buf + offset);
      offset += sizeof(ObCSColumnHeader) * sub_column_cnt;

      sub_schema_data_ptr_ = buf + offset;
      offset += semistruct_header_->schema_len_;

      if (col_header.has_null_or_nop_bitmap()) {
        bitmap_size_ = ObCSEncodingUtil::get_bitmap_byte_size(row_cnt);
        null_bitmap_ = buf + offset;
        offset += bitmap_size_;
      } else {
        bitmap_size_ = 0;
        null_bitmap_ = nullptr;
      }

      sub_col_meta_ptr_ = buf + offset;
      sub_col_meta_len_ = semistruct_header_->header_len_ - offset;

      pos += semistruct_header_->header_len_;
    }
  }
  return ret;
}

}  // end namespace blocksstable
}  // end namespace oceanbase