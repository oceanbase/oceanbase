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

#ifndef OCEANBASE_ENCODING_OB_SEMISTRUCT_ENCODING_STRUCT_H_
#define OCEANBASE_ENCODING_OB_SEMISTRUCT_ENCODING_STRUCT_H_

#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace blocksstable
{

struct ObSemiStructEncodeHeader
{
  ObSemiStructEncodeHeader():
    type_(0),
    header_len_(0),
    reserved0_(0),
    column_cnt_(0),
    stream_cnt_(0),
    schema_len_(0),
    reserved1_(0)
  {}

  enum Type {
    INVALID = 0,
    JSON = 1,
    MAX_TYPE
  };

  TO_STRING_KV(K_(type), K_(header_len), K_(reserved0), K_(column_cnt), K_(stream_cnt), K_(schema_len), K_(reserved1));

  uint32_t type_ : 8;
  uint32_t header_len_ : 20;
  uint32_t reserved0_ : 4;
  uint16_t column_cnt_;
  uint16_t stream_cnt_;

  uint64_t schema_len_ : 20;
  uint64_t reserved1_ : 44;

} __attribute__((packed));

static_assert(sizeof(ObSemiStructEncodeHeader) == 16, "size of ObSemiStructEncodeHeader isn't equal to 16");

class ObCSColumnHeader;
struct ObSemiStructEncodeMetaDesc
{
  ObSemiStructEncodeMetaDesc():
    semistruct_header_(nullptr),
    sub_col_headers_(nullptr),
    sub_schema_data_ptr_(nullptr),
    null_bitmap_(nullptr),
    bitmap_size_(0),
    sub_col_meta_len_(0),
    sub_col_meta_ptr_(nullptr)
  {}

  int deserialize(const ObCSColumnHeader &col_header, const uint32_t row_cnt, const char *buf, const int64_t len, int64_t &pos);

  TO_STRING_KV(KP_(semistruct_header), KPC_(semistruct_header), KP_(sub_col_headers), KP_(sub_schema_data_ptr),
      KP_(null_bitmap), K_(bitmap_size), K_(sub_col_meta_len), KP_(sub_col_meta_ptr))

  const ObSemiStructEncodeHeader *semistruct_header_;
  const ObCSColumnHeader* sub_col_headers_;
  const char* sub_schema_data_ptr_;
  const char* null_bitmap_;
  uint32_t bitmap_size_;
  uint32_t sub_col_meta_len_;
  const char* sub_col_meta_ptr_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
