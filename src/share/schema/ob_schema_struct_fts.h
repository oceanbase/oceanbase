/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H
#define _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H

#include <cstdint>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

enum ObFTSIndexType : uint8_t {
  OB_FTS_INDEX_TYPE_INVALID = 0,
  OB_FTS_INDEX_TYPE_FILTER = 1,
  OB_FTS_INDEX_TYPE_MATCH = 2,
  OB_FTS_INDEX_TYPE_PHRASE_MATCH = 3,
  OB_FTS_INDEX_TYPE_MAX
};

inline constexpr ObFTSIndexType DEFAULT_FTS_INDEX_TYPE = OB_FTS_INDEX_TYPE_MATCH;

struct ObFTSIndexParams {
  ObFTSIndexParams()
      : fts_index_type_(OB_FTS_INDEX_TYPE_INVALID)
  {
  }
  ~ObFTSIndexParams() { }
  TO_STRING_KV(K_(fts_index_type));

  static int from_param_str(const common::ObString &param_str, ObFTSIndexParams &params);
  int append_param_str(char *buf, const int64_t buf_len, int64_t &pos) const;

  bool is_valid() const { return fts_index_type_ != OB_FTS_INDEX_TYPE_INVALID; }

  void reset() { fts_index_type_ = OB_FTS_INDEX_TYPE_INVALID; }
  ObFTSIndexType fts_index_type_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

 #endif // _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H
