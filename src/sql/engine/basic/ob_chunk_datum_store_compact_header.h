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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_CHUNK_DATUM_STORE_COMPACT_HEADER_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_CHUNK_DATUM_STORE_COMPACT_HEADER_H_

#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_unify_serialize.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

struct ObChunkDatumStoreReplayCtx
{};

struct ObChunkDatumStoreCompactHeader
{
  struct BlockDesc
  {
    BlockDesc() : compact_block_start_(0), compact_block_end_(0), rebuilt_size_(0), row_cnt_(0)
    {}
    int64_t compact_block_start_;
    int64_t compact_block_end_;
    int64_t rebuilt_size_;
    int32_t row_cnt_;
    TO_STRING_KV(K_(compact_block_start), K_(compact_block_end), K_(rebuilt_size), K_(row_cnt));
  };

  ObChunkDatumStoreCompactHeader() :
    store_(nullptr), fixed_lens_(nullptr), fixed_len_cnt_(0), compact_buf_(nullptr), compact_buf_len_(0),
    decoded_column_cnt_(0), block_descs_()
  {}
  ObChunkDatumStoreCompactHeader(const ObChunkDatumStore &store, const uint32_t *fixed_lens,
                                 const int64_t fixed_len_cnt) : ObChunkDatumStoreCompactHeader()
  {
    store_ = const_cast<ObChunkDatumStore *>(&store);
    fixed_lens_ = fixed_lens;
    fixed_len_cnt_ = fixed_len_cnt;
  }
  OB_UNIS_VERSION(common::serialization::SER_COMPACT_OPT_VERSION);

public:
  int replay_to(ObChunkDatumStore &dst, ObChunkDatumStoreReplayCtx &ctx) const;

  ObChunkDatumStore *store_;
  const uint32_t *fixed_lens_;
  int64_t fixed_len_cnt_;
  const char *compact_buf_;
  int64_t compact_buf_len_;
  int32_t decoded_column_cnt_;
  common::ObSEArray<BlockDesc, 4> block_descs_;
  TO_STRING_KV(K_(fixed_len_cnt), K_(compact_buf_len), K_(decoded_column_cnt), K_(block_descs));
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_CHUNK_DATUM_STORE_COMPACT_HEADER_H_
