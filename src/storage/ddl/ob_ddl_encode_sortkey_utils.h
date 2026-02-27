/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_DDL_ENCODE_SORTKEY_UTILS_H_
#define OCEANBASE_STORAGE_OB_DDL_ENCODE_SORTKEY_UTILS_H_

#include "share/ob_order_perserving_encoder.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/direct_load/ob_direct_load_batch_rows.h"
#include "storage/fts/ob_fts_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDDLEncodeSortkeyUtils
{
public:
  static int fill_encode_sortkey_column_item(const bool is_oracle_mode, ObColumnSchemaItem &item);
  static int prepare_encode_param(
    const ObIArray<ObObjMeta> &column_descs,
    const bool is_oracle_mode,
    ObIArray<share::ObEncParam> &params);
  static int encode_row(
    const ObDatum *datums,
    const int64_t datum_cnt,
    const ObIArray<int64_t> &sortkey_idxs,
    const int64_t init_encode_row_buf_len,
    ObIArray<share::ObEncParam> &params,
    ObIAllocator &allocator,
    ObDatum &res_datum);
  static int encode_batch(
    const blocksstable::ObBatchDatumRows &batch_rows,
    const ObIArray<int64_t> &sortkey_idxs,
    const ObIArray<ObObjMeta> &column_descs,
    const bool is_oracle_mode,
    ObIAllocator &allocator,
    ObDirectLoadBatchRows &encode_sortkeys);
};

class ObFtsSegmentSortRow
{
public:
  ObFtsSegmentSortRow() : encode_datum_(), token_pair_(nullptr) { }
  ~ObFtsSegmentSortRow() = default;
  bool is_null(const int64_t col_idx) const;
  const char *get_cell_payload(const sql::RowMeta &meta, const int64_t col_idx) const;
  uint32_t get_length(const sql::RowMeta &meta, const int64_t col_idx) const;
  TO_STRING_KV(K_(encode_datum), KPC_(token_pair));
public:
  ObDatum encode_datum_;
  const hash::HashMapPair<ObFTToken, int64_t> *token_pair_;
};

class ObFtsSegmentSortCompare
{
public:
  ObFtsSegmentSortCompare(int &ret, const ObDatumCmpFuncType &cmp_func)
    : ret_(ret), cmp_func_(cmp_func), encode_sortkey_(true)
  {}
  ~ObFtsSegmentSortCompare() = default;
  void fallback_to_disable_encode_sortkey() { encode_sortkey_ = false; }
  bool operator()(const ObFtsSegmentSortRow *l, const ObFtsSegmentSortRow *r) const { return compare(l, r); }
  bool compare(const ObFtsSegmentSortRow *l, const ObFtsSegmentSortRow *r) const;
public:
  int &ret_;
  ObDatumCmpFuncType cmp_func_;
  bool encode_sortkey_;
};

class ObFtsSegmentSort
{
public:
  ObFtsSegmentSort();
  ~ObFtsSegmentSort() = default;
  static constexpr int64_t INIT_ENCODE_ROW_BUF_LEN = 16;
  int init(
    const ObObjMeta &word_meta,
    const bool is_oracle_mode,
    const int64_t row_count,
    lib::MemoryContext *mem_context);
  int add_row(const hash::HashMapPair<ObFTToken, int64_t> &token_pair);
  int sort_inmem_data();
  TO_STRING_KV(K_(word_meta), K_(can_encode_sortkey), K(rows_.count()));
public:
  bool is_inited_;
  ObObjMeta word_meta_;
  bool can_encode_sortkey_;
  lib::MemoryContext *mem_context_;
  ObSEArray<share::ObEncParam, 1> params_;
  ObFixedArray<ObFtsSegmentSortRow, ObIAllocator> rows_;
  ObFixedArray<ObFtsSegmentSortRow *, ObIAllocator> row_ptrs_;
  DISALLOW_COPY_AND_ASSIGN(ObFtsSegmentSort);
};

} //end storage
} //end oceanbase

#endif //OCEANBASE_STORAGE_OB_DDL_ENCODE_SORTKEY_UTILS_H_
