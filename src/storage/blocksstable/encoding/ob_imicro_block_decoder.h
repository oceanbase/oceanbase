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

#ifndef OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_IMICRO_BLOCK_DECODER_H_
#define OB_STORAGE_BLOCKSSTABLE_ENCODING_OB_IMICRO_BLOCK_DECODER_H_

#include "lib/container/ob_bitmap.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"

namespace oceanbase
{
namespace blocksstable
{

class ObIMicroBlockDecoder : public ObIMicroBlockReader
{
public:
  ObIMicroBlockDecoder() : ObIMicroBlockReader() {}
  virtual ~ObIMicroBlockDecoder() {}
  virtual int compare_rowkey(
    const ObDatumRowkey &rowkey, const int64_t index, int32_t &compare_result) override = 0;
  virtual int compare_rowkey(const ObDatumRange &range, const int64_t index,
    int32_t &start_key_compare_result, int32_t &end_key_compare_result) = 0;

  // Filter interface for filter pushdown
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObPhysicalFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) = 0;
  virtual int filter_pushdown_filter(const sql::ObPushdownFilterExecutor *parent,
    sql::ObWhiteFilterExecutor &filter, const sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap) = 0;
  virtual int get_rows(const common::ObIArray<int32_t> &cols,
    const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
    const int32_t *row_ids, const char **cell_datas, const int64_t row_cap,
    common::ObIArray<ObSqlDatumInfo> &datum_infos, const int64_t datum_offset = 0) = 0;
  virtual bool can_apply_black(const common::ObIArray<int32_t> &col_offsets) const = 0;
  virtual int filter_black_filter_batch(const sql::ObPushdownFilterExecutor *parent,
    sql::ObBlackFilterExecutor &filter, sql::PushdownFilterInfo &pd_filter_info,
    common::ObBitmap &result_bitmap, bool &filter_applied) = 0;
  virtual int find_bound(
      const ObDatumRowkey &key,
      const bool lower_bound,
      const int64_t begin_idx,
      int64_t &row_idx,
      bool &equal) override;
  virtual int get_rows(
      const common::ObIArray<int32_t> &cols,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const int64_t row_cap,
      const char **cell_datas,
      const int64_t vec_offset,
      uint32_t *len_array,
      sql::ObEvalCtx &eval_ctx,
      sql::ObExprPtrIArray &exprs) = 0;

protected:
  virtual int find_bound(const ObDatumRange &range, const int64_t begin_idx, int64_t &row_idx,
    bool &equal, int64_t &end_key_begin_idx, int64_t &end_key_end_idx) override;

  // For column store
  virtual int find_bound(const ObDatumRowkey &key, const bool lower_bound, const int64_t begin_idx,
    const int64_t end_idx, int64_t &row_idx, bool &equal) override;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif
