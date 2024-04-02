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
#ifndef OCEANBASE_BASIC_OB_COMPACT_STORE_H_
#define OCEANBASE_BASIC_OB_COMPACT_STORE_H_
#include "share/ob_define.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "lib/alloc/alloc_struct.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "src/share/ob_ddl_common.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/chunk_store/ob_store_row_convertor.h"
#include "sql/engine/basic/ob_temp_row_store.h"

namespace oceanbase
{
namespace sql
{

class ObCompactStore final : public ObTempRowStore
{
  OB_UNIS_VERSION_V(1);
public:
  using ObTempRowStore::add_row;
  explicit ObCompactStore(common::ObIAllocator *alloc = NULL) : ObTempRowStore(alloc),
                      convertor_(), row_meta_(allocator_), start_iter_(false), inited_(false), iter_()
  {
  };
  virtual ~ObCompactStore() {reset();}
  void reset();
  bool has_next() { return iter_.has_next(); }
  int init(const int64_t mem_limit,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           const int64_t mem_ctx_id = common::ObCtxIds::DEFAULT_CTX_ID,
           const char *label = common::ObModIds::OB_SQL_ROW_STORE,
           const bool enable_dump = true,
           const uint32_t row_extra_size = 0,
           const bool reorder_fixed_expr = true,
           const bool enable_trunc = true,
           const ObCompressorType compress_type = NONE_COMPRESSOR,
           const ExprFixedArray *exprs = nullptr);

  int init_iter() {
    return iter_.init(this);
  }

  // called in sort_op
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBitVector &skip, const int64_t batch_size,
                int64_t &stored_rows_count);

  int add_row(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx);
  int add_row(const ObChunkDatumStore::StoredRow &src_sr);

  int get_next_row(const ObChunkDatumStore::StoredRow *&sr);

  // TODO
  void set_blk_holder(ObTempBlockStore::BlockHolder *blk_holder) { iter_.set_blk_holder(blk_holder); }
  //int64_t get_row_cnt() const { return row_cnt_; }
  INHERIT_TO_STRING_KV("ObTempRowStore", ObTempRowStore,
                       K_(row_meta), K_(start_iter), K_(iter));

private:
  int init_convertor();

  int add_batch_fallback(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                         const ObBitVector &skip, const int64_t batch_size,
                         const common::ObIArray<int64_t> &selector, const int64_t size);

private:
  ObStoreRowConvertor convertor_;
  RowMeta row_meta_;
  bool start_iter_;
  bool inited_;
  ObTempRowStore::Iterator iter_;
}
;

} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_BASIC_OB_COMPACT_STORE_H_