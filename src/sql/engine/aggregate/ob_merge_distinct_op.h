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

#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/aggregate/ob_distinct_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObMergeDistinctSpec : public ObDistinctSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObMergeDistinctSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
};

class ObMergeDistinctOp : public ObOperator
{
public:
  ObMergeDistinctOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt);
  virtual void destroy() override;

  class Compare
  {
  public:
    Compare() : eval_ctx_(nullptr), cmp_funcs_(nullptr), ret_code_(common::OB_SUCCESS)
    {}

    int init(ObEvalCtx *eval_ctx, const common::ObIArray<ObCmpFunc> *cmp_funcs);
    int equal(const common::ObIArray<ObExpr*> *l,
      const ObChunkDatumStore::StoredRow *r,
      bool &equal);
    int equal_in_batch(const common::ObIArray<ObExpr*> *set_exprs,
                        const int64_t last_idx,
                        const int64_t curr_idx,
                        bool &equal);
    int equal_in_batch(const common::ObIArray<ObExpr*> *set_exprs,
                        const ObChunkDatumStore::StoredRow *r,
                        const int64_t curr_idx,
                        bool &equal);
    ObEvalCtx *eval_ctx_;
    const common::ObIArray<ObCmpFunc> *cmp_funcs_;
    int ret_code_;
  };
  int deduplicate_for_batch(bool has_last, const ObBatchRows *child_brs);
  typedef ObChunkDatumStore::LastStoredRow LastStoreRow;
  bool first_got_row_; // 是否第一次拿数据
  common::ObArenaAllocator alloc_;
  LastStoreRow last_row_;
  Compare cmp_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_MERGE_DISTINCT_OP_H_ */
