/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_OB_DAS_TOKEN_OP_H_
#define OCEANBASE_SQL_OB_DAS_TOKEN_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/retrieval/ob_text_retrieval_token_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace sql
{
class ObDASScalarScanCtDef;
class ObDASScalarScanRtDef;
// Operator and parameters for single token retrieval on one single fulltext index.

struct ObDASTokenOpParam : public ObIDASSearchOpParam
{
public:
  ObDASTokenOpParam();
  virtual ~ObDASTokenOpParam() {}

  bool is_valid() const;
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    // leaf node, no children
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("ObDASTokenOpParam", ObIDASSearchOpParam,
      KPC_(ir_ctdef),
      KPC_(ir_rtdef),
      KPC_(block_max_param),
      K_(token_boost),
      K_(query_token),
      K_(use_rich_format));

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  storage::ObBlockMaxScoreIterParam *block_max_param_;
  double token_boost_;
  common::ObString query_token_;
  bool use_rich_format_;
};

class ObDASTokenOp : public ObIDASSearchOp
{
public:
  ObDASTokenOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASTokenOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(const ObDASRowID &target,
                         const bool inclusive,
                         const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

private:
  int init_scan_param(const ObDASTokenOpParam &param);
  int init_scan_range(const ObDASIRScanCtDef &ir_ctdef, const ObString &query_token, ObNewRange &scan_range);
  void init_scan_iter_param(
      const ObDASScalarScanCtDef &scan_ctdef,
      ObDASScalarScanRtDef &scan_rtdef,
      ObDASScanIterParam &iter_param);
  void init_text_retrieval_param();
  int init_compact_id_rows();
  int do_inv_idx_bm25_param_estimation_on_demand();
  int init_block_max_iter_on_demand();
  void datum_to_compact_row(const ObDatum &id_datum, ObCompactRow &row);
  int reset_inv_scan_range();
private:
  static constexpr int64_t OBJ_BUF_SIZE = 4;
  ObArenaAllocator arena_allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObTabletID inv_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  ObTableScanParam inv_idx_agg_param_;
  ObDASScanIter inv_idx_scan_iter_;
  ObDASScanIter inv_idx_agg_iter_;
  storage::ObBlockMaxScoreIterParam *block_max_param_;
  ObTextRetrievalScanIterParam text_retrieval_param_;
  storage::ObTextRetrievalBlockMaxIter text_retrieval_iter_;
  ObBM25IndexParamEstimator bm25_param_estimator_;
  ObNewRange inv_idx_scan_range_;
  double token_boost_;
  sql::ObEvalCtx *eval_ctx_;
  ObObj obj_buf_[OBJ_BUF_SIZE];
  ObCompactRow *curr_id_;
  ObCompactRow *min_id_;
  ObCompactRow *max_id_;
  bool is_simple_doc_id_;
  bool use_rich_format_;
  bool bm25_param_estimated_;
  bool block_max_inited_;
  bool is_inited_;
};

}
}

#endif // OCEANBASE_SQL_OB_DAS_TOKEN_OP_H_
