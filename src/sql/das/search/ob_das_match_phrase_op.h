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

#ifndef OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_
#define OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_

#include "sql/das/ob_das_ir_define.h"
#include "ob_i_das_search_op.h"
#include "ob_das_token_utils.h"
#include "storage/retrieval/ob_text_retrieval_token_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace storage
{
class ObPhraseMatchCounter;
}

namespace share
{
class ObFTSPositionListStore;
}

namespace sql
{

struct ObDASMatchPhraseOpParam : public ObIDASSearchOpParam
{
  ObDASMatchPhraseOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_MATCH_PHRASE),
      allocator_(nullptr),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      block_max_param_(nullptr),
      token_ids_(),
      query_tokens_(),
      counter_(nullptr),
      boost_(0.0),
      use_rich_format_(false)
  {}
  virtual ~ObDASMatchPhraseOpParam() {}

  bool is_valid() const
  {
    return nullptr != allocator_
        && nullptr != ir_ctdef_
        && nullptr != ir_rtdef_
        && nullptr != block_max_param_
        && !query_tokens_.empty()
        && nullptr != counter_
        && boost_ > 0.0;
  }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    // leaf node, no children
    return OB_SUCCESS;
  }
  INHERIT_TO_STRING_KV("ObDASMatchPhraseOpParam", ObIDASSearchOpParam,
      KPC_(ir_ctdef),
      KPC_(ir_rtdef),
      K_(query_tokens),
      K_(boost),
      K_(use_rich_format));

  ObIAllocator *allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  storage::ObBlockMaxScoreIterParam *block_max_param_;
  ObSEArray<int64_t, 16> token_ids_;
  ObSEArray<ObString, 16> query_tokens_;
  ObPhraseMatchCounter *counter_;
  double boost_;
  bool use_rich_format_;
};

class ObDASMatchPhraseOp : public ObIDASSearchOp
{
public:
  ObDASMatchPhraseOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASMatchPhraseOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override;
  int do_calc_max_score(double &threshold) override;

private:
  int find_intersection(ObDASRowID &rowid, double &score);
  int evaluate(double &score);
  int calculate_total_token_weight_on_demand();
  int estimate_bm25_param_on_demand();
private:
  ObIAllocator *allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObFixedArray<int64_t, ObIAllocator> token_ids_;
  ObFixedArray<ObDASTokenOpHelper *, ObIAllocator> token_helpers_;
  ObFixedArray<ObTextRetrievalBlockMaxIter *, ObIAllocator> token_iters_;
  ObBM25IndexParamEstimator estimator_;
  bool bm25_param_estimated_;
  double total_token_weight_;
  ObFTSPositionListStore *decoder_;
  ObPhraseMatchCounter *counter_;
  double boost_;
  bool use_rich_format_;
  ObDASRowID curr_id_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_MATCH_PHRASE_OP_H_
