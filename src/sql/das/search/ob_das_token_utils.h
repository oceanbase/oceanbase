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

#ifndef OCEANBASE_SQL_OB_DAS_TOKEN_UTILS_H_
#define OCEANBASE_SQL_OB_DAS_TOKEN_UTILS_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/retrieval/ob_text_retrieval_token_iter.h"
#include "storage/retrieval/ob_inv_idx_param_estimator.h"

namespace oceanbase
{
namespace sql
{

struct ObDASTokenOpParam;

class ObDASTokenOpHelper
{
public:
  ObDASTokenOpHelper(ObDASSearchCtx &search_ctx);
  ~ObDASTokenOpHelper() {}
  TO_STRING_EMPTY();
  int init(const ObDASTokenOpParam &param);
  int init_bm25_param_estimator(ObBM25IndexParamEstimator &estimator);
  int init_text_retrieval_iter(ObTextRetrievalBlockMaxIter &iter);
  int rescan();
  int close();
private:
  int init_scan_param(const ObDASTokenOpParam &param);
  void init_scan_range(
      const ObDASIRScanCtDef &ir_ctdef,
      const ObString &query_token,
      ObNewRange &scan_range);
  void init_scan_iter_param(
      const ObDASScalarScanCtDef &scan_ctdef,
      ObDASScalarScanRtDef &scan_rtdef,
      ObDASScanIterParam &iter_param);
  void init_text_retrieval_param(
      ObTextRetrievalScanIterParam &text_retrieval_param);
private:
  static constexpr int64_t OBJ_BUF_SIZE = 4;
  ObDASSearchCtx &search_ctx_;
  ObArenaAllocator arena_allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObTabletID inv_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  ObTableScanParam inv_idx_agg_param_;
  ObDASScanIter inv_idx_scan_iter_;
  ObDASScanIter inv_idx_agg_iter_;
  storage::ObBlockMaxScoreIterParam *block_max_param_;
  ObNewRange inv_idx_scan_range_;
  sql::ObEvalCtx *eval_ctx_;
  ObObj obj_buf_[OBJ_BUF_SIZE];
  bool use_rich_format_;
  bool is_inited_;
};

} // namespace sql
} // namesapce oceanbase

#endif // OCEANBASE_SQL_OB_DAS_TOKEN_UTILS_H_