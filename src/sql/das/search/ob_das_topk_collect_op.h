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

#ifndef OCEANBASE_SQL_OB_DAS_TOPK_COLLECT_OP_H_
#define OCEANBASE_SQL_OB_DAS_TOPK_COLLECT_OP_H_

#include "ob_i_das_search_op.h"
#include "sql/das/ob_das_ir_define.h"
#include "ob_das_topk_utils.h"

namespace oceanbase
{

namespace sql
{

struct ObDASTopKCollectCtDef : ObIDASSearchCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASTopKCollectCtDef(common::ObIAllocator &alloc)
    : ObIDASSearchCtDef(alloc, DAS_OP_TOPK_COLLECT),
      limit_(nullptr)
  {}
  virtual ~ObDASTopKCollectCtDef() {}
  INHERIT_TO_STRING_KV("ObIDASSearchCtDef", ObIDASSearchCtDef,
      KPC_(limit));

  ObExpr *limit_;
};

struct ObDASTopKCollectRtDef : ObIDASSearchRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASTopKCollectRtDef()
    : ObIDASSearchRtDef(DAS_OP_TOPK_COLLECT)
  {}
  virtual ~ObDASTopKCollectRtDef() {}

  virtual int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override { return OB_SUCCESS; }
  virtual int generate_op(ObDASSearchCost lead_cost, ObDASSearchCtx &search_ctx, ObIDASSearchOp *&op) override;
};

struct ObDASTopKCollectOpParam final : public ObIDASSearchOpParam
{
  ObDASTopKCollectOpParam()
    : ObIDASSearchOpParam(DAS_SEARCH_OP_TOPK_COLLECT),
      limit_(-1),
      child_(nullptr)
  {}
  virtual ~ObDASTopKCollectOpParam() {}

  bool is_valid() const
  { return limit_ > 0 && nullptr != child_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
  INHERIT_TO_STRING_KV("ObDASTopKCollectOpParam", ObIDASSearchOpParam, K_(limit), KP_(child));

  int64_t limit_;
  ObIDASSearchOp *child_;
};

class ObDASTopKCollectOp final : public ObIDASSearchOp
{
public:
  ObDASTopKCollectOp(ObDASSearchCtx &search_ctx);
  virtual ~ObDASTopKCollectOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override { return OB_NOT_SUPPORTED; }
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  { return OB_NOT_SUPPORTED; }

private:
  int load_results();

private:
  ObIDASSearchOp *child_;
  ObDASTopKItemCmp cmp_;
  ObDASTopKHeap heap_;
  ObFixedArray<ObDocIdExt, ObIAllocator> id_cache_;
  ObDASRowID curr_id_;
  int64_t limit_;
  bool is_loaded_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DAS_TOPK_COLLECT_OP_H_
