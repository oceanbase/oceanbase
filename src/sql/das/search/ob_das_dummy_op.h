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

#ifndef OCEANBASE_SQL_OB_DAS_DUMMY_OP_H_
#define OCEANBASE_SQL_OB_DAS_DUMMY_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{

struct ObDASDummyOpParam : public ObIDASSearchOpParam
{
public:
  ObDASDummyOpParam() : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY) {}
  virtual ~ObDASDummyOpParam() {}
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    // leaf node, no children
    return OB_SUCCESS;
  }
};

// For scenario that we found a query would be empty set on runtime analysis, use dummy op as placeholder operator.
class ObDASDummyOp : public ObIDASSearchOp
{
public:
  ObDASDummyOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx) {}
  virtual ~ObDASDummyOp() {}

private:
  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { return OB_SUCCESS; }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override
  {
    return OB_ITER_END;
  }
  int do_next_rowid(ObDASRowID &next_id, double &score) override { return OB_ITER_END; }
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    return OB_ITER_END;
  }
  int do_calc_max_score(double &threshold) override { threshold = 0.0; return OB_SUCCESS; }
};

}
}
#endif // OCEANBASE_SQL_OB_DAS_DUMMY_OP_H_