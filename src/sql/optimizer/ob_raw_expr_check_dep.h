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

#ifndef _OB_RAW_EXPR_CHECK_DEP_H
#define _OB_RAW_EXPR_CHECK_DEP_H 1
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase {
namespace sql {
  class ObRawExpr;
  class ObRawExprCheckDep
  {
  public:
  ObRawExprCheckDep(common::ObIArray<ObRawExpr *> &dep_exprs,
                    common::ObBitSet<common::OB_MAX_BITSET_SIZE> &deps,
                    bool is_access)
      : dep_exprs_(dep_exprs),
        dep_indices_(&deps),
        is_access_(is_access) { }
    virtual ~ObRawExprCheckDep() {}

    /**
     *  The starting point
     */
    int check(const ObRawExpr &expr);

    int check(const ObIArray<ObRawExpr *> &exprs);

    const common::ObBitSet<common::OB_MAX_BITSET_SIZE> *get_dep_indices() const { return dep_indices_; }
  private:
    int check_expr(const ObRawExpr &expr, bool &found);
  private:
    common::ObIArray<ObRawExpr *> &dep_exprs_;
    common::ObBitSet<common::OB_MAX_BITSET_SIZE> *dep_indices_;
    bool is_access_; // mark whether we are do project pruning for access exprs
    DISALLOW_COPY_AND_ASSIGN(ObRawExprCheckDep);
  };
}
}

#endif // _OB_RAW_EXPR_CHECK_DEPENDENCY_H

