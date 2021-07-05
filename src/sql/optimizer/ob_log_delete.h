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

#ifndef _OB_LOG_DELETE_H
#define _OB_LOG_DELETE_H 1
#include "ob_log_del_upd.h"

namespace oceanbase {
namespace sql {
class ObLogDelete : public ObLogDelUpd {
public:
  ObLogDelete(ObLogPlan& plan) : ObLogDelUpd(plan)
  {}
  virtual ~ObLogDelete()
  {}
  int calc_cost();
  /**
   *  Add needed expr to context
   *
   *  For 'delete', we need to add(in this order)
   *  1. Primary keys
   *  2. columns in some index(no duplicate)
   */
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;

  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;

  virtual int est_cost();

  virtual int copy_without_child(ObLogicalOperator*& out)
  {
    out = NULL;
    return common::OB_SUCCESS;
  }
  virtual const char* get_name() const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDelete);
};
}  // namespace sql
}  // namespace oceanbase
#endif
