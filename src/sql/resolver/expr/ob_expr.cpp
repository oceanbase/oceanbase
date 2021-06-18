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

#define USING_LOG_PREFIX SQL
#include "sql/resolver/expr/ob_expr.h"
#include "lib/oblog/ob_log.h"
namespace oceanbase {
namespace jit {
namespace expr {
int64_t ObExpr::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), K_(expr_class));
  J_OBJ_END();
  return pos;
}

using namespace ::oceanbase::common;

//// member function definitions
//////////////////////////////////////////////////////////////////////
}  // namespace expr
}  // namespace jit
}  // namespace oceanbase
