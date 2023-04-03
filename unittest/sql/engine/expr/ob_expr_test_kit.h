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

#ifndef OCEANBASE_EXPR_OB_EXPR_TEST_KIT_H_
#define OCEANBASE_EXPR_OB_EXPR_TEST_KIT_H_

#include "lib/oblog/ob_log_module.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server.h"
#include "share/ob_tenant_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_init.h"
#include "sql/code_generator/ob_expr_generator_impl.h"

namespace oceanbase
{
namespace sql
{

// help create old expr && new expr
struct ObExprTestKit
{
  ObExprTestKit();
  int init();

  void destroy();

  int resolve_const_expr(const char *str, ObRawExpr *&expr);

  // resolve const expr and replace arguments with %args
  int create_expr(const char *expr_str, ObObj *args, int arg_cnt,
                  ObRawExpr *raw_expr, ObSqlExpression &old_expr, ObExpr *&expr);

  OB_INLINE int timed_execute(const char *info,
                              const int64_t times,
                              std::function<int(void)> func)
  {
    int ret = common::OB_SUCCESS;
    int64_t start = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < times; i++) {
      if (OB_FAIL(func())) {
        SQL_LOG(WARN, "call function failed", K(ret));
      }
    }
    int64_t end = common::ObTimeUtility::current_time();
    SQL_LOG(INFO, "timed execute", K(info), K(times), K(end - start));
    return ret;
  }

  uint64_t tenant_id_ = 1001;
  ObArenaAllocator allocator_;
  ObArenaAllocator calc_buf_;
  ObArenaAllocator eval_tmp_alloc_;
  ObSQLSessionInfo session_;
  ObRawExprFactory expr_factory_;
  ObSqlCtx sql_ctx_;
  ObExecContext exec_ctx_;
  ObExprCtx expr_ctx_;
  ObEvalCtx eval_ctx_;
};

} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_EXPR_OB_EXPR_TEST_KIT_H_
