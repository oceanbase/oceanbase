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

#ifndef OCEANBASE_SQL_OB_ANALYZE_EXECUTOR_H_
#define OCEANBASE_SQL_OB_ANALYZE_EXECUTOR_H_
#include "lib/string/ob_sql_string.h"
#include "sql/resolver/ddl/ob_analyze_stmt.h"
#include "share/stat/ob_stat_define.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}
namespace obrpc
{
struct ObUpdateStatCacheArg;
}
namespace sql
{
class ObExecContext;
class ObAnalyzeStmt;
class ObAnalyzeExecutor
{
public:
  static const int64_t FIXED_HISTOGRAM_SEED = 1;
  static const int64_t BUCKET_BITS = 10; // ln2(1024) = 10;
  static const int64_t TOTAL_BUCKET_BITS = 40; // 6 groups
  static const int64_t NUM_LLC_BUCKET =  (1 << BUCKET_BITS);

public:
  ObAnalyzeExecutor() {}
  virtual ~ObAnalyzeExecutor() {}
  int execute(ObExecContext &ctx, ObAnalyzeStmt &stmt);
  DISALLOW_COPY_AND_ASSIGN(ObAnalyzeExecutor);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* SRC_SQL_ENGINE_CMD_OB_ANALYZE_EXECUTOR_H_ */
