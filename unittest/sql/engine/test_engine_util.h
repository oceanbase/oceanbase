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

#ifndef OCEANBASE_ENGINE_TEST_ENGINE_UTIL_H_
#define OCEANBASE_ENGINE_TEST_ENGINE_UTIL_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{

inline int create_test_session(ObExecContext &ctx)
{
  int ret = 0;
  if (!ctx.get_my_session()) {
    ObSQLSessionInfo *s = new ObSQLSessionInfo;
    if (OB_FAIL(s->test_init(0, 123456789, 123456789, NULL))) {
      delete s;
      return ret;
    } else {
      ctx.set_my_session(s);
    }
  }
  return 0;
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_TEST_ENGINE_UTIL_H_
