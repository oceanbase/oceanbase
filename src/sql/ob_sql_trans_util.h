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

#ifndef OCEANBASE_SQL_TRANS_UTIL_
#define OCEANBASE_SQL_TRANS_UTIL_

#include "share/ob_define.h"
#include "sql/ob_sql_define.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObSqlTransUtil
{
public:
  /* 判断一个语句是否应该在远程开启事务 */
  static bool is_remote_trans(bool ac, bool in_trans, ObPhyPlanType ptype)
  {
    return true == ac && false == in_trans && OB_PHY_PLAN_REMOTE == ptype;
  }

  /* 判断是否能够自动开启事务 */
  static bool plan_can_start_trans(bool ac, bool in_trans)
  {
    UNUSED(ac);
    return false == in_trans;
  }

  /* 判断是否能够自动结束当前事务 */
  static bool plan_can_end_trans(bool ac, bool explicit_start_trans)
  {
    return false == explicit_start_trans && true == ac;
  }

  /* 判断cmd是否能够自动结束上一个事务 */
  static bool cmd_need_new_trans(bool ac, bool in_trans)
  {
    UNUSED(ac);
    return true == in_trans;
  }
private:
  ObSqlTransUtil() {};
  ~ObSqlTransUtil() {};
  DISALLOW_COPY_AND_ASSIGN(ObSqlTransUtil);
};
}
}
#endif /* OCEANBASE_SQL_TRANS_UTIL_ */
//// end of header file
