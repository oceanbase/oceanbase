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
 * This file is for implement of func multimode expr helper
 */

#ifndef OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_
#define OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_

#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObMultiModeExprHelper final
{
public:
  static uint64_t get_tenant_id(ObSQLSessionInfo *session); // json and xml get tenant_id public function
};



};
};

#endif // OCEANBASE_SQL_OB_EXPR_MULTI_MODE_FUNC_HELPER_H_