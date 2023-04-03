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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_position.h"
#include "sql/engine/expr/ob_expr_instr.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprPosition::ObExprPosition(ObIAllocator &alloc)
    : ObLocationExprOperator(alloc, T_FUN_SYS_POSITION, N_POSITION, 2, NOT_ROW_DIMENSION) {}


ObExprPosition::~ObExprPosition() {
  // TODO Auto-generated
}

}//end of namespace sql
}//end of namespace oceanbase
