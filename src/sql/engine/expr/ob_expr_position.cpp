/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_position.h"

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
