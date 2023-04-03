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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_create_profile_stmt.h"

#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_strings.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObUserProfileStmt::ObUserProfileStmt(ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_USER_PROFILE),
      create_profile_arg_()
{
}

ObUserProfileStmt::ObUserProfileStmt()
    : ObDDLStmt(NULL, stmt::T_USER_PROFILE),
      create_profile_arg_()
{
}

ObUserProfileStmt::~ObUserProfileStmt()
{
}
