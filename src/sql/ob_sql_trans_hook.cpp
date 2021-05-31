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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/ob_sql_trans_hook.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/oblog/ob_trace_log.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace sql {
int ObSqlTransHook::before_end_trans(ObSQLSessionInfo* session)
{
  int ret = OB_SUCCESS;
  UNUSED(session);
  return ret;
}

int ObSqlTransHook::after_end_trans(
    ObSQLSessionInfo* my_session, ObExclusiveEndTransCallback& callback, int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  UNUSED(callback);
  if (OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session ptr is null", K(ret), K(my_session));
  } else {
    transaction::ObTransDesc& trans_desc = my_session->get_trans_desc();
    LOG_DEBUG("after_end_trans temporary table",
        K(my_session->get_sessid_for_table()),
        K(my_session->get_has_temp_table_flag()),
        K(trans_desc.is_all_select_stmt()),
        K(my_session->get_is_deserialized()));
    if (my_session->get_has_temp_table_flag() && false == trans_desc.is_all_select_stmt()) {
      if (OB_FAIL(my_session->drop_temp_tables(false))) {
        LOG_WARN("fail to drop oracle temporary tables", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
