/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_inner_sql_read_context.h"

namespace oceanbase
{
namespace observer
{

ObInnerSQLReadContext::ObInnerSQLReadContext(ObInnerSQLConnection &conn)
    : conn_ref_(conn), vt_iter_factory_(*conn.get_vt_iter_creator()),
      result_(conn.get_session(), conn.is_inner_session(), conn.get_diagnostic_info())
{
}

ObInnerSQLReadContext::~ObInnerSQLReadContext()
{
  if (this == conn_ref_.get_conn().get_prev_read_ctx()) {
    conn_ref_.get_conn().get_prev_read_ctx() = NULL;
  }
}

} // end of namespace observer
} // end of namespace oceanbase
