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

#include "ob_inner_sql_read_context.h"
#include "ob_inner_sql_connection.h"

namespace oceanbase
{
namespace observer
{

ObInnerSQLReadContext::ObInnerSQLReadContext(ObInnerSQLConnection &conn)
    : conn_ref_(conn), vt_iter_factory_(*conn.get_vt_iter_creator()), result_(conn.get_session())
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
