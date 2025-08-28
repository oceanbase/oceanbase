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

#ifndef OCEANBASE_LOGSERVICE_COMMON_TABLE_OPERATOR_H_
#define OCEANBASE_LOGSERVICE_COMMON_TABLE_OPERATOR_H_

#include "common/ob_timeout_ctx.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_macro_utils.h"     // Macros
#include "share/config/ob_server_config.h"
#include "share/ob_share_util.h"

namespace oceanbase
{
namespace logservice
{
class ObLogTableOperator
{
public:
  ObLogTableOperator() { }
  ~ObLogTableOperator() { }
public:
  template <typename TableOperator>
  int exec_read(const uint64_t &tenant_id,
                const common::ObSqlString &sql,
                ObISQLClient &client,
                TableOperator &table_operator)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid argument", KR(ret), K(tenant_id));
    } else {
      common::ObTimeoutCtx ctx;
      const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
      if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to get exec tenant id", KR(ret), K(tenant_id));
      } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
        SHARE_LOG(WARN, "failed to set default timeout ctx", KR(ret), K(default_timeout));
      } else {
        HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
          common::sqlclient::ObMySQLResult *result = NULL;
          if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
            SHARE_LOG(WARN, "failed to read", KR(ret), K(tenant_id), K(sql));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            SHARE_LOG(WARN, "failed to get sql result", KR(ret));
          } else {
            while (OB_SUCC(ret) && OB_SUCC(result->next())) {
              if (OB_FAIL(table_operator(result))) {
                SHARE_LOG(WARN, "failed to read cell from result", KR(ret), K(sql));
              }
            }  // end while
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            } else if (OB_FAIL(ret)) {
              SHARE_LOG(WARN, "failed to get ls", KR(ret));
            } else {
              ret = OB_ERR_UNEXPECTED;
              SHARE_LOG(WARN, "ret can not be success", KR(ret));
            }
          }  // end heap var 
        }
      }//end else
    }
    return ret;
  }
};

} // namespace logservice
} // namespace oceanbase

#endif
