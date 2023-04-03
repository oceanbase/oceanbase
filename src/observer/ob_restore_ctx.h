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

#ifndef OCEANBASE_OBSERVER_OB_RESTORE_CTX_H_
#define OCEANBASE_OBSERVER_OB_RESTORE_CTX_H_

#include "observer/ob_restore_sql_modifier.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace sql
{
class ObSql;
}
namespace observer
{
class ObVTIterCreator;

class ObRestoreCtx
{
public:
  ObRestoreCtx()
    : schema_service_(NULL),
      sql_client_(NULL),
      ob_sql_(NULL),
      vt_iter_creator_(NULL),
      ls_table_operator_(NULL),
      server_config_(NULL),
      rs_rpc_proxy_(NULL)
  {}
  ~ObRestoreCtx() {}
  bool is_valid()
  {
    return NULL != schema_service_
        && NULL != sql_client_
        && NULL != ob_sql_
        && NULL != vt_iter_creator_
        && NULL != ls_table_operator_
        && NULL != server_config_
        && NULL != rs_rpc_proxy_;
  }
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_client_;
  sql::ObSql *ob_sql_;
  ObVTIterCreator *vt_iter_creator_;
  const share::ObLSTableOperator *ls_table_operator_;
  common::ObServerConfig *server_config_;
  obrpc::ObCommonRpcProxy *rs_rpc_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRestoreCtx);
};

} // end namespace observer
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_RESTORE_CTX_H_
