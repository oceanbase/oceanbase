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

#ifndef OCEANBASE_SQL_OB_CREATE_DBLINK_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_DBLINK_RESOLVER_H_
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObCreateDbLinkStmt;
class ObCreateDbLinkResolver: public ObDDLResolver
{
  static const int64_t DBLINK_NAME = 0;
  static const int64_t USER_NAME = 1;
  static const int64_t TENANT_NAME = 2;
  static const int64_t PASSWORD = 3;
  static const int64_t OPT_DRIVER = 4;
  static const int64_t IP_PORT = 5;
  static const int64_t OPT_CLUSTER = 6;
  static const int64_t DBLINK_NODE_COUNT = 7;
public:
  enum DriverType {
    DRV_UNKNOWN = -1,
    DRV_OB = 0,
    DRV_OCI,
  };
  explicit ObCreateDbLinkResolver(ObResolverParams &params);
  virtual ~ObCreateDbLinkResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  int cut_host_string(const ObString &host_string, ObString &ip_port, ObString &conn_string);
  int resolve_conn_string(const ObString &conn_string, const ObString &ip_port_str,
                          sql::ObCreateDbLinkStmt &link_stmt);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateDbLinkResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_DBLINK_RESOLVER_H_*/
