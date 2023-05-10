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
  static const int64_t IF_NOT_EXIST = 0;
  static const int64_t DBLINK_NAME = 1;
  static const int64_t USER_NAME = 2;
  static const int64_t TENANT_NAME = 3;
  static const int64_t DATABASE_NAME = 4;
  static const int64_t PASSWORD = 5;
  static const int64_t OPT_DRIVER = 6;
  static const int64_t IP_PORT = 7;
  static const int64_t OPT_CLUSTER =8;
  static const int64_t OPT_REVERSE_LINK = 9;
  static const int64_t DBLINK_NODE_COUNT = 10;

  static const int64_t REVERSE_LINK_USER_NAME = 0;
  static const int64_t REVERSE_LINK_TENANT_NAME = 1;
  static const int64_t REVERSE_LINK_PASSWORD = 2;
  static const int64_t REVERSE_LINK_IP_PORT = 3;
  static const int64_t REVERSE_LINK_OPT_CLUSTER = 4;
  static const int64_t REVERSE_DBLINK_NODE_COUNT = 5;
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
  int resolve_opt_reverse_link(const ParseNode *node, sql::ObCreateDbLinkStmt *link_stmt, DriverType drv_type);
  int cut_host_string(const ObString &host_string, ObString &ip_port, ObString &conn_string);
  int resolve_conn_string(const ObString &conn_string, const ObString &ip_port_str,
                          sql::ObCreateDbLinkStmt &link_stmt);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateDbLinkResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_DBLINK_RESOLVER_H_*/
