/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OBMP_INIT_DB_H
#define _OBMP_INIT_DB_H
#include "observer/mysql/obmp_base.h"
#include "rpc/obmysql/ob_mysql_packet.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObMPInitDB: public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_INIT_DB;
public:
  explicit ObMPInitDB(const ObGlobalContext &gctx)
      :ObMPBase(gctx),
       db_name_()
  {}
  virtual ~ObMPInitDB() {}
protected:
  int process();
  int deserialize();
private:
  int do_process(sql::ObSQLSessionInfo *session);
private:
  int get_catalog_id_(sql::ObSQLSessionInfo &session, ObSchemaGetterGuard &schema_guard, uint64_t &catalog_id);
  common::ObString catalog_name_;
  common::ObString db_name_;
  char db_name_conv_buf[common::OB_MAX_DATABASE_NAME_BUF_LENGTH];
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OBMP_INIT_DB_H */
