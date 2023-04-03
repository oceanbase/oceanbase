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
  common::ObString db_name_;
  char db_name_conv_buf[common::OB_MAX_DATABASE_NAME_BUF_LENGTH];
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OBMP_INIT_DB_H */
