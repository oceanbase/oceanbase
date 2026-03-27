/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_CLOSE_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_CLOSE_H_

#include "observer/ob_server_struct.h"
#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPStmtClose : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_CLOSE;
public:
  explicit ObMPStmtClose(const ObGlobalContext &gctx)
      : ObMPBase(gctx), stmt_id_(common::OB_INVALID_STMT_ID)
  {}
  virtual ~ObMPStmtClose() {}

protected:
  int deserialize();
  int process();
  bool is_cursor_close() { return 0 != (stmt_id_ & (1LL << 31)); }
private:
  common::ObPsStmtId stmt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtClose);
};



} //end of namespace observer
} //end of namespace oceanbase

#endif //OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_CLOSE_H_
