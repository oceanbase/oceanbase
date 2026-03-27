/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_RESET_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_RESET_H_

#include "observer/ob_server_struct.h"
#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPStmtReset : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_STMT_RESET;
public:
  explicit ObMPStmtReset(const ObGlobalContext &gctx)
      : ObMPBase(gctx), stmt_id_(common::OB_INVALID_STMT_ID)
  {}
  virtual ~ObMPStmtReset() {}

protected:
  int deserialize();
  int process();
private:
  common::ObPsStmtId stmt_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPStmtReset);
};



} //end of namespace observer
} //end of namespace oceanbase

#endif //OCEANBASE_OBSERVER_MYSQL_OBMP_STMT_RESET_H_
