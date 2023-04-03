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
