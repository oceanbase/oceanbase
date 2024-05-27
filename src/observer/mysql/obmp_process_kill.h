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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_KILL_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_KILL_H_

#include "observer/mysql/obmp_query.h"

namespace oceanbase
{
namespace observer
{

class ObMPProcessKill : public ObMPQuery
{
public:
  const static int64_t KILL_SQL_BUF_SIZE = 64; //actually 16 byte is enough, 32 byte is for safty
  //static const obmysql::ObMySQLCmd COM = obmysql::COM_PROCESS_INFO;
  explicit ObMPProcessKill(const ObGlobalContext &gctx);
  virtual ~ObMPProcessKill();
protected:
  int deserialize() override;
private:
  char kill_sql_buf_[KILL_SQL_BUF_SIZE];
  DISALLOW_COPY_AND_ASSIGN(ObMPProcessKill);
}; // end of class ObMPProcessKill

} // end of namespace observer
} // end of namespace oceanbase


#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_PROCESS_KILL_H_
