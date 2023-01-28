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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_QUIT_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_QUIT_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPQuit
    : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_QUIT;

  explicit ObMPQuit(const ObGlobalContext &gctx)
      : ObMPBase(gctx)
  {}
  virtual ~ObMPQuit() {}

protected:
  int process();
  int deserialize() { return common::OB_SUCCESS; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMPQuit);
}; // end of class ObMPQuit

int ObMPQuit::process()
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *session = NULL;
  if (OB_FAIL(get_session(session))) {
    LOG_WARN("fail to get session", K(ret));
  } else {
    // set NORMAL_QUIT state.
    session->set_disconnect_state(NORMAL_QUIT);
  }
  if (NULL != session) {
    revert_session(session);
  }
  SERVER_LOG(INFO, "quit");
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_QUIT_H_
