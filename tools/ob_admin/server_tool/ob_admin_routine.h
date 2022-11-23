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

#ifndef _OBADMIN_ROUTINE_H_
#define _OBADMIN_ROUTINE_H_

#include <stdint.h>
#include <string>
#include <vector>
#include "lib/net/ob_addr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_net_client.h"
#include "share/ob_rpc_struct.h"

using std::string;

namespace oceanbase {
using namespace common;
namespace tools {
class ObAdminRoutine
{
public:
  ObAdminRoutine(const string &action_name, int version = 1, const string &args = "");
  virtual ~ObAdminRoutine();

  virtual int process() = 0;

  bool match(const string &cmd) const;
  void set_command(const string &cmd)
  {
    cmd_ = cmd;
  }
  void set_timeout(int64_t timeout)
  {
    timeout_ = timeout;
  }

  const string& action_name() const
  {
    return action_name_;
  }

  const string& target() const
  {
    return target_;
  }

  const string usage() const
  {
    return action_name_ + " " + args_;
  }

  void set_client(obrpc::ObSrvRpcProxy* client) { client_ = client; }
protected:
  const string action_name_;
  string args_;
  string cmd_;
  const int version_;
  int64_t timeout_;
  string target_;
  obrpc::ObSrvRpcProxy* client_;
}; /* end of class ObAdminRoutine */


class RoutineComparer
{
public:
  RoutineComparer(const string &cmd)
      : cmd_(cmd)
  {}

  bool operator ()(ObAdminRoutine *routine)
  {
    return routine->match(cmd_);
  }
private:
  const string &cmd_;
}; /* end of class RoutineComparer */

extern std::vector<ObAdminRoutine*> g_routines;

} /* end of namespace tools */
} /* end of namespace oceanbase */

#endif /* _OBADMIN_ROUTINE_H_ */
