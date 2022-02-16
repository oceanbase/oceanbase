/**
 * (C) 2010-2014 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <>
 * Version: $Id$
 * Filename: ob_admin_routine.h
 *
 * Authors:
 *   Shi Yudi <fufeng.syd@alipay.com>
 *
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
