/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef LOGSERVICE_PALF_ELECTION_INTERFACE_OB_I_ELECTION_MSG_HANDLER_H
#define LOGSERVICE_PALF_ELECTION_INTERFACE_OB_I_ELECTION_MSG_HANDLER_H

#include "lib/net/ob_addr.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class ElectionPrepareRequestMsg;
class ElectionAcceptRequestMsg;
class ElectionPrepareResponseMsg;
class ElectionAcceptResponseMsg;
class ElectionChangeLeaderMsg;

class ElectionMsgSender
{
// 需要由外部实现的消息发送接口
public:
  virtual int broadcast(const ElectionPrepareRequestMsg &msg,
                        const common::ObIArray<common::ObAddr> &list) const = 0;
  virtual int broadcast(const ElectionAcceptRequestMsg &msg,
                        const common::ObIArray<common::ObAddr> &list) const = 0;
  virtual int send(const ElectionPrepareResponseMsg &msg) const = 0;
  virtual int send(const ElectionAcceptResponseMsg &msg) const = 0;
  virtual int send(const ElectionChangeLeaderMsg &msg) const = 0;
};

}
}
}

#endif