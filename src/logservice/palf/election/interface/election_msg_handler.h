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