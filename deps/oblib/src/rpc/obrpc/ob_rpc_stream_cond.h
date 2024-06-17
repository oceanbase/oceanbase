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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_STREAM_COND_
#define OCEANBASE_RPC_OBRPC_OB_RPC_STREAM_COND_
#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace rpc
{
class ObRequest;
} // end of namespace rp
namespace obrpc
{

class ObRpcPacket;
class ObRpcSessionHandler;
class ObRpcStreamCond
{
public:
  explicit ObRpcStreamCond(ObRpcSessionHandler &handle);
  virtual ~ObRpcStreamCond();

  virtual int prepare(const ObAddr *src_addr, const ObRpcPacket *packet);
  virtual int wait(rpc::ObRequest *&req, int64_t timeout);
  virtual int wakeup(rpc::ObRequest &req);
  virtual int destroy();
  virtual void reuse();

  int64_t sessid() const { return sessid_; }

private:
  int64_t sessid_;
  ObRpcSessionHandler &handler_;
  int64_t first_pkt_id_;
  int64_t first_send_ts_;
  ObAddr src_addr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcStreamCond);
}; // end of class ObRpcStreamCond

} // end of namespace rpc
} // end of namespace oceanbase

#endif
