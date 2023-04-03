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

#ifndef OCEANBASE_RPC_OB_BLACKLIST_REQ_PROCESSOR_H_
#define OCEANBASE_RPC_OB_BLACKLIST_REQ_PROCESSOR_H_

#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/rpc/ob_blacklist_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObBlacklistReqP : public ObRpcProcessor< obrpc::ObBlacklistRpcProxy::ObRpc<OB_SERVER_BLACKLIST_REQ> >
{
public:
  ObBlacklistReqP() {}
  ~ObBlacklistReqP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObBlacklistReqP);
};
}; // end namespace rpc
}; // end namespace oceanbase

#endif /* OCEANBASE_RPC_OB_BLACKLIST_REQ_PROCESSOR_H_ */
