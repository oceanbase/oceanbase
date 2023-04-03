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

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace observer
{
class ObSrvNetworkFrame;
} // end of namespace observer
} // end of namespace oceanbase

namespace oceanbase
{
namespace obrpc
{
RPC_S_S(@PR5 test, OB_TEST_PCODE);
class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_S_M(@PR5 test, OB_TEST_PCODE);
};

} // end of namespace obrpc
} // end of namespace oceanbase

namespace oceanbase { namespace unittest {

class Client
    : public share::ObThreadPool
{
public:
  Client(const obrpc::TestProxy &proxy)
      : proxy_(proxy)
  {}

  virtual void run1();

private:
  const obrpc::TestProxy &proxy_;
}; // end of class Client

int start_frame(observer::ObSrvNetworkFrame &frame);
bool parse_arg(int argc, char *argv[]);

}}
