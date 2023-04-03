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

#ifndef OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_
#define OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_

#include "io/easy_io_struct.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/frame/ob_req_transport.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
} // end of namespace common

namespace obrpc
{
class ObRpcProxy;

class ObNetClient
{
public:
  ObNetClient();
  virtual ~ObNetClient();

  int init();
  int init(const rpc::frame::ObNetOptions opts);
  void destroy();
  int get_proxy(ObRpcProxy &proxy);

  /*
   * Load ssl config
   * Support local file mode and BKMI mode
   * 1. For local file mode, ca_cert/public_cert/private_key is local file path
   * 2. For BKMI mode, ca_cert/public_cert/private_key is real content
   *
   * @param [in] use_bkmi    whether（or not）to use BKMI
   * @param [in] use_sm      whether（or not）to use China cryptographic algorithm,eg SM2/SM3/SM4
   * @param [in] ca_cert     CA cert
   * @param [in] public_cert public cert
   * @param [in] private_key private key
   *
   * @param OB_SUCCESS  success
   * @param other code  fail
   *
   */
  int load_ssl_config(const bool use_bkmi,
      const bool use_sm,
      const char *ca_cert,
      const char *public_cert, const char *private_key,
      const char *enc_cert, const char *enc_private_key);

private:
  int init_(const rpc::frame::ObNetOptions opts);
private:
  bool inited_;
  rpc::frame::ObNetEasy net_;
  ObRpcNetHandler pkt_handler_;
  rpc::frame::ObReqTransport *transport_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObNetClient);
}; // end of class ObNetClient

} // end of namespace rpc
} // end of namespace oceanbase

#endif //OCEANBASE_RPC_OBRPC_OB_NET_CLIENT_
