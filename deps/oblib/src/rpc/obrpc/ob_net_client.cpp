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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_net_client.h"

#include "lib/ob_define.h"
#include "lib/net/ob_addr.h"
#include "rpc/obrpc/ob_rpc_proxy.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace obrpc {
using rpc::frame::ObNetOptions;

ObNetClient::ObNetClient() : inited_(false), net_(), pkt_handler_(), transport_(NULL)
{
  // empty
}

ObNetClient::~ObNetClient()
{
  destroy();
}

int ObNetClient::init_(const ObNetOptions opts)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("Net client init twice", K(ret));
  } else if (OB_FAIL(net_.init(opts))) {
    LOG_ERROR("Init client network fail", K(ret));
  } else if (OB_FAIL(net_.add_rpc_handler(pkt_handler_, transport_))) {
    LOG_ERROR("add handler fail", K(ret));
  } else if (OB_FAIL(net_.start())) {
    LOG_ERROR("Start client network fail", K(ret));
  } else {
    inited_ = true;
  }

  LOG_INFO("net client init", K(ret), "rpc io", opts.rpc_io_cnt_, "mysql io", opts.mysql_io_cnt_);

  return ret;
}

int ObNetClient::init()
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("Net client init twice", K(ret));
  } else {
    ObNetOptions opts;
    opts.rpc_io_cnt_ = 1;
    opts.mysql_io_cnt_ = 1;

    if (OB_FAIL(init_(opts))) {
      LOG_ERROR("Init client network fail", K(ret));
    }
  }

  return ret;
}

int ObNetClient::init(const ObNetOptions opts)
{
  return init_(opts);
}

void ObNetClient::destroy()
{
  if (inited_) {
    net_.destroy();
    inited_ = false;

    LOG_INFO("net client destory successfully");
  }
}

int ObNetClient::get_proxy(ObRpcProxy& proxy)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Net client not inited", K(ret));
  } else if (OB_FAIL(proxy.init(transport_))) {
    LOG_ERROR("Init proxy error", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

}  // end of namespace obrpc
}  // end of namespace oceanbase
