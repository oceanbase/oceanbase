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

#define USING_LOG_PREFIX SERVER
#include "ob_rpc_intrusion_detect.h"
#include "util/easy_mod_stat.h"
#include "share/ob_i_server_auth.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
using namespace common;
namespace observer
{
share::ObIServerAuth* g_server_auth = NULL;

static int on_connect(easy_connection_t *c)
{
  int ret = OB_SUCCESS;
  ObAddr local_addr;
  bool is_valid = true;
  if (OB_ISNULL(c) || OB_ISNULL(g_server_auth)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(c), KP(g_server_auth));
  } else if (!ez2ob_addr(local_addr, c->addr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ez2ob_addr fail", K(ret));
  } else if (OB_FAIL(g_server_auth->check_ssl_invited_nodes(*c))) {
    LOG_WARN("check_ssl_invited_nodes fail", K(ret), K(easy_connection_str(c)));
  } else if (OB_FAIL(g_server_auth->is_server_legitimate(local_addr, is_valid))) {
    LOG_WARN("check server legitimate fail", K(ret), K(local_addr));
  } else if (!is_valid) {
    LOG_WARN("RPC INTRUSION DETECT: receive TCP connection out of this cluster,"
             "may be a new server just add to this cluster or an 'ob_admin' operation happening",
             K(local_addr));
  }
  if (OB_SUCC(ret) && NULL == c->client) {
    easy_addr_t addr = c->addr;
    addr.port = 0;
    c->pool->mod_stat = easy_fetch_mod_stat(*(uint64_t*)&addr);
  }
  return OB_SUCC(ret) ? EASY_OK : EASY_ERROR;
}

int ob_rpc_intrusion_detect_patch(easy_io_handler_pt* ez_handler, share::ObIServerAuth* auth)
{
  int ret = OB_SUCCESS;
  g_server_auth = auth;
  if (NULL != ez_handler) {
    ez_handler->on_connect = on_connect;
  }
  return ret;
}

}; // end namespace observer
}; // end namespace oceanbase
