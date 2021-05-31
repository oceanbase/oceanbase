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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_random_addrs_provider.h"
#include <stdio.h>
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObRandomAddrsProvider::ObRandomAddrsProvider() : servers_()
{}

ObRandomAddrsProvider::~ObRandomAddrsProvider()
{}

int ObRandomAddrsProvider::select_servers(int64_t select_count, common::ObIArray<ObAddr>& servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(select_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid select count", K(ret), K(select_count));
  } else {
    servers.reset();
    int64_t mod = servers_.count();
    int64_t selected_server_count = 0;
    while (OB_SUCC(ret) && mod > 0 && selected_server_count < select_count) {
      int64_t select_idx = rand() % mod;
      ObAddr server = servers_.at(select_idx);
      if (OB_FAIL(servers.push_back(server))) {
        LOG_WARN("fail to push back server", K(ret), K(server));
      } else {
        // swap
        servers_.at(select_idx) = servers_.at(mod - 1);
        servers_.at(mod - 1) = server;
        mod--;
        selected_server_count++;
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
