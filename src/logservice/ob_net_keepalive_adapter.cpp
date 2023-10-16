// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#include "ob_net_keepalive_adapter.h"
#include "rpc/obrpc/ob_net_keepalive.h"       // ObNetKeepAlive
#include "share/ob_lease_struct.h"            // RSS_IS_STOPPED

namespace oceanbase
{
namespace logservice
{
ObNetKeepAliveAdapter::ObNetKeepAliveAdapter(obrpc::ObNetKeepAlive *net_keepalive)
  : net_keepalive_(net_keepalive)
{
}

ObNetKeepAliveAdapter::~ObNetKeepAliveAdapter()
{
  net_keepalive_ = NULL;
}

int ObNetKeepAliveAdapter::in_black_or_stopped_(const common::ObAddr &server,
                                              bool &in_blacklist,
                                              bool &is_server_stopped)
{
  int ret = OB_SUCCESS;
  obrpc::ObNetKeepAliveData ka_data;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(net_keepalive_->in_black(server, in_blacklist, &ka_data))) {
      CLOG_LOG(WARN, "in_black failed", K(ret), K(server));
  } else {
    is_server_stopped = (share::RSS_IS_WORKING != ka_data.rs_server_status_)
        || (ka_data.start_service_time_ <= 0 || OB_INVALID_TIMESTAMP == ka_data.start_service_time_);
    if (is_server_stopped
        && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(INFO, "this server is stopped", K(server), "server_status", ka_data.rs_server_status_,
          "start_service_time", ka_data.start_service_time_);
    }
    CLOG_LOG(TRACE, "in_black_or_stopped_ failed", K(in_blacklist), K(is_server_stopped), K(server));
  }
  return ret;
}

bool ObNetKeepAliveAdapter::in_black_or_stopped(const common::ObAddr &server)
{
  bool bool_ret = false;
  bool in_blacklist = false;
  bool is_server_stopped = false;
  if (OB_SUCCESS != in_black_or_stopped_(server, in_blacklist, is_server_stopped)) {
  } else {
    bool_ret = in_blacklist || is_server_stopped;
  }
  return bool_ret;
}

bool ObNetKeepAliveAdapter::is_server_stopped(const common::ObAddr &server)
{
  bool bool_ret = false;
  bool unused_in_blacklist = false;
  bool is_server_stopped = false;
  if (OB_SUCCESS != in_black_or_stopped_(server, unused_in_blacklist, is_server_stopped)) {
  } else {
    bool_ret = is_server_stopped;
  }
  return bool_ret;
}

bool ObNetKeepAliveAdapter::in_black(const common::ObAddr &server)
{
  bool bool_ret = false;
  bool in_blacklist = false;
  bool unused_is_server_stopped = false;
  if (OB_SUCCESS != in_black_or_stopped_(server, in_blacklist, unused_is_server_stopped)) {
  } else {
    bool_ret = in_blacklist;
  }
  return bool_ret;
}

int ObNetKeepAliveAdapter::get_last_resp_ts(const common::ObAddr &server,
                                            int64_t &last_resp_ts)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(server));
  } else if (OB_FAIL(net_keepalive_->get_last_resp_ts(server, last_resp_ts))) {
    CLOG_LOG(WARN, "get_last_resp_ts failed", K(ret), K(server));
  } else {
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      CLOG_LOG(TRACE, "get_last_resp_ts", K(server), K(last_resp_ts));
    }
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
