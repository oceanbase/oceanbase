// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_LOGSERVICE_OB_NET_KEEPALIVE_ADPATER_H_
#define OCEANBASE_LOGSERVICE_OB_NET_KEEPALIVE_ADPATER_H_
#include <stdint.h>  // for int64_t etc.

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace obrpc
{
class ObNetKeepAlive;
}
namespace logservice
{
class IObNetKeepAliveAdapter {
public:
  IObNetKeepAliveAdapter() {}
  virtual ~IObNetKeepAliveAdapter() {}
  virtual bool in_black_or_stopped(const common::ObAddr &server) = 0;
  virtual bool is_server_stopped(const common::ObAddr &server) = 0;
  virtual bool in_black(const common::ObAddr &server) = 0;
  virtual int get_last_resp_ts(const common::ObAddr &server, int64_t &last_resp_ts) = 0;
};

class ObNetKeepAliveAdapter : public IObNetKeepAliveAdapter {
public:
  ObNetKeepAliveAdapter(obrpc::ObNetKeepAlive *net_keepalive);
  ~ObNetKeepAliveAdapter() override;
  bool in_black_or_stopped(const common::ObAddr &server) override final;
  bool is_server_stopped(const common::ObAddr &server) override final;
  bool in_black(const common::ObAddr &server) override final;
  int get_last_resp_ts(const common::ObAddr &server, int64_t &last_resp_ts) override final;
private:
  int in_black_or_stopped_(const common::ObAddr &server,
                           bool &in_black,
                           bool &server_is_stopped);
  obrpc::ObNetKeepAlive *net_keepalive_;
};

} // end namespace logservice
} // end namespace oceanbase
#endif
