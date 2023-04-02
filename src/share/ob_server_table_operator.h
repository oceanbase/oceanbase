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

#ifndef OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_

#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "common/ob_zone.h"
#include "share/ob_lease_struct.h"
#include "share/ob_server_status.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObServerConfig;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share 
{
class ObServerTableOperator
{
public:
  ObServerTableOperator();
  virtual ~ObServerTableOperator();

  int init(common::ObISQLClient *proxy);
  common::ObISQLClient &get_proxy() const { return *proxy_; }
  virtual int get(common::ObIArray<share::ObServerStatus> &server_statuses);
  virtual int remove(const common::ObAddr &server, common::ObMySQLTransaction &trans);
  virtual int update(const share::ObServerStatus &server_status);
  virtual int reset_rootserver(const common::ObAddr &except);
  virtual int update_status(const common::ObAddr &server,
                            const share::ObServerStatus::DisplayStatus status,
                            const int64_t last_hb_time,
                            common::ObMySQLTransaction &trans);
  virtual int update_stop_time(const common::ObAddr &server,
      const int64_t stop_time);
  virtual int update_with_partition(const common::ObAddr &server, bool with_partition);
  int get_start_service_time(const common::ObAddr &server, int64_t &start_service_time) const;

private:
  int build_server_status(const common::sqlclient::ObMySQLResult &res,
                          share::ObServerStatus &server_status) const;
private:
  bool inited_;
  common::ObISQLClient *proxy_;
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SERVER_TABLE_OPERATOR_H_
