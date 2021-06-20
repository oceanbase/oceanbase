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

#ifndef _OB_SERVER_CHECKER_H
#define _OB_SERVER_CHECKER_H 1
#include "lib/container/ob_array.h"
#include "lib/net/ob_addr.h"
#include "ob_balance_info.h"
namespace oceanbase {
namespace rootserver {
class ObUnitManager;
class ObServerManager;
class ObEmptyServerChecker;

class ObServerChecker {
public:
  ObServerChecker();
  virtual ~ObServerChecker()
  {}
  int init(ObUnitManager& unit_mgr, ObServerManager& server_mgr, ObEmptyServerChecker& empty_server_checker,
      TenantBalanceStat& tenant_stat);

  int try_delete_server();
  int try_notify_empty_server_checker();

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObServerChecker);
  // function members
  int reuse_replica_count_mgr();

private:
  // data members
  bool inited_;
  ObServerManager* server_mgr_;
  ObUnitManager* unit_mgr_;
  ObEmptyServerChecker* empty_server_checker_;
  TenantBalanceStat* tenant_stat_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_SERVER_CHECKER_H */
