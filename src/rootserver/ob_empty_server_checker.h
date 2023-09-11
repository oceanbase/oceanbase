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

#ifndef OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_

#include "lib/lock/ob_thread_cond.h"
#include "lib/net/ob_addr.h"
#include "lib/thread/ob_reentrant_thread.h"//block_run

#include "rootserver/ob_rs_reentrant_thread.h"

namespace oceanbase
{
namespace share
{
class ObLSTableOperator;
class ObLSInfo;
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace rootserver
{
class ObServerManager;
class ObUnitManager;
class ObServerZoneOpService;
/// Empty server checker thread.
class ObEmptyServerChecker : public ObRsReentrantThread
{
public:
  ObEmptyServerChecker(): inited_(false),
                          cond_(),
                          need_check_(true),
                          empty_servers_(),
                          server_mgr_(NULL),
                          unit_mgr_(NULL),
                          lst_operator_(NULL),
                          schema_service_(NULL),
                          server_zone_op_service_(NULL) {};
  virtual ~ObEmptyServerChecker() {};

  virtual void run3() override;
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }

  int init(ObServerManager &server_mgr,
      ObUnitManager &unit_mgr,
      share::ObLSTableOperator &lst_operator,
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObServerZoneOpService &server_zone_op_service);

  virtual void wakeup();
  virtual void stop();
  /**
   * @description:
   *    check if the given tenant has ls replicas on servers
   * @param[in] tenant_id the tenant which need to be checked
   * @param[in] servers on which the tenant might have ls replicas
   * @param[out] exists true if at least one of the servers has tenant ls replicas
   * @return return code
   */
  static int check_if_tenant_ls_replicas_exist_in_servers(
    const uint64_t tenant_id,
    const common::ObArray<common::ObAddr> &servers,
    bool &exist);
private:
   int try_delete_server_();
   int check_server_empty_();
   static int check_server_emtpy_by_ls_(
      const share::ObLSInfo &ls_info,
      common::ObArray<common::ObAddr> &empty_servers);
   //TODO no need check, check in unit_mgr now
   int check_server_empty_in_unit(const common::ObAddr &addr, bool &is_empty);

private:
  bool inited_;
  common::ObThreadCond cond_;
  bool need_check_;
  common::ObArray<common::ObAddr> empty_servers_;
  ObServerManager *server_mgr_;
  ObUnitManager *unit_mgr_;
  share::ObLSTableOperator *lst_operator_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObServerZoneOpService *server_zone_op_service_;

  DISALLOW_COPY_AND_ASSIGN(ObEmptyServerChecker);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_EMPTY_SERVER_CHECKER_H_
