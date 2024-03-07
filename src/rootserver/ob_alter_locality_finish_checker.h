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

#ifndef OCEANBASE_ROOTSERVER_OB_ALTER_LOCALITY_FINISH_CHECKER_
#define OCEANBASE_ROOTSERVER_OB_ALTER_LOCALITY_FINISH_CHECKER_

#include "share/ob_define.h"
#include "ob_root_utils.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
class ObLSTableOperator;
}
namespace rootserver
{
class ObUnitManager;
class ObZoneManager;
class DRLSInfo;
class LocalityMap;

struct ObCommitAlterTenantLocalityArg : public obrpc::ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCommitAlterTenantLocalityArg() : tenant_id_(common::OB_INVALID_ID) {}
  bool is_valid() const { return common::OB_INVALID_ID != tenant_id_;}
  TO_STRING_KV(K_(tenant_id));

  uint64_t tenant_id_;
};

class ObAlterLocalityFinishChecker : public share::ObCheckStopProvider
{
public:
  ObAlterLocalityFinishChecker(volatile bool &stop);
  virtual ~ObAlterLocalityFinishChecker();
public:
  int init(
      share::schema::ObMultiVersionSchemaService &schema_service,
      obrpc::ObCommonRpcProxy &common_rpc_proxy,
      common::ObAddr &self,
      ObZoneManager &zone_mgr,
      common::ObMySQLProxy &sql_proxy,
      share::ObLSTableOperator &lst_operator);
  int check();
  static int find_rs_job(const uint64_t tenant_id, int64_t &job_id, ObISQLClient &sql_proxy);

private:
  //check whether this checker is stopped
  virtual int check_stop() const override;
  virtual int check_tenant_previous_locality_(const uint64_t tenant_id, bool &is_previous_locality_empty);
private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  obrpc::ObCommonRpcProxy *common_rpc_proxy_;   //use GCTX.rs_rpc_proxy_
  common::ObAddr self_;
  ObZoneManager *zone_mgr_;
  common::ObMySQLProxy *sql_proxy_;
  share::ObLSTableOperator *lst_operator_;
  volatile bool &stop_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterLocalityFinishChecker);
};
} // end namespace rootserver
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_ALTER_LOCALITY_FINISH_CHECKER_
