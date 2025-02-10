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

#ifndef _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_
#define _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_ 1

#include "share/ob_rpc_struct.h"
#include "src/share/ls/ob_ls_table_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
namespace rootserver
{
class ObParallelCreateTenantExecutor
{
public:
  ObParallelCreateTenantExecutor() : create_tenant_arg_(), rpc_proxy_(NULL), common_rpc_(NULL),
    sql_proxy_(NULL), schema_service_(NULL), lst_operator_(NULL), location_service_(NULL),
    ctx_(), create_tenant_schema_result_(), user_tenant_schema_(), meta_tenant_schema_() {}
  int execute(obrpc::UInt64 &tenant_id);
  int init(
      obrpc::ObCreateTenantArg create_tenant_arg,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      obrpc::ObCommonRpcProxy *common_rpc,
      common::ObMySQLProxy *sql_proxy,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLSTableOperator *lst_operator,
      share::ObLocationService *location_service);
  TO_STRING_KV(K_(create_tenant_arg), KP_(rpc_proxy), KP_(common_rpc), KP_(sql_proxy),
      KP_(schema_service), KP_(lst_operator), KP_(location_service), K_(ctx),
      K_(create_tenant_schema_result), K(user_tenant_schema_), K(meta_tenant_schema_));
private:
  int create_user_ls_(ObParallelCreateNormalTenantProxy &proxy);

  int wait_all_(ObParallelCreateNormalTenantProxy &proxy, const int ret_code);

  int init_after_create_tenant_schema_();

  int async_call_create_normal_tenant_(
      const ObTenantSchema &tenant_schema,
      ObParallelCreateNormalTenantProxy &proxy);

  int call_create_normal_tenant_(ObParallelCreateNormalTenantProxy &proxy);

  int wait_ls_leader_(const uint64_t tenant_id, const bool force_renew = false);

  int check_can_create_user_ls_(ObParallelCreateNormalTenantProxy &proxy);

  int finish_create_tenant_(const int ret_code);
  int check_inner_stat_();

  int get_tenant_schema_from_inner_table_();

  int create_tenant_sys_ls_();

  int create_tenant_sys_ls_(const ObTenantSchema &tenant_schema,
      const ObIArray<share::ObResourcePoolName> &pool_list,
      const bool create_ls_with_palf,
      const palf::PalfBaseInfo &palf_base_info,
      const uint64_t source_tenant_id,
      const share::ObAllTenantInfo &tenant_info);

  int create_tenant_user_ls_(ObParallelCreateNormalTenantProxy &proxy);
  int construct_tenant_info_(const uint64_t tenant_id, share::ObAllTenantInfo &tenant_info);
  share::SCN get_recovery_until_scn_();
  palf::PalfBaseInfo get_palf_base_info_();
  bool get_create_ls_with_palf_();

  bool async_rpc_has_error(ObParallelCreateNormalTenantProxy &proxy);
private:
  // set by outside
  obrpc::ObCreateTenantArg create_tenant_arg_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLSTableOperator *lst_operator_;
  share::ObLocationService *location_service_;
  share::ObRsMgr *rs_mgr_;
private:
  // inited in init function
  ObTimeoutCtx ctx_;
  // inited after first rpc
  obrpc::ObCreateTenantSchemaResult create_tenant_schema_result_;
  ObTenantSchema user_tenant_schema_;
  ObTenantSchema meta_tenant_schema_;
};
}
}

#endif // _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_
