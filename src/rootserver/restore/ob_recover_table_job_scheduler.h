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

#ifndef OCEANBASE_ROOTSERVER_RECOVER_TABLE_JOB_SCHEDULER_H
#define OCEANBASE_ROOTSERVER_RECOVER_TABLE_JOB_SCHEDULER_H

#include "share/restore/ob_recover_table_persist_helper.h"
namespace oceanbase
{

namespace obrpc
{
class ObCommonRpcProxy;
class ObSrvRpcProxy;
}

namespace share
{
class ObLocationService;
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace common
{
class ObMySQLProxy;
class ObString;
class ObMySQLTransaction;
class ObISQLClient;
}

namespace rootserver
{
class ObRestoreService;
class ObRecoverTableJobScheduler final
{
public:
  ObRecoverTableJobScheduler()
    : is_inited_(false), schema_service_(nullptr), sql_proxy_(nullptr), rs_rpc_proxy_(nullptr),
      srv_rpc_proxy_(nullptr), helper_() {}
  virtual ~ObRecoverTableJobScheduler() {}
  void reset();
  int init(share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy,
      obrpc::ObCommonRpcProxy &rs_rpc_proxy,
      obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  void do_work();

private:
  int try_advance_status_(share::ObRecoverTableJob &job, const int err_code);

  void sys_process_(share::ObRecoverTableJob &job);
  int check_target_tenant_version_(share::ObRecoverTableJob &job);
  int sys_prepare_(share::ObRecoverTableJob &job);
  int insert_user_job_(const share::ObRecoverTableJob &job, share::ObRecoverTablePersistHelper &helper);
  int recovering_(share::ObRecoverTableJob &job);
  int sys_finish_(const share::ObRecoverTableJob &job);
  int drop_aux_tenant_(const share::ObRecoverTableJob &job);

  void user_process_(share::ObRecoverTableJob &job);
  int user_prepare_(share::ObRecoverTableJob &job);
  int restore_aux_tenant_(share::ObRecoverTableJob &job);
  int check_aux_tenant_(share::ObRecoverTableJob &job, const uint64_t aux_tenant_id);
  int active_aux_tenant_(share::ObRecoverTableJob &job);
  int failover_to_primary_(share::ObRecoverTableJob &job, const uint64_t aux_tenant_id);
  int check_tenant_compatibility(
      share::schema::ObSchemaGetterGuard &aux_tenant_guard,
      share::schema::ObSchemaGetterGuard &recover_tenant_guard,
      bool &is_compatible);
  int check_case_sensitive_compatibility(
      share::schema::ObSchemaGetterGuard &aux_tenant_guard,
      share::schema::ObSchemaGetterGuard &recover_tenant_guard,
      bool &is_compatible);

  int gen_import_job_(share::ObRecoverTableJob &job);
  int importing_(share::ObRecoverTableJob &job);
  int canceling_(share::ObRecoverTableJob &job);
  int user_finish_(const share::ObRecoverTableJob &job);
  int check_compatible_() const;
  void wakeup_();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy                       *sql_proxy_;
  obrpc::ObCommonRpcProxy                    *rs_rpc_proxy_;
  obrpc::ObSrvRpcProxy                       *srv_rpc_proxy_;
  share::ObRecoverTablePersistHelper helper_;
  DISALLOW_COPY_AND_ASSIGN(ObRecoverTableJobScheduler);
};

}
}

#endif