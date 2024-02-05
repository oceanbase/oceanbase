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

#ifndef __OB_RS_RESTORE_COMMON_UTIL_H__
#define __OB_RS_RESTORE_COMMON_UTIL_H__

#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
class ObRootKey;
struct ObLSAttr;
}
namespace rootserver
{

class TenantRestoreStatus
{
public:
  enum Status : int8_t
  {
    INVALID = -1,
    IN_PROGRESS = 0,
    SUCCESS = 1,
    FAILED = 2,
  };

public:
  TenantRestoreStatus() : status_(INVALID) {}
  TenantRestoreStatus(int8_t status) : status_(status) {}

  bool is_finish() { return SUCCESS == status_ || FAILED == status_; }
  bool is_success() { return SUCCESS == status_; }
  bool is_failed() { return FAILED == status_; }

  TenantRestoreStatus operator=(const TenantRestoreStatus &other) { status_ = other.status_; return *this; }
  bool operator == (const TenantRestoreStatus &other) const { return status_ == other.status_; }
  bool operator != (const TenantRestoreStatus &other) const { return status_ != other.status_; }

  TO_STRING_KV(K_(status));
private:
  int8_t status_;
};

class ObRestoreCommonUtil
{
public:
  static int notify_root_key(obrpc::ObSrvRpcProxy *srv_rpc_proxy_,
                             common::ObMySQLProxy *sql_proxy_,
                             const uint64_t tenant_id,
                             const share::ObRootKey &root_key);
  static int create_all_ls(common::ObMySQLProxy *sql_proxy,
             const uint64_t tenant_id,
             const share::schema::ObTenantSchema &tenant_schema,
             const common::ObIArray<share::ObLSAttr> &ls_attr_array,
             const uint64_t source_tenant_id = OB_INVALID_TENANT_ID);
  static int finish_create_ls(common::ObMySQLProxy *sql_proxy,
             const share::schema::ObTenantSchema &tenant_schema,
             const common::ObIArray<share::ObLSAttr> &ls_attr_array);
  static int try_update_tenant_role(common::ObMySQLProxy *sql_proxy,
                                    const uint64_t tenant_id,
                                    const share::SCN &restore_scn,
                                    const bool is_clone,
                                    bool &sync_satisfied);
  static int process_schema(common::ObMySQLProxy *sql_proxy,
                            const uint64_t tenant_id);
  static int check_tenant_is_existed(ObMultiVersionSchemaService *schema_service,
                                     const uint64_t tenant_id,
                                     bool &is_existed);
  static int set_tde_parameters(common::ObMySQLProxy *sql_proxy,
                                obrpc::ObCommonRpcProxy *rpc_proxy,
                                const uint64_t tenant_id,
                                const ObString &tde_method,
                                const ObString &kms_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRestoreCommonUtil);
};
}
}

#endif /* __OB_RS_RESTORE_COMMON_UTIL_H__ */
