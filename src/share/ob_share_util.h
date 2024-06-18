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

#ifndef OCEANBASE_SHARE_OB_SHARE_UTIL_H_
#define OCEANBASE_SHARE_OB_SHARE_UTIL_H_
#include "share/ob_define.h"
#include "share/scn.h"
namespace oceanbase
{
namespace common
{
class ObTimeoutCtx;
class ObISQLClient;
}
namespace share
{

// available range is [start_id, end_id]
class ObIDGenerator
{
public:
  ObIDGenerator()
    : inited_(false),
      step_(0),
      start_id_(common::OB_INVALID_ID),
      end_id_(common::OB_INVALID_ID),
      current_id_(common::OB_INVALID_ID)
  {}
  ObIDGenerator(const uint64_t step)
    : inited_(false),
      step_(step),
      start_id_(common::OB_INVALID_ID),
      end_id_(common::OB_INVALID_ID),
      current_id_(common::OB_INVALID_ID)
  {}

  virtual ~ObIDGenerator() {}
  void reset();

  int init(const uint64_t step,
           const uint64_t start_id,
           const uint64_t end_id);
  int next(uint64_t &current_id);

  int get_start_id(uint64_t &start_id) const;
  int get_current_id(uint64_t &current_id) const;
  int get_end_id(uint64_t &end_id) const;
  int get_id_cnt(uint64_t &cnt) const;
  TO_STRING_KV(K_(inited), K_(step), K_(start_id), K_(end_id), K_(current_id));
protected:
  bool inited_;
  uint64_t step_;
  uint64_t start_id_;
  uint64_t end_id_;
  uint64_t current_id_;
};

class ObShareUtil
{
public:
  // priority to set timeout_ctx: ctx > worker > default_timeout
  static int set_default_timeout_ctx(common::ObTimeoutCtx &ctx, const int64_t default_timeout);
  // priority to get timeout: ctx > worker > default_timeout
  static int get_abs_timeout(const int64_t default_timeout, int64_t &abs_timeout);
  static int get_ctx_timeout(const int64_t default_timeout, int64_t &timeout);
  // data version must up to 4.1 with arbitration service
  // params[in]  tenant_id, which tenant to check
  // params[out] is_compatible, whether it is up to 4.1
  static int check_compat_version_for_arbitration_service(
      const uint64_t tenant_id,
      bool &is_compatible);
  // check whether sys/meta/user tenant has been promoted to target data version
  // params[in]  tenant_id, which tenant to check
  // params[in]  target_data_version, data version to check
  // params[out] is_compatible, whether tenants are promoted to target data version
  static int check_compat_version_for_tenant(
      const uint64_t tenant_id,
      const uint64_t target_data_version,
      bool &is_compatible);
  // tenant data version should up to 430 when cloning primary tenant
  // tenant data version should up to 432 when cloning standby tenant
  // params[in]  tenant_id, which tenant to check
  // params[out] is_compatible, whether it is up to 4.3
  static int check_compat_version_for_clone_tenant_with_tenant_role(
      const uint64_t tenant_id,
      bool &is_compatible);

  // data version must up to 432 with clone standby tenant
  // params[in]  tenant_id, which tenant to check
  // params[out] is_compatible, whether it is up to 4.3.2
  static int check_compat_version_for_clone_standby_tenant(
      const uint64_t tenant_id,
      bool &is_compatible);
  // data version must up to 430 with clone primary tenant
  // params[in]  tenant_id, which tenant to check
  // params[out] is_compatible, whether it is up to 4.3.0
  static int check_compat_version_for_clone_tenant(
      const uint64_t tenant_id,
      bool &is_compatible);
  // generate the count of arb replica of a log stream
  // @params[in]  tenant_id, which tenant to check
  // @params[in]  ls_id, which log stream to check
  // @params[out] arb_replica_num, the result
  static int generate_arb_replica_num(
      const uint64_t tenant_id,
      const ObLSID &ls_id,
      int64_t &arb_replica_num);

  // data version must up to 4.2 with read only replica
  // @params[in]  tenant_id, which tenant to check
  // @params[out] is_compatible, whether it is up to 4.2
  static int check_compat_version_for_readonly_replica(
      const uint64_t tenant_id,
      bool &is_compatible);

  static int fetch_current_cluster_version(
             common::ObISQLClient &client,
             uint64_t &cluster_version);

  static int fetch_current_data_version(
             common::ObISQLClient &client,
             const uint64_t tenant_id,
             uint64_t &data_version);

  // parse GCONF.all_server_list
  // @params[in]  excluded_server_list, servers which will not be included in the output
  // @params[out] config_all_server_list, servers in (GCONF.all_server_list - excluded_server_list)
  static int parse_all_server_list(
    const ObArray<ObAddr> &excluded_server_list,
    ObArray<ObAddr> &config_all_server_list);
  // get ora_rowscn from one row
  // @params[in]: tenant_id, the table owner
  // @params[in]: sql, the sql should be "select ORA_ROWSCN from xxx", where count() is 1
  // @params[out]: the ORA_ROWSCN
  static int get_ora_rowscn(
    common::ObISQLClient &client,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    SCN &ora_rowscn);
  static bool is_tenant_enable_rebalance(const uint64_t tenant_id);
  static bool is_tenant_enable_transfer(const uint64_t tenant_id);
};
}//end namespace share
}//end namespace oceanbase
#endif //OCEANBASE_SHARE_OB_SHARE_UTIL_H_
