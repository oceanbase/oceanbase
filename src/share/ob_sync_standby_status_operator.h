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

#ifndef OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_OPERATOR_H_
#define OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_OPERATOR_H_

#include "deps/oblib/src/lib/utility/ob_unify_serialize.h"
#include "share/ob_tenant_role.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObMySQLTransaction;
}
namespace share
{
class ObSyncStandbyStatusAttr
{
  OB_UNIS_VERSION(1);
public:
  ObSyncStandbyStatusAttr();
  ~ObSyncStandbyStatusAttr();
  int init(const uint64_t cluster_id, const uint64_t tenant_id,
           const ObProtectionStat &protection_stat);
  void reset();
  bool is_valid() const;
  uint64_t get_cluster_id() const { return cluster_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  ObProtectionStat get_protection_stat() const { return protection_stat_; }
  TO_STRING_KV(K_(cluster_id), K_(tenant_id), K_(protection_stat));
private:
  uint64_t cluster_id_;
  uint64_t tenant_id_;
  ObProtectionStat protection_stat_;
};

class ObSyncStandbyStatusOperator
{
public:
  ObSyncStandbyStatusOperator(const uint64_t tenant_id, common::ObISQLClient *proxy)
    : tenant_id_(tenant_id), proxy_(proxy) {}
  ~ObSyncStandbyStatusOperator() {}
  int read_sync_standby_status(ObISQLClient &sql_client, const bool for_update, ObProtectionStat &protection_stat);
  int update_sync_standby_status(const ObProtectionStat &previous_protection_stat, const ObSyncStandbyStatusAttr &attr);
  int update_sync_standby_status_in_trans(common::ObMySQLTransaction &trans,
    const ObProtectionStat &previous_protection_stat,
    const ObSyncStandbyStatusAttr &attr);
  // TODO(shouju.zyp for MPT): used for upgrade from low version
  int init_sync_standby_status(common::ObMySQLTransaction &trans);
private:
  int check_inner_stat_();
private:
  int write_sync_standby_status_log_(common::ObMySQLTransaction &trans, const ObSyncStandbyStatusAttr &attr);

private:
  uint64_t tenant_id_;
  common::ObISQLClient *proxy_;
};
}
}
#endif // OCEANBASE_SHARE_OB_SYNC_STANDBY_STATUS_OPERATOR_H_
