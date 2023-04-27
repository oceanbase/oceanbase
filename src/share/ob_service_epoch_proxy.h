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

#ifndef OCEANBASE_SHARE_OB_SERVICE_EPOCH_H_
#define OCEANBASE_SHARE_OB_SERVICE_EPOCH_H_

#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObMySQLTransaction;
class ObISQLClient;
namespace sqlclient
{
class ObMySQLResult;
}
}
namespace share
{
// NOTICE:
// When use ObServiceEpochProxy to operate __all_service_epoch, you should know
// that this table is 'is_cluster_private=true' & 'meta_record_in_sys=false'.
// There exists implicit conversion about tenant_id, and you should avoid to misuse,
// leading to cross-tenant transaction error.
class ObServiceEpochProxy
{
public:
  ObServiceEpochProxy() {}
  virtual ~ObServiceEpochProxy() {}

public:
  // This function will be invoked when init sys/meta tenant's schema.
  // For meta tenant, it needs to insert both user and its data.
  // For sys tenant, just insert itself data.
  static int init_service_epoch(common::ObISQLClient &sql_proxy,
                                const int64_t tenant_id,
                                const int64_t freeze_service_epoch,
                                const int64_t arbitration_service_epoch,
                                const int64_t server_zone_op_service_epoch,
                                const int64_t heartbeat_service_epoch);

  static int insert_service_epoch(common::ObISQLClient &sql_proxy,
                                  const int64_t tenant_id,
                                  const char *name,
                                  const int64_t epoch_value);
  static int update_service_epoch(common::ObISQLClient &sql_proxy,
                                  const int64_t tenant_id,
                                  const char* name,
                                  const int64_t new_epoch_value,
                                  int64_t &affected_rows);
  static int get_service_epoch(common::ObISQLClient &sql_proxy,
                               const int64_t tenant_id,
                               const char *name,
                               int64_t &epoch_value);
  static int select_service_epoch_for_update(common::ObISQLClient &sql_proxy,
                                             const int64_t tenant_id,
                                             const char *name,
                                             int64_t &epoch_value);
  static int check_service_epoch_with_trans(ObMySQLTransaction &trans,
                                            const int64_t tenant_id,
                                            const char *name,
                                            const int64_t expected_epoch,
                                            bool &is_match);
  static int check_service_epoch(common::ObISQLClient &sql_proxy,
                                 const int64_t tenant_id,
                                 const char *name,
                                 const int64_t expected_epoch,
                                 bool &is_match);
  // if service_epoch = persistent service epoch, do nothing
  // if service_epoch > persistent service epoch, update persistent service epoch
  // otherwise return error code OB_NOT_MASTER;
  static int check_and_update_service_epoch(
      ObMySQLTransaction &trans,
      const int64_t tenant_id,
      const char *name,
      const int64_t service_epoch);

public:
  constexpr static const char * const FREEZE_SERVICE_EPOCH = "freeze_service_epoch";
  constexpr static const char * const ARBITRATION_SERVICE_EPOCH = "arbitration_service_epoch";
  constexpr static const char * const SERVER_ZONE_OP_SERVICE_EPOCH = "server_zone_op_service_epoch";
  constexpr static const char * const HEARTBEAT_SERVICE_EPOCH = "heartbeat_service_epoch";

private:
  static int inner_get_service_epoch_(common::ObISQLClient &sql_proxy,
                                      const int64_t tenant_id,
                                      const bool is_for_update,
                                      const char *name,
                                      int64_t &epoch_value);
};

} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SERVICE_EPOCH_H_
