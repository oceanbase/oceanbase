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
 *
 * definition of PartTransID(identify a PartTrans) and TenantTransID
 */

#ifndef  OCEANBASE_LIBOBCDC_TENANT_CHECKPOINT_H_
#define  OCEANBASE_LIBOBCDC_TENANT_CHECKPOINT_H_

#include "share/ob_tenant_role.h"             // ObTenantRole
#include "share/scn.h"                        // SCN

#include "ob_cdc_tenant_query.h"              // ObCDCTenantQuery

namespace oceanbase
{
namespace libobcdc
{

#define SCN_PROPERTY_DEFINE(FIELD_NAME) \
  private: \
    share::SCN FIELD_NAME##_; \
  public: \
    const share::SCN &get_##FIELD_NAME() const { return FIELD_NAME##_; } \
    void set_##FIELD_NAME(const int64_t FIELD_SCN_INT_VAL) { FIELD_NAME##_.convert_for_inner_table_field(FIELD_SCN_INT_VAL); }

// record tenant checkpoint info such as restore_scn, readable_scn, replayable_scn, sync_scn, recovery_until_scn
// and so on.
// TODO: support record lsn and scn of the first log in palf and archivelog
class TenantCheckpoint
{
public:
  TenantCheckpoint(const uint64_t tenant_id) : tenant_id_(tenant_id) { reset_data(); }
  ~TenantCheckpoint() { destroy(); }
  void destroy()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    reset_data();
  }
  void reset_data(); // won't reset tenant_id
  int set_tenant_id(const uint64_t tenant_id);
  int assign(const TenantCheckpoint &other);
  // TenantCheckpoint &operator=(const TenantCheckpoint &other);
  TO_STRING_KV(K_(tenant_id), K_(tenant_role), K_(restore_scn), K_(readable_scn), K_(replayable_scn), K_(sync_scn), K_(recovery_until_scn));
  DISALLOW_COPY_AND_ASSIGN(TenantCheckpoint);
public:
  int refresh(const int64_t timeout);
  // check clog_scn is served or not
  // for primary_tenant: always served;
  // for standby/restore tenant: clog_scn should large than restore_scn and little than sync_scn
  // need retry if snapshot_scn_val between replayable_scn and recovery_until_scn
  int check_clog_served(const int64_t clog_scn_val, bool &is_served, bool &need_retry) const;
  // SQL should retry until readable_scn >= snapshot_scn_val
  int wait_until_readable(const int64_t snapshot_scn_val, const int64_t timeout_us);
  // should wait replayable_scn >= snapshot_scn_val
  int wait_until_clog_served(const int64_t snapshot_scn_val, bool &is_clog_served, const int64_t timeout_us);

public:
  // gettter and setter
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_role(const share::ObTenantRole &tenant_role) { tenant_role_ = tenant_role; }
  const share::ObTenantRole& get_tenant_role() const { return tenant_role_; }

private:
  // check tenant is primary or not
  //
  // @return OB_SUCCESS:
  //    if cdc can't access observer(offline mode): always set is_primary_tenant = true;
  //    for primary_tenant: is_primary_tenant = true;
  //    for standby/restore tenant: is_primary_tenant = false;
  // @return OB_NEED_RETRY: not offline mode but tenant_role not valid, may try to refresh tenant checkpoint
  // @return OB_ERR_UNEXPECTED: tenant_id not valid
  int check_primary_tenant_(bool &is_primary_tenant) const;

  // check snpatshot is readable
  // for primary_tenant: always readbale;
  // for standby/restore tenant: snapshot_scn_val should large than readable_scn
  int check_snapshot_readable_(const int64_t snapshot_scn_val, bool &is_readable);

private:
  uint64_t tenant_id_;
  share::ObTenantRole tenant_role_;
  SCN_PROPERTY_DEFINE(palf_available_scn); // BEGIN_SCN in PALF
  SCN_PROPERTY_DEFINE(archive_available_scn); // BEGIN_SCN in ARCHIVELOG
  SCN_PROPERTY_DEFINE(restore_scn);
  SCN_PROPERTY_DEFINE(readable_scn);
  SCN_PROPERTY_DEFINE(replayable_scn);
  SCN_PROPERTY_DEFINE(sync_scn);
  SCN_PROPERTY_DEFINE(recovery_until_scn);
}; // TenantCheckpoint

class TenantCheckpointQueryer : public ObCDCTenantQuery<TenantCheckpoint>
{
public:
  TenantCheckpointQueryer(const uint64_t tenant_id, const bool is_cluster_sql_proxy, common::ObMySQLProxy &sql_proxy)
    : ObCDCTenantQuery(is_cluster_sql_proxy, sql_proxy), query_tenant_id_(tenant_id) {}
  ~TenantCheckpointQueryer() {}
public:
  int get_tenant_checkpoint(TenantCheckpoint &tenant_checkpoint, const int64_t timeout);
private:
  int build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql) override;
  int parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<TenantCheckpoint> &result) override;
private:
  uint64_t query_tenant_id_; // tenant_info to query
  static const char* QUERY_SQL_FORMAT;
private:
  DISALLOW_COPY_AND_ASSIGN(TenantCheckpointQueryer);
}; // TenantCheckpointQueryer

} // end namespace libobcdc
} // end namespace oceanbase

#endif
