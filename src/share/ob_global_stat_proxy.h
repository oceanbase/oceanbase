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

#ifndef OCEANBASE_SHARE_OB_GLOBAL_STAT_PROXY_H_
#define OCEANBASE_SHARE_OB_GLOBAL_STAT_PROXY_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_core_table_proxy.h"
#include "share/ob_freeze_info_proxy.h"

namespace oceanbase {
namespace share {
struct ObGlobalStatItem : public common::ObDLinkBase<ObGlobalStatItem> {
  typedef common::ObDList<ObGlobalStatItem> ItemList;
  ObGlobalStatItem(ItemList& list, const char* name, int64_t value) : name_(name), value_(value)
  {
    list.add_last(this);
  }

  TO_STRING_KV(K_(name), K_(value));

  const char* name_;
  int64_t value_;
};

static const char* OB_ALL_GLOBAL_STAT_TNAME = "__all_global_stat";
class ObGlobalStatProxy {
public:
  ObGlobalStatProxy(common::ObISQLClient& client) : core_table_(OB_ALL_GLOBAL_STAT_TNAME, client)
  {}
  virtual ~ObGlobalStatProxy()
  {}
  void set_sql_client(common::ObISQLClient& client)
  {
    core_table_.set_sql_client(client);
  }
  bool is_valid() const
  {
    return core_table_.is_valid();
  }

  virtual int set_init_value(const int64_t core_schema_version, const int64_t baseline_schema_version,
      const int64_t frozen_version, const int64_t rootservice_epoch, const int64_t split_schema_version,
      const int64_t split_schema_version_v2, const int64_t snapshot_gc_ts, const int64_t gc_schema_version,
      const int64_t next_schema_version);

  virtual int set_core_schema_version(const int64_t core_schema_version);
  virtual int set_baseline_schema_version(const int64_t baseline_schema_version);
  virtual int set_frozen_version(const int64_t frozen_version, const bool is_incremental = true);
  virtual int set_split_schema_version(const int64_t split_schema_version);
  virtual int set_split_schema_version_v2(const int64_t split_schema_version_v2);
  virtual int set_schema_snapshot_version(const int64_t snapshot_schema_version, const int64_t snapshot_frozen_version);
  virtual int inc_rootservice_epoch();

  virtual int get_core_schema_version(const int64_t frozen_version, int64_t& core_schema_version);
  virtual int get_baseline_schema_version(const int64_t frozen_version, int64_t& baseline_schema_version);
  virtual int get_split_schema_version(int64_t& split_schema_version);
  virtual int get_split_schema_version_v2(int64_t& split_schema_version_v2);

  virtual int get_rootservice_epoch(int64_t& rootservice_epoch);
  virtual int get_frozen_info(int64_t& frozen_version);
  // before split schema:
  virtual int set_snapshot_info(const int64_t snapshot_gc_ts, const int64_t gc_schema_version);
  virtual int get_snapshot_info(int64_t& snapshot_gc_ts, int64_t& gc_schema_version);
  static int select_gc_timestamp_for_update(common::ObISQLClient& sql_client, int64_t& gc_timestamp);
  // after split schema:
  virtual int set_snapshot_info_v2(common::ObISQLClient& sql_proxy, const int64_t snapshot_gc_ts,
      const common::ObIArray<TenantIdAndSchemaVersion>& tenant_schema_versions, const bool set_gc_schema_version);
  // tenant_id= 0 represent getting all gc informations of all tenants
  int get_snapshot_gc_ts(int64_t& snapshot_gc_ts);
  virtual int get_snapshot_info_v2(common::ObISQLClient& sql_proxy, const uint64_t tenant_id,
      const bool need_gc_schema_version, int64_t& snapshot_gc_ts,
      common::ObIArray<TenantIdAndSchemaVersion>& schema_versions);
  // interface of standby
  int set_snapshot_gc_ts(const int64_t snapshot_gc_ts);
  int set_gc_schema_versions(
      common::ObISQLClient& sql_client, const common::ObIArray<TenantIdAndSchemaVersion>& gc_schema_versions);
  int get_schema_snapshot_version(int64_t& snapshot_schema_version, int64_t& snapshot_frozen_version);
  // next_new_schema_version of primary cluster
  int set_next_schema_version(const int64_t next_schema_version);
  int get_next_schema_version(int64_t& next_schema_version);

private:
  int update(const ObGlobalStatItem::ItemList& list, const bool is_incremental = false);
  int get(ObGlobalStatItem::ItemList& list, const int64_t frozen_version = -1);

private:
  static const char* OB_ALL_GC_SCHEMA_VERSION_TNAME;
  static const char* TENANT_ID_CNAME;
  static const char* GC_SCHEMA_VERSION_CNAME;

private:
  ObCoreTableProxy core_table_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_GLOBAL_STAT_PROXY_H_
