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
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
struct ObGlobalStatItem : public common::ObDLinkBase<ObGlobalStatItem>
{
  typedef common::ObDList<ObGlobalStatItem> ItemList;
  ObGlobalStatItem(ItemList &list, const char *name, int64_t value)
    : name_(name), value_(value) { list.add_last(this); }

  TO_STRING_KV(K_(name), K_(value));

  const char *name_;
  int64_t value_;
};

static const char *OB_ALL_GLOBAL_STAT_TNAME = "__all_global_stat";
class ObGlobalStatProxy
{
public:
  ObGlobalStatProxy(common::ObISQLClient &client,
                    const uint64_t tenant_id)
      : core_table_(OB_ALL_GLOBAL_STAT_TNAME, client, tenant_id)
  {}
  virtual ~ObGlobalStatProxy() {}
  void set_sql_client(common::ObISQLClient &client) { core_table_.set_sql_client(client); }
  bool is_valid() const { return core_table_.is_valid(); }

  virtual int set_init_value(const int64_t core_schema_version,
                             const int64_t baseline_schema_version,
                             const int64_t rootservice_epoch,
                             const SCN &snapshot_gc_scn,
                             const int64_t gc_schema_version,
			                       const int64_t ddl_epoch,
                             const uint64_t target_data_version,
                             const uint64_t current_data_version);

  virtual int set_tenant_init_global_stat(const int64_t core_schema_version,
                                          const int64_t baseline_schema_version,
                                          const SCN &snapshot_gc_scn,
                                          const int64_t ddl_epoch,
                                          const uint64_t target_data_version,
                                          const uint64_t current_data_version);

  virtual int set_core_schema_version(const int64_t core_schema_version);
  virtual int set_baseline_schema_version(const int64_t baseline_schema_version);
  virtual int inc_rootservice_epoch();

  virtual int get_core_schema_version(int64_t &core_schema_version);
  virtual int get_baseline_schema_version(int64_t &baseline_schema_version);

  virtual int get_rootservice_epoch(int64_t &rootservice_epoch);

  int update_current_data_version(const uint64_t current_data_version);
  int get_current_data_version(uint64_t &current_data_version);
  static int get_target_data_version_ora_rowscn(
    const uint64_t tenant_id,
    share::SCN &target_data_version_ora_rowscn);
  int update_target_data_version(const uint64_t target_data_version);
  int get_target_data_version(const bool for_update, uint64_t &target_data_version);
  int update_finish_data_version(const uint64_t finish_data_version, const share::SCN &scn);
  int get_finish_data_version(uint64_t &finish_data_version, share::SCN &scn);

  virtual int get_snapshot_info(int64_t &snapshot_gc_scn,
                                int64_t &gc_schema_version);
  static int select_snapshot_gc_scn_for_update_nowait(common::ObISQLClient &sql_client,
                                               const uint64_t tenant_id,
                                               SCN &snapshot_gc_scn);
  static int select_snapshot_gc_scn_for_update(common::ObISQLClient &sql_client,
                                               const uint64_t tenant_id,
                                               SCN &snapshot_gc_scn);
  static int get_snapshot_gc_scn(common::ObISQLClient &sql_client,
                                 const uint64_t tenant_id,
                                 share::SCN &snapshot_gc_scn);
  static int update_snapshot_gc_scn(common::ObISQLClient &sql_client,
                                    const uint64_t tenant_id,
                                    const SCN &snapshot_gc_scn,
                                    int64_t &affected_rows);
  int get_snapshot_gc_scn(share::SCN &snapshot_gc_scn);
  //interface of standby
  int set_snapshot_gc_scn(const SCN &snapshot_gc_scn);
  int set_snapshot_gc_scn(const int64_t snapshot_gc_scn);

  int set_ddl_epoch(const int64_t ddl_epoch, bool is_incremental = true);

  static int select_ddl_epoch_for_update(common::ObISQLClient &sql_client,
                                               const uint64_t tenant_id,
                                               int64_t &ddl_epoch);
  int get_ddl_epoch(int64_t &ddl_epoch);
private:
  static int inner_get_snapshot_gc_scn_(common::ObISQLClient &sql_client,
                                        const uint64_t tenant_id,
                                        SCN &snapshot_gc_scn,
                                        const char *for_update_str);
  int update(const ObGlobalStatItem::ItemList &list, const bool is_incremental = false);
  int get(ObGlobalStatItem::ItemList &list, bool for_update = false);

private:
  static const char* OB_ALL_GC_SCHEMA_VERSION_TNAME;
  static const char* TENANT_ID_CNAME;
  static const char* GC_SCHEMA_VERSION_CNAME;
private:
  ObCoreTableProxy core_table_;

};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_GLOBAL_STAT_PROXY_H_
