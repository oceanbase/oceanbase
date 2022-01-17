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

#ifndef OCEANBASE_STORAGE_OB_LOCALITY_MANAGER_H_
#define OCEANBASE_STORAGE_OB_LOCALITY_MANAGER_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_locality_info.h"
#include "share/ob_locality_table_operator.h"
#include "common/ob_zone_type.h"
#include "share/ob_alive_server_tracer.h"
#include "share/ob_i_server_auth.h"
namespace oceanbase {
namespace share {
class ObRemoteSqlProxy;
}
namespace storage {
class ObLocalityManager : public share::ObILocalityManager, public share::ObIServerAuth {
public:
  ObLocalityManager();
  virtual ~ObLocalityManager()
  {
    destroy();
  }
  int init(const common::ObAddr& self, common::ObMySQLProxy* sql_proxy, share::ObRemoteSqlProxy* remote_sql_proxy);
  void reset();
  void destroy();
  int is_server_legitimate(const common::ObAddr& addr, bool& is_valid);
  int check_ssl_invited_nodes(easy_connection_t& c);
  void set_ssl_invited_nodes(const common::ObString& new_value);
  int load_region();
  int load_zone();
  int get_locality_info(share::ObLocalityInfo& locality_info);
  int get_version(int64_t& version) const;
  int set_version(int64_t version);
  int get_region_priority(const uint64_t tenant_id, uint64_t& region_priority);
  int get_local_region(common::ObRegion& region) const;
  int get_local_zone_type(common::ObZoneType& zone_type);
  int get_server_locality_array(
      common::ObIArray<share::ObServerLocality>& server_locality_array, bool& has_readonly_zone) const;
  int get_server_region(const common::ObAddr& server, common::ObRegion& region) const;
  int get_server_region_across_cluster(const common::ObAddr& server, common::ObRegion& region) const;
  int get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const;
  int get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const;
  int record_server_region(const common::ObAddr& server, const common::ObRegion& region);
  int record_server_idc(const common::ObAddr& server, const common::ObIDC& idc);
  int record_server_cluster_id(const common::ObAddr& server, const int64_t& cluster_id);
  int get_server_zone(const common::ObAddr& server, common::ObZone& zone) const;
  int get_noempty_zone_region(const common::ObZone& zone, common::ObRegion& region) const;
  virtual int is_local_zone_read_only(bool& is_readonly);
  virtual int is_local_server(const common::ObAddr& server, bool& is_local);
  int is_same_zone(const common::ObAddr& server, bool& is_same_zone);

private:
  int get_locality_zone(const uint64_t tenant_id, share::ObLocalityZone& locality_zone);
  int set_locality_info(share::ObLocalityInfo& locality_info);
  int check_if_locality_has_been_loaded();
  int set_partition_region_priority();

private:
  bool is_inited_;
  mutable common::SpinRWLock rwlock_;
  common::ObAddr self_;
  common::ObMySQLProxy* sql_proxy_;
  share::ObRemoteSqlProxy* remote_sql_proxy_;
  share::ObLocalityInfo locality_info_;
  share::ObServerLocalityCache server_locality_cache_;
  share::ObServerLocalityCache remote_server_locality_cache_;
  share::ObLocalityTableOperator locality_operator_;
  char* ssl_invited_nodes_buf_;  // common::OB_MAX_CONFIG_VALUE_LEN, use new
  bool is_loaded_;
  static const int64_t FAIL_TO_LOAD_LOCALITY_CACHE_TIMEOUT = 60L * 1000L * 1000L;
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_LOCALITY_MANAGER_H_
