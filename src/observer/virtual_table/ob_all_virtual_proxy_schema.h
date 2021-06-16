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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SCHEMA_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SCHEMA_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"

#include "share/ob_virtual_table_iterator.h"
#include "common/ob_range.h"
#include "storage/ob_dup_replica_checker.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObServerConfig;
}  // namespace common
namespace sql {
class ObSQLSessionInfo;
}
namespace share {
class ObReplicaLocation;
class ObPartitionLocationCache;
namespace schema {
class ObTableSchema;
}
}  // namespace share
namespace observer {
class ObAllVirtualProxySchema : public common::ObVirtualTableIterator {
  enum ALL_VIRTUAL_PROXY_SCHEMA_TABLE_COLUMNS {
    TENANT_NAME = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    DATABASE_NAME,
    TABLE_NAME,
    PARTITION_ID,
    SERVER_IP,
    SQL_PORT,
    TABLE_ID,
    ROLE,
    PART_NUM,
    REPLICA_NUM,
    TABLE_TYPE,
    SCHEMA_VERSION,
    SPARE1,                     // int, used for replica type
    SPARE2,                     // int, used for dup replica type
    SPARE3,                     // int, replica_property_memstore_percent
    SPARE4,                     // varchar, used for route_sql
    SPARE5,                     // varchar, unused
    SPARE6,                     // varchar, unused
    COMPLEX_TABLE_TYPE,         // int, complex_table_type
    LEVEL1_DECODED_DB_NAME,     // varchar
    LEVEL1_DECODED_TABLE_NAME,  // varchar
    LEVEL2_DECODED_DB_NAME,     // varchar
    LEVEL2_DECODED_TABLE_NAME,  // varchar
  };

  enum ObComplexTableType {
    CT_DEFAULT = 0,
    CT_SYNONYM,       // base_table --> synonym
    CT_VIEW,          // base_table --> view
    CT_SYNONYM_VIEW,  // base_table --> synonym --> view
    CT_VIEW_SYNONYM,  // base_table --> view --> synonym
    MAX_CT_TYPE
  };

public:
  ObAllVirtualProxySchema();
  virtual ~ObAllVirtualProxySchema();

  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow*& row);

  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  int set_pl_cache(share::ObPartitionLocationCache* pl_cache);
  void set_sql_proxy(common::ObMySQLProxy* sql_proxy)
  {
    sql_proxy_ = sql_proxy;
  }

  void set_config(common::ObServerConfig* config)
  {
    config_ = config;
  }
  void set_schema_service(share::schema::ObMultiVersionSchemaService& schema_service);
  void set_refresh_hint(const bool force_sql_refresh)
  {
    force_sql_refresh_ = force_sql_refresh;
  }
  class ObTenantServer {
  public:
    ObTenantServer() : location_(), virtual_partition_id_(0){};
    ~ObTenantServer(){};

    void reset()
    {
      location_.reset();
      virtual_partition_id_ = 0;
    }
    TO_STRING_KV(K_(virtual_partition_id), K_(location));

    share::ObReplicaLocation location_;
    int64_t virtual_partition_id_;
  };

private:
  int inner_get_next_row();
  int get_tenant_db_name(
      const uint64_t tenant_id, const uint64_t database_id, common::ObString& tenant_name, common::ObString& db_name);
  int get_view_decoded_schema(const uint64_t tenant_id, common::ObString& tenant_name,
      const common::ObString& view_definition, const share::schema::ObTableSchema*& new_table_schema);
  int fill_row(share::schema::ObSchemaGetterGuard& schema_guard, const share::schema::ObTableSchema& table_schema,
      const share::ObReplicaLocation& replica, const int64_t part_idx, const storage::DupReplicaType dup_replica_type);
  int get_table_locations(const int64_t table_idx, const int64_t part_idx);
  int get_table_partition_location(const int64_t table_idx, const int64_t part_idx);

  int get_tenant_servers(const uint64_t tenant_id);
  int fill_tenant_servers(
      common::sqlclient::ObMySQLResult& result, ObIArray<int64_t>& first_idx_in_zone, const uint64_t tenant_id);
  int get_tenant_servers_by_join(const uint64_t tenant_id, ObIArray<int64_t>& first_idx_in_zone, bool& is_mysql_error);
  int get_tenant_servers_separately(const uint64_t tenant_id, ObIArray<int64_t>& first_idx_in_zone);
  int get_tenant_resource_pool_id(const uint64_t tenant_id, int64_t& resource_pool_id);
  int get_tenant_server_addr(const int64_t resource_pool_id, ObIArray<common::ObAddr>& tenant_server_addr);
  int get_tenant_server_sql_port(const ObIArray<common::ObAddr>& tenant_server_addr,
      ObIArray<int64_t>& first_idx_in_zone, const uint64_t tenant_id);
  static common::ObReplicaType get_locality_type(const share::ObZoneReplicaNumSet& zone_locality);

  int add_virtual_partition_location(const int64_t table_id, const int64_t partition_idx);
  int fetch_partition_location(
      const share::schema::ObTableSchema& table_schema, const int64_t table_id, const int64_t partition_idx);
  int get_actual_partition_idx(const share::schema::ObTableSchema& table_schema, int64_t& partition_idx);
  int transfer_table_schema(const share::schema::ObTableSchema*& table_schema);

private:
  share::ObPartitionLocationCache* pl_cache_;
  common::ObMySQLProxy* sql_proxy_;
  uint64_t tenant_id_;
  common::ObSEArray<const share::schema::ObTableSchema*, 1> table_schemas_;
  common::ObSEArray<int64_t, 1> partition_ids_;
  common::ObSEArray<ObTenantServer, 64> tenant_servers_;
  share::ObPartitionLocation location_;
  int64_t next_table_idx_;
  int64_t next_replica_idx_;
  int64_t next_server_idx_;
  char ip_buf[common::OB_IP_STR_BUFF];
  common::ObServerConfig* config_;

  common::ObSEArray<ObString, 1> routine_databases_;
  common::ObSEArray<ObString, 1> routine_names_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::schema::ObSchemaGetterGuard full_schema_guard_;  // Not sure if the incoming SQL is tenant_schema_guard, please
                                                          // try again here for safety

  common::ObString input_tenant_name_;
  common::ObString input_db_name_;
  common::ObString input_table_name_;

  ObComplexTableType complex_table_type_;
  // need deepcopy
  common::ObString level1_decoded_db_name_;
  common::ObString level1_decoded_table_name_;
  common::ObString level2_decoded_db_name_;
  common::ObString level2_decoded_table_name_;
  bool force_sql_refresh_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxySchema);
};

inline int ObAllVirtualProxySchema::set_pl_cache(share::ObPartitionLocationCache* pl_cache)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(pl_cache)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    pl_cache_ = pl_cache;
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SCHEMA_ */
