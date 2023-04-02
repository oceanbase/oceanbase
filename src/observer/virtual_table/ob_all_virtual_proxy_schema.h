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

#include "share/ob_virtual_table_iterator.h" // ObVirtualTableIterator
#include "share/location_cache/ob_location_struct.h" // ObLSLocation
#include "share/schema/ob_schema_getter_guard.h" // ObSchemaGetterGuard
#include "lib/container/ob_se_array.h" // ObSEArray

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;

namespace sqlclient
{
class ObMySQLResult;
}
} // end namespace common

namespace share
{
class ObLocationService;

namespace schema
{
class ObTableSchema;
}
} // end namespace share

namespace observer
{
class ObInnerSQLResult;
class ObAllVirtualProxySchema : public common::ObVirtualTableIterator
{
  enum class DupReplicaType
  {
    INVALID_TYPE = -1,
    NON_DUP_REPLICA = 0,
    DUP_REPLICA = 1,
  };
  enum ALL_VIRTUAL_PROXY_SCHEMA_TABLE_COLUMNS
  {
    TENANT_NAME = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    DATABASE_NAME,
    TABLE_NAME,
    TABLET_ID,
    SERVER_IP,
    SQL_PORT,
    TABLE_ID,
    ROLE,
    PART_NUM,
    REPLICA_NUM,
    TABLE_TYPE,
    SCHEMA_VERSION,
    REPLICA_TYPE,
    DUP_REPICA_TYPE,
    MEMSTORE_PERCENT,
    SPARE1,//int, unused
    SPARE2,//int, unused
    SPARE3,//int, unused
    SPARE4,//varchar, unused
    SPARE5,//varchar, unused
    SPARE6,//varchar, unused
    COMPLEX_TABLE_TYPE,//int, complex_table_type
    LEVEL1_DECODED_DB_NAME,//varchar
    LEVEL1_DECODED_TABLE_NAME,//varchar
    LEVEL2_DECODED_DB_NAME,//varchar
    LEVEL2_DECODED_TABLE_NAME,//varchar
  };

  enum ObComplexTableType
  {
    CT_DEFAULT = 0,
    CT_SYNONYM,     //base_table --> synonym
    CT_VIEW,        //base_table --> view
    CT_SYNONYM_VIEW,//base_table --> synonym --> view
    CT_VIEW_SYNONYM,//base_table --> view --> synonym
    MAX_CT_TYPE
  };

  // ObTenantServer is used to store location of tenant's unit
  class ObTenantServer
  {
  public:
    ObTenantServer() : virtual_tablet_id_(), location_() {};
    ~ObTenantServer() {};
    void reset();
    const share::ObLSReplicaLocation &get_location() const { return location_; }
    const common::ObTabletID &get_virtual_tablet_id() const { return virtual_tablet_id_; }
    void set_virtual_tablet_id(common::ObTabletID tablet_id) { virtual_tablet_id_ = tablet_id; }
    void set_role(const common::ObRole &role) { location_.set_role(role); }
    int set_location(const share::ObLSReplicaLocation &location);
    TO_STRING_KV(K_(virtual_tablet_id), K_(location));
  private:
    common::ObTabletID virtual_tablet_id_;
    share::ObLSReplicaLocation location_;
  };

public:
  ObAllVirtualProxySchema();
  virtual ~ObAllVirtualProxySchema();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  int init(
      const bool force_sql_refresh,
      share::schema::ObMultiVersionSchemaService &schema_service,
      share::ObLocationService *location_service,
      common::ObMySQLProxy *sql_proxy,
      common::ObIAllocator *allocator);

private:
  int init_data();
  // get base table's schema from view
  int get_view_decoded_schema_(
      const uint64_t tenant_id,
      const common::ObString &tenant_name,
      const common::ObString &view_definition,
      const bool is_oracle_mode,
      const share::schema::ObTableSchema *&new_table_schema);
  int inner_get_next_row_();
  int fill_row_(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObString &table_name,
      const share::schema::ObTableSchema &table_schema,
      const share::ObLSReplicaLocation &replica,
      const common::ObTabletID &tablet_id,
      const DupReplicaType dup_replica_type);

  //////////// tenant server related ////////////
  int get_next_tenant_server_(const common::ObString &table_name,
                              const share::schema::ObTableSchema *table_schema);
  int get_tenant_servers_(const uint64_t tenant_id);
  int fill_tenant_servers_(
      const uint64_t tenant_id,
      common::sqlclient::ObMySQLResult &result,
      ObIArray<int64_t> &first_idx_in_zone);
  int get_tenant_servers_by_join_(
      const uint64_t tenant_id,
      ObIArray<int64_t> &first_idx_in_zone);

  //////////// tablet location related ////////////
  int get_next_tablet_location_(
      const common::ObString &table_name,
      const share::schema::ObTableSchema *table_schema,
      const common::ObTabletID &tablet_id);
  int get_table_tablet_location_(
      const int64_t table_idx,
      const common::ObTabletID &tablet_id);
  // add fake location to location_ for virtual table
  int add_virtual_tablet_location_();
  int fetch_tablet_location_(
      const share::schema::ObTableSchema &table_schema,
      const common::ObTabletID &tablet_id);
  // if input tablet_id is invalid, return first tablet of the table
  int get_actual_tablet_id_(
      const share::schema::ObTableSchema &table_schema,
      common::ObTabletID &tablet_id);
  int init_convert_ctx();
  int convert_output_row(ObNewRow *&cur_row);
  int gen_column_value(char *&buf, int64_t len,
                       const ObString &str, const bool is_oracle_mode);

private:
  bool is_inited_;
  common::ObArenaAllocator inner_alloc_;
  common::ObArenaAllocator convert_alloc_;
  common::ObCastCtx cast_ctx_;
  common::ObNewRow convert_row_;
  bool force_sql_refresh_; // use for refreshing location cache forcely
  int64_t next_table_idx_; // index for table_schemas_
  int64_t next_replica_idx_; // index for location_
  int64_t next_server_idx_; // index for tenant_servers_
  char ip_buf_[common::OB_IP_STR_BUFF];
  common::ObString input_tenant_name_;
  common::ObString input_db_name_;
  common::ObSEArray<common::ObString, 1> input_table_names_;
  // use for decoding synonym and view schema
  common::ObString level1_decoded_db_name_;
  common::ObString level1_decoded_table_name_;
  common::ObString level2_decoded_db_name_;
  common::ObString level2_decoded_table_name_;
  ObComplexTableType complex_table_type_;
  common::ObSEArray<const share::schema::ObTableSchema *, 1> table_schemas_;
  common::ObSEArray<common::ObTabletID, 1> tablet_ids_;
  common::ObSEArray<ObTenantServer, 64> tenant_servers_;
  common::ObMySQLProxy::MySQLResult *sql_res_;
  share::ObLSLocation location_;
  share::schema::ObSchemaGetterGuard schema_guard_; // tenant_schema_guard
  // dependency
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLocationService *location_service_;
  common::ObMySQLProxy *sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxySchema);
};

} // end of namespace observer
} // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_SCHEMA_ */
