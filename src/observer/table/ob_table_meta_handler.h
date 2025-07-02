/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBSERVER_TABLE_OB_TABLE_META_HANDLER_H_
#define OBSERVER_TABLE_OB_TABLE_META_HANDLER_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/table/ob_table_rpc_proxy.h"
#include "ob_table_rpc_processor.h"
#include "utils/ob_table_json_utils.h"

namespace oceanbase
{
namespace table
{

/*----------------------------------------------------------------Table Meta Handler----------------------------------------------------------------*/
class ObITableMetaHandler
{
public:
  virtual ~ObITableMetaHandler() = default;
  virtual int pre_check() { return OB_SUCCESS; }
  // parse the json data string to json value
  virtual int parse(const ObTableMetaRequest &request) = 0;
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) = 0;
protected:
  // check _enable_kv_hbase_admin_ddl in ddl related handler's pre_check
  int check_hbase_ddl_enable();
  int parse_htable_name(common::ObIAllocator &allocator, const ObTableMetaRequest &request, ObString &htable_name);
  int get_tablet_boundary_internal(common::ObIAllocator &allocator,
                                   const schema::ObSimpleTableSchemaV2 &table_schema,
                                   const common::ObIArray<ObTabletID> &tablet_ids,
                                   ObIArray<common::ObString> &upperbound_trims);
  int format_tablet_ids(common::ObIAllocator &allocator,
                        const ObIArray<ObTabletID>& tablet_ids,
                        int start_idx,
                        int count,
                        char *&result);
private:
  int check_upperbound_legality_and_trim(common::ObString &upperbound);
protected:
  const int64_t BATCH_SIZE = 100;
};

class ObHTableRegionLocatorHandler : public ObITableMetaHandler
{
public:
  ObHTableRegionLocatorHandler(ObIAllocator &allocator)
    : allocator_(allocator)
  {
    table_schemas_.set_attr(ObMemAttr(MTL_ID(), "TbTableSchemas"));
    tablet_infos_.set_attr(ObMemAttr(MTL_ID(), "TbTabletInfos"));
  }
  virtual ~ObHTableRegionLocatorHandler();
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  int parse(const ObTableMetaRequest &request) override;
  int try_compress(ObTableMetaResponse &response);

private:
  int get_tablet_boundary(ObTableExecCtx &ctx);
  int get_tablet_location(ObTableExecCtx &ctx);
  int binary_search_tablet_info_idx(const int64_t tablet_id, int64_t &index);

  int build_table_and_replica_dicts(ObIAllocator &allocator,
                                    ObIArray<int64_t> &table_id_dict,
                                    hash::ObHashMap<int64_t, int> &table_id_map,
                                    ObIArray<ObString> &replica_dict,
                                    hash::ObHashMap<ObString, int> &replica_map);
  int build_partitions_array(ObIAllocator &allocator,
                            hash::ObHashMap<int64_t, int> &table_id_map,
                            hash::ObHashMap<ObString, int> &replica_map,
                            ObTableJsonArrayBuilder &partitions_builder);
  int build_table_id_dict_array(ObIAllocator &allocator,
                               ObIArray<int64_t> &table_id_dict,
                               ObTableJsonArrayBuilder &table_id_dict_builder);
  int build_replica_dict_array(ObIAllocator &allocator,
                              ObIArray<ObString> &replica_dict,
                              ObTableJsonArrayBuilder &replica_dict_builder);
  int build_root_object(ObIAllocator &allocator,
                       ObTableJsonArrayBuilder &table_id_dict_builder,
                       ObTableJsonArrayBuilder &replica_dict_builder,
                       ObTableJsonArrayBuilder &partitions_builder,
                       json::Value *&root);

private:
  struct TabletLocation {
    TabletLocation(const ObString &svr_ip, int64_t svr_port, int64_t role)
      : svr_ip_(svr_ip),
        svr_port_(svr_port),
        role_(role)
    {
    }
    TO_STRING_KV(K_(svr_ip), K_(svr_port), K_(role));
    ObString svr_ip_;
    int64_t svr_port_;
    int64_t role_;
  };
  struct TabletInfo {
    TabletInfo(int64_t table_id, int64_t tablet_id, const ObString &upperbound)
      : table_id_(table_id),
        tablet_id_(tablet_id),
        upperbound_(upperbound),
        replicas_()
    {
      replicas_.set_attr(ObMemAttr(MTL_ID(), "TbTabletReps"));
    }
    TO_STRING_KV(K_(table_id), K_(tablet_id), K_(upperbound), K_(replicas));
    int64_t table_id_;
    int64_t tablet_id_;
    ObString upperbound_;
    ObSEArray<TabletLocation*, 16> replicas_;
  };
private:
  const char *TABLET_LOCATION_SQL = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id, A.svr_ip, B.svr_port, A.role \
                                    FROM oceanbase.__all_virtual_proxy_schema A JOIN oceanbase.__all_server B ON A.svr_ip = B.svr_ip AND A.sql_port = B.inner_port \
                                    WHERE A.tablet_id in (%.*s) AND A.tenant_name = '%s' AND A.database_name = '%s' AND A.table_name = '%s' \
                                    ORDER BY A.role DESC, A.svr_ip, B.svr_port;";
private:
  ObString htable_name_;
  ObSEArray<const schema::ObSimpleTableSchemaV2*, 3> table_schemas_;
  ObSEArray<TabletInfo*, 16> tablet_infos_;
  // used to allocate memory for json node
  common::ObIAllocator &allocator_;
};

class ObHTableRegionMetricsHandler : public ObITableMetaHandler
{
private:
  static const uint64_t MOCK_SS_TABLET_SIZE = 8L * 1073741824; // 8 GB
public:
  ObHTableRegionMetricsHandler(ObIAllocator &allocator)
    : htable_name_(),
      allocator_(allocator)
  {
  }
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  virtual int parse(const ObTableMetaRequest &request) override;

private:
  struct ObTableRegionMetricsResult
  {
    ObTableRegionMetricsResult(common::ObIAllocator &allocator)
    : table_id_(OB_INVALID_ID),
      tablet_ids_(allocator),
      mem_tablet_sizes_(allocator),
      ss_tablet_sizes_(allocator),
      boundarys_(allocator)
    {}
    ~ObTableRegionMetricsResult() = default;
    uint64_t table_id_;
    ObFixedArray<uint64_t, common::ObIAllocator> tablet_ids_;
    ObFixedArray<uint64_t, common::ObIAllocator> mem_tablet_sizes_;
    ObFixedArray<uint64_t, common::ObIAllocator> ss_tablet_sizes_;
    ObFixedArray<common::ObString, common::ObIAllocator> boundarys_;
    int init(const common::ObIArray<ObTabletID> &tablet_ids, uint64_t table_id);
    TO_STRING_KV(K_(table_id));
  };
  int check_metrics_legality(const ObTableRegionMetricsResult &table_metrics);
  int acquire_table_metric(ObTableExecCtx &ctx,
                           const ObIArray<const schema::ObSimpleTableSchemaV2*> &table_schemas,
                           ObTableRegionMetricsResult &table_metrics);
  int construct_response(const ObTableRegionMetricsResult &table_metrics_result, ObTableMetaResponse &response);

private:
  ObString htable_name_;
  common::ObIAllocator &allocator_;
};

class ObHTableDeleteHandler : public ObITableMetaHandler
{
public:
ObHTableDeleteHandler(ObIAllocator &allocator)
   : htable_name_(),
     allocator_(allocator)
  {
  }
  virtual ~ObHTableDeleteHandler() = default;
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  virtual int parse(const ObTableMetaRequest &request) override;
  virtual int pre_check() override { return check_hbase_ddl_enable(); }

private:
  ObString htable_name_;
  common::ObIAllocator &allocator_;
};

class ObHTableExistsHandler : public ObITableMetaHandler
{
public:
  ObHTableExistsHandler(ObIAllocator &allocator)
   : htable_name_(),
     allocator_(allocator)
  {
  }
  virtual ~ObHTableExistsHandler() = default;
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  virtual int parse(const ObTableMetaRequest &request) override;

private:
  ObString htable_name_;
  common::ObIAllocator &allocator_;
};
class ObHTableCreateHandler : public ObITableMetaHandler
{
public:
  ObHTableCreateHandler(ObIAllocator &allocator)
    : allocator_(allocator),
      partition_type_(PARTITION_TYPE_INVALID),
      htable_name_()
  {
    column_families_.set_attr(ObMemAttr(MTL_ID(), "TbColFamily"));
  }
  virtual ~ObHTableCreateHandler();
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  virtual int parse(const ObTableMetaRequest &request) override;
  virtual int pre_check() override { return check_hbase_ddl_enable(); }
  int get_create_table_sql(ObString &tablegroup_sql, ObIArray<ObString> &column_family_sql_array);
private:
  struct ColumnFamilyDescriptor {
    ColumnFamilyDescriptor(const ObString &name, int64_t ttl, int64_t max_version)
      : name_(name),
        ttl_(ttl),
        max_version_(max_version)
    {
    }

    bool has_ttl() const { return ttl_ != INT_MAX; }
    bool has_max_version() const { return max_version_ != INT_MAX; }
    bool operator<(const ColumnFamilyDescriptor &other) const {
      return name_ < other.name_;
    }

    TO_STRING_KV(K_(name), K_(ttl), K_(max_version));
    ObString name_;
    int64_t ttl_;
    int64_t max_version_;
  };

  enum PartitionType : uint8_t {
    PARTITION_TYPE_NONE = 0,
    PARTITION_TYPE_KEY = 1,
    PARTITION_TYPE_RANGE = 2,
    PARTITION_TYPE_INVALID
  };

  static constexpr const char *CREATE_TABLEGROUP_SQL = "CREATE TABLEGROUP `%.*s`";
  static constexpr const char *TABLE_DEF =
    "CREATE TABLE `%.*s$%.*s` ("
    "K varbinary(1024) NOT NULL, "
    "Q varbinary(256) NOT NULL, "
    "T bigint NOT NULL, "
    "V varbinary(1048576) NOT NULL, "
    "PRIMARY KEY (K, Q, T))";
  static constexpr const char *TABLE_GROUP_TEMPLATE = "TABLEGROUP = `%.*s`";
  static constexpr const char *NO_PARTITION = "";
  static constexpr const char *KEY_PARTITION_TEMPLATE = "PARTITION BY KEY(%.*s) PARTITIONS %d";
  static constexpr const char *RANGE_PARTITION_TEMPLATE = "PARTITION BY RANGE COLUMNS(%.*s) (%.*s)";
  static constexpr const char *KV_EMPTY_TEMPLATE = "kv_attributes = '{\"Hbase\": {\"CreatedBy\": \"Admin\"}}'";
  static constexpr const char *KV_TTL_TEMPLATE = "kv_attributes = '{\"Hbase\": {\"TimeToLive\": %ld, \"CreatedBy\": \"Admin\"}}'";
  static constexpr const char *KV_MAX_VERSIONS_TEMPLATE = "kv_attributes = '{\"Hbase\": {\"MaxVersions\": %ld, \"CreatedBy\": \"Admin\"}}'";
  static constexpr const char *KV_TTL_AND_MAX_VERSIONS_TEMPLATE = "kv_attributes = '{\"Hbase\": {\"TimeToLive\": %ld, \"MaxVersions\": %ld, \"CreatedBy\": \"Admin\"}}'";

  static int build_create_table_sql(ObIAllocator &allocator,
                                    const ObString &htable_name,
                                    const ObString &family_name,
                                    const ObString &tablegroup,
                                    const ObString &kv_attributes,
                                    const ObString &partition,
                                    ObString &sql);

  static int build_range_partition_sql(ObIAllocator &allocator,
                                       const ObString &column,
                                       const ObIArray<ObString> &upperbounds,
                                       ObString &sql);

private:
  ObIAllocator &allocator_;
  PartitionType partition_type_;
  ObString htable_name_; // table group name
  ObSEArray<ColumnFamilyDescriptor*, 3> column_families_;
};
class ObHTableGetDescHandler : public ObITableMetaHandler
{
public:
  ObHTableGetDescHandler(ObIAllocator &allocator)
    : allocator_(allocator)
  {
  }
  virtual ~ObHTableGetDescHandler();
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;
  int parse(const ObTableMetaRequest &request) override;
private:
  int get_cf_desc(ObTableExecCtx &ctx);
  int get_table_desc(ObTableExecCtx &ctx);

private:
  struct CfDesc {
    explicit CfDesc(const ObString &cf_name, int64_t ttl, int64_t max_versions, bool is_disable)
      : cf_name_(cf_name),
        ttl_(ttl),
        max_versions_(max_versions),
        is_disable_(is_disable)
    {
    }
    TO_STRING_KV(K_(cf_name), K_(ttl), K_(max_versions), K_(is_disable));
    ObString cf_name_;
    int64_t ttl_;
    int64_t max_versions_;
    bool is_disable_;
  };
  struct TableDesc {
    TableDesc(const ObString &table_name): table_name_(table_name) {}
    TO_STRING_KV(K_(table_name));
    ObString table_name_;
  };


private:
  common::ObIAllocator &allocator_;
  ObString htable_name_;
  ObSEArray<CfDesc*, 3> cf_descs_;
  ObSEArray<TableDesc*, 1> table_descs_;
};

class ObHTableAccessControlHandler : public ObITableMetaHandler
{
public:
  ObHTableAccessControlHandler(ObIAllocator &allocator) : allocator_(allocator), table_group_name_()
  {
  }
  virtual ~ObHTableAccessControlHandler() = default;
  TO_STRING_KV(K_(table_group_name));

public:
  virtual int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) = 0;
  int parse(const ObTableMetaRequest &request) override;
  virtual int pre_check() override { return check_hbase_ddl_enable(); }

protected:
  // used to allocate memory for json node
  common::ObIAllocator &allocator_;
  common::ObString table_group_name_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableAccessControlHandler);
};

class ObHTableDisableTableHandler : public ObHTableAccessControlHandler
{
public:
  ObHTableDisableTableHandler(ObIAllocator &allocator) : ObHTableAccessControlHandler(allocator) {}
  virtual ~ObHTableDisableTableHandler() = default;

public:
  int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableDisableTableHandler);
};

class ObHTableEnableTableHandler : public ObHTableAccessControlHandler
{
public:
  ObHTableEnableTableHandler(ObIAllocator &allocator) : ObHTableAccessControlHandler(allocator) {}
  virtual ~ObHTableEnableTableHandler() = default;

public:
  int handle(ObTableExecCtx &ctx, ObTableMetaResponse &response) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHTableEnableTableHandler);
};

class ObTableMetaHandlerGuard
{
public:
  ObTableMetaHandlerGuard(ObIAllocator &allocator)
    : handler_(nullptr),
      allocator_(allocator)
  {
  }

  ~ObTableMetaHandlerGuard()
  {
    OB_DELETEx(ObITableMetaHandler, &allocator_, handler_);
  }

  int get_handler(ObTableRpcMetaType meta_type, ObITableMetaHandler *&handler);
private:
  ObITableMetaHandler *handler_;
  ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObTableMetaHandlerGuard);
};

} // namespace table
} // namespace oceanbase

#endif  // OBSERVER_TABLE_OB_TABLE_META_HANDLER_H_
