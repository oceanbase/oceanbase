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


#define USING_LOG_PREFIX SERVER
#include "ob_table_meta_handler.h"
#include "utils/ob_table_sql_utils.h"
#include "ob_table_query_common.h"


using namespace oceanbase::table;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;

int ObITableMetaHandler::parse_htable_name(ObIAllocator &allocator, const ObTableMetaRequest &request, ObString &htable_name)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator parser_allocator;
  parser_allocator.set_attr(ObMemAttr(MTL_ID(), "TbParseAlc"));
  json::Value *root = nullptr;
  if (OB_FAIL(ObTableJsonUtils::parse(parser_allocator, request.data_, root))) {
    LOG_WARN("failed to parse request data", K(ret), K(request.data_));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", K(ret));
  } else if (root->get_type() != json::JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("request data is not a json object", K(ret), K(root->get_type()));
  } else {
    json::Value *htable_name_value = nullptr;
    if (OB_FAIL(ObTableJsonUtils::get_json_value(root, "table_name", json::JT_STRING, htable_name_value))) {
      LOG_WARN("failed to get table name", K(ret), K(request.data_));
    } else if (OB_ISNULL(htable_name_value)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable_name_value is null", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, htable_name_value->get_string(), htable_name, true))) {
      LOG_WARN("failed to write table name", K(ret), K(request.data_), K(htable_name_value->get_string()));
    }
  }
  return ret;
}

int ObITableMetaHandler::check_hbase_ddl_enable()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));

  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed get tenant config", K(ret), K(tenant_id));
  } else if (!tenant_config->_enable_kv_hbase_admin_ddl) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("execute hbase admin ddl but _enable_kv_hbase_admin_ddl is false", K(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "execute hbase admin ddl but _enable_kv_hbase_admin_ddl is false");
  }
  return ret;
}

int ObITableMetaHandler::check_upperbound_legality_and_trim(ObString &upperbound)
{
  int ret = OB_SUCCESS;
  if (upperbound.compare("MAXVALUE") == 0) {
    // do nothing
  } else if (upperbound.length() >= 2
             && upperbound.ptr()[0] == '\''
             && upperbound.ptr()[upperbound.length() - 1] == '\'') {
    upperbound.assign_ptr(upperbound.ptr() + 1, upperbound.length() - 2);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the format of upperbound is illegal", K(ret), K(upperbound));
  }
  return ret;
}

int ObITableMetaHandler::get_tablet_boundary_internal(ObIAllocator &allocator,
                                                      const ObSimpleTableSchemaV2 &table_schema, 
                                                      const ObIArray<ObTabletID> &tablet_ids, 
                                                      ObIArray<ObString> &upperbound_trims)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_partitioned_table()) {
    // non-partitioned table cannot be queried from __all_virtual_part
    // construct an object with empty boundary
    ObString upperbound_trim;
    if (OB_FAIL(upperbound_trims.push_back(upperbound_trim))) {
      LOG_WARN("failed to push back upperbound_trim", K(ret));
    }
  } else {
    if (table_schema.is_range_part() && table_schema.get_part_level() == PARTITION_LEVEL_ONE) {
      const char *PARTITION_BOUNDARY_SQL = "SELECT table_id, tablet_id, high_bound_val AS upperbound \
                                        FROM __all_virtual_part WHERE tenant_id = %d and table_id = %d and tablet_id in (%.*s) \
                                        ORDER BY tablet_id;";
      const char *PARTITION_BOUNDARY_SUBPART_SQL = "SELECT table_id, tablet_id, high_bound_val AS upperbound \
                                          FROM __all_virtual_sub_part WHERE tenant_id = %d and table_id = %d and tablet_id in (%.*s) \
                                          ORDER BY tablet_id;";
      const char* sql_template = table_schema.get_part_level() == PARTITION_LEVEL_ONE ? PARTITION_BOUNDARY_SQL
                                                                                      : PARTITION_BOUNDARY_SUBPART_SQL;
      ObSqlString sql;
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); j += BATCH_SIZE) {
        char *tablet_ids_str = nullptr;
        if (OB_FAIL(format_tablet_ids(allocator, tablet_ids, j, BATCH_SIZE, tablet_ids_str))) {
          LOG_WARN("failed to format tablet ids", K(ret), K(tablet_ids), K(j), K(BATCH_SIZE));
        } else if (OB_ISNULL(tablet_ids_str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to format tablet ids", K(ret));
        } else if (OB_FAIL(sql.assign_fmt(sql_template,
                                          MTL_ID(),
                                          table_schema.get_table_id(),
                                          strlen(tablet_ids_str),
                                          tablet_ids_str))) {
          LOG_WARN("failed to assign format", K(table_schema.get_table_id()), K(tablet_ids_str));
        } else {
          SMART_VAR(common::ObISQLClient::ReadResult, res) {
            ObMySQLResult *result = nullptr;
            if (OB_FAIL(ObTableSqlUtils::read(OB_SYS_TENANT_ID, sql.ptr(), res))) {
              LOG_WARN("failed to execute sql", K(ret), K(sql.string()));
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", K(ret), K(sql.string()));
            } else {
              int64_t table_id = 0;
              int64_t tablet_id = 0;
              ObString upperbound;
              ObString upperbound_trim;
              while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                table_id = 0;
                tablet_id = 0;
                upperbound.reset();
                upperbound_trim.reset();
                EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
                EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, int64_t);
                if (OB_FAIL(ret)) {
                } else if (table_id == 0 || tablet_id == 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid result", K(table_id), K(tablet_id));
                } else {
                  EXTRACT_VARCHAR_FIELD_MYSQL(*result, "upperbound", upperbound);
                  if (OB_FAIL(check_upperbound_legality_and_trim(upperbound))) {
                    LOG_WARN("the format of upperbound is illegal", K(ret), K(upperbound));
                  } else if (OB_FAIL(ob_write_string(allocator, upperbound, upperbound_trim))) {
                    LOG_WARN("failed to write upperbound", K(ret), K(upperbound));
                  } else if (OB_FAIL(upperbound_trims.push_back(upperbound_trim))) {
                    LOG_WARN("failed to push back upperbound_trim", K(ret));
                  }
                }
              }
              if (ret == OB_ITER_END) {
                ret = OB_SUCCESS;
              }
            }
          }
        }
        if (OB_NOT_NULL(tablet_ids_str)) {
          allocator.free(tablet_ids_str);
        }
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        // non-range partition, upperbound is empty string
        ObString upperbound_trim;
        if (OB_FAIL(upperbound_trims.push_back(upperbound_trim))) {
          LOG_WARN("failed to push back upperbound_trim", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObITableMetaHandler::format_tablet_ids(ObIAllocator &allocator,
                                           const ObIArray<ObTabletID>& tablet_ids,
                                           int start_idx,
                                           int count,
                                           char *&result)
{
  int ret = OB_SUCCESS;

  result = nullptr;
  if (start_idx < 0
      || count <= 0
      || start_idx >= tablet_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("format_tablet_ids fail, invalid argument", K(tablet_ids), K(start_idx), K(count));
  } else {
    size_t total_len = 0;
    int64_t end_idx = min(start_idx + count, tablet_ids.count());
    for (size_t i = start_idx; i < end_idx; ++i) {
      if (i > start_idx) total_len += 1;
      total_len += snprintf(nullptr, 0, "%ld", tablet_ids.at(i).id());
    }
    total_len += 1;
    char* buffer = (char*)allocator.alloc(total_len);
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      int pos = 0;
      for (size_t i = start_idx; i < end_idx; ++i) {
        if (i > start_idx) {
          pos += snprintf(buffer + pos, total_len - pos, ",");
        }
        pos += snprintf(buffer + pos, total_len - pos, "%ld", tablet_ids.at(i).id());
      }
      buffer[total_len - 1] = '\0';
      result = buffer;
    }
  }
  return ret;
}

// ================================ ObHTableRegionLocatorHandler ================================

ObHTableRegionLocatorHandler::~ObHTableRegionLocatorHandler()
{
  for (int i = 0; i < tablet_infos_.count(); ++i) {
    TabletInfo* info = tablet_infos_.at(i);
    for (int j = 0; j < info->replicas_.count(); ++j) {
      OB_DELETEx(TabletLocation, &allocator_, info->replicas_.at(j));
    }
    OB_DELETEx(TabletInfo, &allocator_, info);
  }
}

int ObHTableRegionLocatorHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tablet_boundary(ctx))) {
    LOG_WARN("failed to get tablet boundary", K(ret));
  } else if (OB_FAIL(get_tablet_location(ctx))) {
    LOG_WARN("failed to get tablet location", K(ret));
  } else if (OB_FAIL(try_compress(response))) {
    LOG_WARN("failed to compress table meta response", K(ret));
  }
  return ret;
}

int ObHTableRegionLocatorHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, htable_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

int ObHTableRegionLocatorHandler::try_compress(ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "LcCompAlc"));

  ObSEArray<int64_t, 3> table_id_dict;
  table_id_dict.set_attr(ObMemAttr(MTL_ID(), "TbTableIdDict"));
  hash::ObHashMap<int64_t, int> table_id_map;
  table_id_map.create(3, ObMemAttr(MTL_ID(), "TbTableIdMap"));
  ObSEArray<ObString, 32> replica_dict;
  replica_dict.set_attr(ObMemAttr(MTL_ID(), "TbReplicaDict"));
  hash::ObHashMap<ObString, int> replica_map;
  replica_map.create(32, ObMemAttr(MTL_ID(), "TbReplicaMap"));

  ObTableJsonArrayBuilder partitions_builder(tmp_allocator);
  ObTableJsonArrayBuilder table_id_dict_builder(tmp_allocator);
  ObTableJsonArrayBuilder replica_dict_builder(tmp_allocator);
  json::Value *root = nullptr;

  /* example:
  {
    "table_id_dict": [1001, 1002],
    "replica_dict": [
      ["127.0.0.1", 2881],
      ["127.0.0.2", 2882],
      ["127.0.0.3", 2883]
    ],
    "partitions": [
      // 表1001的3个分区，每个分区3副本
      [0, 50001, "rowkey_1", 0, 1], // leader
      [0, 50001, "rowkey_1", 1, 0], // follower
      [0, 50001, "rowkey_1", 2, 0], // follower
      [0, 50002, "rowkey_2", 0, 1],
      [0, 50002, "rowkey_2", 1, 0],
      [0, 50002, "rowkey_2", 2, 0],
      [0, 50003, "rowkey_3", 0, 1],
      [0, 50003, "rowkey_3", 1, 0],
      [0, 50003, "rowkey_3", 2, 0],

      // 表1002的3个分区，每个分区3副本
      [1, 50004, "rowkey_1", 0, 1],
      [1, 50004, "rowkey_1", 1, 0],
      [1, 50004, "rowkey_1", 2, 0],
      [1, 50005, "rowkey_2", 0, 1],
      [1, 50005, "rowkey_2", 1, 0],
      [1, 50005, "rowkey_2", 2, 0],
      [1, 50006, "rowkey_3", 0, 1],
      [1, 50006, "rowkey_3", 1, 0],
      [1, 50006, "rowkey_3", 2, 0]
    ]
  }
  */
  if (OB_FAIL(partitions_builder.init())) {
    LOG_WARN("failed to init partitions builder", K(ret));
  } else if (OB_FAIL(table_id_dict_builder.init())) {
    LOG_WARN("failed to init table id dict builder", K(ret));
  } else if (OB_FAIL(replica_dict_builder.init())) {
    LOG_WARN("failed to init replica dict builder", K(ret));
  } else if (OB_FAIL(build_table_and_replica_dicts(tmp_allocator, table_id_dict, table_id_map, replica_dict, replica_map))) {
    LOG_WARN("failed to build table and replica dicts", K(ret));
  } else if (OB_FAIL(build_partitions_array(tmp_allocator, table_id_map, replica_map, partitions_builder))) {
    LOG_WARN("failed to build partitions array", K(ret));
  } else if (OB_FAIL(build_table_id_dict_array(tmp_allocator, table_id_dict, table_id_dict_builder))) {
    LOG_WARN("failed to build table id dict array", K(ret));
  } else if (OB_FAIL(build_replica_dict_array(tmp_allocator, replica_dict, replica_dict_builder))) {
    LOG_WARN("failed to build replica dict array", K(ret));
  } else if (OB_FAIL(build_root_object(tmp_allocator, table_id_dict_builder, replica_dict_builder, partitions_builder, root))) {
    LOG_WARN("failed to build root object", K(ret));
  } else if (OB_FAIL(ObTableJsonUtils::serialize(allocator_, root, response.data_))) {
    LOG_WARN("failed to serialize root to response", K(ret));
  }
  return ret;
}

int ObHTableRegionLocatorHandler::build_table_and_replica_dicts(ObIAllocator &allocator,
                                    ObIArray<int64_t> &table_id_dict,
                                    hash::ObHashMap<int64_t, int> &table_id_map,
                                    ObIArray<ObString> &replica_dict,
                                    hash::ObHashMap<ObString, int> &replica_map)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
    TabletInfo *tinfo = tablet_infos_[i];
    int idx = 0;
    if (OB_FAIL(table_id_map.get_refactored(tinfo->table_id_, idx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        idx = table_id_dict.count();
        if (OB_FAIL(table_id_dict.push_back(tinfo->table_id_))) {
          LOG_WARN("failed to push back table id", K(ret));
        } else if (OB_FAIL(table_id_map.set_refactored(tinfo->table_id_, idx))) {
          LOG_WARN("failed to set table id map", K(ret));
        }
      } else {
        LOG_WARN("failed to get table id map", K(ret));
      }
    }
    for (int j = 0; OB_SUCC(ret) && j < tinfo->replicas_.count(); ++j) {
      TabletLocation *r = tinfo->replicas_[j];
      ObSqlString ip_port;
      ip_port.append_fmt("%.*s:%ld", r->svr_ip_.length(), r->svr_ip_.ptr(), r->svr_port_);
      char *mem = nullptr;
      if (OB_ISNULL(mem = static_cast<char *>(allocator.alloc(ip_port.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        MEMCPY(mem, ip_port.ptr(), ip_port.length());
        ObString ip_port_str(ip_port.length(), ip_port.length(), mem);
        int ridx = 0;
        if (OB_FAIL(replica_map.get_refactored(ip_port_str, ridx))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            ridx = replica_dict.count();
            if (OB_FAIL(replica_dict.push_back(ip_port_str))) {
              LOG_WARN("failed to push back replica dict", K(ret));
            } else if (OB_FAIL(replica_map.set_refactored(ip_port_str, ridx))) {
              LOG_WARN("failed to set replica map", K(ret), K(ip_port_str));
            }
          } else {
            LOG_WARN("failed to get replica map", K(ret), K(ip_port_str));
          }
        }
      }
    }
  }
  return ret;
}

int ObHTableRegionLocatorHandler::build_partitions_array(ObIAllocator &allocator,
                            hash::ObHashMap<int64_t, int> &table_id_map,
                            hash::ObHashMap<ObString, int> &replica_map,
                            ObTableJsonArrayBuilder &partitions_builder)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < tablet_infos_.count(); ++i) {
    TabletInfo *tinfo = tablet_infos_[i];
    int table_id_idx = 0;
    if (OB_FAIL(table_id_map.get_refactored(tinfo->table_id_, table_id_idx))) {
      LOG_WARN("failed to get table id map", K(ret));
    } else {
      for (int j = 0; OB_SUCC(ret) && j < tinfo->replicas_.count(); ++j) {
        TabletLocation *r = tinfo->replicas_[j];
        ObSqlString ip_port;
        ip_port.append_fmt("%.*s:%ld", r->svr_ip_.length(), r->svr_ip_.ptr(), r->svr_port_);
        int replica_idx = 0;
        if (OB_FAIL(replica_map.get_refactored(ip_port.string(), replica_idx))) {
          LOG_WARN("failed to get replica map", K(ret), K(ip_port.string()));
        } else {
          ObTableJsonArrayBuilder row_builder(allocator);
          if (OB_FAIL(row_builder.init())) {
            LOG_WARN("failed to init row builder", K(ret));
          } else if (OB_FAIL(row_builder.add(table_id_idx))) {
            LOG_WARN("failed to add table id idx", K(ret));
          } else if (OB_FAIL(row_builder.add(tinfo->tablet_id_))) {
            LOG_WARN("failed to add tablet id", K(ret));
          } else if (OB_FAIL(row_builder.add(tinfo->upperbound_))) {
            LOG_WARN("failed to add upperbound", K(ret));
          } else if (OB_FAIL(row_builder.add(replica_idx))) {
            LOG_WARN("failed to add replica idx", K(ret));
          } else if (OB_FAIL(row_builder.add(r->role_))) {
            LOG_WARN("failed to add role", K(ret));
          } else if (OB_FAIL(partitions_builder.add(row_builder))) {
            LOG_WARN("failed to add row builder", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObHTableRegionLocatorHandler::build_table_id_dict_array(ObIAllocator &allocator,
                               ObIArray<int64_t> &table_id_dict,
                               ObTableJsonArrayBuilder &table_id_dict_builder)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < table_id_dict.count(); ++i) {
    if (OB_FAIL(table_id_dict_builder.add(table_id_dict.at(i)))) {
      LOG_WARN("failed to add table id", K(ret), K(table_id_dict.at(i)));
    }
  }
  return ret;
}

int ObHTableRegionLocatorHandler::build_replica_dict_array(ObIAllocator &allocator,
                              ObIArray<ObString> &replica_dict,
                              ObTableJsonArrayBuilder &replica_dict_builder)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < replica_dict.count(); ++i) {
    ObString &dict_str = replica_dict.at(i);
    int port = std::strtol(dict_str.after(':').ptr(), nullptr, 10);
    ObTableJsonArrayBuilder row_builder(allocator);
    if (OB_FAIL(row_builder.init())) {
      LOG_WARN("failed to init row builder", K(ret));
    } else if (OB_FAIL(row_builder.add(dict_str.split_on(':')))) {
      LOG_WARN("failed to add ip", K(ret));
    } else if (OB_FAIL(row_builder.add(port))) {
      LOG_WARN("failed to add port", K(ret));
    } else if (OB_FAIL(replica_dict_builder.add(row_builder))) {
      LOG_WARN("failed to add row builder", K(ret));
    }
  }
  return ret;
}

int ObHTableRegionLocatorHandler::build_root_object(ObIAllocator &allocator,
                                                    ObTableJsonArrayBuilder &table_id_dict_builder,
                                                    ObTableJsonArrayBuilder &replica_dict_builder,
                                                    ObTableJsonArrayBuilder &partitions_builder,
                                                    json::Value *&root)
{
  int ret = OB_SUCCESS;
  root = nullptr;
  ObTableJsonObjectBuilder root_builder(allocator);
  if (OB_FAIL(root_builder.init())) {
    LOG_WARN("failed to init root_builder", K(ret));
  } else if (OB_FAIL(root_builder.add("table_id_dict", strlen("table_id_dict"), table_id_dict_builder.build()))) {
    LOG_WARN("failed to add table id dict", K(ret));
  } else if (OB_FAIL(root_builder.add("replica_dict", strlen("replica_dict"), replica_dict_builder.build()))) {
    LOG_WARN("failed to add replica dict", K(ret));
  } else if (OB_FAIL(root_builder.add("partitions", strlen("partitions"), partitions_builder.build()))) {
    LOG_WARN("failed to add partitions", K(ret));
  } else if (OB_ISNULL(root = root_builder.build())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to build root", K(ret));
  }
  return ret;
}


int ObHTableRegionLocatorHandler::get_tablet_boundary(ObTableExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  const ObSimpleDatabaseSchema *db_schema = nullptr;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(credential));
  } else if (OB_FAIL(ObTableQueryUtils::get_table_schemas(schema_guard,
                                                          htable_name_,
                                                          true,
                                                          credential.tenant_id_,
                                                          credential.database_id_,
                                                          table_schemas_))) {
    LOG_WARN("failed to get table schemas", K(ret));
  } else {
    // sort table_schemas by table_name to keep the order so that region_metrics and region_locator obtain the same table
    lib::ob_sort(table_schemas_.begin(), table_schemas_.end(), [](const ObSimpleTableSchemaV2* lhs,
                                                                const ObSimpleTableSchemaV2* rhs) {
    return lhs->get_table_name() < rhs->get_table_name();
    });
    ObSqlString sql;
    ObSEArray<ObTabletID, 16> tablet_ids;
    tablet_ids.set_attr(ObMemAttr(MTL_ID(), "TbTabletIds"));
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
      tablet_ids.reset();
      if (OB_FAIL(table_schemas_[i]->get_tablet_ids(tablet_ids))) {
        LOG_WARN("failed to get tablet ids", K(ret));
      } else {
        lib::ob_sort(tablet_ids.begin(), tablet_ids.end(), [](const ObTabletID &lhs,
                  const ObTabletID &rhs) {
          return lhs.id() < rhs.id();
        });
        ObSEArray<ObString, 16> upperbound_trims;
        upperbound_trims.set_attr(ObMemAttr(MTL_ID(), "TbUpperbound"));
        TabletInfo *tablet_info = nullptr;
        int64_t table_id = table_schemas_[i]->get_table_id();
        if (OB_FAIL(upperbound_trims.reserve(tablet_ids.count()))) {
          LOG_WARN("failed to reserver for upperbound_trims", K(ret));
        } else if (OB_FAIL(get_tablet_boundary_internal(allocator_, 
                                                        *table_schemas_.at(i), 
                                                        tablet_ids, 
                                                        upperbound_trims))) {
          LOG_WARN("failed to get upperbound", K(ret));
        } else if (tablet_ids.count() != upperbound_trims.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet ids number does not equal to uppperbound number", K(ret), K(tablet_ids.count()), K(upperbound_trims.count()));
        } else {
          for (int j = 0; OB_SUCC(ret) && j < upperbound_trims.count(); ++j) {
            if (OB_ISNULL(tablet_info = OB_NEWx(TabletInfo, &allocator_, table_id, tablet_ids.at(j).id(), upperbound_trims.at(j)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory", K(ret));
            } else if (OB_FAIL(tablet_infos_.push_back(tablet_info))) {
              OB_DELETEx(TabletInfo, &allocator_, tablet_info);
              LOG_WARN("failed to push back tablet info", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHTableRegionLocatorHandler::get_tablet_location(ObTableExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  const ObSimpleDatabaseSchema *db_schema = nullptr;
  ObSqlString sql;
  ObString tenant_name;
  ObString database_name;
  const ObTenantSchema *tenant_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(schema_guard.get_tenant_info(credential.tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(credential.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(credential.tenant_id_, credential.database_id_, database_schema))) {
    LOG_WARN("failed to get database schema", K(ret), K(credential.tenant_id_), K(credential.database_id_));
  } else {
    tenant_name = tenant_schema->get_tenant_name();
    database_name = database_schema->get_database_name();
  }
  ObSEArray<ObTabletID, 16> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "TbTabletIds"));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
    tablet_ids.reset();
    if (OB_FAIL(table_schemas_[i]->get_tablet_ids(tablet_ids))) {
      LOG_WARN("failed to get tablet ids", K(ret), K(table_schemas_[i]->get_table_name()));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); j += BATCH_SIZE) {
        char *tablet_ids_str = nullptr;
        if (OB_FAIL(format_tablet_ids(allocator_, tablet_ids, j, BATCH_SIZE, tablet_ids_str))) {
            LOG_WARN("failed to format tablet ids", K(ret), K(tablet_ids), K(j), K(BATCH_SIZE));
        } else if (OB_ISNULL(tablet_ids_str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to format tablet ids", K(ret));
        } else if (OB_FAIL(sql.assign_fmt(TABLET_LOCATION_SQL,
                          strlen(tablet_ids_str),
                          tablet_ids_str,
                          tenant_name.ptr(),
                          database_name.ptr(),
                          table_schemas_[i]->get_table_name()))) {
          LOG_WARN("failed to assign format", K(tablet_ids_str), K(tenant_name), K(database_name), K(table_schemas_[i]->get_table_name_str()));
        } else {
          SMART_VAR(common::ObISQLClient::ReadResult, res) {
            ObMySQLResult *result = nullptr;
            if (OB_FAIL(ObTableSqlUtils::read(OB_SYS_TENANT_ID, sql.ptr(), res))) {
              LOG_WARN("failed to execute sql", K(ret), K(sql.string()));
            } else if (OB_ISNULL(result = res.get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("result is null", K(ret), K(sql.string()));
            } else {
              int64_t tablet_id = 0;
              ObString svr_ip;
              int64_t svr_port = 0;
              int64_t role = 0;
              while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                svr_ip.reset();
                svr_port = 0;
                role = 0;
                EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", tablet_id, int64_t);
                EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
                EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
                EXTRACT_INT_FIELD_MYSQL(*result, "role", role, int64_t);
                ObString svr_ip_deep_copy;
                TabletLocation *location = nullptr;
                if (OB_FAIL(ret)) {
                } else if (tablet_id == 0 || svr_ip.empty() || svr_port == 0 || role == 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid tablet info", K(ret), K(tablet_id), K(svr_ip), K(svr_port), K(role));
                } else if (OB_FAIL(ob_write_string(allocator_, svr_ip, svr_ip_deep_copy))) {
                  LOG_WARN("failed to write svr ip", K(ret));
                } else {
                  int64_t index = OB_INVALID_ID;
                  if (OB_FAIL(binary_search_tablet_info_idx(tablet_id, index))) {
                    LOG_WARN("failed to binary search tablet info", K(ret), K(index), K(tablet_id));
                  } else if (OB_ISNULL(location = OB_NEWx(TabletLocation, &allocator_, svr_ip_deep_copy, svr_port, role))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("failed to allocate memory", K(ret));
                  } else if (OB_FAIL(tablet_infos_[index]->replicas_.push_back(location))) {
                    OB_DELETEx(TabletLocation, &allocator_, location);
                    LOG_WARN("failed to push back tablet location", K(ret));
                  }
                }
              }
              if (ret == OB_ITER_END) {
                ret = OB_SUCCESS;
              }
            }
          }
        }
        if (OB_NOT_NULL(tablet_ids_str)) {
          allocator_.free(tablet_ids_str);
        }
      }
    }
  }

  return ret;
}

int ObHTableRegionLocatorHandler::binary_search_tablet_info_idx(const int64_t tablet_id, int64_t &index)
{
  int ret = OB_SUCCESS;
  int64_t start = 0, end = tablet_infos_.count() -1;
  while (start < end) {
    int mid = (end - start) / 2 + start;
    if (tablet_infos_[mid]->tablet_id_ == tablet_id) {
      index = mid;
      break;
    } else if (tablet_infos_[mid]->tablet_id_ < tablet_id) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }
  if (start == end) {
    index = start;
  }
  if (index < 0 || index >= tablet_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index overflow", K(ret), K(index), K(tablet_infos_.count()));
  } else if (tablet_infos_[index]->tablet_id_ != tablet_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find tablet in tablet infos", K(ret), K(index), K(tablet_id));
  }
  return ret;
}

// ================================ ObHTableCreateHandler ================================
ObHTableCreateHandler::~ObHTableCreateHandler()
{
  for (int i = 0; i < column_families_.count(); ++i) {
    OB_DELETEx(ColumnFamilyDescriptor, &allocator_, column_families_.at(i));
  }
}


int ObHTableCreateHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  // htable_name_, database_name_, family_names_, ttl_, max_version_ to json string
  ObString tablegroup_sql;
  ObSEArray<ObString, 3> column_family_sql_array;
  const ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(credential));
  } else if (OB_FAIL(schema_guard.get_database_schema(credential.tenant_id_, credential.database_id_, database_schema))) {
    LOG_WARN("failed to get database schema", K(ret), K(credential.tenant_id_), K(credential.database_id_));
  } else if (OB_FAIL(get_create_table_sql(tablegroup_sql, column_family_sql_array))) {
    LOG_WARN("failed to get create table sql", K(ret));
  } else if (OB_FAIL(ObTableSqlUtils::create_table(allocator_,
                                                   schema_guard,
                                                   ctx.get_sess_guard().get_sess_info(),
                                                   ctx.get_timeout_ts(),
                                                   database_schema->get_database_name(),
                                                   tablegroup_sql,
                                                   column_family_sql_array))) {
    LOG_WARN("failed to create table", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, "success", response.data_))) {
    LOG_WARN("failed to write response data", K(ret));
  }
  return ret;
}


/*
  example:
  {
    "htable_name": "test",
    "column_families": {
      "cf1": {"ttl": 10, "max_version": 10},
      "cf2": {"ttl": 100, "max_version": 100}
    }
  }
*/
int ObHTableCreateHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  allocator.set_attr(ObMemAttr(MTL_ID(), "CrtParseAlc"));
  json::Value *root = nullptr;
  if (OB_FAIL(ObTableJsonUtils::parse(allocator, request.data_, root))) {
    LOG_WARN("failed to parse request data", K(ret));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root is null", K(ret));
  } else if (root->get_type() != json::JT_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("request data is not a json object", K(ret));
  } else { // get all the fields
    json::Value *htable_name_value = nullptr;
    if (OB_FAIL(ObTableJsonUtils::get_json_value(root,
                                                "htable_name",
                                                json::JT_STRING,
                                                htable_name_value))) {
      LOG_WARN("failed to get htable name", K(ret));
    } else if (OB_ISNULL(htable_name_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("htable name is null", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, htable_name_value->get_string(), htable_name_))) {
      LOG_WARN("failed to write htable name", K(ret), K(htable_name_value->get_string()));
    }
    json::Value *column_family_descriptors_value = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTableJsonUtils::get_json_value(root,
                                                        "column_families",
                                                        json::JT_OBJECT,
                                                        column_family_descriptors_value))) {
      LOG_WARN("failed to get column family descriptors", K(ret));
    } else if (OB_ISNULL(column_family_descriptors_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column family descriptors is null", K(ret));
    } else {
      partition_type_ = PARTITION_TYPE_NONE;
      json::Pair *pair_iter = column_family_descriptors_value->get_object().get_first();
      int size = column_family_descriptors_value->get_object().get_size();
      for (int i = 0; OB_SUCC(ret) && i < size; ++i, pair_iter = pair_iter->get_next()) {
        ObString family_name;
        int64_t ttl = 0;
        int64_t max_version = 0;
        json::Value *ttl_value = nullptr;
        json::Value *max_version_value = nullptr;
        if (OB_ISNULL(pair_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pair iter is null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator_, pair_iter->name_, family_name))) {
          LOG_WARN("failed to write family name", K(ret), K(pair_iter->name_));
        } else if (OB_ISNULL(pair_iter->value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pair value is null", K(ret));
        } else if (OB_FAIL(ObTableJsonUtils::get_json_value(pair_iter->value_,
                                                            "ttl",
                                                            json::JT_NUMBER,
                                                            ttl_value))) {
          LOG_WARN("failed to get ttl", K(ret), K(pair_iter->name_));
        } else if (OB_FAIL(ObTableJsonUtils::get_json_value(pair_iter->value_,
                                                            "max_version",
                                                            json::JT_NUMBER,
                                                            max_version_value))) {
          LOG_WARN("failed to get max version", K(ret), K(pair_iter->name_));
        } else {
          if (OB_ISNULL(ttl_value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ttl is null", K(ret));
          } else if (OB_ISNULL(max_version_value)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("max version is null", K(ret));
          } else {
            ttl = ttl_value->get_number();
            max_version = max_version_value->get_number();
            ColumnFamilyDescriptor *descriptor = nullptr;
            if (OB_ISNULL(descriptor = OB_NEWx(ColumnFamilyDescriptor,
                                              &allocator_,
                                              family_name,
                                              ttl,
                                              max_version))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory", K(ret), K(family_name));
            } else if (OB_FAIL(column_families_.push_back(descriptor))) {
              OB_DELETEx(ColumnFamilyDescriptor, &allocator_, descriptor);
              LOG_WARN("failed to push back column family descriptor", K(ret), K(family_name));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHTableCreateHandler::build_create_table_sql(ObIAllocator &allocator,
                                                  const ObString &htable_name,
                                                  const ObString &family_name,
                                                  const ObString &tablegroup,
                                                  const ObString &partition,
                                                  const ObString &kv_attributes,
                                                  ObString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = 4096;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    int pos = snprintf(buf, buf_size, TABLE_DEF, htable_name.length(), htable_name.ptr(), family_name.length(), family_name.ptr());
    ObSqlString sql_str;
    if (OB_FAIL(sql_str.assign_fmt("%.*s", pos, buf))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(sql_str.append_fmt(" %.*s", tablegroup.length(), tablegroup.ptr()))) {
      LOG_WARN("failed to append tablegroup", K(ret));
    } else if (OB_FAIL(sql_str.append_fmt(" %.*s", partition.length(), partition.ptr()))) {
      LOG_WARN("failed to append partition", K(ret));
    } else if (OB_FAIL(sql_str.append_fmt(" %.*s", kv_attributes.length(), kv_attributes.ptr()))) {
      LOG_WARN("failed to append kv attributes", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, sql_str.string(), sql))) {
      LOG_WARN("failed to write sql", K(ret));
    }
  }
  return ret;
}

int ObHTableCreateHandler::build_range_partition_sql(ObIAllocator &allocator,
                                                    const ObString &column, /* default: K */
                                                    const ObIArray<ObString> &upperbounds,
                                                    ObString &sql)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = 4096;
  char *buf = nullptr;
  ObSqlString sql_str;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(sql_str.assign_fmt("PARTITION BY RANGE COLUMNS(%.*s) (", column.length(), column.ptr()))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < upperbounds.count(); ++i) {
      if (OB_FAIL(sql_str.append_fmt("PARTITION p%ld VALUES LESS THAN ('%.*s'),", i + 1, upperbounds.at(i).length(), upperbounds.at(i).ptr()))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_str.append_fmt("PARTITION p%ld VALUES LESS THAN (MAXVALUE))", upperbounds.count() + 1))) {
      LOG_WARN("failed to append sql", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, sql_str.string(), sql))) {
      LOG_WARN("failed to write sql", K(ret));
    }
  }
  return ret;
}

int ObHTableCreateHandler::get_create_table_sql(ObString &tablegroup_sql_str, ObIArray<ObString> &column_family_sql_array)
{
  int ret = OB_SUCCESS;
  ObSqlString create_tg_sql;
  ObSqlString partition_sql;
  ObSqlString tablegroup_sql;
  ObSqlString kv_attributes_sql;
  if (OB_FAIL(create_tg_sql.assign_fmt(CREATE_TABLEGROUP_SQL, htable_name_.length(), htable_name_.ptr()))) {
    LOG_WARN("failed to assign tablegroup sql", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, create_tg_sql.string(), tablegroup_sql_str))) {
    LOG_WARN("failed to write tablegroup sql", K(ret));
  } else if (OB_FAIL(tablegroup_sql.assign_fmt(TABLE_GROUP_TEMPLATE, htable_name_.length(), htable_name_.ptr()))) {
    LOG_WARN("failed to assign tablegroup sql", K(ret));
  } else {
    switch (partition_type_) {
      case PARTITION_TYPE_NONE:
        if (OB_FAIL(partition_sql.assign(NO_PARTITION))) {
          LOG_WARN("failed to assign partition sql", K(ret));
        }
        break;
      case PARTITION_TYPE_KEY:
      case PARTITION_TYPE_RANGE:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported partition type yet", K(ret), K_(partition_type));
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid partition type", K(ret), K(partition_type_));
        break;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_families_.count(); ++i) {
    ColumnFamilyDescriptor *descriptor = column_families_[i];
    ObString cf_sql;
    if (descriptor->has_ttl() && descriptor->has_max_version()) {
      if (OB_FAIL(kv_attributes_sql.assign_fmt(KV_TTL_AND_MAX_VERSIONS_TEMPLATE, descriptor->ttl_, descriptor->max_version_))) {
        LOG_WARN("failed to assign kv attributes sql", K(ret), K(htable_name_), K(descriptor->name_));
      }
    } else if (descriptor->has_ttl()) {
      if (OB_FAIL(kv_attributes_sql.assign_fmt(KV_TTL_TEMPLATE, descriptor->ttl_))) {
        LOG_WARN("failed to assign kv attributes sql", K(ret), K(htable_name_), K(descriptor->name_));
      }
    } else if (descriptor->has_max_version()) {
      if (OB_FAIL(kv_attributes_sql.assign_fmt(KV_MAX_VERSIONS_TEMPLATE, descriptor->max_version_))) {
        LOG_WARN("failed to assign kv attributes sql", K(ret), K(htable_name_), K(descriptor->name_));
      }
    } else {
      if (OB_FAIL(kv_attributes_sql.assign(KV_EMPTY_TEMPLATE))) {
        LOG_WARN("failed to assign kv attributes sql", K(ret), K(htable_name_), K(descriptor->name_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_create_table_sql(allocator_,
                                              htable_name_, // hbase table name
                                              descriptor->name_, // column family name
                                              tablegroup_sql.string(), // tablegroup name
                                              partition_sql.string(), // partition info
                                              kv_attributes_sql.string(), // kv attributes
                                              cf_sql))) {
      LOG_WARN("failed to build create table sql", K(ret), K(htable_name_), K(descriptor->name_));
    } else if (OB_FAIL(column_family_sql_array.push_back(cf_sql))) {
      LOG_WARN("failed to push back column family sql", K(ret), K(cf_sql));
    }
  }
  return ret;
}

// ================================ ObHTableRegionMetricsHandler ================================
int ObHTableRegionMetricsHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSimpleTableSchemaV2*, 3> table_schemas;
  table_schemas.set_attr(ObMemAttr(MTL_ID(), "TbTableSchemas"));
  const ObString &tablegroup_name = htable_name_;
  ctx.set_table_name(htable_name_);
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  if (tablegroup_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table name in info is empty", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(credential.tenant_id_));
  } else if (OB_FAIL(ObTableQueryUtils::get_table_schemas(schema_guard,
                                                          tablegroup_name,
                                                          true,
                                                          credential.tenant_id_,
                                                          credential.database_id_,
                                                          table_schemas))) {
    LOG_WARN("failed to get table schemas", K(ret), K(tablegroup_name), K(credential.tenant_id_));
  } else if (table_schemas.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no table under tablegroup", K(ret), K(tablegroup_name), K(credential.tenant_id_));
  } else {
    // sort table_schemas by table_name to keep the order so that region_metrics and region_locator obtain the same table
    lib::ob_sort(table_schemas.begin(), table_schemas.end(), [](const ObSimpleTableSchemaV2* lhs,
                                                                const ObSimpleTableSchemaV2* rhs) {
      return lhs->get_table_name() < rhs->get_table_name();
    });
    SMART_VAR(ObTableRegionMetricsResult, table_metrics_result, allocator_) {
      if (OB_FAIL(acquire_table_metric(ctx, table_schemas, table_metrics_result))) {
        LOG_WARN("failed to acqurie table metrics", K(ret));
      } else if (OB_FAIL(construct_response(table_metrics_result, response))) {
        LOG_WARN("failed to construct response", K(ret));
      }
    }
  }
  response.set_err(ret);

  return ret;
}

int ObHTableRegionMetricsHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, htable_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

int ObHTableRegionMetricsHandler::check_metrics_legality(const ObTableRegionMetricsResult &table_metrics) 
{
  int ret = OB_SUCCESS;
  if (table_metrics.table_id_ == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table id does not inited", K(ret));
  } else if (table_metrics.tablet_ids_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ids is empty", K(ret));
  } else if (table_metrics.tablet_ids_.count() != table_metrics.mem_tablet_sizes_.count()
             || table_metrics.mem_tablet_sizes_.count() != table_metrics.ss_tablet_sizes_.count()
             || table_metrics.ss_tablet_sizes_.count() != table_metrics.boundarys_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet ids, memtable size, sstable size and boundary size are not the same", K(ret), K(table_metrics.table_id_), 
                                                                                           K(table_metrics.tablet_ids_.count()), K(table_metrics.mem_tablet_sizes_.count()),
                                                                                           K(table_metrics.ss_tablet_sizes_.count()), K(table_metrics.boundarys_.count()));
  }
  return ret;
}

int ObHTableRegionMetricsHandler::acquire_table_metric(ObTableExecCtx &ctx,
                                                       const ObIArray<const schema::ObSimpleTableSchemaV2*> &table_schemas,
                                                       ObTableRegionMetricsResult &table_metrics)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ctx.get_credential().tenant_id_;
  const int64_t table_num = table_schemas.count();
  ObSEArray<ObTabletID, 16> tablet_ids;
  tablet_ids.set_attr(ObMemAttr(MTL_ID(), "TbTabletIds"));
  const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(0);
  if (OB_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to get tablet ids", K(ret), K(table_schema->get_table_name()));
  } else {
    // sort tablet ids in acsending order to keep the same order of ORDER BY in sql
    lib::ob_sort(tablet_ids.begin(), tablet_ids.end(), [](const ObTabletID &lhs,
                                                          const ObTabletID &rhs) {
      return lhs.id() < rhs.id();
    });
    if (OB_FAIL(table_metrics.init(tablet_ids, table_schema->get_table_id()))) {
      LOG_WARN("failed to init table metrics", K(ret));
    } else {
      ObSEArray<ObString, 16> upperbound_trims;
      upperbound_trims.set_attr(ObMemAttr(MTL_ID(), "TbUpperbound"));
      if (OB_FAIL(upperbound_trims.reserve(tablet_ids.count()))) {
        LOG_WARN("failed to reserve for upperbound_trims", K(ret));
      } else if (OB_FAIL(get_tablet_boundary_internal(allocator_, *table_schema, tablet_ids, upperbound_trims))) {
        LOG_WARN("failed to get tablet boundary", K(ret), K(table_schema->get_table_name()));
      } else if (tablet_ids.count() != upperbound_trims.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet ids number does not equal to uppperbound number", K(ret), K(tablet_ids.count()), K(upperbound_trims.count()));
      } else {
        for (int j = 0; OB_SUCC(ret) && j < upperbound_trims.count(); ++j) {
          if (OB_FAIL(table_metrics.boundarys_.push_back(upperbound_trims.at(j)))) {
            LOG_WARN("failed to push back boundary", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(check_metrics_legality(table_metrics))) {
          LOG_WARN("table metrics is illegal", K(ret));
        }
      }
    }
  }

  return ret;
}

/* example :
  {
    "tableName": "tablegroup_name",
    "regions": [200051, 200052, 200053, ..., 200191, 200192, 200193, ...],
    "memTableSize":[123, 321, 321, ...,  123, 321, 321, ...],
    "ssTableSize":[5122, 4111, 5661, ..., 5122, 4111, 5661, ...],
    "boundary":["rowkey1", "rowkey2", "rowkey3", ..., "rowkey100", "rowkey101", "rowkey102", ...]
  }
*/
int ObHTableRegionMetricsHandler::construct_response(const ObTableRegionMetricsResult &table_metrics_result,
                                                     ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  tmp_allocator.set_attr(ObMemAttr(MTL_ID(), "LcCompAlc"));
  ObTableJsonObjectBuilder root_builder(tmp_allocator);
  json::Value *root = nullptr;
  if (OB_FAIL(root_builder.init())) {
    LOG_WARN("failed to init root builder", K(ret));
  } else {
    if (OB_FAIL(root_builder.add("tableName", strlen("tableName"), htable_name_))) {
      LOG_WARN("failed to add table name", K(ret));
    } else {
      ObTableJsonArrayBuilder regions_array_builder(tmp_allocator);
      ObTableJsonArrayBuilder mem_table_array_builder(tmp_allocator);
      ObTableJsonArrayBuilder ss_table_array_builder(tmp_allocator);
      ObTableJsonArrayBuilder boundary_array_builder(tmp_allocator);
      if (OB_FAIL(regions_array_builder.init())) {
        LOG_WARN("failed to init regions array builder", K(ret));
      } else if (OB_FAIL(mem_table_array_builder.init())) {
        LOG_WARN("failed to init memtable array builder", K(ret));
      } else if (OB_FAIL(ss_table_array_builder.init())) {
        LOG_WARN("failed to init sstable array builder", K(ret));
      } else if (OB_FAIL(boundary_array_builder.init())) {
        LOG_WARN("failed to init boudary array builder", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && i < table_metrics_result.tablet_ids_.count(); ++i) {
        if (OB_FAIL(regions_array_builder.add(table_metrics_result.tablet_ids_.at(i)))) {
          LOG_WARN("failed to add regions array builder", K(ret));
        } else if (OB_FAIL(mem_table_array_builder.add(table_metrics_result.mem_tablet_sizes_.at(i)))) {
          LOG_WARN("failed to add memtable array builder", K(ret));
        } else if (OB_FAIL(ss_table_array_builder.add(table_metrics_result.ss_tablet_sizes_.at(i)))) {
          LOG_WARN("failed to add sstable array builder", K(ret));
        } else if (OB_FAIL(boundary_array_builder.add(table_metrics_result.boundarys_.at(i)))) {
          LOG_WARN("failed to add boundary array builder", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(root_builder.add("regions", strlen("regions"), regions_array_builder.build()))) {
          LOG_WARN("failed to add regions array", K(ret));
        } else if (OB_FAIL(root_builder.add("memTableSize", strlen("memTableSize"), mem_table_array_builder.build()))) {
          LOG_WARN("failed to add memtable array", K(ret));
        } else if (OB_FAIL(root_builder.add("ssTableSize", strlen("ssTableSize"), ss_table_array_builder.build()))) {
          LOG_WARN("failed to add sstable array", K(ret));
        } else if (OB_FAIL(root_builder.add("boundary", strlen("boundary"), boundary_array_builder.build()))) {
          LOG_WARN("failed to add boundary array", K(ret));
        } else if (OB_ISNULL(root = root_builder.build())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to build root", K(ret));
        } else if (OB_FAIL(ObTableJsonUtils::serialize(allocator_, root, response.data_))) {
          LOG_WARN("failed to serialize root builder", K(ret));
        }
      }
    } 
  }
  return ret;
}

int ObHTableRegionMetricsHandler::ObTableRegionMetricsResult::init(const ObIArray<ObTabletID> &tablet_ids, uint64_t table_id) 
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  uint64_t tablet_num = tablet_ids.count();
  if (OB_FAIL(tablet_ids_.prepare_allocate(tablet_num))) {
    LOG_WARN("failed to reserve for tablet ids", K(ret), K(tablet_num), K(table_id));
  } else if (OB_FAIL(boundarys_.reserve(tablet_num))) {
    LOG_WARN("failed to reserve for boundarys", K(ret), K(tablet_num), K(table_id));
  } else if (OB_FAIL(ss_tablet_sizes_.prepare_allocate(tablet_num))) {
    LOG_WARN("failed to prepare allocate for sstable sizes", K(ret), K(tablet_num), K(table_id));
  } else if (OB_FAIL(mem_tablet_sizes_.prepare_allocate(tablet_num))) {
    LOG_WARN("failed to prepare allocate for memtable sizes", K(ret), K(tablet_num), K(table_id));
  } else {
    // init value for mem_tablet_sizes_
    // mock ss table value for ss_tablet_sizes_ because table __all_virtual_tablet_pointer_status is too slow for no primary key or index
    // TODO: need to calculate correct size for ss_tablet_sizes_
    for (int64_t i = 0; i < tablet_num; ++i) {
      tablet_ids_.at(i) = tablet_ids.at(i).id();
      mem_tablet_sizes_.at(i) = 0;
      ss_tablet_sizes_.at(i) = MOCK_SS_TABLET_SIZE; // 8 GB
    }
  }
  return ret;
}

// ================================ ObHTableDeleteHandler ================================
int ObHTableDeleteHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo &sess_info = ctx.get_sess_guard().get_sess_info();
  int64_t timeout = ctx.get_timeout_ts();
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  const ObDatabaseSchema *db_schema = nullptr;
  const ObString &tablegroup_name = htable_name_;
  ObString db_name = ObString::make_empty_string();
  if (tablegroup_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablegroup name is empty", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(credential.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_database_schema(credential.tenant_id_, credential.database_id_, db_schema))) {
    LOG_WARN("failed to database schema", K(ret), K(credential.tenant_id_), K(credential.database_id_));
  } else if (OB_ISNULL(db_schema) || !db_schema->is_valid()) {
    ret = OB_ERR_BAD_DATABASE;
    LOG_WARN("database schema is null or invalid", K(credential.tenant_id_), K(credential.database_id_), K(ret));
  } else if (db_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("database in recyclebin", K(ret), K(*db_schema));
  } else if (FALSE_IT(db_name = db_schema->get_database_name_str())) {
  } else if (OB_FAIL(ObTableSqlUtils::drop_table(allocator_,
                                                 schema_guard,
                                                 sess_info,
                                                 timeout,
                                                 db_name,
                                                 tablegroup_name))) {
    LOG_WARN("failed to drop table", K(ret), K(tablegroup_name));
  }
  response.set_err(ret);

  return ret;
}

// example: { "table_name":"test"}
int ObHTableDeleteHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, htable_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

// ================================ ObHTableExistsHandler ================================
int ObHTableExistsHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  const ObString &tablegroup_name = htable_name_;
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  uint64_t tablegroup_id = OB_INVALID_ID;
  const ObSimpleTablegroupSchema *tablegroup_schema = nullptr;
  if (tablegroup_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table name in info is empty", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(credential.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(credential.tenant_id_,
                                                    tablegroup_name,
                                                    tablegroup_id))) {
    LOG_WARN("failed to get table group ID", K(ret), K(credential.tenant_id_), K(tablegroup_name));
  } else if (tablegroup_id == OB_INVALID_ID) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    // log level does not have to be higher
    LOG_INFO("tablegroup not exist", K(ret), K(credential.tenant_id_), K(tablegroup_name));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schema(credential.tenant_id_,
                                                        tablegroup_id,
                                                        tablegroup_schema))) {
    LOG_WARN("failed to get tablegroup schema", K(ret), K(credential.tenant_id_), K(tablegroup_name));
  } else if (OB_ISNULL(tablegroup_schema) || !tablegroup_schema->is_valid()) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    // log level does not have to be higher
    LOG_INFO("tablegroup not exist", K(ret), K(credential.tenant_id_), K(tablegroup_name));
  }
  // get response
  if (OB_SUCC(ret) || ret == OB_KV_HBASE_TABLE_NOT_FOUND) {
    ObString res = OB_SUCC(ret) ? ObString::make_string("{\"exists\":true}") 
                                  : ObString::make_string("{\"exists\":false}");
    ret = OB_SUCCESS;
    if (OB_FAIL(ob_write_string(allocator_, res, response.data_, true))) {
      LOG_WARN("failed to write string", K(ret), K(res));
    }
  }
  response.set_err(ret);
  return ret;
}

// example: { "table_name":"test"}
int ObHTableExistsHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, htable_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

// ================================ ObHTableGetDescHandler ================================
ObHTableGetDescHandler::~ObHTableGetDescHandler()
{
  for (int i = 0; i < cf_descs_.count(); ++i) {
    OB_DELETEx(CfDesc, &allocator_, cf_descs_.at(i));
  }
  for (int i = 0; i < table_descs_.count(); ++i) {
    OB_DELETEx(TableDesc, &allocator_, table_descs_.at(i));
  }
}

int ObHTableGetDescHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, htable_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

int ObHTableGetDescHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  ObTableJsonObjectBuilder root_builder(allocator_);
  ObTableJsonObjectBuilder cf_descs_builder(allocator_);
  ObTableJsonObjectBuilder table_desc_builder(allocator_);
  if (OB_FAIL(root_builder.init())) {
    LOG_WARN("failed to init root builder", K(ret));
  } else if (OB_FAIL(cf_descs_builder.init())) {
    LOG_WARN("failed to init cf desc builder", K(ret));
  } else if (OB_FAIL(table_desc_builder.init())) {
    LOG_WARN("failed to init table desc builder", K(ret));
  } else if (OB_FAIL(get_cf_desc(ctx))) {
    LOG_WARN("failed to get cf desc", K(ret));
  } else if (OB_FAIL(get_table_desc(ctx))) {
    LOG_WARN("failed to get table desc", K(ret));
  } else {
    /* example:
    {
      "cfDesc": {
        "cf1": {
          "TTL":604800
          "maxVersions": 3
        },
        "cf2": {
          "TTL":259200
          "maxVersions": 2
        }
      },
      "tbDesc": {
        "name":"test"
        "state":"dsiable" ("enable")
      }
    }
    */
    bool is_htable_disable = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < cf_descs_.count(); ++i) {
      ObTableJsonObjectBuilder attr_builder(allocator_);
      if (OB_FAIL(attr_builder.init())) {
        LOG_WARN("failed to init attr builder", K(ret));
      } else if (OB_FAIL(attr_builder.add("TTL", strlen("TTL"), cf_descs_[i]->ttl_))) {
        LOG_WARN("failed to add TTL", K(ret), K_(cf_descs_[i]->ttl));
      } else if (OB_FAIL(attr_builder.add("maxVersions", strlen("maxVersions"), cf_descs_[i]->max_versions_))) {
        LOG_WARN("failed to add max version", K(ret), K_(cf_descs_[i]->max_versions));
      } else if (OB_FAIL(cf_descs_builder.add(cf_descs_[i]->cf_name_.ptr(), cf_descs_[i]->cf_name_.length(), attr_builder))) {
        LOG_WARN("failed to add cf desc", K(ret), K(cf_descs_[i]));
      }
      if (i == 0 && OB_SUCC(ret)) {
        is_htable_disable = cf_descs_[i]->is_disable_;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_desc_builder.add("name", strlen("name"), ctx.get_table_name()))) {
      LOG_WARN("failed to add table name", K(ret));
    } else if (OB_FAIL(table_desc_builder.add("state", strlen("state"), is_htable_disable ? "disable" : "enable"))) {
      LOG_WARN("failed to add table name", K(ret));
    } else if (OB_FAIL(root_builder.add("cfDescs", strlen("cfDescs"), cf_descs_builder))) {
      LOG_WARN("failed to add cf descs", K(ret));
    } else if (OB_FAIL(root_builder.add("tableDesc", strlen("tableDesc"), table_desc_builder))) {
      LOG_WARN("failed to add table desc", K(ret));
    } else if (OB_FAIL(ObTableJsonUtils::serialize(allocator_, root_builder.build(), response.data_))) {
      LOG_WARN("failed to serialize root builder", K(ret));
    }
  }
  return ret;
}

int ObHTableGetDescHandler::get_cf_desc(ObTableExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableSchema *, 3> table_schemas;
  table_schemas.set_attr(ObMemAttr(MTL_ID(), "TbTableSchemas"));
  const table::ObTableApiCredential &credential = ctx.get_credential();
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  uint64_t tablegroup_id = OB_INVALID_ID;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(credential.tenant_id_, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(credential.tenant_id_));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(credential.tenant_id_, htable_name_, tablegroup_id))) {
    LOG_WARN("Failed to get table group ID", K(ret), K(tablegroup_id), K_(htable_name));
  } else if (tablegroup_id == OB_INVALID_ID) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    LOG_WARN("Tablegroup does not exist", K(ret), K(credential.tenant_id_), K(htable_name_));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(credential.tenant_id_, tablegroup_id, table_schemas))) {
    LOG_WARN("Failed to get table schemas from table group", K(ret), K_(credential.tenant_id), K(tablegroup_id));
  } else {
    CfDesc *cf_desc = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas[i];
      cf_desc = nullptr;
      ObKVAttr kv_attr;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret));
      } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(table_schema->get_kv_attributes(), kv_attr))) {
        LOG_WARN("failed to parse kv attributes", K(ret), K(table_schema->get_kv_attributes()));
      } else {
        int64_t ttl = kv_attr.ttl_;
        int64_t max_versions = kv_attr.max_version_ == 0 ? INT32_MAX : kv_attr.max_version_;
        bool is_disable = kv_attr.is_disable_;
        ObString cf_name;
        ObString table_name = table_schema->get_table_name();
        OZ(ob_write_string(allocator_, table_name.after('$'), cf_name));
        if (OB_ISNULL(cf_desc = OB_NEWx(CfDesc, &allocator_, cf_name, ttl, max_versions, is_disable))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (OB_FAIL(cf_descs_.push_back(cf_desc))) {
          OB_DELETEx(CfDesc, &allocator_, cf_desc);
          LOG_WARN("failed to push back cf desc", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHTableGetDescHandler::get_table_desc(ObTableExecCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObString table_name = ctx.get_table_name();
  TableDesc *table_desc = nullptr;
  if (OB_ISNULL(table_desc = OB_NEWx(TableDesc, &allocator_, table_name))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(table_descs_.push_back(table_desc))) {
    OB_DELETEx(TableDesc, &allocator_, table_desc);
    LOG_WARN("failed to push back table desc", K(ret));
  }
  return ret;
}


/*
  data format is:
  {
    "name": "$table_group_name"
  }
*/
int ObHTableAccessControlHandler::parse(const ObTableMetaRequest &request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_htable_name(allocator_, request, table_group_name_))) {
    LOG_WARN("failed to parse htable name", K(ret), K(request.data_));
  }
  return ret;
}

int ObHTableDisableTableHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  // 1. Find all sub-tables by table_group_name_
  uint64_t table_group_id = OB_INVALID_ID;
  const uint64_t tenant_id = ctx.get_credential().tenant_id_;
  ObSEArray<const ObTableSchema *, 8> table_schemas;
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, table_group_name_, table_group_id))) {
    LOG_WARN("failed to get table group id", K(ret), K(tenant_id), K_(table_group_name));
  } else if (table_group_id == OB_INVALID_ID) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    LOG_WARN("table group is not exist", K(ret), K_(table_group_name));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id,
                                                                  table_group_id,
                                                                  table_schemas))) {
    LOG_WARN("failed to get table schemas", K(ret), K(tenant_id), K(table_group_id));
  } else if (!table_schemas.empty()) {
    // 2. check if sub-tables is disabled. if disabled，then return OB_KV_TABLE_NOT_DISABLED
    const ObTableSchema *schema = table_schemas.at(0);
    bool is_enabled = true;
    ObKVAttr kv_attr;
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table schemas", K(ret), K(tenant_id), K(table_group_id));
    } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(schema->get_kv_attributes(), kv_attr))) {
      LOG_WARN("failed get table state", K(ret), K(schema->get_kv_attributes()));
    } else if (kv_attr.is_disable_) {
      // table is already disabled, error
      ret = OB_KV_TABLE_NOT_ENABLED;
      LOG_WARN("table is not enabled, can't disable", K(ret));
      LOG_USER_ERROR(OB_KV_TABLE_NOT_ENABLED, table_group_name_.length(), table_group_name_.ptr());
    } else {
      // table is enabled, disable table
      ObTableApiSessGuard &session_guard = ctx.get_sess_guard();
      if (OB_FAIL(ObTableSqlUtils::disable_table(allocator_,
                                                 schema_guard,
                                                 session_guard.get_sess_info(),
                                                 ctx.get_timeout_ts(),
                                                 session_guard.get_database_name(),
                                                 table_group_name_))) {
        LOG_WARN("failed to disable table", K(ret), K_(table_group_name));
      }
    }
  }

  return ret;
}

int ObHTableEnableTableHandler::handle(ObTableExecCtx &ctx, ObTableMetaResponse &response)
{
  int ret = OB_SUCCESS;
  uint64_t table_group_id = OB_INVALID_ID;
  const uint64_t tenant_id = ctx.get_credential().tenant_id_;
  ObSEArray<const ObTableSchema *, 8> table_schemas;
  ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_id(tenant_id, table_group_name_, table_group_id))) {
    LOG_WARN("failed to get table group id", K(ret), K(tenant_id), K_(table_group_name));
  } else if (table_group_id == OB_INVALID_ID) {
    ret = OB_KV_HBASE_TABLE_NOT_FOUND;
    LOG_WARN("table group is not exist", K(ret), K_(table_group_name));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(tenant_id,
                                                                  table_group_id,
                                                                  table_schemas))) {
    LOG_WARN("failed to get table schemas", K(ret), K(tenant_id), K(table_group_id));
  } else {
    // 2. check if sub-tables is enabled. if enabled，then return OB_KV_TABLE_NOT_ENABLED
    const ObTableSchema *schema = table_schemas.at(0);
    ObKVAttr kv_attr;
    if (OB_ISNULL(schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table schemas", K(ret), K(tenant_id), K(table_group_id));
    } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(schema->get_kv_attributes(), kv_attr))) {
      LOG_WARN("failed get table state", K(ret), K(schema->get_kv_attributes()));
    } else if (!kv_attr.is_disable_) {
      ret = OB_KV_TABLE_NOT_DISABLED;
      LOG_WARN("table is not disabled, can't enable", K(ret));
      LOG_USER_ERROR(OB_KV_TABLE_NOT_DISABLED, table_group_name_.length(), table_group_name_.ptr());
    } else {
      // table is enabled, disable table
      ObTableApiSessGuard &session_guard = ctx.get_sess_guard();
      if (OB_FAIL(ObTableSqlUtils::enable_table(allocator_,
                                                schema_guard,
                                                session_guard.get_sess_info(),
                                                ctx.get_timeout_ts(),
                                                session_guard.get_database_name(),
                                                table_group_name_))) {
        LOG_WARN("failed to disable table", K(ret), K_(table_group_name));
      }
    }
  }

  return ret;
}

int ObTableMetaHandlerGuard::get_handler(ObTableRpcMetaType meta_type, ObITableMetaHandler *&handler)
{
  int ret = OB_SUCCESS;
  switch (meta_type) {
    case ObTableRpcMetaType::HTABLE_REGION_LOCATOR:
      handler = OB_NEWx(ObHTableRegionLocatorHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_REGION_METRICS:
      handler = OB_NEWx(ObHTableRegionMetricsHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_DELETE_TABLE:
      handler = OB_NEWx(ObHTableDeleteHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_EXISTS:
      handler = OB_NEWx(ObHTableExistsHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_CREATE_TABLE:
      handler = OB_NEWx(ObHTableCreateHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_GET_DESC:
      handler = OB_NEWx(ObHTableGetDescHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_ENABLE_TABLE:
      handler = OB_NEWx(ObHTableEnableTableHandler, &allocator_, allocator_);
      break;
    case ObTableRpcMetaType::HTABLE_DISABLE_TABLE:
      handler = OB_NEWx(ObHTableDisableTableHandler, &allocator_, allocator_);
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported table meta type", K(ret), K(meta_type));
      break;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(handler)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create table meta handler", K(ret), K(meta_type));
  } else {
    handler_ = handler;
  }

  return ret;
}
