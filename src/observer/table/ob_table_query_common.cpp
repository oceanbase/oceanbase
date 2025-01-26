/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_query_common.h"
#include "ob_htable_filter_operator.h"
#include "observer/table/redis/ob_redis_iterator.h"
#include "src/share/table/ob_table_util.h"

namespace oceanbase
{
namespace table
{

int ObTableQueryUtils::check_htable_query_args(const ObTableQuery &query,
                                               const ObTableCtx &tb_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &select_columns = tb_ctx.get_query_col_names();
  int64_t N = select_columns.count();
  if (N < 4 || N > 6) { // htable maybe has prefix generated column or TTL column
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("TableQuery with htable_filter should select 4 columns", K(ret), K(N));
  }
  if (OB_SUCC(ret)) {
    if (ObHTableConstants::ROWKEY_CNAME_STR != select_columns.at(0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select K as the first column", K(ret), K(select_columns));
    } else if (ObHTableConstants::CQ_CNAME_STR != select_columns.at(1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select Q as the second column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VERSION_CNAME_STR != select_columns.at(2)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select T as the third column", K(ret), K(select_columns));
    } else if (ObHTableConstants::VALUE_CNAME_STR != select_columns.at(3)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("TableQuery with htable_filter should select V as the fourth column", K(ret), K(select_columns));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != query.get_offset()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("htable scan should not set Offset and Limit", K(ret), K(query));
    } else if (ObQueryFlag::Forward != query.get_scan_order() && ObQueryFlag::Reverse != query.get_scan_order()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "scan order");
      LOG_WARN("TableQuery with htable_filter only support forward and reverse scan yet", K(ret));
    }
  }
  return ret;
}

template<typename ResultType>
int ObTableQueryUtils::generate_htable_result_iterator(ObIAllocator &allocator,
                                                       const ObTableQuery &query,
                                                       ResultType &one_result,
                                                       const ObTableCtx &tb_ctx,
                                                       ObTableQueryResultIterator *&result_iter)
{
  int ret = OB_SUCCESS;
  bool has_filter = (query.get_htable_filter().is_valid() || query.get_filter_string().length() > 0);
  ObKVAttr kv_attributes;

  ObHTableFilterOperator *htable_result_iter = nullptr;
  ObKvSchemaCacheGuard *schema_cache_guard = tb_ctx.get_schema_cache_guard();
  if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_cache_cache is NULL or not inited", K(ret));
  } else if (OB_FAIL(schema_cache_guard->get_kv_attributes(kv_attributes))) {
    LOG_WARN("get kv attributes failed", K(ret));
  } else if (OB_FAIL(check_htable_query_args(query, tb_ctx))) {
    LOG_WARN("fail to check htable query args", K(ret), K(tb_ctx));
  } else if (OB_ISNULL(htable_result_iter = OB_NEWx(ObHTableFilterOperator,
                                                    (&allocator),
                                                    query,
                                                    one_result))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc htable query result iterator", K(ret));
  } else if (OB_FAIL(htable_result_iter->init(&allocator))) {
    LOG_WARN("fail to init row htable_result_iter", K(ret));
  } else {
    ObHColumnDescriptor desc;
    desc.from_kv_attribute(kv_attributes);
    if (desc.get_time_to_live() > 0) {
      htable_result_iter->set_ttl(desc.get_time_to_live());
    }
    if (desc.get_max_version() > 0) {
      htable_result_iter->set_max_version(desc.get_max_version());
    }
    const ObIArray<ObString> &select_columns = query.get_select_columns();
    if ((select_columns.empty() ||
            ObTableUtils::has_exist_in_columns(select_columns, ObHTableConstants::TTL_CNAME_STR)) &&
        schema_cache_guard->get_schema_flags().has_hbase_ttl_column_) {
      htable_result_iter->set_need_verify_cell_ttl(true);
    }
  }

  if (OB_SUCC(ret)) {
    result_iter = htable_result_iter;
  } else if (OB_NOT_NULL(htable_result_iter)) {
    htable_result_iter->~ObHTableFilterOperator();
    allocator.free(htable_result_iter);
  }

  return ret;
}

// explicit specialization for ObTableQueryIterableResult
template int ObTableQueryUtils::generate_htable_result_iterator(ObIAllocator &allocator,
                                                                const ObTableQuery &query,
                                                                ObTableQueryIterableResult &one_result,
                                                                const ObTableCtx &tb_ctx,
                                                                ObTableQueryResultIterator *&result_iter);
template int ObTableQueryUtils::generate_htable_result_iterator(ObIAllocator &allocator,
                                                                const ObTableQuery &query,
                                                                ObTableQueryResult &one_result,
                                                                const ObTableCtx &tb_ctx,
                                                                ObTableQueryResultIterator *&result_iter);
template int ObTableQueryUtils::generate_htable_result_iterator(ObIAllocator &allocator,
                                                                const ObTableQuery &query,
                                                                ObTableQueryAsyncResult &one_result,
                                                                const ObTableCtx &tb_ctx,
                                                                ObTableQueryResultIterator *&result_iter);

int ObTableQueryUtils::generate_query_result_iterator(ObIAllocator &allocator,
                                                      const ObTableQuery &query,
                                                      bool is_hkv,
                                                      ObTableQueryResult &one_result,
                                                      const ObTableCtx &tb_ctx,
                                                      ObTableQueryResultIterator *&result_iter)
{
  int ret = OB_SUCCESS;
  ObTableQueryResultIterator *tmp_result_iter = nullptr;
  bool has_filter = (query.get_htable_filter().is_valid() || query.get_filter_string().length() > 0);

  if (OB_FAIL(one_result.deep_copy_property_names(tb_ctx.get_query_col_names()))) {
    LOG_WARN("fail to deep copy property names to one result", K(ret), K(tb_ctx));
  } else if (has_filter) {
    if (is_hkv) {
      if (OB_FAIL(generate_htable_result_iterator(allocator, query, one_result, tb_ctx, tmp_result_iter))) {
        LOG_WARN("fail to generate htable result iterator", K(ret), K(query));
      }
    } else { // tableapi
      ObTableFilterOperator *table_result_iter = nullptr;
      if (OB_ISNULL(table_result_iter = OB_NEWx(ObTableFilterOperator,
                                                (&allocator),
                                                query,
                                                one_result))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc table query result iterator", K(ret));
      } else if (OB_FAIL(table_result_iter->init_full_column_name(tb_ctx.get_query_col_names()))) {
        LOG_WARN("fail to int full column name", K(ret));
      } else if (OB_FAIL(table_result_iter->parse_filter_string(&allocator))) {
        LOG_WARN("fail to parse table filter string", K(ret));
      } else {
        if (query.is_aggregate_query()) {
          table_result_iter->init_aggregation();
          table_result_iter->get_agg_calculator().set_projs(tb_ctx.get_agg_projs());
        }
      }
      tmp_result_iter = table_result_iter;
    }
  } else { // no filter
    ObNormalTableQueryResultIterator *normal_result_iter = nullptr;
    if (OB_ISNULL(normal_result_iter = OB_NEWx(ObNormalTableQueryResultIterator,
                                               (&allocator),
                                               query,
                                               one_result))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc normal query result iterator", K(ret));
    } else {
      // ttl table should set corresponding limit and query
      if (tb_ctx.is_ttl_table()) {
        normal_result_iter->set_limit(query.get_limit());
        normal_result_iter->set_offset(query.get_offset());
      }
      // set aggregate params
      if (query.is_aggregate_query()) {
        normal_result_iter->init_aggregation();
        normal_result_iter->get_agg_calculator().set_projs(tb_ctx.get_agg_projs());
      }
    }
    tmp_result_iter = normal_result_iter;
  }

  if (OB_SUCC(ret)) {
    result_iter = tmp_result_iter;
  } else if (OB_NOT_NULL(tmp_result_iter)) {
    destroy_result_iterator(tmp_result_iter);
  }

  return ret;
}

void ObTableQueryUtils::destroy_result_iterator(ObTableQueryResultIterator *&result_iter)
{
  if (OB_NOT_NULL(result_iter)) {
    result_iter->~ObTableQueryResultIterator();
    result_iter = nullptr;
  }
}

int ObTableQueryUtils::get_rowkey_column_names(ObKvSchemaCacheGuard &schema_cache_guard, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  int64_t N = 0;
  if (OB_FAIL(schema_cache_guard.get_rowkey_column_num(N))) {
    LOG_WARN("failed to get rowkey column num", K(ret));
  } else {
    uint64_t column_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableColumnInfo *column_info = nullptr;
      if (OB_FAIL(schema_cache_guard.get_rowkey_column_id(i, column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i));
      } else if (OB_FAIL(schema_cache_guard.get_column_info(column_id, column_info))) {
        LOG_WARN("fail to get column info", K(ret), K(column_id));
      } else if (OB_ISNULL(column_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (OB_FAIL(names.push_back(column_info->column_name_))) {
        LOG_WARN("fail to push back rowkey column name", K(ret), K(names));
      }
    }
  }

  return ret;
}

int ObTableQueryUtils::get_full_column_names(ObKvSchemaCacheGuard &schema_cache_guard, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  int64_t N = 0;
  if (OB_FAIL(schema_cache_guard.get_rowkey_column_num(N))) {
    LOG_WARN("failed to get rowkey column num", K(ret));
  } else {
    uint64_t column_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableColumnInfo *column_info = nullptr;
      if (OB_FAIL(schema_cache_guard.get_rowkey_column_id(i, column_id))) {
        LOG_WARN("failed to get column id", K(ret), K(i));
      } else if (OB_FAIL(schema_cache_guard.get_column_info(column_id, column_info))) {
        LOG_WARN("fail to get column info", K(ret), K(column_id));
      } else if (OB_ISNULL(column_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (OB_FAIL(names.push_back(column_info->column_name_))) {
        LOG_WARN("fail to push back rowkey column name", K(ret), K(names));
      }
    }
  }
  return ret;
}

int ObTableQueryUtils::get_scan_row_interator(const ObTableCtx &tb_ctx,
                                              ObTableApiScanRowIterator *&scan_iter)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = tb_ctx.get_allocator();
  ObKvSchemaCacheGuard *cache_guard = tb_ctx.get_schema_cache_guard();
  ObKVAttr kv_attributes;
  if (OB_ISNULL(cache_guard)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null schema cache guard", K(ret));
  } else if (OB_FAIL(cache_guard->get_kv_attributes(kv_attributes))) {
    LOG_WARN("fail to get kv attributes", K(ret), K(*cache_guard));
  } else if (kv_attributes.is_redis_ttl_) {
    ObRedisRowIterator *redis_row_iter = nullptr;
    if (OB_ISNULL(redis_row_iter = OB_NEWx(ObRedisRowIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObTableTTLDeleteRowIterator", K(ret));
    } else if (OB_FAIL(redis_row_iter->init_scan(kv_attributes, tb_ctx.redis_ttl_ctx()))) {
      LOG_WARN("fail to init redis row iterator", KR(ret));
    } else {
      scan_iter = redis_row_iter;
    }
  } else {
    if (OB_ISNULL(scan_iter = OB_NEWx(ObTableApiScanRowIterator, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObTableTTLDeleteRowIterator", K(ret));
    }
  }
  return ret;
}

int ObTableQueryUtils::get_table_schemas(ObMultiVersionSchemaService *schema_service,
                              ObSchemaGetterGuard& schema_guard,
                              const ObString &arg_table_name,
                              bool is_tablegroup_name,
                              uint64_t arg_tenant_id,
                              uint64_t arg_database_id,
                              common::ObIArray<const schema::ObSimpleTableSchemaV2*> &table_schemas)
{
  int ret = OB_SUCCESS;
  uint64_t tablegroup_id = OB_INVALID_ID;

  if (OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shcema service", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(arg_tenant_id, schema_guard))) {
    LOG_WARN("Failed to get schema guard", K(ret), K(arg_tenant_id));
  } else if (is_tablegroup_name) {
    // Handle table group case
    if (OB_FAIL(schema_guard.get_tablegroup_id(arg_tenant_id, arg_table_name, tablegroup_id))) {
      LOG_WARN("Failed to get table group ID", K(ret), K(arg_tenant_id), K(arg_table_name));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tablegroup(arg_tenant_id, tablegroup_id, table_schemas))) {
      LOG_WARN("Failed to get table schemas from table group", K(ret), K(arg_tenant_id), K(tablegroup_id));
    } else {
      // Proceed to initialize multi_cf_infos_ with the tables in the table group
      // The table_schemas array now contains the schemas of all tables in the table group
    }
  } else { // handle table name case
    const schema::ObSimpleTableSchemaV2* simple_table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_simple_table_schema(arg_tenant_id,
                                                      arg_database_id,
                                                      arg_table_name,
                                                      false, /* is_index */
                                                      simple_table_schema))) {
      LOG_WARN("Failed to get simple table schema", K(ret), K(arg_tenant_id),
                K(arg_database_id), K(arg_table_name));
    } else if (OB_ISNULL(simple_table_schema) || simple_table_schema->get_table_id() == OB_INVALID_ID) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid table schema", K(ret), K(arg_table_name), KP(simple_table_schema));
    } else if (OB_FAIL(table_schemas.push_back(simple_table_schema))) {
      LOG_WARN("Failed to add table schema to array", K(ret));
    }
    // The table_schemas array now contains only the schema of the single table
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase