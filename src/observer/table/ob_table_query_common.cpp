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
  if (N != 4 && N != 5) { // htable maybe has prefix generated column
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
      LOG_WARN("TableQuery with htable_filter only support forward and reverse scan yet", K(ret));
    }
  }
  return ret;
}

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
  const ObString &kv_attributes = tb_ctx.get_table_schema()->get_kv_attributes();

  if (OB_FAIL(one_result.assign_property_names(tb_ctx.get_query_col_names()))) {
    LOG_WARN("fail to assign property names to one result", K(ret), K(tb_ctx));
  } else if (has_filter) {
    if (is_hkv) {
      ObHTableFilterOperator *htable_result_iter = nullptr;
      if (OB_FAIL(check_htable_query_args(query, tb_ctx))) {
        LOG_WARN("fail to check htable query args", K(ret), K(tb_ctx));
      } else if (OB_ISNULL(htable_result_iter = OB_NEWx(ObHTableFilterOperator,
                                                        (&allocator),
                                                        query,
                                                        one_result))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc htable query result iterator", K(ret));
      } else if (OB_FAIL(htable_result_iter->parse_filter_string(&allocator))) {
        LOG_WARN("fail to parse htable filter string", K(ret));
      } else {
        tmp_result_iter = htable_result_iter;
        ObHColumnDescriptor desc;
        if (OB_FAIL(desc.from_string(kv_attributes))) {
          LOG_WARN("fail to parse hcolumn_desc from kv attributes", K(ret), K(kv_attributes));
        } else {
          if (desc.get_time_to_live() > 0) {
            htable_result_iter->set_ttl(desc.get_time_to_live());
          }
          if (desc.get_max_version() > 0) {
            htable_result_iter->set_max_version(desc.get_max_version());
          }
        }
      }
    } else { // tableapi
      ObTableFilterOperator *table_result_iter = nullptr;
      if (OB_ISNULL(table_result_iter = OB_NEWx(ObTableFilterOperator,
                                                (&allocator),
                                                query,
                                                one_result))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc table query result iterator", K(ret));
      } else if (OB_FAIL(table_result_iter->parse_filter_string(&allocator))) {
        LOG_WARN("fail to parse table filter string", K(ret));
      } else {
        if (query.is_aggregate_query()) {
          table_result_iter->init_aggregation();
          table_result_iter->get_agg_calculator().set_projs(tb_ctx.get_agg_projs());
        }
        tmp_result_iter = table_result_iter;
      }
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
      if (query.is_aggregate_query()) {
        normal_result_iter->init_aggregation();
        normal_result_iter->get_agg_calculator().set_projs(tb_ctx.get_agg_projs());
      }
      tmp_result_iter = normal_result_iter;
    }
  }

  if (OB_SUCC(ret)) {
    result_iter = tmp_result_iter;
  }

  return ret;
}

void ObTableQueryUtils::destroy_result_iterator(ObTableQueryResultIterator *result_iter)
{
  if (OB_NOT_NULL(result_iter)) {
    result_iter->~ObTableQueryResultIterator();
  }
}

int ObTableQueryUtils::get_rowkey_column_names(const ObTableSchema &table_schema, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = nullptr;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  const int64_t N = rowkey_info.get_size();

  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("fail to get column id", K(ret), K(i), K(rowkey_info));
    } else if (NULL == (column_schema = table_schema.get_column_schema(column_id))){
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not exists", K(ret), K(column_id));
    } else if (OB_FAIL(names.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("fail to push back rowkey column name", K(ret), K(names));
    }
  }

  return ret;
}

int ObTableQueryUtils::get_full_column_names(const ObTableSchema &table_schema, ObIArray<ObString> &names)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *column_schema = nullptr;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  const int64_t N = rowkey_info.get_size();

  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    uint64_t column_id = OB_INVALID_ID;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      LOG_WARN("fail to get column id", K(ret), K(i), K(rowkey_info));
    } else if (NULL == (column_schema = table_schema.get_column_schema(column_id))){
      ret = OB_ERR_COLUMN_NOT_FOUND;
      LOG_WARN("column not exists", K(ret), K(column_id));
    } else if (OB_FAIL(names.push_back(column_schema->get_column_name_str()))) {
      LOG_WARN("fail to push back rowkey column name", K(ret), K(names));
    }
  }

  return ret;
}

} // end namespace table
} // end namespace oceanbase