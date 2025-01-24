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

#define USING_LOG_PREFIX DDL
#include "ob_dbms_space.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ddl/ob_create_index_resolver.h"
#include "share/stat/ob_opt_stat_manager.h"

#define GET_COMPRESSED_INFO_SQL "select sum(occupy_size)/sum(original_size) as compression_ratio from oceanbase.__all_virtual_tablet_sstable_macro_info "\
                                "where tablet_id in (%.*s) and (svr_ip, svr_port) in (%.*s) and tenant_id = %lu;"\

#define GET_TABLET_INFO_SQL "select case when tablet_id is null then 0 else tablet_id end as tablet_id,"\
                                  " sum(original_size)/sum(row_count) as row_len,"\
                                  " sum(row_count) as row_count, "\
                                  " sum(occupy_size)/sum(original_size) as compression_ratio "\
                            "from __all_virtual_tablet_sstable_macro_info "\
                            "where tablet_id in (%.*s) and (svr_ip, svr_port) in (%.*s) and tenant_id = %lu "\
                            "group by __all_virtual_tablet_sstable_macro_info.tablet_id with rollup;"


#define GET_TABLET_SIZE_SQL "select case when tablet_id is null then 0 else tablet_id end as tablet_id, sum(occupy_size) as tablet_size "\
                            "from __all_virtual_tablet_pointer_status "\
                            "where tablet_id in (%.*s) and (svr_ip, svr_port) in (%.*s) and tenant_id = %lu "\
                            "group by __all_virtual_tablet_pointer_status.tablet_id;"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace pl
{

/**
 * @brief ObDbmsSpace::create_index_cost
 * @param ctx
 * @param params
 *      0. ddl            VARCHAR2, // the create index sql
 *      1. used_bytes     NUMBER,   // the real size
 *      2. alloc_bytes    NUMBER,   // the store size.
 *      3. plan_table     VARCHAR2, // the data table, default NULL
 * @param result
 * @return
 */
int ObDbmsSpace::create_index_cost(sql::ObExecContext &ctx,
                                   sql::ParamStore &params,
                                   common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObCreateIndexStmt *stmt = nullptr;
  IndexCostInfo info;
  OptStats opt_stats;
  ObString ddl_str;

  if (params.count() != 4) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "param count do not match", K(ret), K(params));
  } else if (OB_FAIL(params.at(0).get_string(ddl_str))) {
    SQL_ENG_LOG(WARN, "fail to get string", K(ret));
  } else if (OB_FAIL(parse_ddl_sql(ctx, ddl_str, stmt))) {
    SQL_ENG_LOG(WARN, "fail to parse_ddl_SQL", K(ret));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get unexpected null pointer", K(ret));
  } else if (OB_FAIL(extract_info_from_stmt(ctx,
                                            stmt,
                                            info))) {
    SQL_ENG_LOG(WARN, "fail extract info from stmt", K(ret));
  } else if (OB_FAIL(get_compressed_ratio(ctx, info))) {
    SQL_ENG_LOG(WARN, "fail to get compression ratio", K(ret));
  } else if (OB_FAIL(get_optimizer_stats(info,
                                         opt_stats))) {
    SQL_ENG_LOG(WARN, "fail to get opt stats", K(ret));
  } else if (OB_FAIL(calc_index_size(opt_stats,
                                     info,
                                     params.at(1),
                                     params.at(2)))) {
    SQL_ENG_LOG(WARN, "fail to calc index size", K(ret));
  }

  return ret;
}

// parser sql_string to statement
int ObDbmsSpace::parse_ddl_sql(ObExecContext &ctx,
                               const ObString &ddl_sql,
                               ObCreateIndexStmt *&stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = nullptr;
  ObSchemaChecker schema_checker;
  stmt = nullptr;

  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(session));
  } else if (OB_FAIL(schema_checker.init(*(ctx.get_sql_ctx()->schema_guard_)))) {
    SQL_ENG_LOG(WARN, "fail to init schema checker", K(ret));
  } else {
    ObParser parser(ctx.get_allocator(), session->get_sql_mode());
    ParseResult parse_result;
    SMART_VAR(ObResolverParams, resolver_ctx) {
      resolver_ctx.allocator_  = &ctx.get_allocator();
      resolver_ctx.schema_checker_ = &schema_checker;
      resolver_ctx.session_info_ = session;
      resolver_ctx.expr_factory_ = ctx.get_expr_factory();
      resolver_ctx.stmt_factory_ = ctx.get_stmt_factory();
      if (OB_ISNULL(ctx.get_stmt_factory())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "stmt factory is NULL", K(ret));
      } else if (FALSE_IT(resolver_ctx.query_ctx_ =
                          ctx.get_stmt_factory()->get_query_ctx())) {
      } else if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "phy plan ctx is NULL", K(ret));
      } else {
        resolver_ctx.param_list_ = &ctx.get_physical_plan_ctx()->get_param_store();
        resolver_ctx.query_ctx_->sql_schema_guard_.set_schema_guard(ctx.get_sql_ctx()->schema_guard_);
      }

      if (OB_SUCC(ret)) {
        HEAP_VAR(ObCreateIndexResolver, resolver, resolver_ctx) {
          ParseNode *tree = nullptr;
          if (OB_FAIL(parser.parse(ddl_sql, parse_result))) {
            SQL_ENG_LOG(WARN, "fail to parse stmt", K(ret));
          } else if (OB_ISNULL(tree = parse_result.result_tree_->children_[0])) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "result tree is null", K(ret));
          } else if (OB_FAIL(resolver.resolve(*tree))) {
            SQL_ENG_LOG(WARN, "fail to resove parse tree", K(ret));
          } else {
            stmt = resolver.get_create_index_stmt();
          }
        }
      }
    }
  }

  return ret;
}

// get table_id, index column's column_id, partition_id
// the count of column that have statistics
int ObDbmsSpace::extract_info_from_stmt(ObExecContext &ctx,
                                        ObCreateIndexStmt *stmt,
                                        IndexCostInfo &info)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard* schema_guard = nullptr;
  const share::schema::ObTableSchema* table_schema = nullptr;
  ObSQLSessionInfo *session = nullptr;
  int64_t partition_id = OB_INVALID_PARTITION_ID;

  if (OB_ISNULL(stmt) || OB_ISNULL(schema_guard = ctx.get_sql_ctx()->schema_guard_)
      || OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(stmt), KP(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(session->get_effective_tenant_id(),
                                                    stmt->get_table_id(),
                                                    table_schema))) {
    SQL_ENG_LOG(WARN, "fail to get table_schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    SQL_ENG_LOG(WARN, "can't find table_schema", K(ret));
  } else if (OB_FAIL(get_svr_info_from_schema(table_schema, info.svr_addr_, info.tablet_ids_, info.table_tenant_id_))) {
    SQL_ENG_LOG(WARN, "fail to get info from schema", K(ret));
  } else if (OB_FAIL(get_index_column_ids(table_schema, stmt->get_create_index_arg(), info))) {
    SQL_ENG_LOG(WARN, "fail to get index column ids", K(ret));
  } else {
    info.tenant_id_ = session->get_effective_tenant_id();
    info.table_id_ = ObSchemaUtils::get_extract_schema_id(info.tenant_id_, stmt->get_table_id());
    if (table_schema->is_partitioned_table()) {
      partition_id = -1;
    } else {
      partition_id = ObSchemaUtils::get_extract_schema_id(info.tenant_id_, info.table_id_);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(info.part_ids_.push_back(partition_id))) {
        SQL_ENG_LOG(WARN, "fail to push back partition_id", K(ret));
      } else {
        SQL_ENG_LOG(TRACE, "DBMS_SPACE: finial info is ", K(info));
      }
    }
  }

  return ret;
}

int ObDbmsSpace::get_index_column_ids(const share::schema::ObTableSchema *table_schema,
                                      const obrpc::ObCreateIndexArg &arg,
                                      IndexCostInfo &info)

{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *tmp_col = nullptr;
  ObIArray<uint64_t> &column_ids = info.column_ids_;
  ObString tmp_col_name;
  bool is_tmp_match = true;
  bool is_match = false;
  bool is_oracle_mode = false;

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      // get index column ids;
      const ObColumnSchemaV2 *tmp_col = nullptr;
      tmp_col_name = arg.index_columns_.at(i).column_name_;
      if (OB_ISNULL(tmp_col = table_schema->get_column_schema(tmp_col_name))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        SQL_ENG_LOG(WARN, "fail to get column schema", K(ret), K(tmp_col_name));
      } else if (OB_FAIL(column_ids.push_back(tmp_col->get_column_id()))) {
        SQL_ENG_LOG(WARN, "fail to push back column id", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: final column ids are", K(column_ids));
  }

  return ret;
}

int ObDbmsSpace::get_optimizer_stats(const IndexCostInfo &info,
                                     OptStats &opt_stats)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(info.table_tenant_id_,
                                                              info.table_id_,
                                                              info.part_ids_,
                                                              opt_stats.table_stats_))) {
    SQL_ENG_LOG(WARN, "fail to get table stat", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(info.table_tenant_id_,
                                                                      info.table_id_,
                                                                      info.part_ids_,
                                                                      info.column_ids_,
                                                                      opt_stats.column_stats_))) {
    SQL_ENG_LOG(WARN, "fail to get column stats", K(ret));
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: get optimizer statistics", K(opt_stats.table_stats_), K(opt_stats.column_stats_));
  }

  return ret;
}


int ObDbmsSpace::get_compressed_ratio(ObExecContext &ctx,
                                      IndexCostInfo &info)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = nullptr;
  ObSQLSessionInfo *session = nullptr;

  if (OB_ISNULL(sql_proxy = ctx.get_sql_proxy()) || OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy), KP(session));
  } else if (OB_FAIL(inner_get_compressed_ratio(sql_proxy, info))){
    SQL_ENG_LOG(WARN, "fail to calc compression ratio", K(ret));
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: get compression ratio", K(info.compression_ratio_));
  }

  return ret;
}

int ObDbmsSpace::inner_get_compressed_ratio(ObMySQLProxy *sql_proxy,
                                            IndexCostInfo &info)
{
  int ret = OB_SUCCESS;
  ObSqlString compression_ratio_sql;
  ObSqlString svr_addr_predicate;
  ObSqlString tablet_predicate;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(generate_part_key_str(svr_addr_predicate, info.svr_addr_))) {
    SQL_ENG_LOG(WARN, "fail to get svr_addr_predicate", K(ret));
  } else if (OB_FAIL(generate_tablet_predicate_str(tablet_predicate, info.tablet_ids_))) {
    SQL_ENG_LOG(WARN, "fail to get tablet_predicate", K(ret));
  } else if (svr_addr_predicate.length() == 0 || tablet_predicate.length() == 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "the predicate id unexpected", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(compression_ratio_sql.assign_fmt(GET_COMPRESSED_INFO_SQL,
                                                   static_cast<int32_t>(tablet_predicate.length()),
                                                   tablet_predicate.ptr(),
                                                   static_cast<int32_t>(svr_addr_predicate.length()),
                                                   svr_addr_predicate.ptr(),
                                                   info.table_tenant_id_))) {
        SQL_ENG_LOG(WARN, "fail to construct inner sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(result, OB_SYS_TENANT_ID, compression_ratio_sql.ptr()))) {
        SQL_ENG_LOG(WARN, "exec query fail", K(ret));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get result fail", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
          if (OB_FAIL(extract_total_compression_ratio(result.get_result(), info.compression_ratio_))) {
            SQL_ENG_LOG(WARN, "fail to extract result", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObDbmsSpace::extract_total_compression_ratio(const sqlclient::ObMySQLResult *result,
                                                 double &compression_ratio)
{
  int ret = OB_SUCCESS;
  int64_t col_idx = 0;
  compression_ratio = 0;
  number::ObNumber tmp_comp_ratio;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(result));
  } else if (OB_FAIL(result->get_number(col_idx++, tmp_comp_ratio))) {
    if (ret == OB_ERR_NULL_VALUE) {
      // may not have macro blocks. use the default compression_ratio
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get double from result", K(ret));
    }
  } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(tmp_comp_ratio, compression_ratio))) {
    SQL_ENG_LOG(WARN, "fail to cat number to double", K(ret));
  }

  return ret;
}

int ObDbmsSpace::calc_index_size(OptStats &opt_stats,
                                 IndexCostInfo &info,
                                 ObObjParam &actual_size,
                                 ObObjParam &alloc_size)
{
  int ret = OB_SUCCESS;
  actual_size.set_uint64(0);
  alloc_size.set_uint64(0);
  uint64_t dummy_actual_size = 0;
  uint64_t dummy_alloc_size = 0;

  if (info.part_ids_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "fail to get table stats", K(ret));
  } else if (OB_FAIL(inner_calc_index_size(opt_stats.table_stats_.at(0),
                                           opt_stats.column_stats_,
                                           info,
                                           dummy_actual_size,
                                           dummy_alloc_size))) {
    SQL_ENG_LOG(WARN, "fail to calc stat", K(ret));
  } else {
    actual_size.set_uint64(dummy_actual_size);
    alloc_size.set_uint64(dummy_alloc_size);
  }

  return ret;
}

int ObDbmsSpace::inner_calc_index_size(const ObOptTableStat &table_stat,
                                       const ObIArray<ObOptColumnStatHandle> &column_stats,
                                       const IndexCostInfo &info,
                                       uint64_t &actual_size,
                                       uint64_t &alloc_size)
{
  int ret = OB_SUCCESS;
  uint64_t row_count = 0;
  uint64_t block_count = 0;
  uint64_t index_column_len = 0;
  uint64_t actual_store_size = 0;
  int64_t target_part_id = OB_INVALID_PARTITION_ID;
  const uint64_t block_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
  actual_size = 0;
  alloc_size = 0;
  if (table_stat.get_last_analyzed() > 0) {
    row_count = table_stat.get_row_count();
    target_part_id = table_stat.get_partition_id();
  } else {
    SQL_ENG_LOG(WARN, "the table stat is default", K(ret), K(table_stat));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); i++) {
    if (OB_ISNULL(column_stats.at(i).stat_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "fail to get column stat", K(ret), K(column_stats.at(i).stat_));
    } else if (column_stats.at(i).stat_->get_last_analyzed() < 0) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "the stat is default", K(ret));
    } else if (column_stats.at(i).stat_->get_partition_id() == target_part_id) {
      uint64_t col_len = column_stats.at(i).stat_->get_avg_len();
      if (col_len > sizeof(ObDatum)) {
        col_len = col_len - sizeof(ObDatum);
      }
      index_column_len += col_len;
    }
  }

  if (OB_SUCC(ret)) {
    actual_store_size = row_count * index_column_len * info.compression_ratio_;
    actual_store_size = actual_store_size == 0 ? 1 : actual_store_size;
    if (actual_store_size % block_size != 0) {
      block_count =  actual_store_size / block_size + 1;
    } else {
      block_count = actual_store_size / block_size;
    }
    actual_size = actual_store_size;
    alloc_size = block_count * block_size;
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: result", K(target_part_id),
                                             K(row_count),
                                             K(block_count),
                                             K(index_column_len),
                                             K(actual_size));
  }
  return ret;
}


int ObDbmsSpace::fill_tablet_infos(const ObTableSchema *table_schema,
                                   TabletInfoList &tablet_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObjectID, 1> tmp_partition_id;
  ObSEArray<ObTabletID, 1> tmp_tablet_id;
  tablet_infos.reset();

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(table_schema));
  } else if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(tmp_tablet_id, tmp_partition_id))) {
    SQL_ENG_LOG(WARN, "fail to get tablet and partition id", K(ret));
  } else if (tmp_tablet_id.count() != tmp_partition_id.count()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "tablet id and partition id not match", K(ret), K(tmp_tablet_id), K(tmp_partition_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tablet_id.count(); i++) {
      TabletInfo tmp_tablet_info(tmp_tablet_id.at(i), tmp_partition_id.at(i));
      if (OB_FAIL(tablet_infos.push_back(tmp_tablet_info))) {
        SQL_ENG_LOG(WARN, "tablet id and partition id not match", K(ret), K(tmp_tablet_id), K(tmp_partition_id));
      }
    }
    if (OB_SUCC(ret)) {
      // for partitioned table, we need an item to indicate the global information. The item's tablet_id is 0 and part_id is -1
      // this item is redundant for non-part table.
      if (OB_FAIL(tablet_infos.push_back(TabletInfo(ObTabletID(0), -1)))) {
        SQL_ENG_LOG(WARN, "fail to push back to tablet info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: tablet_info", K(tablet_infos));
  }

  return ret;
}

const ObDbmsSpace::TabletInfo* ObDbmsSpace::get_tablet_info_by_tablet_id(const TabletInfoList &tablet_infos,
                                                                         const ObTabletID tablet_id)
{
  const TabletInfo *res = nullptr;
  for (int64_t i = 0; i < tablet_infos.count(); i++) {
    if (tablet_infos.at(i).tablet_id_ == tablet_id) {
      res = &tablet_infos.at(i);
      break;
    }
  }
  return res;
}

const ObDbmsSpace::TabletInfo* ObDbmsSpace::get_tablet_info_by_part_id(const TabletInfoList &tablet_infos,
                                                                       const ObObjectID partition_id)
{
  const TabletInfo *res = nullptr;
  for (int64_t i = 0; i < tablet_infos.count(); i++) {
    if (tablet_infos.at(i).partition_id_ == partition_id) {
      res = &tablet_infos.at(i);
      break;
    }
  }
  return res;
}

int ObDbmsSpace::get_each_tablet_size(ObMySQLProxy *sql_proxy,
                                      TabletInfoList &tablet_infos,
                                      IndexCostInfo &info)
{
  int ret = OB_SUCCESS;
  ObSqlString get_tablet_size_sql;
  ObSqlString svr_addr_predicate;
  ObSqlString tablet_predicate;

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(generate_part_key_str(svr_addr_predicate, info.svr_addr_))) {
    SQL_ENG_LOG(WARN, "fail to get svr_addr_predicate", K(ret));
  } else if (OB_FAIL(generate_tablet_predicate_str(tablet_predicate, info.tablet_ids_))) {
    SQL_ENG_LOG(WARN, "fail to get tablet_predicate", K(ret));
  } else if (svr_addr_predicate.length() == 0 || tablet_predicate.length() == 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "the predicate id unexpected", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(get_tablet_size_sql.assign_fmt(GET_TABLET_INFO_SQL,
                                                 static_cast<int32_t>(tablet_predicate.length()),
                                                 tablet_predicate.ptr(),
                                                 static_cast<int32_t>(svr_addr_predicate.length()),
                                                 svr_addr_predicate.ptr(),
                                                 info.table_tenant_id_))) {
        SQL_ENG_LOG(WARN, "fail to construct inner sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(result, OB_SYS_TENANT_ID, get_tablet_size_sql.ptr()))) {
        SQL_ENG_LOG(WARN, "exec query fail", K(ret));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get result fail", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
          if (OB_FAIL(extract_tablet_size(result.get_result(), tablet_infos))) {
            SQL_ENG_LOG(WARN, "fail to extract result", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  return ret;
}

int ObDbmsSpace::get_each_tablet_size(ObMySQLProxy *sql_proxy,
                                      const ObTableSchema *table_schema,
                                      ObIArray<std::pair<ObTabletID, uint64_t>> &tablet_size)
{
  int ret = OB_SUCCESS;
  ObSqlString get_tablet_size_sql;
  ObSqlString svr_addr_predicate;
  ObSqlString tablet_predicate;
  ObSEArray<ObAddr, 4> svr_addr;
  ObSEArray<ObTabletID, 4> tablet_ids;
  uint64_t table_tenant_id = OB_INVALID_TENANT_ID;
  tablet_size.reset();

  if (OB_ISNULL(sql_proxy) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy), KP(table_schema));
  } else if (OB_FAIL(get_svr_info_from_schema(table_schema, svr_addr, tablet_ids, table_tenant_id))) {
    SQL_ENG_LOG(WARN, "fail to get svr_info", K(ret));
  } else if (OB_FAIL(generate_part_key_str(svr_addr_predicate, svr_addr))) {
    SQL_ENG_LOG(WARN, "fail to get svr_addr_predicate", K(ret));
  } else if (OB_FAIL(generate_tablet_predicate_str(tablet_predicate, tablet_ids))) {
    SQL_ENG_LOG(WARN, "fail to get tablet_predicate", K(ret));
  } else if (svr_addr_predicate.length() == 0 || tablet_predicate.length() == 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "the predicate id unexpected", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result) {
      if (OB_FAIL(get_tablet_size_sql.assign_fmt(GET_TABLET_SIZE_SQL,
                                                 static_cast<int32_t>(tablet_predicate.length()),
                                                 tablet_predicate.ptr(),
                                                 static_cast<int32_t>(svr_addr_predicate.length()),
                                                 svr_addr_predicate.ptr(),
                                                 table_tenant_id))) {
        SQL_ENG_LOG(WARN, "fail to construct inner sql", K(ret));
      } else if (OB_FAIL(sql_proxy->read(result, OB_SYS_TENANT_ID, get_tablet_size_sql.ptr()))) {
        SQL_ENG_LOG(WARN, "exec query fail", K(ret));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get result fail", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result.get_result()->next())) {
          if (OB_FAIL(extract_tablet_size(result.get_result(), tablet_size))) {
            SQL_ENG_LOG(WARN, "fail to extract result", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: get each tablet size", K(tablet_size));
  }

  return ret;
}

int ObDbmsSpace::extract_tablet_size(const sqlclient::ObMySQLResult *result,
                                     ObIArray<std::pair<ObTabletID, uint64_t>> &tablet_size)
{
  int ret = OB_SUCCESS;
  int64_t col_idx = 0;
  int64_t tablet_id = 0;
  int64_t tmp_tablet_size_int = 0;
  number::ObNumber tmp_tablet_size;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(result));
  } else if (OB_FAIL(result->get_int(col_idx++, tablet_id))) {
    if (ret == OB_ERR_NULL_VALUE) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get int from result", K(ret));
    }
  } else if (OB_FAIL(result->get_number(col_idx++, tmp_tablet_size))) {
    if (ret == OB_ERR_NULL_VALUE) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get number from result", K(ret));
    }
  } else if (OB_FAIL(tmp_tablet_size.cast_to_int64(tmp_tablet_size_int))) {
    SQL_ENG_LOG(WARN, "fail to cast number to int", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_size.push_back(std::pair<ObTabletID, uint64_t>(ObTabletID(tablet_id), tmp_tablet_size_int)))) {
      SQL_ENG_LOG(WARN, "fail to push back  tablet size", K(ret));
    }
  }

  return ret;
}


int ObDbmsSpace::extract_tablet_size(const sqlclient::ObMySQLResult *result,
                                     TabletInfoList &tablet_infos)
{
  int ret = OB_SUCCESS;
  int64_t col_idx = 0;
  int64_t tablet_id = 0;
  int64_t tmp_row_count_int = 0;
  double tmp_row_len = 0;
  double tmp_compression_ratio = 0;
  number::ObNumber tmp_row_count;
  number::ObNumber row_len;
  number::ObNumber compression_ratio;
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(result));
  } else if (OB_FAIL(result->get_int(col_idx++, tablet_id))) {
    if (ret == OB_ERR_NULL_VALUE) {
      // may not have macro blocks. use the default compression_ratio
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get int from result", K(ret));
    }
  } else if (OB_FAIL(result->get_number(col_idx++, row_len))) {
    if (ret == OB_ERR_NULL_VALUE) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get double from result", K(ret));
    }
  } else if (OB_FAIL(result->get_number(col_idx++, tmp_row_count))) {
    if (ret == OB_ERR_NULL_VALUE) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get double from result", K(ret));
    }
  } else if (OB_FAIL(result->get_number(col_idx++, compression_ratio))) {
    if (ret == OB_ERR_NULL_VALUE) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "fail to get double from result", K(ret));
    }
  } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(row_len, tmp_row_len))) {
    SQL_ENG_LOG(WARN, "fail to cast number to double", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::cast_number_to_double(compression_ratio, tmp_compression_ratio))) {
    SQL_ENG_LOG(WARN, "fail to cast number to double", K(ret));
  } else if (OB_FAIL(tmp_row_count.cast_to_int64(tmp_row_count_int))) {
    SQL_ENG_LOG(WARN, "fail to cast number int", K(ret));
  } else if (OB_FAIL(set_tablet_info_by_tablet_id(ObTabletID(tablet_id), tmp_row_len, tmp_row_count_int, tmp_compression_ratio,
                                          tablet_infos))) {
    SQL_ENG_LOG(WARN, "fail to set tablet info", K(ret));
  }
  return ret;
}

int ObDbmsSpace::set_tablet_info_by_tablet_id(const ObTabletID tablet_id,
                                              const double row_len,
                                              const uint64_t row_count,
                                              const double compression_ratio,
                                              TabletInfoList &tablet_infos)
{
  int ret = OB_SUCCESS;

  TabletInfo *tablet_info = nullptr;
  if (row_len < 0 || compression_ratio < 0) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "get invalid argument", K(ret), K(row_len), K(compression_ratio));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_infos.count(); i++) {
    if (tablet_infos.at(i).tablet_id_ == tablet_id) {
      tablet_info = &tablet_infos.at(i);
      break;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(tablet_info)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "fail to get tablet info", K(ret), KP(tablet_info));
    } else {
      tablet_info->row_count_ = row_count;
      tablet_info->row_len_ = row_len;
      tablet_info->compression_ratio_ = compression_ratio;
    }
  }

  return ret;
}

int ObDbmsSpace::estimate_index_table_size(ObMySQLProxy *sql_proxy,
                                           const ObTableSchema *table_schema,
                                           IndexCostInfo &info,
                                           ObIArray<uint64_t> &table_size)
{
  int ret = OB_SUCCESS;
  OptStats opt_stats;
  bool is_valid = false;
  table_size.reset();

  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy));
  } else if (OB_FAIL(get_optimizer_stats(info, opt_stats))) {
    SQL_ENG_LOG(WARN, "fail to get opt stats", K(ret));
  } else if (OB_FAIL(check_stats_valid(opt_stats, is_valid))) {
    SQL_ENG_LOG(WARN, "fail to check opt stats", K(ret));
  } else if (is_valid) {
    if (OB_FAIL(estimate_index_table_size_by_opt_stats(sql_proxy, table_schema, opt_stats, info, table_size))) {
      SQL_ENG_LOG(WARN, "fail to estimate index table size", K(ret));
    }
  } else if (OB_FAIL(estimate_index_table_size_default(sql_proxy, table_schema, info, table_size))) {
    SQL_ENG_LOG(WARN, "fail to estimate index table size by default", K(ret));
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: table_size", K(table_size));
  }

  return ret;
}

int ObDbmsSpace::check_stats_valid(const OptStats &opt_stats, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; is_valid && i < opt_stats.table_stats_.count(); i++) {
    if (opt_stats.table_stats_.at(i).get_last_analyzed() > 0) {
    } else {
      is_valid = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < opt_stats.column_stats_.count(); i++) {
    const ObOptColumnStat *col_stat = nullptr;
    if (OB_ISNULL(col_stat = opt_stats.column_stats_.at(i).stat_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get unexpected null pointer", K(ret));
    } else if (col_stat->get_last_analyzed() > 0) {
    } else {
      is_valid = false;
    }
  }

  return ret;
}

int ObDbmsSpace::estimate_index_table_size_by_opt_stats(ObMySQLProxy *sql_proxy,
                                                        const ObTableSchema *table_schema,
                                                        const OptStats &opt_stats,
                                                        IndexCostInfo &info,
                                                        ObIArray<uint64_t> &table_size)
{
  int ret = OB_SUCCESS;
  table_size.reset();

  if (OB_ISNULL(sql_proxy) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(sql_proxy), KP(table_schema));
  } else if (OB_FAIL(get_svr_info_from_schema(table_schema,
                                              info.svr_addr_,
                                              info.tablet_ids_,
                                              info.table_tenant_id_))) {
    SQL_ENG_LOG(WARN, "fail to get info from schema", K(ret));
  } else if (OB_FAIL(inner_get_compressed_ratio(sql_proxy, info))) {
    SQL_ENG_LOG(WARN, "fail to get compression ratio", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < info.part_ids_.count(); i++) {
      uint64_t dummy_alloc_size = 0;
      uint64_t actual_size = 0;
      if (OB_FAIL(inner_calc_index_size(opt_stats.table_stats_.at(i), opt_stats.column_stats_, info,
                                        actual_size, dummy_alloc_size))) {
        SQL_ENG_LOG(WARN, "fail to calc index size", K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_size.push_back(actual_size))) {
          SQL_ENG_LOG(WARN, "fail to push back table_size", K(ret), K(actual_size));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE: get_tablet info by opt_stats", K(opt_stats), K(table_size), K(info));
  }
  return ret;
}

int ObDbmsSpace::estimate_index_table_size_default(ObMySQLProxy *sql_proxy,
                                                   const ObTableSchema *table_schema,
                                                   IndexCostInfo &info,
                                                   ObIArray<uint64_t> &table_size)
{
  int ret = OB_SUCCESS;
  // 1. get each partition's data size;
  TabletInfoList tablet_infos;
  table_size.reset();
  if (OB_ISNULL(table_schema) || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(table_schema), KP(sql_proxy));
  } else if (OB_FAIL(fill_tablet_infos(table_schema, tablet_infos))) {
    SQL_ENG_LOG(WARN, "fail to fill tablet info", K(ret));
  } else if (OB_FAIL(get_svr_info_from_schema(table_schema,
                                              info.svr_addr_,
                                              info.tablet_ids_,
                                              info.table_tenant_id_))) {
    SQL_ENG_LOG(WARN, "fail to get info from schema", K(ret));
  } else if (OB_FAIL(get_each_tablet_size(sql_proxy, tablet_infos, info))) {
    SQL_ENG_LOG(WARN, "fail to get tablet size", K(ret));
  } else if (OB_FAIL(get_default_index_column_len(table_schema,
                                                  tablet_infos,
                                                  info))) {
    SQL_ENG_LOG(WARN, "fail to get default index column len", K(ret));
  } else if (OB_FAIL(inner_calc_index_size_by_default(info, tablet_infos, table_size))) {
    SQL_ENG_LOG(WARN, "fail to calc index size", K(ret));
  }

  if (OB_SUCC(ret)) {
    SQL_ENG_LOG(TRACE, "DBMS_SPACE by default", K(tablet_infos), K(info));
  }

  return ret;
}

int ObDbmsSpace::inner_calc_index_size_by_default(IndexCostInfo &info,
                                                  TabletInfoList &tablet_infos,
                                                  ObIArray<uint64_t> &table_size)
{
  int ret = OB_SUCCESS;

  const TabletInfo *tablet_info = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < info.part_ids_.count(); i++) {
    if (OB_ISNULL(tablet_info = get_tablet_info_by_part_id(tablet_infos, info.part_ids_.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "get unexpected null pointer", K(ret));
    } else {
      uint64_t partition_size = info.default_index_len_ * tablet_info->row_count_ * tablet_info->compression_ratio_;
      if (OB_FAIL(table_size.push_back(partition_size))) {
        SQL_ENG_LOG(WARN, "fail to push back partition size", K(ret));
      }
    }
  }

  return ret;
}

// TODO if no optimizer_stats should use the infomation in macro info to calc the size.
int ObDbmsSpace::get_default_index_column_len(const ObTableSchema *table_schema,
                                              const TabletInfoList &tablet_infos,
                                              IndexCostInfo &info)
{
  int ret = OB_SUCCESS;
  // get row_len from macro_info
  const TabletInfo *global_tablet_info = nullptr;
  info.default_index_len_ = 0;

  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the args is null", K(ret), KP(table_schema));
  } else if (OB_ISNULL(global_tablet_info = get_tablet_info_by_tablet_id(tablet_infos, ObTabletID(0)))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get unexpected null pointer", K(ret));
  } else {
    uint64_t index_var_column_cnt = 0;
    uint64_t index_fix_column_len = 0;
    uint64_t fix_column_len = 0;
    uint64_t var_column_cnt = 0;
    const ObColumnSchemaV2 *tmp_col = nullptr;

    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); i++) {
      if (OB_ISNULL(tmp_col = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "get unexpected null pointer", K(ret));
      } else {
        bool is_index_column = has_exist_in_array(info.column_ids_, tmp_col->get_column_id());
        bool is_fix_column = is_fixed_length_storage(tmp_col->get_meta_type().get_type());
        if (is_fix_column) {
          int16_t len = get_type_fixed_length(tmp_col->get_meta_type().get_type());
          fix_column_len += len;
          if (is_index_column) {
            index_fix_column_len += len;
          }
        } else if (tmp_col->get_meta_type().is_binary()) {
          int16_t len = tmp_col->get_data_length();
          fix_column_len += len;
          if (is_index_column) {
            index_fix_column_len += len;
          }
        } else {
          if (is_index_column) {
            index_var_column_cnt++;
          }
          var_column_cnt++;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (global_tablet_info->row_len_ > 0) {
        if (var_column_cnt != 0) {
          if (global_tablet_info->row_len_ - fix_column_len > 0) {
            uint64_t avg_var_column_len = (global_tablet_info->row_len_ - fix_column_len) / var_column_cnt;
            info.default_index_len_ = index_fix_column_len + avg_var_column_len * index_var_column_cnt;
          } else {
            info.default_index_len_ = 0;
            SQL_ENG_LOG(INFO, "the var column len is less than 0", K(global_tablet_info->row_len_), K(fix_column_len));
          }
        } else {
          info.default_index_len_ = index_fix_column_len;
        }
      } else {
        info.default_index_len_ = 0;
      }
    }
  }
  return ret;
}

int ObDbmsSpace::get_svr_info_from_schema(const ObTableSchema *table_schema,
                                          ObIArray<ObAddr> &addr_list,
                                          ObIArray<ObTabletID> &tablet_list,
                                          uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  addr_list.reset();
  tablet_list.reset();

  tablet_list.reset();
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "the arg is null", K(ret), KP(table_schema));
  } else if (OB_FAIL(table_schema->get_tablet_ids(tablet_list))) {
    SQL_ENG_LOG(WARN, "fail to get tablet_ids", K(ret));
  } else if (tablet_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "can't find tablet", K(ret));
  } else {
    const int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    ObLSID dummy_ls_id;
    ObAddr leader_addr;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); i++) {
      if (OB_FAIL(ObDDLUtil::get_tablet_leader_addr(GCTX.location_service_,
                                                    table_schema->get_tenant_id(),
                                                    tablet_list.at(i),
                                                    rpc_timeout,
                                                    dummy_ls_id,
                                                    leader_addr
                                                    ))) {
        SQL_ENG_LOG(WARN, "fail to get tablet's leader_addr", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(addr_list, leader_addr))) {
        SQL_ENG_LOG(WARN, "fail to add add to addr_list", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      tenant_id = table_schema->get_tenant_id();
    }
  }

  return ret;
}


int ObDbmsSpace::generate_part_key_str(ObSqlString &target_str,
                                       const ObIArray<ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  target_str.reset();
  const static int MAX_IP_BUFFER_LEN = 32;
  char host[MAX_IP_BUFFER_LEN];
  for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); i++) {
    host[0] = '\0';
    if (!addr_list.at(i).ip_to_string(host, MAX_IP_BUFFER_LEN)) {
      ret = OB_BUF_NOT_ENOUGH;
      SQL_ENG_LOG(WARN, "fail to get host.", K(ret));
    } else if (OB_FAIL(target_str.append_fmt((i == addr_list.count() - 1) ? "('%.*s', %d)" : "('%.*s', %d),",
                                              (int)strlen(host),
                                              host,
                                              addr_list.at(i).get_port()))) {
      SQL_ENG_LOG(WARN, "fail to append fmt.", K(ret));
    }
  }

  return ret;
}

int ObDbmsSpace::generate_tablet_predicate_str(ObSqlString &target_str,
                                               const ObIArray<ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  target_str.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_list.count(); i++) {
    if (OB_FAIL(target_str.append_fmt((i == tablet_list.count() - 1) ? "%lu" : "%lu,",
                                       tablet_list.at(i).id()))) {
      SQL_ENG_LOG(WARN, "fail to generate tablet predicate", K(ret));
    }
  }

  return ret;
}

}
}
