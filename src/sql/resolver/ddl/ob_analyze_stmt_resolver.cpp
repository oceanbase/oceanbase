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

#define USING_LOG_PREFIX SQL_RESV
#include "share/schema/ob_part_mgr_util.h"
#include "sql/resolver/ddl/ob_analyze_stmt_resolver.h"
#include "sql/resolver/ddl/ob_analyze_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_dbms_stats_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

ObAnalyzeStmtResolver::ObAnalyzeStmtResolver(ObResolverParams &params)
  : ObDDLResolver(params)
{
  // TODO Auto-generated constructor stub
}

ObAnalyzeStmtResolver::~ObAnalyzeStmtResolver()
{
  // TODO Auto-generated destructor stub
}

int ObAnalyzeStmtResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObAnalyzeStmt *analyze_stmt = NULL;
  uint64_t parallel_degree = 1;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info should not be null", K(ret));
  } else if (OB_ISNULL(analyze_stmt = create_stmt<ObAnalyzeStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create analyze stmt", K(ret));
  } else if (FALSE_IT(analyze_stmt->set_tenant_id(session_info_->get_effective_tenant_id()))) {
    /*do nothing*/
  } else if (OB_FAIL(session_info_->get_force_parallel_query_dop(parallel_degree))) {
    LOG_WARN("failed to get force parallel query dop", K(ret));
  } else if (T_ANALYZE == parse_tree.type_) {
    if (lib::is_mysql_mode() && !session_info_->is_enable_sql_extension()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "The Oracle-mode analyze syntax is used in the disable sql extension MySQL-mode");
    } else if (OB_FAIL(resolve_oracle_analyze(parse_tree, *analyze_stmt))) {
      LOG_WARN("failed to resolve oracle analyze stmt", K(ret));
    } else { /*do nothing*/ }
  } else if (T_MYSQL_ANALYZE == parse_tree.type_) {
    if (OB_FAIL(resolve_oracle_analyze(parse_tree, *analyze_stmt))) {
      LOG_WARN("failed to resolve oracle analyze stmt", K(ret));
    } else { /*do nothing*/ }
  } else if (T_MYSQL_UPDATE_HISTOGRAM == parse_tree.type_) {
    if (OB_FAIL(resolve_mysql_update_histogram(parse_tree, *analyze_stmt))) {
      LOG_WARN("failed to resolve mysql update histogram info", K(ret));
    } else { /*do nothing*/ }
  } else if (T_MYSQL_DROP_HISTOGRAM == parse_tree.type_) {
    // TODO link.zt drop is not supported at the present
    analyze_stmt->set_is_drop();
    if (OB_FAIL(resolve_mysql_delete_histogram(parse_tree, *analyze_stmt))) {
      LOG_WARN("failed to resolve mysql update histogram info", K(ret));
    } else { /*do nothing*/ }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpcted parse tree type", K(parse_tree.type_), K(ret));
  }
  if (OB_SUCC(ret)) {
    analyze_stmt->set_degree(parallel_degree);
    stmt_ = analyze_stmt;
    ObSEArray<ObColumnStatParam, 4> new_column_params;
    for (int64_t j = 0; OB_SUCC(ret) && j < analyze_stmt->get_tables().count(); ++j) {
      ObAnalyzeTableInfo &table = analyze_stmt->get_tables().at(j);
      new_column_params.reuse();
      for (int64_t i = 0 ; OB_SUCC(ret) && i < table.get_column_params().count(); ++i) {
        if (table.get_column_params().at(i).need_col_stat()) {
          if (OB_FAIL(new_column_params.push_back(table.get_column_params().at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table.get_column_params().assign(new_column_params))) {
          LOG_WARN("failed to assign", K(ret));
        }
      }
    }
    LOG_DEBUG("analyze statement", K(*analyze_stmt));
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_oracle_analyze(const ParseNode &parse_node,
                                                  ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 != parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have 3 children", K(parse_node.num_child_), K(ret));
  } else {
    ParseNode *table_node = parse_node.children_[0];
    ParseNode *part_node = parse_node.children_[1];
    ParseNode *statistic_node = parse_node.children_[2];
    if (OB_ISNULL(table_node) || OB_ISNULL(statistic_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null parse node", K(table_node), K(statistic_node), K(ret));
    } else if (OB_FAIL(recursive_resolve_table_info(table_node, analyze_stmt))) {
      LOG_WARN("failed to resolve table info", K(ret));
    } else if (OB_FAIL(resolve_partition_info(part_node, analyze_stmt))) {
      LOG_WARN("failed to resolve partition info", K(ret));
    } else if (OB_FAIL(resolve_statistic_info(statistic_node, analyze_stmt))) {
      LOG_WARN("failed to resolve statistic info", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_mysql_update_histogram(const ParseNode &parse_node,
                                                          ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(3 != parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have 3 children", K(parse_node.num_child_), K(ret));
  } else {
    ParseNode *table_node = parse_node.children_[0];
    ParseNode *column_node = parse_node.children_[1];
    ParseNode *bucket_node = parse_node.children_[2];
    int64_t bkt_num = 1;
    if (OB_ISNULL(bucket_node)) {
      bkt_num = ObColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM;
    } else {
      bkt_num = bucket_node->value_;
    }
    if (OB_ISNULL(table_node) || OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null parse node", K(table_node), K(column_node), K(ret));
    } else if (OB_FAIL(resolve_table_info(table_node, analyze_stmt))) {
      LOG_WARN("failed to resolve table info", K(ret));
    } else if (OB_FAIL(resolve_partition_info(NULL, analyze_stmt))) {
      LOG_WARN("failed to resolve partition info", K(ret));
    } else if (OB_FAIL(resolve_mysql_column_bucket_info(column_node,
                                                        bkt_num,
                                                        analyze_stmt))) {
      LOG_WARN("failed to resolve column bucket info", K(ret));
    } else {
      ObAnalyzeSampleInfo sample_info;
      sample_info.is_sample_ = true;
      sample_info.sample_type_ = SampleType::RowSample;
      sample_info.sample_value_ = bkt_num * DEFAULT_SAMPLE_ROWCOUNT_PER_BUCKET;
      analyze_stmt.set_sample_info(sample_info);
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_mysql_delete_histogram(const ParseNode &parse_node,
                                                          ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != parse_node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have 2 children", K(parse_node.num_child_), K(ret));
  } else {
    ParseNode *table_node = parse_node.children_[0];
    ParseNode *column_node = parse_node.children_[1];
    bool dumy_bool = false;
    if (OB_ISNULL(table_node) || OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null parse node", K(ret));
    } else if (OB_FAIL(resolve_table_info(table_node, analyze_stmt))) {
      LOG_WARN("failed to resolve table info", K(ret));
    } else if (OB_FAIL(resolve_partition_info(NULL, analyze_stmt))) {
      LOG_WARN("failed to resolve partition info", K(ret));
    } else if (OB_FAIL(resolve_mysql_column_bucket_info(column_node, 0,
                                                        analyze_stmt))) {
      LOG_WARN("failed to resolve column bucket info", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_mysql_column_bucket_info(const ParseNode *column_node,
                                                            const int64_t bucket_number,
                                                            ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObAnalyzeTableInfo &table_info = analyze_stmt.get_tables().at(0);
  if (OB_ISNULL(column_node) || OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid params", K(ret), K(column_node), K(schema_checker_));
  } else if (OB_FAIL(schema_checker_->get_table_schema(analyze_stmt.get_tenant_id(), table_info.get_table_id(),
                                                       table_schema))) {
    LOG_WARN("failed to get table schema", K(analyze_stmt.get_tenant_id()), K(table_info.get_table_id()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_node->num_child_; i++) {
    const ObColumnSchemaV2 *column_schema = NULL;
    ObColumnStatParam *col_param = NULL;
    ObString column_name;
    if (OB_ISNULL(column_node->children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null parse node", K(ret));
    } else {
      column_name.assign_ptr(const_cast<char *>(column_node->children_[i]->str_value_),
                             static_cast<int32_t>(column_node->children_[i]->str_len_));
      if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_name))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("failed to get column schema", K(column_name), K(ret));
      } else if (OB_ISNULL(col_param = table_info.get_column_param(column_schema->get_column_id()))) {
        // do nothing
      } else if (col_param->is_valid_opt_col()) {
        col_param->set_need_basic_stat();
        col_param->bucket_num_ = bucket_number;
        col_param->set_size_manual();
      }
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::recursive_resolve_table_info(const ParseNode *table_list_node,
                                                        ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_list_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(ret), KP(table_list_node));
  } else if (T_LINK_NODE == table_list_node->type_) {
    if (OB_FAIL(SMART_CALL(recursive_resolve_table_info(table_list_node->children_[0], analyze_stmt)))) {
      LOG_WARN("recursive resolve table list node failed", K(ret));
    } else if (OB_FAIL(SMART_CALL(recursive_resolve_table_info(table_list_node->children_[1], analyze_stmt)))) {
      LOG_WARN("recursive resolve table list node failed", K(ret));
    }
  } else if (OB_FAIL(resolve_table_info(table_list_node, analyze_stmt))) {
    LOG_WARN("resolve table info failed", K(ret));
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_table_info(const ParseNode *table_node,
                                              ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  ObString database_name;
  const ObTableSchema *table_schema = NULL;
  int64_t tenant_id = analyze_stmt.get_tenant_id();
  uint64_t database_id = OB_INVALID_ID;
  if (OB_ISNULL(table_node) || OB_ISNULL(schema_checker_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(table_node), K(schema_checker_), K(ret));
  } else if (OB_FAIL(resolve_table_relation_node(table_node, table_name, database_name))) {
    LOG_WARN("failed to resolve table relation node", K(ret));
  } else if (OB_FAIL(schema_checker_->get_database_id(tenant_id, database_name, database_id))) {
    LOG_WARN("failed to get database id", K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, database_name,
                                                       table_name, false, table_schema))){
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("null table schema", K(ret));
  } else if (OB_FAIL(analyze_stmt.add_table(database_name, database_id, table_name,
                           table_schema->is_materialized_view() ? table_schema->get_data_table_id() : table_schema->get_table_id(),
                           table_schema->get_table_type()))) {
    LOG_WARN("add table failed", K(ret));
  } else {
    ObAnalyzeTableInfo &last_table = analyze_stmt.get_tables().at(analyze_stmt.get_tables().count() - 1);
    if (OB_FAIL(pl::ObDbmsStats::init_column_stat_params(*allocator_,
                                                        *schema_checker_->get_schema_guard(),
                                                        *table_schema,
                                                        last_table.get_column_params()))) {
      LOG_WARN("failed to init column stat param", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::init_column_group_stat_param(*table_schema,
                                                                     last_table.get_column_group_params()))) {
      LOG_WARN("failed to init column stat param", K(ret));
    } else {
      LOG_TRACE("succeed to resolve table info", K(last_table));
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_partition_info(const ParseNode *part_node,
                                                  ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  ObIArray<ObAnalyzeTableInfo> &tables = analyze_stmt.get_tables();
  for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(inner_resolve_partition_info(part_node, analyze_stmt.get_tenant_id(), tables.at(i)))) {
      LOG_WARN("resolve table partition info failed", K(ret));
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::inner_resolve_partition_info(const ParseNode *part_node,
                                                        uint64_t tenant_id,
                                                        ObAnalyzeTableInfo &table_info)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  int64_t table_id = table_info.get_table_id();
  ObSEArray<PartInfo, 4> part_infos;
  ObSEArray<PartInfo, 32> subpart_infos;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<int64_t, 4> subpart_ids;
  bool is_subpart_name = false;

  ObString &partition_name = table_info.get_partition_name();
  if (OB_ISNULL(schema_checker_) || OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null schema checker", K(schema_checker_), K(params_.allocator_), K(ret));
  } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(tenant_id), K(table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_part_infos(*table_schema,
                                                      *params_.allocator_,
                                                      part_infos,
                                                      subpart_infos,
                                                      part_ids,
                                                      subpart_ids))) {
    LOG_WARN("failed to get part infos", K(ret));
  } else if (OB_FAIL(table_info.set_all_part_infos(part_infos))) {
    LOG_WARN("failed to set part infos", K(ret));
  } else if (OB_FAIL(table_info.set_all_subpart_infos(subpart_infos))) {
    LOG_WARN("failed to set subpart infos", K(ret));
  } else if (NULL == part_node) {
    table_info.set_part_level(table_schema->get_part_level());
    if (OB_FAIL(table_info.set_part_infos(part_infos))) {
      LOG_WARN("failed to set part infos", K(ret));
    } else if (OB_FAIL(table_info.set_subpart_infos(subpart_infos))) {
      LOG_WARN("failed to set subpart infos", K(ret));
    }
  } else if (is_virtual_table(table_id) &&
             table_schema->get_part_option().get_part_num() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("virtual table should not be partitioned",
        K(table_id), K(table_schema->get_part_option().get_part_num()), K(ret));
  } else if (part_node->num_child_ != 1 || OB_ISNULL(part_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part node should have only one non-null children",
        K(part_node->num_child_), K(part_node->children_[0]), K(ret));
  } else {
    const ParseNode *name_list = part_node->children_[0];
    table_info.set_part_level(table_schema->get_part_level());
    for (int64_t i = 0; OB_SUCC(ret) && i < name_list->num_child_; i++) {
      partition_name.assign_ptr(name_list->children_[i]->str_value_,
                                static_cast<int32_t>(name_list->children_[i]->str_len_));
      if (ObCharset::case_insensitive_equal(partition_name, table_schema->get_table_name())) {
        // do nothing
        partition_name.reset();
      } else if (OB_FAIL(pl::ObDbmsStats::find_selected_part_infos(partition_name,
                                                                   part_infos,
                                                                   subpart_infos,
                                                                   false,
                                                                   table_info.get_part_infos(),
                                                                   table_info.get_subpart_infos(),
                                                                   is_subpart_name))) {
        LOG_WARN("failed to find selected part infos", K(ret));
      } else {
        table_info.set_is_sepcify_subpart(is_subpart_name);
      }
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_statistic_info(const ParseNode *statistic_node,
                                                  ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(statistic_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point", K(statistic_node), K(ret));
  } else if (T_ANALYZE_STATISTICS != statistic_node->type_ ||
             2 != statistic_node->num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not valid statistic node", K(statistic_node->type_),
        K(statistic_node->num_child_), K(ret));
  } else {
    const ParseNode *for_clause_node = statistic_node->children_[0];
    const ParseNode *sample_clause_node = statistic_node->children_[1];
    if (OB_FAIL(resolve_for_clause_info(for_clause_node, analyze_stmt))) {
      LOG_WARN("failed to resolve for clause info", K(ret));
    } else if (OB_FAIL(resolve_sample_clause_info(sample_clause_node, analyze_stmt))) {
      LOG_WARN("failed to resolve sample clause info", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_for_clause_info(const ParseNode *for_clause_node,
                                                   ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObIArray<ObAnalyzeTableInfo> &tables = analyze_stmt.get_tables();
  bool is_hist_subpart = false;
  if (OB_ISNULL(schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null schema checker", K(schema_checker_), K(ret));
  } else if (NULL == for_clause_node) {
    //there can be multi tables in mysql mode without for clause
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_FAIL(schema_checker_->get_table_schema(analyze_stmt.get_tenant_id(), tables.at(i).get_table_id(), table_schema))) {
        LOG_WARN("failed to get table schema", K(analyze_stmt.get_tenant_id()), K(tables.at(i).get_table_id()), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null table schema", K(ret));
      } else if (OB_FAIL(pl::ObDbmsStats::set_default_column_params(tables.at(i).get_column_params()))) {
        LOG_WARN("failed to set default column params", K(ret));
      } else {
        if (share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
          is_hist_subpart = (is_range_part(table_schema->get_sub_part_option().get_part_func_type()) ||
                            is_list_part(table_schema->get_sub_part_option().get_part_func_type()));
        }
        tables.at(i).set_gather_subpart_hist(is_hist_subpart);
      }
    }
  } else if (OB_FAIL(schema_checker_->get_table_schema(analyze_stmt.get_tenant_id(), tables.at(0).get_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(analyze_stmt.get_tenant_id()), K(tables.at(0).get_table_id()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    if (share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
      is_hist_subpart = (is_range_part(table_schema->get_sub_part_option().get_part_func_type()) ||
                        is_list_part(table_schema->get_sub_part_option().get_part_func_type()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < for_clause_node->num_child_; i++) {
      if (OB_FAIL(resolve_for_clause_element(for_clause_node->children_[i],
                                             is_hist_subpart,
                                             analyze_stmt))) {
        LOG_WARN("failed to resolve for clause element", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_for_clause_element(const ParseNode *for_clause_node,
                                                      const bool is_hist_subpart,
                                                      ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> all_for_col;
  ObAnalyzeTableInfo &table_info = analyze_stmt.get_tables().at(0);
  if (OB_ISNULL(for_clause_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null parse node", K(ret));
  } else if (T_ANALYZE_TABLE == for_clause_node->type_) {
    analyze_stmt.set_statistic_scope(StatisticType::TableStatistics);
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("analyze table not supported yet", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "analyze table");
  } else if (T_FOR_ALL == for_clause_node->type_) {
    bool use_size_auto = false;
    if (OB_FAIL(pl::ObDbmsStats::parser_for_all_clause(for_clause_node,
                                                       table_info.get_column_params(),
                                                       use_size_auto))) {
      LOG_WARN("failed to resolve for all clause", K(ret));
    } else {
      table_info.set_gather_subpart_hist(!use_size_auto || (use_size_auto && is_hist_subpart));
    }
  } else if (T_FOR_COLUMNS == for_clause_node->type_) {
    if (OB_FAIL(pl::ObDbmsStats::parser_for_columns_clause(for_clause_node,
                                                           table_info.get_column_params(),
                                                           all_for_col))) {
      LOG_WARN("failed to parser for columns clause", K(ret));
    } else {
      table_info.set_gather_subpart_hist(true);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(for_clause_node->type_), K(ret));
  }
  return ret;
}

int ObAnalyzeStmtResolver::resolve_sample_clause_info(const ParseNode *sample_clause_node,
                                                      ObAnalyzeStmt &analyze_stmt)
{
  int ret = OB_SUCCESS;
  if (NULL == sample_clause_node) {
    /*do nothing*/
  } else if (T_ANALYZE_SAMPLE_INFO != sample_clause_node->type_ ||
             2 != sample_clause_node->num_child_){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sample clause node", K(sample_clause_node->type_),
        K(sample_clause_node->num_child_), K(ret));
  } else {
    ObAnalyzeSampleInfo sample_info;
    sample_info.is_sample_ = true;
    const ParseNode *row_node = sample_clause_node->children_[0];
    const ParseNode *sample_option_node = sample_clause_node->children_[1];
    if (OB_ISNULL(row_node) || OB_ISNULL(sample_option_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null parse node", K(row_node), K(sample_option_node), K(ret));
    } else if (sample_info.sample_type_ == SampleType::PercentSample &&
               (sample_info.sample_value_ < 1 || sample_info.sample_value_ > 99)) {
      ret = OB_ERR_INVALID_PERCENTAGE;
      LOG_WARN("invalid percenate", K(ret), K(sample_info.sample_value_));
    } else {
      sample_info.set_is_block_sample(false);
      if (0 == sample_option_node->value_) {
        // TODO link.zt should I use block sampling?
        sample_info.set_rows(row_node->value_);
      } else {
        sample_info.set_percent(row_node->value_);
      }
      analyze_stmt.set_sample_info(sample_info);
    }
  }
  return ret;
}

int ObAnalyzeStmtResolver::get_bucket_size(const ParseNode *node,
                                           int64_t &bucket_num)
{
  int ret = OB_SUCCESS;
  if (NULL == node) {
    // do nothing
  } else if (T_INT != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", K(ret));
  } else {
    bucket_num = node->value_;
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
