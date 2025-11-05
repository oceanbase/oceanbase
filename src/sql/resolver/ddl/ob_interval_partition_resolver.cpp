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
#include "sql/resolver/ddl/ob_interval_partition_resolver.h"

#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
using namespace share::schema;
namespace sql
{

int ObIntervalPartitionResolver::resolve(const ParseNode &parse_tree)
{
  UNUSED(parse_tree);
  return OB_SUCCESS;
}

int ObIntervalPartitionResolver::resolve_interval_clause(
  ObPartitionedStmt *stmt,
  ParseNode *node,
  ObTableSchema &table_schema,
  common::ObSEArray<ObRawExpr*, 8> &range_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KR(ret), K(stmt), K(node));
  } else if (node->num_child_ > ObDDLResolver::RANGE_INTERVAL_NODE) {
    /* interval info record in 6th param */
    ParseNode *interval_node = node->children_[ObDDLResolver::RANGE_INTERVAL_NODE];
    ObRawExpr *transition_expr = range_exprs.at(range_exprs.count() - 1);
    ObRawExpr *interval_value = NULL;
    if (OB_ISNULL(transition_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null transition expr", KR(ret));
    } else if (NULL == interval_node) {
      // no nothing
    } else if (OB_FAIL(resolve_interval_expr_low_(params_,
                                                  interval_node,
                                                  table_schema,
                                                  transition_expr,
                                                  interval_value))) {
      LOG_WARN("failed to resolve interval expr low", KR(ret));
    } else {
      stmt->set_interval_expr(interval_value);
    }
  }
  return ret;
}

int ObIntervalPartitionResolver::resolve_set_interval(
  ObAlterTableStmt &stmt,
  const ParseNode &node,
  const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret), K(session_info_->get_effective_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_4_2_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.4.2, set interval is not supported",
             KR(ret), KDV(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.4.2, set interval");
  } else if (!table_schema.is_range_part()) {
    ret = OB_ERR_SET_INTERVAL_IS_NOT_LEGAL_ON_THIS_TABLE;
    SQL_RESV_LOG(WARN, "set interval on no range partitioned table", KR(ret));
  } else if (OB_ISNULL(node.children_[0])) {
    /* set interval () */
    if (!table_schema.is_interval_part()) {
      ret = OB_ERR_TABLE_IS_ALREADY_A_RANGE_PARTITIONED_TABLE;
      SQL_RESV_LOG(WARN, "table alreay a range partitionted table", KR(ret));
    } else {
      /* 设置为interval -> range */
      stmt.get_alter_table_arg().alter_part_type_ = ObAlterTableArg::INTERVAL_TO_RANGE;
    }
  } else if (OB_INVALID_ID != table_schema.get_tablegroup_id()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("set interval in tablegroup not allowed", KR(ret), K(table_schema.get_tablegroup_id()));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "add/drop table partition in 2.0 tablegroup");
  } else if (table_schema.is_interval_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("change interval range in inteval table not support yet", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change interval range in inteval table");
  } else if (OB_FAIL(table_schema.check_support_interval_part())) {
    LOG_WARN("fail to check support interval part", KR(ret));
  } else if (OB_ISNULL(table_schema.get_part_array())
             || OB_UNLIKELY(0 == table_schema.get_part_option().get_part_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range part array is null or part_num is 0", KR(ret));
  } else {
    ObRawExpr *expr = NULL;

    /* set interval (expr) */
    if (false == table_schema.is_interval_part()) {
      /* 设置为 range -> interval */
      stmt.get_alter_table_arg().alter_part_type_ = ObAlterTableArg::SET_INTERVAL;
    } else {
      stmt.get_alter_table_arg().alter_part_type_ = ObAlterTableArg::SET_INTERVAL;
    }
    const ObRowkey *rowkey_last =
        &table_schema.get_part_array()[table_schema.get_part_option().get_part_num()- 1]
        ->get_high_bound_val();

    if (OB_SUCC(ret) && NULL != rowkey_last) {
      if (rowkey_last->get_obj_cnt() != 1) {
        ret = OB_ERR_INTERVAL_CLAUSE_HAS_MORE_THAN_ONE_COLUMN;
        SQL_RESV_LOG(WARN, "interval clause has more then one column", KR(ret));
      } else if (OB_ISNULL(rowkey_last->get_obj_ptr())) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "row key is null", KR(ret));
      } else {
        ObObj transition_value = rowkey_last->get_obj_ptr()[0];
        ObItemType item_type;
        ObConstRawExpr *transition_expr = NULL;
        if (false == ObResolverUtils::is_valid_oracle_interval_data_type(
                                      transition_value.get_type(), item_type)) {
          ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
          SQL_RESV_LOG(WARN, "invalid interval column data type", KR(ret));
        }
        OZ (params_.expr_factory_->create_raw_expr(item_type, transition_expr));
        OX (transition_expr->set_value(transition_value));
        OZ (resolve_interval_expr_low_(params_,
                                       node.children_[0],
                                       table_schema,
                                       transition_expr,
                                       expr));
        OX (stmt.set_transition_expr_for_set_interval(transition_expr));
        OX (stmt.set_interval_expr_for_set_interval(expr));
      }
    }
  }
  return ret;
}

int ObIntervalPartitionResolver::resolve_interval_and_transition(
  const ParseNode &node,
  ObAlterTableStmt &alter_stmt,
  const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObRawExpr *part_func_expr = NULL;
  ObRawExpr *interval_expr = NULL;
  ObRawExpr *transition_expr = NULL;
  if (3 != node.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children num is unexpected", KR(ret), K(node.num_child_));
  } else if (!table_schema.is_interval_part()) {
    // for non-interval table add partition
    if (OB_NOT_NULL(node.children_[1]) || OB_NOT_NULL(node.children_[2])) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("interval and transition clause can only be used on interval table", KR(ret));
    } else {
      // skip, no interval or transition clause
    }
  } else {
    // for interval table add partition
    if (OB_ISNULL(node.children_[1]) || OB_ISNULL(node.children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("interval and transition clause must be specified for interval table add partition", KR(ret));
    } else if (OB_FAIL(alter_stmt.get_part_fun_exprs().at(0, part_func_expr))) {
      LOG_WARN("failed to get part func expr", KR(ret));
    } else if (OB_ISNULL(part_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part func expr is null", KR(ret));
    } else if (OB_ISNULL(node.children_[2]->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transition expr is null", KR(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params_,
                                                                           *(node.children_[2]->children_[0]),
                                                                           "dummy",
                                                                           PARTITION_FUNC_TYPE_RANGE_COLUMNS,
                                                                           *part_func_expr,
                                                                           transition_expr,
                                                                           false))) {
      LOG_WARN("failed to resolve partition range value expr", KR(ret));
    } else if (OB_FAIL(resolve_interval_expr_low_(params_,
                                                  node.children_[1],
                                                  table_schema,
                                                  transition_expr,
                                                  interval_expr))) {
      LOG_WARN("failed to resolve interval expr", KR(ret));
    } else {
      alter_stmt.set_interval_expr_for_add_partition(interval_expr);
      alter_stmt.set_transition_expr_for_add_partition(transition_expr);
    }
  }

  return ret;
}

int ObIntervalPartitionResolver::resolve_interval_node_(
  ObResolverParams &params,
  ParseNode *interval_node,
  common::ColumnType &col_dt,
  int64_t precision,
  int64_t scale,
  ObRawExpr *&interval_value_expr_out)
{
  int ret = OB_SUCCESS;
  ParseNode * expr_node = NULL;

  CK (OB_NOT_NULL(interval_node));
  OX (expr_node = interval_node->children_[0]);
  CK (OB_NOT_NULL(expr_node));
  CK (OB_NOT_NULL(params.allocator_));
  if (OB_SUCC(ret)) {
    if (expr_node->type_ == T_NULL) {
      ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
    } else {
      ObRawExpr *interval_value_expr = NULL;
      OZ (ObResolverUtils::resolve_partition_range_value_expr(params, *expr_node, "interval_part",
                                             PARTITION_FUNC_TYPE_RANGE_COLUMNS,
                                             interval_value_expr, false, true));
      if (ret == OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED) {
        ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
      } else if (!OB_SUCC(ret)) {
        ret = OB_WRONG_COLUMN_NAME;
        LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int>(interval_node->str_len_),
                       interval_node->str_value_);
      } else {
        common::ObObjType expr_type = interval_value_expr->get_data_type();
        switch (col_dt) {
          case ObIntType:
          case ObFloatType:
          case ObDoubleType:
          case ObNumberFloatType:
          case ObNumberType: {
            if (expr_type != ObIntType && expr_type != ObFloatType
               && expr_type != ObNumberType && expr_type != ObDoubleType) {
              ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
              LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), KR(ret));
            } else if (col_dt == ObNumberType) {
              ObAccuracy acc;
              acc.set_precision(precision);
              acc.set_scale(scale);
              interval_value_expr->set_accuracy(acc);
            }
            if (OB_SUCC(ret)) {
              ParamStore dummy_params;
              ObRawExprFactory expr_factory(*(params.allocator_));
              ObObj out_val;
              ObRawExpr *sign_expr = NULL;
              OZ (ObRawExprUtils::build_sign_expr(expr_factory, interval_value_expr, sign_expr));
              OZ (sign_expr->formalize(params.session_info_));
              OZ (ObSQLUtils::calc_simple_expr_without_row(params.session_info_,
                                                          sign_expr, out_val,
                                                          &dummy_params, *(params.allocator_)));

              if (OB_FAIL(ret)) {
                // do nothing
              } else if (out_val.is_negative_number()) {
                ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
                LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), KR(ret));
              }
            }
            break;
          }
          case ObDateTimeType:
          case ObTimestampNanoType: {
            if (expr_type != ObIntervalYMType && expr_type != ObIntervalDSType) {
              ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
              LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), KR(ret));
            } else {
              ParamStore dummy_params;
              ObRawExprFactory expr_factory(*(params.allocator_));
              ObObj interval_value;
              if (OB_FAIL(ObSQLUtils::calc_simple_expr_without_row(params.session_info_,
                                                                   interval_value_expr,
                                                                   interval_value,
                                                                   &dummy_params,
                                                                   *(params.allocator_)))) {
                LOG_WARN("fail to calc simple expr without row", KR(ret), KPC(interval_value_expr));
              } else {
                bool is_negative = (ObIntervalYMType == expr_type && interval_value.get_interval_ym().is_negative())
                                || (ObIntervalDSType == expr_type && interval_value.get_interval_ds().is_negative());
                if (is_negative) {
                  ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
                  LOG_WARN("fail to check interval expr datatype", KR(ret), K(expr_type), K(col_dt));
                }
              }
            }
            break;
          }
          default: {
            ret = OB_ERR_INTERVAL_EXPR_NOT_CORRECT_TYPE;
            LOG_WARN("fail to check interval expr datatype", K(expr_type), K(col_dt), KR(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        interval_value_expr_out = interval_value_expr;
      }
    }
  }

  return ret;
}

int ObIntervalPartitionResolver::resolve_interval_expr_low_(
  ObResolverParams &params,
  ParseNode *interval_node,
  const share::schema::ObTableSchema &table_schema,
  ObRawExpr *transition_expr,
  ObRawExpr *&interval_value)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2 *col_schema = NULL;
  common::ColumnType col_dt = ObNullType;

  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session_info_ is null", KR(ret));
  } else if (OB_ISNULL(transition_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transition_expr is null", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret), K(session_info_->get_effective_tenant_id()));
  } else if (tenant_data_version < DATA_VERSION_4_4_2_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.4.2, interval partition is not supported",
            KR(ret), KDV(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "version is less than 4.4.2, interval partition");
  }

  /* 1. interval 分区只支持一个分区键 否则 ORA-14750*/
  if (OB_SUCC(ret)) {
    if (table_schema.get_partition_key_column_num() > 1) {
      ret = OB_ERR_INTERVAL_CLAUSE_HAS_MORE_THAN_ONE_COLUMN;
      SQL_RESV_LOG(WARN, "interval clause has more then one column", KR(ret));
    }
  }

  /* 2. interval 分区列只支持数据类型： number, date, float, timestamp。 否则 ORA-14751 */
  if (OB_SUCC(ret)) {
    uint64_t col_id = OB_INVALID_ID;
    ObItemType item_type;
    const ObPartitionKeyInfo &part_key_info = table_schema.get_partition_key_info();

    OZ (part_key_info.get_column_id(0, col_id));
    CK (OB_NOT_NULL(col_schema = table_schema.get_column_schema(col_id)));
    if (OB_SUCC(ret)) {
      col_dt = col_schema->get_data_type();
      if (ObFloatType == col_dt || ObDoubleType == col_dt) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support float or double as interval partition column", KR(ret), K(col_dt));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "interval partition with float or double type partition column");
      } else if (false == ObResolverUtils::is_valid_oracle_interval_data_type(col_dt, item_type)) {
        ret = OB_ERR_INVALID_DATA_TYPE_INTERVAL_TABLE;
        SQL_RESV_LOG(WARN, "invalid interval column data type", KR(ret), K(col_dt));
      }
    }
  }
  /* 3. 最大分区不能是maxvalue。否则 ORA-14761 */
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret) && transition_expr->get_data_type() == ObMaxType) {
      ret = OB_ERR_MAXVALUE_PARTITION_WITH_INTERVAL;
      SQL_RESV_LOG(WARN, "interval with maxvalue ", KR(ret), K(table_schema.get_table_name()));
    }
  }
  /* 4. 检查inteval的表达式
    4.1 检查是否是立即数，1+1 不算
    4.2 expr的类型是否和col匹配 否则 ORA-14752
  */
  CK (OB_NOT_NULL(col_schema));
  OZ (resolve_interval_node_(params,
                             interval_node,
                             col_dt,
                             col_schema->get_accuracy().get_precision(),
                             col_schema->get_accuracy().get_scale(),
                             interval_value));

  /* 5. check support interval part */
  if (FAILEDx(table_schema.check_support_interval_part())) {
    LOG_WARN("fail to check support interval part", KR(ret));
  }

  return ret;
}

} // end namespace sql
} // end namespace oceanbase
