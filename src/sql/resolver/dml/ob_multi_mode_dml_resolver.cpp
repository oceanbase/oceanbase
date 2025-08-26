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
#include "share/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_autoincrement_param.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_multi_mode_dml_resolver.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_info_extractor.h"
#include "sql/resolver/dml/ob_view_table_resolver.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/parse_malloc.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/dml/ob_default_value_utils.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_routine_info.h"
#include "share/ob_get_compat_mode.h"
#include "sql/ob_sql_utils.h"
#include "lib/oblog/ob_trace_log.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"
#include "pl/ob_pl_stmt.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "objit/expr/ob_iraw_expr.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/parser/ob_item_type_str.h"
#include "sql/printer/ob_select_stmt_printer.h"
#include "observer/virtual_table/ob_table_columns.h"
#include "share/ob_lob_access_utils.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/stat/ob_opt_ds_stat.h"
#include "lib/udt/ob_udt_type.h"
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_inlist_resolver.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "lib/xml/ob_path_parser.h"
#include "sql/engine/expr/ob_json_param_type.h"
#include "sql/engine/expr/ob_expr_json_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace pl;

namespace sql
{

int ObMultiModeDMLResolver::multimode_table_resolve_item(const ParseNode &parse_tree,
                                                        TableItem *&tbl_item,
                                                        ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObDMLStmt *stmt = dml_resolver->get_stmt();
    ObRawExpr *json_doc_expr = NULL;
    TableItem *item = NULL;
    ObJsonTableDef* table_def = NULL;
    ObJtDmlCtx jt_dml_ctx(0, OB_APP_MIN_COLUMN_ID, dml_resolver);
    ObRawExpr *error_expr = NULL;
    ObRawExpr *empty_expr = NULL;
    ParseNode *doc_node = NULL;
    ParseNode *path_node = NULL;
    ParseNode *alias_node = NULL;
    ParseNode *on_err_seq_node = NULL;
    ParseNode *chil_col_node = NULL;
    ParseNode *namespace_node = NULL;
    //store json table column info
    common::ObSEArray<ObDmlJtColDef *, 1, common::ModulePageAllocator, true> json_table_infos;

    if (T_JSON_TABLE_EXPRESSION == parse_tree.type_ && JsnTablePubClause::JT_PARAM_NUM == parse_tree.num_child_) {
      doc_node = parse_tree.children_[JsnTablePubClause::JT_DOC];
      path_node = parse_tree.children_[JsnTablePubClause::JT_PATH];
      alias_node = parse_tree.children_[JsnTablePubClause::JT_ALIAS];
      on_err_seq_node = parse_tree.children_[JsnTablePubClause::JT_ON_ERROR];
      // namespace_node = parse_tree.children_[5];
      chil_col_node =parse_tree.children_[JsnTablePubClause::JT_COL_NODE];
    } else if (T_XML_TABLE_EXPRESSION == parse_tree.type_ && XmlTablePubClause::XT_PARAM_NUM == parse_tree.num_child_ && OB_NOT_NULL(parse_tree.children_[0])) {
      doc_node = parse_tree.children_[XmlTablePubClause::XT_DOC];
      path_node = parse_tree.children_[XmlTablePubClause:: XT_PATH];
      alias_node = parse_tree.children_[XmlTablePubClause::XT_ALIAS];
      on_err_seq_node = parse_tree.children_[XmlTablePubClause::XT_ON_ERROR];
      namespace_node = parse_tree.children_[XmlTablePubClause::XT_NS];
      chil_col_node =parse_tree.children_[XmlTablePubClause::XT_COL_NODE];
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table type not support ot param num mismatch", K(parse_tree.type_), K(parse_tree.num_child_));
    }

    CK (OB_NOT_NULL(parse_tree.children_[0]));
    CK (OB_NOT_NULL(parse_tree.children_[1]));
    CK (OB_NOT_NULL(parse_tree.children_[2]));
    CK (OB_NOT_NULL(parse_tree.children_[3]));

    // json document node
    if (OB_FAIL(ret)) {
    } else if ((OB_ISNULL(stmt) || OB_ISNULL(dml_resolver->allocator_))) {
      ret = OB_NOT_INIT;
      LOG_WARN("resolver isn't init", K(ret), KP(stmt), KP(dml_resolver->allocator_));
    } else {
      OZ (dml_resolver->resolve_sql_expr(*(doc_node), json_doc_expr));
      CK (OB_NOT_NULL(json_doc_expr));
      if (OB_SUCC(ret)) {
        uint64_t extra = json_doc_expr->get_extra();
        extra |= CM_ERROR_ON_SCALE_OVER;
        json_doc_expr->set_extra(extra);
      }
      OZ (json_doc_expr->deduce_type(dml_resolver->session_info_));
    }

    if (OB_SUCC(ret)) {
      char* table_buf = static_cast<char*>(dml_resolver->allocator_->alloc(sizeof(ObJsonTableDef)));
      if (OB_ISNULL(table_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("faield to allocate memory json table def buffer", K(ret));
      } else {
        table_def = static_cast<ObJsonTableDef*>(new (table_buf) ObJsonTableDef());
        table_def->doc_expr_ = json_doc_expr;
        if (T_JSON_TABLE_EXPRESSION == parse_tree.type_) {
          table_def->table_type_ = MulModeTableType::OB_ORA_JSON_TABLE_TYPE;
        } else if (T_XML_TABLE_EXPRESSION == parse_tree.type_) {
          table_def->table_type_ = MulModeTableType::OB_ORA_XML_TABLE_TYPE;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknow table function", K(ret), K(parse_tree.type_));
        }
      }
    }

    // json table alias node
    ObDmlJtColDef* root_col_def = NULL;
    if (OB_SUCC(ret)) {
      ObString alias_name;
      if (lib::is_mysql_mode() && OB_ISNULL(alias_node)) {
        ret = OB_ERR_TABLE_WITHOUT_ALIAS;
        LOG_WARN("table function need alias", K(ret));
      } else if (OB_ISNULL(item = stmt->create_table_item(*(dml_resolver->allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to create table item", K(ret));
      } else if (alias_node) {
        alias_name.assign_ptr(alias_node->str_value_, alias_node->str_len_);
      } else if (OB_FAIL(stmt->generate_json_table_name(*(dml_resolver->allocator_), alias_name))) {
        LOG_WARN("failed to generate json table name", K(ret));
      }
      OX (item->table_name_ = alias_name);
      OX (item->alias_name_ = alias_name);
      OX (item->table_id_ = dml_resolver->generate_table_id());
      OX (item->type_ = TableItem::JSON_TABLE);
      OX (item->json_table_def_ = table_def);
      OX (tbl_item = item);

      OZ (stmt->add_table_item(dml_resolver->session_info_, item));

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(root_col_def = static_cast<ObDmlJtColDef*>(dml_resolver->allocator_->alloc(sizeof(ObDmlJtColDef))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate column def", K(ret));
      } else {
        root_col_def = new (root_col_def) ObDmlJtColDef();
        root_col_def->table_id_ = item->table_id_;
        root_col_def->col_base_info_.col_type_ = NESTED_COL_TYPE;
      }
    }

    // path node process
    if (OB_SUCC(ret)) {
      ObString path_str;
      if (path_node->type_ == T_NULL) {
        path_str = ObString("$");
      } else if (OB_FAIL(json_table_resolve_str_const(*path_node, path_str, dml_resolver, table_def->table_type_))) {
        LOG_WARN("fail to resolve json path", K(ret));
      }
      if (OB_SUCC(ret)
          && table_def->table_type_ == MulModeTableType::OB_ORA_XML_TABLE_TYPE) { // xmltable check xpath
        if (path_str.length() == 0 || path_node->type_ == T_NULL) {
          ret = OB_ERR_LACK_XQUERY_LITERAL;
          LOG_WARN("xmltable need xquery literal", K(ret));
        }
      }

      ObIAllocator& alloc_ref = *(dml_resolver->allocator_);
      if (OB_SUCC(ret) && OB_FAIL(ob_write_string(alloc_ref, path_str, root_col_def->col_base_info_.path_))) {
        LOG_WARN("failed to write string.", K(ret), K(path_str.length()));
      }
    }

    // error node process
    if (OB_SUCC(ret) && T_JSON_TABLE_EXPRESSION == parse_tree.type_) {
      if (on_err_seq_node->num_child_ != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to resolve json table error node count not correct", K(ret), K(on_err_seq_node->num_child_));
      } else {
        ParseNode *error_node = on_err_seq_node->children_[JtOnErrorNode::JT_ERROR_OPT];
        ParseNode *empty_node = on_err_seq_node->children_[JtOnErrorNode::JT_EMPTY_OPT];

        if (OB_ISNULL(error_node) || OB_ISNULL(empty_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error and empty node is null", K(ret));
        } else {
          root_col_def->col_base_info_.on_error_ = error_node->value_;
          // default literal value
          if (error_node->value_ == 2) {
            CK (error_node->num_child_ == 1);
            OZ (dml_resolver->resolve_sql_expr(*(error_node->children_[0]), error_expr));

            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(error_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("error expr is null", K(ret));
            } else {
              uint64_t extra = error_expr->get_extra();
              extra &= ~CM_EXPLICIT_CAST;
              error_expr->set_extra(extra);
            }
            OZ (error_expr->deduce_type(dml_resolver->session_info_));
            OX (root_col_def->error_expr_ = error_expr);
          }
        }

        if (OB_SUCC(ret)) {
          root_col_def->col_base_info_.on_empty_ = empty_node->value_;
          if (empty_node->value_ == 2) {
            CK (empty_node->num_child_ == 1);
            OZ (dml_resolver->resolve_sql_expr(*(empty_node->children_[0]), empty_expr));

            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(empty_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("empty expr is null", K(ret));
            } else {
              uint64_t extra = empty_expr->get_extra();
              extra &= ~CM_EXPLICIT_CAST;
              empty_expr->set_extra(extra);
              root_col_def->col_base_info_.empty_expr_id_ = 0;
            }
            OZ (empty_expr->deduce_type(dml_resolver->session_info_));
            OX (root_col_def->empty_expr_ = empty_expr);
          }
        }
      }
    }
    // xmltable sequence & namespace node
    if (OB_SUCC(ret) && T_XML_TABLE_EXPRESSION == parse_tree.type_) {
      root_col_def->col_base_info_.allow_scalar_ = on_err_seq_node->value_;
      root_col_def->col_base_info_.on_empty_ = 1;
      root_col_def->col_base_info_.on_error_ = 0;
      if (OB_FAIL(xml_table_resolve_xml_namespaces(namespace_node, table_def, dml_resolver->allocator_))) {
        LOG_WARN("fail to resolve xml namespace", K(ret));
      }
    }

    // column node process
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(json_table_infos.push_back(root_col_def))) {
      LOG_WARN("failed to push back column info", K(ret));
    } else if (OB_FAIL(multimode_table_resolve_column_item(*chil_col_node, item, root_col_def,
                                                      json_table_infos, jt_dml_ctx, -1))) {
      LOG_WARN("failed to resovle json table column item", K(ret));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_resolve_column_item(const ParseNode &parse_tree,
                                                                TableItem *table_item,
                                                                ObDmlJtColDef *col_def,
                                                                ObIArray<ObDmlJtColDef*>& json_table_infos,
                                                                ObJtDmlCtx& jt_dml_ctx,
                                                                int32_t parent)
{
  int ret = OB_SUCCESS;
  ObRawExpr *table_expr = NULL;
  ColumnItem *col_item = NULL;
  ParseNode* col_node = NULL;
  int32_t cur_node_id = 0;
  ObDmlJtColDef* cur_col_def = NULL;
  ObJsonTableDef *table_def = NULL;
  ObDMLResolver* dml_resolver = jt_dml_ctx.dml_resolver_;

  if (OB_ISNULL(dml_resolver)
      || OB_ISNULL(table_item)
      || OB_ISNULL(col_def)
      || OB_ISNULL(table_def = table_item->json_table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param input is null", K(dml_resolver), KP(table_item), KP(col_def));
  } else if (OB_UNLIKELY(parse_tree.num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param input is null", K(parse_tree.num_child_));
  } else {
    col_def->col_base_info_.parent_id_ = parent;
    col_def->col_base_info_.id_ = jt_dml_ctx.id_;
    cur_node_id = jt_dml_ctx.id_++;
    if (OB_FAIL(table_def->all_cols_.push_back(&col_def->col_base_info_))) {
      LOG_WARN("json table cols add param fail", K(ret));
    }
  }


  for (int64_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
    cur_col_def = NULL;
    ParseNode* cur_node = parse_tree.children_[i];
    if (OB_ISNULL(cur_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bad column type defination in json table.", K(ret));
    } else {
      JtColType col_type = static_cast<JtColType>(cur_node->value_);
      if (col_type == COL_TYPE_VALUE ||
          col_type == COL_TYPE_QUERY ||
          col_type == COL_TYPE_EXISTS ||
          col_type == COL_TYPE_ORDINALITY ||
          col_type == COL_TYPE_ORDINALITY_XML ||
          col_type == COL_TYPE_QUERY_JSON_COL ||
          col_type == COL_TYPE_VAL_EXTRACT_XML ||
          col_type == COL_TYPE_XMLTYPE_XML) {
        if (OB_FAIL(multimode_table_resolve_regular_column(*cur_node, table_item, cur_col_def, json_table_infos, jt_dml_ctx, cur_node_id))) {
          LOG_WARN("resolve column defination in json table failed.", K(ret), K(cur_node->value_));
        } else if (OB_ISNULL(cur_col_def)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current col def is null", K(ret), K(cur_col_def));
        } else if (OB_FAIL(col_def->regular_cols_.push_back(cur_col_def))) {
          LOG_WARN("failed to store column defination.", K(ret), K(cur_node->value_));
        }
      } else if (col_type == NESTED_COL_TYPE) {
        if (OB_FAIL(json_table_resolve_nested_column(*cur_node, table_item, cur_col_def, json_table_infos, jt_dml_ctx, cur_node_id))) {
          LOG_WARN("resolve column defination in json table failed.", K(ret), K(cur_node->value_));
        } else if (OB_ISNULL(cur_col_def)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current col def is null", K(ret), KP(cur_col_def));
        } else if (lib::is_oracle_mode() && OB_FAIL(ObMultiModeDMLResolver::json_table_check_dup_path(col_def->nested_cols_, cur_col_def->col_base_info_.path_))) {
          LOG_WARN("failed to check dup path.", K(ret), K(cur_node->value_));
        } else if (OB_FAIL(col_def->nested_cols_.push_back(cur_col_def))) {
          LOG_WARN("failed to store column defination.", K(ret), K(cur_node->value_));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("bad column type defination in json table.", K(ret), K(cur_node->value_));
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::multimode_table_resolve_column_item(const TableItem &table_item,
                                                                const ObString &column_name,
                                                                ColumnItem *&col_item,
                                                                ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  col_item = NULL;
  CK (OB_NOT_NULL(stmt));
  CK (OB_LIKELY(table_item.is_json_table()));
  CK (OB_NOT_NULL(table_item.json_table_def_));
  if (OB_FAIL(ret)) {
  } else {
    col_item = stmt->get_column_item(table_item.table_id_, column_name);
    if (OB_ISNULL(col_item)) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("column not exists", K(column_name), K(ret));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_resolve_column_name_and_path(const ParseNode *name_node,
                                                                         const ParseNode *path_node,
                                                                         ObDMLResolver* dml_resolver,
                                                                         ObDmlJtColDef *col_def,
                                                                         MulModeTableType table_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_node) || OB_ISNULL(path_node) || OB_ISNULL(dml_resolver) || OB_ISNULL(col_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("function input is null", K(ret), KP(name_node), KP(path_node), KP(dml_resolver), KP(col_def));
  } else if (path_node->type_ == T_NULL && path_node->value_ != 0) {
    ret = OB_ERR_PATH_EXPRESSION_NOT_LITERAL;
    LOG_WARN("failed to resolve json column as path is null", K(ret));
  } else if (OB_ISNULL(name_node->str_value_) || name_node->str_len_ == 0 ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to resolve json column as name node is null", K(ret));
  } else if (path_node->type_ != T_NULL
              && (path_node->str_len_ > 0 && OB_NOT_NULL(path_node->str_value_))) {
    if ((path_node->type_ == T_CHAR || path_node->type_ == T_VARCHAR)
          && OB_FAIL(json_table_resolve_str_const(*path_node, col_def->col_base_info_.path_, dml_resolver, table_type))) {
      LOG_WARN("fail to resolve path const", K(ret));
    } else if (lib::is_mysql_mode()) {
      (const_cast<ParseNode *>(path_node))->type_ = T_CHAR;
      if (OB_FAIL(json_table_resolve_str_const(*path_node, col_def->col_base_info_.path_, dml_resolver, table_type))) {
        LOG_WARN("fail to resolve path const in mysql", K(ret));
      }
    } else if (((table_type == OB_ORA_JSON_TABLE_TYPE && *path_node->str_value_ != '$' && path_node->value_ != 1))
                && OB_FAIL(ObMultiModeDMLResolver::json_table_make_json_path(*path_node, dml_resolver->allocator_, col_def->col_base_info_.path_, table_type))) {
      LOG_WARN("failed to make json path", K(ret));
    } else if (table_type == OB_ORA_JSON_TABLE_TYPE && path_node->type_ == T_IDENT && path_node->is_input_quoted_ == 1) {
      ret = OB_ERR_INVALID_IDENTIFIER_JSON_TABLE;
      LOG_WARN("invalid identifier used for path expression in JSON_TABLE", K(ret), K(path_node->type_));
    }
  } else if (path_node->type_ == T_NULL
             && OB_FAIL(ObMultiModeDMLResolver::json_table_make_json_path(*name_node, dml_resolver->allocator_, col_def->col_base_info_.path_, table_type))) {
    LOG_WARN("failed to make json path by name", K(ret));
  } else if (path_node->type_  == T_OBJ_ACCESS_REF
             && OB_FAIL(ObMultiModeDMLResolver::json_table_make_json_path(*path_node, dml_resolver->allocator_, col_def->col_base_info_.path_, table_type))) {
    LOG_WARN("failed to make json path by lists", K(ret));
  } else if (table_type == MulModeTableType::OB_ORA_XML_TABLE_TYPE && col_def->col_base_info_.path_.length() == 0) {
    ret = OB_ERR_LACK_XQUERY_LITERAL;
    LOG_WARN("xmltable need xquery literal", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (lib::is_mysql_mode() && (name_node->str_value_[name_node->str_len_ - 1] == ' ')) {
      ret = OB_WRONG_COLUMN_NAME;
      LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int32_t>(name_node->str_len_), name_node->str_value_);
    } else {
      col_def->col_base_info_.col_name_.assign_ptr(name_node->str_value_, name_node->str_len_);
      col_def->col_base_info_.is_name_quoted_ = name_node->is_input_quoted_;
    }

  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_resolve_regular_column(const ParseNode &parse_tree,
                                                                   TableItem *table_item,
                                                                   ObDmlJtColDef *&col_def,
                                                                   ObIArray<ObDmlJtColDef*>& json_table_infos,
                                                                   ObJtDmlCtx& jt_dml_ctx,
                                                                   int32_t parent)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  void* buf = NULL;
  ObRawExpr *error_expr = NULL;
  ObRawExpr *empty_expr = NULL;
  ObJsonTableDef* table_def = NULL;
  ObDmlJtColDef* root_col_def = NULL;
  ObIAllocator * allocator = nullptr;
  ObDMLResolver* dml_resolver = jt_dml_ctx.dml_resolver_;

  JtColType col_type = static_cast<JtColType>(parse_tree.value_);
  for (size_t i = 0; OB_SUCC(ret) && i < parse_tree.num_child_; ++i) {
    if (OB_ISNULL(parse_tree.children_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("json table param should not be null", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_item) || OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("json table table item should not be null", KP(table_item), KP(col_def), KP(dml_resolver), K(ret));
  } else if (OB_FALSE_IT(allocator = dml_resolver->allocator_)) {
  } else if (OB_ISNULL(table_def = table_item->json_table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table def is null", K(ret), KP(table_def));
  } else if (OB_FAIL(ObMultiModeDMLResolver::multimode_table_get_column_by_id(table_item->table_id_, root_col_def, json_table_infos))) {
    LOG_WARN("internal error find jt column failed", K(ret));
  } else if ((col_type == COL_TYPE_EXISTS && parse_tree.num_child_ != MTableParamNum::JS_EXIST_COL_PARAM_NUM) ||
             ((col_type == COL_TYPE_VALUE
                || col_type == COL_TYPE_VAL_EXTRACT_XML
                || col_type == COL_TYPE_XMLTYPE_XML) && parse_tree.num_child_ != MTableParamNum::XML_EXTRACT_COL_PARAM_NUM) ||
             ((col_type == COL_TYPE_QUERY
                || col_type == COL_TYPE_QUERY_JSON_COL) && parse_tree.num_child_ != MTableParamNum::JS_QUERY_COL_PARAM_NUM) ||
             ((col_type == COL_TYPE_ORDINALITY
                || col_type == COL_TYPE_ORDINALITY_XML) && parse_tree.num_child_ != MTableParamNum::COL_ORDINALITY_PARAM_NUM)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to resolve json table regular column", K(ret), K(parse_tree.num_child_), K(col_type));
  } else {
    void* buf = NULL;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObDmlJtColDef)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      // make coldef node
      col_def = new (buf) ObDmlJtColDef();

      // name node & returning type is 0,1
      const ParseNode* name_node = parse_tree.children_[JtColClause::JT_COL_NAME];
      const ParseNode* return_type = parse_tree.children_[JtColClause::JT_COL_TYPE];
      col_def->col_base_info_.res_type_ = return_type->value_;

      if (col_type == COL_TYPE_ORDINALITY || col_type == COL_TYPE_ORDINALITY_XML) {
        if (OB_ISNULL(name_node->str_value_) || name_node->str_len_ == 0 ) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("failed to resolve json column as name node is null", K(ret));
        } else {
          col_def->col_base_info_.col_name_.assign_ptr(name_node->str_value_, name_node->str_len_);
          col_def->col_base_info_.is_name_quoted_ = name_node->is_input_quoted_;
          col_def->col_base_info_.col_type_ = col_type;
          col_def->col_base_info_.is_name_quoted_ = name_node->is_input_quoted_;
        }
      } else if (col_type == COL_TYPE_EXISTS) {
        const ParseNode* path_node = parse_tree.children_[JtColClause::JT_EXISTS_COL_PATH];
        const ParseNode* on_err_node = parse_tree.children_[JtColClause::JT_EXISTS_COL_ON_ERR];
        const ParseNode* truncate_node = parse_tree.children_[JtColClause::JT_COL_TRUNCATE];

        if (OB_FAIL(multimode_table_resolve_column_name_and_path(name_node, path_node, dml_resolver, col_def, table_item->json_table_def_->table_type_))) {
          LOG_WARN("failed to resolve json column name node or path node", K(ret));
        } else if (on_err_node->num_child_ != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to resolve column node as error empty node count not correct",
                    K(ret), K(on_err_node->num_child_));
        } else {
          const ParseNode* error_node = on_err_node->children_[JtOnErrorNode::JT_ERROR_OPT];
          const ParseNode* empty_node = on_err_node->children_[JtOnErrorNode::JT_EMPTY_OPT];

          if (OB_ISNULL(error_node) || OB_ISNULL(empty_node)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("json table exist err node is null", K(ret));
          } else {
            col_def->col_base_info_.on_error_ = error_node->value_;
            col_def->col_base_info_.on_empty_ = empty_node->value_;
            col_def->col_base_info_.truncate_ = truncate_node->value_;
            col_def->col_base_info_.col_type_ = COL_TYPE_EXISTS;

            int jt_on_error = root_col_def->col_base_info_.on_error_;
            if (col_def->col_base_info_.on_error_ == 3 && jt_on_error == 0) {
              col_def->col_base_info_.on_error_ = 2;
            }
          }
        }
      } else if (col_type == COL_TYPE_QUERY || col_type == COL_TYPE_QUERY_JSON_COL) {
        const ParseNode* scalar_node = parse_tree.children_[JtColClause::JT_QUERY_COL_SCALAR];
        const ParseNode* wrapper_node = parse_tree.children_[JtColClause::JT_QUERY_COL_WRAPPER];
        const ParseNode* path_node = parse_tree.children_[JtColClause::JT_QUERY_COL_PATH];
        const ParseNode* on_err_node = parse_tree.children_[JtColClause::JT_QUERY_ON_ERROR];

        const ParseNode* truncate_node = parse_tree.children_[JtColClause::JT_COL_TRUNCATE];


        if (OB_ISNULL(on_err_node->children_[JtOnErrorNode::JT_ERROR_OPT])
            || OB_ISNULL(on_err_node->children_[JtOnErrorNode::JT_EMPTY_OPT])
            || OB_ISNULL(on_err_node->children_[JtOnErrorNode::JT_MISMATCH_OPT])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json table error empty mismatch is null", K(ret));
        } else if (OB_FAIL(multimode_table_resolve_column_name_and_path(name_node, path_node, dml_resolver, col_def, table_item->json_table_def_->table_type_))) {
          LOG_WARN("failed to resolve json column name node or path node", K(ret));
        } else {
          col_def->col_base_info_.col_type_ = COL_TYPE_QUERY;
          col_def->col_base_info_.truncate_ = truncate_node->value_;
          col_def->col_base_info_.format_json_ = true;
          col_def->col_base_info_.allow_scalar_ = scalar_node->value_;
          col_def->col_base_info_.wrapper_ = wrapper_node->value_;
          col_def->col_base_info_.on_empty_ = on_err_node->children_[JtOnErrorNode::JT_EMPTY_OPT]->value_;
          col_def->col_base_info_.on_error_ = on_err_node->children_[JtOnErrorNode::JT_ERROR_OPT]->value_;
          col_def->col_base_info_.on_mismatch_ = on_err_node->children_[JtOnErrorNode::JT_MISMATCH_OPT]->value_;

          // 5 is default
          int jt_on_error = root_col_def->col_base_info_.on_error_;
          if (col_def->col_base_info_.on_error_ == JsnQueryOnError::JSN_QUERY_DEFAULT_ON_ERROR && jt_on_error != JsnQueryOnError::JSN_QUERY_EMPTY_ARRAY_ON_ERROR) {
            if (jt_on_error == JsnQueryOnError::JSN_QUERY_ERROR_ON_ERROR || jt_on_error == JsnQueryOnError::JSN_QUERY_NULL_ON_ERROR) {
              col_def->col_base_info_.on_error_ = jt_on_error;
            }
          }

          // 5 is default
          int jt_on_empty = root_col_def->col_base_info_.on_empty_;
          if (col_def->col_base_info_.on_empty_ == JsnQueryOnEmpty::JSN_QUERY_DEFAULT_ON_EMPTY && jt_on_error != JsnQueryOnEmpty::JSN_QUERY_EMPTY_ARRAY_ON_EMPTY) {
            if (jt_on_empty == JsnQueryOnEmpty::JSN_QUERY_ERROR_ON_EMPTY || jt_on_empty == JsnQueryOnEmpty::JSN_QUERY_NULL_ON_EMPTY) {
              col_def->col_base_info_.on_empty_ = jt_on_empty;
            }
          }
        }
      } else { // COL_TYPE_VALUE
        const ParseNode* trunc_node = parse_tree.children_[JtColClause::JT_VAL_TRUNCATE];
        const ParseNode* path_node = parse_tree.children_[JtColClause::JT_VAL_PATH];
        col_def->col_base_info_.col_type_ = col_type;
        col_def->col_base_info_.truncate_ = trunc_node->value_;

        if (OB_FAIL(multimode_table_resolve_column_name_and_path(name_node, path_node, dml_resolver, col_def, table_item->json_table_def_->table_type_))) {
          LOG_WARN("failed to resolve json column name node or path node", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        ColumnItem *col_item = NULL;
        common::ObDataType data_type;
        if (OB_FAIL(ObMultiModeDMLResolver::json_table_check_dup_name(table_def,
                                                                              col_def->col_base_info_.col_name_,
                                                                              exists))) {
          LOG_WARN("json table check dup name fail", K(ret));
        } else if (exists) {
          ret = OB_NON_UNIQ_ERROR;
          LOG_WARN("column in json table is ambiguous", K(col_def->col_base_info_.col_name_));
        } else if (OB_FAIL(multimode_table_resolve_column_type(*return_type,
                                                          static_cast<int>(col_type),
                                                          data_type,
                                                          col_def,
                                                          dml_resolver->session_info_))) {
          LOG_WARN("failed to resolve json column type.", K(ret));
        } else if (table_item->json_table_def_->table_type_
                                  == MulModeTableType::OB_ORA_XML_TABLE_TYPE
                   && OB_FAIL(xml_check_xpath(col_def, data_type, json_table_infos, dml_resolver->allocator_))) {
          LOG_WARN("fail to check xpath in xmltype column", K(ret));
        } else if (OB_FAIL(multimode_table_generate_column_item(table_item,
                                                                data_type,
                                                                *return_type,
                                                                col_def->col_base_info_.col_name_,
                                                                jt_dml_ctx.cur_column_id_,
                                                                col_item,
                                                                dml_resolver))) {
          LOG_WARN("failed to generate json column.", K(ret));
        } else {
          col_def->col_base_info_.parent_id_ = parent;
          col_def->col_base_info_.id_ = jt_dml_ctx.id_++;
          col_item->col_idx_ = table_item->json_table_def_->all_cols_.count();
          OZ (table_item->json_table_def_->all_cols_.push_back(&col_def->col_base_info_));

          col_def->col_base_info_.data_type_ = data_type;
          col_def->col_base_info_.output_column_idx_ = jt_dml_ctx.cur_column_id_++;

          if (OB_FAIL(ret)) {
          } else if (table_def->table_type_ == MulModeTableType::OB_ORA_JSON_TABLE_TYPE
                      && OB_FAIL(ObMultiModeDMLResolver::json_table_check_column_constrain(col_def))) {
            LOG_WARN("failed to check json column constrain.", K(ret));
          } else if (col_type == COL_TYPE_VALUE /*COL_TYPE_VALUE is used by json table, only json table will check on error clause*/
                      || col_type == COL_TYPE_VAL_EXTRACT_XML
                      || col_type == COL_TYPE_XMLTYPE_XML) {
            const ParseNode* on_err_node = parse_tree.children_[XmlTablePubClause::XT_COL_NODE];

            // error default value
            if (OB_ISNULL(on_err_node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("json table error node should not be null", K(ret));
            } else {
              ParseNode* error_node = NULL;
              ParseNode* empty_node = NULL;
              ParseNode *empty_default_value = NULL;
              ParseNode *error_default_value = NULL;
              if (on_err_node->children_[0]->is_input_quoted_ == 1) { // empty error clause order
                empty_node = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_SECOND_OPT];
                empty_default_value = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_SECOND_DEFAULT_VAL];
                error_node = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_FIRST_OPT];
                error_default_value = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_FIRST_DEFAULT_VAL];
              } else {
                empty_node = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_FIRST_OPT];
                empty_default_value = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_FIRST_DEFAULT_VAL];
                error_node = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_SECOND_OPT];
                error_default_value = on_err_node->children_[XTColClauseOpt::XT_COL_ERROR_SECOND_DEFAULT_VAL];
              }
              const ParseNode* mismatch_node = on_err_node->children_[XTColClauseOpt::XT_COL_MISMATCH];
              if (OB_ISNULL(error_node) || OB_ISNULL(empty_node)
                  || OB_ISNULL(mismatch_node)
                  || OB_ISNULL(empty_default_value)
                  || OB_ISNULL(error_default_value)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("error node is null", K(ret));
              } else if (error_node->value_ == JsnValueOnError::JSN_VALUE_LITERAL_ON_ERROR) {
                if (OB_FAIL(dml_resolver->resolve_sql_expr(*(error_default_value), error_expr))) {
                  LOG_WARN("resolver sql expr fail", K(ret));
                } else if (OB_ISNULL(error_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("error expr is null", K(ret));
                } else {
                  uint64_t extra = error_expr->get_extra();
                  extra &= ~CM_EXPLICIT_CAST;
                  error_expr->set_extra(extra);
                }
                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(error_expr->deduce_type(dml_resolver->session_info_))) {
                  LOG_WARN("error expr fail to deduce", K(ret));
                } else {
                  col_item->default_value_expr_ = error_expr;
                }
              }

              if (OB_SUCC(ret)) {
                col_def->col_base_info_.on_error_ = error_node->value_;
                int jt_on_error = root_col_def->col_base_info_.on_error_;

                if (error_node->value_ == JsnValueOnError::JSN_VALUE_DEFAULT_ON_ERROR && jt_on_error != JsnValueOnError::JSN_VALUE_DEFAULT_ON_ERROR) {
                  col_def->col_base_info_.on_error_ = jt_on_error;
                  if (jt_on_error == JsnValueOnError::JSN_VALUE_LITERAL_ON_ERROR) {
                    col_item->default_value_expr_ = root_col_def->error_expr_;
                  }
                }
              }

              // empty default value
              if (OB_FAIL(ret)) {
              } else if (empty_node->value_ == JsnValueOnEmpty::JSN_VALUE_LITERAL_ON_EMPTY) {
                if (OB_FAIL(dml_resolver->resolve_sql_expr(*(empty_default_value), empty_expr))) {
                  LOG_WARN("resolver sql expr fail", K(ret));
                } else {
                  uint64_t extra = empty_expr->get_extra();
                  extra &= ~CM_EXPLICIT_CAST;
                  empty_expr->set_extra(extra);
                }

                if (OB_FAIL(ret)) {
                } else if (OB_FAIL(empty_expr->deduce_type(dml_resolver->session_info_))) {
                  LOG_WARN("error expr fail to deduce", K(ret));
                } else {
                  col_item->default_empty_expr_ = empty_expr;
                }
              }

              if (OB_SUCC(ret)) {
                col_def->col_base_info_.on_empty_ = empty_node->value_;
                int jt_on_empty = root_col_def->col_base_info_.on_empty_;
                if (empty_node->value_ == JsnValueOnEmpty::JSN_VALUE_DEFAULT_ON_EMPTY && jt_on_empty != JsnValueOnEmpty::JSN_VALUE_DEFAULT_ON_EMPTY) {
                  col_def->col_base_info_.on_empty_ = jt_on_empty;
                  if (jt_on_empty == JsnValueOnEmpty::JSN_VALUE_LITERAL_ON_EMPTY) {
                    col_item->default_empty_expr_ = root_col_def->empty_expr_;
                  }
                }
              }

              if (OB_FAIL(ret)) {
              } else if (mismatch_node->num_child_ != JsnValueType::JSN_VALUE_DEFAULT) {
                ret = OB_ERR_INVALID_CLAUSE;
                LOG_USER_ERROR(OB_ERR_INVALID_CLAUSE, "ERROR CLAUSE");
              } else if (OB_ISNULL(mismatch_node->children_[JsnValueType::JSN_VALUE_ERROR]) || OB_ISNULL(mismatch_node->children_[JsnValueType::JSN_VALUE_NULL])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("mismatch node is null", K(ret));
              } else {
                col_def->col_base_info_.on_mismatch_ = mismatch_node->children_[JsnValueType::JSN_VALUE_ERROR]->value_;
                col_def->col_base_info_.on_mismatch_type_ = mismatch_node->children_[JsnValueType::JSN_VALUE_NULL]->value_;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_get_column_by_id(uint64_t table_id,
                                                            ObDmlJtColDef *&col_def,
                                                            ObIArray<ObDmlJtColDef*>& json_table_infos)
{
  int ret = OB_SUCCESS;
  col_def = NULL;
  bool found_id = false;

  for (size_t i = 0; i < json_table_infos.count() && OB_SUCC(ret) && !found_id; ++i) {
    ObDmlJtColDef* tmp_def = json_table_infos.at(i);
    if (OB_ISNULL(tmp_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to valid dml json table column define info", K(ret));
    } else if (tmp_def->table_id_ == table_id) {
      col_def = tmp_def;
      found_id = true;
    }
  }

  if (OB_SUCC(ret)) {
    ret = found_id ? OB_SUCCESS : OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_resolve_column_type(const ParseNode &parse_tree,
                                                                const int column_type,
                                                                ObDataType &data_type,
                                                                ObDmlJtColDef *col_def,
                                                                ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;

  data_type.set_collation_level(CS_LEVEL_IMPLICIT);
  JtColType col_type = static_cast<JtColType>(column_type);
  ObObjType obj_type;
  if (lib::is_oracle_mode()) {
    obj_type = static_cast<ObObjType>(parse_tree.int16_values_[0]);
  } else {
    obj_type = static_cast<ObObjType>(parse_tree.type_);
  }
  if (OB_ISNULL(col_def) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (col_type == COL_TYPE_ORDINALITY || col_type == COL_TYPE_ORDINALITY_XML) {
    data_type.set_int();
    data_type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObInt32Type]);
  } else if (col_type == COL_TYPE_QUERY && obj_type == ObJsonType) {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_WARN("failed to resolve column, not support return json in query column define", K(ret));
  } else if (col_type == COL_TYPE_EXISTS
             || col_type == COL_TYPE_VALUE
             || col_type == COL_TYPE_QUERY
             || col_type == COL_TYPE_QUERY_JSON_COL
             || col_type == COL_TYPE_VAL_EXTRACT_XML) {
    if (OB_UNLIKELY(!ob_is_valid_obj_type(obj_type))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid obj type", K(ret), K(obj_type));
    } else if (lib::is_mysql_mode()) {
      omt::ObTenantConfigGuard tcg(TENANT_CONF(session_info->get_effective_tenant_id()));
      bool convert_real_to_decimal = (tcg.is_valid() && tcg->_enable_convert_real_to_decimal);
      uint64_t tenant_data_version = 0;
      bool enable_mysql_compatible_dates = session_info->is_enable_mysql_compatible_dates();
      if (OB_FAIL(GET_MIN_DATA_VERSION(session_info->get_effective_tenant_id(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (FALSE_IT(enable_mysql_compatible_dates = (enable_mysql_compatible_dates &&
          (tenant_data_version >= DATA_VERSION_4_2_5_0)))) {
      } else if (OB_FAIL(ObResolverUtils::resolve_data_type(parse_tree,
                                                          col_def->col_base_info_.col_name_,
                                                          data_type,
                                                          (OB_NOT_NULL(session_info) && is_oracle_mode()),
                                                          false,
                                                          session_info->get_session_nls_params(),
                                                          session_info->get_effective_tenant_id(),
                                                          enable_mysql_compatible_dates,
                                                          convert_real_to_decimal))) {
        LOG_WARN("resolve data type failed", K(ret), K(col_def->col_base_info_.col_name_));
      } else {
        ObCharsetType charset_type = data_type.get_charset_type();
        ObCollationType coll_type = data_type.get_collation_type();
        if (CS_TYPE_INVALID != coll_type && CHARSET_INVALID != charset_type) {
          data_type.set_collation_type(coll_type);
          data_type.set_charset_type(charset_type);
        } else if (OB_ISNULL(session_info)) { // use connection_collation. for cast('a' as char)
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected collation type", K(ret));
        } else if (OB_FAIL(session_info->get_collation_connection(coll_type))
                    || OB_FAIL(session_info->get_character_set_connection(charset_type))) {
          LOG_WARN("failed to get collation", K(ret));
        } else {
          data_type.set_charset_type(charset_type);
          data_type.set_collation_type(coll_type);
        }
        if (OB_SUCC(ret) && ob_is_json_tc(obj_type)) {
          data_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    } else {
      if (ObNumberType == obj_type
          && parse_tree.int16_values_[2] == -1 && parse_tree.int16_values_[3] == 0) {
        obj_type = ObIntType;
      } else if (ObFloatType == obj_type) {
        // boundaries already checked in calc result type
        if (parse_tree.int16_values_[OB_NODE_CAST_N_PREC_IDX] > OB_MAX_FLOAT_PRECISION) {
          obj_type = ObDoubleType;
        }
      }
      common::ObAccuracy accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[1][obj_type];
      common::ObLengthSemantics length_semantics = parse_tree.length_semantics_;
      accuracy.set_length_semantics(length_semantics);
      ObObjTypeClass dest_tc = ob_obj_type_class(obj_type);

      if (ObExtendTC == dest_tc) {
        ret = OB_ERR_INVALID_CAST_UDT;
        LOG_WARN("invalid CAST to a type that is not a nested table or VARRAY", K(ret));
      } else if (ObStringTC == dest_tc) {
        if (parse_tree.length_semantics_ == LS_DEFAULT) {
          length_semantics = (OB_NOT_NULL(session_info) ?
                session_info->get_actual_nls_length_semantics() : LS_BYTE);
        }
        accuracy.set_full_length(parse_tree.int32_values_[1], length_semantics, true);
      } else if (ObRawTC == dest_tc) {
        accuracy.set_length(parse_tree.int32_values_[1]);
      } else if (ObTextTC == dest_tc || ObJsonTC == dest_tc) {
        accuracy.set_length(parse_tree.int32_values_[1] <= 0 ?
            ObAccuracy::DDL_DEFAULT_ACCURACY[obj_type].get_length() : parse_tree.int32_values_[1]);
      } else if (ObIntervalTC == dest_tc) {
        if (OB_UNLIKELY(!ObIntervalScaleUtil::scale_check(parse_tree.int16_values_[3]) ||
                        !ObIntervalScaleUtil::scale_check(parse_tree.int16_values_[2]))) {
          ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;
          LOG_WARN("Invalid scale.", K(ret), KP(parse_tree.int16_values_[3]), KP(parse_tree.int16_values_[2]));
        } else {
          ObScale scale = (obj_type == ObIntervalYMType) ?
            ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(parse_tree.int16_values_[3]))
            : ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
                static_cast<int8_t>(parse_tree.int16_values_[2]),
                static_cast<int8_t>(parse_tree.int16_values_[3]));
          accuracy.set_scale(scale);
        }
      } else {
        const ObAccuracy &def_acc = ObAccuracy::DDL_DEFAULT_ACCURACY2[1][obj_type];
        if (ObNumberType == obj_type && parse_tree.int16_values_[2] == -1) {
          accuracy.set_precision(parse_tree.int16_values_[2]);
          accuracy.set_scale(parse_tree.int16_values_[3]);
        } else if (ObIntType == obj_type) {
          data_type.set_int();
          accuracy = def_acc;
        } else {
          accuracy.set_precision(parse_tree.int16_values_[2]);
          accuracy.set_scale(parse_tree.int16_values_[3]);
        }
        if (ObDoubleType == obj_type) {
          accuracy.set_accuracy(def_acc.get_precision());
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        data_type.set_accuracy(accuracy);
        data_type.set_obj_type(obj_type);
        ObCollationType coll_type = static_cast<ObCollationType>(parse_tree.int16_values_[OB_NODE_CAST_COLL_IDX]);
        if (CS_TYPE_INVALID != coll_type) {
          data_type.set_collation_type(coll_type);
        } else if (OB_ISNULL(session_info)) { // use connection_collation. for cast('a' as char)
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected collation type", K(ret));
        } else if (OB_FAIL(session_info->get_collation_connection(coll_type))) {
          LOG_WARN("failed to get collation", K(ret));
        } else {
          data_type.set_collation_type(coll_type);
        }
        if (OB_SUCC(ret) && ob_is_json_tc(obj_type)) {
          data_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  } else if (col_type == COL_TYPE_XMLTYPE_XML) { // not check, default returning xml type
    data_type.set_obj_type(ObUserDefinedSQLType);
    data_type.set_subschema_id(ObXMLSqlType);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to resolve regular column type.", K(ret), KP(col_type));
  }
  return ret;
}

int ObMultiModeDMLResolver::multimode_table_generate_column_item(TableItem *table_item,
                                                                const ObDataType &data_type,
                                                                const ParseNode &data_type_node,
                                                                const ObString &column_name,
                                                                int64_t column_id,
                                                                ColumnItem *&col_item,
                                                                ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr *col_expr = NULL;
  ColumnItem column_item;
  sql::ObExprResType result_type;
  if (OB_ISNULL(dml_resolver) || OB_ISNULL(table_item)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("shouldn't be null", KP(dml_resolver), KP(table_item), K(ret));
  } else {
    ObDMLStmt *stmt = dml_resolver->get_stmt();
    CK (OB_NOT_NULL(dml_resolver->params_.expr_factory_));
    CK (OB_LIKELY(table_item->is_json_table()));
    OZ (dml_resolver->params_.expr_factory_->create_raw_expr(T_REF_COLUMN, col_expr));
    CK (OB_NOT_NULL(col_expr));
    OX (col_expr->set_ref_id(table_item->table_id_, column_id));
    OX (result_type.set_meta(data_type.get_meta_type()));
    OX (result_type.set_accuracy(data_type.get_accuracy()));
    OX (col_expr->set_result_type(result_type));
    if (OB_FAIL(ret)) {
    } else if (table_item->get_object_name().empty()) {
        OX (col_expr->set_column_name(column_name));
    } else {
        OX (col_expr->set_column_attr(table_item->get_object_name(), column_name));
    }

    OX (col_expr->set_database_name(table_item->database_name_));
    if (OB_FAIL(ret)) {
    } else if (lib::is_oracle_mode() && ob_is_enumset_tc(col_expr->get_data_type())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support enum set in table function", K(ret));
    } else {
        col_expr->unset_result_flag(NOT_NULL_FLAG);
    }
    OX (column_item.expr_ = col_expr);
    OX (column_item.table_id_ = col_expr->get_table_id());
    OX (column_item.column_id_ = col_expr->get_column_id());
    OX (column_item.column_name_ = col_expr->get_column_name());
    OZ (col_expr->extract_info());
    OZ (stmt->add_column_item(column_item));
    OX (col_item = stmt->get_column_item(stmt->get_column_size() - 1));
    if (ob_is_enumset_tc(data_type.get_obj_type())) {
      ObArray<ObString> type_info_array;
      if (OB_FAIL(ObResolverUtils::resolve_extended_type_info(*(data_type_node.children_[3]), type_info_array))) {
        LOG_WARN("fail to resolve extended type info", K(ret));
      } else if (col_expr->set_enum_set_values(type_info_array)) {
        LOG_WARN("fail to set enum string info", K(ret));
      }
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_table_check_column_constrain(ObDmlJtColDef *col_def)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(col_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col def is null", K(ret));
  } else {
    ObDataType &data_type = col_def->col_base_info_.data_type_;
    ObObjType obj_type = data_type.get_obj_type();
    if (obj_type != ObVarcharType && col_def->col_base_info_.truncate_) {
      ret = OB_ERR_UNSUPPORT_TRUNCATE_TYPE;
      LOG_USER_ERROR(OB_ERR_UNSUPPORT_TRUNCATE_TYPE);
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_table_check_dup_path(ObIArray<ObDmlJtColDef*>& columns, const ObString& column_path)
{
  INIT_SUCC(ret);

  for (size_t i = 0; i < columns.count() && OB_SUCC(ret); ++i) {
    ObDmlJtColDef* col_def = columns.at(i);
    if (OB_ISNULL(col_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col def is null", K(ret));
    } else if (col_def->col_base_info_.path_.length() == column_path.length()) {
      if (0 == memcmp(column_path.ptr(), col_def->col_base_info_.path_.ptr(), column_path.length())) {
        ret = OB_ERR_NESTED_PATH_DISJUNCT;
        LOG_WARN("check nestet path duplicated", K(ret), K(column_path), K(col_def->col_base_info_.path_));
      }
    } else {
      size_t min_len = std::min(col_def->col_base_info_.path_.length(), column_path.length());
      ObString l_str = column_path.length() > min_len ? column_path : col_def->col_base_info_.path_;
      if (0 == memcmp(column_path.ptr(), col_def->col_base_info_.path_.ptr(), min_len)
          && l_str.ptr()[min_len] == '[') {
        ret = OB_ERR_NESTED_PATH_DISJUNCT;
        LOG_WARN("check nestet path duplicated", K(ret), K(column_path), K(col_def->col_base_info_.path_));
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::json_table_check_dup_name(const ObJsonTableDef* table_def,
                                                      const ObString& column_name,
                                                      bool& exists)
{
  int ret = OB_SUCCESS;
  exists = false;
  if (OB_ISNULL(table_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table define is null", K(ret));
  } else {
    for (size_t i = 0; i < table_def->all_cols_.count() && OB_SUCC(ret) && !exists; ++i) {
      const ObJtColBaseInfo *info = table_def->all_cols_.at(i);
      if (OB_ISNULL(info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("info is null", K(ret));
      } else if (info->col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)) {
      } else if (ObCharset::case_compat_mode_equal(info->col_name_, column_name)) {
        exists = true;
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::json_table_resolve_all_column_items(const TableItem &table_item,
                                                                ObIArray<ColumnItem> &col_items,
                                                                ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *table_expr = NULL;
  ObJsonTableDef* jt_def = NULL;

  CK (OB_LIKELY(table_item.is_json_table()));
  CK (OB_NOT_NULL(table_item.json_table_def_));
  CK (OB_NOT_NULL(stmt));

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(jt_def = table_item.json_table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item json_table_def_ is null", K(table_item.json_table_def_));
  } else {
    for (size_t i = 0; OB_SUCC(ret) && i < jt_def->all_cols_.count(); ++i) {
      ObJtColBaseInfo* col_info = jt_def->all_cols_.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col info is null", K(ret), K(i));
      } else if (col_info->col_type_ != NESTED_COL_TYPE) {
        ColumnItem* col_item = stmt->get_column_item_by_id(table_item.table_id_, col_info->output_column_idx_);
        CK (OB_NOT_NULL(col_item));
        if (OB_SUCC(ret) && OB_FAIL(col_items.push_back(*col_item))) {
          LOG_WARN("fail to store col result", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::json_table_path_printer(ParseNode *&tmp_path, ObJsonBuffer &res_str)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(tmp_path) || OB_ISNULL(tmp_path->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("path node should not be null", K(ret));
  } else if (tmp_path->children_[0]->type_ == T_LINK_NODE && tmp_path->children_[0]->value_ == 3) { // [array]
    int64_t num_child = tmp_path->children_[0]->num_child_;
    if (OB_FAIL(res_str.append("["))) {
      LOG_WARN("[ symbol write fail", K(ret));
    }
    for (int64_t i = 0; i < num_child && OB_SUCC(ret); i ++) {
      ParseNode *cur_node_ = tmp_path->children_[0]->children_[i];
      if (i > 0 && cur_node_->value_ != 2) {
        if (OB_FAIL(res_str.append(","))) {
          LOG_WARN(", symbol write fail", K(ret));
        }
      }
      if (OB_SUCC(ret) && cur_node_->value_ == 2) {
        if (OB_FAIL(res_str.append(" to "))) {
          LOG_WARN("to number write fail", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(cur_node_->raw_text_)) {
        if (OB_FAIL(res_str.append(cur_node_->raw_text_, cur_node_->text_len_))) {
          LOG_WARN("raw_text write fail", K(cur_node_->raw_text_), K(cur_node_->text_len_),K(ret));
        }
      } else if (OB_ISNULL(cur_node_->raw_text_)) {
        if (OB_FAIL(res_str.append(cur_node_->str_value_, cur_node_->str_len_))) {
          LOG_WARN("str_value write fail", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(res_str.append("]"))) {
      LOG_WARN("] symbol write fail", K(ret));
    }
    tmp_path = tmp_path->children_[1];
  } else {
    if (OB_FAIL(res_str.append("."))) {
      LOG_WARN("dot symbol write fail", K(ret));
    } else if (tmp_path->children_[0]->is_input_quoted_ == 1 && OB_FAIL(res_str.append("\""))) {
      LOG_WARN("add \" fail in side", K(ret));
    } else if (OB_NOT_NULL(tmp_path->children_[0]->raw_text_)
                && OB_FAIL(res_str.append(tmp_path->children_[0]->raw_text_, tmp_path->children_[0]->text_len_))) {
      LOG_WARN("raw_text write fail", K(ret));
    } else if (OB_ISNULL(tmp_path->children_[0]->raw_text_)) {
      if (OB_FAIL(res_str.append(tmp_path->children_[0]->str_value_, tmp_path->children_[0]->str_len_))) {
        LOG_WARN("str_value write fail", K(ret));
      }
    } else if (tmp_path->children_[0]->is_input_quoted_ == 1 && OB_FAIL(res_str.append("\""))) {
      LOG_WARN("add \" fail in side", K(ret));
    }
    if (tmp_path->type_ == T_FUN_SYS) {
      tmp_path = NULL;
    } else {
      tmp_path = tmp_path->children_[1];
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_table_make_json_path(const ParseNode &parse_tree,
                                                      ObIAllocator* allocator,
                                                      ObString& path_str,
                                                      MulModeTableType table_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (parse_tree.type_  == T_OBJ_ACCESS_REF) {
    ObJsonBuffer* path_buffer = nullptr;
    if (OB_ISNULL(path_buffer = static_cast<ObJsonBuffer*>(allocator->alloc(sizeof(ObJsonBuffer))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (FALSE_IT(path_buffer = new (path_buffer) ObJsonBuffer(allocator))) {
    } else if (OB_FAIL(path_buffer->append("$."))) {
      LOG_WARN("failed to append path start", K(ret));
    } else if (parse_tree.num_child_ != 2
               || OB_ISNULL(parse_tree.children_)
               || OB_ISNULL(parse_tree.children_[0])
               || OB_ISNULL(parse_tree.children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to make path, param not expected", K(ret), K(parse_tree.num_child_),
              KP(parse_tree.children_));
    } else if (OB_FAIL(path_buffer->append(parse_tree.children_[0]->raw_text_,
                                          parse_tree.children_[0]->text_len_))) {
      LOG_WARN("failed to append raw text", K(ret));
    } else {
      ParseNode *tmp_path = parse_tree.children_[1];
      while (OB_SUCC(ret) && OB_NOT_NULL(tmp_path)) {
        if (OB_ISNULL(tmp_path->children_[0])) {
          tmp_path = NULL;
          // do nothing
        } else if (OB_FAIL(ObMultiModeDMLResolver::json_table_path_printer(tmp_path, *path_buffer))) {
            LOG_WARN("failed to print path", K(ret));
        }
      }

      char* path_buf = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(path_buf = static_cast<char*>(allocator->alloc(path_buffer->length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate path buffer", K(ret), K(path_buffer->length()));
      } else {
        MEMCPY(path_buf, path_buffer->ptr(), path_buffer->length());
        path_buf[path_buffer->length()] = 0;
        path_str.assign_ptr(path_buf, strlen(path_buf));
      }
    }
  } else {
    if (table_type == MulModeTableType::OB_ORA_JSON_TABLE_TYPE) {
      char* str_buf = static_cast<char*>(allocator->alloc(parse_tree.text_len_ + 3));
      if (OB_ISNULL(str_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(parse_tree.str_len_));
      } else {
        MEMCPY(str_buf, "$.", 2);
        str_buf[parse_tree.text_len_ + 2] = '\0';
        if (parse_tree.text_len_ > 0
            && (parse_tree.raw_text_[0] == '\'' && parse_tree.raw_text_[parse_tree.text_len_-1] == '\'')) {
          MEMCPY(str_buf + 2, parse_tree.raw_text_ + 1, parse_tree.text_len_ - 1);
          str_buf[parse_tree.text_len_] = '\0';
        } else {
          MEMCPY(str_buf + 2, parse_tree.raw_text_, parse_tree.text_len_);
        }
        path_str.assign_ptr(str_buf, strlen(str_buf));
      }
    } else if (table_type == MulModeTableType::OB_ORA_XML_TABLE_TYPE) {
      if (parse_tree.str_len_ == 0) {
        ret = OB_ERR_LACK_XQUERY_LITERAL;
        LOG_WARN("xmltable need xquery literal", K(ret));
      } else {
        char* str_buf = static_cast<char*>(allocator->alloc(parse_tree.text_len_));
        if (OB_ISNULL(str_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(parse_tree.str_len_));
        } else {
          MEMCPY(str_buf, parse_tree.raw_text_, parse_tree.text_len_);
          for (int64_t i = 0; i < parse_tree.text_len_; i ++) {
            str_buf[i] = toupper(str_buf[i]);
          }
          path_str.assign_ptr(str_buf, parse_tree.text_len_);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("INVALID table function", K(ret), K(table_type));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_table_resolve_nested_column(const ParseNode &parse_tree,
                                                             TableItem *table_item,
                                                             ObDmlJtColDef *&col_def,
                                                             ObIArray<ObDmlJtColDef*>& json_table_infos,
                                                             ObJtDmlCtx& jt_dml_ctx,
                                                             int32_t parent)
{
  int ret = OB_SUCCESS;

  size_t alloc_size = sizeof(ObDmlJtColDef);
  void* buf = NULL;
  ObDMLResolver* dml_resolver = jt_dml_ctx.dml_resolver_;

  if (parse_tree.num_child_ != 2
      || OB_ISNULL(parse_tree.children_[0])
      || OB_ISNULL(parse_tree.children_[1])
      || OB_ISNULL(dml_resolver)
      || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to resolve json column as param num not illegal",
              K(ret), K(parse_tree.num_child_));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_ISNULL(buf = dml_resolver->allocator_->alloc(alloc_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(alloc_size));
  } else {
    col_def = new (buf) ObDmlJtColDef();
    col_def->col_base_info_.col_type_ = NESTED_COL_TYPE;

    ParseNode* path_node = const_cast<ParseNode*>(parse_tree.children_[0]);

    // json table nested path syntax, not quoted:
    // nested path employees[*] columns ( name, job )
    if (path_node->value_ == 2) {
      if (OB_FAIL(ObMultiModeDMLResolver::json_table_make_json_path(*path_node, dml_resolver->allocator_, col_def->col_base_info_.path_, table_item->json_table_def_->table_type_))) {
        LOG_WARN("failed to make json path.", K(ret));
      }
    } else if (OB_ISNULL(path_node->str_value_) || path_node->str_len_ == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("allocate memory failed", K(ret), K(alloc_size));
    } else {
      if (path_node->str_value_[0] == '$') {
        col_def->col_base_info_.path_.assign_ptr(path_node->str_value_, path_node->str_len_);
      } else if (OB_FAIL(ObMultiModeDMLResolver::json_table_make_json_path(*path_node, dml_resolver->allocator_, col_def->col_base_info_.path_, table_item->json_table_def_->table_type_))) {
        LOG_WARN("failed to make json path.", K(ret));
      }
    }

    if (OB_SUCC(ret)
        && OB_FAIL(multimode_table_resolve_column_item(*parse_tree.children_[1],
                    table_item, col_def, json_table_infos, jt_dml_ctx, parent))) {
      LOG_WARN("failed to resolve nested column defination.", K(ret));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_table_resolve_str_const(const ParseNode &parse_tree,
                                                         ObString& path_str,
                                                         ObDMLResolver* dml_resolver,
                                                         MulModeTableType table_type)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObObjParam val;
    char *buf = NULL;
    ObString literal_prefix;
    bool is_paramlize = false;
    ObExprInfo parents_expr_info;
    const ObSQLSessionInfo *session_info = dml_resolver->session_info_;
    const ObLengthSemantics default_length_semantics = (OB_NOT_NULL(session_info) ? session_info->get_actual_nls_length_semantics() : LS_BYTE);
    int64_t server_collation = CS_TYPE_INVALID;
    ObCollationType nation_collation = OB_NOT_NULL(session_info) ? session_info->get_nls_collation_nation() : CS_TYPE_INVALID;
    ObCollationType collation_connection = CS_TYPE_INVALID;
    ObCharsetType character_set_connection = CHARSET_INVALID;
    ObCompatType compat_type = COMPAT_MYSQL57;
    bool enable_mysql_compatible_dates = false;
    if (OB_ISNULL(dml_resolver->params_.expr_factory_) || OB_ISNULL(dml_resolver->params_.session_info_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("resolve status is invalid", K_(dml_resolver->params_.expr_factory), K_(dml_resolver->params_.session_info));
    } else if (OB_FAIL(dml_resolver->params_.session_info_->get_collation_connection(collation_connection))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (OB_FAIL(dml_resolver->params_.session_info_->get_character_set_connection(character_set_connection))) {
      LOG_WARN("fail to get character_set_connection", K(ret));
    } else if (lib::is_oracle_mode() && nullptr == session_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session info is null", K(ret));
    } else if (OB_FAIL(session_info->get_compatibility_control(compat_type))) {
      LOG_WARN("failed to get compat type", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(
      session_info->get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
      LOG_WARN("get sys variables failed", K(ret));
    } else if (OB_FAIL(ObSQLUtils::check_enable_mysql_compatible_dates(
                          dml_resolver->params_.session_info_, false,
                          enable_mysql_compatible_dates))) {
      LOG_WARN("fail to check enable mysql compatible dates", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_const(&parse_tree,
                                                    // stmt_type is only used in oracle mode
                                                    lib::is_oracle_mode() ? session_info->get_stmt_type() : stmt::T_NONE,
                                                    dml_resolver->params_.expr_factory_->get_allocator(),
                                                    collation_connection, nation_collation, TZ_INFO(dml_resolver->params_.session_info_),
                                                    val, is_paramlize,
                                                    literal_prefix,
                                                    default_length_semantics,
                                                    static_cast<ObCollationType>(server_collation),
                                                    &parents_expr_info,
                                                    session_info->get_sql_mode(),
                                                    compat_type,
                                                    enable_mysql_compatible_dates,
                                                    nullptr != dml_resolver->params_.secondary_namespace_))) {
      LOG_WARN("failed to resolve const", K(ret));
    } else if (val.get_string().length() == 0) {
      if (table_type == MulModeTableType::OB_ORA_JSON_TABLE_TYPE) {
        ret = OB_ERR_INVALID_JSON_PATH;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
      } else if (table_type == MulModeTableType::OB_ORA_XML_TABLE_TYPE) {
        ret = OB_ERR_INVALID_XPATH_EXPRESSION;
        LOG_USER_ERROR(OB_ERR_INVALID_XPATH_EXPRESSION);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to resolve json column as name node is null", K(ret));
      }
    } else if (OB_ISNULL(buf = static_cast<char*>(dml_resolver->allocator_->alloc(val.get_string().length())))) { // deep copy str value
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(buf));
    } else {
      MEMCPY(buf, val.get_string().ptr(), val.get_string().length());
      path_str.assign_ptr(buf, val.get_string().length());
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_check_size_obj_access_ref(ParseNode *node)
{
  INIT_SUCC(ret);
  ParseNode *cur_node = node;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node) && (cur_node->type_ == T_OBJ_ACCESS_REF || cur_node->type_ == T_FUN_SYS)) {
    if (OB_ISNULL(cur_node->children_[0])) {
      if (cur_node->num_child_ >= 2) {
        cur_node = cur_node->children_[1];
      } else {
        cur_node = NULL;
      }
    } else if (cur_node->children_[0]->type_ == T_FUN_SYS) {
      if (cur_node->num_child_ >= 1) {
        cur_node = cur_node->children_[0];
      } else {
        cur_node = NULL;
      }
    } else if (cur_node->children_[0]->type_ == T_IDENT) {
      if (cur_node->children_[0]->str_len_ > 128) {
        ret = OB_ERR_TOO_LONG_IDENT;
        LOG_WARN("node oversize 128", K(ret), K(cur_node->children_[0]->str_len_));
      }
      if (cur_node->num_child_ >= 2) {
        cur_node = cur_node->children_[1];
      } else {
        cur_node = NULL;
      }
    } else {
      cur_node = NULL;
    }
  }
  return ret;
}

/*
JSON_VALUE '(' js_doc_expr ',' js_literal opt_js_value_returning_type opt_ascii opt_value_on_empty_or_error_or_mismatch ')'
*/
int ObMultiModeDMLResolver::json_transform_dot_notation2_value(ParseNode &node,
                                                               const ObString &sql_str,
                                                               ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  int64_t alloc_size = sizeof(ParseNode *) * (10);
  ParseNode **param_vec = NULL;       // children
  ParseNode **param_mismatch = NULL;  // mismatch node
  ParseNode *tmp_node = NULL;         // json doc node
  ParseNode *table_node = NULL;       // table node
  ParseNode *path_node = NULL;        // path node
  ParseNode *ret_node = NULL;         // returning node
  ParseNode *opt_truncate_node = NULL; // truncate node
  ParseNode *opt_node = NULL;         // clause node
  ParseNode *match_node = NULL;       // mismatch node
  ParseNode *match_node_l = NULL;
  ParseNode *match_node_r = NULL;
  ObColumnRefRawExpr *col_expr = NULL; // unused
  bool is_json_cst = false;
  bool is_json_type = false;

  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", KP(dml_resolver), K(ret));
  } else if (OB_ISNULL(param_vec = static_cast<ParseNode **>(dml_resolver->allocator_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else if (OB_FAIL(json_check_size_obj_access_ref(&node))) {       // create json doc
    LOG_WARN("node context oversize", K(ret));
  } else if (OB_ISNULL(node.children_[JsnValueClause::JSN_VAL_DOC]) || OB_ISNULL(node.children_[JsnValueClause::JSN_VAL_PATH]) || OB_ISNULL(node.children_[1]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or column name is null", K(ret));
  } else if (OB_ISNULL(tmp_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    tmp_node = new(tmp_node) ParseNode;
    if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), node.children_[1]->children_[0]->str_value_, tmp_node, T_COLUMN_REF))) {
      LOG_WARN("create json doc node fail", K(ret));
    } else {
      if (OB_ISNULL(table_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        table_node = new(table_node) ParseNode;
        memset(table_node, 0, sizeof(ParseNode));
        table_node->type_ = T_VARCHAR;
        table_node->str_value_ = node.children_[JsnValueClause::JSN_VAL_DOC]->str_value_;
        table_node->raw_text_ = node.children_[JsnValueClause::JSN_VAL_DOC]->raw_text_;
        table_node->str_len_ = node.children_[JsnValueClause::JSN_VAL_DOC]->str_len_;
        table_node->text_len_ = node.children_[JsnValueClause::JSN_VAL_DOC]->text_len_;
        tmp_node->children_[1] = table_node;
      }
    }
    param_vec[0] = tmp_node;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_check_column_is_json_type(tmp_node, dml_resolver, col_expr, is_json_cst, is_json_type))) {
    LOG_WARN("check column type failed", K(ret));
  } else if (!(is_json_cst || is_json_type)) {
    ret = OB_WRONG_COLUMN_NAME;
    LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int32_t>(sql_str.length() - 1), sql_str.ptr());
    LOG_WARN("column type not json", K(ret));
  }
  // create path node
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(node.children_[JsnValueClause::JSN_VAL_PATH]->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node transform fail", K(ret));
    // do nothing
  } else if (OB_ISNULL(path_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    path_node = new(path_node) ParseNode;
    ParseNode *tmp_path = node.children_[JsnValueClause::JSN_VAL_PATH]->children_[1];
    ObJsonBuffer res_str(dml_resolver->allocator_);
    if (OB_FAIL(res_str.append("$"))) {
      LOG_WARN("path symbol write fail", K(ret));
    } else {
      while (OB_SUCC(ret) && OB_NOT_NULL(tmp_path)) {
        if (OB_ISNULL(tmp_path->children_[0])) {
          tmp_path = NULL;
          // do nothing
        } else {
          if (tmp_path->children_[0]->type_ == T_FUN_SYS) {
            tmp_path = tmp_path->children_[0];
          }
          if (OB_FAIL(ObMultiModeDMLResolver::json_table_path_printer(tmp_path, res_str))) {
            LOG_WARN("generate path fail", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(res_str.append("()"))) {
        LOG_WARN("() write fail", K(ret));
      } else if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), res_str.string(), path_node, T_CHAR))) {
        LOG_WARN("create path node failed", K(ret));
      } else {
        param_vec[1] = path_node;
      }
    }
  }
  // create return node
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(ret_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    ret_node = new(ret_node) ParseNode;
    memset(ret_node, 0, sizeof(ParseNode));
    ret_node->type_ = T_CAST_ARGUMENT;
    ret_node->value_ = 0;
    ret_node->is_hidden_const_ = 1;
    ret_node->num_child_ = 0;
    ret_node->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_VARCHAR;
    ret_node->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
    ret_node->int32_values_[0] = ObObjType::ObVarcharType;
    ret_node->int32_values_[OB_NODE_CAST_C_LEN_IDX] = OB_MAX_CONTEXT_VALUE_LENGTH;
    ret_node->is_tree_not_param_ = 1;
    ret_node->length_semantics_ = 0;
    ret_node->raw_text_ = "default";
    ret_node->text_len_ = strlen("default");
    param_vec[JsnValueClause::JSN_VAL_RET] = ret_node;          // return type pos is 2 in json value clause
  }
  //  opt_truncate(3) opt_ascii(4) opt_value_on_empty(5)_or_error(6) mismatch (7, 8)
  for (int8_t i = JsnValueClause::JSN_VAL_TRUNC; OB_SUCC(ret) && i < JsnValueClause::JSN_VAL_MISMATCH; i++) {
    opt_node = NULL;
    if (OB_ISNULL(opt_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      opt_node = new(opt_node) ParseNode;
      if (i == JsnValueClause::JSN_VAL_EMPTY_DEF || i == JsnValueClause::JSN_VAL_ERROR_DEF) {
        if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", opt_node, T_NULL))) {
          LOG_WARN("create path node failed", K(ret));
        } else {
          param_vec[i] = opt_node;
        }
      } else {
        if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", opt_node, T_INT))) {
          LOG_WARN("create path node failed", K(ret));
        } else {
          int8_t val = 0;
          if (i == JsnValueClause::JSN_VAL_TRUNC) {
            val = JsnValueOpt::JSN_VAL_TRUNC_OPT;
          } else if (i == JsnValueClause::JSN_VAL_EMPTY) {
            val = JsnValueOnEmpty::JSN_VALUE_DEFAULT_ON_EMPTY;
          } else if (i == JsnValueClause::JSN_VAL_ERROR) {
            val = JsnValueOnError::JSN_VALUE_NULL_ON_ERROR;
          }
          opt_node->value_ = val;
          opt_node->int32_values_[0] = val;
          opt_node->int16_values_[0] = val;
          param_vec[i] = opt_node;
        }
      }
    }
  }
  // create mismatch
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(match_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    match_node = new(match_node) ParseNode;
    if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", match_node, T_NULL))) {
      LOG_WARN("create mismatch node failed", K(ret));
    } else {
      int64_t alloc_match_size = sizeof(ParseNode *) * 2;
      if (OB_ISNULL(param_mismatch = static_cast<ParseNode **>(dml_resolver->allocator_->alloc(alloc_match_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else if (OB_ISNULL(match_node_l = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        match_node_l = new(match_node_l) ParseNode;
        if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", match_node_l, T_INT))) {
          LOG_WARN("create mismatch left node failed", K(ret));
        } else {
          match_node_l->value_ = JsnValueMisMatch::OB_JSON_ON_MISMATCH_IMPLICIT;
          match_node_l->int32_values_[0] = JsnValueMisMatch::OB_JSON_ON_MISMATCH_IMPLICIT;
          match_node_l->int16_values_[0] = JsnValueMisMatch::OB_JSON_ON_MISMATCH_IMPLICIT;
          param_mismatch[0] = match_node_l;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(match_node_r = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        match_node_r = new(match_node_r) ParseNode;
        if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", match_node_r, T_INT))) {
          LOG_WARN("create mismatch left node failed", K(ret));
        } else {
          match_node_r->value_ = JsnValueMisMatch::OB_JSON_TYPE_DOT;
          match_node_r->int32_values_[0] = JsnValueMisMatch::OB_JSON_TYPE_DOT;
          match_node_r->int16_values_[0] = JsnValueMisMatch::OB_JSON_TYPE_DOT;
          param_mismatch[1] = match_node_r;
        }
      }
      if (OB_SUCC(ret)) {
        match_node->num_child_ = 2;
        match_node->type_ = T_LINK_NODE;
        match_node->children_ = param_mismatch;
        param_vec[JsnValueClause::JSN_VAL_MISMATCH] = match_node;
      }
    }
  }
  // create json value node
  if (OB_SUCC(ret)) {
    node.num_child_ = JsnQueryClause::JSN_QUE_MISMATCH;
    node.type_ = T_FUN_SYS_JSON_VALUE;
    node.children_ = param_vec;
  }
  return ret;
}

/*
JSON_QUERY '(' js_doc_expr ',' js_literal opt_js_query_returning_type opt_scalars opt_pretty opt_ascii opt_wrapper opt_query_on_error_or_empty_or_mismatch ')'
*/
int ObMultiModeDMLResolver::json_transform_dot_notation2_query(ParseNode &node,
                                                               const ObString &sql_str,
                                                               ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  int64_t alloc_vec_size = sizeof(ParseNode *) * (JsnQueryClause::JSN_QUE_PARAM_NUM);
  ParseNode **param_vec = NULL;     // children_
  ParseNode *opt_node = NULL;       // clause node
  ParseNode *ret_node = NULL;       // returning node
  ParseNode *truncate_node = NULL;       // truncate node
  ParseNode *path_node = NULL;      // path node
  ParseNode *table_node = NULL;     // table node
  ParseNode *tmp_node = NULL;       // json doc node
  ObColumnRefRawExpr *col_expr = NULL;
  bool is_json_cst = false;
  bool is_json_type = false;

  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", KP(dml_resolver), K(ret));
  } else if (OB_ISNULL(param_vec = static_cast<ParseNode **>(dml_resolver->allocator_->alloc(alloc_vec_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  }
  // check node size    create json_doc
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_check_size_obj_access_ref(&node))) {
    LOG_WARN("ident context oversize", K(ret));
  } else if (OB_ISNULL(node.children_[JsnQueryClause::JSN_QUE_DOC]) || OB_ISNULL(node.children_[JsnQueryClause::JSN_QUE_PATH]) || OB_ISNULL(node.children_[JsnQueryClause::JSN_QUE_PATH]->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table or column name is null", K(ret));
  } else if (OB_ISNULL(tmp_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    tmp_node = new(tmp_node) ParseNode;
    if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), node.children_[JsnQueryClause::JSN_QUE_PATH]->children_[0]->str_value_, tmp_node, T_COLUMN_REF))) {
      LOG_WARN("create json doc node fail", K(ret));
    } else {
      if (OB_ISNULL(table_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        table_node = new(table_node) ParseNode;
        memset(table_node, 0, sizeof(ParseNode));
        table_node->type_ = T_VARCHAR;
        table_node->str_value_ = node.children_[JsnQueryClause::JSN_QUE_DOC]->str_value_;
        table_node->raw_text_ = node.children_[JsnQueryClause::JSN_QUE_DOC]->raw_text_;
        table_node->str_len_ = node.children_[JsnQueryClause::JSN_QUE_DOC]->str_len_;
        table_node->text_len_ = node.children_[JsnQueryClause::JSN_QUE_DOC]->text_len_;
        tmp_node->children_[1] = table_node;
      }
    }
    param_vec[0] = tmp_node;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(json_check_column_is_json_type(tmp_node, dml_resolver, col_expr, is_json_cst, is_json_type))) {
    LOG_WARN("check column type failed", K(ret));
  } else if (OB_NOT_NULL(col_expr) && ob_is_geometry(col_expr->get_result_type().get_type())) {
    if (OB_FAIL(geo_transform_dot_notation_attr(node, sql_str, *col_expr, dml_resolver))) {
      LOG_WARN("transform dot notation udt attribute failed", K(ret));
    }
  } else if (!(is_json_cst || is_json_type)) {
    /* ret = OB_WRONG_COLUMN_NAME;
    LOG_USER_ERROR(OB_WRONG_COLUMN_NAME, static_cast<int32_t>(sql_str.length() - 1), sql_str.ptr());
    LOG_WARN("column type not json", K(ret)); */
  } else {
    // create path node
    if (OB_ISNULL(node.children_[JsnQueryClause::JSN_QUE_PATH]->children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node transform fail", K(ret));
      // do nothing
    } else if (OB_ISNULL(path_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      path_node = new(path_node) ParseNode;
      ParseNode *tmp_path = node.children_[JsnQueryClause::JSN_QUE_PATH]->children_[1];
      ObJsonBuffer res_str(dml_resolver->allocator_);
      if (OB_FAIL(res_str.append("$"))) {
        LOG_WARN("path symbol write fail", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_NOT_NULL(tmp_path)) {
          if (OB_ISNULL(tmp_path->children_[0])) {
            tmp_path = NULL;
            // do nothing
          } else {
            if (OB_FAIL(ObMultiModeDMLResolver::json_table_path_printer(tmp_path, res_str))) {
              LOG_WARN("generate path fail", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), res_str.string(), path_node, T_CHAR))) {
          LOG_WARN("create path node failed", K(ret));
        } else {
          param_vec[JsnQueryClause::JSN_QUE_PATH] = path_node;
        }
      }
    }
    // create return node
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ret_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      ret_node = new(ret_node) ParseNode;
      memset(ret_node, 0, sizeof(ParseNode));
      ret_node->type_ = T_NULL;
      ret_node->is_hidden_const_ = 1;
      param_vec[JsnQueryClause::JSN_QUE_RET] = ret_node;       // return type pos is 2 in json value clause
    }
    // opt_scalars opt_pretty opt_ascii opt_wrapper opt_query_on_error_or_empty_or_mismatch 7
    for (int8_t i = JsnQueryClause::JSN_QUE_TRUNC; OB_SUCC(ret) && i < JsnQueryClause::JSN_QUE_PARAM_NUM; i++) {
      opt_node = NULL;
      if (OB_ISNULL(opt_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        opt_node = new(opt_node) ParseNode;
        memset(opt_node, 0, sizeof(ParseNode));
        if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*(dml_resolver->allocator_), "", opt_node, T_INT))) {
          LOG_WARN("create path node failed", K(ret));
        } else {
          int8_t val = 0;
          if (i == JsnQueryClause::JSN_QUE_TRUNC) {
            val = JsnQueryOpt::JSN_QUE_TRUNC_OPT;
          } else if (i == JsnQueryClause::JSN_QUE_SCALAR) {
            val = JsnQueryScalar::JSN_QUERY_SCALARS_IMPLICIT;
          } else if (i == JsnQueryClause::JSN_QUE_ERROR) {
            val = JsnValueOnError::JSN_VALUE_NULL_ON_ERROR;
          } else if (i == JsnQueryClause::JSN_QUE_EMPTY || i == JsnQueryClause::JSN_QUE_WRAPPER) {
            val = JsnValueClause::JSN_VAL_EMPTY;
          } else if (i == JsnQueryClause::JSN_QUE_MISMATCH) {
            val = JsnQueryMisMatch::JSN_QUERY_MISMATCH_DOT; // mismatch default is 3 from dot notation
          }
          opt_node->value_ = val;
          opt_node->int32_values_[0] = val;
          opt_node->int16_values_[0] = val;
          param_vec[i] = opt_node;
        }
      }
    }
    // create json query node
    if (OB_SUCC(ret)) {
      node.num_child_ = JsnQueryClause::JSN_QUE_PARAM_NUM;
      node.type_ = T_FUN_SYS_JSON_QUERY;
      node.children_ = param_vec;
    }
  }
  return ret;
}

// only_is_json: 1 is json & json type ; 0 is json; 2 json type
// only_is_json == 1: when has is json constraint or json type, return true;
// only_is_json == 0: when has is json constraint, return true
// only_is_json == 2: when is json type, return true
int ObMultiModeDMLResolver::json_check_column_is_json_type(ParseNode *tab_col, ObDMLResolver* dml_resolver,
                                                           ObColumnRefRawExpr *&column_expr, bool &is_json_cst,
                                                           bool &is_json_type, int8_t only_is_json)
{
  INIT_SUCC(ret);
  ObSEArray<ColumnItem, 4> columns_list;
  TableItem *table_item = NULL;
  const ParseNode *node = NULL;
  int16_t pos_col = -1;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  bool tab_has_alias = false;
  ObString tab_str;
  bool is_col = false;
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_NOT_NULL(tab_col) && tab_col->type_ == T_COLUMN_REF) {
    if (OB_NOT_NULL(tab_col->children_[1])) {
      tab_str.assign_ptr(tab_col->children_[1]->str_value_, tab_col->children_[1]->str_len_);
    } else if (OB_ISNULL(tab_col->children_[1]) && OB_NOT_NULL(tab_col->children_[2])) {
      is_col = true;
      tab_str.assign_ptr(tab_col->children_[2]->str_value_, tab_col->children_[2]->str_len_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table name and col name can't be null at the same time.", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml_resolver->get_target_column_list(columns_list, tab_str, false, tab_has_alias, table_item, is_col))) {
      LOG_WARN("parse column fail", K(ret));
    } else if (columns_list.count() > 0 && !tab_has_alias && !is_col) {
    } else {
      ColumnItem the_col_item;
      ObString col_name(tab_col->children_[2]->str_len_, tab_col->children_[2]->str_value_);
      for (int64_t i = 0; pos_col == -1 && i < columns_list.count(); i++) {
        if (0 == col_name.case_compare(columns_list.at(i).column_name_)) {
          pos_col = columns_list.at(i).column_id_;
          the_col_item = columns_list.at(i);
        }
      }
      if (pos_col != -1) {
        if (OB_ISNULL(table_item)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("get invalid table name", K(ret), K(tab_str));
        } else if (FALSE_IT(column_expr = the_col_item.get_expr())) {
        } else if (table_item->is_json_table() || table_item->is_temp_table()
                    || table_item->is_generated_table() || table_item->is_function_table()) {
          if (only_is_json > 0) {
            ObColumnRefRawExpr* col_expr = the_col_item.get_expr();
            if (OB_NOT_NULL(col_expr)) {
              is_json_type = (col_expr->get_result_type().get_calc_type() == ObJsonType
                            || col_expr->get_result_type().get_type() == ObJsonType
                            || col_expr->is_strict_json_column());
            } else {
              is_json_type = false;
            }
            if (!is_json_type) {
              if (table_item->is_json_table()) {
                for (size_t i = 0; i < table_item->json_table_def_->all_cols_.count() && !is_json_type; ++i) {
                  const ObJtColBaseInfo& info = *table_item->json_table_def_->all_cols_.at(i);
                  const ObString& cur_column_name = info.col_name_;
                  if (info.col_type_ == static_cast<int32_t>(COL_TYPE_QUERY)) {
                    if (ObCharset::case_compat_mode_equal(cur_column_name, col_name)) {
                      is_json_type = true;
                    }
                  }
                }
              }
            }
          } else {
            is_json_type = false;
          }
        } else if (OB_FAIL(dml_resolver->schema_checker_->get_table_schema(dml_resolver->session_info_->get_effective_tenant_id(), table_item->ref_id_, table_schema))) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("get table schema failed", K_(table_item->table_name), K(table_item->ref_id_), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("get table schema failed", K_(table_item->table_name), K(table_item->ref_id_), K(ret));
        } else {
          for (ObTableSchema::const_constraint_iterator iter = table_schema->constraint_begin();
                OB_SUCC(ret) && iter != table_schema->constraint_end() && !is_json_cst && only_is_json <= 1; iter ++) {
            const ObConstraint* ptr_constrain = *iter;
            if (OB_ISNULL(ptr_constrain)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table schema constrain is null", K(ret));
            } else if (OB_ISNULL(ptr_constrain->get_check_expr_str().ptr())) {
            } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(
                ptr_constrain->get_check_expr_str(), *(dml_resolver->params_.allocator_), node))) {
              LOG_WARN("parse expr node from string failed", K(ret));
            } else if (OB_ISNULL(node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parse expr get node failed", K(ret));
            } else if (node->type_ == T_FUN_SYS_IS_JSON
                       && ptr_constrain->get_column_cnt() > 0
                       && pos_col == *(ptr_constrain->cst_col_begin())) {
              is_json_cst = true;
            }
          }

          if (OB_SUCC(ret) && ((only_is_json >= 1 && !is_json_cst ) || (only_is_json == 0 && is_json_cst))) {
            if (OB_NOT_NULL(tab_col->children_[2])
                && OB_NOT_NULL(table_schema->get_column_schema(tab_col->children_[2]->str_value_))
                && table_schema->get_column_schema(tab_col->children_[2]->str_value_)->is_json()) {
              if (only_is_json == 0 && is_json_cst) {
                is_json_type = false;
                is_json_cst = false;
              } else {
                is_json_type = true;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_check_depth_obj_access_ref(ParseNode *node, int8_t &depth, bool &exist_fun, ObJsonBuffer &sql_str, bool obj_check)
{
  INIT_SUCC(ret);
  ParseNode *cur_node = node;
  ObString dot_name(strlen("dot_notation_array"), "dot_notation_array");
  ObString dot_point(1, ".");
  bool is_fun_sys = false;         // dot notation function as last element
  bool is_exist_array = false;

  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node) && (cur_node->type_ == T_OBJ_ACCESS_REF || cur_node->type_ == T_FUN_SYS)) {
    if (OB_ISNULL(cur_node->children_[0])) {
      if (cur_node->num_child_ >= 2) {
        cur_node = cur_node->children_[1];
      } else {
        cur_node = NULL;
      }
    } else if (cur_node->children_[0]->type_ == T_FUN_SYS) {
      if (obj_check && (depth == 3 && is_exist_array == true)) {
        ret = OB_ERR_NOT_OBJ_REF;
        LOG_WARN("not an object or REF", K(ret));
      } else {
        exist_fun = true;
        is_fun_sys = true;
        if (cur_node->num_child_ >= 1) {
          cur_node = cur_node->children_[0];
        } else {
          cur_node = NULL;
        }
      }
    } else if (cur_node->children_[0]->type_ == T_LINK_NODE && cur_node->children_[0]->value_ == 3
              && OB_NOT_NULL(cur_node->children_[0]->raw_text_)
              && (0 == dot_name.compare(cur_node->children_[0]->raw_text_))) { // [*, ]
      if (is_fun_sys) {
        cur_node = NULL;
      } else {
        cur_node = cur_node->children_[1];
      }
      depth += 1;
      is_exist_array = true;
    } else if (cur_node->children_[0]->type_ == T_IDENT
                && cur_node->children_[0]->str_len_ == 1
                && 0 == dot_point.case_compare(cur_node->children_[0]->str_value_)) {
      ret = OB_ERR_INVALID_COLUMN_SPE;
      LOG_WARN("invalid user.table.column, table.column, or column specification", K(ret));
    } else if (cur_node->children_[0]->type_ == T_IDENT) {
      if (OB_FAIL(sql_str.append("\""))) {
        LOG_WARN("fail to add \"", K(ret));
      } else if (OB_FAIL(sql_str.append(cur_node->children_[0]->str_value_))) {
        LOG_WARN("fail to add node value", K(ret));
      } else if (OB_FAIL(sql_str.append("\"."))) {
        LOG_WARN("fail to add \".", K(ret));
      }
      if (cur_node->num_child_ >= 2 && !is_fun_sys) {
        cur_node = cur_node->children_[1];
      } else {
        cur_node = NULL;
      }
      depth += 1;
    } else {
      cur_node = NULL;
    }
  }
  return ret;
}

// pre check whether dot notation
int ObMultiModeDMLResolver::json_pre_check_dot_notation(ParseNode &node, int8_t& depth, bool& exist_fun,
                                                        ObJsonBuffer& sql_str, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  bool check_res = true;
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_ISNULL(node.children_[0])) {  // check not return error
  } else if (OB_FAIL(json_check_first_node_name(node.children_[0]->str_value_, check_res, dml_resolver))) {
    LOG_WARN("fail to check first node", K(ret), K(node.children_[0]->str_value_));
  } else if (check_res) {
    // normal query do nothing
  } else if (OB_FAIL(json_check_depth_obj_access_ref(&node, depth, exist_fun, sql_str))) {
    LOG_WARN("get depth of obj access ref failed", K(ret));
  } else if (depth == 3 && exist_fun && OB_FAIL(xml_check_column_udt_type(&node, dml_resolver))) {
    // cases like: a.b.fun(), a must be table alias, b must be col name, and b must be udt type
    LOG_WARN("not an object or REF", K(ret));
  }
  return ret;
}

//
// columns with is json constraint should set_strict_json_column > 00
bool ObMultiModeDMLResolver::json_check_generated_column_with_json_constraint(const ObSelectStmt *stmt,
                                                                              const ObColumnRefRawExpr *col_expr,
                                                                              ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  bool ret_bool = false;

  const ParseNode *node = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;

  if (OB_NOT_NULL(stmt) && OB_NOT_NULL(col_expr) && OB_NOT_NULL(dml_resolver)) {
    int table_num = stmt->get_table_size();
    for (int i = 0; OB_SUCC(ret) && i < table_num && !ret_bool; ++i) {
      const TableItem *tmp_table_item = stmt->get_table_item(i);
      if (OB_NOT_NULL(tmp_table_item)
          && (OB_SUCC(dml_resolver->schema_checker_->get_table_schema(dml_resolver->session_info_->get_effective_tenant_id(), tmp_table_item->ref_id_, table_schema)))) {
        if (OB_NOT_NULL(table_schema)) {
          for (ObTableSchema::const_constraint_iterator iter = table_schema->constraint_begin();
               OB_SUCC(ret) && !ret_bool && iter != table_schema->constraint_end(); iter ++) {
            const ObConstraint* ptr_constrain = *iter;
            if (OB_ISNULL(ptr_constrain)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table schema constrain is null", K(ret));
            } else if (OB_ISNULL(ptr_constrain->get_check_expr_str().ptr())) {
            } else if (OB_FAIL(ObRawExprUtils::parse_bool_expr_node_from_str(ptr_constrain->get_check_expr_str(),
                                                                             *(dml_resolver->params_.allocator_), node))) {
              LOG_WARN("parse expr node from string failed", K(ret));
            } else if (OB_ISNULL(node)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parse expr get node failed", K(ret));
            } else if (node->type_ != T_FUN_SYS_IS_JSON) {
            } else if (ptr_constrain->get_column_cnt() == 0 || OB_ISNULL(ptr_constrain->cst_col_begin())) {
            } else if (col_expr->get_column_id() == *(ptr_constrain->cst_col_begin())) {
              ret_bool = true;
            }
          } // table item is null OR fail to get schema, do nothing, return false
        }
      } // for each table
    } // else is null, do nothing, return false
  }
  if (OB_FAIL(ret)) {
    ret_bool = false;
  }
  return ret_bool;
}

//only_is_json: 1 is json & json type ; 0 is json; 2 json type
int ObMultiModeDMLResolver::json_check_is_json_constraint(ObDMLResolver* dml_resolver,
                                                          ParseNode *col_node,
                                                          bool& is_json_cst,
                                                          bool& is_json_type,
                                                          int8_t only_is_json)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(ret), KP(dml_resolver), KP(dml_resolver->allocator_));
  } else {
    common::ObIAllocator* allocator = dml_resolver->allocator_;
    ParseNode *tmp_node = NULL;
    ParseNode *table_node = NULL;
    int8_t depth = 0;
    bool exist_fun = false;
    bool check_res = true;
    bool check_valid = false;
    ObColumnRefRawExpr *col_expr = NULL; // unused

    if (OB_ISNULL(col_node)) { // do nothing
    } else if (OB_ISNULL(tmp_node = static_cast<ParseNode*>(allocator->alloc(sizeof(ParseNode))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else {
      tmp_node = new(tmp_node) ParseNode;
      ObJsonBuffer sql_str(allocator);
      if (OB_ISNULL(col_node->children_[0]) || col_node->children_[0]->type_ != T_IDENT
          || OB_ISNULL(col_node->children_[0]->str_value_)) { // do not check
      } else if (OB_FAIL(json_check_depth_obj_access_ref(col_node, depth, exist_fun, sql_str, false))) {
        LOG_WARN("get depth of obj access ref failed", K(ret));
      } else if (exist_fun || depth >= 3) {
        // do nothing
      } else if (OB_UNLIKELY(depth < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wrong col name", K(ret), KP(col_node));
      } else if (depth == 1) {
        if (OB_ISNULL(col_node->children_[0]) || OB_ISNULL(col_node->children_[0]->str_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("col name should not be null", K(ret));
        } else if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*allocator,
                            col_node->children_[0]->str_value_, tmp_node, T_COLUMN_REF))) {
          LOG_WARN("create json doc node fail", K(ret));
        } else {
          check_valid = true;
        }
      } else if (depth == 2) {
        // childe[1] column name,  child[0] table name
        if (OB_ISNULL(col_node->children_[1]) || OB_ISNULL(col_node->children_[1]->children_[0])
            || OB_ISNULL(col_node->children_[1]->children_[0]->str_value_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("col name should not be null", K(ret));
        } else if (OB_FAIL(ObRawExprResolverImpl::malloc_new_specified_type_node(*allocator,
                      col_node->children_[1]->children_[0]->str_value_, tmp_node, T_COLUMN_REF))) {
          LOG_WARN("create json doc node fail", K(ret));
        } else {
          if (OB_ISNULL(col_node->children_[0])) { // do nothing
          } else if (OB_ISNULL(table_node = static_cast<ParseNode*>(allocator->alloc(sizeof(ParseNode))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
          } else {
            table_node = new(table_node) ParseNode;
            memset(table_node, 0, sizeof(ParseNode));
            table_node->type_ = T_VARCHAR;
            table_node->str_value_ = col_node->children_[0]->str_value_;
            table_node->raw_text_ = col_node->children_[0]->raw_text_;
            table_node->str_len_ = col_node->children_[0]->str_len_;
            table_node->text_len_ = col_node->children_[0]->text_len_;
            tmp_node->children_[1] = table_node;
            check_valid = true;
          }
        }
      }
    }

    if (OB_SUCC(ret) && check_valid && OB_FAIL(json_check_column_is_json_type(tmp_node, dml_resolver, col_expr, is_json_cst, is_json_type, only_is_json))) {
      LOG_WARN("fail to check is_json", K(ret));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_process_dot_notation_in_json_object(ParseNode*& expr_node,
                                                                     ParseNode* cur_node,
                                                                     ObDMLResolver* dml_resolver,
                                                                     int& pos)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolver isn't init", K(ret), KP(dml_resolver), KP(dml_resolver->allocator_));
  } else {
    int8_t depth = 0;
    bool exist_fun = false;
    ObJsonBuffer sql_str(dml_resolver->allocator_);
    bool is_dot_notation = false;
    ParseNode* key_node = NULL;

    if (OB_ISNULL(cur_node)
        || cur_node->type_ != T_OBJ_ACCESS_REF
        || OB_ISNULL(cur_node->children_[0])
        || OB_ISNULL(cur_node->children_[0]->str_value_)) { // do not check
    } else if (OB_FAIL(json_pre_check_dot_notation(*cur_node, depth, exist_fun, sql_str, dml_resolver))) {
      LOG_WARN("get depth of obj access ref failed", K(ret));
    } else if (!exist_fun && depth >= 3) {
      is_dot_notation = true;
    }
    // case : json_object(t1.c1.key1) -> json_object(key1 : t1.c1.key1);
    // type == T_NULL : input value only.
    // value_ = 2 : distinct true NULL value
    if (OB_SUCC(ret) && is_dot_notation && OB_NOT_NULL(expr_node->children_[pos + 1])
        && expr_node->children_[pos + 1]->type_ == T_NULL && expr_node->children_[pos + 1]->value_ == 2) { // case only has dot notation
      if (OB_ISNULL(key_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
      } else {
        memset(key_node, 0, sizeof(ParseNode));
        while (OB_NOT_NULL(cur_node->children_[1]) && cur_node->children_[1]->type_ == T_OBJ_ACCESS_REF) {
          cur_node = cur_node->children_[1];
        }
        if (OB_ISNULL(cur_node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("json object first node should not be null", K(ret));
        } else {
          key_node->type_ = T_VARCHAR;
          key_node->str_value_ = cur_node->children_[0]->raw_text_;
          key_node->raw_text_ = cur_node->children_[0]->raw_text_;
          key_node->str_len_ = cur_node->children_[0]->text_len_;
          key_node->text_len_ = cur_node->children_[0]->text_len_;
          expr_node->children_[pos + 1] = expr_node->children_[pos];
          expr_node->children_[pos] = key_node;
        }
      }
    }
  }
  return ret;
} // deal dot notation in json object

//const_cast<ParseNode *>(&node)
// process is json constraint for: json_array, json_object, json_arraragg, json_objectagg
int ObMultiModeDMLResolver::json_pre_process_json_constraint(ParseNode *node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  if ((node->type_ == T_FUN_SYS_JSON_OBJECT || node->type_ == T_FUN_SYS_JSON_ARRAY)
        && OB_FAIL(json_process_object_array_expr_node(node, dml_resolver))) {
    LOG_WARN("fail to process json object & array with json constraint", K(ret));
  } else if ((node->type_ == T_FUN_ORA_JSON_ARRAYAGG || node->type_ == T_FUN_ORA_JSON_OBJECTAGG)
              && OB_FAIL(json_process_json_agg_node(node, dml_resolver))) {
    LOG_WARN("fail to process json object & array agg", K(ret));
  }
  return ret;
}

int ObMultiModeDMLResolver::json_process_json_agg_node(ParseNode*& node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  ObString def_val(7, "default");
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if ((OB_NOT_NULL(node->children_[1]) && OB_NOT_NULL(node->children_[2]) && OB_NOT_NULL(node->children_[4]))) {
    ParseNode *value_node = node->children_[1];
    ParseNode *format_node = node->children_[2];
    ParseNode *returning_node = node->children_[4];
    bool is_default_ret = returning_node->value_ == 0 && def_val.case_compare(returning_node->raw_text_) == 0;
    bool is_json_cst = format_node->value_;
    bool is_json_type = false;

    if ((!is_json_cst || is_default_ret) && (value_node->type_ == T_OBJ_ACCESS_REF)) {
      if (OB_FAIL(json_check_is_json_constraint(dml_resolver, value_node, is_json_cst, is_json_type, 1))) {
        LOG_WARN("fail to check is_json constraint of col", K(ret));
      } else {
        if (is_json_cst) { // set format json
          format_node->value_ = 1;
        }
        if (is_default_ret && is_json_type) { // set default returning
          returning_node->int16_values_[OB_NODE_CAST_TYPE_IDX] = T_JSON; /* data type */
          returning_node->int16_values_[OB_NODE_CAST_COLL_IDX] = INVALID_COLLATION;
          returning_node->int32_values_[OB_NODE_CAST_C_LEN_IDX] = 0;        /* length */
          returning_node->param_num_ = 0;
        }
      }
    }
  }
  return ret;
}

// json_check_first_node_name only check first node whether table or database, not return error
int ObMultiModeDMLResolver::json_check_first_node_name(const ObString &node_name, bool &check_res, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  uint64_t database_id = 0;
  bool is_table = false;
  int64_t num = 0;
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(dml_resolver->get_stmt());
    if (OB_ISNULL(node_name)) {
      LOG_WARN("node_name input null", K(ret));
    } else if (OB_FAIL(dml_resolver->schema_checker_->get_database_id(dml_resolver->session_info_->get_effective_tenant_id(), node_name, database_id))) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(select_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select stmt is null", K(ret));
      } else {
        num = select_stmt->get_table_size();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
        const TableItem *table_item = select_stmt->get_table_item(i);
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if (table_item->table_name_ != node_name && table_item->alias_name_ != node_name) {
          // other table should chose
        } else {
          is_table = true;
        }
      }
      if (OB_SUCC(ret) && is_table) {
        check_res = false;
      }
      // just judge database not print error.
    }
    ret = OB_SUCCESS; // check fail but ignore
  }
  return ret;
}

int ObMultiModeDMLResolver::json_expand_column_in_json_object_star(ParseNode *node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  ObSEArray<ColumnItem, 4> columns_list;
  TableItem *table_item = NULL;
  bool tab_has_alias = false;
  ObString tab_name;
  bool all_tab = true;
  ObDMLStmt *stmt = nullptr;

  if (OB_ISNULL(node) || OB_ISNULL(dml_resolver) || OB_ISNULL(stmt = static_cast<ObDMLStmt *>(dml_resolver->get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null", K(ret));
  } else if (OB_NOT_NULL(node->children_[1])) {
    tab_name.assign_ptr(node->children_[1]->str_value_, node->children_[1]->str_len_);
    all_tab = false;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml_resolver->get_target_column_list(columns_list, tab_name, all_tab, tab_has_alias, table_item))) {
    LOG_WARN("parse column fail", K(ret));
  }
  return ret;
}

// process json_expr in query sql
int ObMultiModeDMLResolver::json_pre_process_one_expr(ParseNode &node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null", KP(dml_resolver), K(ret));
  } else {
    int8_t depth = 0;
    bool exist_fun = false;
    ObJsonBuffer sql_str(dml_resolver->allocator_);
    if (node.type_ == T_OBJ_ACCESS_REF) { // check dot notation node
      if (OB_FAIL(json_pre_check_dot_notation(node, depth, exist_fun, sql_str, dml_resolver))) {
        LOG_WARN("get fail to check dot notation node", K(ret));
      } else if (!exist_fun) {
        if (depth < 3) {
          // do nothing
        } else if (OB_FAIL(json_transform_dot_notation2_query(node, sql_str.string(), dml_resolver))) { // transform to json query
          LOG_WARN("transform to json query failed", K(depth), K(sql_str.string()));
        }
      } else if (exist_fun && depth >= 4) {
        if (OB_FAIL(json_transform_dot_notation2_value(node, sql_str.string(), dml_resolver))) { // transform to json value
          LOG_WARN("transform to json value failed", K(depth), K(sql_str.string()));
        }
      }
    } else if (OB_FAIL(json_pre_process_json_constraint(&node, dml_resolver))) { // check json expr with is json constraint
      LOG_WARN("fail to process json exor with json constraint", K(ret));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::json_pre_process_expr(ParseNode &node, ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  ObArray<void *> parse_node_list;
  void *tmp_node = nullptr;

  if (OB_FAIL(parse_node_list.push_back(static_cast<void *>(&node)))) {
    LOG_WARN("fail to push back parse node", K(ret));
  } else {
    while (OB_SUCC(ret) && !parse_node_list.empty()) {
      if (OB_FAIL(parse_node_list.pop_back(tmp_node))) {
        LOG_WARN("fail to pop back parse node", K(ret));
      } else {
        ParseNode *curr_node = static_cast<ParseNode *>(tmp_node);

        if (OB_FAIL(json_pre_process_one_expr(*curr_node, dml_resolver))) {
          LOG_WARN("deal dot notation fail", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < curr_node->num_child_; i++) {
            int64_t index = curr_node->num_child_ - 1 - i;
            ParseNode *child = curr_node->children_[index];
            if (OB_ISNULL(child)) {
            } else if (OB_FAIL(parse_node_list.push_back(child))) {
              LOG_WARN("parse node list push back failed", K(ret), K(index));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::json_process_object_array_expr_node(ParseNode *node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver) || OB_ISNULL(dml_resolver->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null", KP(dml_resolver), K(ret));
  } else {
    ParseNode *cur_node = NULL; // current node in iter
    ParseNode *param_node = NULL; // first param node
    ParseNode* format_node = NULL;
    ParseNode* value_node = NULL;
    ObJsonBuffer sql_str(dml_resolver->allocator_);

    param_node = node->children_[0];
    bool is_object = node->type_ == T_FUN_SYS_JSON_OBJECT;
    CK (OB_NOT_NULL(param_node));
    // json_array child:[i]->json_data, [i+1]->format_json
    // json_object child: [i]->json_key, [i+1]->json_data, [i+2]->format_json
    int8_t step = is_object ? 3 : 2;
    for (int i = 0; OB_SUCC(ret) && i + (step - 1) < param_node->num_child_; i += step) {
      // Singular: parse format json parameter
      // first judge format json is or not true, true then pass
      // false then check whether has is json constraint in this col , or set format json is true
      format_node = param_node->children_[i + step - 1];
      value_node = param_node->children_[i + step - 2];
      cur_node = param_node->children_[i];
      if (is_object && OB_NOT_NULL(value_node)
          && value_node->type_ == T_NULL && value_node->value_ == 2) {
        value_node = cur_node;
      }
      if (OB_NOT_NULL(cur_node) && cur_node->type_ == T_COLUMN_REF
          && OB_NOT_NULL(cur_node->children_[2])
          && cur_node->children_[2]->type_ == T_STAR) { // ignore wild card node
        if (OB_FAIL(json_expand_column_in_json_object_star(cur_node, dml_resolver))) {  // append column item into stmt
          LOG_WARN("fail to expand column item of json object star", K(ret));
        }
      } else {
        if (OB_NOT_NULL(value_node) && OB_NOT_NULL(format_node)) {
          bool format_json = format_node->value_;
          bool is_json_type = false;
          if (!format_json && (value_node->type_ == T_OBJ_ACCESS_REF)) {
            if (OB_FAIL(json_check_is_json_constraint(dml_resolver, value_node, format_json, is_json_type))) {
              LOG_WARN("fail to check is_json constraint of col", K(ret), K(i));
            } else if (format_json) {
              format_node->value_ = 1;
            }
          }
        } // check json_expr is json constraint
        if (OB_SUCC(ret) && OB_NOT_NULL(cur_node) && is_object
            && cur_node->type_ == T_OBJ_ACCESS_REF
            && OB_FAIL(json_process_dot_notation_in_json_object(param_node, cur_node, dml_resolver, i))) {
          LOG_WARN("fail to process dot notation node in json object", K(ret));
        }
      }
    }
  }

  return ret;
}

// xml
bool ObMultiModeDMLResolver::xml_table_check_path_need_transform(ObString& path)
{
  bool res = true;
  size_t len = path.length();
  size_t l = 0;
  if ((path[0] >= 'a' && path[0] <= 'z')
        || (path[0] >= 'A' && path[0] <= 'Z')
        || path[0] >= '_') {
  } else {
    res = false;
  }
  while(res && l < len
        && !ObPathParserUtil::is_xpath_transform_terminator(path[l])) {
    if (path[l] == ObPathItem::BRACE_START || path[l] == ObPathItem::COLON) {   // exist item method, not transform
      res = false;
    }
    l ++;
  }
  if (res && l == 0) { // first char is '/', ignore
    res = false;
  }
  return res;
}

// xmltype need transform xpath 'a' -> '/a'
int ObMultiModeDMLResolver::xml_check_xpath(ObDmlJtColDef *col_def,
                                            const ObDataType &data_type,
                                            ObIArray<ObDmlJtColDef*>& json_table_infos,
                                            ObIAllocator* allocator)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else if (!ob_is_xml_sql_type(data_type.get_obj_type(), data_type.get_meta_type().get_subschema_id())) {
  } else if (col_def->col_base_info_.path_.length() == 0) {
    ret = OB_ERR_LACK_XQUERY_LITERAL;
    LOG_WARN("xmltable need xquery literal", K(ret));
  } else if (json_table_infos.at(0)->col_base_info_.path_ == "/"
              && xml_table_check_path_need_transform(col_def->col_base_info_.path_)) {
    int64_t len = col_def->col_base_info_.path_.length() + 1;
    char* str_buf = static_cast<char*>(allocator->alloc(len));
    if (OB_ISNULL(str_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(col_def->col_base_info_.path_));
    } else {
      MEMCPY(str_buf, "/", 1);
      MEMCPY(str_buf + 1, col_def->col_base_info_.path_.ptr(), col_def->col_base_info_.path_.length());
      col_def->col_base_info_.path_.assign_ptr(str_buf, len);
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::xml_table_resolve_xml_namespaces(const ParseNode *namespace_node,
                                                             ObJsonTableDef*& table_def,
                                                             ObIAllocator* allocator)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("should not be null", K(ret));
  } else {
    ObString t_str;
    common::hash::ObHashSet<ObString> key_ns;
    bool has_default = false;
    int64_t bucket_num = (namespace_node->num_child_ / 2) + 1;
    if (namespace_node->type_ == T_NULL) { // ns is null
    } else if (namespace_node->num_child_ > 0 && OB_FAIL(key_ns.create(bucket_num))) {
        LOG_WARN("init hash failed", K(ret), K(bucket_num));
    } else {
        for (int i = 0; OB_SUCC(ret) && i < namespace_node->num_child_; i++) {
        if (i % 2 == 1 && namespace_node->children_[i]->type_ == T_NULL && namespace_node->children_[i]->value_ == 1) {
          if (has_default) {
            ret = OB_ERR_DUP_DEF_NAMESPACE;
            ObString str_def(namespace_node->children_[i]->text_len_, namespace_node->children_[i]->raw_text_);
            LOG_USER_ERROR(OB_ERR_DUP_DEF_NAMESPACE, str_def.ptr());
            LOG_WARN("can not input one more default ns", K(ret));
          } else {
            t_str.assign_ptr("", 0);
            has_default = true;
          }
        } else if (i % 2 == 1 && namespace_node->children_[i]->text_len_ == 0) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_WARN("can not input without ns prefix", K(ret));
        } else {
          if (i % 2 == 0) { // drop single quote
            size_t path_len = namespace_node->children_[i]->text_len_;
            char* str_buf = static_cast<char*>(allocator->alloc(path_len - 2));
            if (OB_ISNULL(str_buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret), K(path_len));
            } else {
              MEMCPY(str_buf, namespace_node->children_[i]->raw_text_ + 1, path_len - 2);
              t_str.assign_ptr(str_buf, path_len - 2);
            }
          } else {
            t_str.assign_ptr(namespace_node->children_[i]->raw_text_, namespace_node->children_[i]->text_len_);
          }
        }
        if (OB_FAIL(ret)) {
        } else if (i % 2 == 1 && OB_HASH_EXIST == key_ns.exist_refactored(t_str)) {
          ret = OB_ERR_TOO_MANY_PREFIX_DECLARE;
          LOG_USER_ERROR(OB_ERR_TOO_MANY_PREFIX_DECLARE, t_str.length(), t_str.ptr());
          LOG_WARN("duplicate key", K(ret));
        } else if (i % 2 == 1 && OB_FAIL(key_ns.set_refactored(t_str, 0))) {
          LOG_WARN("store key to vector failed", K(ret), K(key_ns.size()));
        } else if (OB_FAIL(table_def->namespace_arr_.push_back(t_str))) {
          LOG_WARN("failed push string in namespace", K(ret), K(t_str), K(i));
        }
      }
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::xml_check_column_udt_type(ParseNode *root_node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  ObSEArray<ColumnItem, 4> columns_list;
  TableItem *table_item = NULL;

  int16_t pos_col = -1;
  share::schema::ObSchemaGetterGuard *schema_guard = NULL;
  const share::schema::ObTableSchema *table_schema = NULL;
  bool tab_has_alias = false;
  ObString tab_str;
  ObString col_str;

  if (OB_ISNULL(root_node)
      || OB_ISNULL(dml_resolver)
      || root_node->type_ != T_OBJ_ACCESS_REF
      || root_node->num_child_ != 2
      || OB_ISNULL(root_node->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not obj access ref node for check", K(ret));
  } else {
    const ParseNode *node_ptr = root_node->children_[0];
    if (OB_NOT_NULL(node_ptr)) {
      tab_str.assign_ptr(node_ptr->str_value_, node_ptr->str_len_);
    }
    node_ptr = root_node->children_[1]->children_[0];
    if (OB_NOT_NULL(node_ptr)) {
      col_str.assign_ptr(node_ptr->str_value_, node_ptr->str_len_);
    }
    if (OB_FAIL(dml_resolver->get_target_column_list(columns_list, tab_str, false, tab_has_alias, table_item, false))) {
      LOG_WARN("parse table fail", K(ret));
    } else if (OB_ISNULL(table_item) || !tab_has_alias) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      LOG_WARN("get invalid table name", K(ret), K(tab_str), K(tab_has_alias));
    } else {
      ColumnItem the_col_item;
      for (int64_t i = 0; i < columns_list.count() && pos_col < 0; i++) {
        if (0 == col_str.case_compare(columns_list.at(i).column_name_)) {
          pos_col = columns_list.at(i).column_id_;
          the_col_item = columns_list.at(i);
        }
      }
      if (pos_col < 0) { // not found
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get invalid identifier name", K(ret), K(tab_str), K(col_str), K(tab_has_alias));
      } else {
        ObColumnRefRawExpr* col_expr = the_col_item.get_expr();
        if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("get invalid identifier name", K(ret), K(tab_str), K(col_str), K(tab_has_alias));
        } else if (!col_expr->get_result_type().is_user_defined_sql_type() &&
                   !col_expr->get_result_type().is_geometry() &&
                   !col_expr->get_result_type().is_ext()) {
          ret = OB_ERR_NOT_OBJ_REF;
        }
      }
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::xml_replace_col_udt_qname(ObQualifiedName& q_name, ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  ObQualifiedName udt_col_func_q_name;
  // Only support:
  // 1. table_alias.col_name.udf_member_func, for example: select a.c2.getclobval() from t1 a;
  // 2. table_alias.col_name.member_func.udf, for example: select a.c2.transfrom(xxx).getclobval() from t1 a;
  // Notice:
  // 1. must have table alias name;
  // 2. table_alias.col_name.static_func.udf is not supported, for example:
  //    select a.c2.createxml(xxx).getclobval() from t1 a; creatxml is an static function, not support
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc qualified name buffer", K(ret));
  } else if (q_name.access_idents_.count() >= 3
        && q_name.access_idents_.at(0).type_ == UNKNOWN
        && q_name.access_idents_.at(1).type_ == UNKNOWN
        && q_name.access_idents_.at(2).type_ == PL_UDF) {
    ObQualifiedName udt_col_candidate;
    ObRawExpr* udt_col_ref_expr = NULL;
    if (OB_FAIL(udt_col_candidate.access_idents_.push_back(q_name.access_idents_.at(0)))) {
      LOG_WARN("push back table alias ident failed", K(ret), K(q_name.access_idents_.at(0)));
    } else if (OB_FAIL(udt_col_candidate.access_idents_.push_back(q_name.access_idents_.at(1)))) {
      LOG_WARN("push back column ident failed", K(ret), K(q_name.access_idents_.at(0)));
    } else {
      udt_col_candidate.tbl_name_ = q_name.access_idents_.at(0).access_name_;
      udt_col_candidate.col_name_ = q_name.access_idents_.at(1).access_name_;
      udt_col_candidate.ref_expr_= q_name.ref_expr_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dml_resolver->resolve_column_ref_expr(udt_col_candidate, udt_col_ref_expr))) {
      LOG_WARN("try get udt col ref failed", K(ret), K(udt_col_candidate));
      // should not return error if not found
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ObRawExprUtils::implict_cast_sql_udt_to_pl_udt(dml_resolver->params_.expr_factory_,
                                                  dml_resolver->params_.session_info_, udt_col_ref_expr))) {
      LOG_WARN("try add implict cast above sql udt col ref failed",
        K(ret), K(udt_col_candidate), K(udt_col_ref_expr));
    } else {
      // mock new q_name with ref_expr and access_idents_.data_[0].type_ = oceanbase::sql::SYS_FUNC
      udt_col_func_q_name.ref_expr_= q_name.ref_expr_;
      if (OB_FAIL(udt_col_func_q_name.access_idents_.push_back(ObObjAccessIdent(ObString("UDT_REF"), OB_INVALID_INDEX)))) {
        LOG_WARN("push back col ref ident failed", K(ret));
      } else {
        for (int64_t i = 2; OB_SUCC(ret) && i < q_name.access_idents_.count(); i++) {
          if (OB_FAIL(udt_col_func_q_name.access_idents_.push_back(q_name.access_idents_.at(i)))) {
            LOG_WARN("push back udt member function failed", K(ret), K(i), K(q_name.access_idents_.at(i)));
          }
        }
        if (OB_SUCC(ret)) {
          udt_col_func_q_name.access_idents_.at(0).type_ = SYS_FUNC;
          udt_col_func_q_name.access_idents_.at(0).sys_func_expr_ = udt_col_ref_expr;
          q_name = udt_col_func_q_name;
        }
      }
    }
  }

  return ret;
}

// geo
int ObMultiModeDMLResolver::geo_transform_dot_notation_attr(ParseNode &node, const ObString &sql_str,
                                                            const ObColumnRefRawExpr &col_expr,
                                                            ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator;
  ObString raw_text;
  ObString attr_name;
  uint64_t udt_id = T_OBJ_SDO_GEOMETRY;
  int64_t schema_version = OB_INVALID_VERSION;
  uint64_t tenant_id = pl::get_tenant_id_by_object_id(udt_id);
  ObArray<ObString> udt_qualified_name;
  ObString qualified_name;
  ParseNode *table_node = NULL;
  char *tmp = static_cast<char *>(tmp_allocator.alloc(OB_MAX_QUALIFIED_COLUMN_NAME_LENGTH));
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_2_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sdo_geometry attribute access is not supported when cluster_version is less than 4.2.2.0", K(ret));
  } else if (OB_ISNULL(tmp) || OB_ISNULL(dml_resolver)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to alloc qualified name buffer", K(ret));
  } else if (FALSE_IT(qualified_name.assign_buffer(tmp, OB_MAX_QUALIFIED_COLUMN_NAME_LENGTH))) {
  } else if (qualified_name.write(col_expr.get_column_name().ptr(),
                                  col_expr.get_column_name().length()) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write qualified column name failed", K(ret), K(col_expr.get_column_name()));
  } else if (OB_FAIL(dml_resolver->schema_checker_->flatten_udt_attributes(tenant_id, udt_id, tmp_allocator,
                                                              qualified_name, schema_version, udt_qualified_name))) {
    LOG_WARN("failed to flatten udt attributes", K(ret));
  } else if (OB_FAIL(geo_transform_udt_attrbute_name(sql_str, tmp_allocator, attr_name))) {
    LOG_WARN("failed to transform udt attributes name", K(ret));
  } else if (FALSE_IT(raw_text = attr_name.after('.'))) {
  } else if (OB_ISNULL(table_node = static_cast<ParseNode*>(dml_resolver->allocator_->alloc(sizeof(ParseNode))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
  } else {
    table_node = new(table_node) ParseNode;
    memset(table_node, 0, sizeof(ParseNode));
    table_node->type_ = T_VARCHAR;
    table_node->str_value_ = node.children_[0]->str_value_;
    table_node->raw_text_ = node.children_[0]->raw_text_;
    table_node->str_len_ = node.children_[0]->str_len_;
    table_node->text_len_ = node.children_[0]->text_len_;

    bool is_basic_attr = false;
    ParseNode **param_vec = NULL;
    uint32_t param_count = 4;
    int64_t attr_idx = 0;
    uint64_t sub_udt_id = OB_INVALID_ID;
    for (; attr_idx + 2 < udt_qualified_name.count() && !is_basic_attr; attr_idx++) {
      if (udt_qualified_name.at(attr_idx).case_compare(raw_text) == 0) {
        is_basic_attr = true;
      }
    }
    if (!is_basic_attr) {
      ObString sub_name = raw_text.after(raw_text.reverse_find('.'));
      uint64_t attr_pos = 0;
      if (OB_FAIL(dml_resolver->schema_checker_->get_udt_attribute_id(udt_id, sub_name, sub_udt_id, attr_pos, schema_version))) {
        LOG_WARN("fail to get tenant udt id", K(ret));
      } else if (sub_udt_id == OB_INVALID_ID || sub_udt_id <= ObMaxType) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("type_node or stmt_ or datatype is invalid", K(ret), K(sub_name), K(sub_udt_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(param_vec = static_cast<ParseNode **>(dml_resolver->allocator_->alloc(param_count * sizeof(ParseNode *))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ParseNode)));
    } else if (OB_FAIL(dml_resolver->create_col_ref_node(table_node, col_expr.get_column_name(), param_vec[0]))) {
      LOG_WARN("fail to create column parse node", K(ret));
    } else if (OB_FAIL(dml_resolver->create_int_val_node(table_node, udt_id, param_vec[1]))) {
      LOG_WARN("fail to create value node", K(ret), K(udt_id), K(sub_udt_id));
    } else if (OB_FAIL(dml_resolver->create_int_val_node(table_node, is_basic_attr ? attr_idx : sub_udt_id, param_vec[2]))) {
      LOG_WARN("fail to create value node", K(ret), K(udt_id), K(sub_udt_id));
    } else if (OB_FAIL(dml_resolver->create_int_val_node(table_node, schema_version, param_vec[3]))) {
      LOG_WARN("fail to create value node", K(ret), K(udt_id), K(sub_udt_id));
    } else {
      node.num_child_ = param_count;
      node.type_ = is_basic_attr ? T_FUN_SYS_PRIV_SQL_UDT_ATTR_ACCESS : T_FUN_SYS_PRIV_SQL_UDT_CONSTRUCT;
      node.children_ = param_vec;
    }
  }

  return ret;
}

int ObMultiModeDMLResolver::geo_transform_udt_attrbute_name(const ObString &sql_str, ObIAllocator &allocator, ObString &attr_name)
{
  INIT_SUCC(ret);
  const ObString::obstr_size_t len = sql_str.length();
  char *ptr = NULL;
  if (OB_ISNULL(sql_str.ptr()) || OB_UNLIKELY(0 >= len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql string should not be null", K(ret));
  } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(len));
  } else {
    const char *start = sql_str.ptr();
    const char *end = sql_str.ptr() + sql_str.length();
    int64_t pos = 0;
    for (; start < end; start++) {
      if ((*start) != '"') {
        ptr[pos++] = *start;
      }
    }
    attr_name.assign_ptr(ptr, pos > 0 ? pos - 1 : pos);
  }
  return ret;
}

int ObMultiModeDMLResolver::geo_pre_process_mvt_agg(ParseNode &node, ObDMLResolver* dml_resolver)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(dml_resolver)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("dml resolver is null", K(ret));
  } else if (node.type_ == T_FUN_SYS_ST_ASMVT) {
    int ori_param_num = node.num_child_;
    if (ori_param_num < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected param number", K(ret), K(ori_param_num));
    } else if (node.reserved_) {
      // already processed, do nothing
    } else if (OB_ISNULL(node.children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr param is null", K(ret));
    } else if (node.children_[0]->type_ != T_COLUMN_REF) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K(ret), K(node.children_[0]->type_));
    } else if (OB_ISNULL(node.children_[0]->children_[2])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr param is null", K(ret));
    } else if (node.children_[0]->children_[2]->type_ != T_STAR) {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("parameter row cannot be other than a rowtype", K(ret));
    } else {
      ObQualifiedName column_ref;
      ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
      TableItem *table_item = NULL;
      bool tab_has_alias = false;
      ObSEArray<ColumnItem, 4> columns_list;
      if (OB_FAIL(dml_resolver->session_info_->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(node.children_[0], case_mode, column_ref))) {
        LOG_WARN("fail to resolve table name", K(ret));
      } else if (OB_FAIL(dml_resolver->get_target_column_list(columns_list, column_ref.tbl_name_, false, tab_has_alias, table_item))) {
        LOG_WARN("parse column fail", K(ret));
      } else if (columns_list.count() < 1) {
        // do nothing, as_mvt might be in sub_stmt
      } else {
        ParseNode **param_vec = NULL;
        uint32_t para_idx = 0;
        // *(row) + other params = column_ref + column_name + other params + other params cnt
        uint32_t param_count =  columns_list.count() * 2 + ori_param_num;
        ParseNode* param_cnt_node = NULL;
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(param_vec = static_cast<ParseNode **>(dml_resolver->allocator_->alloc(param_count * sizeof(ParseNode *))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret), K(columns_list.count()));
        } else if (OB_FAIL(dml_resolver->create_int_val_node(NULL, ori_param_num - 1, param_cnt_node))) {
          LOG_WARN("fail to create int val node", K(ret));
        } else {
          param_vec[para_idx++] = param_cnt_node;
          for (int i = 1; i < ori_param_num; i++) {
            param_vec[para_idx++] = node.children_[i];
          }
        }

        for (int i = 0; i < columns_list.count() && OB_SUCC(ret); i++) {
          ParseNode* column_node = NULL;
          ParseNode* column_name = NULL;
          if (OB_FAIL(dml_resolver->create_col_ref_node(node.children_[0]->children_[1], columns_list.at(i).column_name_, column_node))) {
            LOG_WARN("fail to create column parse node", K(ret));
          } else if (OB_FAIL(dml_resolver->create_char_node(columns_list.at(i).column_name_, column_name))) {
            LOG_WARN("fail to create column name parse node", K(ret));
          } else {
            param_vec[para_idx++] = column_node;
            param_vec[para_idx++] = column_name;
          }
        }
        if (OB_SUCC(ret)) {
          node.num_child_ = param_count;
          node.children_ = param_vec;
          node.reserved_ = 1;
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
    if (OB_ISNULL(node.children_[i])) {
    } else if (OB_FAIL(SMART_CALL(geo_pre_process_mvt_agg(*node.children_[i], dml_resolver)))) {
      LOG_WARN("pre process dot notation failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObMultiModeDMLResolver::geo_resolve_mbr_column(ObDMLResolver* dml_resolver)
{
  int ret = OB_SUCCESS;
  // try to get mbr generated column
  for (int64_t j = 0; OB_SUCC(ret) && j < dml_resolver->gen_col_exprs_.count(); ++j) {
    const ObRawExpr *dep_expr = dml_resolver->gen_col_exprs_.at(j).dependent_expr_;
    if (dep_expr->get_expr_type() == T_FUN_SYS_SPATIAL_MBR) {
      ObColumnRefRawExpr *left_expr = NULL;
      ObDMLStmt *stmt = dml_resolver->gen_col_exprs_.at(j).stmt_;
      TableItem *table_item = dml_resolver->gen_col_exprs_.at(j).table_item_;
      const ObString &column_name = dml_resolver->gen_col_exprs_.at(j).column_name_;
      ColumnItem *col_item = NULL;
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (OB_FAIL(dml_resolver->resolve_basic_column_item(*table_item, column_name, true, col_item, stmt))) {
        LOG_WARN("resolve basic column reference failed", K(ret));
      } else if (OB_ISNULL(col_item) || OB_ISNULL(left_expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is invalid", K(col_item), K(left_expr));
      } else {
        left_expr->set_explicited_reference();
        if (OB_FAIL(stmt->add_column_item(*col_item))) {
          LOG_WARN("push back error", K(ret));
        }
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase