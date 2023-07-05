/**
 * Copyright (c) 2022 OceanBase
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
#include "ob_table_cg_service.h"
#include "share/datum/ob_datum_util.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "share/system_variable/ob_system_variable.h" // for ObBinlogRowImage::FULL
#include "sql/engine/expr/ob_expr_autoinc_nextval.h" // for ObAutoincNextvalExtra

namespace oceanbase
{
namespace table
{
ObRawExpr* ObTableExprCgService::get_ref_raw_expr(const ObIArray<ObRawExpr *> &all_exprs,
                                                  const ObString &col_name)
{
  bool found = false;
  ObColumnRefRawExpr *expr = nullptr;
  for (int64_t i = 0; i < all_exprs.count() && !found; i++) {
    if (all_exprs.at(i)->is_column_ref_expr()) {
      expr = static_cast<ObColumnRefRawExpr*>(all_exprs.at(i));
      if (0 == col_name.case_compare(expr->get_column_name())) {
        found = true;
      }
    }
  }

  return expr;
}

int ObTableExprCgService::build_generated_column_expr(ObTableCtx &ctx,
                                                      ObColumnRefRawExpr &col_expr,
                                                      const ObString &expr_str,
                                                      const ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(ctx));
  } else {
    ObArray<ObQualifiedName> columns;
    ObSchemaChecker schema_checker;
    ObSchemaGetterGuard &schema_guard = ctx.get_schema_guard();
    ObRawExprFactory &expr_factory = ctx.get_expr_factory();
    ObSQLSessionInfo &sess_info = ctx.get_session_info();
    ObRawExpr *gen_expr = nullptr;
    if (OB_FAIL(schema_checker.init(schema_guard))) {
      LOG_WARN("fail to init schema checker", K(ret));
    } else if(OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_str,
                                                                  expr_factory,
                                                                  sess_info,
                                                                  gen_expr,
                                                                  columns,
                                                                  table_schema,
                                                                  false, /* allow_sequence */
                                                                  nullptr,
                                                                  &schema_checker))) {
      LOG_WARN("fail to build generated expr", K(ret), K(expr_str), K(ctx));
    } else if (OB_ISNULL(gen_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated expr is null", K(ret));
    } else {
      ObRawExpr *real_ref_expr = nullptr;
      bool is_inc_or_append_replace = !ctx.get_delta_exprs().empty();
      if (is_inc_or_append_replace && 2 != columns.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid column for increment or append", K(ret), K(columns.count()));
      }
      ObIArray<ObTableCtx::ObGenDenpendantsPair> &gen_dependants_pairs = ctx.get_gen_dependants_pairs();
      ObTableCtx::ObGenDenpendantsPair pair;
      pair.first = &col_expr; // 记录生成列
      const ObIArray<ObRawExpr *> *src_exprs = &exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
        if (1 == i && is_inc_or_append_replace) {
          src_exprs = &ctx.get_delta_exprs();
        }
        if (OB_ISNULL(real_ref_expr = get_ref_raw_expr(*src_exprs,
                                                       columns.at(i).col_name_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get ref expr", K(ret), K(columns.at(i).ref_expr_->get_column_id()));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(gen_expr,
                                                              columns.at(i).ref_expr_,
                                                              real_ref_expr))) {
          LOG_WARN("fail to replace column reference expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(pair.second, real_ref_expr))) { // 记录依赖的列
          LOG_WARN("fail to add expr to array", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gen_expr->formalize(&sess_info))) {
          LOG_WARN("fail to formailize column reference expr", K(ret));
        } else if (ObRawExprUtils::need_column_conv(col_expr.get_result_type(), *gen_expr)) {
          if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(expr_factory,
                                                             ctx.get_allocator(),
                                                             col_expr,
                                                             gen_expr,
                                                             &sess_info))) {
            LOG_WARN("fail to build column convert expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        gen_expr->set_for_generated_column();
        col_expr.set_dependant_expr(gen_expr);
        if (OB_FAIL(gen_dependants_pairs.push_back(pair))) {
          LOG_WARN("fail to push back generate ref expr pair", K(ret), K(gen_dependants_pairs));
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::resolve_generated_column_expr(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr *> &exprs = (ctx.is_for_update() || ctx.is_for_insertup()) ?
      ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (exprs.at(i)->is_column_ref_expr()) {
      ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(exprs.at(i));
      if (ref_expr->is_generated_column()) {
        const ObColumnSchemaV2 *gen_col_schema = nullptr;
        if (OB_ISNULL(gen_col_schema = ctx.get_table_schema()->get_column_schema(ref_expr->get_column_id()))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("fail to get generated column schema", K(ret), K(ref_expr->get_column_id()));
        } else if (OB_FAIL(build_generated_column_expr(ctx,
                                                       *ref_expr,
                                                       gen_col_schema->get_cur_default_value().get_string(),
                                                       exprs))) {
          LOG_WARN("fail to build generated raw expr", K(ret), K(*ref_expr));
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::generate_column_ref_raw_expr(ObTableCtx &ctx,
                                                       const ObColumnSchemaV2 &col_schema,
                                                       ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;

  ObColumnRefRawExpr *col_ref_expr = NULL;
  // 存储最开始生成的列原生表达式, 用于后续生成主键冲突
  ObIArray<ObColumnRefRawExpr*> &all_column_exprs = ctx.get_all_column_ref_exprs();
  // 在update场景下, 会多次调用该函数，我们只需要存最开始生成的列引用表达式，记录下是否存储满
  bool is_generate_full = (ctx.get_table_schema()->get_column_count() == all_column_exprs.count());

  if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx.get_expr_factory(),
                                                col_schema,
                                                col_ref_expr))) {
    LOG_WARN("fail to build column expr", K(ret), K(col_schema));
  } else if (OB_ISNULL(col_ref_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (FALSE_IT(expr = col_ref_expr)) {
  } 
  if (!is_generate_full) {
    if (OB_FAIL(all_column_exprs.push_back(col_ref_expr))) {
      LOG_WARN("fail to push back column ref expr to all column exprs", K(ret));  
    }
  }
  if (col_schema.is_autoincrement() && (ctx.get_opertion_type() != ObTableOperationType::DEL)    // 特判下，若为delete/update/get/scan操作
                                    && (ctx.get_opertion_type() != ObTableOperationType::UPDATE) // 则走原有的列引用表达式
                                    && (ctx.get_opertion_type() != ObTableOperationType::GET)    // 自增表达式结构为: con_conv_expr - auto_inc_expr - con_conv_expr
                                    && (ctx.get_opertion_type() != ObTableOperationType::SCAN)) {
    if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(ctx.get_expr_factory(),
                                                       ctx.get_allocator(),
                                                        *col_ref_expr,
                                                        expr,
                                                        &ctx.get_session_info()))) {
      LOG_WARN("fail to build column conv expr", K(ret), K(*col_ref_expr));
    } else if (OB_FAIL(generate_autoinc_nextval_expr(ctx, expr, col_schema))) {
      LOG_WARN("fail to generate auto inc next val expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(ctx.get_expr_factory(),
                                                              ctx.get_allocator(),
                                                              *col_ref_expr,
                                                              expr,
                                                              &ctx.get_session_info()))) {
        LOG_WARN("fail to build column conv expr", K(ret));
    }
  }
  return ret;
}

int ObTableExprCgService::generate_exprs(ObTableCtx &ctx,
                                          oceanbase::common::ObIAllocator &allocator,
                                          oceanbase::sql::ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  if (ctx.is_for_update() || ctx.is_for_insertup()) {
    if (OB_FAIL(generate_update_raw_exprs(ctx))) {
      LOG_WARN("fail to generate update raw exprs", K(ret), K(ctx));
    }
  } else if (OB_FAIL(generate_column_raw_exprs(ctx))) {
    LOG_WARN("fail to generate column raw exprs", K(ret), K(ctx));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_expr_frame_info(ctx, allocator, expr_frame_info))) {
      LOG_WARN("fail to generate expr frame info", K(ret), K(ctx));
    }
  }
  return ret;
}

int ObTableExprCgService::generate_column_raw_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObIArray<ObRawExpr *> *exprs = (ctx.is_for_update() || ctx.is_for_insertup())?
      &ctx.get_old_row_exprs() : const_cast<ObIArray<ObRawExpr *> *>(&ctx.get_all_exprs().get_expr_array());

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(ctx));
  } else {
    ObRawExpr *col_ref_expr = nullptr;
    const ObColumnSchemaV2 *column_schema = nullptr;
    const uint64_t column_cnt = table_schema->get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
      if (OB_ISNULL(column_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret));
      } else if (OB_FAIL(generate_column_ref_raw_expr(ctx, *column_schema, col_ref_expr))) {
        LOG_WARN("fail to generate column ref raw expr", K(ret), K(*column_schema));
      } else if (OB_FAIL(exprs->push_back(col_ref_expr))) {
        LOG_WARN("fail to push back column ref raw expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && table_schema->has_generated_column()) {
    if (OB_FAIL(resolve_generated_column_expr(ctx))) {
      LOG_WARN("fail to resolver generated column expr", K(ret));
    }
  }

  return ret;
}

int ObTableExprCgService::generate_full_assign_raw_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObIArray<sql::ObRawExpr *> &full_assign_exprs = ctx.get_full_assign_exprs();
  bool is_inc_or_append = ctx.is_inc_or_append();
  bool has_stored_gen_col = false;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(ctx));
  } else {
    ObRawExpr *col_ref_expr = nullptr;
    const ObColumnSchemaV2 *column_schema = nullptr;
    const uint64_t column_cnt = table_schema->get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
      bool is_virtual_col = false;
      if (OB_ISNULL(column_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(i));
      } else if (column_schema->is_virtual_generated_column()) {
        is_virtual_col = true;
      } else if (column_schema->is_stored_generated_column()) {
        has_stored_gen_col = true;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(!is_virtual_col && generate_column_ref_raw_expr(ctx, *column_schema, col_ref_expr))) {
        LOG_WARN("fail to generate column ref raw expr", K(ret), K(*column_schema));
      } else if (OB_FAIL(!is_virtual_col && full_assign_exprs.push_back(col_ref_expr))) {
        LOG_WARN("fail to push back column ref raw expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && has_stored_gen_col) {
    for (int64_t i = 0; OB_SUCC(ret) && i < full_assign_exprs.count(); i++) {
      if (full_assign_exprs.at(i)->is_column_ref_expr()) {
        ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(full_assign_exprs.at(i));
        if (ref_expr->is_stored_generated_column()) {
          const ObColumnSchemaV2 *gen_col_schema = nullptr;
          if (OB_ISNULL(gen_col_schema = ctx.get_table_schema()->get_column_schema(ref_expr->get_column_id()))) {
            ret = OB_SCHEMA_ERROR;
            LOG_WARN("fail to get generated column schema", K(ret), K(ref_expr->get_column_id()));
          } else if (OB_FAIL(build_generated_column_expr(ctx,
                                                         *ref_expr,
                                                         gen_col_schema->get_cur_default_value().get_string(),
                                                         full_assign_exprs))) {
            LOG_WARN("fail to build generated column expr", K(ret), K(*ref_expr));
          }
        }
      }
    }
  }

  // 构造delta expr，作为生成列表达式的参数表达式
  // 针对increment和append场景，将对应的列表达式设置为stored生成列表达式，并且构造生成列
  if (OB_SUCC(ret) && is_inc_or_append) {
    const ObColumnSchemaV2 *column_schema = nullptr;
    ObRawExpr *col_ref_expr = nullptr;
    ObIArray<sql::ObRawExpr *> &delta_exprs = ctx.get_delta_exprs();
    const ObIArray<ObString> &expr_strs = ctx.get_expr_strs();
    const ObTableCtx::ObAssignIds &assign_ids = ctx.get_assign_ids();
    const int64_t N = assign_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      uint64_t idx = assign_ids.at(i).idx_;
      uint64_t column_id = assign_ids.at(i).column_id_;
      if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(column_id));
      } else if (column_schema->is_autoincrement()) { // do not support delta on auto increment column currently
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("auto increment column do not support delta", K(ret), K(*column_schema));
      } else if (OB_FAIL(generate_column_ref_raw_expr(ctx, *column_schema, col_ref_expr))) {
        LOG_WARN("fail to generate column ref raw expr", K(ret), K(*column_schema));
      } else if (OB_FAIL(delta_exprs.push_back(col_ref_expr))) {
        LOG_WARN("fail to push back column ref raw expr", K(ret));
      } else {
        ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(full_assign_exprs.at(idx));
        ref_expr->set_column_flags(ref_expr->get_column_flags() | STORED_GENERATED_COLUMN_FLAG);
        if (OB_FAIL(build_generated_column_expr(ctx,
                                                *ref_expr,
                                                expr_strs.at(i),
                                                ctx.get_old_row_exprs()))) {
          LOG_WARN("fail to build generated column expr", K(ret), K(*ref_expr), K(expr_strs.at(i)));
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::generate_assign_exprs(ObTableCtx &ctx,
                                                const ObTableCtx::ObAssignIds &assign_ids,
                                                common::ObIArray<sql::ObRawExpr *> &assign_exprs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr *> &full_exprs = ctx.get_full_assign_exprs();

  if (assign_ids.count() > full_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid assign ids count", K(ret), K(assign_ids.count()), K(full_exprs.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_ids.count(); i++) {
      const ObTableCtx::ObAssignId &assign_id = assign_ids.at(i);
      if (OB_FAIL(assign_exprs.push_back(full_exprs.at(assign_id.idx_)))) {
        LOG_WARN("fail to push back assign expr", K(ret), K(i), K(assign_id));
      }
    }
  }

  return ret;
}

int ObTableExprCgService::generate_update_raw_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_column_raw_exprs(ctx))) {
    LOG_WARN("fail to generate column raw exprs", K(ret), K(ctx));
  } else if (OB_FAIL(generate_full_assign_raw_exprs(ctx))) {
    LOG_WARN("fail to generate assign raw exprs", K(ret), K(ctx));
  } else {
    ObRawExprUniqueSet &all_exprs = ctx.get_all_exprs();
    ObIArray<sql::ObRawExpr *> &old_row_exprs = ctx.get_old_row_exprs();
    ObIArray<sql::ObRawExpr *> &assign_exprs = ctx.get_full_assign_exprs();
    ObIArray<sql::ObRawExpr *> &delta_exprs = ctx.get_delta_exprs();

    // 将 old row exprs加到all exprs里面
    for (int64_t i = 0; OB_SUCC(ret) && i < old_row_exprs.count(); i++) {
      if (OB_FAIL(all_exprs.append(old_row_exprs.at(i)))) {
        LOG_WARN("fail to append old row expr to all exprs", K(ret), K(i));
      }
    }

    // 将 assign row exprs加到all exprs里面
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_exprs.count(); i++) {
      if (OB_FAIL(all_exprs.append(assign_exprs.at(i)))) {
        LOG_WARN("fail to append assign expr to all exprs", K(ret), K(i));
      }
    }

    // 将 delta exprs加到all exprs里面
    for (int64_t i = 0; OB_SUCC(ret) && i < delta_exprs.count(); i++) {
      if (OB_FAIL(all_exprs.append(delta_exprs.at(i)))) {
        LOG_WARN("fail to append delta expr to all exprs", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTableExprCgService::generate_expr_frame_info(ObTableCtx &ctx,
                                                     common::ObIAllocator &allocator,
                                                     ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  ObStaticEngineExprCG expr_cg(allocator,
                               &ctx.get_session_info(),
                               &ctx.get_schema_guard(),
                               0,
                               0,
                               ctx.get_cur_cluster_version());
  if (OB_FAIL(expr_cg.generate(ctx.get_all_exprs(), expr_frame_info))) {
    LOG_WARN("fail to generate expr frame info by expr cg", K(ret), K(ctx));
  }
  return ret;
}

int ObTableExprCgService::alloc_exprs_memory(ObTableCtx &ctx, ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = ctx.get_exec_ctx();
  uint64_t frame_cnt = 0;
  char **frames = NULL;
  common::ObArray<char*> param_frame_ptrs;

  if (OB_FAIL(expr_frame_info.alloc_frame(ctx.get_allocator(),
                                          param_frame_ptrs,
                                          frame_cnt,
                                          frames))) {
    LOG_WARN("fail to alloc frame", K(ret), K(expr_frame_info));
  } else {
    exec_ctx.set_frame_cnt(frame_cnt);
    exec_ctx.set_frames(frames);
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiScanSpec &spec)
{
  int ret = OB_SUCCESS;
  // init tsc_ctdef_
  if (OB_FAIL(ObTableTscCgService::generate_tsc_ctdef(ctx, alloc, spec.get_ctdef()))) {
    LOG_WARN("fail to generate table scan ctdef", K(ret), K(ctx));
  }

  return ret;
}

/*
             table_loc_id_    ref_table_id_
    主表:     主表table_id     主表table_id
    索引表:   主表table_id     索引表table_id
    回表:     主表table_id     主表table_id
*/
int ObTableLocCgService::generate_table_loc_meta(const ObTableCtx &ctx,
                                                 ObDASTableLocMeta &loc_meta,
                                                 bool is_lookup)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  const ObTableSchema *index_schema = ctx.get_index_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (ctx.is_index_scan() && OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is null", K(ret));
  } else {
    loc_meta.reset();
    // is_lookup 有什么用？好像都是 false
    loc_meta.ref_table_id_ = is_lookup ? ctx.get_ref_table_id() : ctx.get_index_table_id();
    loc_meta.table_loc_id_ = ctx.get_ref_table_id();
    if (is_lookup) {
      loc_meta.is_dup_table_ = table_schema->is_duplicate_table();
    } else {
      loc_meta.is_dup_table_ = ctx.is_index_scan() ? index_schema->is_duplicate_table()
                              : table_schema->is_duplicate_table();
    }
    if (ctx.is_weak_read()) {
      loc_meta.is_weak_read_ = 1;
      loc_meta.select_leader_ = 0;
    } else if (loc_meta.is_dup_table_) {
      loc_meta.select_leader_ = 0;
      loc_meta.is_weak_read_ = 0;
    } else {
      //strong consistency read policy is used by default
      loc_meta.select_leader_ = 1;
      loc_meta.is_weak_read_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    const ObIArray<common::ObTableID> &related_index_ids = ctx.get_related_index_ids();
    loc_meta.related_table_ids_.set_capacity(related_index_ids.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < related_index_ids.count(); i++) {
      if (OB_FAIL(loc_meta.related_table_ids_.push_back(related_index_ids.at(i)))) {
        LOG_WARN("fail to store related table id", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTableExprCgService::refresh_update_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &old_row,
                                                     const ObIArray<ObExpr *> &new_row,
                                                     const ObIArray<ObExpr *> &full_assign_row,
                                                     const ObTableCtx::ObAssignIds &assign_ids,
                                                     const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(refresh_assign_exprs_frame(ctx,
                                         old_row,
                                         new_row,
                                         full_assign_row,
                                         assign_ids,
                                         entity))) {
    LOG_WARN("fail to refresh assign exprs frame", K(ret), K(assign_ids));
  }

  return ret;
}

int ObTableExprCgService::refresh_insert_up_exprs_frame(ObTableCtx &ctx,
                                                        const ObIArray<ObExpr *> &ins_new_row,
                                                        const ObIArray<ObExpr *> &delta_exprs,
                                                        const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObObj> &rowkey = entity.get_rowkey_objs();

  if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, ins_new_row, rowkey))) {
    LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
  } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, ins_new_row, entity))) {
    LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx));
  } else if (ctx.is_inc_or_append() && OB_FAIL(refresh_delta_exprs_frame(ctx, delta_exprs, entity))) {
    LOG_WARN("fail to init delta exprs frame", K(ret), K(ctx));
  }

  return ret;
}

int ObTableExprCgService::refresh_insert_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObTableEntity &entity)
{
  return refresh_exprs_frame(ctx, exprs, entity);
}

int ObTableExprCgService::refresh_replace_exprs_frame(ObTableCtx &ctx,
                                                      const ObIArray<ObExpr *> &exprs,
                                                      const ObTableEntity &entity)
{
  return refresh_exprs_frame(ctx, exprs, entity);
}

// only for htable
int ObTableExprCgService::refresh_delete_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObj, 3> rowkey;
  ObObj k_obj;
  ObObj q_obj;
  ObObj t_obj;
  int64_t time = 0;

  // htable场景rowkey都在properties中，所以需要从properties中提取出rowkey
  if (OB_FAIL(entity.get_property(ObHTableConstants::ROWKEY_CNAME_STR, k_obj))) {
    LOG_WARN("fail to get K", K(ret));
  } else if (OB_FAIL(entity.get_property(ObHTableConstants::CQ_CNAME_STR, q_obj))) {
    LOG_WARN("fail to get Q", K(ret));
  } else if (OB_FAIL(entity.get_property(ObHTableConstants::VERSION_CNAME_STR, t_obj))) {
    LOG_WARN("fail to get T", K(ret));
  } else if (OB_FAIL(rowkey.push_back(k_obj))) {
    LOG_WARN("fail to push back k_obj", K(ret), K(k_obj));
  } else if (OB_FAIL(rowkey.push_back(q_obj))) {
    LOG_WARN("fail to push back q_obj", K(ret), K(q_obj));
  } else if (FALSE_IT(time = t_obj.get_int())) {
    // do nothing
  } else if (FALSE_IT(t_obj.set_int(-1 * time))) {
    // do nothing
  } else if (OB_FAIL(rowkey.push_back(t_obj))) {
    LOG_WARN("fail to push back t_obj", K(ret), K(t_obj));
  } else if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, exprs, rowkey))) {
    LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
  } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, exprs, entity))) {
    LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx));
  }

  return ret;
}

int ObTableExprCgService::write_datum(ObTableCtx &ctx,
                                      ObIAllocator &allocator,
                                      const ObExpr &expr,
                                      ObEvalCtx &eval_ctx,
                                      const ObObj &obj)
{
  int ret = OB_SUCCESS;
  const ObExpr *write_expr = &expr;

  if (is_lob_storage(obj.get_type()) && (obj.has_lob_header() != expr.obj_meta_.has_lob_header())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check lob header", K(ret), K(expr), K(obj));
  } else if (expr.type_ == T_FUN_COLUMN_CONV && ctx.is_auto_inc()) { // 为列转换表达式, 且为自增场景下
    if (expr.arg_cnt_ != 6) {
      ret = OB_ERR_UNDEFINED;
      LOG_WARN("invalid arg count for auto inc expr", K(ret), K(expr));
    } else {
      const ObExpr *auto_inc_expr = expr.args_[4]; // 取出中间的列自增表达式
      if (auto_inc_expr->get_eval_info(eval_ctx).evaluated_ == true) {
        // do nothing
      } else if (auto_inc_expr->arg_cnt_ != 1) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("invalid arg count for auto inc expr", K(ret), K(*auto_inc_expr));
      } else {
        write_expr = auto_inc_expr->args_[0];  // 取出底部的列转换表达式
      }
      // 用于生成自增主键冲突的列引用表达式，在write_datum时刷值
      ObExpr *auto_inc_ref_col_expr = write_expr->args_[4];  
      ObDatum &datum = auto_inc_ref_col_expr->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(datum.from_obj(obj))) {
        LOG_WARN("fail to convert object from datum", K(ret), K(obj));
      } else {
        auto_inc_ref_col_expr->get_eval_info(eval_ctx).evaluated_ = true;
        auto_inc_ref_col_expr->get_eval_info(eval_ctx).projected_ = true;
      }  
    }
  }

  if (OB_SUCC(ret)) {
    ObDatum &datum = write_expr->locate_datum_for_write(eval_ctx);
    if (OB_FAIL(datum.from_obj(obj))) {
      LOG_WARN("fail to convert object from datum", K(ret), K(obj));
    } else {
      write_expr->get_eval_info(eval_ctx).evaluated_ = true;
      write_expr->get_eval_info(eval_ctx).projected_ = true;
    }
  }

  return ret;
}

int ObTableExprCgService::refresh_exprs_frame(ObTableCtx &ctx,
                                              const ObIArray<ObExpr *> &exprs,
                                              const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObObj> &rowkey = entity.get_rowkey_objs();

  if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, exprs, rowkey))) {
    LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
  } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, exprs, entity))) {
    LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx));
  }

  return ret;
}

// exprs必须是按照schema序的完整行
	int ObTableExprCgService::refresh_rowkey_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObIArray<ObObj> &rowkey)
{
  int ret = OB_SUCCESS;
  // 原定是用户在自增场景下主键可不传数据，而目前的实现是主键传0为自增，暂时保留校验修改
  ObObj null_obj;
  null_obj.set_null();
  int64_t rowkey_cnt = ctx.get_table_schema()->get_rowkey_column_num();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());
  bool is_full_filled = rowkey_cnt == rowkey.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
    const ObExpr *expr = exprs.at(i);
    if (ctx.is_auto_inc() && !is_full_filled && expr->type_ == T_FUN_COLUMN_CONV) {
      const ObObj *obj = NULL; // 若不填, 只有自增的列不填, 则写入null值
      if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *expr, eval_ctx, *obj))) {
        LOG_WARN("fail to write datum", K(ret), K(rowkey.at(i)), K(*expr));
      } 
    } else if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *expr, eval_ctx, rowkey.at(i)))) {
      LOG_WARN("fail to write datum", K(ret), K(rowkey.at(i)), K(*expr));
    }
  }
  return ret;
}

// exprs必须是按照schema序的完整行
int ObTableExprCgService::refresh_properties_exprs_frame(ObTableCtx &ctx,
                                                         const ObIArray<ObExpr *> &exprs,
                                                         const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    ObObj prop_value;
    ObObj *obj = nullptr;
    const int64_t rowkey_column_cnt = table_schema->get_rowkey_column_num();
    for (int64_t i = rowkey_column_cnt; OB_SUCC(ret) && i < exprs.count(); i++) {
      const ObExpr *expr = exprs.at(i);
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(i), K(*table_schema));
      } else if (col_schema->is_generated_column()) { // 生成列需要eval一次
        ObDatum *tmp_datum = nullptr;
        if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
          LOG_WARN("fail to eval generate expr", K(ret));
        }
      } else {
        const ObString &col_name = col_schema->get_column_name_str();
        // 这里使用schema的列名在entity中查找property，有可能出现本身entity中的prop_name是不对的，导致找不到
        bool use_default = (OB_SEARCH_NOT_FOUND == entity.get_property(col_name, prop_value));
        if (use_default) {
          obj = const_cast<ObObj *>(&col_schema->get_cur_default_value());
        } else {
          obj = &prop_value;
        }
        if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *expr, eval_ctx, *obj))) {
          LOG_WARN("fail to write datum", K(ret), K(*obj), K(*expr));
        }
      }
    }
  }

  return ret;
}

// 将生成列引用的，并且entity没有填充的列刷成旧值
// eg: create table t(c1 int primary key,
//                    c2 varchar(10),
//                    c3 varchar(10),
//                    gen varchar(30) generated always as (concat(c2,c3)) stored);
// update t set c3 = ? where c1 = ?;
// full row: c1(old) | c2(old) | c3(old) | gen(old) concat(c1(old), c2(old))
//           c1(new) | c2(new) | c3(new) | gen(new) concat(c1(new), c2(new))
// 这个时候，只是更新了c3的值，gen引用的是c2,c3的值，这里将c2(old)填到c2(new)中
int ObTableExprCgService::refresh_generated_column_related_frame(ObTableCtx &ctx,
                                                                 const ObIArray<ObExpr *> &old_row,
                                                                 const ObIArray<ObExpr *> &full_assign_row,
                                                                 const ObTableCtx::ObAssignIds &assign_ids,
                                                                 const ObColumnSchemaV2 &col_schema)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());
  ObArray<uint64_t> schema_column_ids;
  ObArray<uint64_t> ref_column_ids;

  if (!col_schema.is_stored_generated_column()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid expr", K(ret), K(col_schema));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(table_schema->get_column_ids(schema_column_ids))) {
    LOG_WARN("fail to get schema column ids", K(ret));
  } else if (OB_FAIL(col_schema.get_cascaded_column_ids(ref_column_ids))) {
    LOG_WARN("fail to get cascade column ids", K(ret));
  } else {
    ObArray<uint64_t> not_upd_col_ids;
    // 查询生成列依赖的列是否都被更新，记下未被更新的列到not_upd_col_ids
    for (int64_t i = 0; OB_SUCC(ret) && i < ref_column_ids.count(); i++) {
      bool found = false;
      uint64_t col_id = ref_column_ids.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < assign_ids.count() && !found; j++) {
        if (col_id == assign_ids.at(j).column_id_) {
          found = true;
        }
      }
      if (!found && OB_FAIL(not_upd_col_ids.push_back(col_id))) {
        LOG_WARN("fail to push back not update column id", K(ret), K(col_id));
      }
    }

    // 通过column id找到old row对应的列
    for (int64_t i = 0; OB_SUCC(ret) && i < not_upd_col_ids.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < schema_column_ids.count(); j++) {
        if (schema_column_ids.at(j) == not_upd_col_ids.at(i)) {
          if (j >= old_row.count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid column id", K(ret), K(j), K(old_row.count()));
          } else if (j >= full_assign_row.count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid column id", K(ret), K(j), K(full_assign_row.count()));
          } else {
            ObExpr *old_expr = old_row.at(j);
            ObExpr *new_expr = full_assign_row.at(j);
            new_expr->locate_expr_datum(eval_ctx) = old_expr->locate_expr_datum(eval_ctx);
            new_expr->get_eval_info(eval_ctx).evaluated_ = true;
            new_expr->get_eval_info(eval_ctx).projected_ = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::refresh_assign_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &old_row,
                                                     const ObIArray<ObExpr *> &new_row,
                                                     const ObIArray<ObExpr *> &full_assign_row,
                                                     const ObTableCtx::ObAssignIds &assign_ids,
                                                     const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    ObObj prop_value;
    const int64_t N = assign_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      uint64_t assign_id = assign_ids.at(i).idx_;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(assign_id))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(assign_id), K(*table_schema));
      } else if (col_schema->is_virtual_generated_column()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not have virtual generated expr", K(ret));
      } else if (col_schema->is_stored_generated_column()) {
        if (OB_FAIL(refresh_generated_column_related_frame(ctx,
                                                           old_row,
                                                           full_assign_row,
                                                           assign_ids,
                                                           *col_schema))) {
          LOG_WARN("fail to refresh generated column related frame", K(ret));
        } else { // 计算一次
          ObExpr *expr = full_assign_row.at(assign_id);
          ObDatum *tmp_datum = nullptr;
          if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
            LOG_WARN("fail to eval datum", K(ret), K(*expr));
          }
        }
      } else if (OB_FAIL(entity.get_property(col_schema->get_column_name_str(), prop_value))) {
        LOG_WARN("fail to get assign propertity value", K(ret), K(col_schema->get_column_name_str()));
      } else {
        if (assign_id >= new_row.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid assign idx", K(ret), K(assign_id), K(new_row.count()));
        } else {
          const ObExpr *expr = new_row.at(assign_id);
          if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *expr, eval_ctx, prop_value))) {
            LOG_WARN("fail to write datum", K(ret), K(prop_value), K(*expr));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::refresh_delta_exprs_frame(ObTableCtx &ctx,
                                                    const ObIArray<ObExpr *> &delta_exprs,
                                                    const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());

  if (!ctx.is_inc_or_append()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation type", K(ret), K(ctx));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    ObObj prop_value;
    const ObTableCtx::ObAssignIds &assign_ids = ctx.get_assign_ids();
    const int64_t N = assign_ids.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      uint64_t column_id = assign_ids.at(i).column_id_;
      if (OB_ISNULL(col_schema = table_schema->get_column_schema(column_id))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(column_id), K(*table_schema));
      } else if (OB_FAIL(entity.get_property(col_schema->get_column_name_str(), prop_value))) {
        LOG_WARN("fail to get assign propertity value", K(ret), K(col_schema->get_column_name_str()));
      } else {
        const ObExpr *expr = delta_exprs.at(i);
        if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *expr, eval_ctx, prop_value))) {
          LOG_WARN("fail to write datum", K(ret), K(prop_value), K(*expr));
        }
      }
    }
  }

  return ret;
}
	
int ObTableDmlCgService::replace_exprs_with_dependant(const ObIArray<ObRawExpr *> &src_exprs,
                                                      ObIArray<ObRawExpr *> &dst_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < src_exprs.count() && OB_SUCC(ret); i++) {
    ObRawExpr *expr = src_exprs.at(i);
    if (expr->is_column_ref_expr()) {
      ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(expr);
      if (col_ref_expr->is_generated_column()) {
        expr = col_ref_expr->get_dependant_expr();
      }
      if (OB_FAIL(dst_exprs.push_back(expr))) {
        LOG_WARN("fail to push back expr", K(ret), K(dst_exprs));
      }
    } else if (expr->get_expr_type() == T_FUN_COLUMN_CONV) { // 兼容自增场景下的列转换表达式
      if (OB_FAIL(dst_exprs.push_back(expr))) {
        LOG_WARN("fail to push back expr", K(ret), K(dst_exprs));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr type", K(ret), K(*expr));
    }
  }
  return ret;
}

int ObTableDmlCgService::generate_insert_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObTableInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  const ObIArray<ObRawExpr *> &exprs = ctx.is_for_insertup() ?
      ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();
  ObSEArray<ObRawExpr*, 64> tmp_exprs;
  if (OB_FAIL(replace_exprs_with_dependant(exprs, tmp_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret), K(exprs));
  } else if (OB_FAIL(new_row.assign(tmp_exprs))) {
    LOG_WARN("fail to assign new row", K(ret));
  } else if (OB_FAIL(generate_base_ctdef(ctx,
                                         ins_ctdef,
                                         old_row,
                                         new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                            ctx.get_ref_table_id(),
                                            ins_ctdef.das_ctdef_,
                                            new_row))) {
    LOG_WARN("fail to generate das insert ctdef", K(ret));
  } else if (OB_FAIL(generate_related_ins_ctdef(ctx,
                                                allocator,
                                                new_row,
                                                ins_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related ins ctdef", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_update_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               const ObTableCtx::ObAssignIds &assign_ids,
                                               ObTableUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  ObSEArray<ObRawExpr*, 64> full_row;
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  ObIArray<sql::ObRawExpr *> &old_exprs = ctx.get_old_row_exprs();
  ObSEArray<ObRawExpr*, 64> tmp_old_exprs;
  ObSEArray<ObRawExpr*, 64> tmp_full_assign_exprs;

  if (OB_FAIL(replace_exprs_with_dependant(old_exprs, tmp_old_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret), K(old_exprs));
  } else if (OB_FAIL(old_row.assign(tmp_old_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(new_row.assign(old_row))) {
    LOG_WARN("fail to assign new row", K(ret));
  } else if (OB_FAIL(append(full_row, old_row))) {
    LOG_WARN("fail to append old row expr to full row", K(ret), K(old_row));
  } else if (OB_FAIL(ObTableExprCgService::generate_assign_exprs(ctx, assign_ids, assign_exprs))) {
    LOG_WARN("fail to generate assign exprs", K(ret), K(ctx), K(assign_ids));
  } else {
    ObColumnRefRawExpr *col_ref_expr = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < assign_exprs.count(); i++) {
      ObRawExpr *expr = assign_exprs.at(i);
      if (expr->get_expr_type() == T_FUN_COLUMN_CONV && ctx.is_auto_inc()) { // 兼容自增场景下的列转换表达式
        if (OB_FAIL(full_row.push_back(expr))) {
          LOG_WARN("fail to add assign expr to full row", K(ret), K(i));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < old_exprs.count(); j++) {
          if (old_exprs.at(j)->get_expr_type() == T_FUN_COLUMN_CONV) {
            new_row.at(j) = expr;
          }
        }
      } else {
        if (!expr->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr type", K(ret), K(*expr));
        } else if (FALSE_IT(col_ref_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
        } else if (col_ref_expr->is_generated_column()) {
          expr = col_ref_expr->get_dependant_expr();
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(full_row.push_back(expr))) {
          LOG_WARN("fail to add assign expr to full row", K(ret), K(i));
        } else {
          ObColumnRefRawExpr *old_col_expr = nullptr;
          for (int64_t j = 0; OB_SUCC(ret) && j < old_exprs.count(); j++) {
            if (!old_exprs.at(j)->is_column_ref_expr() && (old_exprs.at(j)->get_expr_type() != T_FUN_COLUMN_CONV)) { // 兼容自增场景下的列转换表达式
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected expr type", K(ret), K(*old_exprs.at(i)));
            } else if (old_exprs.at(j)->get_expr_type() == T_FUN_COLUMN_CONV) {
              // do nothing
            } else if (FALSE_IT(old_col_expr = static_cast<ObColumnRefRawExpr*>(old_exprs.at(j)))) {
            } else if (old_col_expr->get_column_id() == col_ref_expr->get_column_id()) {
              new_row.at(j) = expr;
            }
          }
        }        
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(generate_base_ctdef(ctx,
                                         upd_ctdef,
                                         old_row,
                                         new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(full_row, upd_ctdef.full_row_))) {
    LOG_WARN("fail to generate dml update full row exprs", K(ret), K(full_row));
  } else if (OB_FAIL(replace_exprs_with_dependant(ctx.get_full_assign_exprs(), tmp_full_assign_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(tmp_full_assign_exprs, upd_ctdef.full_assign_row_))) {
    LOG_WARN("fail to generate full assign row exprs", K(ret));
  } else if (ctx.is_inc_or_append() && OB_FAIL(cg.generate_rt_exprs(ctx.get_delta_exprs(), upd_ctdef.delta_exprs_))) {
    LOG_WARN("fail to generate delta exprs", K(ret));
  } else if (OB_FAIL(generate_das_upd_ctdef(ctx,
                                            ctx.get_ref_table_id(),
                                            assign_exprs,
                                            upd_ctdef.das_ctdef_,
                                            old_row,
                                            new_row,
                                            full_row))) {
    LOG_WARN("fail to generate das upd ctdef", K(ret));
  } else if (OB_FAIL(generate_related_upd_ctdef(ctx,
                                                allocator,
                                                assign_exprs,
                                                old_row,
                                                new_row,
                                                full_row,
                                                upd_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related upd ctdef", K(ret));
  } else if (OB_FAIL(generate_upd_assign_infos(ctx, allocator, assign_exprs, upd_ctdef))) {
    LOG_WARN("fail to generate related upd assign info", K(ret));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ctx.is_for_insertup()) {
    ObDMLCtDefAllocator<ObDASDelCtDef> ddel_allocator(allocator);
    ObDMLCtDefAllocator<ObDASInsCtDef> dins_allocator(allocator);
    if (OB_ISNULL(upd_ctdef.ddel_ctdef_ = ddel_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate das del ctdef", K(ret));
    } else if (OB_ISNULL(upd_ctdef.dins_ctdef_ = dins_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate das ins ctdef", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                              ctx.get_ref_table_id(),
                                              *upd_ctdef.ddel_ctdef_,
                                              old_row))) {
      LOG_WARN("fail to generate das delete ctdef for update", K(ret));
    } else if (OB_FAIL(generate_related_del_ctdef(ctx,
                                                  allocator,
                                                  old_row,
                                                  upd_ctdef.related_del_ctdefs_))) {
      LOG_WARN("fail to generate related del ctdef", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                              ctx.get_ref_table_id(),
                                              *upd_ctdef.dins_ctdef_,
                                              new_row))) {
      LOG_WARN("fail to generate das insert ctdef for update", K(ret));
    } else if (OB_FAIL(generate_related_ins_ctdef(ctx,
                                                  allocator,
                                                  new_row,
                                                  upd_ctdef.related_ins_ctdefs_))) {
      LOG_WARN("fail to generate related ins ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_das_upd_ctdef(ObTableCtx &ctx,
                                                uint64_t index_tid,
                                                const ObIArray<ObRawExpr *> &assign_exprs,
                                                ObDASUpdCtDef &das_upd_ctdef,
                                                const ObIArray<ObRawExpr*> &old_row,
                                                const ObIArray<ObRawExpr*> &new_row,
                                                const ObIArray<ObRawExpr*> &full_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_upd_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_updated_column_ids(ctx,
                                                 assign_exprs,
                                                 das_upd_ctdef.column_ids_,
                                                 das_upd_ctdef.updated_column_ids_))) {
    LOG_WARN("fail to add updated column ids", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx,
                                         ctx.get_old_row_exprs(),
                                         dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids, // new row and old row's columns id
                                        das_upd_ctdef.column_ids_, // schmea column ids for given index_tid
                                        old_row,
                                        new_row,
                                        full_row,
                                        das_upd_ctdef))) {
    LOG_WARN("fail to generate projector", K(ret), K(full_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_updated_column_ids(ObTableCtx &ctx,
                                                     const ObIArray<ObRawExpr *> &assign_exprs,
                                                     const ObIArray<uint64_t> &column_ids,
                                                     ObIArray<uint64_t> &updated_column_ids)
{
  int ret = OB_SUCCESS;
  updated_column_ids.reset();

  if (!assign_exprs.empty()) {
    if (OB_FAIL(updated_column_ids.reserve(assign_exprs.count()))) {
      LOG_WARN("fail to init updated column ids array", K(ret), K(assign_exprs.count()));
    } else {
      ObColumnRefRawExpr *col_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < assign_exprs.count(); i++) {
        if (assign_exprs.at(i)->get_expr_type() == T_FUN_COLUMN_CONV && ctx.is_auto_inc()) { // 兼容自增场景下的列转换表达式
          if (OB_FAIL(updated_column_ids.push_back(ctx.get_auto_inc_column_id()))) {
            LOG_WARN("fail to add updated column id", K(ret), K(ctx.get_auto_inc_column_id()));
          }
        } else {
          if (!assign_exprs.at(i)->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected expr type", K(ret), K(assign_exprs.at(i)->get_expr_type()));
          } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(assign_exprs.at(i)))) {
          } else if (!has_exist_in_array(column_ids, col_expr->get_column_id())) {
            //not found in column ids, ignore it
          } else if (OB_FAIL(updated_column_ids.push_back(col_expr->get_column_id()))) {
            LOG_WARN("fail to add updated column id", K(ret), K(col_expr->get_column_id()));
          }          
        }
      }
    }
  }
  
  return ret;
}

int ObTableDmlCgService::generate_upd_assign_infos(ObTableCtx &ctx,
                                                   ObIAllocator &allocator,
                                                   const ObIArray<ObRawExpr *> &assign_exprs,
                                                   ObTableUpdCtDef &udp_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t assign_cnt = assign_exprs.count();
  ColContentFixedArray &assign_infos = udp_ctdef.assign_columns_;

  if (OB_FAIL(assign_infos.init(assign_cnt))) {
    LOG_WARN("fail to init assign info array", K(ret), K(assign_cnt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < assign_cnt; ++i) {
    ColumnContent column_content;
    int64_t idx = 0;
    if (assign_exprs.at(i)->get_expr_type() == T_FUN_COLUMN_CONV) { // 兼容自增场景下的列转换表达式
      column_content.auto_filled_timestamp_ = false;
      column_content.is_nullable_ = false;
      column_content.is_predicate_column_ = false;
      column_content.is_implicit_ = false;
      if (OB_FAIL(ob_write_string(allocator,
                                  ctx.get_auto_inc_column_name(),
                                  column_content.column_name_))) {
        LOG_WARN("fail to copy column name", K(ret), K(ctx.get_auto_inc_column_name()));
      } else if (!has_exist_in_array(udp_ctdef.das_ctdef_.column_ids_, ctx.get_auto_inc_column_id(), &idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column not exists in old column", K(ret), K(ctx.get_auto_inc_column_name()));
      } else if (FALSE_IT(column_content.projector_index_ = static_cast<uint64_t>(idx))) {
        //do nothing
      } else if (OB_FAIL(assign_infos.push_back(column_content))) {
        LOG_WARN("fail to store colum content to assign infos", K(ret), K(column_content));
      }
    } else {
      ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr*>(assign_exprs.at(i));
      column_content.auto_filled_timestamp_ = col->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG);
      column_content.is_nullable_ = !col->get_result_type().is_not_null_for_write();
      column_content.is_predicate_column_ = false;
      column_content.is_implicit_ = false;
      if (OB_FAIL(ob_write_string(allocator,
                                  col->get_column_name(),
                                  column_content.column_name_))) {
        LOG_WARN("fail to copy column name", K(ret), K(col->get_column_name()));
      } else if (!has_exist_in_array(udp_ctdef.das_ctdef_.column_ids_, col->get_column_id(), &idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column not exists in old column", K(ret), KPC(col));
      } else if (FALSE_IT(column_content.projector_index_ = static_cast<uint64_t>(idx))) {
        //do nothing
      } else if (OB_FAIL(assign_infos.push_back(column_content))) {
        LOG_WARN("fail to store colum content to assign infos", K(ret), K(column_content));
      }
    }      
  }
  return ret;
}

int ObTableDmlCgService::generate_delete_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObTableDelCtDef &del_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;

  const ObIArray<ObRawExpr *> &exprs = ctx.get_all_exprs().get_expr_array();
  ObSEArray<ObRawExpr*, 64> tmp_exprs;
  if (OB_FAIL(replace_exprs_with_dependant(exprs, tmp_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(old_row.assign(tmp_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(generate_base_ctdef(ctx,
                                         del_ctdef,
                                         old_row,
                                         new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                            ctx.get_ref_table_id(),
                                            del_ctdef.das_ctdef_,
                                            old_row))) {
    LOG_WARN("fail to generate das delete ctdef", K(ret));
  } else if (OB_FAIL(generate_related_del_ctdef(ctx,
                                                allocator,
                                                old_row,
                                                del_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related del ctdef", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_del_ctdef(ObTableCtx &ctx,
                                                uint64_t index_tid,
                                                ObDASDelCtDef &das_del_ctdef,
                                                const ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_new_row;

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_del_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx,
                                         ctx.get_all_exprs().get_expr_array(),
                                         dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids,
                                        das_del_ctdef.column_ids_,
                                        old_row,
                                        empty_new_row,
                                        old_row,
                                        das_del_ctdef))) {
    LOG_WARN("fail to add old row projector", K(ret), K(old_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_related_del_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    const ObIArray<ObRawExpr*> &old_row,
                                                    DASDelCtDefArray &del_ctdefs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableID> &related_index_tids = ctx.get_related_index_ids();
  del_ctdefs.set_capacity(related_index_tids.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASDelCtDef> das_alloc(allocator);
    ObDASDelCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate delete related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                              related_index_tids.at(i),
                                              *related_das_ctdef,
                                              old_row))) {
      LOG_WARN("fail to generate das del ctdef", K(ret));
    } else if (OB_FAIL(del_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_replace_ctdef(ObTableCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObTableReplaceCtDef &replace_ctdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_insert_ctdef(ctx, allocator, replace_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_table_rowkey_info(ctx, replace_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate table rowkey info", K(ret), K(ctx));
  } else if (OB_FAIL(generate_delete_ctdef(ctx, allocator, replace_ctdef.del_ctdef_))) {
    LOG_WARN("fail to generate delete ctdef", K(ret), K(ctx));
  }

  return ret;
}

int ObTableDmlCgService::generate_table_rowkey_info(ObTableCtx &ctx,
                                                    ObTableInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObDASInsCtDef &das_ins_ctdef = ins_ctdef.das_ctdef_;
  ObSEArray<uint64_t, 8> rowkey_column_ids;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  ObSEArray<ObObjMeta, 8> rowkey_column_types;

  if (OB_FAIL(get_rowkey_exprs(ctx, rowkey_exprs))) {
    LOG_WARN("fail to get rowkey exprs", K(ret), K(ctx));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); ++i) {
    ObRawExpr *expr = rowkey_exprs.at(i);
    if (expr->get_expr_type() == T_FUN_COLUMN_CONV) { // 兼容自增场景下的列转换表达式
        if (OB_FAIL(rowkey_column_ids.push_back(ctx.get_auto_inc_column_id()))) {
          LOG_WARN("fail to push base column id", K(ret), K(ctx.get_auto_inc_column_id()));
        } else if (OB_FAIL(rowkey_column_types.push_back(expr->get_result_type()))) {
          LOG_WARN("fail to push column type", K(ret), KPC(expr));
        }
    } else {
      if (!expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr type is not column_ref expr", K(ret), KPC(expr));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
        if (OB_FAIL(rowkey_column_ids.push_back(col_expr->get_column_id()))) {
          LOG_WARN("fail to push base column id", K(ret), K(col_expr->get_column_id()));
        } else if (OB_FAIL(rowkey_column_types.push_back(col_expr->get_result_type()))) {
          LOG_WARN("fail to push column type", K(ret), KPC(col_expr));
        }
      }      
    }
  }

  if (FAILEDx(das_ins_ctdef.table_rowkey_cids_.init(rowkey_column_ids.count()))) {
    LOG_WARN("fail to init table rowkey column ids", K(ret), K(rowkey_column_ids.count()));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_cids_, rowkey_column_ids))) {
    LOG_WARN("fail to append table rowkey column id", K(ret), K(rowkey_column_ids));
  } else if (OB_FAIL(das_ins_ctdef.table_rowkey_types_.init(rowkey_column_types.count()))) {
    LOG_WARN("fail to init table_rowkey_types", K(ret), K(rowkey_column_types.count()));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_types_, rowkey_column_types))) {
    LOG_WARN("fail to append table rowkey column type", K(ret), K(rowkey_column_types));
  }

  return ret;
}

int ObTableDmlCgService::get_rowkey_exprs(ObTableCtx &ctx,
                                          ObIArray<ObRawExpr*> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else {
    const int rowkey_cnt = table_schema->get_rowkey_column_num();
    const ObIArray<ObRawExpr *> &all_exprs = ctx.is_for_insertup() ?
        ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; i++) {
      if (OB_FAIL(rowkey_exprs.push_back(all_exprs.at(i)))) {
        LOG_WARN("fail to push back rowkey expr", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_tsc_ctdef(ObTableCtx &ctx,
                                            ObDASScanCtDef &tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObIArray<ObRawExpr *> &all_exprs = ctx.is_for_insertup() ?
        ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();
  ObSEArray<ObRawExpr*, 16> access_exprs;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  tsc_ctdef.ref_table_id_ = ctx.get_index_table_id();
  const uint64_t tenant_id = MTL_ID();

  if (OB_FAIL(ctx.get_schema_guard().get_schema_version(TABLE_SCHEMA,
                                                        tenant_id,
                                                        tsc_ctdef.ref_table_id_,
                                                        tsc_ctdef.schema_version_))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id), K(tsc_ctdef.ref_table_id_));
  } else if (OB_FAIL(access_exprs.assign(all_exprs))) {
    LOG_WARN("fail to assign access exprs", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(access_exprs,
                                          tsc_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("fail to generate rt exprs ", K(ret));
  } else if (OB_FAIL(tsc_ctdef.access_column_ids_.init(access_exprs.count()))) {
    LOG_WARN("fail to init access_column_ids_ ", K(ret));
  } else {
    ARRAY_FOREACH(access_exprs, i) {
      if (access_exprs.at(i)->get_expr_type() == T_FUN_COLUMN_CONV) { // 兼容自增场景下的列转换表达式
        if (OB_FAIL(tsc_ctdef.access_column_ids_.push_back(ctx.get_auto_inc_column_id()))) {
          LOG_WARN("fail to add column id", K(ret), K(ctx.get_auto_inc_column_id()));
        }  
      } else {
        ObColumnRefRawExpr *ref_col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
        if (OB_ISNULL(ref_col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null column ref expr", K(ret), K(i));
        } else if (OB_FAIL(tsc_ctdef.access_column_ids_.push_back(ref_col_expr->get_column_id()))) {
          LOG_WARN("fail to add column id", K(ret), K(ref_col_expr->get_column_id()));
        }  
      }      
    }
  }
  if (OB_SUCC(ret)) {
    tsc_ctdef.table_param_.get_enable_lob_locator_v2() = (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
    if (OB_FAIL(tsc_ctdef.table_param_.convert(*table_schema, tsc_ctdef.access_column_ids_))) {
      LOG_WARN("fail to convert table param", K(ret));
    } else if (OB_FAIL(ObTableTscCgService::generate_das_result_output(tsc_ctdef,
                                                                       tsc_ctdef.access_column_ids_))) {
      LOG_WARN("generate das result output failed", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_single_constraint_info(ObTableCtx &ctx,
                                                         const ObTableSchema &index_schema,
                                                         const uint64_t table_id,
                                                         ObUniqueConstraintInfo &constraint_info)
{
  int ret = OB_SUCCESS;

  constraint_info.table_id_ = table_id;
  constraint_info.index_tid_ = index_schema.get_table_id();
  if (!index_schema.is_index_table()) {
    constraint_info.constraint_name_ = "PRIMARY";
  } else if (OB_FAIL(index_schema.get_index_name(constraint_info.constraint_name_))) {
    LOG_WARN("fail to get index name", K(ret));
  }

  if (OB_SUCC(ret)) {
    uint64_t rowkey_column_id = OB_INVALID_ID;
    const ObIArray<ObRawExpr *> &all_exprs = ctx.is_for_insertup() ?
        ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();
    ObIArray<ObColumnRefRawExpr*> &column_exprs = constraint_info.constraint_columns_;
    const ObRowkeyInfo &rowkey_info = index_schema.get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("fail to get rowkey column id", K(ret));
      } else {
        if (ctx.is_auto_inc()) { // 若为自增，则用ctx下挂的列引用表达式, 其他情况保留原有方案
          const ObIArray<ObColumnRefRawExpr *> &all_column_exprs = ctx.get_all_column_ref_exprs();
          for (int64_t j = 0; OB_SUCC(ret) && j < all_column_exprs.count(); j++) {
            ObColumnRefRawExpr *ref_expr = all_column_exprs.at(j);
            if (ref_expr->get_column_id() == rowkey_column_id
                && OB_FAIL(column_exprs.push_back(ref_expr))) {
              LOG_WARN("fail to push back column ref expr to constraint columns", K(ret), K(j));
            }
          } 
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < all_exprs.count(); j++) {
            if (all_exprs.at(j)->is_column_ref_expr()) {
              ObColumnRefRawExpr *ref_expr = static_cast<ObColumnRefRawExpr *>(all_exprs.at(j));
              if (ref_expr->get_column_id() == rowkey_column_id
                  && OB_FAIL(column_exprs.push_back(ref_expr))) {
                LOG_WARN("fail to push back column ref expr to constraint columns", K(ret), K(j));
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_constraint_infos(ObTableCtx &ctx,
                                                   ObIArray<ObUniqueConstraintInfo> &cst_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &schema_suard = ctx.get_schema_guard();
  const ObTableSchema *table_schema = ctx.get_table_schema();
  const uint64_t ref_table_id = ctx.get_ref_table_id();
  ObUniqueConstraintInfo constraint_info;
  ObSEArray<ObAuxTableMetaInfo, 16> index_infos;

  // 1. primary key
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(generate_single_constraint_info(ctx,
                                                     *table_schema,
                                                     ref_table_id,
                                                     constraint_info))) {
    LOG_WARN("fail to generate primary key constraint info", K(ret), K(ref_table_id));
  } else if (OB_FAIL(cst_infos.push_back(constraint_info))) {
    LOG_WARN("fail to push back constraint info", K(ret));
  }

  // 2. unique key
  if (FAILEDx(table_schema->get_simple_index_infos(index_infos))) {
    LOG_WARN("fail to get index infos", K(ret));
  } else {
    const ObTableSchema *index_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      constraint_info.reset();
      if (OB_FAIL(schema_suard.get_table_schema(ctx.get_session_info().get_effective_tenant_id(),
                                                index_infos.at(i).table_id_,
                                                index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema null", K(ret));
      } else if (!index_schema->is_final_invalid_index() && index_schema->is_unique_index()) {
        if (OB_FAIL(generate_single_constraint_info(ctx,
                                                    *index_schema,
                                                    ref_table_id,
                                                    constraint_info))) {
          LOG_WARN("fail to generate unique key constraint info", K(ret));
        } else if (OB_FAIL(cst_infos.push_back(constraint_info))) {
          LOG_WARN("fail to push back constraint info", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_constraint_ctdefs(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    sql::ObRowkeyCstCtdefArray &cst_ctdefs)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObRowkeyCstCtdef> cst_ctdef_allocator(allocator);
  ObSEArray<ObUniqueConstraintInfo, 2> cst_infos;
  ObRowkeyCstCtdef *rowkey_cst_ctdef = nullptr;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());

  if (OB_FAIL(generate_constraint_infos(ctx, cst_infos))) {
    LOG_WARN("fail to generate constraint infos", K(ret), K(ctx));
  } else if (OB_FAIL(cst_ctdefs.init(cst_infos.count()))) {
    LOG_WARN("fail to allocate conflict checker spec array", K(ret), K(cst_infos.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cst_infos.count(); i++) {
    const ObIArray<ObColumnRefRawExpr*> &cst_columns = cst_infos.at(i).constraint_columns_;
    if (OB_ISNULL(rowkey_cst_ctdef = cst_ctdef_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cst ctdef memory", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator,
                                       cst_infos.at(i).constraint_name_,
                                       rowkey_cst_ctdef->constraint_name_))) {
      LOG_WARN("fail to write string", K(ret), K(cst_infos.at(i).constraint_name_));
    } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.init(cst_columns.count()))) {
      LOG_WARN("fail to init rowkey", K(ret), K(cst_columns.count()));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < cst_columns.count(); ++j) {
        ObExpr *expr = nullptr;
        if (OB_FAIL(cg.generate_rt_expr(*cst_columns.at(j), expr))) {
          LOG_WARN("fail to generate rt expr", K(ret));
        } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.push_back(expr))) {
          LOG_WARN("fail to push back rt expr", K(ret));
        }
      }

      if (FAILEDx(cst_ctdefs.push_back(rowkey_cst_ctdef))) {
        LOG_WARN("fail to push back rowkey constraint ctdef", K(ret));
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_conflict_checker_ctdef(ObTableCtx &ctx,
                                                         ObIAllocator &allocator,
                                                         ObConflictCheckerCtdef &conflict_checker_ctdef)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr *> &exprs = ctx.is_for_insertup() ?
      ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();
  ObSEArray<ObRawExpr*, 16> table_column_exprs;
  ObSEArray<ObRawExpr*, 8> rowkey_exprs;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());

  if (OB_FAIL(get_rowkey_exprs(ctx, rowkey_exprs))) {
    LOG_WARN("fail to get table rowkey exprs", K(ret), K(ctx));
  } else if (OB_FAIL(generate_tsc_ctdef(ctx, conflict_checker_ctdef.das_scan_ctdef_))) {
    LOG_WARN("fail to generate das_scan_ctdef", K(ret));
  } else if (OB_FAIL(generate_constraint_ctdefs(ctx,
                                                allocator,
                                                conflict_checker_ctdef.cst_ctdefs_))) {
    LOG_WARN("fail to generate constraint infos", K(ret), K(ctx));
  } else if (OB_FAIL(cg.generate_rt_exprs(rowkey_exprs,
                                          conflict_checker_ctdef.data_table_rowkey_expr_))) {
    LOG_WARN("fail to generate data table rowkey expr", K(ret), K(rowkey_exprs));
  } else if (OB_FAIL(cg.generate_rt_exprs(exprs, conflict_checker_ctdef.table_column_exprs_))) {
    LOG_WARN("fail to generate table columns rt exprs ", K(ret));
  } else {
    conflict_checker_ctdef.rowkey_count_ = ctx.get_table_schema()->get_rowkey_column_num();
  }

  return ret;
}

int ObTableDmlCgService::generate_insert_up_ctdef(ObTableCtx &ctx,
                                                  ObIAllocator &allocator,
                                                  const ObTableCtx::ObAssignIds &assign_ids,
                                                  ObTableInsUpdCtDef &ins_up_ctdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_insert_ctdef(ctx, allocator, ins_up_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_table_rowkey_info(ctx, ins_up_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate table rowkey info", K(ret), K(ctx));
  } else if (OB_FAIL(generate_update_ctdef(ctx,
                                           allocator,
                                           assign_ids,
                                           ins_up_ctdef.upd_ctdef_))) {
    LOG_WARN("fail to generate delete ctdef", K(ret), K(ctx));
  }

  return ret;
}

int ObTableDmlCgService::generate_lock_ctdef(ObTableCtx &ctx,
                                             ObTableLockCtDef &lock_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  ObArray<ObRawExpr*> old_row;
  const ObIArray<ObRawExpr *> &exprs = ctx.get_all_exprs().get_expr_array();
  ObSEArray<ObRawExpr*, 64> tmp_exprs;

  if (OB_FAIL(replace_exprs_with_dependant(exprs, tmp_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(old_row.assign(tmp_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(old_row,
                                          lock_ctdef.old_row_))) {
    LOG_WARN("fail to generate lock rt exprs", K(ret), K(old_row));
  } else if (OB_FAIL(generate_das_lock_ctdef(ctx,
                                             ctx.get_ref_table_id(),
                                             lock_ctdef.das_ctdef_,
                                             old_row))) {
    LOG_WARN("fail to generate das lock ctdef", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_lock_ctdef(ObTableCtx &ctx,
                                                 uint64_t index_tid,
                                                 ObDASLockCtDef &das_lock_ctdef,
                                                 const ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_new_row;

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_lock_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx,
                                         ctx.get_all_exprs().get_expr_array(),
                                         dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids,
                                        das_lock_ctdef.column_ids_,
                                        old_row,
                                        empty_new_row,
                                        old_row,
                                        das_lock_ctdef))) {
    LOG_WARN("fail to add old row projector", K(ret), K(old_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_base_ctdef(ObTableCtx &ctx,
                                             ObTableDmlBaseCtDef &base_ctdef,
                                             ObIArray<ObRawExpr*> &old_row,
                                             ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObIArray<ObRawExpr *> &exprs = (ctx.is_for_update() || ctx.is_for_insertup()) ?
      ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();

  if (OB_FAIL(generate_column_ids(ctx, exprs, base_ctdef.column_ids_))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(old_row, base_ctdef.old_row_))) {
    LOG_WARN("fail to generate old row exprs", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(new_row, base_ctdef.new_row_))) {
    LOG_WARN("fail to generate new row exprs", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_ins_ctdef(ObTableCtx &ctx,
                                                uint64_t index_tid,
                                                ObDASInsCtDef &das_ins_ctdef,
                                                const ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_old_row;
  const ObIArray<ObRawExpr *> &exprs = ctx.is_for_insertup() ?
      ctx.get_old_row_exprs() : ctx.get_all_exprs().get_expr_array();

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_ins_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx,
                                         exprs,
                                         dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids, // new row and old row's columns id
                                        das_ins_ctdef.column_ids_, // schmea column ids for given index_tid
                                        empty_old_row,
                                        new_row,
                                        new_row,
                                        das_ins_ctdef))) {
    LOG_WARN("fail to add new row projector", K(ret), K(new_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_base_ctdef(uint64_t index_tid,
                                                 ObTableCtx &ctx,
                                                 ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  base_ctdef.table_id_ = ctx.get_ref_table_id();
  base_ctdef.index_tid_ = index_tid;
  base_ctdef.is_ignore_ = false; // insert ignore
  base_ctdef.is_batch_stmt_ = false;
  int64_t binlog_row_image = share::ObBinlogRowImage::FULL;
  ObSQLSessionInfo &session = ctx.get_session_info();

  if (OB_FAIL(generate_column_info(index_tid, ctx, base_ctdef))) {
    LOG_WARN("fail to generate column info", K(ret), K(index_tid), K(ctx));
  } else if (OB_FAIL(ctx.get_schema_guard().get_schema_version(TABLE_SCHEMA,
                                                               ctx.get_tenant_id(),
                                                               index_tid,
                                                               base_ctdef.schema_version_))) {
    LOG_WARN("fail to get table schema version", K(ret));
  } else if (OB_FAIL(convert_table_param(ctx, base_ctdef))) {
    LOG_WARN("fail to convert table dml param", K(ret));
  } else if (OB_FAIL(session.get_binlog_row_image(binlog_row_image))) {
    LOG_WARN("fail to get binlog row image", K(ret));
  } else {
    base_ctdef.tz_info_ = *session.get_tz_info_wrap().get_time_zone_info();
    base_ctdef.is_total_quantity_log_ = (share::ObBinlogRowImage::FULL == binlog_row_image);
    base_ctdef.encrypt_meta_.reset();
  }

  return ret;
}

// add column_ids, column_types, column_accuracys, rowkey_cnt, spk_cnt to ObDASDMLBaseCtDef
// according to table schema order
int ObTableDmlCgService::generate_column_info(ObTableID index_tid,
                                              ObTableCtx &ctx,
                                              ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  base_ctdef.column_ids_.reset();
  base_ctdef.column_types_.reset();
  const ObTableSchema *index_schema = nullptr;

  if (OB_FAIL(ctx.get_schema_guard().get_table_schema(ctx.get_tenant_id(),
                                                      index_tid,
                                                      index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_tid));
  } else {
    int64_t column_count = index_schema->get_column_count();
    base_ctdef.column_ids_.set_capacity(column_count);
    base_ctdef.column_types_.set_capacity(column_count);
    base_ctdef.column_accuracys_.set_capacity(column_count);
    base_ctdef.rowkey_cnt_ = index_schema->get_rowkey_info().get_size();
    base_ctdef.spk_cnt_ = index_schema->get_shadow_rowkey_info().get_size();

    // add rowkey column infos
    const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(i);
      const ObColumnSchemaV2 *column = index_schema->get_column_schema(rowkey_column->column_id_);
      ObObjMeta column_type;
      column_type = column->get_meta_type();
      column_type.set_scale(column->get_accuracy().get_scale());
      if (is_lob_storage(column_type.get_type())) {
        if (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
          column_type.set_has_lob_header();
        }
      }
      if (OB_FAIL(base_ctdef.column_ids_.push_back(column->get_column_id()))) {
        LOG_WARN("fail to add column id", K(ret));
      } else if (OB_FAIL(base_ctdef.column_types_.push_back(column_type))) {
        LOG_WARN("fail to add column type", K(ret));
      } else if (OB_FAIL(base_ctdef.column_accuracys_.push_back(column->get_accuracy()))) {
        LOG_WARN("fail to add column accuracys", K(ret));
      }
    }

    // add normal column infos if need
    if (OB_SUCC(ret)) {
      ObTableSchema::const_column_iterator iter = index_schema->column_begin();
      for (; OB_SUCC(ret) && iter != index_schema->column_end(); ++iter) {
        const ObColumnSchemaV2 *column = *iter;
        if (column->is_rowkey_column() || column->is_virtual_generated_column()) {
          // do nothing
        } else {
          ObObjMeta column_type;
          column_type = column->get_meta_type();
          column_type.set_scale(column->get_accuracy().get_scale());
          if (is_lob_storage(column_type.get_type())) {
            if (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
              column_type.set_has_lob_header();
            }
          }
          if (OB_FAIL(base_ctdef.column_ids_.push_back(column->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (OB_FAIL(base_ctdef.column_types_.push_back(column_type))) {
            LOG_WARN("fail to add column type", K(ret));
          } else if (OB_FAIL(base_ctdef.column_accuracys_.push_back(column->get_accuracy()))) {
            LOG_WARN("fail to add column accuracys", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::convert_table_param(ObTableCtx &ctx,
                                             ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  const ObTableSchema *table_schema = NULL;
  uint64_t tenant_id = ctx.get_tenant_id();

  if (OB_FAIL(ctx.get_schema_guard().get_table_schema(tenant_id,
                                                      base_ctdef.index_tid_,
                                                      table_schema))) {
    LOG_WARN("fail to get schema", K(ret), K(base_ctdef));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(ctx.get_schema_guard().get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(base_ctdef.table_param_.convert(table_schema,
                                                     schema_version,
                                                     base_ctdef.column_ids_))) {
    LOG_WARN("fail to convert table param", K(ret), K(base_ctdef));
  }

  return ret;
}

int ObTableDmlCgService::generate_column_ids(ObTableCtx &ctx,
                                             const ObIArray<ObRawExpr*> &exprs,
                                             ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();

  if (!exprs.empty()) {
    if (OB_FAIL(column_ids.reserve(exprs.count()))) {
      LOG_WARN("fail to reserve column ids capacity", K(ret), K(exprs.count()));
    } else {
      ARRAY_FOREACH(exprs, i) {
        if (exprs.at(i)->get_expr_type() == T_FUN_COLUMN_CONV && (ctx.get_opertion_type() != ObTableOperationType::DEL)    // 特判下，若为delete/update/get/scan操作
                                                              && (ctx.get_opertion_type() != ObTableOperationType::UPDATE) // 则走原有的列引用表达式
                                                              && (ctx.get_opertion_type() != ObTableOperationType::GET)
                                                              && (ctx.get_opertion_type() != ObTableOperationType::SCAN)) {
          if (OB_FAIL(column_ids.push_back(ctx.get_auto_inc_column_id()))) {
            LOG_WARN("fail to push back column id", K(ret), K(ctx.get_auto_inc_column_id()));
          }
        } else {
          ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(exprs.at(i));
          if (OB_FAIL(column_ids.push_back(col_expr->get_column_id()))) {
            LOG_WARN("fail to push back column id", K(ret), K(col_expr->get_column_id()));
          }  
        }
      }
    }
  }

  return ret;
}

// 构造 das_ctdef 中的 old_row_projector 和 new_row_projector，
// 其中存储 storage column 对应表达式在 full row exprs 数组中下标
int ObTableDmlCgService::generate_projector(const ObIArray<uint64_t> &dml_column_ids,
                                            const ObIArray<uint64_t> &storage_column_ids,
                                            const ObIArray<ObRawExpr*> &old_row,
                                            const ObIArray<ObRawExpr*> &new_row,
                                            const ObIArray<ObRawExpr*> &full_row,
                                            ObDASDMLBaseCtDef &das_ctdef)
{
  int ret = OB_SUCCESS;
  IntFixedArray &old_row_projector = das_ctdef.old_row_projector_;
  IntFixedArray &new_row_projector = das_ctdef.new_row_projector_;

  // generate old row projector
  // 查找old row expr 在full row expr中的位置（投影）
  if (!old_row.empty()) {
    if (OB_FAIL(old_row_projector.prepare_allocate(storage_column_ids.count()))) {
      LOG_WARN("fail to init row projector array", K(ret), K(storage_column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      old_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = old_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(old_row.at(column_idx)));
        } else {
          old_row_projector.at(i) = projector_idx;
        }
      }
    }
  }

  // generate new row projector
  // 查找new row expr 在full row expr中的位置（投影）
  if (!new_row.empty() && OB_SUCC(ret)) {
    if (OB_FAIL(new_row_projector.prepare_allocate(storage_column_ids.count()))) {
      LOG_WARN("fail to init row projector array", K(ret), K(storage_column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      // 如果 projector[i] = j, 表达式按照 schema 顺序，第 i 个 column 在 full_row 中的下标为 j，如果在 new_row 中不存在，那么 j == -1
      new_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = new_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(new_row.at(column_idx)));
        } else {
          new_row_projector.at(i) = projector_idx; // projector_idx 为 column storage_column_ids[i] 在 full row expr 中的 index
        }
      }
    }
  }
  return ret;
}


int ObTableDmlCgService::generate_related_ins_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    const ObIArray<ObRawExpr*> &new_row,
                                                    DASInsCtDefArray &ins_ctdefs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableID> &related_index_tids = ctx.get_related_index_ids();
  ins_ctdefs.set_capacity(related_index_tids.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASInsCtDef> das_alloc(allocator);
    ObDASInsCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate insert related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                              related_index_tids.at(i),
                                              *related_das_ctdef,
                                              new_row))) {
      LOG_WARN("fail to generate das ins ctdef", K(ret));
    } else if (OB_FAIL(ins_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_related_upd_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    const ObIArray<ObRawExpr *> &assign_exprs,
                                                    const ObIArray<ObRawExpr*> &old_row,
                                                    const ObIArray<ObRawExpr*> &new_row,
                                                    const ObIArray<ObRawExpr*> &full_row,
                                                    DASUpdCtDefArray &upd_ctdefs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableID> &related_index_tids = ctx.get_related_index_ids();
  upd_ctdefs.set_capacity(related_index_tids.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASUpdCtDef> das_alloc(allocator);
    ObDASUpdCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate update related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_upd_ctdef(ctx,
                                              related_index_tids.at(i),
                                              assign_exprs,
                                              *related_das_ctdef,
                                              old_row,
                                              new_row,
                                              full_row))) {
      LOG_WARN("fail to generate das update ctdef", K(ret));
    } else if (related_das_ctdef->updated_column_ids_.empty()) {
      // ignore invalid update ctdef
    } else if (OB_FAIL(upd_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableTscCgService::generate_rt_exprs(const ObTableCtx &ctx,
                                           ObIAllocator &allocator,
                                           const ObIArray<ObRawExpr *> &src,
                                           ObIArray<ObExpr *> &dst)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = const_cast<ObSQLSessionInfo*>(&ctx.get_session_info());
  ObSchemaGetterGuard *schema_guard = const_cast<ObSchemaGetterGuard*>(&ctx.get_schema_guard());
  ObStaticEngineExprCG expr_cg(allocator,
                               session_info,
                               schema_guard,
                               0,
                               0,
                               ctx.get_cur_cluster_version());
  if (!src.empty()) {
    if (OB_FAIL(dst.reserve(src.count()))) {
      LOG_WARN("fail to init fixed array", K(ret), K(src.count()));
    } else {
      ObArray<ObRawExpr*> exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); i++) {
        ObExpr *e = nullptr;
        if (OB_FAIL(ObStaticEngineExprCG::generate_rt_expr(*src.at(i), exprs, e))) {
          LOG_WARN("fail to generate rt expr", K(ret));
        } else if (OB_ISNULL(e)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (OB_FAIL(dst.push_back(e))) {
          LOG_WARN("fail to push back rt expr", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

// 访问虚拟生成列转换为访问其依赖的列
int ObTableTscCgService::replace_gen_col_exprs(const ObTableCtx &ctx,
                                               ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  if (!ctx.get_table_schema()->has_generated_column()) {
    // do nothing
  } else {
    ObArray<ObRawExpr*> res_access_expr;
    const ObIArray<ObTableCtx::ObGenDenpendantsPair> &pairs = ctx.get_gen_dependants_pairs();
    ObColumnRefRawExpr *ref_expr = nullptr;
    const int64_t N = access_exprs.count();

    for (int64_t i = 0; i < N && OB_SUCC(ret); i++) {
      ObRawExpr *expr = access_exprs.at(i);
      if (!expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(*expr));
      } else if (FALSE_IT(ref_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (!ref_expr->is_virtual_generated_column()) {
        if (OB_FAIL(res_access_expr.push_back(expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        }
      } else {
        for (int j = 0; j < pairs.count() && OB_SUCC(ret); j++) {
          if (ref_expr == pairs.at(j).first) {
            const ObIArray<ObRawExpr*> &ref_exprs = pairs.at(j).second;
            if (OB_FAIL(append_array_no_dup(res_access_expr, ref_exprs))) {
              LOG_WARN("fail to append array no dup", K(ret), K(res_access_expr), K(ref_exprs));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      access_exprs.reset();
      if (OB_FAIL(access_exprs.assign(res_access_expr))) {
        LOG_WARN("fail to assign access expr", K(ret));
      }
    }
  }

  return ret;
}

// 非索引扫描: access exprs = select exprs
// 索引表: access exprs = [index column exprs][rowkey expr]
// 索引回表: access expr = [rowkey expr][select without rowkey exprs]
int ObTableTscCgService::generate_access_ctdef(const ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObDASScanCtDef &das_tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> access_exprs;
  const ObIArray<oceanbase::sql::ObRawExpr *> &select_exprs = ctx.get_select_exprs();
  const ObIArray<oceanbase::sql::ObRawExpr *> &rowkey_exprs = ctx.get_rowkey_exprs();
  const ObIArray<oceanbase::sql::ObRawExpr *> &index_exprs = ctx.get_index_exprs();
  const bool is_index_table = (ctx.is_index_scan() && das_tsc_ctdef.ref_table_id_ == ctx.get_index_table_id());

  if (!ctx.is_index_scan() && OB_FAIL(access_exprs.assign(select_exprs))) { // 非索引扫描
    LOG_WARN("fail to assign access exprs", K(ret));
  } else if (is_index_table) { // 索引表
    if (OB_FAIL(access_exprs.assign(index_exprs))) {
      LOG_WARN("fail to assign access exprs", K(ret), K(ctx.get_index_table_id()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); i++) {
        if (is_in_array(index_exprs, rowkey_exprs.at(i))) {
          // 在index_exprs中的index expr不需要再次添加
        } else if (OB_FAIL(access_exprs.push_back(rowkey_exprs.at(i)))) {
          LOG_WARN("fail to push back rowkey expr", K(ret), K(i));
        }
      }
    }
  } else if (ctx.is_index_scan() && das_tsc_ctdef.ref_table_id_ == ctx.get_ref_table_id()) { // 索引回表
    if (OB_FAIL(access_exprs.assign(rowkey_exprs))) {
      LOG_WARN("fail to assign access exprs", K(ret), K(ctx.get_ref_table_id()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
        if (is_in_array(rowkey_exprs, select_exprs.at(i))) {
          // 已经在rowkey中，不需要再次添加
        } else if (OB_FAIL(access_exprs.push_back(select_exprs.at(i)))) {
          LOG_WARN("fail to push back select expr", K(ret), K(i));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!is_index_table && OB_FAIL(replace_gen_col_exprs(ctx, access_exprs))) {
      LOG_WARN("fail to replace generate exprs", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(ctx, allocator, access_exprs, das_tsc_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("fail to generate access rt exprs", K(ret));
    } else if (OB_FAIL(das_tsc_ctdef.access_column_ids_.init(access_exprs.count()))) {
      LOG_WARN("fail to init access column ids", K(ret), K(access_exprs.count()));
    } else {
      ObColumnRefRawExpr *col_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); i++) {
        if (!access_exprs.at(i)->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected expr type", K(ret), K(access_exprs.at(i)->get_expr_type()));
        } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i)))) {
        } else if (OB_FAIL(das_tsc_ctdef.access_column_ids_.push_back(col_expr->get_column_id()))) {
          LOG_WARN("fail to push back column id", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableTscCgService::generate_das_result_output(ObDASScanCtDef &das_tsc_ctdef,
                                                    const ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  ExprFixedArray &access_exprs = das_tsc_ctdef.pd_expr_spec_.access_exprs_;
  const ObIArray<uint64_t> &access_cids = das_tsc_ctdef.access_column_ids_;
  int64_t access_column_cnt = access_cids.count();
  int64_t access_expr_cnt = access_exprs.count();
  if (OB_UNLIKELY(access_column_cnt != access_expr_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt));
  } else if (OB_FAIL(das_tsc_ctdef.result_output_.init(output_cids.count() + 1))) {
    LOG_WARN("fail to init result output", K(ret), K(output_cids.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < output_cids.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < access_column_cnt; j++) {
      if (output_cids.at(i) == access_cids.at(j)) {
        if (OB_FAIL(das_tsc_ctdef.result_output_.push_back(access_exprs.at(j)))) {
          LOG_WARN("fail to push result output expr", K(ret), K(i), K(j));
        }
      }
    }
  }

  return ret;
}

// tsc_out_cols
// 主表/索引回表/索引扫描不需要回表: select column ids
// 索引表: rowkey column ids
int ObTableTscCgService::generate_table_param(const ObTableCtx &ctx,
                                              ObDASScanCtDef &das_tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tsc_out_cols;
  const ObTableSchema *table_schema = nullptr;
  const ObIArray<ObRawExpr *> &select_exprs = ctx.get_select_exprs();

  if (!ctx.is_index_scan() // 非索引扫描
      || (ctx.is_index_scan() && das_tsc_ctdef.ref_table_id_ == ctx.get_ref_table_id()) // 索引扫描回表
      || (ctx.is_index_scan() && !ctx.is_index_back())) { //索引扫描不需要回表
    ObColumnRefRawExpr *col_expr = NULL;
    if (ctx.is_index_scan() && !ctx.is_index_back()) {
      table_schema = ctx.get_index_schema();
    } else {
      table_schema = ctx.get_table_schema();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
      ObRawExpr *expr = select_exprs.at(i);
      if (!expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(ret), K(*expr));
      } else if (FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (!col_expr->is_virtual_generated_column()) {
        if (OB_FAIL(tsc_out_cols.push_back(col_expr->get_column_id()))) {
          LOG_WARN("fail to push back column id", K(ret));
        }
      } else {
        // output虚拟生成列转换为ouput其依赖的列
        const ObIArray<ObTableCtx::ObGenDenpendantsPair> &pairs = ctx.get_gen_dependants_pairs();
        for (int64_t j = 0; j < pairs.count() && OB_SUCC(ret); j++) {
          if (col_expr == pairs.at(j).first) {
            const ObIArray<ObRawExpr*> &dependant_exprs = pairs.at(j).second;
            for (int64_t k = 0; k < dependant_exprs.count() && OB_SUCC(ret); k++) {
              ObColumnRefRawExpr *dep_col_expr = static_cast<ObColumnRefRawExpr*>(dependant_exprs.at(k));
              if (OB_FAIL(add_var_to_array_no_dup(tsc_out_cols, dep_col_expr->get_column_id()))) {
                LOG_WARN("fail to add column id", K(ret), K(tsc_out_cols), K(*dep_col_expr));
              }
            }
          }
        }
      }
    }
  } else if (ctx.is_index_scan() && das_tsc_ctdef.ref_table_id_ == ctx.get_index_table_id()) { // 索引表
    table_schema = ctx.get_index_schema();
    if (OB_FAIL(ctx.get_table_schema()->get_rowkey_column_ids(tsc_out_cols))) {
      LOG_WARN("fail to get rowkey column ids", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(das_tsc_ctdef.table_param_.get_enable_lob_locator_v2()
                          = (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0))) {
  } else if (OB_FAIL(das_tsc_ctdef.table_param_.convert(*table_schema,
                                                        das_tsc_ctdef.access_column_ids_,
                                                        &tsc_out_cols))) {
    LOG_WARN("fail to convert schema", K(ret), K(*table_schema));
  } else if (OB_FAIL(generate_das_result_output(das_tsc_ctdef, tsc_out_cols))) {
    LOG_WARN("fail to generate das result outpur", K(ret), K(tsc_out_cols));
  }

  return ret;
}

int ObTableTscCgService::generate_das_tsc_ctdef(const ObTableCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObDASScanCtDef &das_tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard &schema_guard = (const_cast<ObTableCtx&>(ctx)).get_schema_guard();
  das_tsc_ctdef.is_get_ = ctx.is_get();
  das_tsc_ctdef.schema_version_ = ctx.is_index_scan() ? ctx.get_index_schema()->get_schema_version() :
                                                  ctx.get_table_schema()->get_schema_version();

  if (OB_FAIL(generate_access_ctdef(ctx, allocator, das_tsc_ctdef))) { // init access_column_ids_,pd_expr_spec_.access_exprs_
    LOG_WARN("fail to generate asccess ctdef", K(ret));
  } else if (OB_FAIL(generate_table_param(ctx, das_tsc_ctdef))) { // init table_param_, result_output_
    LOG_WARN("fail to generate table param", K(ret));
  }

  return ret;
}

int ObTableTscCgService::generate_output_exprs(const ObTableCtx &ctx,
                                               ObIArray<ObExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObIArray<ObRawExpr *> &select_exprs = ctx.get_select_exprs();
  const int64_t N = select_exprs.count();
  for (int64_t i = 0; i < N && OB_SUCC(ret); i++) {
    ObExpr *rt_expr = nullptr;
    ObRawExpr *expr = select_exprs.at(i);
    ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(expr);
    if (col_ref_expr->is_virtual_generated_column()) {
      expr = col_ref_expr->get_dependant_expr();
    }
    if (OB_FAIL(cg.generate_rt_expr(*expr, rt_expr))) {
      LOG_WARN("fail to generate rt expr", K(ret));
    } else if (OB_FAIL(output_exprs.push_back(rt_expr))) {
      LOG_WARN("fail to push back rt expr", K(ret), K(output_exprs));
    }
  }

  return ret;
}

int ObTableTscCgService::generate_tsc_ctdef(const ObTableCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObTableApiScanCtDef &tsc_ctdef)
{
  int ret = OB_SUCCESS;
  // init scan_ctdef_.ref_table_id_
  tsc_ctdef.scan_ctdef_.ref_table_id_ = ctx.get_index_table_id();
  if (OB_FAIL(tsc_ctdef.output_exprs_.init(ctx.get_select_exprs().count()))) {
    LOG_WARN("fail to init output expr", K(ret));
  } else if (OB_FAIL(generate_output_exprs(ctx, tsc_ctdef.output_exprs_))) {
    LOG_WARN("fail to generate output exprs", K(ret));
  } else if (OB_FAIL(generate_das_tsc_ctdef(ctx, allocator, tsc_ctdef.scan_ctdef_))) { // init scan_ctdef_
    LOG_WARN("fail to generate das scan ctdef", K(ret));
  } else if (ctx.is_index_back()) {
    // init lookup_ctdef_,lookup_loc_meta_
    void *lookup_buf = allocator.alloc(sizeof(ObDASScanCtDef));
    void *loc_meta_buf = allocator.alloc(sizeof(ObDASTableLocMeta));
    if (OB_ISNULL(lookup_buf) || OB_ISNULL(loc_meta_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate lookup ctdef buffer", K(ret), KP(lookup_buf), KP(loc_meta_buf));
    } else {
      tsc_ctdef.lookup_ctdef_ = new(lookup_buf) ObDASScanCtDef(allocator);
      tsc_ctdef.lookup_ctdef_->ref_table_id_ = ctx.get_ref_table_id();
      tsc_ctdef.lookup_loc_meta_ = new(loc_meta_buf) ObDASTableLocMeta(allocator);
      if (OB_FAIL(generate_das_tsc_ctdef(ctx, allocator, *tsc_ctdef.lookup_ctdef_))) {
        LOG_WARN("fail to generate das lookup scan ctdef", K(ret));
      } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(ctx,
                                                                      *tsc_ctdef.lookup_loc_meta_,
                                                                      true /* is_lookup */))) {
        LOG_WARN("fail to generate table loc meta", K(ret));
      }
    }
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiInsertSpec &spec)
{
  return ObTableDmlCgService::generate_insert_ctdef(ctx, alloc, spec.get_ctdef());
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiUpdateSpec &spec)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableDmlCgService::generate_update_ctdef(ctx,
                                                         alloc,
                                                         ctx.get_assign_ids(),
                                                         spec.get_ctdef()))) {
    LOG_WARN("fail to generate update ctdef", K(ret));
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiDelSpec &spec)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableDmlCgService::generate_delete_ctdef(ctx, alloc, spec.get_ctdef()))) {
    LOG_WARN("fail to generate delete ctdef", K(ret));
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiReplaceSpec &spec)
{
  int ret = OB_SUCCESS;
  ObTableReplaceCtDef &ctdef = spec.get_ctdef();

  if (OB_FAIL(ObTableDmlCgService::generate_replace_ctdef(ctx, alloc, ctdef))) {
    LOG_WARN("fail to generate replace ctdef", K(ret));
  } else if (ObTableDmlCgService::generate_conflict_checker_ctdef(ctx, alloc, spec.get_conflict_checker_ctdef())) {
    LOG_WARN("fail to generate conflict checker ctdef", K(ret));
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiInsertUpSpec &spec)
{
  int ret = OB_SUCCESS;
  ObTableInsUpdCtDef &ctdef = spec.get_ctdef();

  if (OB_FAIL(ObTableDmlCgService::generate_insert_up_ctdef(ctx,
                                                            alloc,
                                                            ctx.get_assign_ids(),
                                                            ctdef))) {
    LOG_WARN("fail to generate insert up ctdef", K(ret));
  } else {
    const ObIArray<ObRawExpr *> &exprs = ctx.get_old_row_exprs();
    ObStaticEngineCG cg(ctx.get_cur_cluster_version());
    if (ObTableDmlCgService::generate_conflict_checker_ctdef(ctx,
                                                             alloc,
                                                             spec.get_conflict_checker_ctdef())) {
      LOG_WARN("fail to generate conflict checker ctdef", K(ret));
    } else if (OB_FAIL(cg.generate_rt_exprs(exprs,
                                            spec.get_all_saved_exprs()))) {
      LOG_WARN("fail to generate rt exprs ", K(ret));
    }
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(common::ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiLockSpec &spec)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTableDmlCgService::generate_lock_ctdef(ctx, spec.get_ctdef()))) {
    LOG_WARN("fail to generate lock ctdef", K(ret));
  }

  return ret;
}

int ObTableExprCgService::generate_autoinc_nextval_expr(ObTableCtx &ctx,
                                                        ObRawExpr *&expr,
                                                        const ObColumnSchemaV2 &col_schema)
{
  int ret = OB_SUCCESS;

  ObSysFunRawExpr *func_expr = NULL;
  if (OB_FAIL(ctx.get_expr_factory().create_raw_expr(T_FUN_SYS_AUTOINC_NEXTVAL, func_expr))) {
    LOG_WARN("fail to create nextval expr", K(ret));
  } else {
    func_expr->set_func_name(ObString::make_string(N_AUTOINC_NEXTVAL));
    if (OB_NOT_NULL(expr) && OB_FAIL(func_expr->add_param_expr(expr))) { // 将最底部的列转换表达式挂到自增列表达式之下
      LOG_WARN("fail to add collumn conv expr to function param", K(ret));
    } else if (OB_FAIL(func_expr->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to extract info", K(ret));
    } else if (OB_FAIL(ObAutoincNextvalExtra::init_autoinc_nextval_extra(&ctx.get_allocator(),
                                                                         reinterpret_cast<ObRawExpr *&>(func_expr),
                                                                         ctx.get_table_id(),
                                                                         col_schema.get_column_id(),
                                                                         ctx.get_table_name(),
                                                                         col_schema.get_column_name()))) {
      LOG_WARN("fail to init autoinc_nextval_extra", K(ret), K(ctx.get_table_name()), K(col_schema));
    } else {
      expr = func_expr;
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
