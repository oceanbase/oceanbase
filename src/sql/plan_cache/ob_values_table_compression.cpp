/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_PC
#include "sql/plan_cache/ob_values_table_compression.h"
#include "sql/plan_cache/ob_plan_cache_struct.h"
#include "sql/parser/ob_fast_parser.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/engine/expr/ob_expr_version.h"

using namespace oceanbase::common;

namespace oceanbase
{

namespace sql
{
// based in ob_parser.h
const char *ObValuesTableCompression::lower_[ObParser::S_MAX] = {
  "", "", "", "", "", "", "", "", "", "", /* 0 ~9 */
  "", "", "", "update", "", "", "", "", "", "", /* 10 ~19 */
  "", "", "", "", "", "", "", "", "", "", /* 20 ~29 */
  "", "", "", "select", "insert", "delete", "values", "table", "into" /* 30 ~38 */
};

const char *ObValuesTableCompression::upper_[ObParser::S_MAX] = {
  "", "", "", "", "", "", "", "", "", "", /* 0 ~9 */
  "", "", "", "UPDATE", "", "", "", "", "", "", /* 10 ~19 */
  "", "", "", "", "", "", "", "", "", "", /* 20 ~29 */
  "", "", "", "SELECT", "INSERT", "DELETE", "VALUES", "TABLE", "INTO" /* 30 ~38 */
};

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

void ObValuesTableCompression::match_one_state(const char *&p,
                                               const char *p_end,
                                               const ObParser::State next_state,
                                               ObParser::State &state)
{
  int compare_len = strlen(lower_[state]);
  if (p_end - p < compare_len) {
    state = ObParser::S_INVALID;
  } else if (0 == strncasecmp(p, lower_[state], compare_len)) {
    p += compare_len;
    state = next_state;
  } else {
    state = ObParser::S_INVALID;
  }
}

bool ObValuesTableCompression::is_support_compress_values_table(const ObString &stmt)
{
  ObParser::State state = ObParser::S_START;
  ObParser::State save_state = state;
  const char *p = stmt.ptr();
  const char *p_start = p;
  const char *p_end = p + stmt.length();
  bool is_dml_stmt = false;
  bool has_error = false;
  while (p < p_end && !has_error && !is_dml_stmt) {
    switch (state) {
      case ObParser::S_START: {
        if (ISSPACE(*p)) {
          p++;
        } else {
          if (!ObParser::is_comment(p, p_end, save_state, state, ObParser::S_INVALID) &&
              state != ObParser::S_INVALID) {
            if (ObParser::S_START == state) {
              save_state = state;
              if (*p == lower_[ObParser::S_SELECT][0] || *p == upper_[ObParser::S_SELECT][0]) {
                state = ObParser::S_SELECT;
              } else if (*p == lower_[ObParser::S_INSERT][0] || *p == upper_[ObParser::S_INSERT][0]) {
                state = ObParser::S_INSERT;
              } else if (*p == lower_[ObParser::S_UPDATE][0] || *p == upper_[ObParser::S_UPDATE][0]) {
                state = ObParser::S_UPDATE;
              } else if (*p == lower_[ObParser::S_DELETE][0] || *p == upper_[ObParser::S_DELETE][0]) {
                state = ObParser::S_DELETE;
              } else if (*p == lower_[ObParser::S_VALUES][0] || *p == upper_[ObParser::S_VALUES][0]) {
                state = ObParser::S_VALUES;
              } else {
                state = ObParser::S_INVALID;
              }
            }
          }
        }
      } break;
      case ObParser::S_SELECT:
      case ObParser::S_DELETE:
      case ObParser::S_UPDATE:
      case ObParser::S_VALUES: {
        match_one_state(p, p_end, ObParser::S_NORMAL, state);
      } break;
      case ObParser::S_INSERT: {
        match_one_state(p, p_end, ObParser::S_INTO, state);
      } break;
      case ObParser::S_INTO: {
        if (ISSPACE(*p)) {
          p++;
        } else {
          if (!ObParser::is_comment(p, p_end, save_state, state, ObParser::S_INVALID) &&
              state != ObParser::S_INVALID) {
            match_one_state(p, p_end, ObParser::S_NORMAL, state);
          }
        }
      } break;
      case ObParser::S_NORMAL: {
        is_dml_stmt = true;
        has_error = false;
      } break;
      case ObParser::S_COMMENT: {
        if (*p == '\n') {
          // end of '--' comments
          state = save_state;
        }
        p++;
      } break;
      case ObParser::S_C_COMMENT: {
        if (*p == '*') {
          if ((p + 1 < p_end) && '/' == *(p + 1)) {
            // end of '/**/' comments
            state = save_state;
            p++;
          }
        }
        p++;
      } break;
      case ObParser::S_INVALID:
      default: {
        is_dml_stmt = false;
        has_error = true;
      } break;
    }
  }
  return is_dml_stmt && !has_error;
}

int ObValuesTableCompression::add_raw_array_params(ObIAllocator &allocator,
                                                   ObPlanCacheCtx &pc_ctx,
                                                   const ObFastParserResult &fp_result,
                                                   const int64_t begin_param,
                                                   const int64_t row_count,
                                                   const int64_t param_count)
{
  int ret = OB_SUCCESS;
  if (begin_param + row_count * param_count > fp_result.raw_params_.count() ||
      row_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected raw_params", K(ret), K(begin_param), K(row_count), K(param_count),
             K(fp_result.raw_params_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
      void *buf = nullptr;
      ObArrayPCParam *params_array = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObArrayPCParam)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObArrayPCParam)));
      } else {
        params_array = new(buf) ObArrayPCParam(allocator);
        params_array->set_capacity(row_count);
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < row_count; ++j) {
        int64_t param_idx = begin_param + j * param_count + i;
        if (OB_FAIL(params_array->push_back(fp_result.raw_params_.at(param_idx)))) {
          LOG_WARN("fail to push back", K(ret), K(i), K(j));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pc_ctx.fp_result_.array_params_.push_back(params_array))) {
        LOG_WARN("fail to push params array", K(ret));
      }
    }
  }
  return ret;
}

int ObValuesTableCompression::rebuild_new_raw_sql(ObPlanCacheCtx &pc_ctx,
                                                  const ObIArray<ObPCParam*> &raw_params,
                                                  const int64_t begin_idx,
                                                  const int64_t param_cnt,
                                                  const int64_t delta_length,
                                                  const ObString &no_param_sql,
                                                  ObString &new_raw_sql,
                                                  int64_t &no_param_sql_pos,
                                                  int64_t &new_raw_pos)
{
  int ret = OB_SUCCESS;
  char *buff = new_raw_sql.ptr();
  int64_t buff_len = pc_ctx.raw_sql_.length();
  int64_t len = 0;
  if (OB_ISNULL(buff)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buff is null", K(ret), KP(buff));
  } else if (begin_idx < 0 || raw_params.count() < begin_idx + param_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is wrong", K(ret), K(begin_idx), K(param_cnt));
  } else {
    for (int64_t i = begin_idx; OB_SUCC(ret) && i < begin_idx + param_cnt; i++) {
      const ObPCParam *pc_param = raw_params.at(i);
      if (OB_ISNULL(pc_param) || OB_ISNULL(pc_param->node_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL ptr", K(ret), KP(pc_param));
      } else {
        int64_t param_pos = pc_param->node_->pos_ - delta_length; // get pos is in new no param sql
        int64_t param_len = pc_param->node_->text_len_;
        len = param_pos - no_param_sql_pos;
        if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(new_raw_pos + len + param_len > buff_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected params", K(ret), K(len), K(param_len), K(new_raw_pos));
        } else {
          if (len > 0) {
            //copy text
            MEMCPY(buff + new_raw_pos, no_param_sql.ptr() + no_param_sql_pos, len);
            new_raw_pos += len;
            no_param_sql_pos += len;
          }
          if (param_pos == no_param_sql_pos) {
            //copy raw param
            MEMCPY(buff + new_raw_pos, pc_param->node_->raw_text_, param_len);
            new_raw_pos += param_len;
            no_param_sql_pos += 1;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      new_raw_sql.assign_ptr(buff, new_raw_pos);
    }
  }
  return ret;
}

int ObValuesTableCompression::try_batch_exec_params(ObIAllocator &allocator,
                                                    ObPlanCacheCtx &pc_ctx,
                                                    ObSQLSessionInfo &session_info,
                                                    ObFastParserResult &fp_result)
{
  int ret = OB_SUCCESS;
  bool can_fold_params = false;
  // used for no_param_sql --> new_no_param_sql
  int64_t old_sql_pos = 0;
  int64_t new_sql_pos = 0;
  int64_t array_param_idx = 0;
  int64_t last_raw_param_idx = 0;
  int64_t new_raw_idx = 0;
  int64_t total_delta_len = 0;
  ObString new_no_param_sql;
  ObSEArray<ObPCParam*, 4> temp_store;
  char *buff = NULL;
  int64_t buff_len = pc_ctx.raw_sql_.length();
  int64_t no_param_sql_pos = 0;
  int64_t new_raw_sql_pos = 0;
  ObString &new_raw_sql = pc_ctx.new_raw_sql_;
  ObSEArray<int64_t, 16> raw_pos;
  ObPhysicalPlanCtx *phy_ctx = NULL;
  uint64_t data_version = 0;
  if (pc_ctx.sql_ctx_.handle_batched_multi_stmt() ||
      lib::is_oracle_mode() ||
      session_info.is_inner() ||
      session_info.get_is_in_retry() ||
      fp_result.values_tokens_.empty() ||
      !GCONF._enable_values_table_folding) {
    /* do nothing */
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info.get_effective_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", K(ret), K(session_info.get_effective_tenant_id()));
  } else if (data_version < DATA_VERSION_4_2_1_0 ||
             !is_support_compress_values_table(pc_ctx.raw_sql_)) {
    /* do nothing */
  } else if (OB_ISNULL(phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fp_result.values_tokens_.count(); ++i) {
      bool is_valid = false;
      int64_t values_token_pos = fp_result.values_tokens_.at(i).no_param_sql_pos_;
      int64_t param_idx = fp_result.values_tokens_.at(i).param_idx_;  // idx in fp_result.raw_params_
      int64_t batch_count = 0;
      int64_t param_count = 0;
      int64_t delta_len = 0;
      if (OB_FAIL(ObValuesTableCompression::parser_values_row_str(allocator, fp_result.pc_key_.name_,
                                            values_token_pos, new_no_param_sql, old_sql_pos,
                                            new_sql_pos, batch_count, param_count, delta_len,
                                            is_valid))) {
        LOG_WARN("fail to parser insert string", K(ret), K(fp_result.pc_key_.name_));
      } else if (!is_valid || param_count <= 0 || batch_count <= 1 || delta_len <= 0) {
        LOG_TRACE("can not do batch opt", K(ret), K(is_valid), K(param_count), K(batch_count), K(delta_len));
      } else if (OB_FAIL(add_raw_array_params(allocator, pc_ctx, fp_result, param_idx, batch_count,
                                              param_count))) {
        LOG_WARN("fail to rebuild raw_param", K(ret));
      } else {
        if (!can_fold_params) {
          if (OB_ISNULL(buff = (char *)allocator.alloc(buff_len))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("buff is null", K(ret), KP(buff));
          } else {
            new_raw_sql.assign_ptr(buff, buff_len);
          }
        }
        for (int64_t j = last_raw_param_idx; OB_SUCC(ret) && j < param_idx + param_count; j++) {
          ObPCParam *pc_param = pc_ctx.fp_result_.raw_params_.at(j);
          if (OB_ISNULL(pc_param) || OB_ISNULL(pc_param->node_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("pc_param is null", K(ret), KP(pc_param));
          } else if (OB_FAIL(temp_store.push_back(pc_param))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(raw_pos.push_back(pc_param->node_->pos_ - total_delta_len))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(rebuild_new_raw_sql(pc_ctx, temp_store, new_raw_idx,
                           temp_store.count() - new_raw_idx, total_delta_len, new_no_param_sql,
                           new_raw_sql, no_param_sql_pos, new_raw_sql_pos))) {
          LOG_WARN("failed to rebuild new raw sql", K(ret));
        } else {
          int64_t batch_begin_idx = new_raw_idx + param_idx - last_raw_param_idx;
          if (OB_FAIL(phy_ctx->get_array_param_groups().push_back(ObArrayParamGroup(batch_count,
                                                                  param_count, batch_begin_idx)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {
            total_delta_len += delta_len;
            can_fold_params = true;
            last_raw_param_idx = param_idx + param_count * batch_count;
            new_raw_idx = temp_store.count();
          }
        }
      }
    }
    if (OB_SUCC(ret) && can_fold_params) {
      for (int64_t j = last_raw_param_idx; OB_SUCC(ret) && j < pc_ctx.fp_result_.raw_params_.count(); j++) {
        ObPCParam *pc_param = pc_ctx.fp_result_.raw_params_.at(j);
        if (OB_ISNULL(pc_param) || OB_ISNULL(pc_param->node_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pc_param is null", K(ret), KP(pc_param));
        } else if (OB_FAIL(temp_store.push_back(pc_param))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(raw_pos.push_back(pc_param->node_->pos_ - total_delta_len))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(rebuild_new_raw_sql(pc_ctx, temp_store, new_raw_idx,
                    temp_store.count() - new_raw_idx, total_delta_len, new_no_param_sql,
                    new_raw_sql, no_param_sql_pos, new_raw_sql_pos))) {
          LOG_WARN("failed to rebuild new raw sql", K(ret));
        } else if (OB_UNLIKELY(raw_pos.count() != temp_store.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param is invalid", K(ret));
        } else {
          int64_t len = new_no_param_sql.length() - no_param_sql_pos;
          if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(new_raw_sql_pos + len > buff_len)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected params", K(ret), K(len), K(new_raw_sql_pos));
          } else if (len > 0) {
            MEMCPY(buff + new_raw_sql_pos, new_no_param_sql.ptr() + no_param_sql_pos, len);
            new_raw_sql_pos += len;
            no_param_sql_pos += len;
            new_raw_sql.assign_ptr(buff, new_raw_sql_pos);
          }
        }
      }
    }
    // handle error.
    if (ret != OB_SUCCESS) {
      // 这里边的无论什么报错，都可以被吞掉，只是报错后就不能再做batch优化
      LOG_TRACE("failed to try fold params for values table", K(ret));
      phy_ctx->get_array_param_groups().reset();
      pc_ctx.fp_result_.array_params_.reset();
      pc_ctx.new_raw_sql_.reset();
      can_fold_params = false;
      ret = OB_SUCCESS;
    } else if (can_fold_params) {
      fp_result.pc_key_.name_.assign_ptr(new_no_param_sql.ptr(), new_no_param_sql.length());
      fp_result.raw_params_.reset();
      fp_result.raw_params_.set_allocator(&allocator);
      fp_result.raw_params_.set_capacity(temp_store.count());
      for (int64_t i = 0; i < temp_store.count(); i++) {
        // checked null before
        temp_store.at(i)->node_->pos_ = raw_pos.at(i);
      }
      if (OB_FAIL(fp_result.raw_params_.assign(temp_store))) {
        LOG_WARN("fail to assign raw_param", K(ret));
      }
    }
  }
  return ret;
}

/* for choose plan, will resolve all params */
int ObValuesTableCompression::resolve_params_for_values_clause(ObPlanCacheCtx &pc_ctx,
                                                  const stmt::StmtType stmt_type,
                                                  const ObIArray<NotParamInfo> &not_param_info,
                                                  const ObIArray<ObCharsetType> &param_charset_type,
                                                  const ObBitSet<> &neg_param_index,
                                                  const ObBitSet<> &not_param_index,
                                                  const ObBitSet<> &must_be_positive_idx,
                                                  ParamStore *&ab_params)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = pc_ctx.exec_ctx_.get_my_session();
  ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  ObObjParam obj_param;
  ObObjParam array_param;
  void *ptr = NULL;
  int64_t raw_param_cnt = pc_ctx.fp_result_.raw_params_.count();
  int64_t raw_idx = 0;  // idx in pc_ctx.fp_result_.raw_params_
  int64_t array_param_idx = 0;  // idx in pc_ctx.fp_result_.array_params_
  int64_t not_param_cnt = 0;
  bool is_param = false;
  if (OB_UNLIKELY(!pc_ctx.exec_ctx_.has_dynamic_values_table()) || OB_ISNULL(session) ||
      OB_ISNULL(phy_ctx) || OB_UNLIKELY(param_charset_type.count() != raw_param_cnt) ||
      OB_ISNULL(ab_params)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql should be mutil stmt", K(ret), KP(session), KP(phy_ctx), K(raw_param_cnt),
             K(param_charset_type.count()), KP(ab_params));
  } else if (OB_FAIL(ab_params->reserve(raw_param_cnt))) {
    LOG_WARN("failed to reserve param num", K(ret));
  } else {
    ParamStore &phy_param_store = phy_ctx->get_param_store_for_update();
    ObIArray<ObArrayParamGroup> &array_param_groups = phy_ctx->get_array_param_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < array_param_groups.count(); ++i) {
      int64_t param_num = array_param_groups.at(i).column_count_;
      int64_t batch_num = array_param_groups.at(i).row_count_;
      int64_t array_idx = array_param_groups.at(i).start_param_idx_;
      if (OB_UNLIKELY(array_idx + param_num > raw_param_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql should be mutil stmt", K(ret));
      }
      // 1.1 build params before batch group
      for (; OB_SUCC(ret) && raw_idx < array_idx; raw_idx++) {
        if (OB_FAIL(ObResolverUtils::resolver_param(pc_ctx, *session, phy_param_store, stmt_type,
                    param_charset_type.at(raw_idx), neg_param_index, not_param_index,
                    must_be_positive_idx, pc_ctx.fp_result_.raw_params_.at(raw_idx), raw_idx,
                    obj_param, is_param))) {
          LOG_WARN("failed to resolver param", K(ret), K(raw_idx));
        } else if (!is_param) {
          not_param_cnt++;  // in value clause, which wonn't happen actually
        } else if (OB_FAIL(ab_params->push_back(obj_param))) {
          LOG_WARN("fail to push item to array", K(ret), K(raw_idx));
        }
      }

      // 1.2 build array_param in batch group
      for (int64_t j = 0; OB_SUCC(ret) && j < param_num; j++, raw_idx++, array_param_idx++) {
        ObArrayPCParam *raw_array_param = pc_ctx.fp_result_.array_params_.at(array_param_idx);
        ObSEArray<ObExprResType, 4> res_types;
        ObSqlArrayObj *array_param_ptr = ObSqlArrayObj::alloc(pc_ctx.allocator_, batch_num);
        bool is_same = true;
        ObExprResType new_res_type;
        if (OB_ISNULL(array_param_ptr) || OB_ISNULL(raw_array_param)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          for (int64_t k = 0; OB_SUCC(ret) && k < batch_num; k++) {
            if (OB_FAIL(ObResolverUtils::resolver_param(pc_ctx, *session, phy_param_store, stmt_type,
                        param_charset_type.at(raw_idx), neg_param_index, not_param_index,
                        must_be_positive_idx, raw_array_param->at(k), raw_idx,
                        array_param_ptr->data_[k], is_param))) {
              LOG_WARN("failed to resolver param", K(ret), K(k), K(raw_idx), K(j));
            } else {
              const ObObjParam &param = array_param_ptr->data_[k];
              ObExprResType res_type;
              res_type.set_meta(ObSQLUtils::is_oracle_empty_string(param) ? param.get_param_meta() : param.get_meta());
              res_type.set_accuracy(param.get_accuracy());
              res_type.set_result_flag(param.get_result_flag());
              if (res_type.get_length() == -1) {
                if (res_type.is_varchar() || res_type.is_nvarchar2()) {
                  res_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
                } else if (res_type.is_char() || res_type.is_nchar()) {
                  res_type.set_length(OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
                }
              }
              if (k == 0) {
                new_res_type = res_type;
                if (OB_FAIL(res_types.push_back(res_type))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              } else if (k > 0 && is_same) {
                is_same = ObSQLUtils::is_same_type(res_type, res_types.at(0));
                if (!is_same) {
                  if (OB_FAIL(res_types.push_back(res_type))) {
                    LOG_WARN("failed to push back", K(ret));
                  }
                }
              } else {
                if (OB_FAIL(res_types.push_back(res_type))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              }
            }
          }
          /* 推导类型 */
          if (OB_SUCC(ret) && !is_same) {
            new_res_type.reset();
            ObExprVersion dummy_op(pc_ctx.allocator_);
            const ObLengthSemantics length_semantics = session->get_actual_nls_length_semantics();
            ObCollationType coll_type = CS_TYPE_INVALID;
            if (OB_FAIL(session->get_collation_connection(coll_type))) {
              LOG_WARN("fail to get_collation_connection", K(ret));
            } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                                                                        &res_types.at(0),
                                                                        res_types.count(),
                                                                        coll_type,
                                                                        false,
                                                                        length_semantics,
                                                                        session))) {
              LOG_WARN("failed to aggregate result type for merge", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            array_param.reset();
            array_param_ptr->element_.set_meta_type(new_res_type.get_obj_meta());
            array_param_ptr->element_.set_accuracy(new_res_type.get_accuracy());
            array_param.set_extend(reinterpret_cast<int64_t>(array_param_ptr), T_EXT_SQL_ARRAY);
            array_param.set_param_meta();
            array_param.get_param_flag().is_batch_parameter_ = true;
            if (OB_FAIL(ab_params->push_back(array_param))) {
              LOG_WARN("failed to push back param", K(ret));
            }
          }
        }
      }
    }
    for (; OB_SUCC(ret) && raw_idx < raw_param_cnt; raw_idx++) {
      if (OB_FAIL(ObResolverUtils::resolver_param(pc_ctx, *session, phy_param_store, stmt_type,
                  param_charset_type.at(raw_idx), neg_param_index, not_param_index,
                  must_be_positive_idx, pc_ctx.fp_result_.raw_params_.at(raw_idx), raw_idx,
                  obj_param, is_param))) {
        LOG_WARN("failed to resolver param", K(ret), K(raw_idx));
      } else if (!is_param) {
        not_param_cnt++;
      } else if (OB_FAIL(ab_params->push_back(obj_param))) {
        LOG_WARN("fail to push item to array", K(ret), K(raw_idx));
      }
    }
  }
  return ret;
}

/* after handle parser, only resolve array params */
int ObValuesTableCompression::resolve_params_for_values_clause(ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  const ObBitSet<> bit_set_dummy;
  ObSQLSessionInfo *session = pc_ctx.exec_ctx_.get_my_session();
  ObPhysicalPlanCtx *phy_ctx = pc_ctx.exec_ctx_.get_physical_plan_ctx();
  ObObjParam obj_param;
  ObObjParam array_param;
  int64_t raw_param_cnt = pc_ctx.fp_result_.raw_params_.count();
  int64_t raw_idx = 0;  // idx in pc_ctx.fp_result_.raw_params_
  int64_t array_param_idx = 0;  // idx in pc_ctx.fp_result_.array_params_
  bool is_param = false;
  const ObIArray<ObCharsetType> &param_charset_type = pc_ctx.param_charset_type_;
  if (OB_UNLIKELY(!pc_ctx.exec_ctx_.has_dynamic_values_table()) || OB_ISNULL(session) ||
      OB_ISNULL(phy_ctx) || OB_UNLIKELY(param_charset_type.count() != raw_param_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql should be mutil stmt", K(ret), KP(session), KP(phy_ctx), K(raw_param_cnt),
             K(param_charset_type.count()));
  } else {
    ParamStore &phy_param_store = phy_ctx->get_param_store_for_update();
    ObIArray<ObArrayParamGroup> &array_param_groups = phy_ctx->get_array_param_groups();
    for (int64_t i = 0; OB_SUCC(ret) && i < array_param_groups.count(); ++i) {
      int64_t param_num = array_param_groups.at(i).column_count_;
      int64_t batch_num = array_param_groups.at(i).row_count_;
      int64_t raw_idx = array_param_groups.at(i).start_param_idx_;
      if (OB_UNLIKELY(raw_idx + param_num > raw_param_cnt) ||
          OB_UNLIKELY(raw_idx + param_num > phy_param_store.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql should be mutil stmt", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < param_num; j++, raw_idx++, array_param_idx++) {
        ObArrayPCParam *raw_array_param = pc_ctx.fp_result_.array_params_.at(array_param_idx);
        ObSEArray<ObExprResType, 4> res_types;
        ObSqlArrayObj *array_param_ptr = ObSqlArrayObj::alloc(pc_ctx.allocator_, batch_num);
        bool is_same = true;
        ObExprResType new_res_type;
        if (OB_ISNULL(array_param_ptr) || OB_ISNULL(raw_array_param)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          for (int64_t k = 0; OB_SUCC(ret) && k < batch_num; k++) {
            if (OB_FAIL(ObResolverUtils::resolver_param(pc_ctx, *session, phy_param_store, stmt::T_SELECT,
                        param_charset_type.at(raw_idx), bit_set_dummy, bit_set_dummy,
                        bit_set_dummy, raw_array_param->at(k), raw_idx,
                        array_param_ptr->data_[k], is_param))) {
              LOG_WARN("failed to resolver param", K(ret), K(k), K(raw_idx), K(j));
            } else {
              const ObObjParam &param = array_param_ptr->data_[k];
              ObExprResType res_type;
              res_type.set_meta(ObSQLUtils::is_oracle_empty_string(param) ? param.get_param_meta() : param.get_meta());
              res_type.set_accuracy(param.get_accuracy());
              res_type.set_result_flag(param.get_result_flag());
              if (res_type.get_length() == -1) {
                if (res_type.is_varchar() || res_type.is_nvarchar2()) {
                  res_type.set_length(OB_MAX_ORACLE_VARCHAR_LENGTH);
                } else if (res_type.is_char() || res_type.is_nchar()) {
                  res_type.set_length(OB_MAX_ORACLE_CHAR_LENGTH_BYTE);
                }
              }
              if (k == 0) {
                new_res_type = res_type;
                if (OB_FAIL(res_types.push_back(res_type))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              } else if (k > 0 && is_same) {
                is_same = ObSQLUtils::is_same_type(res_type, res_types.at(0));
                if (!is_same) {
                  if (OB_FAIL(res_types.push_back(res_type))) {
                    LOG_WARN("failed to push back", K(ret));
                  }
                }
              } else {
                if (OB_FAIL(res_types.push_back(res_type))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              }
            }
          }
          /* 推导类型 */
          if (OB_SUCC(ret) && !is_same) {
            new_res_type.reset();
            ObExprVersion dummy_op(pc_ctx.allocator_);
            const ObLengthSemantics length_semantics = session->get_actual_nls_length_semantics();
            ObCollationType coll_type = CS_TYPE_INVALID;
            if (OB_FAIL(session->get_collation_connection(coll_type))) {
              LOG_WARN("fail to get_collation_connection", K(ret));
            } else if (OB_FAIL(dummy_op.aggregate_result_type_for_merge(new_res_type,
                                                                        &res_types.at(0),
                                                                        res_types.count(),
                                                                        coll_type,
                                                                        false,
                                                                        length_semantics,
                                                                        session))) {
              LOG_WARN("failed to aggregate result type for merge", K(ret));
            } else {
              LOG_TRACE("get result type", K(new_res_type), K(res_types));
            }
          }
          if (OB_SUCC(ret)) {
            array_param.reset();
            array_param_ptr->element_.set_meta_type(new_res_type.get_obj_meta());
            array_param_ptr->element_.set_accuracy(new_res_type.get_accuracy());
            array_param.set_extend(reinterpret_cast<int64_t>(array_param_ptr), T_EXT_SQL_ARRAY);
            array_param.set_param_meta();
            array_param.get_param_flag().is_batch_parameter_ = true;
            phy_param_store.at(raw_idx) = array_param;
          }
        }
      }
    }
  }
  return ret;
}

void ObValuesTableCompression::skip_space(ObRawSql &raw_sql)
{
  int64_t space_len = 0;
  bool is_space = true;
  while (!raw_sql.search_end_ && is_space && raw_sql.cur_pos_ < raw_sql.raw_sql_len_) {
    if (is_mysql_space(raw_sql.raw_sql_[raw_sql.cur_pos_])) {
      raw_sql.scan(1);
    } else {
      is_space = false;
    }
  }
}

bool ObValuesTableCompression::skip_row_constructor(ObRawSql &raw_sql)
{
  bool b_ret = false;
  int64_t space_len = 0;
  if (!raw_sql.search_end_ && 0 == raw_sql.strncasecmp(raw_sql.cur_pos_, "row", 3)) {
    raw_sql.scan(3);
    skip_space(raw_sql);
    b_ret = true;
  }
  return b_ret;
}

/* only row(?,?,?) is valid */
void ObValuesTableCompression::get_one_row_str(ObRawSql &no_param_sql,
                                               int64_t &param_count,
                                               int64_t &end_pos,
                                               bool &is_valid)
{
  enum ROW_STATE {
    START_STATE = 0,
    LEFT_PAR_STATE, // "("
    PARS_MATCH,     // ")"
    UNEXPECTED_STATE
  };
  ROW_STATE row_state = START_STATE;
  int left_count = 0;
  int comma_count = 0;
  bool need_break = false;
  int64_t curr_pos = 0;
  is_valid = false;
  param_count = 0;
  end_pos = 0;
  skip_space(no_param_sql);
  if (no_param_sql.is_search_end() || !skip_row_constructor(no_param_sql)) {
    /* do nothing */
  } else {
    while (!need_break && !no_param_sql.is_search_end()) {
      skip_space(no_param_sql);
      char ch = no_param_sql.char_at(no_param_sql.cur_pos_);
      curr_pos = no_param_sql.cur_pos_;
      no_param_sql.scan(1);
      /* state machine */
      switch (row_state) {
        case START_STATE:
          if ('(' == ch) {
            row_state = LEFT_PAR_STATE;
            left_count++;
          } else {
            row_state = UNEXPECTED_STATE;
          }
          break;
        case LEFT_PAR_STATE:
          if (')' == ch) {
            left_count--;
            if (0 == left_count) {
              row_state = PARS_MATCH;
              end_pos = curr_pos;
            }
          } else if ('(' == ch) {
            left_count++;
          } else if ('?' == ch) {
            param_count++;
          } else if (',' == ch && comma_count + 1 == param_count) {
            comma_count++;
          } else {
            row_state = UNEXPECTED_STATE;
          }
          break;
        case PARS_MATCH:
          if (',' != ch) {
            no_param_sql.search_end_ = true;
          }
          need_break = true;
          break;
        case UNEXPECTED_STATE:
          need_break = true;
          break;
        default:
          break;
      }
    }
    if (PARS_MATCH == row_state) {
      if (param_count > 0 && param_count == comma_count + 1) {
        is_valid = true;
      }
    }
  }
}

int ObValuesTableCompression::parser_values_row_str(ObIAllocator &allocator,
                                                    const ObString &no_param_sql,
                                                    const int64_t values_token_pos,
                                                    ObString &new_no_param_sql,
                                                    int64_t &old_pos,
                                                    int64_t &new_pos,
                                                    int64_t &row_count,
                                                    int64_t &param_count,
                                                    int64_t &delta_length,
                                                    bool &can_batch_opt)
{
  int ret = OB_SUCCESS;
  int64_t no_param_sql_len = no_param_sql.length();
  ObRawSql raw_sql;
  raw_sql.init(no_param_sql.ptr(), no_param_sql_len);
  raw_sql.cur_pos_ = values_token_pos;
  int64_t new_sql_len = 0;
  int64_t cur_param_count = 0;
  int64_t end_pos = 0;
  int64_t first_sql_end_pos = 0;
  bool is_first = true;
  bool is_valid = true;
  row_count = 0;
  param_count = 0;
  delta_length = 0;
  can_batch_opt = false;
  if (0 == raw_sql.strncasecmp(raw_sql.cur_pos_, "values", 6)) {
    raw_sql.scan(6);
    while (OB_SUCC(ret) && is_valid && !raw_sql.is_search_end()) {
      get_one_row_str(raw_sql, cur_param_count, end_pos, is_valid);
      if (!is_valid) {
      } else if (is_first) {
        param_count = cur_param_count;
        first_sql_end_pos = end_pos;
        is_first = false;
      } else if (cur_param_count != param_count) {
        LOG_WARN("should not be here", K(ret), K(cur_param_count), K(param_count));
      }
      row_count++;
    }

    if (OB_SUCC(ret) && is_valid && row_count > 1) {
      char *buffer = NULL;
      if (OB_ISNULL(buffer = static_cast<char*>(allocator.alloc(no_param_sql_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(no_param_sql_len));
      } else {
        // init
        int64_t length = new_pos;
        MEMCPY(buffer, new_no_param_sql.ptr(), length);
        new_sql_len += length;

        length = first_sql_end_pos + 1 - old_pos;
        MEMCPY(buffer + new_pos, no_param_sql.ptr() + old_pos, length);
        new_sql_len += length;
        new_pos += length;
        old_pos = end_pos + 1;

        length = no_param_sql_len - old_pos;
        MEMCPY(buffer + new_pos, no_param_sql.ptr() + old_pos, length);
        new_sql_len += length;

        delta_length = end_pos - first_sql_end_pos;
        new_no_param_sql.assign_ptr(buffer, new_sql_len);
        can_batch_opt = true;
      }
    }
  }
  return ret;
}

}
}