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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_search.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/signal/safe_snprintf.h"
#include "ob_expr_json_func_helper.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

// cmp for ObJsonBaseSortedVector
struct ObJsonBufferPathCmp {
  bool operator()(const ObJsonBuffer* a, const ObJsonBuffer* b) {
    return (a->string().compare(b->string()) < 0);
  }
};

// cmp for ObJsonBaseSortedVector
struct ObJsonBufferPathUnique {
  bool operator()(const ObJsonBuffer* a, const ObJsonBuffer* b){
    return (a->string().compare(b->string()) == 0);
  }
};

int ObExprJsonSearch::add_path_unique(const ObJsonBuffer* path,
                                      ObJsonBufferSortedVector &duplicates,
                                      ObJsonBufferVector &hits)
{
  INIT_SUCC(ret);
  ObJsonBufferPathCmp path_cmp;
  ObJsonBufferPathUnique path_unique;
  ObJsonBufferSortedVector::iterator insert_pos = duplicates.end();

  if ((OB_SUCC(duplicates.insert_unique(path, insert_pos, path_cmp, path_unique)))) {
    if (OB_FAIL(hits.push_back(path))) {
      LOG_WARN("fail to push_back path into result", K(ret));
    }
  } else if (ret == OB_CONFLICT_VALUE) { // value is already insert, set ret to success
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObExprJsonSearch::ObExprJsonSearch::path_add_key(ObJsonBuffer &path, ObString key)
{
  INIT_SUCC(ret);
  bool is_ecmas = ObJsonPathUtil::is_ecmascript_identifier(key.ptr(), key.length());

  if (!is_ecmas) {
    if (OB_FAIL(ObJsonPathUtil::double_quote(key, &path))) {
      LOG_WARN("failed to add quote.", K(ret), K(key));
    }
  } else {
    if (OB_FAIL(path.append(key))) {
      LOG_WARN("failed to append key to path.", K(ret), K(key));
    }
  }

  return ret;
}

int ObExprJsonSearch::find_matches(common::ObIAllocator *allocator,
                                   const ObIJsonBase *j_base,
                                   ObJsonBuffer &path,
                                   ObJsonBufferVector &hits,
                                   ObJsonBufferSortedVector &duplicates,
                                   const ObString &target,
                                   bool one_match,
                                   const int32_t &escape_wc)
{
  INIT_SUCC(ret);

  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null", K(ret));
  } else if (one_match && hits.size() > 0) {
    // do nothing
  } else {
    switch (j_base->json_type()) {
      case ObJsonNodeType::J_STRING: {
        const char *data = j_base->get_data();
        uint64_t length = j_base->get_data_length();
        ObString curr_str(length, data);
        bool b = ObCharset::wildcmp(CS_TYPE_UTF8MB4_BIN, curr_str, target, escape_wc,
                                    static_cast<int32_t>('_'), static_cast<int32_t>('%'));
        if (b) {
          void *buf = allocator->alloc(sizeof(ObJsonBuffer));
          if (OB_ISNULL(buf)){
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("falied to alloc new path buffer.", K(ret));
          } else {
            ObJsonBuffer *temp = new (buf) ObJsonBuffer(allocator);
            if (OB_FAIL(temp->append(path.ptr(), path.length()))) {
              LOG_WARN("failed to append curr path.", K(ret));
            } else if (OB_FAIL(add_path_unique(temp, duplicates, hits))) {
              LOG_WARN("failed to add path to duplicates.", K(ret));
            }
          }
        }
        break;
      }

      case ObJsonNodeType::J_OBJECT: {
        uint64_t pos = path.length();
        uint64_t count = j_base->element_count();
        bool is_finish = false;
        for (uint64_t i = 0; OB_SUCC(ret) && i < count && !is_finish; i++) {
          if (one_match && hits.size() > 0) {
            is_finish = true;
          } else {
            ObIJsonBase *child = NULL;
            ret = j_base->get_object_value(i, child);
            if (OB_ISNULL(child)) {
              ret = OB_ERR_NULL_VALUE;
              LOG_WARN("fail to get child_dom",K(ret), K(i));
            } else {
              ObString key;
              if (OB_FAIL(j_base->get_key(i, key))) {
                LOG_WARN("fail to get key by index.",K(ret), K(i));
              } else if (OB_FAIL(path.append("."))) {
                LOG_WARN("fail to append key to path",K(ret), K(i));
              } else if (OB_FAIL(path_add_key(path, key))) {
                LOG_WARN("fail to append key to path",K(ret), K(i));
              } else if (OB_FAIL(find_matches(allocator, child, path, hits, duplicates,
                  target, one_match, escape_wc))) {
                LOG_WARN("fail to seek recursively",K(ret), K(i));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(path.set_length(pos))) { // reset path pos
              LOG_WARN("failed to reset path length.", K(ret), K(path.capacity()), K(pos));
            }
          }
        }
        break;
      }

      case ObJsonNodeType::J_ARRAY: {
        uint64_t pos = path.length();
        uint64_t size = j_base->element_count();
        bool is_finish = false;
        for (uint64_t i = 0; OB_SUCC(ret) && i < size && !is_finish; i++) {
          if (one_match && hits.size() > 0) {
            is_finish = true;
          } else {
            ObIJsonBase *child = NULL;
            ret = j_base->get_array_element(i, child);
            if (OB_ISNULL(child)) {
              ret = OB_ERR_NULL_VALUE;
              LOG_WARN("fail to get child_dom",K(ret), K(i));
            } else {
              uint64_t reserve_len = i == 0 ? 3 : static_cast<uint64_t>(std::log10(i)) + 3;
              char temp_buf[reserve_len + 1];
              int64_t count = safe_snprintf(temp_buf, reserve_len + 1, "[%lu]", i);
              if (count < 0) {
                LOG_WARN("fail to snprintf", K(i), K(count));
              } else if (OB_FAIL(path.append(temp_buf, count))) {
                LOG_WARN("fail to append key to path",K(ret), K(i));
              } else if (OB_FAIL(find_matches(allocator, child, path, hits, duplicates,
                  target, one_match, escape_wc))) {
                LOG_WARN("fail to seek recursively",K(ret), K(i));
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(path.set_length(pos))) { // reset path pos
              LOG_WARN("failed to reset path length.", K(ret), K(path.capacity()), K(pos));
            }
          }
        }
        break; 
      }

      default: {
        // do nothing
        break;
      }
    }
  }

  return ret;
}

ObExprJsonSearch::ObExprJsonSearch(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_SEARCH, N_JSON_SEARCH, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonSearch::~ObExprJsonSearch()
{
}

int ObExprJsonSearch::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);

  if (OB_UNLIKELY(param_num < 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    
    // json doc
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, "json_search"))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    }

    // [one_or_all][target_str][escape][path_string...]
    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
      if (types_stack[i].get_type() == ObNullType) {
      } else if (ob_is_string_type(types_stack[i].get_type())) {
        if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[i].set_calc_type(ObLongTextType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  }

  return ret;
}

int ObExprJsonSearch::eval_json_search(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObIJsonBase *j_base = NULL;
  bool is_null = false;

  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
      j_base, is_null, false))) {
    LOG_WARN("failed to get json doc.", K(ret));
  }

  // check one_or_all flag
  bool one_flag = false;
  if (OB_SUCC(ret) && !is_null) {
    json_arg = expr.args_[1];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else if (!ob_is_string_type(val_type)) {
      LOG_WARN("input type error", K(val_type));
    } else {
      ObString target_str = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target_str, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(target_str));
      } else if (0 == target_str.case_compare("one")) {
        one_flag = true;
      } else if (0 == target_str.case_compare("all")) {
        one_flag = false;
      } else {
        ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
        LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
      }
    }
  }

  // check escape if exist
  int32_t escape_wc = static_cast<int32_t>('\\'); // use \ for default escape
  if (OB_SUCC(ret) && expr.arg_cnt_ >= 4 && !is_null) {
    json_arg = expr.args_[3];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      // do nothing, null type use default escape
    } else if (!ob_is_string_type(val_type)) {
      LOG_WARN("input type error", K(val_type));
    } else {
      ObString escape = json_datum->get_string();
      bool is_null_str = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, escape, is_null_str))) {
        LOG_WARN("fail to get real data.", K(ret), K(escape));
      } else if (escape.length() > 0) {
        const ObCollationType escape_coll = json_arg->datum_meta_.cs_type_;
        size_t length = ObCharset::strlen_char(escape_coll, escape.ptr(), escape.length());
        if (length != 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument to ESCAPE", K(escape), K(length), K(ret));
        } else if (OB_FAIL(ObCharset::mb_wc(escape_coll, escape, escape_wc))) {
          LOG_WARN("failed to convert escape to wc", K(ret), K(escape),
                  K(escape_coll), K(escape_wc));
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }

  // get target string
  ObString target;
  if (OB_SUCC(ret) && !is_null) {
    json_arg = expr.args_[2];
    val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else if (!ob_is_string_type(val_type)) {
      LOG_WARN("input type error", K(val_type));
    } else {
      target = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(target));
      }
    }
  }

  ObJsonBuffer path_str(&temp_allocator);
  ObJsonBufferVector hits;
  ObJsonBufferSortedVector duplicates;
  if (OB_SUCC(ret) && !is_null) {
    if (expr.arg_cnt_ < 5) {
      if (OB_FAIL(path_str.append("$"))) {
        LOG_WARN("faild to append '$' to path str.", K(ret));
      } else if (OB_FAIL(find_matches(&temp_allocator, j_base, path_str, hits, duplicates,
          target, one_flag, escape_wc))) {
        LOG_WARN("failed to find matches for path.", K(ret), K(one_flag), K(escape_wc));
      }
    } else {
      // read all JsonPath from arg
      ObVector<ObJsonPath*> json_paths;
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache *path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_,
          &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      for (uint64_t i = 4; OB_SUCC(ret) && !is_null && i < expr.arg_cnt_; i++) {        
        json_arg = expr.args_[i];
        val_type = json_arg->datum_meta_.type_;
        if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
          LOG_WARN("eval json arg failed", K(ret));
        } else if (val_type == ObNullType || json_datum->is_null()) {
          is_null = true;
        } else if (!ob_is_string_type(val_type)) {
          LOG_WARN("input type error", K(val_type));
        } else {
          ObString j_path_text = json_datum->get_string();
          ObJsonPath *j_path;
          if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, j_path_text, is_null))) {
            LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
          } else if (j_path_text.length() == 0) {
            is_null = true;
          } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path,
              j_path_text, i, true))) {
            LOG_WARN("parse text to path failed", K(j_path_text), K(ret));
          } else if (OB_FAIL(json_paths.push_back(j_path))) {
            LOG_WARN("push new path to vector failed", K(i), K(ret));
          }
        }
      }

      bool is_finish = false;
      for (uint64_t i = 4; OB_SUCC(ret) && !is_null && i < expr.arg_cnt_ && !is_finish; i++) {
        ObJsonPath *j_path = json_paths[i - 4];
        ObJsonBaseVector hit;
        if (one_flag && hits.size() > 0) {
          is_finish = true;
        } else if (j_path->can_match_many()) {
          ObIJsonBase *j_temp = NULL;
          if (OB_FAIL(ObJsonBaseFactory::transform(&temp_allocator, j_base, ObJsonInType::JSON_TREE,
              j_temp))) {
            LOG_WARN("failed to transform to tree", K(ret), K(*j_base));
          } else {
            j_base = j_temp;
          }
        } 
        
        if (OB_SUCC(ret)) {
          if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), false, false, hit))) {
            LOG_WARN("failed to seek path", K(ret), K(i));
          } else {
            bool is_finish_inner = false;
            for (uint64_t j = 0; OB_SUCC(ret) && !is_null && j < hit.size()
                && !is_finish_inner; j++) {
              if (one_flag && hits.size() > 0) {
                is_finish_inner = true;
              } else {
                path_str.reuse();
                if (j_path->can_match_many()) {
                  if (OB_FAIL(hit[j]->get_location(path_str))) {
                    LOG_WARN("falied to get loaction.", K(ret));
                  }
                } else if (OB_FAIL(j_path->to_string(path_str))) {
                  LOG_WARN("falied to get string for path.", K(ret));
                }

                if (OB_SUCC(ret)) {
                  if (OB_FAIL(find_matches(&temp_allocator, hit[j], path_str, hits, duplicates,
                      target, one_flag, escape_wc))) {
                    LOG_WARN("failed to find matches.", K(ret), K(j), K(one_flag), K(escape_wc));
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  // packed string to return
  ObIJsonBase *j_res = NULL;
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    LOG_WARN("json_search failed", K(ret));
  } else if (hits.size() == 0) {
    is_null = true;
  } else if (hits.size() == 1) {
    void *buf = temp_allocator.alloc(sizeof(ObJsonString));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("json_search alloc ObJsonString failed", K(ret));
    } else {
      ObJsonString *j_str = new (buf) ObJsonString(hits[0]->ptr(), hits[0]->length());
      j_res = j_str;
    }
  } else {
    void *buf = temp_allocator.alloc(sizeof(ObJsonArray));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("json_search alloc ObJsonArray failed", K(ret));
    } else {
      ObJsonArray *j_arr = new (buf) ObJsonArray(&temp_allocator);
      for (int32_t i = 0; OB_SUCC(ret) && i < hits.size(); i++) {
        buf = temp_allocator.alloc(sizeof(ObJsonString));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("json_search alloc jsonString failed", K(ret));
        } else {
          ObJsonString *j_str = new (buf) ObJsonString(hits[i]->ptr(), hits[i]->length());
          if (OB_FAIL(j_arr->append(j_str))) {
            LOG_WARN("failed to append path to array.", K(ret), K(i), K(*j_str));
          }
        }
      }
      if (OB_SUCC(ret)) {
        j_res = j_arr;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_null) {
      res.set_null();
    } else {
      ObString raw_bin;
      if (OB_FAIL(j_res->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("json_keys get result binary failed", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
    }
  }

  return ret;
}

int ObExprJsonSearch::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_search;
  return OB_SUCCESS;
}


}
}
