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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_SQL_PARAMETERIZATION_
#define OCEANBASE_SQL_PLAN_CACHE_OB_SQL_PARAMETERIZATION_

#include "lib/hash/ob_link_hashmap.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "common/object/ob_object.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/ob_fast_parser.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_id_manager_allocator.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace sql
{
typedef common::LinkHashValue<common::ObString> ParameterizationHashValue;

struct SqlInfo: public ParameterizationHashValue
{
  SqlInfo &operator=(const SqlInfo &that)
  {
    total_ = that.total_;
    not_param_index_ = that.not_param_index_;
    neg_param_index_ = that.neg_param_index_;
    fixed_param_index_ = that.fixed_param_index_;
    param_charset_type_ = that.param_charset_type_;
    sql_traits_ = that.sql_traits_;
    last_active_time_ = that.last_active_time_;
    hit_count_ = that.hit_count_;
    trans_from_minus_index_ = that.trans_from_minus_index_;
    must_be_positive_index_ = that.must_be_positive_index_;
    ps_not_param_offsets_ = that.ps_not_param_offsets_;
    fixed_param_idx_ = that.fixed_param_idx_;
    no_check_type_offsets_ = that.no_check_type_offsets_;
    need_check_type_param_offsets_ = that.need_check_type_param_offsets_;
    ps_need_parameterized_ = that.ps_need_parameterized_;
    parse_infos_ = that.parse_infos_;
    need_check_fp_ = that.need_check_fp_;
    return *this;
  }
  void destroy() {}
  int64_t total_;
  common::ObBitSet<> not_param_index_;
  common::ObBitSet<> neg_param_index_;
  common::ObBitSet<> fixed_param_index_;//记录限流语句中可参数化的位置不为?的位置
  common::ObBitSet<> trans_from_minus_index_;
  common::ObBitSet<> must_be_positive_index_; // 记录那些常量必须是正数
  common::ObSEArray<common::ObCharsetType, 16> param_charset_type_;
  ObSqlTraits sql_traits_;

  int64_t last_active_time_;
  uint64_t hit_count_;

  common::ObSEArray<int64_t, 32> ps_not_param_offsets_; //used for ps mode not param
  common::ObSEArray<int64_t, 32> fixed_param_idx_;
  common::ObSEArray<int64_t, 32> no_check_type_offsets_;
  common::ObBitSet<> need_check_type_param_offsets_;
  // Check whether the ps parameter can be a parameterized template.
  // this is the optimization of the following statement hitting the same plan_cache:
  // prepare stmt from 'select * from t1 where c1 = 1 and c2 = 1';
  // prepare stmt from 'select * from t1 where c1 = 1 and c2 = ?';
  // prepare stmt from 'select * from t1 where c1 = ? and c2 = ?';
  // but if the SQL statement contains "is null" or "is not null" or "no wait 1", etc.
  // this optimization is not done
  // because this will cause the parameterized SQL to report a syntax error in the parser.
  // such as prepare stmt from 'select * from t1 where c1 = ? and c2 is null';
  bool ps_need_parameterized_;
  common::ObSEArray<ObPCParseInfo, 4> parse_infos_;
  bool need_check_fp_;

  SqlInfo();
};

struct TransformTreeCtx;
struct SelectItemTraverseCtx;

enum SQL_EXECUTION_MODE
{
  INVALID_MODE = -1,
  TEXT_MODE,
  PS_PREPARE_MODE,
  PS_EXECUTE_MODE,
  PL_PREPARE_MODE,
  PL_EXECUTE_MODE,
  MAX_EXECUTION_MODE
};

class ObSqlParameterization
{
public:
  static const int64_t SQL_PARAMETERIZATION_BUCKET_NUM = 1L << 20;
  static const int64_t NO_VALUES = -1;        //表示没有values()
  static const int64_t VALUE_LIST_LEVEL = 0;  //表示在parse的T_VALUE_LIST层
  static const int64_t VALUE_VECTOR_LEVEL = 1;//表示在parse的T_VALUE_VECTOR层
  static const int64_t ASSIGN_LIST_LEVEL = 0;
  static const int64_t ASSIGN_ITEM_LEVEL = 1;

  ObSqlParameterization() {}
  virtual ~ObSqlParameterization() {}
  static int fast_parser(common::ObIAllocator &allocator,
                         const FPContext &fp_ctx,
                         const common::ObString &sql,
                         ObFastParserResult &fp_result);

  static int transform_syntax_tree(common::ObIAllocator &allocator,
                                   const ObSQLSessionInfo &session,
                                   const ObIArray<ObPCParam *> *raw_params,
                                   ParseNode *tree,
                                   SqlInfo &sql_info,
                                   ParamStore &params,
                                   SelectItemParamInfoArray *select_item_param_infos,
                                   share::schema::ObMaxConcurrentParam::FixParamStore &fixed_param_store,
                                   bool is_transform_outline,
                                   SQL_EXECUTION_MODE execution_mode = INVALID_MODE,
                                   const ObIArray<FixedParamValue> *udr_fixed_params = NULL,
                                   bool is_from_pl = false);
  static int raw_fast_parameterize_sql(common::ObIAllocator &allocator,
                                       const ObSQLSessionInfo &session,
                                       const common::ObString &sql,
                                       common::ObString &no_param_sql,
                                       common::ObIArray<ObPCParam *> &raw_params,
                                       ParseMode parse_mode = FP_MODE);
  static int check_and_generate_param_info(const common::ObIArray<ObPCParam *> &raw_params,
                                           const SqlInfo &not_param_info,
                                           common::ObIArray<ObPCParam *> &special_param_info);
  static int transform_neg_param(ObIArray<ObPCParam *> &pc_params);
  static int construct_not_param(const ObString &no_param_sql,
                                  ObPCParam *pc_param,
                                  char *buf,
                                  int32_t buf_len,
                                  int32_t &pos,
                                  int32_t &idx);
  static int construct_neg_param(const ObString &no_param_sql,
                                  ObPCParam *pc_param,
                                  char *buf,
                                  int32_t buf_len,
                                  int32_t &pos,
                                  int32_t &idx);
  static int construct_trans_neg_param(const ObString &no_param_sql,
                                      ObPCParam *pc_param,
                                      char *buf,
                                      int32_t buf_len,
                                      int32_t &pos,
                                      int32_t &idx);
  static int construct_sql(const common::ObString &no_param_sql,
                           common::ObIArray<ObPCParam *> &not_params,
                           char *buf,
                           int32_t buf_len,
                           int32_t &pos);
  static int construct_sql_for_pl(const common::ObString &no_param_sql,
                                  common::ObIArray<ObPCParam *> &not_params,
                                  char *buf,
                                  int32_t buf_len,
                                  int32_t &pos);
  static int parameterize_syntax_tree(common::ObIAllocator &allocator,
                                      bool is_transform_outline,
                                      ObPlanCacheCtx &pc_ctx,
                                      ParseNode *tree,
                                      ParamStore &params,
                                      ObCharsets4Parser charsets4parser);
  static int gen_special_param_info(SqlInfo &sql_info, ObPlanCacheCtx &pc_ctx);
  static int gen_ps_not_param_var(const ObIArray<int64_t> &offsets,
                                  ParamStore &params,
                                  ObPlanCacheCtx &pc_ctx);
  static int construct_no_check_type_params(const ObIArray<int64_t> &no_check_type_offsets,
                                            const ObBitSet<> &need_check_type_offsets,
                                            ParamStore &params);
  static int insert_neg_sign(common::ObIAllocator &alloc_buf, ParseNode *node);
  static bool is_tree_not_param(const ParseNode *tree);
  static SQL_EXECUTION_MODE get_sql_execution_mode(ObPlanCacheCtx &pc_ctx);
  static bool is_prepare_mode(SQL_EXECUTION_MODE mode);
  static bool is_execute_mode(SQL_EXECUTION_MODE mode);
  static bool is_ignore_scale_check(TransformTreeCtx &ctx, const ParseNode *parent);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlParameterization);
  static int is_fast_parse_const(TransformTreeCtx &ctx);

  static bool is_node_not_param(TransformTreeCtx &ctx);
  static bool is_udr_not_param(TransformTreeCtx &ctx);
  static int transform_tree(TransformTreeCtx &ctx, const ObSQLSessionInfo &session_info);
  static int add_param_flag(const ParseNode *node, SqlInfo &sql_info);
  static int add_not_param_flag(const ParseNode *node, SqlInfo &sql_info);
  static int add_varchar_charset(const ParseNode *node, SqlInfo &sql_info);
  static int mark_args(ParseNode *arg_tree,
                       const bool *mark_arr,
                       int64_t arg_num,
                       SqlInfo &sql_info);
  static int mark_tree(ParseNode *tree, SqlInfo &sql_info);
  static int get_related_user_vars(const ParseNode *tree, common::ObIArray<common::ObString> &user_vars);

  static int get_select_item_param_info(const common::ObIArray<ObPCParam *> &raw_params,
                                        ParseNode *tree,
                                        SelectItemParamInfoArray *select_item_param_infos);
  static int parameterize_fields(SelectItemTraverseCtx &ctx);

  static int resolve_paramed_const(SelectItemTraverseCtx &ctx);
  static int transform_minus_op(ObIAllocator &, ParseNode *, bool is_from_pl=false);

  static int find_leftest_const_node(ParseNode &cur_node, ParseNode *&const_node);
  static bool need_fast_parser(const ObString &sql);
};

}
}

#endif /* _OB_SQL_PARAMETERIZATION_H */
