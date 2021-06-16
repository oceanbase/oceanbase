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
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_id_manager_allocator.h"
#include "sql/plan_cache/ob_plan_cache_util.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}

namespace sql {
typedef common::LinkHashValue<common::ObString> ParameterizationHashValue;

struct SqlInfo : public ParameterizationHashValue {
  SqlInfo& operator=(const SqlInfo& that)
  {
    total_ = that.total_;
    not_param_index_ = that.not_param_index_;
    neg_param_index_ = that.neg_param_index_;
    fixed_param_index_ = that.fixed_param_index_;
    param_charset_type_ = that.param_charset_type_;
    sql_traits_ = that.sql_traits_;
    last_active_time_ = that.last_active_time_;
    hit_count_ = that.hit_count_;
    change_char_index_ = that.change_char_index_;
    trans_from_minus_index_ = that.trans_from_minus_index_;
    must_be_positive_index_ = that.must_be_positive_index_;
    return *this;
  }
  void destroy()
  {}
  int64_t total_;
  common::ObBitSet<> not_param_index_;
  common::ObBitSet<> neg_param_index_;
  common::ObBitSet<> fixed_param_index_;
  common::ObBitSet<> trans_from_minus_index_;
  common::ObBitSet<> must_be_positive_index_;
  common::ObSEArray<common::ObCharsetType, 16> param_charset_type_;
  ObSqlTraits sql_traits_;

  int64_t last_active_time_;
  uint64_t hit_count_;

  common::ObBitSet<> change_char_index_;

  SqlInfo();
};

class TransformTreeCtx;
struct SelectItemTraverseCtx;

class ObSqlParameterization {
public:
  static const int64_t SQL_PARAMETERIZATION_BUCKET_NUM = 1L << 20;
  static const int64_t NO_VALUES = -1;
  static const int64_t VALUE_LIST_LEVEL = 0;
  static const int64_t VALUE_VECTOR_LEVEL = 1;

  ObSqlParameterization()
  {}
  virtual ~ObSqlParameterization()
  {}
  static int fast_parser(common::ObIAllocator& allocator, ObSQLMode sql_mode,
      common::ObCollationType connection_collation, const common::ObString& sql, const bool enable_batched_multi_stmt,
      ObFastParserResult& fp_result);

  static int transform_syntax_tree(common::ObIAllocator& allocator, const ObSQLSessionInfo& session,
      const ObIArray<ObPCParam*>* raw_params, const int64_t sql_offset, ParseNode* tree, SqlInfo& sql_info,
      ParamStore& params, SelectItemParamInfoArray* select_item_param_infos,
      share::schema::ObMaxConcurrentParam::FixParamStore& fixed_param_store, bool is_transform_outline);
  static int raw_fast_parameterize_sql(common::ObIAllocator& allocator, const ObSQLSessionInfo& session,
      const common::ObString& sql, common::ObString& no_param_sql, common::ObIArray<ObPCParam*>& raw_params,
      ParseMode parse_mode = FP_MODE);
  static int check_and_generate_param_info(const common::ObIArray<ObPCParam*>& raw_params,
      const SqlInfo& not_param_info, common::ObIArray<ObPCParam*>& special_param_info);
  static int construct_sql(const common::ObString& no_param_sql, common::ObIArray<ObPCParam*>& not_params, char* buf,
      int32_t buf_len, int32_t& pos);
  static int parameterize_syntax_tree(common::ObIAllocator& allocator, bool is_transform_outline,
      ObPlanCacheCtx& pc_ctx, ParseNode* tree, ParamStore& params);
  static int gen_special_param_info(SqlInfo& sql_info, ObPlanCacheCtx& pc_ctx);
  static int insert_neg_sign(common::ObIAllocator& alloc_buf, ParseNode* node);
  static bool is_tree_not_param(const ParseNode* tree);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlParameterization);
  static int is_fast_parse_const(TransformTreeCtx& ctx);

  static bool is_node_not_param(TransformTreeCtx& ctx);
  static int transform_tree(TransformTreeCtx& ctx, const ObSQLSessionInfo& session_info);
  static int add_param_flag(const ParseNode* node, SqlInfo& sql_info);
  static int add_not_param_flag(const ParseNode* node, SqlInfo& sql_info);
  static int add_varchar_charset(const ParseNode* node, SqlInfo& sql_info);
  static int mark_args(ParseNode* arg_tree, const bool* mark_arr, int64_t arg_num);
  static int mark_tree(ParseNode* tree);
  static int get_related_user_vars(const ParseNode* tree, common::ObIArray<common::ObString>& user_vars);

  static int get_select_item_param_info(const common::ObIArray<ObPCParam*>& raw_params, ParseNode* tree,
      SelectItemParamInfoArray* select_item_param_infos);
  static int parameterize_fields(SelectItemTraverseCtx& ctx);

  static int resolve_paramed_const(SelectItemTraverseCtx& ctx);
  static int transform_minus_op(ObIAllocator&, ParseNode*);

  static int find_leftest_const_node(ParseNode& cur_node, ParseNode*& const_node);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_SQL_PARAMETERIZATION_H */
