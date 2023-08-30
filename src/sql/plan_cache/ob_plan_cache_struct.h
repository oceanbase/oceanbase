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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_STRUCT_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_STRUCT_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/utility/serialization.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/ob_sql_utils.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace sql
{

typedef common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> TmpTableNameArray;

enum PlanCacheMode
{
  PC_INVALID_MODE = -1,
  PC_TEXT_MODE = 0,
  PC_PS_MODE = 1,
  PC_PL_MODE = 2,
  PC_MAX_MODE = 3
};

struct ObPlanCacheKey : public ObILibCacheKey
{
  ObPlanCacheKey()
      : key_id_(common::OB_INVALID_ID),
        db_id_(common::OB_INVALID_ID),
        sessid_(0),
        mode_(PC_TEXT_MODE),
        is_weak_read_(false) {}

  inline void reset()
  {
    name_.reset();
    key_id_ = common::OB_INVALID_ID;
    db_id_ = common::OB_INVALID_ID;
    sessid_ = 0;
    mode_ = PC_TEXT_MODE;
    sys_vars_str_.reset();
    config_str_.reset();
    is_weak_read_ = false;
    namespace_ = NS_INVALID;
  }

  virtual inline int deep_copy(common::ObIAllocator &allocator,
                               const ObILibCacheKey &other)
  {
    int ret = common::OB_SUCCESS;
    const ObPlanCacheKey &pc_key = static_cast<const ObPlanCacheKey&>(other);
    if (OB_FAIL(common::ob_write_string(allocator, pc_key.name_, name_))) {
      SQL_PC_LOG(WARN, "write string failed", K(ret), K(pc_key.name_));
    } else if (OB_FAIL(common::ob_write_string(allocator, pc_key.sys_vars_str_,
                                               sys_vars_str_))) {
      SQL_PC_LOG(WARN, "write sys vars str failed", K(ret),
                 K(pc_key.sys_vars_str_));
    } else if (OB_FAIL(common::ob_write_string(allocator, pc_key.config_str_,
                                                config_str_))) {
      SQL_PC_LOG(WARN, "write config str failed", K(ret),
                K(pc_key.config_str_));
    } else {
      db_id_ = pc_key.db_id_;
      key_id_ = pc_key.key_id_;
      sessid_ = pc_key.sessid_;
      mode_ = pc_key.mode_;
      namespace_ = pc_key.namespace_;
      is_weak_read_ = pc_key.is_weak_read_;
    }
    return ret;
  }

  inline void destory(common::ObIAllocator &allocator)
  {
    if (NULL != name_.ptr()) {
      allocator.free(name_.ptr());
    }
    if (NULL != sys_vars_str_.ptr()) {
      allocator.free(sys_vars_str_.ptr());
    }
    if (NULL != config_str_.ptr()) {
      allocator.free(config_str_.ptr());
    }
  }
  virtual inline uint64_t hash() const
  {
    uint64_t hash_ret = name_.hash();
    hash_ret = common::murmurhash(&key_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&db_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&sessid_, sizeof(uint32_t), hash_ret);
    hash_ret = common::murmurhash(&mode_, sizeof(PlanCacheMode), hash_ret);
    hash_ret = sys_vars_str_.hash(hash_ret);
    hash_ret = config_str_.hash(hash_ret);
    hash_ret = common::murmurhash(&namespace_, sizeof(ObLibCacheNameSpace), hash_ret);

    return hash_ret;
  }

  virtual inline bool is_equal(const ObILibCacheKey &other) const
  {
    const ObPlanCacheKey &pc_key = static_cast<const ObPlanCacheKey&>(other);
    bool cmp_ret = name_ == pc_key.name_ &&
                   db_id_ == pc_key.db_id_ &&
                   key_id_ == pc_key.key_id_ &&
                   sessid_ == pc_key.sessid_ &&
                   mode_ == pc_key.mode_ &&
                   sys_vars_str_ == pc_key.sys_vars_str_ &&
                   config_str_ == pc_key.config_str_ &&
                   is_weak_read_ == pc_key.is_weak_read_ &&
                   namespace_ == pc_key.namespace_;

    return cmp_ret;
  }
  TO_STRING_KV(K_(name),
               K_(key_id),
               K_(db_id),
               K_(sessid),
               K_(mode),
               K_(sys_vars_str),
               K_(config_str),
               K_(is_weak_read),
               K_(namespace));
  //通过name来进行查找，一般是shared sql/procedure
  //cursor用这种方式，对应的namespace是CRSR
  common::ObString name_;
  //通过schema id来进行查找，这种方式一般是直接的schema obj需要缓存到plan
  //cache中，例如store procedure, package, function
  uint64_t key_id_; //在ps中key_id_的含有为statement id
  uint64_t db_id_;
  uint32_t sessid_;
  PlanCacheMode mode_;
  common::ObString sys_vars_str_;
  common::ObString config_str_;
  bool is_weak_read_;
};

//记录快速化参数后不需要扣参数的原始字符串及相关信息
struct NotParamInfo
{
  int64_t idx_;
  common::ObString raw_text_;

  NotParamInfo()
    :idx_(common::OB_INVALID_ID)
  {}

  void reset()
  {
    idx_ = common::OB_INVALID_ID;
    raw_text_.reset();
  }

  TO_STRING_KV(K_(idx), K_(raw_text));
};

typedef common::ObSEArray<NotParamInfo, 4> NotParamInfoList;

// Template SQL Constant Constraints
typedef common::ObSEArray<NotParamInfoList, 4> TplSqlConstCons;

struct PsNotParamInfo
{
  int64_t idx_;
  common::ObObjParam ps_param_;
  TO_STRING_KV(K_(idx), K_(ps_param));
};

struct ObValuesTokenPos {
	ObValuesTokenPos() : no_param_sql_pos_(0), param_idx_(0) {}
	ObValuesTokenPos(const int64_t no_param_pos, const int64_t param_idx)
		: no_param_sql_pos_(no_param_pos), param_idx_(param_idx) {}
  int64_t no_param_sql_pos_;
  int64_t param_idx_;
  TO_STRING_KV(K(no_param_sql_pos_), K(param_idx_));
};

typedef common::ObFixedArray<ObPCParam *, common::ObIAllocator> ObArrayPCParam;

struct ObFastParserResult
{
private:
  common::ModulePageAllocator inner_alloc_;

public:
  ObFastParserResult()
    : inner_alloc_("FastParserRes"),
      raw_params_(&inner_alloc_),
      parameterized_params_(&inner_alloc_),
      cache_params_(NULL),
      values_token_pos_(0),
      values_tokens_(&inner_alloc_)
  {
    reset_question_mark_ctx();
  }
  ObPlanCacheKey pc_key_; //plan cache key, parameterized by fast parser
  common::ObFixedArray<ObPCParam *, common::ObIAllocator> raw_params_;
  common::ObFixedArray<const common::ObObjParam *, common::ObIAllocator> parameterized_params_;
  ParamStore *cache_params_;
  ObQuestionMarkCtx question_mark_ctx_;
  int64_t values_token_pos_; // for insert values
  common::ObFixedArray<ObValuesTokenPos, common::ObIAllocator> values_tokens_; // for values table
  common::ObSEArray<ObArrayPCParam *, 4, common::ModulePageAllocator, true> array_params_;

  void reset() {
    pc_key_.reset();
    raw_params_.reuse();
    parameterized_params_.reuse();
    cache_params_ = NULL;
    values_token_pos_ = 0;
    values_tokens_.reuse();
    array_params_.reuse();
  }
  void reset_question_mark_ctx()
  {
    question_mark_ctx_.name_ = NULL;
    question_mark_ctx_.count_ = 0;
    question_mark_ctx_.capacity_ = 0;
    question_mark_ctx_.by_ordinal_ = false;
    question_mark_ctx_.by_name_ = false;
    question_mark_ctx_.by_defined_name_ = false;
  }
  int assign(const ObFastParserResult &other)
  {
    int ret = OB_SUCCESS;
    pc_key_ = other.pc_key_;
    raw_params_.set_allocator(&inner_alloc_);
    parameterized_params_.set_allocator(&inner_alloc_);
    cache_params_ = other.cache_params_;
    question_mark_ctx_ = other.question_mark_ctx_;
    values_tokens_.set_allocator(&inner_alloc_);
    if (OB_FAIL(raw_params_.assign(other.raw_params_))) {
      SQL_PC_LOG(WARN, "failed to assign fix array", K(ret));
    } else if (OB_FAIL(parameterized_params_.assign(other.parameterized_params_))) {
      SQL_PC_LOG(WARN, "failed to assign fix array", K(ret));
    } else if (OB_FAIL(values_tokens_.assign(other.values_tokens_))) {
      SQL_PC_LOG(WARN, "failed to assign fix array", K(ret));
    } else if (OB_FAIL(array_params_.assign(other.array_params_))) {
      SQL_PC_LOG(WARN, "failed to assign array", K(ret));
    }
    return ret;
  }
  TO_STRING_KV(K(pc_key_), K(raw_params_), K(parameterized_params_), K(cache_params_), K(values_token_pos_),
               K(values_tokens_), K(array_params_));
};

enum WayToGenPlan {
  WAY_DEPENDENCE_ENVIRONMENT,
  WAY_ACS,
  WAY_PLAN_BASELINE,
  WAY_OPTIMIZER,
};

struct SelectItemParamInfo
{
  // 比最大长度的column名多一倍的buffer
  static const int64_t PARAMED_FIELD_BUF_LEN = MAX_COLUMN_CHAR_LENGTH;
  // 对于 select -1 + a + 1 + b + 2 from dual，参数化后的sql为select ? + a + ? b + ? from dual
  // questions_pos_记录每一个?相对于column表达式的偏移，即[0, 4, 9]
  // params_idx_记录每一个?在raw_params中的下标，即[0, 1, 2]
  // neg_params_idx_记录哪一个常量是负号，即[0]
  // paramed_field_name_记录参数化后的column模板，即'? + a + ? + b + ?'
  // esc_str_flag_标记z这一个column是不是字符串常量，比如 select 'abc' from dual，'abc'对应的标记为true
  common::ObSEArray<int64_t, 16> questions_pos_;
  common::ObSEArray<int64_t, 16> params_idx_;
  common::ObBitSet<> neg_params_idx_;
  char paramed_field_name_[PARAMED_FIELD_BUF_LEN];
  int64_t name_len_;
  bool esc_str_flag_;

  SelectItemParamInfo()
    : questions_pos_(),
      params_idx_(),
      neg_params_idx_(),
      name_len_(0),
      esc_str_flag_(false) {}

  void reset()
  {
    questions_pos_.reset();
    params_idx_.reset();
    neg_params_idx_.reset();
    esc_str_flag_ = false;
    name_len_ = 0;
  }

  TO_STRING_KV(K_(questions_pos),
               K_(params_idx),
               K_(neg_params_idx),
               K_(name_len),
               K_(esc_str_flag),
               K(common::ObString(name_len_, paramed_field_name_)));
};

typedef common::ObFixedArray<SelectItemParamInfo, common::ObIAllocator> SelectItemParamInfoArray;

typedef common::ObFixedArray<ObPCParam *, common::ObIAllocator> ObRawParams;
typedef common::ObFixedArray<ObRawParams *, common::ObIAllocator> ObRawParams2DArray;


struct ObInsertBatchOptInfo
{

  ObInsertBatchOptInfo(common::ObIAllocator &allocator)
    : insert_params_count_(0),
      update_params_count_(0),
      sql_delta_length_(0),
      multi_raw_params_(allocator),
      new_reconstruct_sql_()
  {}

  TO_STRING_KV(K_(insert_params_count), K_(update_params_count),
      K_(sql_delta_length), K_(new_reconstruct_sql));

  int64_t insert_params_count_;
  int64_t update_params_count_;
  int64_t sql_delta_length_;
  ObRawParams2DArray multi_raw_params_;
  ObString new_reconstruct_sql_;
};

struct ObPlanCacheCtx : public ObILibCacheCtx
{
  ObPlanCacheCtx(const common::ObString &sql,
                 const PlanCacheMode mode,
                 common::ObIAllocator &allocator,
                 ObSqlCtx &sql_ctx,
                 ObExecContext &exec_ctx,
                 uint64_t tenant_id)
    : mode_(mode),
      raw_sql_(sql),
      allocator_(allocator),
      sql_ctx_(sql_ctx),
      exec_ctx_(exec_ctx),
      fp_result_(),
      not_param_info_(allocator),
      not_param_var_(allocator),
      param_charset_type_(allocator),
      normal_parse_const_cnt_(0),
      need_evolution_(false),
      select_item_param_infos_(allocator),
      should_add_plan_(true),
      begin_commit_stmt_(false),
      must_be_positive_index_(),
      multi_stmt_fp_results_(allocator),
      handle_id_(MAX_HANDLE),
      is_remote_executor_(false),
      is_parameterized_execute_(false),
      ps_need_parameterized_(false),
      fixed_param_idx_(allocator),
      need_add_obj_stat_(true),
      is_inner_sql_(false),
      is_original_ps_mode_(false),
      ab_params_(NULL),
      new_raw_sql_(),
      is_rewrite_sql_(false),
      rule_name_(),
      def_name_ctx_(NULL),
      fixed_param_info_list_(allocator),
      dynamic_param_info_list_(allocator),
      tpl_sql_const_cons_(allocator),
      need_retry_add_plan_(true),
      insert_batch_opt_info_(allocator)
  {
    fp_result_.pc_key_.mode_ = mode_;
  }

  int get_not_param_info_str(common::ObIAllocator &allocator, common::ObString &str)
  {
    int ret = common::OB_SUCCESS;
    if (not_param_info_.count() > 0) {
      int64_t size = 0;
      int64_t not_param_num = not_param_info_.count();
      for (int64_t i = 0; i < not_param_num; i++) {
        size += not_param_info_.at(i).raw_text_.length() + 2;
      }
      char *buf = (char *)allocator.alloc(size);
      if (OB_ISNULL(buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_PC_LOG(WARN, "fail to alloc memory for special param info", K(ret));
      } else {
        int64_t pos = 0;
        for (int64_t i = 0; i < not_param_num; i++) {
          pos += not_param_info_.at(i).raw_text_.to_string(buf + pos, size - pos);
          if (i != not_param_num - 1) {
            pos += snprintf(buf + pos, size - pos, ",");
          }
        }
        str = common::ObString::make_string(buf);
      }
    } else {
      /*do nothing*/
    }

    return ret;
  }

  int is_retry(bool &v) const;  //是否在重试之中
  int is_retry_for_dup_tbl(bool &v) const; //仅复制表原因的重试才会设置为true
  void set_begin_commit_stmt() { begin_commit_stmt_ = true; }
  bool is_begin_commit_stmt() const { return begin_commit_stmt_; }
  void set_is_parameterized_execute() { is_parameterized_execute_ = true; }
  bool is_parameterized_execute() { return is_parameterized_execute_; }
  void set_is_inner_sql(bool v) { is_inner_sql_ = v; };
  bool is_inner_sql() const { return is_inner_sql_; } 
  void set_is_rewrite_sql(bool v) { is_rewrite_sql_ = v; }
  bool is_rewrite_sql() const { return is_rewrite_sql_; }
  void set_need_retry_add_plan(bool v) { need_retry_add_plan_ = v; }
  bool need_retry_add_plan() const { return need_retry_add_plan_; }
  void set_pc_key_mode() { fp_result_.pc_key_.mode_ = mode_; }
  TO_STRING_KV(
    K(mode_),
    K(raw_sql_),
    K(not_param_info_),
    K(not_param_var_),
    K(not_param_index_),
    K(neg_param_index_),
    K(param_charset_type_),
    K(should_add_plan_),
    K(begin_commit_stmt_),
    K(is_remote_executor_),
    K(is_parameterized_execute_),
    K(ps_need_parameterized_),
    K(fixed_param_idx_),
    K(need_add_obj_stat_),
    K(is_inner_sql_),
    K(is_rewrite_sql_),
    K(rule_name_),
    K(def_name_ctx_),
    K(fixed_param_info_list_),
    K(dynamic_param_info_list_),
    K(tpl_sql_const_cons_),
    K(is_original_ps_mode_),
    K(new_raw_sql_),
    K(need_retry_add_plan_),
    K(insert_batch_opt_info_)
    );
  PlanCacheMode mode_; //control use which variables to do match

  const common::ObString &raw_sql_; //query sql
  common::ObIAllocator &allocator_; //result mem_pool
  ObSqlCtx &sql_ctx_;
  ObExecContext &exec_ctx_;
  ObFastParserResult fp_result_; //result after fast parser
  common::ObFixedArray<NotParamInfo, common::ObIAllocator> not_param_info_; //used for match pcv in pcv_set, gen when add plan
  common::ObFixedArray<PsNotParamInfo, common::ObIAllocator> not_param_var_; //used for ps mode not param
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> not_param_index_;
  //记录负数信息，在get plan时将fast parser的负数添加负号，现在是为了兼容outline中signature的生成,
  //以前为解决负数问题引入正常parser时识别负数，然后将fast parser中?sql负号去掉，并将参数化的参数原
  //始串前加'-', 从而生成的plan cache key中无负号,现在plan cache对负号的处理没有依赖这个方案，
  //但outline的signature生成依赖了plan cache key, 导致对负号的处理还需保留。
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> neg_param_index_;
  common::ObFixedArray<common::ObCharsetType, common::ObIAllocator> param_charset_type_;
  // 用于存储临时表计划所包含的临时表名
  TmpTableNameArray tmp_table_names_;
  ObSqlTraits sql_traits_;
  int64_t normal_parse_const_cnt_;

  // *****  for spm ****
  bool need_evolution_;
  //******  for spm end *****

  // select item参数化信息
  SelectItemParamInfoArray select_item_param_infos_;

  // 根据get plan时的一些信息(get plan失败)，判断新生成的计划是否加入plan cache
  bool should_add_plan_;
  // 记录是否为begin/commit的语句，用于优化部分路径的调用
  bool begin_commit_stmt_;

  // record which const param must be positive
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE,
                   common::ModulePageAllocator, true> must_be_positive_index_;
  // used for store fp results for multi_stmt optimization
  common::ObFixedArray<ObFastParserResult, common::ObIAllocator> multi_stmt_fp_results_;
  CacheRefHandleID handle_id_;
  bool is_remote_executor_;
  bool is_parameterized_execute_;
  bool ps_need_parameterized_;
  common::ObFixedArray<int64_t, common::ObIAllocator> fixed_param_idx_;
  bool need_add_obj_stat_;
  bool is_inner_sql_;
  bool is_original_ps_mode_;
  ParamStore *ab_params_;  // arraybinding batch parameters,
  ObString new_raw_sql_;  // values clause rebuild raw sql

  // **********  for rewrite rule **********
  bool is_rewrite_sql_;
  common::ObString rule_name_;
  QuestionMarkDefNameCtx *def_name_ctx_;
  common::ObFixedArray<FixedParamValue, common::ObIAllocator> fixed_param_info_list_;
  common::ObFixedArray<DynamicParamInfo, common::ObIAllocator> dynamic_param_info_list_;
  common::ObFixedArray<NotParamInfoList, common::ObIAllocator> tpl_sql_const_cons_;
  // **********  for rewrite end **********
  // when schema version of cache node is old, whether remove this node and retry add cache obj.
  bool need_retry_add_plan_;
  ObInsertBatchOptInfo insert_batch_opt_info_;
};

struct ObPlanCacheStat
{
  uint64_t access_count_;
  uint64_t hit_count_;

  ObPlanCacheStat()
    : access_count_(0),
      hit_count_(0)
  {}

  TO_STRING_KV("access_count", access_count_,
               "hit_count", hit_count_);
};

}
}
#endif //OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_STRUCT_
