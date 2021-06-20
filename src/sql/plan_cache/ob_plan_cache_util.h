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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_UTIL_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_UTIL_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "common/object/ob_object.h"
#include "sql/plan_cache/ob_param_info.h"
#include "sql/ob_sql_define.h"
#include "sql/ob_sql_utils.h"
#include "sql/parser/parse_node.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
namespace common {
class ObString;
class ObObj;
}  // namespace common

namespace storage {
class ObPartitionService;
}

namespace sql {
class ObPCVSet;
class ObPhysicalPlan;
class ObSqlCtx;
class ObTableLocation;
class ObPhyTableLocation;
class ObPhyTableLocationInfo;
class ObTablePartitionInfo;
class ObPlanCacheValue;
class ObCacheObject;

typedef uint64_t ObCacheObjID;
typedef common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> TmpTableNameArray;
enum ObjNameSpace {
  NS_INVALID = 0,
  NS_CRSR = 1,  // shared cursor:sql text or anonymous block or call store procedure
  NS_PRCD = 2,  // store procedure, function
  NS_ANON = 3,  // anonymous
  NS_TRGR = 4,  // trigger
  NS_PKG = 5,   // package
  NS_MAX
};

struct ObPlanCacheKey {
  ObPlanCacheKey()
      : key_id_(common::OB_INVALID_ID),
        db_id_(common::OB_INVALID_ID),
        sessid_(0),
        is_ps_mode_(false),
        namespace_(NS_INVALID)
  {}
  ObPlanCacheKey(const ObString& name, uint64_t key_id, uint64_t db_id, uint64_t sessid, bool is_ps_mode,
      const ObString& sys_vars_str, ObjNameSpace namespace_arg)
      : name_(name),
        key_id_(key_id),
        db_id_(db_id),
        sessid_(sessid),
        is_ps_mode_(is_ps_mode),
        sys_vars_str_(sys_vars_str),
        namespace_(namespace_arg)
  {}

  inline void reset()
  {
    name_.reset();
    key_id_ = common::OB_INVALID_ID;
    db_id_ = common::OB_INVALID_ID;
    sessid_ = 0;
    is_ps_mode_ = false;
    sys_vars_str_.reset();
    namespace_ = NS_INVALID;
  }

  inline int deep_copy(common::ObIAllocator& allocator, const ObPlanCacheKey& other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(common::ob_write_string(allocator, other.name_, name_))) {
      SQL_PC_LOG(WARN, "write string failed", K(ret), K(other.name_));
    } else if (OB_FAIL(common::ob_write_string(allocator, other.sys_vars_str_, sys_vars_str_))) {
      SQL_PC_LOG(WARN, "write sys vars str failed", K(ret), K(other.sys_vars_str_));
    } else {
      db_id_ = other.db_id_;
      key_id_ = other.key_id_;
      sessid_ = other.sessid_;
      is_ps_mode_ = other.is_ps_mode_;
      namespace_ = other.namespace_;
    }
    return ret;
  }

  inline void destory(common::ObIAllocator& allocator)
  {
    if (NULL != name_.ptr()) {
      allocator.free(name_.ptr());
    }
    if (NULL != sys_vars_str_.ptr()) {
      allocator.free(sys_vars_str_.ptr());
    }
  }

  inline uint64_t hash() const
  {
    uint64_t hash_ret = name_.hash(0);
    hash_ret = common::murmurhash(&key_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&db_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&sessid_, sizeof(uint32_t), hash_ret);
    hash_ret = common::murmurhash(&is_ps_mode_, sizeof(bool), hash_ret);
    hash_ret = sys_vars_str_.hash(hash_ret);
    hash_ret = common::murmurhash(&namespace_, sizeof(ObjNameSpace), hash_ret);

    return hash_ret;
  }

  inline bool operator==(const ObPlanCacheKey& other) const
  {
    bool cmp_ret = name_ == other.name_ && db_id_ == other.db_id_ && key_id_ == other.key_id_ &&
                   sessid_ == other.sessid_ && is_ps_mode_ == other.is_ps_mode_ &&
                   sys_vars_str_ == other.sys_vars_str_ && namespace_ == other.namespace_;

    return cmp_ret;
  }
  TO_STRING_KV(K_(name), K_(key_id), K_(db_id), K_(sessid), K_(is_ps_mode), K_(sys_vars_str), K_(namespace));
  common::ObString name_;
  uint64_t key_id_;
  uint64_t db_id_;
  uint32_t sessid_;
  bool is_ps_mode_;
  common::ObString sys_vars_str_;
  ObjNameSpace namespace_;
};

#define SERIALIZE_VERSION(arr, allocator, str)                                            \
  do {                                                                                    \
    if (OB_SUCC(ret)) {                                                                   \
      int64_t size = 0;                                                                   \
      size = serialization::encoded_length_i64(size) * arr.count();                       \
      if (0 != size) {                                                                    \
        char* buf = (char*)allocator.alloc(size);                                         \
        if (NULL == buf) {                                                                \
          ret = OB_ALLOCATE_MEMORY_FAILED;                                                \
          LOG_WARN("alloc memory failed", K(ret), K(size));                               \
        }                                                                                 \
        if (OB_SUCC(ret)) {                                                               \
          int64_t pos = 0;                                                                \
          for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); i++) {                     \
            if (OB_FAIL(serialization::encode_i64(buf, size, pos, arr.at(i).version_))) { \
              LOG_WARN("encode failed", K(size), K(pos), K(ret));                         \
            }                                                                             \
          }                                                                               \
        }                                                                                 \
        (void)str.assign(buf, static_cast<int32_t>(size));                                \
      }                                                                                   \
    }                                                                                     \
  } while (0)

struct ObPCKeyValue {
  ObPCKeyValue() : pcv_set_(NULL)
  {}
  ObPCKeyValue(ObPlanCacheKey& key, ObPCVSet* pcv_set) : key_(key), pcv_set_(pcv_set)
  {}

  TO_STRING_KV(K_(key), KP(pcv_set_));

  ObPlanCacheKey key_;
  ObPCVSet* pcv_set_;
};

typedef ObPCKeyValue PCKeyValue;
typedef common::ObSEArray<PCKeyValue, 16> PCKeyValueArray;

struct ObSysVarInPC {
  common::ObSEArray<common::ObObj, 32> system_variables_;
  char* buf_;
  int64_t buf_size_;

  ObSysVarInPC() : buf_(NULL), buf_size_(0)
  {}

  int push_back(const common::ObObj& value)
  {
    return system_variables_.push_back(value);
  }

  bool empty()
  {
    return 0 == system_variables_.count();
  }

  int deep_copy_to_self()
  {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    common::ObObj obj;
    if (NULL == buf_ || buf_size_ <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K_(buf), K_(buf_size));
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < system_variables_.count(); i++) {
      if OB_FAIL (obj.deep_copy(system_variables_.at(i), buf_, buf_size_, pos)) {
        SQL_PC_LOG(WARN, "fail to deep copy obj", K(buf_size_), K(pos), K(ret));
      } else {
        system_variables_.at(i) = obj;
      }
    }
    return ret;
  }

  int deep_copy(const ObSysVarInPC& other)
  {
    int ret = common::OB_SUCCESS;
    common::ObObj obj;
    int64_t pos = 0;
    if (NULL == buf_ || buf_size_ <= 0 || system_variables_.count() != 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K_(buf), K_(buf_size), "system variables count", system_variables_.count());
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < other.system_variables_.count(); i++) {
      if (OB_FAIL(obj.deep_copy(other.system_variables_.at(i), buf_, buf_size_, pos))) {
        SQL_PC_LOG(WARN, "fail to deep copy obj", K(buf_size_), K(pos), K(ret));
      } else if (OB_FAIL(system_variables_.push_back(obj))) {
        SQL_PC_LOG(WARN, "fail to push sys value", K(ret));
      }
    }
    return ret;
  }

  bool operator==(const ObSysVarInPC& other) const
  {
    bool ret = true;
    if (system_variables_.count() == other.system_variables_.count()) {
      for (int64_t i = 0; i < system_variables_.count(); ++i) {
        if (system_variables_.at(i) == other.system_variables_.at(i)) {
          continue;
        } else {
          ret = false;
          break;
        }
      }
    } else {
      ret = false;
    }
    return ret;
  }

  int64_t hash(int64_t seed) const
  {
    int64_t hash_val = seed;
    for (int64_t i = 0; i < system_variables_.count(); ++i) {
      hash_val = system_variables_.at(i).hash(hash_val);
    }
    return hash_val;
  }

  void reset()
  {
    system_variables_.reset();
    buf_ = NULL;
    buf_size_ = 0;
  }

  int serialize_sys_vars(char* buf, int64_t buf_len, int64_t& pos)
  {
    int ret = common::OB_SUCCESS;
    pos = 0;
    int64_t size = 0;
    int64_t sys_var_cnt = system_variables_.count();
    for (int32_t i = 0; OB_SUCC(ret) && i < sys_var_cnt; ++i) {
      size = 0;
      if (OB_FAIL(system_variables_.at(i).print_plain_str_literal(buf + pos, buf_len - pos, size))) {
        SQL_PC_LOG(
            WARN, "fail to encode obj", K(i), K(buf + pos), K(buf_len), K(pos), K(system_variables_.at(i)), K(ret));
      } else {
        pos += size;
        if (i != sys_var_cnt - 1) {
          if (buf_len - pos <= 0) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_PC_LOG(WARN, "fail to databuf print", K(buf), K(pos));
          } else {
            char delimiter = ',';
            memcpy(buf + pos, &delimiter, sizeof(delimiter));
            pos += sizeof(char);
          }
          /*
          if (0 > (size = snprintf(buf + pos, buf_len - pos, ","))) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_PC_LOG(WARN, "fail to databuf print", K(buf), K(pos), K(size));
          } else {
            pos += size;
          }*/
        }
      }
    }

    return ret;
  }

  TO_STRING_KV(K_(system_variables));
};

struct ObPCMemPctConf {
  int64_t limit_pct_;
  int64_t high_pct_;
  int64_t low_pct_;

  ObPCMemPctConf()
      : limit_pct_(common::OB_PLAN_CACHE_PERCENTAGE),
        high_pct_(common::OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE),
        low_pct_(common::OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE)
  {}
};

enum ParamProperty {
  INVALID_PARAM,
  NORMAL_PARAM,
  NOT_PARAM,
  NEG_PARAM,
  TRANS_NEG_PARAM,
};

enum AsynUpdateBaselineStat { ASYN_NOTHING = 0, ASYN_REPLACE, ASYN_INSERT };

struct NotParamInfo {
  int64_t idx_;
  common::ObString raw_text_;

  NotParamInfo() : idx_(common::OB_INVALID_ID)
  {}

  void reset()
  {
    idx_ = common::OB_INVALID_ID;
    raw_text_.reset();
  }

  TO_STRING_KV(K_(idx), K_(raw_text));
};

struct PsNotParamInfo {
  int64_t idx_;
  common::ObObjParam ps_param_;
  TO_STRING_KV(K_(idx), K_(ps_param));
};

struct ObPCParam {
  ParseNode* node_;
  ParamProperty flag_;

  ObPCParam() : node_(NULL), flag_(INVALID_PARAM)
  {}
  TO_STRING_KV(KP_(node), K_(flag));
};

struct ObPCConstParamInfo {
  common::ObSEArray<int64_t, 4> const_idx_;
  common::ObSEArray<common::ObObj, 4> const_params_;
  TO_STRING_KV(K_(const_idx), K_(const_params));
};

struct ObPCParamEqualInfo {
  int64_t first_param_idx_;
  int64_t second_param_idx_;
  TO_STRING_KV(K_(first_param_idx), K_(second_param_idx));
};

struct ObFastParserResult {
  ObFastParserResult()
      : inner_alloc_("FastParserRes"), raw_params_(&inner_alloc_), ps_params_(&inner_alloc_), cache_params_(NULL)
  {}
  ObPlanCacheKey pc_key_;  // plan cache key, parameterized by fast parser
  common::ModulePageAllocator inner_alloc_;
  common::ObFixedArray<ObPCParam*, common::ObIAllocator> raw_params_;
  common::ObFixedArray<const common::ObObjParam*, common::ObIAllocator> ps_params_;
  ParamStore* cache_params_;
  void reset()
  {
    pc_key_.reset();
    raw_params_.reuse();
    ps_params_.reuse();
    cache_params_ = NULL;
  }
  TO_STRING_KV(K(pc_key_), K(raw_params_), K(ps_params_), K(cache_params_));
};

enum WayToGenPlan {
  WAY_DEPENDENCE_ENVIRONMENT,
  WAY_ACS,
  WAY_PLAN_BASELINE,
  WAY_OPTIMIZER,
};

struct SelectItemParamInfo {
  static const int64_t PARAMED_FIELD_BUF_LEN = MAX_COLUMN_CHAR_LENGTH;
  // for select -1 + a + 1 + b + 2 from dual, the parameterized sql is select? + a +? B +? From dual
  // questions_pos_ record the offset of each? relative to the column expression, ie [0, 4, 9]
  // params_idx_ record each? The subscript in raw_params, ie [0, 1, 2]
  // neg_params_idx_ record which constant is the negative sign, ie [0]
  // paramed_field_name_ record the parameterized column template, that is,'? + a +? + b + ?'
  // esc_str_flag_ marks z whether this column is a string constant, such as select'abc' from dual,
  // the corresponding mark of'abc' is true
  common::ObSEArray<int64_t, 16> questions_pos_;
  common::ObSEArray<int64_t, 16> params_idx_;
  common::ObBitSet<> neg_params_idx_;
  char paramed_field_name_[PARAMED_FIELD_BUF_LEN];
  int64_t name_len_;
  bool esc_str_flag_;

  SelectItemParamInfo() : questions_pos_(), params_idx_(), neg_params_idx_(), name_len_(0), esc_str_flag_(false)
  {}

  void reset()
  {
    questions_pos_.reset();
    params_idx_.reset();
    neg_params_idx_.reset();
    esc_str_flag_ = false;
    name_len_ = 0;
  }

  TO_STRING_KV(K_(questions_pos), K_(params_idx), K_(neg_params_idx), K_(name_len), K_(esc_str_flag),
      K(common::ObString(name_len_, paramed_field_name_)));
};

typedef common::ObFixedArray<SelectItemParamInfo, common::ObIAllocator> SelectItemParamInfoArray;

struct ObPlanCacheCtx {
  ObPlanCacheCtx(const common::ObString& sql, const bool is_ps_mode, common::ObIAllocator& allocator, ObSqlCtx& sql_ctx,
      ObExecContext& exec_ctx, uint64_t tenant_id)
      : is_ps_mode_(is_ps_mode),
        raw_sql_(sql),
        allocator_(allocator),
        sql_ctx_(sql_ctx),
        exec_ctx_(exec_ctx),
        fp_result_(),
        not_param_info_(allocator),
        not_param_var_(allocator),
        param_charset_type_(allocator),
        normal_parse_const_cnt_(0),
        plan_baseline_id_(common::OB_INVALID_ID),
        need_real_add_(true),
        add_pre_acs_(false),
        gen_plan_way_(WAY_DEPENDENCE_ENVIRONMENT),
        need_evolution_(false),
        is_baseline_fixed_(false),
        is_baseline_enabled_(true),
        select_item_param_infos_(allocator),
        outlined_sql_len_(sql.length()),
        should_add_plan_(true),
        must_be_positive_index_(),
        multi_stmt_fp_results_(allocator),
        handle_id_(MAX_HANDLE)
  {
    bl_key_.tenant_id_ = tenant_id;
    fp_result_.pc_key_.is_ps_mode_ = is_ps_mode_;
  }

  int get_not_param_info_str(common::ObIAllocator& allocator, common::ObString& str)
  {
    int ret = common::OB_SUCCESS;
    if (not_param_info_.count() > 0) {
      int64_t size = 0;
      int64_t not_param_num = not_param_info_.count();
      for (int64_t i = 0; i < not_param_num; i++) {
        size += not_param_info_.at(i).raw_text_.length() + 2;
      }
      char* buf = (char*)allocator.alloc(size);
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

  int is_retry(bool& v) const;
  int is_retry_for_dup_tbl(bool& v) const;
  TO_STRING_KV(K(is_ps_mode_), K(raw_sql_), K(need_real_add_), K(add_pre_acs_), K(not_param_info_), K(not_param_var_),
      K(not_param_index_), K(neg_param_index_), K(param_charset_type_), K(outlined_sql_len_), K(should_add_plan_));
  bool is_ps_mode_;  // control use which variables to do match

  const common::ObString& raw_sql_;  // query sql
  common::ObIAllocator& allocator_;  // result mem_pool
  ObSqlCtx& sql_ctx_;
  ObExecContext& exec_ctx_;
  ObFastParserResult fp_result_;                                              // result after fast parser
  common::ObFixedArray<NotParamInfo, common::ObIAllocator> not_param_info_;   // used for match pcv in pcv_set, gen when
                                                                              // add plan
  common::ObFixedArray<PsNotParamInfo, common::ObIAllocator> not_param_var_;  // used for ps mode not param
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> not_param_index_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> neg_param_index_;
  common::ObFixedArray<common::ObCharsetType, common::ObIAllocator> param_charset_type_;
  TmpTableNameArray tmp_table_names_;
  ObSqlTraits sql_traits_;
  int64_t normal_parse_const_cnt_;

  // *****  for spm ****

  uint64_t plan_baseline_id_;
  bool need_real_add_;
  bool add_pre_acs_;
  WayToGenPlan gen_plan_way_;
  bool need_evolution_;
  bool is_baseline_fixed_;
  bool is_baseline_enabled_;  // not used for now
  share::schema::BaselineKey bl_key_;
  //******  for spm end *****

  // record idxes for params which should be changed from T_VARCHAR to T_CHAR
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> change_char_index_;
  SelectItemParamInfoArray select_item_param_infos_;
  int64_t outlined_sql_len_;
  bool should_add_plan_;
  // record which const param must be positive
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> must_be_positive_index_;
  // used for store fp results for multi_stmt optimization
  common::ObFixedArray<ObFastParserResult, common::ObIAllocator> multi_stmt_fp_results_;
  CacheRefHandleID handle_id_;
};

struct ObPlanCacheStat {
  uint64_t access_count_;
  uint64_t hit_count_;

  ObPlanCacheStat() : access_count_(0), hit_count_(0)
  {}

  TO_STRING_KV("access_count", access_count_, "hit_count", hit_count_);
};

struct ObOperatorStat {
  int64_t plan_id_;
  int64_t operation_id_;  // operator_id
  int64_t execute_times_;

  int64_t input_rows_;
  int64_t rescan_times_;
  int64_t output_rows_;
  ObOperatorStat()
      : plan_id_(-1), operation_id_(-1), execute_times_(0), input_rows_(0), rescan_times_(0), output_rows_(0)
  {}
  ObOperatorStat(const ObOperatorStat& other)
      : plan_id_(other.plan_id_),
        operation_id_(other.operation_id_),
        execute_times_(other.execute_times_),
        input_rows_(other.input_rows_),
        rescan_times_(other.rescan_times_),
        output_rows_(other.output_rows_)
  {}
  void init()
  {
    input_rows_ = 0;
    rescan_times_ = 0;
    output_rows_ = 0;
  }
  TO_STRING_KV(K_(plan_id), K_(operation_id), K_(execute_times), K_(input_rows), K_(rescan_times), K_(output_rows));
};

struct ObAcsIdxSelRange {
  ObAcsIdxSelRange() : index_name_(), low_bound_sel_(0.0), high_bound_sel_(1.0)
  {}
  ObAcsIdxSelRange(const ObAcsIdxSelRange& rhs)
      : index_name_(rhs.index_name_), low_bound_sel_(rhs.low_bound_sel_), high_bound_sel_(rhs.high_bound_sel_)
  {}
  ObString index_name_;
  double low_bound_sel_;
  double high_bound_sel_;
  TO_STRING_KV(K_(index_name), K_(low_bound_sel), K_(high_bound_sel));
};

struct ObTableScanStat {
  ObTableScanStat()
      : query_range_row_count_(-1),
        indexback_row_count_(-1),
        output_row_count_(-1),
        bf_filter_cnt_(0),
        bf_access_cnt_(0),
        fuse_row_cache_hit_cnt_(0),
        fuse_row_cache_miss_cnt_(0),
        row_cache_hit_cnt_(0),
        row_cache_miss_cnt_(0),
        block_cache_hit_cnt_(0),
        block_cache_miss_cnt_(0)
  {}
  int64_t query_range_row_count_;
  int64_t indexback_row_count_;
  int64_t output_row_count_;
  int64_t bf_filter_cnt_;
  int64_t bf_access_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;
  void reset_cache_stat()
  {
    bf_filter_cnt_ = 0;
    bf_access_cnt_ = 0;
    fuse_row_cache_hit_cnt_ = 0;
    fuse_row_cache_miss_cnt_ = 0;
    row_cache_hit_cnt_ = 0;
    row_cache_miss_cnt_ = 0;
    block_cache_hit_cnt_ = 0;
    block_cache_miss_cnt_ = 0;
  }
  TO_STRING_KV(K_(query_range_row_count), K_(indexback_row_count), K_(output_row_count), K_(bf_filter_cnt),
      K_(bf_access_cnt), K_(row_cache_hit_cnt), K_(row_cache_miss_cnt), K_(fuse_row_cache_hit_cnt),
      K_(fuse_row_cache_miss_cnt));
  OB_UNIS_VERSION(1);
};

struct ObTableRowCount {
  int64_t op_id_;
  int64_t row_count_;

  ObTableRowCount() : op_id_(OB_INVALID_ID), row_count_(0)
  {}
  ObTableRowCount(int64_t op_id, int64_t row_count) : op_id_(op_id), row_count_(row_count)
  {}
  TO_STRING_KV(K_(op_id), K_(row_count));

  OB_UNIS_VERSION(1);
};

struct ObPlanStat {
  static const int32_t STMT_MAX_LEN = 4096;
  static const int32_t MAX_SCAN_STAT_SIZE = 100;
  static const int64_t CACHE_POLICY_UPDATE_INTERVAL = 60 * 1000 * 1000;  // 1 min
  // static const int64_t CACHE_POLICY_UPDATE_INTERVAL = 30L * 60 * 1000 * 1000; // 30 min
  static const int64_t CACHE_POLICY_UDPATE_THRESHOLD = (1L << 20) - 1;
  static const int64_t CACHE_ACCESS_THRESHOLD = 3000;
  static constexpr double ENABLE_BF_CACHE_THRESHOLD = 0.10;
  static constexpr double ENABLE_ROW_CACHE_THRESHOLD = 0.06;

  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t plan_id_;  // plan id
  // uint64_t hash_;                 // plan hash value
  int64_t gen_time_;            // plan generated time
  int64_t schema_version_;      // plan schema version when generated
  int64_t merged_version_;      // plan merged version when generated
  int64_t last_active_time_;    // plan last hit time
  uint64_t hit_count_;          // plan hit count
  uint64_t mem_used_;           // plan memory size
  uint64_t slow_count_;         // plan execution slow count
  int64_t slowest_exec_time_;   // plan execution slowest time
  uint64_t slowest_exec_usec_;  // plan execution slowest usec
  char stmt_[STMT_MAX_LEN];     //  sql stmt
  int32_t stmt_len_;            // stmt_len_
  ObPhysicalPlan* plan_;        // used in explain

  int64_t execute_times_;
  int64_t disk_reads_;
  int64_t direct_writes_;
  int64_t buffer_gets_;
  int64_t application_wait_time_;
  int64_t concurrency_wait_time_;
  int64_t user_io_wait_time_;
  int64_t rows_processed_;
  int64_t elapsed_time_;
  int64_t total_process_time_;
  int64_t cpu_time_;
  int64_t large_querys_;
  int64_t delayed_large_querys_;
  int64_t delayed_px_querys_;
  int64_t expected_worker_count_;
  int64_t outline_version_;
  int64_t outline_id_;
  bool is_last_open_succ_;
  common::ObString sp_info_str_;
  common::ObString param_infos_;
  common::ObString sys_vars_str_;
  common::ObString raw_sql_;
  //******** for spm ******
  bool is_evolution_;
  AsynUpdateBaselineStat asyn_baseline_stat_;
  share::schema::ObPlanBaselineInfo bl_info_;
  //******** for spm end ******
  // ***** for acs
  bool is_bind_sensitive_;
  bool is_bind_aware_;
  char plan_sel_info_str_[STMT_MAX_LEN];
  int32_t plan_sel_info_str_len_;
  common::ObSEArray<ObAcsIdxSelRange*, 4> plan_sel_info_array_;
  ObTableScanStat table_scan_stat_[MAX_SCAN_STAT_SIZE];
  // ***** for acs end ****

  int64_t timeout_count_;
  int64_t ps_stmt_id_;  // prepare stmt id

  ObTableRowCount* table_row_count_first_exec_;
  int64_t access_table_num_;
  bool is_expired_;

  // check whether plan has stable performance
  bool enable_plan_expiration_;
  int64_t first_exec_row_count_;

  uint64_t sessid_;
  char plan_tmp_tbl_name_str_[STMT_MAX_LEN];
  int32_t plan_tmp_tbl_name_str_len_;

  bool is_use_jit_;

  bool enable_bf_cache_;
  bool enable_fuse_row_cache_;
  bool enable_row_cache_;
  bool enable_early_lock_release_;
  int64_t bf_filter_cnt_;
  int64_t bf_access_cnt_;
  int64_t fuse_row_cache_hit_cnt_;
  int64_t fuse_row_cache_miss_cnt_;
  int64_t row_cache_hit_cnt_;
  int64_t row_cache_miss_cnt_;
  int64_t cache_stat_update_times_;
  int64_t block_cache_hit_cnt_;
  int64_t block_cache_miss_cnt_;

  // following fields will be used for plan set memory management
  PreCalcExprHandler* pre_cal_expr_handler_;  // the handler that pre-calculable expression holds

  ObPlanStat()
      : plan_id_(0),
        // hash_(0),
        gen_time_(0),
        schema_version_(0),
        merged_version_(0),
        last_active_time_(0),
        hit_count_(0),
        mem_used_(0),
        slow_count_(0),
        slowest_exec_time_(0),
        slowest_exec_usec_(0),
        stmt_len_(0),
        plan_(NULL),
        execute_times_(0),
        disk_reads_(0),
        direct_writes_(0),
        buffer_gets_(0),
        application_wait_time_(0),
        concurrency_wait_time_(0),
        user_io_wait_time_(0),
        rows_processed_(0),
        elapsed_time_(0),
        total_process_time_(0),
        cpu_time_(0),
        large_querys_(0),
        delayed_large_querys_(0),
        delayed_px_querys_(0),
        expected_worker_count_(-1),
        outline_version_(common::OB_INVALID_VERSION),
        outline_id_(common::OB_INVALID_ID),
        is_last_open_succ_(true),
        is_evolution_(false),
        asyn_baseline_stat_(ASYN_NOTHING),
        is_bind_sensitive_(false),
        is_bind_aware_(false),
        plan_sel_info_str_len_(0),
        plan_sel_info_array_(),
        timeout_count_(0),
        ps_stmt_id_(common::OB_INVALID_ID),
        table_row_count_first_exec_(NULL),
        access_table_num_(0),
        is_expired_(false),
        enable_plan_expiration_(false),
        first_exec_row_count_(-1),
        sessid_(0),
        plan_tmp_tbl_name_str_len_(0),
        is_use_jit_(false),
        enable_bf_cache_(true),
        enable_fuse_row_cache_(true),
        enable_row_cache_(true),
        enable_early_lock_release_(false),
        bf_filter_cnt_(0),
        bf_access_cnt_(0),
        fuse_row_cache_hit_cnt_(0),
        fuse_row_cache_miss_cnt_(0),
        row_cache_hit_cnt_(0),
        row_cache_miss_cnt_(0),
        cache_stat_update_times_(0),
        block_cache_hit_cnt_(0),
        block_cache_miss_cnt_(0),
        pre_cal_expr_handler_(NULL)
  {
    sql_id_[0] = '\0';
  }

  ObPlanStat(const ObPlanStat& rhs)
      : plan_id_(rhs.plan_id_),
        // hash_(rhs.hash_),
        gen_time_(rhs.gen_time_),
        schema_version_(rhs.schema_version_),
        merged_version_(rhs.merged_version_),
        last_active_time_(rhs.last_active_time_),
        hit_count_(rhs.hit_count_),
        mem_used_(rhs.mem_used_),
        slow_count_(rhs.slow_count_),
        slowest_exec_time_(rhs.slowest_exec_time_),
        slowest_exec_usec_(rhs.slowest_exec_usec_),
        plan_(rhs.plan_),
        execute_times_(rhs.execute_times_),
        disk_reads_(rhs.disk_reads_),
        direct_writes_(rhs.direct_writes_),
        buffer_gets_(rhs.buffer_gets_),
        application_wait_time_(rhs.application_wait_time_),
        concurrency_wait_time_(rhs.concurrency_wait_time_),
        user_io_wait_time_(rhs.user_io_wait_time_),
        rows_processed_(rhs.rows_processed_),
        elapsed_time_(rhs.elapsed_time_),
        total_process_time_(rhs.total_process_time_),
        cpu_time_(rhs.cpu_time_),
        large_querys_(rhs.large_querys_),
        delayed_large_querys_(rhs.delayed_large_querys_),
        delayed_px_querys_(rhs.delayed_px_querys_),
        expected_worker_count_(rhs.expected_worker_count_),
        outline_version_(rhs.outline_version_),
        outline_id_(rhs.outline_id_),
        is_last_open_succ_(rhs.is_last_open_succ_),
        is_evolution_(rhs.is_evolution_),
        asyn_baseline_stat_(rhs.asyn_baseline_stat_),
        bl_info_(rhs.bl_info_),
        is_bind_sensitive_(rhs.is_bind_sensitive_),
        is_bind_aware_(rhs.is_bind_aware_),
        plan_sel_info_str_len_(rhs.plan_sel_info_str_len_),
        plan_sel_info_array_(rhs.plan_sel_info_array_),
        timeout_count_(rhs.timeout_count_),
        ps_stmt_id_(rhs.ps_stmt_id_),
        table_row_count_first_exec_(NULL),
        access_table_num_(0),
        is_expired_(false),
        enable_plan_expiration_(rhs.enable_plan_expiration_),
        first_exec_row_count_(rhs.first_exec_row_count_),
        sessid_(rhs.sessid_),
        plan_tmp_tbl_name_str_len_(rhs.plan_tmp_tbl_name_str_len_),
        is_use_jit_(rhs.is_use_jit_),
        enable_bf_cache_(rhs.enable_bf_cache_),
        enable_fuse_row_cache_(rhs.enable_fuse_row_cache_),
        enable_row_cache_(rhs.enable_row_cache_),
        enable_early_lock_release_(rhs.enable_early_lock_release_),
        bf_filter_cnt_(rhs.bf_filter_cnt_),
        bf_access_cnt_(rhs.bf_access_cnt_),
        fuse_row_cache_hit_cnt_(rhs.fuse_row_cache_hit_cnt_),
        fuse_row_cache_miss_cnt_(rhs.fuse_row_cache_miss_cnt_),
        row_cache_hit_cnt_(rhs.row_cache_hit_cnt_),
        row_cache_miss_cnt_(rhs.row_cache_miss_cnt_),
        cache_stat_update_times_(rhs.cache_stat_update_times_),
        block_cache_hit_cnt_(rhs.block_cache_hit_cnt_),
        block_cache_miss_cnt_(rhs.block_cache_miss_cnt_),
        pre_cal_expr_handler_(rhs.pre_cal_expr_handler_)
  {
    stmt_len_ = rhs.stmt_len_;
    sql_id_[0] = '\0';
    MEMCPY(stmt_, rhs.stmt_, stmt_len_);
    MEMCPY(plan_sel_info_str_, rhs.plan_sel_info_str_, rhs.plan_sel_info_str_len_);
    MEMCPY(table_scan_stat_, rhs.table_scan_stat_, sizeof(rhs.table_scan_stat_));
    MEMCPY(plan_tmp_tbl_name_str_, rhs.plan_tmp_tbl_name_str_, rhs.plan_tmp_tbl_name_str_len_);
  }

  int to_str_acs_sel_info()
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < plan_sel_info_array_.count(); i++) {
      if (OB_ISNULL(plan_sel_info_array_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(WARN, "null plan sel info", K(ret));
      } else if (OB_ISNULL(plan_sel_info_str_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(WARN, "null plan sel info str", K(ret));
      } else if (OB_FAIL(databuff_printf(plan_sel_info_str_,
                     STMT_MAX_LEN,
                     pos,
                     "{%.*s%s%f%s%f%s}",
                     plan_sel_info_array_.at(i)->index_name_.length(),
                     plan_sel_info_array_.at(i)->index_name_.ptr(),
                     "[",
                     plan_sel_info_array_.at(i)->low_bound_sel_,
                     ",",
                     plan_sel_info_array_.at(i)->high_bound_sel_,
                     "]"))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          SQL_PC_LOG(WARN, "failed to write plan sel info", K(plan_sel_info_array_.at(i)), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      plan_sel_info_str_len_ = static_cast<int32_t>(pos);
    }
    return ret;
  }

  int64_t get_execute_count()
  {
    return timeout_count_ + execute_times_;
  }

  inline void update_cache_stat(const ObTableScanStat& stat)
  {
    const int64_t current_time = common::ObTimeUtility::current_time();
    if (current_time > gen_time_ + CACHE_POLICY_UPDATE_INTERVAL) {
      const int64_t update_times = ATOMIC_AAF(&cache_stat_update_times_, 1);
      ATOMIC_AAF(&bf_filter_cnt_, stat.bf_filter_cnt_);
      ATOMIC_AAF(&bf_access_cnt_, stat.bf_access_cnt_);
      ATOMIC_AAF(&fuse_row_cache_hit_cnt_, stat.fuse_row_cache_hit_cnt_);
      ATOMIC_AAF(&fuse_row_cache_miss_cnt_, stat.fuse_row_cache_miss_cnt_);
      ATOMIC_AAF(&row_cache_hit_cnt_, stat.row_cache_hit_cnt_);
      ATOMIC_AAF(&row_cache_miss_cnt_, stat.row_cache_miss_cnt_);
      if (0 == (update_times & CACHE_POLICY_UDPATE_THRESHOLD)) {
        if (bf_access_cnt_ > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(bf_filter_cnt_) / static_cast<double>(bf_access_cnt_) <= ENABLE_BF_CACHE_THRESHOLD) {
            enable_bf_cache_ = false;
          } else {
            enable_bf_cache_ = true;
          }
        }
        const int64_t row_cache_access_cnt = row_cache_miss_cnt_ + row_cache_hit_cnt_;
        if (row_cache_access_cnt > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(row_cache_hit_cnt_) / static_cast<double>(row_cache_access_cnt) <=
              ENABLE_ROW_CACHE_THRESHOLD) {
            enable_row_cache_ = false;
          } else {
            enable_row_cache_ = true;
          }
        }
        const int64_t fuse_row_cache_access_cnt = fuse_row_cache_hit_cnt_ + fuse_row_cache_miss_cnt_;
        if (fuse_row_cache_access_cnt > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(fuse_row_cache_hit_cnt_) / static_cast<double>(fuse_row_cache_access_cnt) <=
              ENABLE_ROW_CACHE_THRESHOLD) {
            enable_fuse_row_cache_ = false;
          } else {
            enable_fuse_row_cache_ = true;
          }
        }
        SQL_PC_LOG(DEBUG,
            "update cache policy",
            K(sql_id_),
            K(enable_bf_cache_),
            K(enable_row_cache_),
            K(enable_fuse_row_cache_),
            K(bf_filter_cnt_),
            K(bf_access_cnt_),
            K(row_cache_hit_cnt_),
            K(row_cache_access_cnt),
            K(fuse_row_cache_hit_cnt_),
            K(fuse_row_cache_access_cnt));
        row_cache_hit_cnt_ = 0;
        row_cache_miss_cnt_ = 0;
        bf_access_cnt_ = 0;
        bf_filter_cnt_ = 0;
        fuse_row_cache_hit_cnt_ = 0;
        fuse_row_cache_miss_cnt_ = 0;
      }
    }
  }

  /* XXX: support printing maxium 30 class members.
   * if you want to print more members, remove some first
   */
  TO_STRING_KV(K_(plan_id), "sql_text", ObString(stmt_len_, stmt_), K_(raw_sql), K_(gen_time), K_(schema_version),
      K_(last_active_time), K_(hit_count), K_(mem_used), K_(slow_count), K_(slowest_exec_time), K_(slowest_exec_usec),
      K_(execute_times), K_(disk_reads), K_(direct_writes), K_(buffer_gets), K_(application_wait_time),
      K_(concurrency_wait_time), K_(user_io_wait_time), K_(rows_processed), K_(elapsed_time), K_(cpu_time),
      K_(large_querys), K_(delayed_large_querys), K_(outline_version), K_(outline_id), K_(is_evolution),
      K_(is_last_open_succ), K_(is_bind_sensitive), K_(is_bind_aware), K_(is_last_open_succ), K_(timeout_count),
      K_(bl_info));
};

struct SysVarNameVal {
  common::ObString name_;
  common::ObObj value_;

  TO_STRING_KV(K_(name), K_(value));
};

struct StmtStat {
  int64_t memory_used_;
  int64_t last_active_timestamp_;  // used now
  int64_t execute_average_time_;
  int64_t execute_slowest_time_;
  int64_t execute_slowest_timestamp_;
  int64_t execute_count_;  // used now
  int64_t execute_slow_count_;
  int64_t ps_count_;
  bool to_delete_;
  StmtStat()
      : memory_used_(0),
        last_active_timestamp_(0),
        execute_average_time_(0),
        execute_slowest_time_(0),
        execute_slowest_timestamp_(0),
        execute_count_(0),
        execute_slow_count_(0),
        ps_count_(0),
        to_delete_(false)
  {}

  void reset()
  {
    memory_used_ = 0;
    last_active_timestamp_ = 0;
    execute_average_time_ = 0;
    execute_slowest_time_ = 0;
    execute_slowest_timestamp_ = 0;
    execute_count_ = 0;
    execute_slow_count_ = 0;
    ps_count_ = 0;
    to_delete_ = false;
  }

  double weight()
  {
    int64_t time_interval = common::ObTimeUtility::current_time() - last_active_timestamp_;
    double weight = common::OB_PC_WEIGHT_NUMERATOR / static_cast<double>(time_interval);
    return weight;
  }

  TO_STRING_KV(K_(memory_used), K_(last_active_timestamp), K_(execute_average_time), K_(execute_slowest_time),
      K_(execute_slowest_timestamp), K_(execute_count), K_(execute_slow_count), K_(ps_count), K_(to_delete));
};

struct ObGetAllPlanIdOp {
  explicit ObGetAllPlanIdOp(common::ObIArray<uint64_t>* key_array) : key_array_(key_array)
  {}
  ObGetAllPlanIdOp() : key_array_(NULL)
  {}
  void reset()
  {
    key_array_ = NULL;
  }
  int set_key_array(common::ObIArray<uint64_t>* key_array);
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObCacheObject*>& entry);

public:
  common::ObIArray<uint64_t>* key_array_;
};

struct ObPhyLocationGetter {
public:
  // used for getting plan
  static int get_phy_locations(const ObIArray<ObTableLocation>& table_locations, const ObPlanCacheCtx& pc_ctx,
      share::ObIPartitionLocationCache& location_cache, ObIArray<ObPhyTableLocationInfo>& phy_location_infos,
      bool& need_check_on_same_server);

  // used for adding plan
  static int get_phy_locations(const common::ObIArray<ObTablePartitionInfo*>& partition_infos,
      ObIArray<ObPhyTableLocation>& phy_locations, ObIArray<ObPhyTableLocationInfo>& phy_location_infos);

  // used for replica re-select optimization for duplicate table
  static int reselect_duplicate_table_best_replica(
      const ObIArray<ObPhyTableLocationInfo>& phy_locations, bool& on_same_server);
};

struct ObPlanBaselineHeler {

  static int init_baseline_params_info_str(
      const common::Ob2DArray<ObParamInfo, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false>&
          params_info,
      ObIAllocator& alloc, ObString& param_info_str);
};

extern const char* plan_cache_gc_confs[3];
}  // namespace sql
}  // namespace oceanbase
#endif  //_OB_PLAN_CACHE_UTIL_H_
