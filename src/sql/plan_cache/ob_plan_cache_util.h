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
#include "common/object/ob_object.h"
#include "sql/spm/ob_spm_struct.h"
#include "sql/ob_sql_define.h"
#include "sql/ob_sql_utils.h"
#include "sql/parser/parse_node.h"
#include "sql/plan_cache/ob_pc_ref_handle.h"
#include "sql/das/ob_das_define.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace omt
{
  class ObTenantConfigMgr;
  class ObTenantConfig;
}

namespace common
{
class ObString;
class ObObj;
}

namespace sql
{
class ObPCVSet;
class ObPhysicalPlan;
struct ObSqlCtx;
class ObTableLocation;
class ObPhyTableLocation;
class ObCandiTableLoc;
class ObTablePartitionInfo;
class ObPlanCacheValue;
class ObDASCtx;
class ObILibCacheObject;
class ObILibCacheKey;
class ObILibCacheNode;
class ObPlanCacheKey;
class ObPlanCacheCtx;

typedef uint64_t ObCacheObjID;

#define SERIALIZE_VERSION(arr, allocator, str)\
do {                                      \
  if (OB_SUCC(ret)) { \
    int64_t size = 0; \
    size = serialization::encoded_length_i64(size) * arr.count(); \
    if (0 != size) { \
      char* buf = (char *)allocator.alloc(size); \
      if (NULL == buf) {  \
        ret = OB_ALLOCATE_MEMORY_FAILED;   \
        LOG_WARN("alloc memory failed", K(ret), K(size)); \
      } \
      if (OB_SUCC(ret)) { \
        int64_t pos = 0; \
        for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); i++) { \
          if (OB_FAIL(serialization::encode_i64(buf , size , pos, arr.at(i).version_))) { \
            LOG_WARN("encode failed", K(size), K(pos), K(ret)); \
          } \
        }                         \
      }                           \
      (void)str.assign(buf, static_cast<int32_t>(size)); \
    } \
  }  \
} while(0)

struct ObLCKeyValue
{
  ObLCKeyValue() : node_(NULL) {}
  ObLCKeyValue(ObILibCacheKey *key, ObILibCacheNode *node)
    : key_(key),
      node_(node) {}

  TO_STRING_KV(KP(key_), KP(node_));

  ObILibCacheKey *key_;
  ObILibCacheNode *node_;
};

typedef ObLCKeyValue LCKeyValue;
typedef common::ObSEArray<LCKeyValue , 16> LCKeyValueArray;

struct ObSysVarInPC
{
  common::ObSEArray<common::ObObj, 32> system_variables_;
  char *buf_;
  int64_t buf_size_;

  ObSysVarInPC() : buf_(NULL), buf_size_(0) {
  }

  int push_back(const common::ObObj &value) {
    return system_variables_.push_back(value);
  }

  bool empty() {
    return 0 == system_variables_.count();
  }

  int deep_copy_to_self() {
    int ret = common::OB_SUCCESS;
    int64_t pos = 0;
    common::ObObj obj;
    if (NULL == buf_ || buf_size_ <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K_(buf), K_(buf_size));
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < system_variables_.count(); i ++) {
      if OB_FAIL(obj.deep_copy(system_variables_.at(i), buf_, buf_size_, pos)) {
        SQL_PC_LOG(WARN, "fail to deep copy obj", K(buf_size_), K(pos), K(ret));
      } else {
        system_variables_.at(i) = obj;
      }
    }
    return ret;
  }

  int deep_copy(const ObSysVarInPC &other) {
    int ret = common::OB_SUCCESS;
    common::ObObj obj;
    int64_t pos = 0;
    if (NULL == buf_ || buf_size_ <= 0 || system_variables_.count() != 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K_(buf), K_(buf_size),
                 "system variables count", system_variables_.count());
    }
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < other.system_variables_.count(); i ++) {
      if (OB_FAIL(obj.deep_copy(other.system_variables_.at(i), buf_, buf_size_, pos))) {
        SQL_PC_LOG(WARN, "fail to deep copy obj", K(buf_size_), K(pos), K(ret));
      } else if (OB_FAIL(system_variables_.push_back(obj))) {
        SQL_PC_LOG(WARN, "fail to push sys value", K(ret));
      }
    }
    return ret;
  }

  bool operator==(const ObSysVarInPC &other) const
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
    uint64_t hash_val = seed;
    for (int64_t i = 0; i < system_variables_.count(); ++i) {
      system_variables_.at(i).hash(hash_val, hash_val);
    }
    return static_cast<int64_t>(hash_val);
  }

  void reset()
  {
    system_variables_.reset();
    buf_ = NULL;
    buf_size_ = 0;
  }

  int serialize_sys_vars(char *buf, int64_t buf_len, int64_t &pos)
  {
    int ret = common::OB_SUCCESS;
    pos = 0;
    int64_t size = 0;
    int64_t sys_var_cnt = system_variables_.count();
    for (int32_t i = 0; OB_SUCC(ret) && i < sys_var_cnt; ++i) {
      size = 0;
      if (OB_FAIL(system_variables_.at(i).print_plain_str_literal(buf + pos, buf_len - pos, size))) {
        SQL_PC_LOG(WARN, "fail to encode obj", K(i), K(buf + pos), K(buf_len), K(pos), K(system_variables_.at(i)), K(ret));
      } else {
        pos += size;
        if (i != sys_var_cnt - 1) { //输出间隔符
          if (buf_len - pos <= 0) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_PC_LOG(WARN, "fail to databuf print", K(buf), K(pos));
          } else {
            char delimiter = ',';
            memcpy(buf+pos, &delimiter, sizeof(delimiter));
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

struct ObPCMemPctConf
{
  int64_t limit_pct_; //plan cache可使用的租户内存百分比
  int64_t high_pct_;  //淘汰高水位在plan cache内存上限的百分比
  int64_t low_pct_; //淘汰低水位在plan cache内存上限的百分比

  ObPCMemPctConf() : limit_pct_(common::OB_PLAN_CACHE_PERCENTAGE),
                     high_pct_(common::OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE),
                     low_pct_(common::OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE)
  {
  }
};

enum ParamProperty
{
  INVALID_PARAM,
  NORMAL_PARAM,
  NOT_PARAM,
  NEG_PARAM,
  TRANS_NEG_PARAM,
};

enum AsynUpdateBaselineStat {
  ASYN_NOTHING = 0,
  ASYN_REPLACE,
  ASYN_INSERT
};

struct ObPCParam
{
  ParseNode *node_;
  ParamProperty flag_;

  ObPCParam() : node_(NULL), flag_(INVALID_PARAM)
  {
  }
  TO_STRING_KV(KP_(node), K_(flag));
};

struct ObPCParseInfo
{
  int64_t raw_text_pos_;
  int64_t param_idx_;
  ParamProperty flag_;
  ObPCParseInfo() : raw_text_pos_(-1), param_idx_(-1), flag_(INVALID_PARAM)
  {
  }
  TO_STRING_KV(K_(raw_text_pos), K_(param_idx), K_(flag));
};

struct ObPCConstParamInfo
{
  common::ObSEArray<int64_t, 4> const_idx_;
  common::ObSEArray<common::ObObj, 4> const_params_;
  TO_STRING_KV(K_(const_idx), K_(const_params));
  bool operator==(const ObPCConstParamInfo &other) const
  {
    bool cmp_ret = const_idx_.count() == other.const_idx_.count()
                   && const_params_.count() == other.const_params_.count();
    for (int i=0; cmp_ret && i < const_idx_.count(); i++) {
      cmp_ret = const_idx_.at(i) == other.const_idx_.at(i);
    }
    for (int i=0; cmp_ret && i < const_params_.count(); i++) {
      cmp_ret = const_params_.at(i) == other.const_params_.at(i);
    }
    return cmp_ret;
  }
};

struct ObPCParamEqualInfo
{
  int64_t first_param_idx_;
  int64_t second_param_idx_;
  bool use_abs_cmp_;
  TO_STRING_KV(K_(first_param_idx), K_(second_param_idx), K_(use_abs_cmp));
  ObPCParamEqualInfo():use_abs_cmp_(false) {}
  inline bool operator==(const ObPCParamEqualInfo &other) const
  {
    bool cmp_ret = first_param_idx_ == other.first_param_idx_ &&
                   second_param_idx_ == other.second_param_idx_ &&
                   use_abs_cmp_ == other.use_abs_cmp_;

    return cmp_ret;
  }
};

struct ObDupTabConstraint
{
  uint64_t first_;
  uint64_t second_;
  TO_STRING_KV(K_(first), K_(second));
  ObDupTabConstraint()
    : first_(common::OB_INVALID_ID),
      second_(common::OB_INVALID_ID)
  {}
  ObDupTabConstraint(int64_t first, int64_t second)
    : first_(first),
      second_(second)
  {}
  inline bool operator==(const ObDupTabConstraint &other) const
  {
    return first_ == other.first_ && second_ == other.second_;
  }
};

struct ObPCPrivInfo
{
  share::ObRawPriv sys_priv_;
  bool has_privilege_;
  TO_STRING_KV(K_(sys_priv), K_(has_privilege));
  ObPCPrivInfo() : sys_priv_(PRIV_ID_NONE), has_privilege_(false)
  {
  }
  int assign(const ObPCPrivInfo &other)
  {
    int ret = OB_SUCCESS;
    sys_priv_ = other.sys_priv_;
    has_privilege_ = other.has_privilege_;
    return ret;
  }
  inline bool operator==(const ObPCPrivInfo &other) const
  {
    return sys_priv_ == other.sys_priv_ && has_privilege_ == other.has_privilege_;
  }
};

struct ObOperatorStat
{
  int64_t plan_id_;
  int64_t operation_id_;  //operator_id
  int64_t execute_times_;        //执行次数

  int64_t input_rows_; //累计input rows
  int64_t rescan_times_; //rescan的次数
  int64_t output_rows_; //output rows total
  //由于修改stat的时候没有加锁，所以记录的上一次执行的数据可能不属于一次执行的结果，
  //不再记录last的执行结果
  //int64_t last_input_rows_; //上次input rows
  //int64_t last_rescan_times_; //rescan的次数
  //int64_t last_output_rows_; //output rows in last execution
  //暂时不支持以下和oracle兼容的统计项
  //int64_t last_cr_buffer_gets_; //上次执行逻辑读次数
  //int64_t cr_buffer_gets_; //累计逻辑读次数
  //int64_t last_disk_reads_; //上次物理读次数
  //int64_t disk_reads_; //累计物理读次数
  //int64_t last_disk_writes_; //上次物理写次数
  //int64_t disk_writes_; //累计物理写次数
  //int64_t last_elapsed_time_; //上次本op执行时间
  //int64_t elapsed_time_; //累计执行时间
  ObOperatorStat() : plan_id_(-1),
    operation_id_(-1),
    execute_times_(0),
    input_rows_(0),
    rescan_times_(0),
    output_rows_(0)
  {
  }
  ObOperatorStat(const ObOperatorStat &other)
      : plan_id_(other.plan_id_),
      operation_id_(other.operation_id_),
      execute_times_(other.execute_times_),
      input_rows_(other.input_rows_),
      rescan_times_(other.rescan_times_),
      output_rows_(other.output_rows_)
  {
  }
  void init()
  {
    input_rows_ = 0;
    rescan_times_ = 0;
    output_rows_ = 0;
  }
  TO_STRING_KV(K_(plan_id),
               K_(operation_id),
               K_(execute_times),
               K_(input_rows),
               K_(rescan_times),
               K_(output_rows));

};

struct ObAcsIdxSelRange
{
  ObAcsIdxSelRange()
   : index_name_(),
     low_bound_sel_(0.0),
     high_bound_sel_(1.0)
  { }
  ObAcsIdxSelRange(const ObAcsIdxSelRange &rhs)
   : index_name_(rhs.index_name_),
     low_bound_sel_(rhs.low_bound_sel_),
     high_bound_sel_(rhs.high_bound_sel_)
  { }
  ObString index_name_;
  double low_bound_sel_;
  double high_bound_sel_;
  TO_STRING_KV(K_(index_name),
               K_(low_bound_sel),
               K_(high_bound_sel));
};

struct ObTableScanStat
{
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
  { }
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
  TO_STRING_KV(K_(query_range_row_count),
               K_(indexback_row_count),
               K_(output_row_count),
               K_(bf_filter_cnt),
               K_(bf_access_cnt),
               K_(row_cache_hit_cnt),
               K_(row_cache_miss_cnt),
               K_(fuse_row_cache_hit_cnt),
               K_(fuse_row_cache_miss_cnt));
  OB_UNIS_VERSION(1);
};

struct ObTableRowCount
{
  int64_t op_id_;
  int64_t row_count_;

  ObTableRowCount() : op_id_(OB_INVALID_ID), row_count_(0) {}
  ObTableRowCount(int64_t op_id, int64_t row_count) : op_id_(op_id), row_count_(row_count) {}
  TO_STRING_KV(K_(op_id), K_(row_count));

  OB_UNIS_VERSION(1);
};



struct ObPlanStat
{
  static const int64_t DEFAULT_ADDR_NODE_NUM = 16;
  typedef common::hash::ObHashMap<ObAddr, int64_t,
        common::hash::LatchReadWriteDefendMode, common::hash::hash_func<ObAddr>,
        common::hash::equal_to<ObAddr>,
        common::hash::SimpleAllocer<typename common::hash::HashMapTypes<ObAddr, int64_t>::AllocType,
                                    DEFAULT_ADDR_NODE_NUM>> AddrMap;
  static const int32_t STMT_MAX_LEN = 4096;
  static const int32_t MAX_SCAN_STAT_SIZE = 100;
  static const int64_t CACHE_POLICY_UPDATE_INTERVAL = 60 * 1000 * 1000; // 1 min
 // static const int64_t CACHE_POLICY_UPDATE_INTERVAL = 30L * 60 * 1000 * 1000; // 30 min
  static const int64_t CACHE_POLICY_UDPATE_THRESHOLD = (1L << 20) - 1;
  static const int64_t CACHE_ACCESS_THRESHOLD = 3000;
  static constexpr double ENABLE_BF_CACHE_THRESHOLD = 0.10;
  static constexpr double ENABLE_ROW_CACHE_THRESHOLD = 0.06;

  char exact_mode_sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1]; // sql id for exact mode
  uint64_t plan_id_;              // plan id
  //uint64_t hash_;                 // plan hash value
  int64_t  gen_time_;             // plan generated time
  int64_t  schema_version_;       // plan schema version when generated
  int64_t  last_active_time_;     // plan last hit time
  uint64_t hit_count_;            // plan hit count
  uint64_t mem_used_;             // plan memory size
  uint64_t slow_count_;           // plan execution slow count
  int64_t  slowest_exec_time_;    // plan execution slowest time
  uint64_t slowest_exec_usec_;    // plan execution slowest usec
  common::ObString stmt_;       //  sql stmt
  ObPhysicalPlan *plan_;          // used in explain

  int64_t execute_times_;        //SUCC下执行次数
  int64_t disk_reads_;           //物理读次数
  int64_t direct_writes_;        //物理写次数
  int64_t buffer_gets_;          //逻辑读次数
  int64_t application_wait_time_; //application类等待事件的总等待时间，主要是lock
  int64_t concurrency_wait_time_; //concurrency类等待事件的总等待时间，主要是latch
  int64_t user_io_wait_time_;     //user io类等待事件的总等待时间，主要是block cache miss
  int64_t rows_processed_;        //返回行数
  int64_t elapsed_time_;          //执行时间rt
  int64_t total_process_time_;    //总的process时间
  int64_t cpu_time_;              //CPU时间，目前可以由执行时间减去所有等待事件总等待时间来获得
  int64_t large_querys_;     //被判定为大查询的次数
  int64_t delayed_large_querys_;
  int64_t delayed_px_querys_;    // px query 被丢回队列重试的次数
  int64_t expected_worker_count_;  // px 预期分配线程数
  int64_t minimal_worker_count_;  // minimal threads required for query
  int64_t outline_version_;
  int64_t outline_id_;
  bool is_last_exec_succ_;        // record whether last execute success
  common::ObString sp_info_str_; //记录非参数的常数的信息
  common::ObString param_infos_;  //记录所有被参数化参数的数据类型
  common::ObString sys_vars_str_;
  common::ObString config_str_;
  common::ObString raw_sql_; //记录生成plan时的原始sql
  common::ObCollationType sql_cs_type_;
  common::ObString rule_name_;
  bool is_rewrite_sql_;
  int64_t rule_version_; // the rule version when query rewrite generates a plan
  bool enable_udr_;
  //******** for spm ******
  //该计划是否正在演进过程中
  bool is_evolution_;
  uint64_t  db_id_;
  common::ObString constructed_sql_;
  common::ObString sql_id_;
  ObEvolutionStat evolution_stat_; //baseline相关统计信息
  //******** for spm end ******
  // ***** for acs
  bool is_bind_sensitive_;
  bool is_bind_aware_;
  char plan_sel_info_str_[STMT_MAX_LEN];
  int32_t plan_sel_info_str_len_;
  common::ObSEArray<ObAcsIdxSelRange*, 4> plan_sel_info_array_;
  ObTableScanStat table_scan_stat_[MAX_SCAN_STAT_SIZE];
  // ***** for acs end ****

  int64_t timeout_count_; //超时次数
  int64_t ps_stmt_id_;//prepare stmt id

  /** 记录第一次计划执行时计划涉及的各个表的行数的数组，数组应该有access_table_num_个元素 */
  ObTableRowCount *table_row_count_first_exec_;
  int64_t access_table_num_;         //plan访问的表的个数，目前只统计whole range扫描的表
  bool is_expired_; // 这个计划是否已经由于数据的表行数变化和执行时间变化而失效

  // check whether plan has stable performance
  bool enable_plan_expiration_;
  int64_t first_exec_row_count_;
  int64_t first_exec_usec_;
  int64_t sample_times_;
  int64_t sample_exec_row_count_;
  int64_t sample_exec_usec_;

  // 临时表计划的sessid表示对应的会话id，非临时表sessid为0
  uint64_t sessid_;
  // 临时表计划所包含的临时表名
  char plan_tmp_tbl_name_str_[STMT_MAX_LEN];
  int32_t plan_tmp_tbl_name_str_len_;

  // plan是否使用了jit编译表达式
  bool is_use_jit_;

  // 以下的字段用于存储层cache访问策略的自使用选择
  bool enable_bf_cache_; // 表示是否访问bloomfilter cache的开关
  bool enable_fuse_row_cache_; // 表示是否访问fuse row cache的开关
  bool enable_row_cache_; // 表示是否访问row cache的开关
  bool enable_early_lock_release_; // 表示是否提前解行锁
  int64_t bf_filter_cnt_; // 表示bloomfilter成功过滤的次数
  int64_t bf_access_cnt_; // 表示bloomfilter访问的次数
  int64_t fuse_row_cache_hit_cnt_; // 表示fuse row cache命中次数
  int64_t fuse_row_cache_miss_cnt_; // 表示fuse row cache不命中次数
  int64_t row_cache_hit_cnt_; // 表示row cache命中次数
  int64_t row_cache_miss_cnt_; // 表示row cache不命中次数
  int64_t cache_stat_update_times_; // 表示cache统计信息更新的次数，用于控制更新cache访问策略的频率
  int64_t block_cache_hit_cnt_; // 表示block cache命中次数
  int64_t block_cache_miss_cnt_; // 表示block cache不命中次数

  // following fields will be used for plan set memory management
  PreCalcExprHandler* pre_cal_expr_handler_; //the handler that pre-calculable expression holds
  AddrMap expected_worker_map_; // px 全局预期分配线程数
  AddrMap minimal_worker_map_;  // global minial threads required for query
  uint64_t plan_hash_value_;
  common::ObString outline_data_;
  common::ObString hints_info_;
  bool hints_all_worked_;


  ObPlanStat()
    : plan_id_(0),
      //hash_(0),
      gen_time_(0),
      schema_version_(0),
      last_active_time_(0),
      hit_count_(0),
      mem_used_(0),
      slow_count_(0),
      slowest_exec_time_(0),
      slowest_exec_usec_(0),
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
      minimal_worker_count_(-1),
      outline_version_(common::OB_INVALID_VERSION),
      outline_id_(common::OB_INVALID_ID),
      is_last_exec_succ_(true),
      sql_cs_type_(common::CS_TYPE_INVALID),
      rule_name_(),
      is_rewrite_sql_(false),
      rule_version_(OB_INVALID_VERSION),
      enable_udr_(false),
      is_evolution_(false),
      db_id_(common::OB_INVALID_ID),
      constructed_sql_(),
      sql_id_(),
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
      pre_cal_expr_handler_(NULL),
      plan_hash_value_(0),
      hints_all_worked_(true)
{
  exact_mode_sql_id_[0] = '\0';
}

  ObPlanStat(const ObPlanStat &rhs)
    : plan_id_(rhs.plan_id_),
      //hash_(rhs.hash_),
      gen_time_(rhs.gen_time_),
      schema_version_(rhs.schema_version_),
      last_active_time_(rhs.last_active_time_),
      hit_count_(rhs.hit_count_),
      mem_used_(rhs.mem_used_),
      slow_count_(rhs.slow_count_),
      slowest_exec_time_(rhs.slowest_exec_time_),
      slowest_exec_usec_(rhs.slowest_exec_usec_),
      stmt_(rhs.stmt_),
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
      minimal_worker_count_(rhs.minimal_worker_count_),
      outline_version_(rhs.outline_version_),
      outline_id_(rhs.outline_id_),
      is_last_exec_succ_(rhs.is_last_exec_succ_),
      sql_cs_type_(rhs.sql_cs_type_),
      rule_name_(),
      is_rewrite_sql_(false),
      rule_version_(OB_INVALID_VERSION),
      enable_udr_(false),
      is_evolution_(rhs.is_evolution_),
      db_id_(rhs.db_id_),
      evolution_stat_(rhs.evolution_stat_),
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
      pre_cal_expr_handler_(rhs.pre_cal_expr_handler_),
      plan_hash_value_(rhs.plan_hash_value_),
      hints_all_worked_(rhs.hints_all_worked_)
  {
    exact_mode_sql_id_[0] = '\0';
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
     } else if (OB_FAIL(databuff_printf(plan_sel_info_str_, STMT_MAX_LEN, pos, "{%.*s%s%f%s%f%s}",
                                        plan_sel_info_array_.at(i)->index_name_.length(),
                                        plan_sel_info_array_.at(i)->index_name_.ptr(), "[",
                                        plan_sel_info_array_.at(i)->low_bound_sel_, ",",
                                        plan_sel_info_array_.at(i)->high_bound_sel_, "]"))) {
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

  //包含超时和SUCC的执行次数
  int64_t get_execute_count()
  {
    return timeout_count_ + execute_times_;
  }

  inline void update_cache_stat(const ObTableScanStat &stat)
  {
    if (ObClockGenerator::getClock() > gen_time_ + CACHE_POLICY_UPDATE_INTERVAL) {
      const int64_t update_times = ATOMIC_AAF(&cache_stat_update_times_, 1);
      ATOMIC_AAF(&bf_filter_cnt_, stat.bf_filter_cnt_);
      ATOMIC_AAF(&bf_access_cnt_, stat.bf_access_cnt_);
      ATOMIC_AAF(&fuse_row_cache_hit_cnt_, stat.fuse_row_cache_hit_cnt_);
      ATOMIC_AAF(&fuse_row_cache_miss_cnt_, stat.fuse_row_cache_miss_cnt_);
      ATOMIC_AAF(&row_cache_hit_cnt_, stat.row_cache_hit_cnt_);
      ATOMIC_AAF(&row_cache_miss_cnt_, stat.row_cache_miss_cnt_);
      if (0 == (update_times & CACHE_POLICY_UDPATE_THRESHOLD)) {
        if (bf_access_cnt_ > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(bf_filter_cnt_) / static_cast<double>(bf_access_cnt_)
              <= ENABLE_BF_CACHE_THRESHOLD) {
            enable_bf_cache_ = false;
          } else {
            enable_bf_cache_ = true;
          }
        }
        const int64_t row_cache_access_cnt = row_cache_miss_cnt_ + row_cache_hit_cnt_;
        if (row_cache_access_cnt > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(row_cache_hit_cnt_) / static_cast<double>(row_cache_access_cnt)
              <= ENABLE_ROW_CACHE_THRESHOLD) {
            enable_row_cache_ = false;
          } else {
            enable_row_cache_ = true;
          }
        }
        const int64_t fuse_row_cache_access_cnt = fuse_row_cache_hit_cnt_ + fuse_row_cache_miss_cnt_;
        if (fuse_row_cache_access_cnt > CACHE_ACCESS_THRESHOLD) {
          if (static_cast<double>(fuse_row_cache_hit_cnt_) / static_cast<double>(fuse_row_cache_access_cnt)
              <= ENABLE_ROW_CACHE_THRESHOLD) {
            enable_fuse_row_cache_ = false;
          } else {
            enable_fuse_row_cache_ = true;
          }
        }
        SQL_PC_LOG(DEBUG, "update cache policy", K(sql_id_), K(exact_mode_sql_id_),
            K(enable_bf_cache_), K(enable_row_cache_), K(enable_fuse_row_cache_),
            K(bf_filter_cnt_), K(bf_access_cnt_),
            K(row_cache_hit_cnt_), K(row_cache_access_cnt),
            K(fuse_row_cache_hit_cnt_), K(fuse_row_cache_access_cnt));
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
  TO_STRING_KV(K_(plan_id),
               "sql_text", stmt_,
               K_(raw_sql),
               K_(gen_time),
               K_(schema_version),
               K_(last_active_time),
               K_(hit_count),
               K_(mem_used),
               K_(slow_count),
               K_(slowest_exec_time),
               K_(slowest_exec_usec),
               K_(execute_times),
               K_(disk_reads),
               K_(direct_writes),
               K_(buffer_gets),
               K_(application_wait_time),
               K_(concurrency_wait_time),
               K_(user_io_wait_time),
               K_(rows_processed),
               K_(elapsed_time),
               K_(cpu_time),
               K_(large_querys),
               K_(delayed_large_querys),
               K_(outline_version),
               K_(outline_id),
               K_(is_evolution),
               K_(is_last_exec_succ),
               K_(is_bind_sensitive),
               K_(is_bind_aware),
               K_(is_last_exec_succ),
               K_(timeout_count),
               K_(evolution_stat),
               K_(plan_hash_value),
               K_(hints_all_worked));
};

struct SysVarNameVal
{
  common::ObString name_;
  common::ObObj value_;

  TO_STRING_KV(K_(name), K_(value));
};

struct ObGetAllPlanIdOp
{
  explicit ObGetAllPlanIdOp(common::ObIArray<uint64_t> *key_array)
    : key_array_(key_array)
  {
  }
  ObGetAllPlanIdOp()
    : key_array_(NULL)
  {
  }
  void reset() { key_array_ = NULL; }
  int set_key_array(common::ObIArray<uint64_t> *key_array);
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry);

public:
  common::ObIArray<uint64_t> *key_array_;
};

struct ObGetAllCacheIdOp
{
  explicit ObGetAllCacheIdOp(common::ObIArray<uint64_t> *key_array)
    : key_array_(key_array)
  {
  }
  ObGetAllCacheIdOp()
    : key_array_(NULL)
  {
  }
  void reset() { key_array_ = NULL; }
  int set_key_array(common::ObIArray<uint64_t> *key_array);
  int operator()(common::hash::HashMapPair<ObCacheObjID, ObILibCacheObject *> &entry);

public:
  common::ObIArray<uint64_t> *key_array_;
};

struct ObPhyLocationGetter
{
public:
  // used for getting plan
  static int get_phy_locations(const ObIArray<ObTableLocation> &table_locations,
                               const ObPlanCacheCtx &pc_ctx,
                               ObIArray<ObCandiTableLoc> &phy_location_infos,
                               bool &need_check_on_same_server);

  // used for adding plan
  static int get_phy_locations(const common::ObIArray<ObTablePartitionInfo *> &partition_infos,
                               //ObIArray<ObDASTableLoc> &phy_locations,
                               ObIArray<ObCandiTableLoc> &phy_location_infos);

  static int build_table_locs(ObDASCtx &das_ctx,
                              const common::ObIArray<ObTableLocation> &table_locations,
                              const common::ObIArray<ObCandiTableLoc> &candi_table_locs);
  static int build_related_tablet_info(const ObTableLocation &table_location,
                                       ObExecContext &exec_ctx,
                                       DASRelatedTabletMap *&related_map);

  // used for replica re-select optimization for duplicate table
  static int reselect_duplicate_table_best_replica(const ObIArray<ObCandiTableLoc> &phy_locations,
                                                   bool &on_same_server);
};

/**
 * This class is the entity of configuration infos that has influence in execution plan.
 *
 * Firstly, @Funciton is_out_of_date() will find out whether configs cached in
 * @Class ObConfigInfoInPC had changed; if changed, do update cached configs.
 *
 * update cached configs
 * 1. @Funciton load_influence_plan_config() will load values
 * 2. @Function serialize_configs() will serialize config values to strings and plan cache will
 *    compare this string so as to figure out whether configs has changed.
 * 3. after generate string, @Function should do @Function update_version()
 *
 * add configs has influence in execution plan. @see load_influence_plan_config();
 *
 * NOTES:
 * to add configs that will influence execution plan, please add to following funcs:
 *  1. load_influence_plan_config();
 *  2. serialize_configs();
 *  3. adds default values to ObBasicSessionInfo::load_default_configs_in_pc()
 * */
class ObConfigInfoInPC
{
public:
  static const int DEFAULT_PUSHDOWN_STORAGE_LEVEL = 3;

  ObConfigInfoInPC()
  : pushdown_storage_level_(DEFAULT_PUSHDOWN_STORAGE_LEVEL),
    rowsets_enabled_(false),
    enable_px_batch_rescan_(true),
    bloom_filter_enabled_(true),
    enable_newsort_(true),
    px_join_skew_handling_(true),
    is_strict_defensive_check_(true),
    px_join_skew_minfreq_(30),
    min_cluster_version_(0),
    is_enable_px_fast_reclaim_(false),
    enable_var_assign_use_das_(true),
    cluster_config_version_(-1),
    tenant_config_version_(-1),
    tenant_id_(0)
  {
  }
  // init tenant_id_
  void init(int t_id) {tenant_id_ = t_id;}
  // load configs which will influence execution plan
  int load_influence_plan_config();
  // generate config string
  int serialize_configs(char *buf, int buf_len, int64_t &pos);
  // whether configs has been changed
  bool is_out_of_date(int64_t cluster_config, int64_t tenant_config)
  {
    return !(cluster_config==cluster_config_version_ &&
              tenant_config==tenant_config_version_);
  }
  void update_version(int64_t cluster_config, int64_t tenant_config)
  {
    cluster_config_version_ = cluster_config;
    tenant_config_version_ = tenant_config;
  }
private:
  int get_all_influence_plan_config();
  bool is_equal(ObString& str);

public:
  //
  // here to add config values
  //
  int pushdown_storage_level_;
  bool rowsets_enabled_;
  bool enable_px_batch_rescan_;
  bool enable_px_ordered_coord_;
  bool bloom_filter_enabled_;
  bool enable_newsort_;
  bool px_join_skew_handling_;
  bool is_strict_defensive_check_;
  int8_t px_join_skew_minfreq_;
  uint64_t min_cluster_version_;
  bool is_enable_px_fast_reclaim_;
  bool enable_var_assign_use_das_;

private:
  // current cluster config version_
  int64_t cluster_config_version_;
  // current tenant config version_
  int64_t tenant_config_version_;
  int64_t tenant_id_;
};

extern const char* plan_cache_gc_confs[3];
}
}
#endif //_OB_PLAN_CACHE_UTIL_H_
