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

#ifndef OCEANBASE_SQL_CONTEXT_
#define OCEANBASE_SQL_CONTEXT_

#include "ob_sql_utils.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_placement_hashset.h"
#include "lib/container/ob_2d_array.h"
#include "sql/optimizer/ob_table_partition_info.h"
#include "sql/monitor/ob_exec_stat.h"
#include "lib/hash_func/murmur_hash.h"
#include "sql/ob_sql_temp_table.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/client_feedback/ob_feedback_partition_struct.h"
#include "sql/dblink/ob_dblink_utils.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_define.h"
#endif

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObPartMgr;
}
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObTableSchema;
class ObColumnSchemaV2;
}
}
namespace pl
{
class ObPL;
}
namespace sql
{
typedef common::ObIArray<ObTablePartitionInfo *> ObTablePartitionInfoArray;
//ObLocationConstraint如果只有一项, 则仅需要约束该location type是否一致；
//                    如果有多项，则需要校验每一项location对应的物理分布是否一样
struct LocationConstraint;
typedef common::ObSEArray<LocationConstraint, 1, common::ModulePageAllocator, true> ObLocationConstraint;
typedef common::ObFixedArray<LocationConstraint, common::ObIAllocator> ObPlanLocationConstraint;
typedef common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> ObPwjConstraint;
typedef common::ObFixedArray<int64_t, common::ObIAllocator> ObPlanPwjConstraint;
class ObShardingInfo;

struct LocationConstraint
{
  enum InclusionType {
    NotSubset = 0,              // no inclusion relationship
    LeftIsSuperior,             // left contains all the elements in right set
    RightIsSuperior             // right contains all the elements in left set
  };
  enum ConstraintFlag {
    NoExtraFlag        = 0,
    IsMultiPartInsert  = 1,
    // 分区裁剪后基表只涉及到一个一级分区
    SinglePartition    = 1 << 1,
    // 分区裁剪后基表每个一级分区都只涉及一个二级分区
    SingleSubPartition = 1 << 2,
    // is duplicate table not in dml
    DupTabNotInDML     = 1 << 3
  };
  TableLocationKey key_;
  ObTableLocationType phy_loc_type_;
  int64_t constraint_flags_;
  // only used in plan generate
  ObTablePartitionInfo *table_partition_info_;

  LocationConstraint() : key_(), phy_loc_type_(), constraint_flags_(NoExtraFlag), table_partition_info_(NULL) {}

  inline uint64_t hash() const {
    uint64_t hash_ret = key_.hash();
    hash_ret = common::murmurhash(&phy_loc_type_, sizeof(ObTableLocationType), hash_ret);
    return hash_ret;
  }
  inline void add_constraint_flag(ConstraintFlag flag) { constraint_flags_ |= flag; }
  inline bool is_multi_part_insert() const { return constraint_flags_ & IsMultiPartInsert; }
  inline bool is_partition_single() const { return constraint_flags_ & SinglePartition; }
  inline bool is_subpartition_single() const { return constraint_flags_ & SingleSubPartition; }
  inline bool is_dup_table_not_in_dml() const {return constraint_flags_ & DupTabNotInDML; }

  bool operator==(const LocationConstraint &other) const;
  bool operator!=(const LocationConstraint &other) const;

  // calculate the inclusion relationship between ObLocationConstraints
  static int calc_constraints_inclusion(const ObLocationConstraint *left,
                                        const ObLocationConstraint *right,
                                        InclusionType &inclusion_result);

  TO_STRING_KV(K_(key), K_(phy_loc_type), K_(constraint_flags));
};

struct ObLocationConstraintContext
{
  enum InclusionType {
    NotSubset = 0,              // no inclusion relationship
    LeftIsSuperior,             // left contains all the elements in right set
    RightIsSuperior             // right contains all the elements in left set
  };

  ObLocationConstraintContext()
      : base_table_constraints_(),
        strict_constraints_(),
        non_strict_constraints_(),
        dup_table_replica_cons_()
  {
  }
  ~ObLocationConstraintContext()
  {
  }
  static int calc_constraints_inclusion(const ObPwjConstraint *left,
                                        const ObPwjConstraint *right,
                                        InclusionType &inclusion_result);

  TO_STRING_KV(K_(base_table_constraints),
               K_(strict_constraints),
               K_(non_strict_constraints),
               K_(dup_table_replica_cons));
  // 基表location约束，包括TABLE_SCAN算子上的基表和INSERT算子上的基表
  ObLocationConstraint base_table_constraints_;
  // 严格partition wise join约束，要求同一个分组内的基表分区逻辑上和物理上都相等。
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  common::ObSEArray<ObPwjConstraint *, 8, common::ModulePageAllocator, true> strict_constraints_;
  // 严格partition wise join约束，要求用一个分组内的基表分区物理上相等。
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  common::ObSEArray<ObPwjConstraint *, 8, common::ModulePageAllocator, true> non_strict_constraints_;
  // constraints for duplicate table's replica selection
  // if not found values in this array, just use local server's replica.
  common::ObSEArray<ObDupTabConstraint, 1, common::ModulePageAllocator, true> dup_table_replica_cons_;
};

class ObIVtScannerableFactory;
class ObSQLSessionMgr;
class ObSQLSessionInfo;
class ObIVirtualTableIteratorFactory;
class ObRawExpr;
class ObSQLSessionInfo;

class ObSelectStmt;

class ObMultiStmtItem
{
public:
  ObMultiStmtItem()
    : is_part_of_multi_stmt_(false),
      seq_num_(0),
      sql_(),
      batched_queries_(NULL),
      is_ps_mode_(false),
      ab_cnt_(0)
  {
  }
  ObMultiStmtItem(bool is_part_of_multi, int64_t seq_num, const common::ObString &sql)
    : is_part_of_multi_stmt_(is_part_of_multi),
      seq_num_(seq_num),
      sql_(sql),
      batched_queries_(NULL),
      is_ps_mode_(false),
      ab_cnt_(0)
  {
  }

  ObMultiStmtItem(bool is_part_of_multi,
                  int64_t seq_num,
                  const common::ObString &sql,
                  const common::ObIArray<ObString> *queries,
                  bool is_multi_vas_opt)
    : is_part_of_multi_stmt_(is_part_of_multi),
      seq_num_(seq_num),
      sql_(sql),
      batched_queries_(queries),
      is_ps_mode_(false),
      ab_cnt_(0)
  {
  }
  virtual ~ObMultiStmtItem() {}

  void reset()
  {
    is_part_of_multi_stmt_ = false;
    seq_num_ = 0;
    sql_.reset();
    batched_queries_ = NULL;
  }

  inline bool is_part_of_multi_stmt() const { return is_part_of_multi_stmt_; }
  inline int64_t get_seq_num() const { return seq_num_; }
  inline const common::ObString &get_sql() const { return sql_; }
  inline bool is_batched_multi_stmt() const
  {
    bool is_batch = false;
    if (is_ps_mode_) {
      is_batch = (ab_cnt_ > 0);
    } else {
      is_batch = NULL != batched_queries_;
    }
    return is_batch;
  }
  inline int64_t get_batched_stmt_cnt() const
  {
    int64_t batch_cnt = 0;
    if (is_ps_mode_) {
      batch_cnt = ab_cnt_;
    } else if (batched_queries_ != nullptr) {
      batch_cnt = batched_queries_->count();
    }
    return batch_cnt;
  }
  inline const common::ObIArray<ObString> *get_queries() const { return batched_queries_; }
  inline void set_batched_queries(const common::ObIArray<ObString> *batched_queries)
  { batched_queries_ = batched_queries; }
  inline bool is_ab_batch_opt() { return (is_ps_mode_ && ab_cnt_ > 0); }
  inline void set_ps_mode(bool ps) { is_ps_mode_ = ps; }
  inline bool is_ps_mode() { return is_ps_mode_; }
  inline void set_ab_cnt(int64_t cnt) { ab_cnt_ = cnt; }
  inline int64_t get_ab_cnt() { return ab_cnt_; }

  TO_STRING_KV(K_(is_part_of_multi_stmt), K_(seq_num), K_(sql), KPC_(batched_queries),
               K_(is_ps_mode), K_(ab_cnt));

private:
  bool is_part_of_multi_stmt_; // 是否为multi stmt，非multi stmt也使用这个结构体，因此需要这个标记
  int64_t seq_num_; // 表示是在multi stmt中的第几条
  common::ObString sql_;
  // is set only when doing multi-stmt optimization
  const common::ObIArray<ObString> *batched_queries_;
  bool is_ps_mode_;
  int64_t ab_cnt_;
};

struct ObInsertRewriteOptCtx
{
  ObInsertRewriteOptCtx()
    : can_do_opt_(false),
      row_count_(0)
  {}

  void set_can_do_insert_batch_opt(int64_t row_count)
  {
    can_do_opt_ = true;
    row_count_ = row_count;
  }
  bool is_do_insert_batch_opt() const
  {
    bool bret = false;
    if (can_do_opt_ && row_count_ > 1) {
      bret = true;
    }
    return bret;
  }
  inline void clear()
  {
    can_do_opt_ = false;
    row_count_ = 0;
  }
  inline void reset() { clear(); }

  bool can_do_opt_;
  int64_t row_count_;
};


class ObQueryRetryInfo
{
public:
  ObQueryRetryInfo()
    : inited_(false),
      is_rpc_timeout_(false),
      last_query_retry_err_(common::OB_SUCCESS),
      retry_cnt_(0),
      query_switch_leader_retry_timeout_ts_(0)
  {
  }
  virtual ~ObQueryRetryInfo() {}

  int init();
  void reset();
  void clear();
  void clear_state_before_each_retry()
  {
    is_rpc_timeout_ = false;
    // 这里不能清除逐次重试累计的成员，如：invalid_servers_，last_query_retry_err_
  }

  bool is_inited() const { return inited_; }
  void set_is_rpc_timeout(bool is_rpc_timeout);
  bool is_rpc_timeout() const;
  void set_last_query_retry_err(int last_query_retry_err)
  {
    last_query_retry_err_ = last_query_retry_err;
  }
  bool should_fast_fail(uint64_t tenant_id)
  {
    bool fast_fail = false;
    if (0 == query_switch_leader_retry_timeout_ts_) {
      query_switch_leader_retry_timeout_ts_ = INT64_MAX;
      // start timing from first retry, not from query start
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        int64_t timeout = tenant_config->ob_query_switch_leader_retry_timeout;
        if (timeout > 0) {
          query_switch_leader_retry_timeout_ts_ = timeout + common::ObTimeUtility::current_time();
        }
      }
    }
    if (query_switch_leader_retry_timeout_ts_ < common::ObTimeUtility::current_time()) {
      fast_fail = true;
    }
    return fast_fail;
  }
  // 1. timeout 场景下，尽量反馈上次的错误码，使得报错原因可理解
  // 2. 其余场景下，用于获取上次错误码，来决策本地重试行为（如 remote plan 优化走不走）
  int get_last_query_retry_err() const { return last_query_retry_err_; }
  void inc_retry_cnt() { retry_cnt_++; }
  int64_t get_retry_cnt() const { return retry_cnt_; }


  TO_STRING_KV(K_(inited), K_(is_rpc_timeout), K_(last_query_retry_err));

private:
  bool inited_; // 这个变量用于写一些防御性代码，基本没用
  // 用于标记是否是rpc返回的timeout错误码（包括本地超时和回包中的超时错误码）
  bool is_rpc_timeout_;
  // 重试阶段可以将错误码的处理分为三类:
  // 1.重试到超时，将timeout返回给客户端;
  // 2.不再重试的错误码，直接将其返回给客客户端;
  // 3.重试到超时，但是将原始错误码返回给客户端，当前仅有 OB_NOT_SUPPORTED，
  //   对于这类错误码需要正确记录到last_query_retry_err_中，不应该被类型1或2的错误码覆盖。
  int last_query_retry_err_;
  // this value include local retry & packet retry
  int64_t retry_cnt_;
  // for fast fail,
  int64_t query_switch_leader_retry_timeout_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRetryInfo);
};

class ObSqlSchemaGuard
{
public:
  ObSqlSchemaGuard()
  { reset(); }
  ~ObSqlSchemaGuard()
  { reset(); }
  void set_schema_guard(share::schema::ObSchemaGetterGuard *schema_guard)
  { schema_guard_ = schema_guard; }
  share::schema::ObSchemaGetterGuard *get_schema_guard() const
  { return schema_guard_; }
  void reset();
  int get_dblink_schema(const uint64_t tenant_id,
                        const uint64_t dblink_id,
                        const share::schema::ObDbLinkSchema *&dblink_schema);
  int get_table_schema(uint64_t dblink_id,
                       const common::ObString &database_name,
                       const common::ObString &table_name,
                       const share::schema::ObTableSchema *&table_schema,
                       sql::ObSQLSessionInfo *session_info,
                       const ObString &dblink_name,
                       bool is_reverse_link);
  int set_link_table_schema(uint64_t dblink_id,
                            const common::ObString &database_name,
                            share::schema::ObTableSchema *table_schema);
  int get_table_schema(uint64_t table_id,
                       uint64_t ref_table_id,
                       const ObDMLStmt *stmt,
                       const ObTableSchema *&table_schema);
  int get_table_schema(uint64_t table_id,
                       const TableItem *table_item,
                       const ObTableSchema *&table_schema);
  int get_table_schema(uint64_t table_id,
                       const share::schema::ObTableSchema *&table_schema,
                       bool is_link = false) const;
  int get_column_schema(uint64_t table_id, const common::ObString &column_name,
                        const share::schema::ObColumnSchemaV2 *&column_schema,
                        bool is_link = false) const;
  int get_column_schema(uint64_t table_id, uint64_t column_id,
                        const share::schema::ObColumnSchemaV2 *&column_schema,
                        bool is_link = false) const;
  int get_table_schema_version(const uint64_t table_id, int64_t &schema_version) const;
  int get_can_read_index_array(uint64_t table_id,
                               uint64_t *index_tid_array,
                               int64_t &size,
                               bool with_mv,
                               bool with_global_index = true,
                               bool with_domain_index = true,
                               bool with_spatial_index = true);
  int get_link_table_schema(uint64_t table_id,
                            const share::schema::ObTableSchema *&table_schema) const;
  int get_link_column_schema(uint64_t table_id, const common::ObString &column_name,
                             const share::schema::ObColumnSchemaV2 *&column_schema) const;
  int get_link_column_schema(uint64_t table_id, uint64_t column_id,
                             const share::schema::ObColumnSchemaV2 *&column_schema) const;
  int fetch_link_current_scn(uint64_t dblink_id, uint64_t tenant_id, ObSQLSessionInfo *session_info,
                             uint64_t &current_scn);
  // get current scn from dblink. return OB_INVALID_ID if remote server not support current_scn
  int get_link_current_scn(uint64_t dblink_id, uint64_t tenant_id, ObSQLSessionInfo *session_info,
                           uint64_t &current_scn);
public:
  static TableItem *get_table_item_by_ref_id(const ObDMLStmt *stmt, uint64_t ref_table_id);
  static bool is_link_table(const ObDMLStmt *stmt, uint64_t table_id);
private:
  share::schema::ObSchemaGetterGuard *schema_guard_;
  common::ObArenaAllocator allocator_;
  common::ObSEArray<const share::schema::ObTableSchema *, 1> table_schemas_;
  uint64_t next_link_table_id_;
  // key is dblink_id, value is current scn.
  common::hash::ObHashMap<uint64_t, uint64_t> dblink_scn_;
};

#ifndef OB_BUILD_SPM
struct ObBaselineKey
{
  ObBaselineKey()
  : db_id_(common::OB_INVALID_ID),
    constructed_sql_(),
    sql_id_() {}
  ObBaselineKey(uint64_t db_id, const ObString &constructed_sql, const ObString &sql_id)
  : db_id_(db_id),
    constructed_sql_(constructed_sql),
    sql_id_(sql_id) {}

  inline void reset()
  {
    db_id_ = common::OB_INVALID_ID;
    constructed_sql_.reset();
    sql_id_.reset();
  }

  TO_STRING_KV(K_(db_id),
               K_(constructed_sql),
               K_(sql_id));

  uint64_t  db_id_;
  common::ObString constructed_sql_;
  common::ObString sql_id_;
};

struct ObSpmCacheCtx
{
  ObSpmCacheCtx()
    : bl_key_()
  {}
  inline void reset() { bl_key_.reset(); }
  ObBaselineKey bl_key_;
};
#endif

struct ObSqlCtx
{
  OB_UNIS_VERSION(1);
public:
  ObSqlCtx();
  ~ObSqlCtx() { reset(); }
  int set_partition_infos(const ObTablePartitionInfoArray &info, common::ObIAllocator &allocator);
  int set_related_user_var_names(const common::ObIArray<common::ObString> &user_var_names, common::ObIAllocator &allocator);
  int set_location_constraints(const ObLocationConstraintContext &location_constraint,
                               ObIAllocator &allocator);
  int set_multi_stmt_rowkey_pos(const common::ObIArray<int64_t> &multi_stmt_rowkey_pos,
                                common::ObIAllocator &alloctor);
  void reset();

  bool handle_batched_multi_stmt() const { return multi_stmt_item_.is_batched_multi_stmt(); }
  void reset_reroute_info() {
    if (nullptr != reroute_info_) {
      op_reclaim_free(reroute_info_);
    }
    reroute_info_ = NULL;
  }
  share::ObFeedbackRerouteInfo *get_or_create_reroute_info()
  {
    if (nullptr == reroute_info_) {
      reroute_info_ = op_reclaim_alloc(share::ObFeedbackRerouteInfo);
    }
    return reroute_info_;
  }
  share::ObFeedbackRerouteInfo *get_reroute_info() {
    return reroute_info_;
  }

  bool is_batch_params_execute() const
  {
    return multi_stmt_item_.is_batched_multi_stmt() || is_do_insert_batch_opt();
  }

  int64_t get_batch_params_count() const
  {
    int64_t count = 0;
    if (multi_stmt_item_.is_batched_multi_stmt()) {
      count = multi_stmt_item_.get_batched_stmt_cnt();
    } else if (is_do_insert_batch_opt()) {
      count = get_insert_batch_row_cnt();
    }
    return count;
  }

  bool is_do_insert_batch_opt() const
  {
    return ins_opt_ctx_.is_do_insert_batch_opt();
  }

  inline int64_t get_insert_batch_row_cnt() const
  {
    return ins_opt_ctx_.row_count_;
  }
  void set_is_do_insert_batch_opt(int64_t row_count)
  {
    ins_opt_ctx_.set_can_do_insert_batch_opt(row_count);
  }
  void reset_do_insert_batch_opt()
  {
    ins_opt_ctx_.reset();
  }

  void set_enable_strict_defensive_check(bool v)
  {
    enable_strict_defensive_check_ = v;
  }

  bool get_enable_strict_defensive_check()
  {
    return enable_strict_defensive_check_;
  }

  void set_enable_user_defined_rewrite(bool v)
  {
    enable_user_defined_rewrite_ = v;
  }

  bool get_enable_user_defined_rewrite()
  {
    return enable_user_defined_rewrite_;
  }
  // release dynamic allocated memory
  //
  void clear();
public:
  ObMultiStmtItem multi_stmt_item_;
  ObSQLSessionInfo *session_info_;
  share::schema::ObSchemaGetterGuard *schema_guard_;
  pl::ObPLBlockNS *secondary_namespace_;
  bool plan_cache_hit_;
  bool self_add_plan_; //used for retry query, and add plan to plan cache in this query;
  int  disable_privilege_check_;  //internal user set disable privilege check
  bool force_print_trace_; // [OUT]if the trace log is enabled by hint
  bool is_show_trace_stmt_;  // [OUT]
  int64_t retry_times_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  ExecType exec_type_;
  bool is_prepare_protocol_;
  bool is_pre_execute_;
  bool is_prepare_stage_;
  bool is_dynamic_sql_;
  bool is_dbms_sql_;
  bool is_cursor_;
  bool is_remote_sql_;
  uint64_t statement_id_;
  common::ObString cur_sql_;
  stmt::StmtType stmt_type_;
  common::ObFixedArray<ObTablePartitionInfo*, common::ObIAllocator> partition_infos_;
  bool is_restore_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> related_user_var_names_;
  //use for plan cache support dist plan
  // 基表location约束，包括TABLE_SCAN算子上的基表和INSERT算子上的基表
  common::ObFixedArray<LocationConstraint, common::ObIAllocator> base_constraints_;
  // 严格partition wise join约束，要求同一个分组内的基表分区逻辑上和物理上都相等。
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  common::ObFixedArray<ObPwjConstraint *, common::ObIAllocator> strict_constraints_;
  // 严格partition wise join约束，要求用一个分组内的基表分区物理上相等。
  // 每个分组是一个array，保存了对应基表在base_table_constraints_中的偏移
  common::ObFixedArray<ObPwjConstraint *, common::ObIAllocator> non_strict_constraints_;
  // constraints for duplicate table's replica selection
  // if not found values in this array, just use local server's replica.
  common::ObFixedArray<ObDupTabConstraint, common::ObIAllocator> dup_table_replica_cons_;

  // wether need late compilation
  bool need_late_compile_;

  // 从resolver传递过来的常量约束
  // all_possible_const_param_constraints_ 表示该sql中可能的全部常量约束
  // all_plan_const_param_constraints_ 表示该sql中存在的全部常量约束
  // 比如：create table t (a bigint, b bigint as (a + 1 + 2), c bigint as (a + 2 + 3), index idx_b(b), index idx_c(c));
  // 对于：select * from t where a + 1 + 2 > 0;
  // 有：all_plan_const_param_constraints_ = {[1, 2]}, all_possible_const_param_constraints_ = {[1, 2], [2, 3]}
  // 对于：select * from t where a + 3 + 4 > 0;
  // 有：all_plan_const_param_constraints_ = {}, all_possible_const_param_constraints_ = {[1, 2], [2, 3]}
  common::ObIArray<ObPCConstParamInfo> *all_plan_const_param_constraints_;
  common::ObIArray<ObPCConstParamInfo> *all_possible_const_param_constraints_;
  common::ObIArray<ObPCParamEqualInfo> *all_equal_param_constraints_;
  common::ObDList<ObPreCalcExprConstraint> *all_pre_calc_constraints_;
  common::ObIArray<ObExprConstraint> *all_expr_constraints_;
  common::ObIArray<ObPCPrivInfo> *all_priv_constraints_;
  bool need_match_all_params_; //only used for matching plans
  bool is_ddl_from_primary_;//备集群从主库同步过来需要处理的ddl sql语句
  const sql::ObStmt *cur_stmt_;
  const ObPhysicalPlan *cur_plan_;

  bool can_reroute_sql_; // 是否可以重新路由
  bool is_sensitive_;    // 是否含有敏感信息，若有则不记入 sql_audit
  bool is_protocol_weak_read_; // record whether proxy set weak read for this request in protocol flag
  common::ObFixedArray<int64_t, common::ObIAllocator> multi_stmt_rowkey_pos_;
  ObRawExpr *flashback_query_expr_;
  ObSpmCacheCtx spm_ctx_;
  bool is_execute_call_stmt_;
  bool enable_sql_resource_manage_;
  uint64_t res_map_rule_id_;
  int64_t res_map_rule_param_idx_;
  uint64_t res_map_rule_version_;
  bool is_text_ps_mode_;
  uint64_t first_plan_hash_;
  common::ObString first_outline_data_;
  bool is_bulk_;
  ObInsertRewriteOptCtx ins_opt_ctx_;
  union
  {
    uint32_t flags_;
    struct {
      uint32_t enable_strict_defensive_check_: 1; //TRUE if the _enable_defensive_check is '2'
      uint32_t enable_user_defined_rewrite_ : 1;//TRUE if enable_user_defined_rewrite_rules is open
      uint32_t reserved_ : 30;
    };
  };
private:
  share::ObFeedbackRerouteInfo *reroute_info_;
};

struct ObQueryCtx
{
public:
  ObQueryCtx()
    : question_marks_count_(0),
      calculable_items_(),
      ab_param_exprs_(),
      fetch_cur_time_(true),
      is_contain_virtual_table_(false),
      is_contain_inner_table_(false),
      is_contain_select_for_update_(false),
      has_dml_write_stmt_(false),
      ins_values_batch_opt_(false),
      available_tb_id_(common::OB_INVALID_ID - 1),
      stmt_count_(0),
      subquery_count_(0),
      temp_table_count_(0),
      anonymous_view_count_(0),
      all_user_variable_(),
      need_match_all_params_(false),
      has_udf_(false),
      disable_udf_parallel_(false),
      has_is_table_(false),
      reference_obj_tables_(),
      is_table_gen_col_with_udf_(false),
      query_hint_(),
#ifdef OB_BUILD_SPM
      is_spm_evolution_(false),
#endif
      literal_stmt_type_(stmt::T_NONE),
      sql_stmt_(),
      sql_stmt_coll_type_(CS_TYPE_INVALID),
      prepare_param_count_(0),
      is_prepare_stmt_(false),
      has_nested_sql_(false),
      tz_info_(NULL),
      res_map_rule_id_(common::OB_INVALID_ID),
      res_map_rule_param_idx_(common::OB_INVALID_INDEX),
      root_stmt_(NULL),
      udf_has_select_stmt_(false)
  {
  }
  TO_STRING_KV(N_PARAM_NUM, question_marks_count_,
               N_FETCH_CUR_TIME, fetch_cur_time_,
               K_(calculable_items));

  void reset()
  {
    question_marks_count_ = 0;
    calculable_items_.reset();
    fetch_cur_time_ = true;
    is_contain_virtual_table_ = false;
    is_contain_inner_table_ = false;
    is_contain_select_for_update_ = false;
    has_dml_write_stmt_ = false;
    ins_values_batch_opt_ = false;
    available_tb_id_ = common::OB_INVALID_ID - 1;
    stmt_count_ = 0;
    subquery_count_ = 0;
    temp_table_count_ = 0;
    anonymous_view_count_ = 0;
    all_user_variable_.reset();
    need_match_all_params_= false;
    has_udf_ = false;
    disable_udf_parallel_ = false;
    has_is_table_ = false;
    sql_schema_guard_.reset();
    reference_obj_tables_.reset();
    is_table_gen_col_with_udf_ = false;
    query_hint_.reset();
#ifdef OB_BUILD_SPM
    is_spm_evolution_ = false;
#endif
    literal_stmt_type_ = stmt::T_NONE;
    sql_stmt_.reset();
    sql_stmt_coll_type_ = CS_TYPE_INVALID;
    prepare_param_count_ = 0;
    is_prepare_stmt_ = false;
    has_nested_sql_ = false;
    tz_info_ = NULL;
    res_map_rule_id_ = common::OB_INVALID_ID;
    res_map_rule_param_idx_ = common::OB_INVALID_INDEX;
    root_stmt_ = NULL;
    udf_has_select_stmt_ = false;
  }

  int64_t get_new_stmt_id() { return stmt_count_++; }
  int64_t get_new_subquery_id() { return ++subquery_count_; }
  int64_t get_temp_table_id() { return ++temp_table_count_; }
  int64_t get_anonymous_view_id() { return ++anonymous_view_count_; }

  ObQueryHint &get_query_hint_for_update() { return query_hint_; };
  const ObQueryHint &get_query_hint() const { return query_hint_; };
  const ObGlobalHint &get_global_hint() const { return query_hint_.get_global_hint(); }
  int get_qb_name(int64_t stmt_id, ObString &qb_name) const { return query_hint_.get_qb_name(stmt_id, qb_name); }
  void set_literal_stmt_type(const stmt::StmtType type) { literal_stmt_type_ = type; }
  stmt::StmtType get_literal_stmt_type() const { return literal_stmt_type_; }
  inline common::ObString &get_sql_stmt() { return sql_stmt_; }
  inline const common::ObString &get_sql_stmt() const { return sql_stmt_; }
  inline void set_sql_stmt(const char *sql, int32_t sql_len) { sql_stmt_.assign_ptr(sql, sql_len); }
  inline void set_sql_stmt(const common::ObString sql_stmt) { sql_stmt_ = sql_stmt; }
  void set_sql_stmt_coll_type(common::ObCollationType coll_type) { sql_stmt_coll_type_ = coll_type;}
  common::ObCollationType get_sql_stmt_coll_type() { return sql_stmt_coll_type_; }
  void set_prepare_param_count(const int64_t prepare_param_count) { prepare_param_count_ = prepare_param_count; }
  int64_t get_prepare_param_count() const { return prepare_param_count_; }
  bool is_prepare_stmt() const { return is_prepare_stmt_; }
  void set_is_prepare_stmt(bool is_prepare) { is_prepare_stmt_ = is_prepare; }
  bool has_nested_sql() const { return has_nested_sql_; }
  void set_has_nested_sql(bool has_nested_sql) { has_nested_sql_ = has_nested_sql; }
  void set_timezone_info(const common::ObTimeZoneInfo *tz_info) { tz_info_ = tz_info; }
  const common::ObTimeZoneInfo *get_timezone_info() const { return tz_info_; }

public:
  static const int64_t CALCULABLE_EXPR_NUM = 1;
  typedef common::ObSEArray<ObHiddenColumnItem, CALCULABLE_EXPR_NUM, common::ModulePageAllocator, true> CalculableItems;
public:
  int64_t question_marks_count_;
  CalculableItems calculable_items_;
  //array binding param exprs, mark the all array binding param expr in the batch stmt
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> ab_param_exprs_;
  common::ObSArray<share::schema::ObSchemaObjVersion> global_dependency_tables_;
  bool fetch_cur_time_;
  bool is_contain_virtual_table_;
  bool is_contain_inner_table_;
  bool is_contain_select_for_update_;
  bool has_dml_write_stmt_;
  bool ins_values_batch_opt_;
  uint64_t available_tb_id_;
  int64_t stmt_count_;
  int64_t subquery_count_;
  int64_t temp_table_count_;
  int64_t anonymous_view_count_;
  // record all system variables or user variables in this statement
  common::ObSArray<ObVarInfo, common::ModulePageAllocator, true> variables_;
  common::ObSArray<ObPCConstParamInfo, common::ModulePageAllocator, true> all_plan_const_param_constraints_;
  common::ObSArray<ObPCConstParamInfo, common::ModulePageAllocator, true> all_possible_const_param_constraints_;
  common::ObSArray<ObPCParamEqualInfo, common::ModulePageAllocator, true> all_equal_param_constraints_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> var_init_exprs_;
  common::ObDList<ObPreCalcExprConstraint> all_pre_calc_constraints_;
  common::ObSArray<ObExprConstraint, common::ModulePageAllocator, true> all_expr_constraints_;
  common::ObSArray<ObPCPrivInfo, common::ModulePageAllocator, true> all_priv_constraints_;
  common::ObSArray<ObUserVarIdentRawExpr *, common::ModulePageAllocator, true> all_user_variable_;
  common::hash::ObHashMap<uint64_t, ObObj, common::hash::NoPthreadDefendMode> calculable_expr_results_;
  bool need_match_all_params_; //only used for matching plans
  bool has_udf_;
  bool disable_udf_parallel_; //used to deterministic pl udf parallel execute
  bool has_is_table_; // used to mark query has information schema table
  ObSqlSchemaGuard sql_schema_guard_;
  share::schema::ObReferenceObjTable reference_obj_tables_;
  bool is_table_gen_col_with_udf_; // for data consistent check
  ObQueryHint query_hint_;
#ifdef OB_BUILD_SPM
  bool is_spm_evolution_;
#endif
  stmt::StmtType  literal_stmt_type_;
  common::ObString sql_stmt_;
  common::ObCollationType sql_stmt_coll_type_;
  int64_t prepare_param_count_;
  bool is_prepare_stmt_;
  bool has_nested_sql_;
  const common::ObTimeZoneInfo *tz_info_;
  uint64_t res_map_rule_id_;
  int64_t res_map_rule_param_idx_;
  ObDMLStmt *root_stmt_;
  bool udf_has_select_stmt_; // udf has select stmt, not contain other dml stmt
};
} /* ns sql*/
} /* ns oceanbase */
#endif //OCEANBASE_SQL_CONTEXT_
