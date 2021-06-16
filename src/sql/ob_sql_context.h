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
#include "share/client_feedback/ob_feedback_partition_struct.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObPartMgr;
}  // namespace common
namespace share {
namespace schema {
class ObSchemaGetterGuard;
class ObTableSchema;
class ObColumnSchemaV2;
}  // namespace schema
}  // namespace share
namespace share {
class ObPartitionTableOperator;
class ObIPartitionLocationCache;
}  // namespace share

namespace sql {
typedef common::ObIArray<ObTablePartitionInfo*> ObTablePartitionInfoArray;
struct LocationConstraint;
typedef common::ObSEArray<LocationConstraint, 4, common::ModulePageAllocator, true> ObLocationConstraint;
typedef common::ObFixedArray<LocationConstraint, common::ObIAllocator> ObPlanLocationConstraint;
typedef common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> ObPwjConstraint;
typedef common::ObFixedArray<int64_t, common::ObIAllocator> ObPlanPwjConstraint;
class ObShardingInfo;

struct LocationConstraint {
  enum InclusionType {
    NotSubset = 0,   // no inclusion relationship
    LeftIsSuperior,  // left contains all the elements in right set
    RightIsSuperior  // right contains all the elements in left set
  };
  enum ConstraintFlag {
    NoExtraFlag = 0,
    IsMultiPartInsert = 1,
    // After the partition pruning, the base table only involves one first-level partition
    SinglePartition = 1 << 1,
    // After partition pruning, each primary partition of the base table involves
    // only one secondary partition
    SingleSubPartition = 1 << 2
  };
  TableLocationKey key_;
  ObTableLocationType phy_loc_type_;
  int64_t constraint_flags_;
  // only used in plan generate
  ObShardingInfo* sharding_info_;

  LocationConstraint() : key_(), phy_loc_type_(), constraint_flags_(NoExtraFlag), sharding_info_(NULL)
  {}

  inline uint64_t hash() const
  {
    uint64_t hash_ret = key_.hash();
    hash_ret = common::murmurhash(&phy_loc_type_, sizeof(ObTableLocationType), hash_ret);
    return hash_ret;
  }
  inline void add_constraint_flag(ConstraintFlag flag)
  {
    constraint_flags_ |= flag;
  }
  inline bool is_multi_part_insert() const
  {
    return constraint_flags_ & IsMultiPartInsert;
  }
  inline bool is_partition_single() const
  {
    return constraint_flags_ & SinglePartition;
  }
  inline bool is_subpartition_single() const
  {
    return constraint_flags_ & SingleSubPartition;
  }

  bool operator==(const LocationConstraint& other) const;
  bool operator!=(const LocationConstraint& other) const;

  // calculate the inclusion relationship between ObLocationConstaints
  static int calc_constraints_inclusion(
      const ObLocationConstraint* left, const ObLocationConstraint* right, InclusionType& inclusion_result);

  TO_STRING_KV(K_(key), K_(phy_loc_type), K_(constraint_flags));
};

struct ObLocationConstraintContext {
  enum InclusionType {
    NotSubset = 0,   // no inclusion relationship
    LeftIsSuperior,  // left contains all the elements in right set
    RightIsSuperior  // right contains all the elements in left set
  };

  ObLocationConstraintContext() : base_table_constraints_(), strict_constraints_(), non_strict_constraints_()
  {}
  ~ObLocationConstraintContext()
  {}
  static int calc_constraints_inclusion(
      const ObPwjConstraint* left, const ObPwjConstraint* right, InclusionType& inclusion_result);

  TO_STRING_KV(K_(base_table_constraints), K_(strict_constraints), K_(non_strict_constraints));
  // Base table location constraints, including the base table on the TABLE_SCAN operator and the
  // base table on the INSERT operator
  ObLocationConstraint base_table_constraints_;
  // Strict partition wise join constraints, requiring that the base table partitions in the same
  // group are logically and physically equal.
  // Each group is an array, which saves the offset of the corresponding base table in
  // base_table_constraints_
  common::ObSEArray<ObPwjConstraint*, 8, common::ModulePageAllocator, true> strict_constraints_;
  // Strict partition wise join constraints, requiring the base table partitions in a group to be
  // physically equal.
  // Each group is an array, which saves the offset of the corresponding base table in
  // base_table_constraints_
  common::ObSEArray<ObPwjConstraint*, 8, common::ModulePageAllocator, true> non_strict_constraints_;
};

class ObIVtScannerableFactory;
class ObSQLSessionMgr;
class ObSQLSessionInfo;
class ObIVirtualTableIteratorFactory;
class ObRawExpr;
class planText;
class ObSQLSessionInfo;

class ObSelectStmt;

class ObMultiStmtItem {
public:
  ObMultiStmtItem() : is_part_of_multi_stmt_(false), seq_num_(0), sql_(), batched_queries_(NULL)
  {}
  ObMultiStmtItem(bool is_part_of_multi, int64_t seq_num, const common::ObString& sql)
      : is_part_of_multi_stmt_(is_part_of_multi), seq_num_(seq_num), sql_(sql), batched_queries_(NULL)
  {}
  ObMultiStmtItem(
      bool is_part_of_multi, int64_t seq_num, const common::ObString& sql, const common::ObIArray<ObString>* queries)
      : is_part_of_multi_stmt_(is_part_of_multi), seq_num_(seq_num), sql_(sql), batched_queries_(queries)
  {}
  virtual ~ObMultiStmtItem()
  {}

  void reset()
  {
    is_part_of_multi_stmt_ = false;
    seq_num_ = 0;
    sql_.reset();
    batched_queries_ = NULL;
  }

  inline bool is_part_of_multi_stmt() const
  {
    return is_part_of_multi_stmt_;
  }
  inline int64_t get_seq_num() const
  {
    return seq_num_;
  }
  inline const common::ObString& get_sql() const
  {
    return sql_;
  }
  inline bool is_batched_multi_stmt() const
  {
    return NULL != batched_queries_;
  }
  inline const common::ObIArray<ObString>* get_queries() const
  {
    return batched_queries_;
  }
  inline void set_batched_queries(const common::ObIArray<ObString>* batched_queries)
  {
    batched_queries_ = batched_queries;
  }

  TO_STRING_KV(K_(is_part_of_multi_stmt), K_(seq_num), K_(sql));

private:
  bool is_part_of_multi_stmt_;
  int64_t seq_num_;  // Indicates which item is in the multi stmt
  common::ObString sql_;
  // is set only when doing multi-stmt optimization
  const common::ObIArray<ObString>* batched_queries_;
};

class ObQueryRetryInfo {
public:
  ObQueryRetryInfo()
      : inited_(false), is_rpc_timeout_(false), invalid_servers_(), last_query_retry_err_(common::OB_SUCCESS)
  {}
  virtual ~ObQueryRetryInfo()
  {}

  int init();
  void reset();
  void clear();
  void clear_state_before_each_retry();
  int merge(const ObQueryRetryInfo& other);

  bool is_inited() const
  {
    return inited_;
  }
  void set_is_rpc_timeout(bool is_rpc_timeout);
  bool is_rpc_timeout() const;
  int add_invalid_server_distinctly(const common::ObAddr& invalid_server, bool print_info_log = false);
  const common::ObIArray<common::ObAddr>& get_invalid_servers() const
  {
    return invalid_servers_;
  }
  void set_last_query_retry_err(int last_query_retry_err)
  {
    last_query_retry_err_ = last_query_retry_err;
  }
  int get_last_query_retry_err() const
  {
    return last_query_retry_err_;
  }

  TO_STRING_KV(K_(inited), K_(is_rpc_timeout), K_(invalid_servers), K_(last_query_retry_err));

private:
  bool inited_;  // This variable is used to write some defensive code, basically useless
  // Used to mark whether it is the timeout error code returned by rpc (including the local timeout and the timeout
  // error code in the response packet)
  bool is_rpc_timeout_;
  common::ObSEArray<common::ObAddr, 1> invalid_servers_;
  // The error code processing can be divided into three categories in the retry phase:
  // 1. Retry to timeout and return timeout to the client;
  // 2. The error code that is no longer retried, will be directly returned to the guest client;
  // 3. Retry to timeout, but return the original error code to the client, currently only OB_NOT_SUPPORTED,
  // For this type of error code, it needs to be correctly recorded in last_query_retry_err_ and should not be
  // overwritten by type 1 or 2 error codes.
  int last_query_retry_err_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryRetryInfo);
};

class ObSqlSchemaGuard {
public:
  ObSqlSchemaGuard()
  {
    reset();
  }
  void set_schema_guard(share::schema::ObSchemaGetterGuard* schema_guard)
  {
    schema_guard_ = schema_guard;
  }
  share::schema::ObSchemaGetterGuard* get_schema_guard() const
  {
    return schema_guard_;
  }
  void reset();
  int get_table_schema(uint64_t dblink_id, const common::ObString& database_name, const common::ObString& table_name,
      const share::schema::ObTableSchema*& table_schema);
  int get_table_schema(uint64_t table_id, const share::schema::ObTableSchema*& table_schema) const;
  int get_column_schema(uint64_t table_id, const common::ObString& column_name,
      const share::schema::ObColumnSchemaV2*& column_schema) const;
  int get_column_schema(
      uint64_t table_id, uint64_t column_id, const share::schema::ObColumnSchemaV2*& column_schema) const;
  int get_table_schema_version(const uint64_t table_id, int64_t& schema_version) const;
  int get_partition_cnt(uint64_t table_id, int64_t& part_cnt) const;
  int get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size, bool with_mv,
      bool with_global_index = true, bool with_domain_index = true);

private:
  int get_link_table_schema(uint64_t table_id, const share::schema::ObTableSchema*& table_schema) const;
  int get_link_column_schema(uint64_t table_id, const common::ObString& column_name,
      const share::schema::ObColumnSchemaV2*& column_schema) const;
  int get_link_column_schema(
      uint64_t table_id, uint64_t column_id, const share::schema::ObColumnSchemaV2*& column_schema) const;

private:
  share::schema::ObSchemaGetterGuard* schema_guard_;
  common::ModulePageAllocator allocator_;
  common::ObSEArray<const share::schema::ObTableSchema*, 8> table_schemas_;
  uint64_t next_link_table_id_;
};

struct ObSqlCtx {
public:
  ObSqlCtx();
  ~ObSqlCtx()
  {
    reset();
  }
  int set_partition_infos(const ObTablePartitionInfoArray& info, common::ObIAllocator& allocator);
  int set_acs_index_info(const common::ObIArray<ObAcsIndexInfo*>& acs_index_infos, common::ObIAllocator& allocator);
  int set_related_user_var_names(
      const common::ObIArray<common::ObString>& user_var_names, common::ObIAllocator& allocator);
  int set_location_constraints(const ObLocationConstraintContext& location_constraint, ObIAllocator& allocator);
  int set_multi_stmt_rowkey_pos(const common::ObIArray<int64_t>& multi_stmt_rowkey_pos, common::ObIAllocator& alloctor);
  void reset();

  bool handle_batched_multi_stmt() const
  {
    return multi_stmt_item_.is_batched_multi_stmt();
  }

  // release dynamic allocated memory
  void clear();

public:
  ObMultiStmtItem multi_stmt_item_;
  ObSQLSessionInfo* session_info_;
  ObSqlSchemaGuard sql_schema_guard_;
  share::schema::ObSchemaGetterGuard* schema_guard_;
  common::ObMySQLProxy* sql_proxy_;
  ObIVirtualTableIteratorFactory* vt_iter_factory_;
  bool use_plan_cache_;
  bool plan_cache_hit_;
  bool self_add_plan_;           // used for retry query, and add plan to plan cache in this query;
  int disable_privilege_check_;  // internal user set disable privilege check
  const share::ObPartitionTableOperator* partition_table_operator_;
  ObSQLSessionMgr* session_mgr_;
  common::ObPartMgr* part_mgr_;
  share::ObIPartitionLocationCache* partition_location_cache_;
  int64_t merged_version_;
  bool force_print_trace_;   // [OUT]if the trace log is enabled by hint
  bool is_show_trace_stmt_;  // [OUT]
  int64_t retry_times_;
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  ExecType exec_type_;
  bool is_prepare_protocol_;
  bool is_prepare_stage_;
  bool is_dynamic_sql_;
  bool is_dbms_sql_;
  bool is_cursor_;
  bool is_remote_sql_;
  bool is_execute_async_;
  uint64_t statement_id_;
  common::ObString cur_sql_;
  stmt::StmtType stmt_type_;
  common::ObFixedArray<ObTablePartitionInfo*, common::ObIAllocator> partition_infos_;
  bool is_restore_;
  ObLoctionSensitiveHint loc_sensitive_hint_;
  common::ObFixedArray<ObAcsIndexInfo*, common::ObIAllocator> acs_index_infos_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> related_user_var_names_;

  // use for plan cache support dist plan
  // Base table location constraints, including the base table on the TABLE_SCAN operator and the base table on the
  // INSERT operator
  common::ObFixedArray<LocationConstraint, common::ObIAllocator> base_constraints_;
  // Strict partition wise join constraints, requiring that the base table partitions in the same group are logically
  // and physically equal. Each group is an array, which saves the offset of the corresponding base table in
  // base_table_constraints_
  common::ObFixedArray<ObPwjConstraint*, common::ObIAllocator> strict_constraints_;
  // Strict partition wise join constraints, requiring the base table partitions in a group to be physically equal.
  // Each group is an array, which saves the offset of the corresponding base table in base_table_constraints_
  common::ObFixedArray<ObPwjConstraint*, common::ObIAllocator> non_strict_constraints_;
  // wether need late compilation
  bool need_late_compile_;
  bool is_bushy_tree_;

  // Constant constraints passed from the resolver
  // all_possible_const_param_constraints_ indicates all possible constant constraints in the sql
  // all_plan_const_param_constraints_ indicates all the constant constraints that exist in the sql
  // For example: create table t (a bigint, b bigint as (a + 1 + 2), c bigint as (a + 2 + 3), index idx_b(b), index
  // idx_c(c)); For: select * from t where a + 1 + 2> 0; Yes: all_plan_const_param_constaints_ = {[1, 2]},
  // all_possible_const_param_constraints_ = {[1, 2], [2, 3]} For: select * from t where a + 3 + 4> 0; Yes:
  // all_plan_const_param_constraints_ = {}, all_possible_const_param_constraints_ = {[1, 2], [2, 3]}
  common::ObIArray<ObPCConstParamInfo>* all_plan_const_param_constraints_;
  common::ObIArray<ObPCConstParamInfo>* all_possible_const_param_constraints_;
  common::ObIArray<ObPCParamEqualInfo>* all_equal_param_constraints_;
  common::ObIArray<uint64_t>* trans_happened_route_;
  bool is_ddl_from_primary_;  // The standby cluster synchronizes the ddl sql statement that needs to be processed from
                              // the main library
  const sql::ObStmt* cur_stmt_;

  bool can_reroute_sql_;
  share::ObFeedbackRerouteInfo reroute_info_;
  common::ObFixedArray<int64_t, common::ObIAllocator> multi_stmt_rowkey_pos_;
};

struct ObQueryCtx {
public:
  struct IdNamePair final {
    IdNamePair() : id_(common::OB_INVALID_STMT_ID), name_(), origin_name_(), stmt_type_(stmt::T_NONE)
    {}
    IdNamePair(uint64_t id, const common::ObString& name, const common::ObString& origin_name, stmt::StmtType stmt_type)
        : id_(id), name_(name), origin_name_(origin_name), stmt_type_(stmt_type)
    {}
    bool operator<(const IdNamePair& pair_r) const
    {
      return id_ < pair_r.id_;
    }

    int64_t id_;
    common::ObString name_;
    common::ObString origin_name_;
    stmt::StmtType stmt_type_;
    TO_STRING_KV(K_(id), K_(name), K_(origin_name), K_(stmt_type));
  };

  ObQueryCtx()
      : question_marks_count_(0),
        calculable_items_(),
        table_partition_infos_(common::ObModIds::OB_COMMON_ARRAY_OB_STMT, common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        exec_param_ref_exprs_(),
        table_ids_(),
        fetch_cur_time_(true),
        is_contain_virtual_table_(false),
        is_contain_inner_table_(false),
        is_contain_select_for_update_(false),
        is_contain_mv_(false),
        available_tb_id_(common::OB_INVALID_ID - 1),
        stmt_count_(0),
        subquery_count_(0),
        temp_table_count_(0),
        valid_qb_name_stmt_ids_(),
        monitoring_ids_(),
        all_user_variable_(),
        forbid_use_px_(false),
        has_udf_(false),
        has_pl_udf_(false),
        temp_table_infos_()
  {}
  TO_STRING_KV(N_PARAM_NUM, question_marks_count_, N_FETCH_CUR_TIME, fetch_cur_time_, K_(calculable_items));

  void reset()
  {
    question_marks_count_ = 0;
    calculable_items_.reset();
    table_partition_infos_.reset();
    exec_param_ref_exprs_.reset();
    table_ids_.reset();
    fetch_cur_time_ = true;
    is_contain_virtual_table_ = false;
    is_contain_inner_table_ = false;
    is_contain_select_for_update_ = false;
    is_contain_mv_ = false;
    available_tb_id_ = common::OB_INVALID_ID - 1;
    stmt_count_ = 0;
    subquery_count_ = 0;
    temp_table_count_ = 0;
    valid_qb_name_stmt_ids_.reset();
    forbid_use_px_ = false;
    monitoring_ids_.reset();
    all_user_variable_.reset();
    has_udf_ = false;
    has_pl_udf_ = false;
    temp_table_infos_.reset();
  }
  int add_index_hint(common::ObString& qb_name, ObOrgIndexHint& index_hint)
  {
    return org_indexes_.push_back(ObQNameIndexHint(qb_name, index_hint));
  }

  int64_t get_new_stmt_id()
  {
    return stmt_count_++;
  }
  int64_t get_new_subquery_id()
  {
    return ++subquery_count_;
  }
  int64_t get_temp_table_id()
  {
    return ++temp_table_count_;
  }

  int store_id_stmt(const int64_t stmt_id, const ObStmt* stmt);

  int find_stmt_id(const common::ObString& stmt_name, int64_t& stmt_id);

  ObStmt* find_stmt_by_id(int64_t stmt_id);

  int add_stmt_id_name(const int64_t stmt_id, const common::ObString& stmt_name, ObStmt* stmt);

  int generate_stmt_name(common::ObIAllocator* allocator);

  int get_dml_stmt_name(stmt::StmtType stmt_type, char* buf, int64_t buf_len, int64_t& pos);

  int append_id_to_stmt_name(char* buf, int64_t buf_len, int64_t& pos);

  int get_stmt_name_by_id(const int64_t stmt_id, common::ObString& stmt_name) const;
  int get_stmt_org_name_by_id(const int64_t stmt_id, common::ObString& org_name) const;
  int get_stmt_org_name(const common::ObString& stmt_name, common::ObString& org_name) const;
  int distribute_hint_to_stmt();
  int distribute_index_hint();
  int distribute_pq_hint();

  int replace_name_in_tables(const ObSQLSessionInfo& session_info, ObDMLStmt& stmt,
      common::ObIArray<ObTablesInHint>& arr, uint64_t table_id, common::ObString& to_db_name,
      common::ObString& to_table_name);

  // When a single table view is merged, the global hint needs to replace the name.
  int replace_name_for_single_table_view(const ObSQLSessionInfo& session_info, ObDMLStmt* stmt, uint64_t table_id,
      common::ObString& to_db_name, common::ObString& to_table_name);

  int distribute_rewrite_hint(const common::ObIArray<ObString>& qb_names, ObStmtHint::RewriteHint method);
  int distribute_tables_hint(common::ObIArray<ObTablesInHint>& tables_hint_arr, ObStmtHint::TablesHint method);
  int print_qb_name_hint(planText& plan_text) const;
  bool is_cross_tenant_query(uint64_t effective_tenant_id) const;
  int add_tracing_hint(common::ObIArray<uint64_t>& tracing_ids);
  int add_stat_hint(common::ObIArray<uint64_t>& stat_ids);
  int add_pq_map_hint(common::ObString& qb_name, ObOrgPQMapHint& pq_map_hint);
  const common::ObIArray<ObMonitorHint>& get_monitor_ids()
  {
    return monitoring_ids_;
  };
  int add_temp_table(ObSqlTempTableInfo* temp_table_info);
  int get_temp_table_plan(const uint64_t temp_table_id, ObLogicalOperator*& temp_table_insert);

public:
  static const int64_t CALCULABLE_EXPR_NUM = 16;
  typedef common::ObSEArray<ObHiddenColumnItem, CALCULABLE_EXPR_NUM, common::ModulePageAllocator, true> CalculableItems;

public:
  int64_t question_marks_count_;
  CalculableItems calculable_items_;
  common::ObSEArray<ObTablePartitionInfo*, 8, common::ModulePageAllocator, true> table_partition_infos_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> exec_param_ref_exprs_;
  common::hash::ObPlacementHashSet<uint64_t, common::OB_MAX_TABLE_NUM_PER_STMT, true> table_ids_;
  common::ObSEArray<share::schema::ObSchemaObjVersion, 4> global_dependency_tables_;
  common::ObSEArray<ObOptTableStatVersion, 4> table_stat_versions_;
  bool fetch_cur_time_;
  bool is_contain_virtual_table_;
  bool is_contain_inner_table_;
  bool is_contain_select_for_update_;
  bool is_contain_mv_;
  uint64_t available_tb_id_;
  int64_t stmt_count_;
  int64_t subquery_count_;
  int64_t temp_table_count_;
  // record all system variables or user variables in this statement
  common::ObSEArray<ObVarInfo, 4, common::ModulePageAllocator, true> variables_;
  common::ObSEArray<IdNamePair, 3, common::ModulePageAllocator, true> stmt_id_name_map_;
  common::ObSEArray<std::pair<int64_t, ObStmt*>, 3, common::ModulePageAllocator, true> id_stmt_map_;
  common::ObSEArray<ObQNameIndexHint, 3, common::ModulePageAllocator, true> org_indexes_;
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> join_order_;
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> use_merge_;     // record table infos to use
                                                                                          // merge for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> no_use_merge_;  // record table infos to no
                                                                                          // use merge for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> use_hash_;  // record table infos to use hash
                                                                                      // for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> no_use_hash_;  // record table infos to no use
                                                                                         // hash for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> use_nl_;       // record table infos to use
                                                                                         // nestedloop for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> no_use_nl_;    // record table infos to no use
                                                                                         // nestedloop for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> use_bnl_;      // record table names to use
                                                                                         // block-nestedloop for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> no_use_bnl_;   // record table infos to no use
                                                                                         // block-nestedloop for join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true>
      use_nl_materialization_;  // record table infos to use material before nl join
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true>
      no_use_nl_materialization_;  // record table infos to no use material before nl join
  common::ObSEArray<uint64_t, 3, common::ModulePageAllocator, true> valid_qb_name_stmt_ids_;
  common::ObSEArray<ObQBNamePQDistributeHint, 2, common::ModulePageAllocator, true> org_pq_distributes_;
  common::ObSEArray<ObQBNamePQMapHint, 2, common::ModulePageAllocator, true> org_pq_maps_;
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> px_join_filter_;
  common::ObSEArray<ObTablesInHint, 3, common::ModulePageAllocator, true> no_px_join_filter_;

  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> merge_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> no_merge_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> unnest_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> no_unnest_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> no_expand_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> use_concat_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> place_group_by_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> no_place_group_by_;
  common::ObSEArray<ObString, 3, common::ModulePageAllocator, true> no_pred_deduce_;

  common::ObSEArray<ObPCConstParamInfo, 4, common::ModulePageAllocator, true> all_plan_const_param_constraints_;
  common::ObSEArray<ObPCConstParamInfo, 4, common::ModulePageAllocator, true> all_possible_const_param_constraints_;
  common::ObSEArray<ObPCParamEqualInfo, 4, common::ModulePageAllocator, true> all_equal_param_constraints_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> trans_happened_route_;

  common::ObSEArray<ObMonitorHint, 8> monitoring_ids_;
  common::ObSEArray<ObUserVarIdentRawExpr*, 8, common::ModulePageAllocator, true> all_user_variable_;
  bool forbid_use_px_;
  bool has_udf_;
  bool has_pl_udf_;  // used to mark query has pl udf
  common::ObSEArray<ObSqlTempTableInfo*, 4, common::ModulePageAllocator, true> temp_table_infos_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_CONTEXT_
