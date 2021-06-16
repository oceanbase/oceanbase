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

#ifndef _OCEANBASE_SQL_OB_SQL_UTILS_H
#define _OCEANBASE_SQL_OB_SQL_UTILS_H

#include "common/ob_range.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_vector.h"
#include "lib/container/ob_2d_array.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_define.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
namespace oceanbase {
namespace common {
class ObStatManager;
}
namespace sql {
class RowDesc;
class ObSQLSessionInfo;
class ObRawExpr;
class ObRawExprPointer;
class ObStmt;
class ObDMLStmt;
class ObOpRawExpr;
class ObTaskExecutorCtx;
class ObTableLocation;
class ObPhyTableLocation;
class ObQueryRange;
class ObSqlExpression;
class ObPhysicalPlan;
class ObExprResType;
class ObStmtHint;
struct ObTransformerCtx;
typedef common::ObSEArray<common::ObNewRange*, 1, common::ModulePageAllocator, true> ObQueryRangeArray;
typedef common::ObSEArray<bool, 2, common::ModulePageAllocator, true> ObGetMethodArray;
typedef common::Ob2DArray<ObObjParam, common::OB_MALLOC_BIG_BLOCK_SIZE, common::ObWrapperAllocator, false> ParamStore;

struct EstimatedPartition {
  common::ObPartitionKey pkey_;
  common::ObAddr addr_;
  int64_t row_count_;

  EstimatedPartition() : pkey_(), addr_(), row_count_(0)
  {}

  bool is_valid() const
  {
    return pkey_.is_valid() && addr_.is_valid();
  }

  void reset()
  {
    pkey_.reset();
    addr_.reset();
    row_count_ = 0;
  }

  void set(const common::ObPartitionKey& key, const common::ObAddr& addr, const int64_t row_count)
  {
    pkey_ = key;
    addr_ = addr;
    row_count_ = row_count;
  }

  TO_STRING_KV(K_(pkey), K_(addr), K_(row_count));
};

struct ObHiddenColumnItem {
  ObHiddenColumnItem()
  {
    expr_ = NULL;
    hidden_idx_ = common::OB_INVALID_INDEX;
  }
  ObRawExpr* expr_;
  int64_t hidden_idx_;
  TO_STRING_KV(K_(hidden_idx));
};

class ObSQLUtils {
public:
  const static int64_t WITHOUT_FUNC_REGEXP = 1;
  const static int64_t WITHOUT_FUNC_ADDR_TO_PARTITION_ID = 2;
  const static int64_t OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH = 9;
  const static int64_t NO_VALUES = -1;
  const static int64_t VALUE_LIST_LEVEL = 0;
  const static int64_t VALUE_VECTOR_LEVEL = 1;

  static bool is_trans_commit_need_disconnect_err(int err);

  static void check_if_need_disconnect_after_end_trans(
      const int end_trans_err, const bool is_rollback, const bool is_explicit, bool& is_need_disconnect);

  /**
   *  This function gets a list of partition ids from a list of ranges
   *  and a partition func.
   *
   *  The range array is assumed to only contain single_value ranges and
   *  the partiton_ids will contain all the resulted partition ids without
   *  duplicates.
   */
  static int calc_partition_ids(const common::ObIArray<common::ObNewRange*>& ranges,
      const ObSqlExpression& partition_func, const uint64_t part_num, common::ObIArray<uint64_t>& partition_ids);

  static int get_phy_plan_type(common::ObIArray<share::ObPartitionLocation>& part_location_set,
      const common::ObAddr& my_address, ObPhyPlanType& plan_type);

  static int has_local_leader_replica(const ObPartitionLocationIArray& part_array, const common::ObAddr& addr,
      common::ObPartitionKey& key, bool& has_local);

  static int has_local_replica(const ObPhyPartitionLocationInfoIArray& part_loc_info_array, const common::ObAddr& addr,
      common::ObPartitionKey& key, bool& has_local);

  static int find_all_local_replica(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
      const common::ObAddr& addr, common::ObIArray<common::ObPartitionKey>& keys);

  static int replace_questionmarks(ParseNode* tree, const ParamStore& params);

  static int calc_const_or_calculable_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session,
      const ObRawExpr* raw_expr, common::ObObj& result, const ParamStore* params, common::ObIAllocator& allocator);
  static int calc_simple_expr_without_row(const stmt::StmtType stmt_type, ObSQLSessionInfo* session,
      const ObRawExpr* raw_expr, common::ObObj& result, const ParamStore* params, common::ObIAllocator& allocator);
  static int calc_raw_expr_without_row(ObExecContext& exec_ctx, const ObRawExpr* raw_expr, ObObj& result,
      const ParamStore* params, ObIAllocator& allocator);

  static int calc_sql_expression_without_row(ObExecContext& exec_ctx, const ObISqlExpression& expr, ObObj& result);

  static int calc_const_expr(const ObRawExpr* expr, const ParamStore* params, common::ObObj& result, bool& need_check);

  static int get_param_value(
      const common::ObObj& param, const ParamStore& params_array, ObObjParam& result, bool& need_check);

  static int get_param_value(
      const common::ObObj& param, const ParamStore& params_array, ObObj& result, bool& need_check);

  template <class T>
  static int get_param_value(const common::ObObj& param, const ParamStore& params_array, T& result, bool& need_check);

  static int calc_calculable_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session, const ObRawExpr* expr,
      common::ObObj& result, common::ObIAllocator* allocator, const ParamStore& params_array);
  static int calc_const_expr(const stmt::StmtType stmt_type, ObExecContext& exec_ctx, const ObRawExpr* expr,
      common::ObObj& result, common::ObIAllocator* allocator, const ParamStore& params_array);
  static int calc_const_expr(const stmt::StmtType stmt_type, ObSQLSessionInfo* session, const ObRawExpr& expr,
      ObObj& result, ObIAllocator& allocator, const ParamStore& params_array, ObExecContext* exec_ctx = NULL);

  static int calc_const_expr(ObSQLSessionInfo* session, const ObRawExpr* expr, const ParamStore& params,
      ObIAllocator& allocator, common::ObObj& result);

  static int convert_calculable_expr_to_question_mark(ObDMLStmt& stmt, ObRawExpr*& expr, ObTransformerCtx& ctx);

  static int make_generated_expression_from_str(const common::ObString& expr_str,
      const share::schema::ObTableSchema& schema, const share::schema::ObColumnSchemaV2& gen_col,
      const common::ObIArray<share::schema::ObColDesc>& col_ids, common::ObIAllocator& allocator,
      common::ObISqlExpression*& expression);
  static int make_generated_expression_from_str(const common::ObString& expr_str, ObSQLSessionInfo& session,
      const share::schema::ObTableSchema& schema, const share::schema::ObColumnSchemaV2& gen_col,
      const common::ObIArray<share::schema::ObColDesc>& col_ids, common::ObIAllocator& allocator,
      common::ObISqlExpression*& expression, const bool make_column_expression);
  static int make_default_expr_context(ObIAllocator& allocator, ObExprCtx& expr_ctx);
  static int calc_sql_expression(const ObISqlExpression* expr, const share::schema::ObTableSchema& schema,
      const ObIArray<share::schema::ObColDesc>& col_ids, const ObNewRow& row, ObIAllocator& allocator,
      ObExprCtx& expr_ctx, ObObj& result);
  static void destruct_default_expr_context(ObExprCtx& expr_ctx);
  static int64_t get_usec();
  static int check_and_convert_db_name(
      const common::ObCollationType cs_type, const bool perserver_lettercase, common::ObString& name);
  static int cvt_db_name_to_org(
      share::schema::ObSchemaGetterGuard& schema_guard, const ObSQLSessionInfo* session, common::ObString& name);
  static int check_and_convert_table_name(const common::ObCollationType cs_type, const bool perserve_lettercase,
      common::ObString& name, const stmt::StmtType stmt_type = stmt::T_NONE, const bool is_index_table = false);
  static int check_index_name(const common::ObCollationType cs_type, common::ObString& name);
  static int check_column_name(const common::ObCollationType cs_type, common::ObString& name);
  static int check_and_copy_column_alias_name(const common::ObCollationType cs_type, const bool is_auto_gen,
      common::ObIAllocator* allocator, common::ObString& name);
  static bool cause_implicit_commit(ParseResult& result);
  static bool is_end_trans_stmt(const ParseResult& result);
  static bool is_commit_stmt(const ParseResult& result);
  static bool is_modify_tenant_stmt(ParseResult& result);
  static bool is_mysql_ps_not_support_stmt(const ParseResult& result);
  static bool is_readonly_stmt(ParseResult& result);
  static int make_field_name(const char* src, int64_t len, const common::ObCollationType cs_type,
      common::ObIAllocator* allocator, common::ObString& name);
  static int set_compatible_cast_mode(const ObSQLSessionInfo* session, ObCastMode& cast_mode);
  static int get_default_cast_mode(
      const stmt::StmtType& stmt_type, const ObSQLSessionInfo* session, common::ObCastMode& cast_mode);
  static int get_default_cast_mode(
      const ObPhysicalPlan* plan, const ObSQLSessionInfo* session, common::ObCastMode& cast_mode);
  static int get_default_cast_mode(const ObSQLSessionInfo* session, common::ObCastMode& cast_mode);
  // CM_EXPLICIT_CAST, CM_ZERO_FILL, CM_STRICT_MODE
  static int get_default_cast_mode(const bool is_explicit_cast, const uint32_t result_flag,
      const ObSQLSessionInfo* session, common::ObCastMode& cast_mode);
  static int check_well_formed_str(const ObString& src_str, const ObCollationType cs_type, ObString& dst_str,
      bool& is_null, const bool is_strict_mode, const bool ret_error = false);
  static int check_well_formed_str(
      const common::ObObj& src, common::ObObj& dest, const bool is_strict_mode, const bool ret_error = false);
  static void set_insert_update_scope(common::ObCastMode& cast_mode);
  static bool is_insert_update_scope(common::ObCastMode& cast_mode);
  static int get_outline_key(common::ObIAllocator& allocator, const ObSQLSessionInfo* session,
      const common::ObString& query_sql, common::ObString& outline_key,
      share::schema::ObMaxConcurrentParam::FixParamStore& fixed_param_store, ParseMode mode,
      bool& has_questionmark_in_sql);
  static int md5(const common::ObString& stmt, char* sql_id, int32_t len);
  static int filter_hint_in_query_sql(common::ObIAllocator& allocator, const ObSQLSessionInfo& session,
      const common::ObString& sql, common::ObString& param_sql);
  static int filter_head_space(ObString& sql);
  static int construct_outline_sql(common::ObIAllocator& allocator, const ObSQLSessionInfo& session,
      const common::ObString& outline_content, const common::ObString& orig_sql, bool is_need_filter_hint,
      common::ObString& outline_sql);

  static int reconstruct_sql(
      ObIAllocator& allocator, const ObStmt* stmt, ObString& sql, ObObjPrintParams print_params = ObObjPrintParams());

  static int wrap_expr_ctx(const stmt::StmtType& stmt_type, ObExecContext& exec_ctx, common::ObIAllocator& allocator,
      common::ObExprCtx& expr_ctx);

  static int get_partition_service(ObTaskExecutorCtx& executor_ctx, int64_t table_id, ObIDataAccessService*& das);
  /*static int calculate_phy_table_location(ObExecContext &exec_ctx,
                                          ObPartMgr *part_mgr,
                                          const common::ObIArray<ObObjParam> &params,
                                          share::ObIPartitionLocationCache &location_cache,
                                          const common::ObTimeZoneInfo *tz_info,
                                          ObTableLocation &table_location,
                                          ObPhyTableLocation &phy_location);*/

  // use get_tablet_ranges instead.
  static int extract_pre_query_range(const ObQueryRange& pre_query_range, common::ObIAllocator& allocator,
      const ParamStore& param_store, ObQueryRangeArray& key_ranges, ObGetMethodArray get_method,
      const ObDataTypeCastParams& dtc_params, common::ObIArray<int64_t>* range_pos = NULL);

  static bool is_same_type(const ObExprResType& type1, const ObExprResType& type2);

  static bool is_same_type(
      const ObObjMeta& meta1, const ObObjMeta& meta2, const ObAccuracy& accuracy1, const ObAccuracy& accuracy2);

  static bool is_same_type_for_compare(const ObObjMeta& meta1, const ObObjMeta& meta2);

  static int get_partition_range(ObObj* start_row_key, ObObj* end_row_key, ObObj* function_obj,
      const share::schema::ObPartitionFuncType part_type, const ObSqlExpression& part_expr, int64_t range_key_count,
      uint64_t table_id, ObExprCtx& expr_ctx, common::ObNewRange& partition_range);

  static int get_partition_range(ObObj* start_row_key, ObObj* end_row_key, ObObj* function_obj,
      const share::schema::ObPartitionFuncType part_type, const ObExpr* part_expr, int64_t range_key_count,
      uint64_t table_id, ObEvalCtx& eval_ctx, common::ObNewRange& partition_range);

  static int revise_hash_part_object(common::ObObj& obj, const ObNewRow& row, const bool calc_oracle_hash,
      const share::schema::ObPartitionFuncType part_type);

  static int choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
      const common::ObAddr& addr, common::ObPartitionKey& key, common::ObStatManager& stat_manager, bool& has_local);

  static int choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
      const common::ObAddr& addr, common::ObPartitionKey& key, bool& has_local);

  static int choose_best_partition_for_estimation(const ObPhyPartitionLocationInfoIArray& part_loc_info_array,
      storage::ObPartitionService* partition_service, common::ObStatManager& stat_manager, const ObAddr& local_addr,
      const common::ObIArray<ObAddr>& addrs_list, const bool no_use_remote, EstimatedPartition& best_partition);

  static int choose_best_partition_replica_addr(const ObAddr& local_addr,
      const ObPhyPartitionLocationInfo& phy_part_loc_info, storage::ObPartitionService* partition_service,
      ObAddr& selected_addr);

  static int has_global_index(share::schema::ObSchemaGetterGuard* schema_guard, const uint64_t table_id, bool& exists);

  static int wrap_column_convert_ctx(const common::ObExprCtx& expr_ctx, common::ObCastCtx& column_conv_ctx);

  static void init_type_ctx(const ObSQLSessionInfo* session, ObExprTypeCtx& type_ctx);

  /**
   * the px framework switch.
   * if the sql don't give a use_px and parallel hint, we wanna open the px framework in default.
   */
  static bool use_px(const ObStmtHint& hint, bool dist_table = true);

  static bool is_oracle_sys_view(const ObString& table_name);

  static int extend_checker_stmt(ObExecContext& ctx, uint64_t table_id, uint64_t ref_table_id,
      const common::ObIArray<int64_t>& part_ids, const bool is_weak);

  static int make_whole_range(
      ObIAllocator& allocator, const uint64_t ref_table_id, const int64_t rowkey_count, ObNewRange*& whole_range);

  static int make_whole_range(
      ObIAllocator& allocator, const uint64_t ref_table_id, const int64_t rowkey_count, ObNewRange& whole_range);
  static int ConvertFiledNameAttribute(common::ObIAllocator& allocator, const common::ObString& src,
      common::ObString& dst, const uint16_t collation_type);

  static int clear_evaluated_flag(const ObExprPtrIArray& calc_exprs, ObEvalCtx& eval_ctx);

  static int copy_and_convert_string_charset(common::ObIAllocator& allocator, const common::ObString& src,
      common::ObString& dst, common::ObCollationType src_coll, common::ObCollationType dst_coll);
  static int update_session_last_schema_version(
      share::schema::ObMultiVersionSchemaService& schema_service, ObSQLSessionInfo& session_info);
  static int extract_calc_expr_idx(ObDMLStmt& stmt, ObRawExpr* expr, int64_t old_idx, int64_t old_pre_param_size,
      common::ObIArray<int64_t>& calculable_expr_idx);
  static int recursively_extract_calc_expr_idx(ObDMLStmt& stmt, ObRawExpr* expr, int64_t old_idx,
      int64_t old_pre_param_size, common::ObIArray<ObRawExpr*>& record_exprs,
      common::ObIArray<int64_t>& calculable_expr_idx);
  static int extract_calc_exprs(ObDMLStmt& stmt, ObRawExpr* expr, int64_t old_idx, int64_t old_pre_param_size,
      common::ObIArray<ObHiddenColumnItem>& calculable_exprs);
  static int recursively_extract_calc_exprs(common::ObIArray<ObHiddenColumnItem>& stmt_calc_exprs, ObRawExpr* expr,
      int64_t old_idx, int64_t old_pre_param_size, common::ObIArray<ObRawExpr*>& record_exprs, bool& is_need_adjust,
      common::ObIArray<ObHiddenColumnItem>& calc_exprs);
  static int generate_new_name_with_escape_character(
      common::ObIAllocator& allocator, const common::ObString& src, common::ObString& dest, bool is_oracle_mode);
  static int check_table_version(
      bool& equal, const DependenyTableStore& dependency_tables, share::schema::ObSchemaGetterGuard& schema_guard);

  static int generate_view_definition_for_resolve(common::ObIAllocator& allocator,
      common::ObCollationType connection_collation, const share::schema::ObViewSchema& view_schema,
      common::ObString& view_definition);
  static bool is_oracle_empty_string(const common::ObObjParam& param);
  static int handle_audit_record(
      bool need_retry, const ObExecuteMode exec_mode, ObSQLSessionInfo& session, ObExecContext& exec_ctx);
  static int convert_sql_text_from_schema_for_resolve(
      common::ObIAllocator& allocator, const common::ObDataTypeCastParams& dtc_params, ObString& sql_text);
  static int convert_sql_text_to_schema_for_storing(
      common::ObIAllocator& allocator, const common::ObDataTypeCastParams& dtc_params, ObString& sql_text);

  static int print_identifier(char* buf, const int64_t buf_len, int64_t& pos,
      common::ObCollationType connection_collation, const common::ObString& identifier_name);

private:
  static int check_ident_name(const common::ObCollationType cs_type, common::ObString& name,
      const bool check_for_path_char, const int64_t max_ident_len);
  static bool check_mysql50_prefix(common::ObString& db_name);
  struct SessionInfoCtx {
    common::ObCollationType collation_type_;
    common::ObTimeZoneInfo tz_info_;
  };
};  // end of ObSQLUtils

/* used for acs plan */
struct ObAcsIndexInfo {
  ObAcsIndexInfo()
      : index_id_(common::OB_INVALID_ID),
        index_name_(),
        is_index_back_(false),
        is_whole_range_(false),
        prefix_filter_sel_(1.0),
        column_id_(),
        query_range_(NULL),
        allocator_(NULL)
  {}
  ObAcsIndexInfo(common::ObIAllocator* allocator)
      : index_id_(common::OB_INVALID_ID),
        index_name_(),
        is_index_back_(false),
        is_whole_range_(false),
        prefix_filter_sel_(1.0),
        column_id_(allocator),
        query_range_(NULL),
        allocator_(allocator)
  {}
  virtual ~ObAcsIndexInfo();
  int deep_copy(const ObAcsIndexInfo& other);
  TO_STRING_KV(
      K_(index_id), K_(index_name), K_(is_index_back), K_(is_whole_range), K_(prefix_filter_sel), K_(column_id));
  uint64_t index_id_;
  common::ObString index_name_;
  bool is_index_back_;
  bool is_whole_range_;
  double prefix_filter_sel_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_id_;
  ObQueryRange* query_range_;
  common::ObIAllocator* allocator_;
};

class RelExprCheckerBase {
public:
  const static int32_t FIELD_LIST_SCOPE;
  const static int32_t WHERE_SCOPE;
  const static int32_t GROUP_SCOPE;
  const static int32_t HAVING_SCOPE;
  /* const static int32_t INSERT_SCOPE; */
  /* const static int32_t UPDATE_SCOPE; */
  /* const static int32_t AGG_SCOPE; */
  /* const static int32_t VARIABLE_SCOPE; */
  /* const static int32_t WHEN_SCOPE; */
  const static int32_t ORDER_SCOPE;
  //  const static int32_t EXPIRE_SCOPE;
  //  const static int32_t PARTITION_SCOPE;
  const static int32_t FROM_SCOPE;
  const static int32_t LIMIT_SCOPE;
  //  const static int32_t PARTITION_RANGE_SCOPE;
  //  const static int32_t INTO_SCOPE;
  const static int32_t START_WITH_SCOPE;
  const static int32_t CONNECT_BY_SCOPE;
  const static int32_t JOIN_CONDITION_SCOPE;
  const static int32_t EXTRA_OUTPUT_SCOPE;

public:
  RelExprCheckerBase() : duplicated_checker_(), ignore_scope_(0)
  {}
  RelExprCheckerBase(int32_t ignore_scope) : duplicated_checker_(), ignore_scope_(ignore_scope)
  {}
  virtual ~RelExprCheckerBase()
  {
    duplicated_checker_.destroy();
  }
  bool is_ignore(int32_t ignore_scope)
  {
    return ignore_scope & ignore_scope_;
  }
  virtual int init(int64_t bucket_num = CHECKER_BUCKET_NUM);
  virtual int add_expr(ObRawExpr*& expr) = 0;
  int add_exprs(common::ObIArray<ObRawExpr*>& exprs);

protected:
  static const int64_t CHECKER_BUCKET_NUM = 1000;
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> duplicated_checker_;
  int32_t ignore_scope_;
};

class RelExprChecker : public RelExprCheckerBase {
public:
  RelExprChecker(common::ObIArray<ObRawExpr*>& rel_array) : RelExprCheckerBase(), rel_array_(rel_array)
  {}

  RelExprChecker(common::ObIArray<ObRawExpr*>& rel_array, int32_t ignore_scope)
      : RelExprCheckerBase(ignore_scope), rel_array_(rel_array)
  {}
  virtual ~RelExprChecker()
  {}
  int add_expr(ObRawExpr*& expr);

private:
  common::ObIArray<ObRawExpr*>& rel_array_;
};

class FastRelExprChecker : public RelExprCheckerBase {
public:
  FastRelExprChecker(common::ObIArray<ObRawExpr*>& rel_array);
  FastRelExprChecker(common::ObIArray<ObRawExpr*>& rel_array, int32_t ignore_scope);
  virtual ~FastRelExprChecker();
  int add_expr(ObRawExpr*& expr);
  int dedup();

private:
  common::ObIArray<ObRawExpr*>& rel_array_;
  int64_t init_size_;
};

class RelExprPointerChecker : public RelExprCheckerBase {
public:
  RelExprPointerChecker(common::ObIArray<ObRawExprPointer>& rel_array)
      : RelExprCheckerBase(), rel_array_(rel_array), expr_id_map_()
  {}
  RelExprPointerChecker(common::ObIArray<ObRawExprPointer>& rel_array, int32_t ignore_scope)
      : RelExprCheckerBase(ignore_scope), rel_array_(rel_array), expr_id_map_()
  {}
  virtual ~RelExprPointerChecker()
  {}
  virtual int init(int64_t bucket_num = CHECKER_BUCKET_NUM) override;
  int add_expr(ObRawExpr*& expr) override;

private:
  common::ObIArray<ObRawExprPointer>& rel_array_;
  common::hash::ObHashMap<uint64_t, uint64_t, common::hash::NoPthreadDefendMode> expr_id_map_;
};

class AllExprPointerCollector : public RelExprCheckerBase {
public:
  AllExprPointerCollector(common::ObIArray<ObRawExpr**>& rel_array) : RelExprCheckerBase(), rel_array_(rel_array)
  {}
  AllExprPointerCollector(common::ObIArray<ObRawExpr**>& rel_array, int32_t ignore_scope)
      : RelExprCheckerBase(ignore_scope), rel_array_(rel_array)
  {}
  virtual ~AllExprPointerCollector()
  {}
  int add_expr(ObRawExpr*& expr);

private:
  common::ObIArray<ObRawExpr**>& rel_array_;
};

struct ObSqlTraits {
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  bool is_readonly_stmt_;
  bool is_modify_tenant_stmt_;
  bool is_cause_implicit_commit_;
  bool is_commit_stmt_;
  ObItemType stmt_type_;

  ObSqlTraits();
  void reset()
  {
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    is_readonly_stmt_ = false;
    is_modify_tenant_stmt_ = false;
    is_cause_implicit_commit_ = false;
    is_commit_stmt_ = false;
    stmt_type_ = T_INVALID;
  }
  TO_STRING_KV(
      K(is_readonly_stmt_), K(is_modify_tenant_stmt_), K(is_cause_implicit_commit_), K(is_commit_stmt_), K(stmt_type_));
};

template <typename ValueType>
class ObValueChecker {
public:
  ObValueChecker() = delete;

  constexpr ObValueChecker(ValueType min_value, ValueType max_value, int err_ret_code)
      : min_value_(min_value), max_value_(max_value), err_ret_code_(err_ret_code)
  {}

  constexpr inline int validate(const ValueType& value) const
  {
    return (value < min_value_ || value > max_value_) ? err_ret_code_ : common::OB_SUCCESS;
  }

  constexpr inline int validate_str_length(const common::ObString& value) const
  {
    return (value.length() < min_value_ || value.length() > max_value_) ? err_ret_code_ : common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(min_value), K_(max_value), K_(err_ret_code));

private:
  ValueType min_value_;
  ValueType max_value_;
  int err_ret_code_;
};

template <typename ValueType>
class ObPointerChecker {
public:
  ObPointerChecker() = delete;

  constexpr ObPointerChecker(int err_ret_code) : err_ret_code_(err_ret_code)
  {}

  constexpr inline int validate(const ValueType* value) const
  {
    return (nullptr == value) ? err_ret_code_ : common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(err_ret_code));

private:
  int err_ret_code_;
};

template <typename T>
class ObEnumBitSet {
  static const int MAX_ENUM_VALUE = 64;
  static_assert(std::is_enum<T>::value, "typename must be a enum type");
  static_assert(static_cast<int>(T::MAX_VALUE) < MAX_ENUM_VALUE, "Please add MAX_VALUE in enum class");

public:
  inline ObEnumBitSet() : flag_(0)
  {}
  inline ObEnumBitSet(T value)
  {
    set_bit(value);
  }
  inline void reset()
  {
    flag_ = 0;
  }
  inline void set_bit(T bit_pos)
  {
    flag_ |= bit2flag(static_cast<int>(bit_pos));
  }
  inline bool test_bit(T bit_pos) const
  {
    return !!((flag_)&bit2flag(static_cast<int>(bit_pos)));
  }
  inline bool contain(const ObEnumBitSet& other) const
  {
    return (this & other) == other;
  }
  ObEnumBitSet<T>& operator|=(ObEnumBitSet<T> other)
  {
    this->flag_ |= other.flag_;
    return *this;
  }
  ObEnumBitSet<T>& operator&=(ObEnumBitSet<T> other)
  {
    this->flag_ &= other.flag_;
    return *this;
  }
  ObEnumBitSet<T> operator|(ObEnumBitSet<T> other) const
  {
    return ObEnumBitSet<T>(this->flag_ | other.flag_);
  }
  ObEnumBitSet<T> operator&(ObEnumBitSet<T> other) const
  {
    return ObEnumBitSet<T>(this->flag_ & other.flag_);
  }
  TO_STRING_KV(K_(flag));

private:
  inline uint64_t bit2flag(int bit) const
  {
    uint64_t v = 1;
    return v << bit;
  }
  uint64_t flag_;
  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER_TEMP(template <typename T>, ObEnumBitSet<T>, flag_);

struct ObImplicitCursorInfo {
  OB_UNIS_VERSION(1);

public:
  ObImplicitCursorInfo()
      : stmt_id_(common::OB_INVALID_INDEX),
        affected_rows_(0),
        found_rows_(0),
        matched_rows_(0),
        duplicated_rows_(0),
        deleted_rows_(0)
  {}

  int merge_cursor(const ObImplicitCursorInfo& other);
  TO_STRING_KV(K_(stmt_id), K_(affected_rows), K_(found_rows), K_(matched_rows), K_(duplicated_rows), K_(deleted_rows));

  int64_t stmt_id_;
  int64_t affected_rows_;
  int64_t found_rows_;
  int64_t matched_rows_;
  int64_t duplicated_rows_;
  int64_t deleted_rows_;
};

struct ObParamPosIdx {
  OB_UNIS_VERSION_V(1);

public:
  ObParamPosIdx() : pos_(0), idx_(0)
  {}
  ObParamPosIdx(int32_t pos, int32_t idx) : pos_(pos), idx_(idx)
  {}
  virtual ~ObParamPosIdx()
  {}
  TO_STRING_KV(N_POS, pos_, N_IDX, idx_);
  int32_t pos_;
  int32_t idx_;
};

class ObVirtualTableResultConverter {
public:
  ObVirtualTableResultConverter()
      : key_alloc_(nullptr),
        key_cast_ctx_(),
        output_row_types_(nullptr),
        key_types_(nullptr),
        key_with_tenant_ids_(nullptr),
        has_extra_tenant_ids_(nullptr),
        output_row_alloc_(),
        init_alloc_(nullptr),
        inited_row_(false),
        convert_row_(),
        output_row_cast_ctx_(),
        base_table_id_(UINT64_MAX),
        cur_tenant_id_(UINT64_MAX),
        table_schema_(nullptr),
        output_column_ids_(nullptr),
        cols_schema_(),
        tenant_id_col_id_(UINT64_MAX),
        max_col_cnt_(INT64_MAX),
        use_real_tenant_id_(false),
        has_tenant_id_col_(false)
  {}
  ~ObVirtualTableResultConverter()
  {
    destroy();
  }

  void destroy();
  int reset_and_init(ObIAllocator* key_alloc, sql::ObSQLSessionInfo* session,
      const common::ObIArray<ObObjMeta>* output_row_types, const common::ObIArray<ObObjMeta>* key_types,
      const common::ObIArray<bool>* key_with_tenant_ids, const common::ObIArray<bool>* extra_tenant_id,
      ObIAllocator* init_alloc, const share::schema::ObTableSchema* table_schema,
      const common::ObIArray<uint64_t>* output_column_ids, const bool use_real_tenant_id, const bool has_tenant_id_col,
      const int64_t max_col_cnt = INT64_MAX);
  int init_without_allocator(const common::ObIArray<const share::schema::ObColumnSchemaV2*>& col_schemas,
      const common::ObIArray<bool>* key_with_tenant_ids, uint64_t cur_tenant_id, uint64_t tenant_id_col_id,
      const bool use_real_tenant_id);
  int convert_key_ranges(ObIArray<ObNewRange>& key_ranges);
  int convert_output_row(ObNewRow*& src_row);
  int convert_output_row(
      ObEvalCtx& eval_ctx, const common::ObIArray<ObExpr*>& src_exprs, const common::ObIArray<ObExpr*>& dst_exprs);
  int convert_column(ObObj& obj, uint64_t column_id, uint64_t idx);

private:
  int process_tenant_id(const ObIArray<bool>* extract_tenant_ids, const int64_t nth_col, ObIAllocator& allocator,
      bool decode, ObObj& obj);
  int convert_key(const ObRowkey& src, ObRowkey& dst, bool is_start_key, int64_t pos);
  bool need_process_tenant_id(int64_t nth_col, ObObj& obj);
  int get_all_columns_schema();
  int init_output_row(int64_t cell_cnt);
  int get_need_convert_key_ranges_pos(ObNewRange& key_range, int64_t& pos);

public:
  // the memory that allocated must be reset by caller
  ObIAllocator* key_alloc_;
  ObCastCtx key_cast_ctx_;
  const common::ObIArray<ObObjMeta>* output_row_types_;
  const common::ObIArray<ObObjMeta>* key_types_;
  const common::ObIArray<bool>* key_with_tenant_ids_;
  const common::ObIArray<bool>* has_extra_tenant_ids_;
  // every row must be reset
  common::ObArenaAllocator output_row_alloc_;
  // for init output row, lifecycle is from open to close(called destroy)
  common::ObIAllocator* init_alloc_;
  bool inited_row_;
  common::ObNewRow convert_row_;
  ObCastCtx output_row_cast_ctx_;
  uint64_t base_table_id_;
  uint64_t cur_tenant_id_;
  const share::schema::ObTableSchema* table_schema_;
  const common::ObIArray<uint64_t>* output_column_ids_;
  common::ObArray<const share::schema::ObColumnSchemaV2*> cols_schema_;
  uint64_t tenant_id_col_id_;
  int64_t max_col_cnt_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  bool use_real_tenant_id_;
  bool has_tenant_id_col_;
};

class ObLinkStmtParam {
public:
  static int write(char* buf, int64_t buf_len, int64_t& pos, int64_t param_idx);
  static int read_next(const char* buf, int64_t buf_len, int64_t& pos, int64_t& param_idx);
  static int64_t get_param_len();

private:
  static const int64_t PARAM_LEN;
};

class ObSqlFatalErrExtraInfoGuard : public common::ObFatalErrExtraInfoGuard {
public:
  ObSqlFatalErrExtraInfoGuard()
  {
    reset();
  }
  ~ObSqlFatalErrExtraInfoGuard(){};
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    cur_sql_.reset();
    plan_ = nullptr;
    exec_ctx_ = nullptr;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_cur_sql(const common::ObString& cur_sql)
  {
    cur_sql_ = cur_sql;
  }
  void set_cur_plan(const ObPhysicalPlan* plan)
  {
    plan_ = plan;
  }
  void set_exec_context(ObExecContext* exec_ctx)
  {
    exec_ctx_ = exec_ctx;
  }
  DECLARE_TO_STRING;

private:
  uint64_t tenant_id_;
  common::ObString cur_sql_;
  const ObPhysicalPlan* plan_;
  ObExecContext* exec_ctx_;
};

}  // namespace sql
}  // namespace oceanbase

#endif
