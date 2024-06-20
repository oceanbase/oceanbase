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
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/geo/ob_s2adapter.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/ob_i_sql_expression.h"          // ObISqlExpression,ObExprCtx
#include "share/schema/ob_table_param.h"        // ObColDesc
#include "share/schema/ob_multi_version_schema_service.h"     // ObMultiVersionSchemaService
#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_define.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/monitor/flt/ob_flt_span_mgr.h"
#include "share/ob_compatibility_control.h"
namespace oceanbase
{
namespace share {
class ObBackupStorageInfo;
}
namespace sql
{
class RowDesc;
class ObSQLSessionInfo;
class ObRawExpr;
class ObRawExprPointer;
class ObStmt;
class ObDMLStmt;
class ObOpRawExpr;
class OrderItem;
class ObTaskExecutorCtx;
class ObTableLocation;
class ObPhyTableLocation;
class ObQueryRange;
class ObSqlExpression;
class ObPhysicalPlan;
class ObExprResType;
class ObStmtHint;
struct ObTransformerCtx;
struct ObPreCalcExprFrameInfo;
typedef common::ObSEArray<common::ObNewRange *, 1> ObQueryRangeArray;
struct ObExprConstraint;
typedef common::ObSEArray<common::ObSpatialMBR, 1> ObMbrFilterArray;
class ObSelectStmt;

struct EstimatedPartition {
  common::ObAddr addr_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;

  EstimatedPartition() : addr_(), tablet_id_(), ls_id_()
  {}

  bool is_valid() const {
    return addr_.is_valid() && tablet_id_.is_valid() && ls_id_.is_valid();
  }

  void reset() {
    addr_.reset();
    tablet_id_.reset();
    ls_id_.reset();
  }

  void set(const common::ObAddr &addr,
           const common::ObTabletID &tablet_id,
           const share::ObLSID &ls_id) {
    addr_ = addr;
    tablet_id_ = tablet_id;
    ls_id_ = ls_id;
  }

  TO_STRING_KV(K_(addr), K_(tablet_id), K_(ls_id));
};

struct ObHiddenColumnItem
{
  ObHiddenColumnItem()
  {
    expr_ = NULL;
    hidden_idx_ = common::OB_INVALID_INDEX;
  }
  ObRawExpr* expr_;
  int64_t hidden_idx_;
  TO_STRING_KV(K_(hidden_idx));
};

class ObSqlArrayExpandGuard
{
public:
  ObSqlArrayExpandGuard(ParamStore &params, common::ObIAllocator &allocator);
  ~ObSqlArrayExpandGuard();
  int get_errcode() const { return ret_; }
private:
  typedef std::pair<common::ObObjParam*, common::ObObjParam> ArrayObjPair;
  common::ObList<ArrayObjPair, common::ObIAllocator> array_obj_list_;
  int ret_;
};

class ObISqlPrinter
{
protected:
  virtual int inner_print(char *buf, int64_t buf_len, int64_t &pos) = 0;
public:
  virtual int do_print(ObIAllocator &allocator, ObString &result);
  virtual ~ObISqlPrinter() = default;
};

class ObSqlPrinter : public ObISqlPrinter
{
public:
  ObSqlPrinter(const ObStmt *stmt,
               ObSchemaGetterGuard *schema_guard,
               ObObjPrintParams print_params,
               const ParamStore *param_store,
               const ObSQLSessionInfo *session) :
    stmt_(stmt),
    schema_guard_(schema_guard),
    print_params_(print_params),
    param_store_(param_store),
    session_(session)
    {}
  virtual int inner_print(char *buf, int64_t buf_len, int64_t &res_len) override;

protected:
  const ObStmt *stmt_;
  ObSchemaGetterGuard *schema_guard_;
  ObObjPrintParams print_params_;
  const ParamStore *param_store_;
  const ObSQLSessionInfo *session_;
};

class ObSQLUtils
{
public:

  const static int64_t WITHOUT_FUNC_REGEXP = 1;
  const static int64_t WITHOUT_FUNC_ADDR_TO_PARTITION_ID = 2;
  const static int64_t OB_MYSQL50_TABLE_NAME_PREFIX_LENGTH = 9;
  const static int64_t NO_VALUES = -1;        //表示没有values()
  const static int64_t VALUE_LIST_LEVEL = 0;  //表示在parse的T_VALUE_LIST层
  const static int64_t VALUE_VECTOR_LEVEL = 1;//表示在parse的T_VALUE_VECTOR层

  static bool is_trans_commit_need_disconnect_err(int err);

  static int check_enable_decimalint(const sql::ObSQLSessionInfo *session, bool &enable_decimalint);

  static void check_if_need_disconnect_after_end_trans(const int end_trans_err,
                                                       const bool is_rollback,
                                                       const bool is_explicit,
                                                       bool &is_need_disconnect);

  /**
   *  This function gets a list of partition ids from a list of ranges
   *  and a partition func.
   *
   *  The range array is assumed to only contain single_value ranges and
   *  the partition_ids will contain all the resulted partition ids without
   *  duplicates.
   */
  static int calc_partition_ids(const common::ObIArray<common::ObNewRange*> &ranges,
                                const ObSqlExpression &partition_func,
                                const uint64_t part_num,
                                common::ObIArray<uint64_t> &partition_ids);

  static int get_phy_plan_type(common::ObIArray<share::ObPartitionLocation> &part_location_set,
                               const common::ObAddr &my_address,
                               ObPhyPlanType &plan_type);
  static int has_outer_join_symbol(const ParseNode *node, bool &has);
  static int replace_questionmarks(ParseNode *tree,
                                   const ParamStore &params);

  static int calc_const_or_calculable_expr(ObExecContext *exec_ctx,
                                           const ObRawExpr *raw_expr,
                                           common::ObObj &result,
                                           bool &is_valid,
                                           common::ObIAllocator &allocator,
                                           bool ignore_failure = true,
                                           ObIArray<ObExprConstraint> *constraints = NULL);
  static int calc_simple_expr_without_row(ObSQLSessionInfo *session,
                                          const ObRawExpr *raw_expr,
                                          common::ObObj &result,
                                          const ParamStore *params,
                                          common::ObIAllocator &allocator);
  static int calc_raw_expr_without_row(ObExecContext &exec_ctx,
                                       const ObRawExpr *raw_expr,
                                       ObObj &result,
                                       const ParamStore *params,
                                       ObIAllocator &allocator);

  static void clear_expr_eval_flags(const ObExpr &expr, ObEvalCtx &ctx);
  static int calc_sql_expression_without_row(ObExecContext &exec_ctx,
                                             const ObISqlExpression &expr,
                                             ObObj &result,
                                             ObIAllocator *allocator = NULL);

  static int calc_const_expr(const ObRawExpr *expr,
                             const ParamStore *params,
                             common::ObObj &result,
                             bool &need_check);

  template<class T>
  static int get_param_value(const common::ObObj &param,
                             const ParamStore &params_array,
                             T &result,
                             bool &need_check)
  {
    int ret = common::OB_SUCCESS;
    int64_t param_idx = -1;
    need_check = false;
    if (param.is_unknown()) {
      if (OB_FAIL(param.get_unknown(param_idx))) {
        SQL_LOG(WARN, "get question mark value failed", K(param), K(ret));
      } else if (param_idx < 0 || param_idx >= params_array.count()) {
        ret = common::OB_ERR_ILLEGAL_INDEX;
        SQL_LOG(WARN, "Wrong index of question mark position", K(ret), K(param_idx));
      } else {
        need_check = params_array.at(param_idx).need_to_check_bool_value();
        result = params_array.at(param_idx);
        if (result.is_nop_value()) {
          ret = common::OB_ERR_NOP_VALUE;
        }
      }
    }
    return ret;
  }

  static void access_expr_sanity_check(const ObIArray<ObExpr *> &exprs,
                                      ObEvalCtx &eval_ctx,
                                      int64_t check_size)
  {
    SQL_LOG(TRACE, "enable datum ptr check", K(exprs), K(check_size));
    // TODO: add sanity check for vector formats

    // auto expr_idx = 0;
    // FOREACH_CNT(e, exprs) {
    //   if (OB_ISNULL((*e)->eval_func_) &&
    //       OB_ISNULL((*e)->eval_batch_func_) && ((*e)->arg_cnt_ == 0) &&
    //       !((*e)->is_variable_res_buf() || ob_is_decimal_int((*e)->datum_meta_.type_))) {
    //     // exclude generated column, string type column
    //     auto datum = (*e)->locate_batch_datums(eval_ctx);
    //     if ((*e)->is_batch_result()) {
    //       for (auto idx = 0; idx < check_size; idx++) {
    //         const char *datum_ptr = datum[idx].ptr_;
    //         const char *res_ptr = eval_ctx.frames_[(*e)->frame_idx_] + (*e)->res_buf_off_
    //                         + (*e)->res_buf_len_ * idx;
    //         if (datum_ptr != res_ptr) {
    //           SQL_LOG_RET(WARN, OB_ERR_UNEXPECTED, "sanity check failure, column index", K(expr_idx), K(idx),
    //                                  KP(datum_ptr), KP(res_ptr), KP(*e), K(eval_ctx));
    //           abort();
    //         }
    //       }
    //     } else {
    //       const char *datum_ptr = datum->ptr_;
    //       const char *res_ptr = eval_ctx.frames_[(*e)->frame_idx_] + (*e)->res_buf_off_;
    //       if (datum_ptr != res_ptr) {
    //         SQL_LOG_RET(WARN, OB_ERR_UNEXPECTED, "sanity check failure, column index",
    //                  K(expr_idx), KP(datum_ptr), KP(res_ptr), KP(*e), K(eval_ctx));
    //         abort();
    //       }
    //     }
    //   }
    //   expr_idx++;
    // }
  }
  static int is_charset_data_version_valid(ObCharsetType charset_type, const int64_t tenant_id);
  static int is_collation_data_version_valid(ObCollationType collation_type, const int64_t tenant_id);
  static int calc_calculable_expr(ObSQLSessionInfo *session,
                                  const ObRawExpr *expr,
                                  common::ObObj &result,
                                  common::ObIAllocator *allocator,
                                  const ParamStore &params_array);
  static int calc_const_expr(ObExecContext &exec_ctx,
                             const ObRawExpr *expr,
                             common::ObObj &result,
                             common::ObIAllocator &allocator,
                             const ParamStore &params_array);
  static int calc_const_expr(ObSQLSessionInfo *session,
                             const ObRawExpr &expr,
                             ObObj &result,
                             ObIAllocator &allocator,
                             const ParamStore &params_array,
                             ObExecContext* exec_ctx = NULL);

  // Calc const expr value under static engine.
  static int se_calc_const_expr(ObSQLSessionInfo *session,
                                const ObRawExpr *expr,
                                const ParamStore &params,
                                ObIAllocator &allocator,
                                ObExecContext *exec_ctx,
                                common::ObObj &result);

  static int make_generated_expression_from_str(const common::ObString &expr_str,
                                                const share::schema::ObTableSchema &schema,
                                                const share::schema::ObColumnSchemaV2 &gen_col,
                                                const common::ObIArray<share::schema::ObColDesc> &col_ids,
                                                common::ObIAllocator &allocator,
                                                ObTempExpr *&temp_expr);
  static int make_generated_expression_from_str(const common::ObString &expr_str,
                                                ObSQLSessionInfo &session,
                                                const share::schema::ObTableSchema &schema,
                                                const share::schema::ObColumnSchemaV2 &gen_col,
                                                const common::ObIArray<share::schema::ObColDesc> &col_ids,
                                                common::ObIAllocator &allocator,
                                                ObTempExpr *&temp_expr);
  static int make_default_expr_context(uint64_t tenant_id, ObIAllocator &allocator, ObExprCtx &expr_ctx);
  static int calc_sql_expression(const ObTempExpr *expr,
                                 const ObIArray<share::schema::ObColDesc> &col_ids,
                                 const ObNewRow &row,
                                 ObExecContext &exec_ctx,
                                 ObObj &result);
  static void destruct_default_expr_context(ObExprCtx &expr_ctx);
  static int64_t get_usec();
  static int check_and_convert_db_name(const common::ObCollationType cs_type, const bool preserve_lettercase,
                                       common::ObString &name);
  static int cvt_db_name_to_org(share::schema::ObSchemaGetterGuard &schema_guard,
                                const ObSQLSessionInfo *session,
                                common::ObString &name,
                                ObIAllocator *allocator);
  static int check_and_convert_table_name(const common::ObCollationType cs_type,
                                          const bool preserve_lettercase,
                                          common::ObString &name,
                                          const bool is_oracle_mode,
                                          const stmt::StmtType stmt_type = stmt::T_NONE,
                                          const bool is_index_table = false);
  static int check_and_convert_table_name(const common::ObCollationType cs_type,
                                          const bool preserve_lettercase,
                                          common::ObString &name,
                                          const stmt::StmtType stmt_type = stmt::T_NONE,
                                          const bool is_index_table = false);
  static int check_index_name(const common::ObCollationType cs_type, common::ObString &name);
  static int check_column_name(const common::ObCollationType cs_type,
                               common::ObString &name,
                               bool is_from_view = false);
  static int check_and_copy_column_alias_name(const common::ObCollationType cs_type, const bool is_auto_gen,
                                              common::ObIAllocator *allocator, common::ObString &name);
  static int check_and_convert_context_namespace(const common::ObCollationType cs_type,
                                                 common::ObString &name);
  static bool cause_implicit_commit(ParseResult &result);
  static bool is_end_trans_stmt(const ParseResult &result);
  static bool is_commit_stmt(const ParseResult &result);
  static bool is_modify_tenant_stmt(ParseResult &result);
  static bool is_mysql_ps_not_support_stmt(const ParseResult &result);
  static bool is_readonly_stmt(ParseResult &result);
  static int make_field_name(const char *src, int64_t len, const common::ObCollationType cs_type,
                             common::ObIAllocator *allocator, common::ObString &name);
  static int set_compatible_cast_mode(const ObSQLSessionInfo *session, ObCastMode &cast_mode);
  static int get_default_cast_mode(const stmt::StmtType &stmt_type,
                                   const ObSQLSessionInfo *session,
                                   common::ObCastMode &cast_mode);
  static int get_default_cast_mode(const ObPhysicalPlan *plan,
                                   const ObSQLSessionInfo *session,
                                   common::ObCastMode &cast_mode);
  static int get_default_cast_mode(const ObSQLSessionInfo *session, common::ObCastMode &cast_mode);
  static void get_default_cast_mode(const ObSQLMode sql_mode, ObCastMode &cast_mode);
  // 比上面三个方法多了一些cast mode的设置，例如:
  // CM_EXPLICIT_CAST, CM_ZERO_FILL, CM_STRICT_MODE
  static int get_default_cast_mode(const bool is_explicit_cast,
                                    const uint32_t result_flag,
                                    const ObSQLSessionInfo *session,
                                    common::ObCastMode &cast_mode);
  static void get_default_cast_mode(const bool is_explicit_cast,
                                   const uint32_t result_flag,
                                   const stmt::StmtType &stmt_type,
                                   bool is_ignore_stmt,
                                   ObSQLMode sql_mode,
                                   ObCastMode &cast_mode);
  static void get_default_cast_mode(const stmt::StmtType &stmt_type,
                                    bool is_ignore_stmt,
                                    ObSQLMode sql_mode,
                                    ObCastMode &cast_mode);
  static int check_well_formed_str(const ObString &src_str, const ObCollationType cs_type,
                                   ObString &dst_str, bool &is_null,
                                   const bool is_strict_mode,
                                   const bool ret_error = false);
  static int check_well_formed_str(const common::ObObj &src,
                                   common::ObObj &dest,
                                   const bool is_strict_mode,
                                   const bool ret_error = false);
  static void set_insert_update_scope(common::ObCastMode &cast_mode);
  static bool is_insert_update_scope(common::ObCastMode &cast_mode);
  static int get_cast_mode_for_replace(const ObRawExpr *expr,
                                       const ObExprResType &dst_type,
                                       const ObSQLSessionInfo *session,
                                       ObCastMode &cast_mode);
  static common::ObCollationLevel transform_cs_level(const common::ObCollationLevel cs_level);
  static int set_cs_level_cast_mode(const common::ObCollationLevel cs_level,
                                    common::ObCastMode &cast_mode);
  static int get_cs_level_from_cast_mode(const common::ObCastMode cast_mode,
                                         const common::ObCollationLevel default_level,
                                         common::ObCollationLevel &cs_level);
  static int  get_outline_key(common::ObIAllocator &allocator,
                              const ObSQLSessionInfo *session,
                              const common::ObString &query_sql,
                              common::ObString &outline_key,
                              share::schema::ObMaxConcurrentParam::FixParamStore &fixed_param_store,
                              ParseMode mode,
                              bool &has_questionmark_in_sql);
  static int md5(const common::ObString &stmt, char *sql_id, int32_t len);
  static int filter_hint_in_query_sql(common::ObIAllocator &allocator,
                                      const ObSQLSessionInfo &session,
                                      const common::ObString &sql,
                                      common::ObString &param_sql);
  static int filter_head_space(ObString &sql);
  static char find_first_empty_char(const ObString &sql);
  static int construct_outline_sql(common::ObIAllocator &allocator,
                                   const ObSQLSessionInfo &session,
                                   const common::ObString &outline_content,
                                   const common::ObString &orig_sql,
                                   bool is_need_filter_hint,
                                   common::ObString &outline_sql);

  static int reconstruct_sql(ObIAllocator &allocator, const ObStmt *stmt, ObString &sql,
                             ObSchemaGetterGuard *schema_guard,
                             ObObjPrintParams print_params = ObObjPrintParams(),
                             const ParamStore *param_store = NULL,
                             const ObSQLSessionInfo *session = NULL);
  static int print_sql(char *buf,
                       int64_t buf_len,
                       int64_t &pos,
                       const ObStmt *stmt,
                       ObSchemaGetterGuard *schema_guard,
                       ObObjPrintParams print_params,
                       const ParamStore *param_store = NULL,
                       const ObSQLSessionInfo *session = NULL);

  static int wrap_expr_ctx(const stmt::StmtType &stmt_type,
                           ObExecContext &exec_ctx,
                           common::ObIAllocator &allocator,
                           common::ObExprCtx &expr_ctx);

  // use get_tablet_ranges instead.
  static int extract_pre_query_range(const ObQueryRange &pre_query_range,
                                     common::ObIAllocator &allocator,
                                     ObExecContext &exec_ctx,
                                     ObQueryRangeArray &key_ranges,
                                     const ObDataTypeCastParams &dtc_params);

  static int extract_equal_pre_query_range(const ObQueryRange &pre_query_range,
                                           void *range_buffer,
                                           const ParamStore &param_store,
                                           ObQueryRangeArray &key_ranges);
  static int extract_geo_query_range(const ObQueryRange &pre_query_range,
                                       ObIAllocator &allocator,
                                       ObExecContext &exec_ctx,
                                       ObQueryRangeArray &key_ranges,
                                       ObMbrFilterArray &mbr_filters,
                                       const ObDataTypeCastParams &dtc_params);

  static bool is_same_type(const ObExprResType &type1, const ObExprResType &type2);

  static bool is_same_type(const ObObjMeta &meta1,
                           const ObObjMeta &meta2,
                           const ObAccuracy &accuracy1,
                           const ObAccuracy &accuracy2);

  static bool is_same_type_for_compare(const ObObjMeta &meta1,
                                       const ObObjMeta &meta2);

  static int get_partition_range(ObObj *start_row_key,
  															 ObObj *end_row_key,
																 ObObj *function_obj,
																 const share::schema::ObPartitionFuncType part_type,
  															 const ObSqlExpression &part_expr,
																 int64_t range_key_count,
																 uint64_t table_id,
																 ObExprCtx &expr_ctx,
																 common::ObNewRange &partition_range);

  static int get_partition_range(ObObj *start_row_key,
                                 ObObj *end_row_key,
                                 ObObj *function_obj,
                                 const share::schema::ObPartitionFuncType part_type,
                                 const ObExpr *part_expr,
                                 int64_t range_key_count,
                                 uint64_t table_id,
                                 ObEvalCtx &eval_ctx,
                                 common::ObNewRange &partition_range,
                                 ObArenaAllocator &allocator);

  static int revise_hash_part_object(common::ObObj &obj,
                                     const ObNewRow &row,
                                     const bool calc_oracle_hash,
                                     const share::schema::ObPartitionFuncType part_type);

  static int choose_best_replica_for_estimation(
                              const ObCandiTabletLoc &phy_part_loc_info,
                              const ObAddr &local_addr,
                              const common::ObIArray<ObAddr> &addrs_list,
                              const bool no_use_remote,
                              EstimatedPartition &best_partition);

  static int choose_best_partition_replica_addr(const ObAddr &local_addr,
                                                const ObCandiTabletLoc &phy_part_loc_info,
                                                const bool is_est_block_count,
                                                ObAddr &selected_addr);

  static int has_global_index(share::schema::ObSchemaGetterGuard *schema_guard,
                              const uint64_t table_id,
                              bool &exists);

  static int wrap_column_convert_ctx(const common::ObExprCtx &expr_ctx, common::ObCastCtx &column_conv_ctx);

  static void init_type_ctx(const ObSQLSessionInfo *session, ObExprTypeCtx &type_ctx);
  static int merge_solidified_vars_into_type_ctx(ObExprTypeCtx &type_ctx,
                                                 const share::schema::ObLocalSessionVar &session_vars_snapshot);
  static int merge_solidified_var_into_dtc_params(const share::schema::ObLocalSessionVar *local_vars,
                                            const ObTimeZoneInfo *local_timezone,
                                            ObDataTypeCastParams &dtc_param);
  static int merge_solidified_var_into_sql_mode(const share::schema::ObLocalSessionVar *local_vars,
                                                ObSQLMode &sql_mode);
  static int merge_solidified_var_into_collation(const share::schema::ObLocalSessionVar &session_vars_snapshot,
                                                  ObCollationType &cs_type);
  static int merge_solidified_var_into_max_allowed_packet(const share::schema::ObLocalSessionVar *local_vars,
                                                          int64_t &max_allowed_packet);
  static int merge_solidified_var_into_compat_version(const share::schema::ObLocalSessionVar *local_vars,
                                                      uint64_t &compat_version);

  static bool is_oracle_sys_view(const ObString &table_name);

  static int make_whole_range(ObIAllocator &allocator,
                              const uint64_t ref_table_id,
                              const int64_t rowkey_count,
                              ObNewRange *&whole_range);

  static int make_whole_range(ObIAllocator &allocator,
                              const uint64_t ref_table_id,
                              const int64_t rowkey_count,
                              ObNewRange &whole_range);
  static int get_ext_obj_data_type(const ObObjParam &obj, ObDataType &data_type);

  static int ConvertFiledNameAttribute(common::ObIAllocator& allocator,
                                       const common::ObString &src,
                                       common::ObString &dst,
                                       const uint16_t collation_type);

  static int clear_evaluated_flag(const ObExprPtrIArray &calc_exprs, ObEvalCtx &eval_ctx);

  static int copy_and_convert_string_charset(common::ObIAllocator& allocator,
      const common::ObString &src,
      common::ObString &dst,
      common::ObCollationType src_coll,
      common::ObCollationType dst_coll);
  static int update_session_last_schema_version(
      share::schema::ObMultiVersionSchemaService &schema_service,
      ObSQLSessionInfo &session_info
      );
  static int generate_new_name_with_escape_character(
          common::ObIAllocator &allocator,
          const common::ObString &src,
          common::ObString &dest,
          bool is_oracle_mode);
  static int check_table_version(bool &equal,
            const DependenyTableStore &dependency_tables,
            share::schema::ObSchemaGetterGuard &schema_guard);

  static int generate_view_definition_for_resolve(common::ObIAllocator &allocator,
                                                  common::ObCollationType connection_collation,
                                                  const share::schema::ObViewSchema &view_schema,
                                                  common::ObString &view_definition);
  static void record_execute_time(const ObPhyPlanType type,
                                  const int64_t time_cost);
  static int handle_audit_record(bool need_retry,
                                 const ObExecuteMode exec_mode,
                                 ObSQLSessionInfo &session,
                                 bool is_sensitive = false);

  // convert escape char from '\\' to '\\\\';
  static int convert_escape_char(common::ObIAllocator &allocator,
                                 const ObString &in,
                                 ObString &out);
  //检查参数是否为Oracle模式下的''
  static bool is_oracle_empty_string(const common::ObObjParam &param);
  static int convert_sql_text_from_schema_for_resolve(common::ObIAllocator &allocator,
                                                    const common::ObDataTypeCastParams &dtc_params,
                                                    ObString &sql_text,
                                                    int64_t convert_flag = 0,
                                                    int64_t *action_flag = NULL);
  static int convert_sql_text_to_schema_for_storing(common::ObIAllocator &allocator,
                                                    const common::ObDataTypeCastParams &dtc_params,
                                                    ObString &sql_text,
                                                    int64_t convert_flag = 0,
                                                    int64_t *action_flag = NULL);

  static int print_identifier(char *buf, const int64_t buf_len, int64_t &pos,
                              common::ObCollationType connection_collation,
                              const common::ObString &identifier_name,
                              bool is_oracle_mode);
  static bool is_one_part_table_can_skip_part_calc(const share::schema::ObTableSchema &schema);

  static int create_encode_sortkey_expr(ObRawExprFactory &expr_factory,
                                        ObExecContext* exec_ctx,
                                        const common::ObIArray<OrderItem> &order_keys,
                                        int64_t start_key,
                                        OrderItem &encode_sortkey);
  static ObItemType get_sql_item_type(const ParseResult &result);
  static bool is_enable_explain_batched_multi_statement();
  static bool is_support_batch_exec(ObItemType type);
  static bool is_pl_nested_sql(ObExecContext *cur_ctx);
  static bool is_fk_nested_sql(ObExecContext *cur_ctx);
  static bool is_online_stat_gathering_nested_sql(ObExecContext *cur_ctx);
  static bool is_iter_uncommitted_row(ObExecContext *cur_ctx);
  static bool is_nested_sql(ObExecContext *cur_ctx);
  static bool is_in_autonomous_block(ObExecContext *cur_ctx);
  static bool is_select_from_dual(ObExecContext &ctx);

  static int get_obj_from_ext_obj(const ObObjParam &ext_obj, int64_t pos, ObObj *&obj);
  static int get_result_from_ctx(ObExecContext &exec_ctx,
                                 const ObRawExpr *expr,
                                 ObObj &result,
                                 bool &is_valid,
                                 bool &hit_cache);
  static int store_result_to_ctx(ObExecContext &exec_ctx,
                                 const ObRawExpr *raw_expr,
                                 const ObObj &result,
                                 bool is_valid);
  static int add_calc_failure_constraint(const ObRawExpr *raw_expr,
                                         ObIArray<ObExprConstraint> &constraints);
  static int create_multi_stmt_param_store(common::ObIAllocator &allocator,
                                           int64_t query_num,
                                           int64_t param_num,
                                           ParamStore &param_store);
  static int transform_pl_ext_type(ParamStore &src, int64_t array_binding_size, ObIAllocator &alloc, ParamStore *&dst, bool is_forall = false);
  static int get_one_group_params(int64_t &pos, ParamStore &src, ParamStore &obj_params);
  static int copy_params_to_array_params(int64_t query_pos, ParamStore &src, ParamStore &dst, ObIAllocator &alloc, bool is_forall = false);
  static int init_elements_info(ParamStore &src, ParamStore &dst);
  /*-----------------------
  *  Observer no longer depends on Linux NTP service to adjust server time since 4.0.
  *  So, timestamp between servers can be vary large.
  *  A sql executed across servers needs to be corrected
  *  according to the THIS_WORKER.get_ntp_offset(),
  *  That is the time correctly set by the processor of the RPC
  ------------------------*/
  static void adjust_time_by_ntp_offset(int64_t &dst_timeout_ts);

  static int split_remote_object_storage_url(common::ObString &url, share::ObBackupStorageInfo &storage_info);
  static bool is_external_files_on_local_disk(const common::ObString &url);
  static int check_location_access_priv(const common::ObString &location, ObSQLSessionInfo *session);

#ifdef OB_BUILD_SPM
  static int handle_plan_baseline(const ObAuditRecordData &audit_record,
                                  ObPhysicalPlan *plan,
                                  const int ret_code,
                                  ObSqlCtx &sql_ctx);
#endif
  static int async_recompile_view(const share::schema::ObTableSchema &old_view_schema,
                                  ObSelectStmt *select_stmt,
                                  bool reset_column_infos,
                                  common::ObIAllocator &alloc,
                                  sql::ObSQLSessionInfo &session_info);
  static int check_sys_view_changed(const share::schema::ObTableSchema &old_view_schema,
                                    const share::schema::ObTableSchema &new_view_schema,
                                    bool &changed);
  static int find_synonym_ref_obj(const ObString &database_name,
                                  const ObString &object_name,
                                  const uint64_t tenant_id,
                                  bool &exist,
                                  uint64_t &object_id,
                                  share::schema::ObObjectType &obj_type,
                                  uint64_t &schema_version);
  static int find_synonym_ref_obj(const uint64_t database_id,
                                  const ObString &object_name,
                                  const uint64_t tenant_id,
                                  bool &exist,
                                  uint64_t &object_id,
                                  share::schema::ObObjectType &obj_type,
                                  uint64_t &schema_version);
  static bool check_need_disconnect_parser_err(const int ret_code);
  static bool check_json_expr(ObItemType type);

  static int print_identifier_require_quotes(ObCollationType collation_type,
                                             const ObString &ident,
                                             bool &require);

  static int64_t get_next_ts(int64_t &old_ts) {
    int64_t next_ts = common::OB_INVALID_TIMESTAMP;

    while (true) {
      int64_t origin_ts = ATOMIC_LOAD(&old_ts);
      int64_t now = ObClockGenerator::getClock();
      next_ts = (now > origin_ts) ? now : (origin_ts + 1);
      if (origin_ts == ATOMIC_VCAS(&old_ts, origin_ts, next_ts)) {
        break;
      } else {
        PAUSE();
      }
    };
    return next_ts;
  }
  static int64_t combine_server_id(int64_t ts, uint64_t server_id) {
    return (ts & ((1LL << 43) - 1LL)) | ((server_id & 0xFFFF) << 48);
  }
  static int check_ident_name(const common::ObCollationType cs_type, common::ObString &name,
                              const bool check_for_path_char, const int64_t max_ident_len);

  static int compatibility_check_for_mysql_role_and_column_priv(uint64_t tenant_id);
  static bool is_data_version_ge_422_or_431(uint64_t data_version);
  static bool is_data_version_ge_423_or_431(uint64_t data_version);
  static bool is_data_version_ge_423_or_432(uint64_t data_version);

  static int get_proxy_can_activate_role(const ObIArray<uint64_t> &role_id_array,
                                            const ObIArray<uint64_t> &role_id_option_array,
                                            const ObProxyInfo &proxied_info,
                                            ObIArray<uint64_t> &new_role_id_array,
                                            ObIArray<uint64_t> &new_role_id_option_array);
private:
  static bool check_mysql50_prefix(common::ObString &db_name);
  static bool part_expr_has_virtual_column(const ObExpr *part_expr);
  static int get_range_for_vector(
                                  ObObj *start_row_key,
                                  ObObj *end_row_key,
                                  int64_t range_key_count,
                                  uint64_t table_id,
                                  common::ObNewRange &part_range);
  static int get_partition_range_common(
                                    ObObj *function_obj,
                                    const share::schema::ObPartitionFuncType part_type,
                                    const ObExpr *part_expr,
                                    ObEvalCtx &eval_ctx);
  static int get_range_for_scalar(ObObj *start_row_key,
                                  ObObj *end_row_key,
                                  ObObj *function_obj,
                                  const share::schema::ObPartitionFuncType part_type,
                                  const ObExpr *part_expr,
                                  int64_t range_key_count,
                                  uint64_t table_id,
                                  ObEvalCtx &eval_ctx,
                                  common::ObNewRange &part_range,
                                  ObArenaAllocator &allocator);
  struct SessionInfoCtx
  {
    common::ObCollationType collation_type_;
    common::ObTimeZoneInfo tz_info_;
  };
}; // end of ObSQLUtils

class ObSqlGeoUtils
{
public:
  static int check_srid_by_srs(uint64_t tenant_id, uint64_t srid);
  static int check_srid(uint32_t column_srid, uint32_t input_srid);
};

class RelExprCheckerBase
{
public:
  RelExprCheckerBase()
      : duplicated_checker_()
  {
  }
  virtual ~RelExprCheckerBase()
  {
    duplicated_checker_.destroy();
  }
  virtual int init(int64_t bucket_num = CHECKER_BUCKET_NUM);
  virtual int add_expr(ObRawExpr *&expr) = 0;
  int add_exprs(common::ObIArray<ObRawExpr*> &exprs);

protected:
  static const int64_t CHECKER_BUCKET_NUM = 1000;
  common::hash::ObHashSet<uint64_t, common::hash::NoPthreadDefendMode> duplicated_checker_;
};


class RelExprChecker : public RelExprCheckerBase
{
public:
  RelExprChecker(common::ObIArray<ObRawExpr*> &rel_array)
      : RelExprCheckerBase(), rel_array_(rel_array)
  {
  }

  virtual ~RelExprChecker() {}
  int add_expr(ObRawExpr *&expr);
private:
  common::ObIArray<ObRawExpr*> &rel_array_;
};

class FastRelExprChecker : public RelExprCheckerBase
{
public:
  FastRelExprChecker(common::ObIArray<ObRawExpr *> &rel_array);
  virtual ~FastRelExprChecker();
  int add_expr(ObRawExpr *&expr);
  int dedup();
private:
  common::ObIArray<ObRawExpr *> &rel_array_;
  int64_t init_size_;
};

class RelExprPointerChecker : public RelExprCheckerBase
{
public:
  RelExprPointerChecker(common::ObIArray<ObRawExprPointer> &rel_array)
      : RelExprCheckerBase(), rel_array_(rel_array), expr_id_map_()
  {
  }
  virtual ~RelExprPointerChecker() {}
  virtual int init(int64_t bucket_num = CHECKER_BUCKET_NUM) override;
  int add_expr(ObRawExpr *&expr);
private:
  common::ObIArray<ObRawExprPointer> &rel_array_;
  common::hash::ObHashMap<uint64_t, uint64_t, common::hash::NoPthreadDefendMode> expr_id_map_;
};

class AllExprPointerCollector : public RelExprCheckerBase
{
public:
  AllExprPointerCollector(common::ObIArray<ObRawExpr**> &rel_array)
      : RelExprCheckerBase(), rel_array_(rel_array)
  {
  }
  virtual ~AllExprPointerCollector() {}
  int add_expr(ObRawExpr *&expr);
private:
  common::ObIArray<ObRawExpr**> &rel_array_;
};

class FastUdtExprChecker : public RelExprCheckerBase
{
public:
  FastUdtExprChecker(common::ObIArray<ObRawExpr *> &rel_array);
  virtual ~FastUdtExprChecker() {}
  int add_expr(ObRawExpr *&expr);
  int dedup();
private:
  common::ObIArray<ObRawExpr *> &rel_array_;
  int64_t init_size_;
};

class JsonObjectStarChecker : public RelExprCheckerBase
{
public:
  JsonObjectStarChecker(common::ObIArray<ObRawExpr *> &rel_array);
  virtual ~JsonObjectStarChecker() {}
  int add_expr(ObRawExpr *&expr);
private:
  common::ObIArray<ObRawExpr *> &rel_array_;
  int64_t init_size_;
};

struct ObSqlTraits
{
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];// sql id //最后一个字节存放'\0'
  bool is_readonly_stmt_;
  bool is_modify_tenant_stmt_;
  bool is_cause_implicit_commit_;
  bool is_commit_stmt_;
  ObItemType stmt_type_;

  ObSqlTraits() : is_readonly_stmt_(false),
                  is_modify_tenant_stmt_(false),
                  is_cause_implicit_commit_(false),
                  is_commit_stmt_(false),
                  stmt_type_(T_INVALID)
  {
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  }

  void reset() {
    sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
    is_readonly_stmt_ = false;
    is_modify_tenant_stmt_ = false;
    is_cause_implicit_commit_ = false;
    is_commit_stmt_ = false;
    stmt_type_ = T_INVALID;
  }
   TO_STRING_KV(K(is_readonly_stmt_),
                K(is_modify_tenant_stmt_),
                K(is_cause_implicit_commit_),
                K(is_commit_stmt_),
                K(stmt_type_));
};

template<typename ValueType>
class ObValueChecker
{
public:

  ObValueChecker() = delete;

  constexpr ObValueChecker(ValueType min_value, ValueType max_value, int err_ret_code)
          : min_value_(min_value), max_value_(max_value), err_ret_code_(err_ret_code)
  {}

  constexpr inline int validate(const ValueType &value) const {
    return (value < min_value_ || value > max_value_) ? err_ret_code_ : common::OB_SUCCESS;
  }

  constexpr inline int validate_str_length(const common::ObString &value) const {
    return (value.length() < min_value_ || value.length() > max_value_) ? err_ret_code_ : common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(min_value), K_(max_value), K_(err_ret_code));
private:
  ValueType min_value_;
  ValueType max_value_;
  int err_ret_code_;
};

template<typename ValueType>
class ObPointerChecker
{
public:

    ObPointerChecker() = delete;

    constexpr ObPointerChecker(int err_ret_code)
            : err_ret_code_(err_ret_code)
    {}

    constexpr inline int validate(const ValueType *value) const {
        return (nullptr == value) ? err_ret_code_ : common::OB_SUCCESS;
    }

    TO_STRING_KV(K_(err_ret_code));
private:
    int err_ret_code_;
};

//用uint64_t存储的flag集合，最多有63个flag
//T是enum类型
// enum class A {
//    F1,
//    F2,
//    F3,
//    MAX_VALUE,
// };
// ObEnumBitSet<A> flag_set;
//
template <typename T>
class ObEnumBitSet {
  static const int MAX_ENUM_VALUE = 64;
  static_assert(std::is_enum<T>::value, "typename must be a enum type");
  static_assert(static_cast<int>(T::MAX_VALUE) < MAX_ENUM_VALUE,
                "Please add MAX_VALUE in enum class");
public:
  inline ObEnumBitSet() : flag_(0) {}
  inline ObEnumBitSet(T value) { set_bit(value); }
  inline void reset() { flag_ = 0; }
  inline void set_bit(T bit_pos) {
    flag_ |= bit2flag(static_cast<int>(bit_pos));
  }
  inline bool test_bit(T bit_pos) const {
    return !!((flag_) & bit2flag(static_cast<int>(bit_pos)));
  }
  inline bool contain(const ObEnumBitSet &other) const {
    return (this & other) == other;
  }
  ObEnumBitSet<T>& operator |= (ObEnumBitSet<T> other)
  {
    this->flag_ |= other.flag_;
    return *this;
  }
  ObEnumBitSet<T>& operator &= (ObEnumBitSet<T> other)
  {
    this->flag_ &= other.flag_;
    return *this;
  }
  ObEnumBitSet<T> operator | (ObEnumBitSet<T> other) const
  {
    return ObEnumBitSet<T>(this->flag_ | other.flag_);
  }
  ObEnumBitSet<T> operator & (ObEnumBitSet<T> other) const
  {
    return ObEnumBitSet<T>(this->flag_ & other.flag_);
  }
  TO_STRING_KV(K_(flag));

private:
  inline uint64_t bit2flag(int bit) const {
    uint64_t v = 1;
    return v<<bit;
  }
  uint64_t flag_;
  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER_TEMP(template<typename T>, ObEnumBitSet<T>, flag_);

//隐式游标信息
struct ObImplicitCursorInfo
{
  OB_UNIS_VERSION(1);
public:
  ObImplicitCursorInfo()
  : stmt_id_(common::OB_INVALID_INDEX),
    affected_rows_(0),
    found_rows_(0),
    matched_rows_(0),
    duplicated_rows_(0),
    deleted_rows_(0) {}

  int merge_cursor(const ObImplicitCursorInfo &other);
  TO_STRING_KV(K_(stmt_id),
               K_(affected_rows),
               K_(found_rows),
               K_(matched_rows),
               K_(duplicated_rows),
               K_(deleted_rows));

  int64_t stmt_id_;
  int64_t affected_rows_;
  int64_t found_rows_;
  int64_t matched_rows_;
  int64_t duplicated_rows_;
  int64_t deleted_rows_;
};


enum ObThreeStageAggrStage {
  NONE_STAGE,
  FIRST_STAGE,
  SECOND_STAGE,
  THIRD_STAGE,
};

enum ObRollupStatus {
  NONE_ROLLUP,          // no rollup
  ROLLUP_NORMAL,        // normal rollup
  ROLLUP_DISTRIBUTOR,   // rollup distributor
  ROLLUP_COLLECTOR,     // rollup collector
};

class ObVirtualTableResultConverter
{
public:
  ObVirtualTableResultConverter()
    : key_alloc_(nullptr), key_cast_ctx_(), output_row_types_(nullptr), key_types_(nullptr),
      output_row_alloc_(), init_alloc_(nullptr), inited_row_(false), convert_row_(), output_row_cast_ctx_(),
      base_table_id_(UINT64_MAX), cur_tenant_id_(UINT64_MAX), table_schema_(nullptr), output_column_ids_(nullptr),
      cols_schema_(), tenant_id_col_id_(UINT64_MAX), max_col_cnt_(INT64_MAX),
      has_tenant_id_col_(false), tenant_id_col_idx_(-1)
  {}
  ~ObVirtualTableResultConverter() { destroy(); }

  void destroy();
  int reset_and_init(
      ObIAllocator *key_alloc,
      sql::ObSQLSessionInfo *session,
      const common::ObIArray<ObObjMeta> *output_row_types,
      const common::ObIArray<ObObjMeta> *key_types,
      ObIAllocator *init_alloc,
      const share::schema::ObTableSchema *table_schema,
      const common::ObIArray<uint64_t> *output_column_ids,
      const bool has_tenant_id_col,
      const int64_t tenant_id_col_idx,
      const int64_t max_col_cnt = INT64_MAX);
  int init_without_allocator(
    const common::ObIArray<const share::schema::ObColumnSchemaV2*> &col_schemas,
        uint64_t cur_tenant_id,
        uint64_t tenant_id_col_id);
  int convert_key_ranges(ObIArray<ObNewRange> &key_ranges);
  int convert_output_row(ObNewRow *&src_row);
  int convert_output_row(ObEvalCtx &eval_ctx,
                        const common::ObIArray<ObExpr*> &src_exprs,
                        const common::ObIArray<ObExpr*> &dst_exprs);
  int convert_column(ObObj &obj, uint64_t column_id, uint64_t idx);
  //attention!!!, following function is inited to convert range only, can't do other operator.
  int init_convert_key_ranges_info(
      ObIAllocator *key_alloc,
      sql::ObSQLSessionInfo *session,
      const share::schema::ObTableSchema *table_schema,
      const common::ObIArray<ObObjMeta> *key_types,
      const bool has_tenant_id_col,
      const int64_t tenant_id_col_idx);
private:
  int convert_key(const ObRowkey &src, ObRowkey &dst, bool is_start_key, int64_t pos);
  int get_all_columns_schema();
  int init_output_row(int64_t cell_cnt);
  int get_need_convert_key_ranges_pos(ObNewRange &key_range, int64_t &pos);
public:
  // the memory that allocated must be reset by caller
  ObIAllocator *key_alloc_;
  ObCastCtx key_cast_ctx_;
  const common::ObIArray<ObObjMeta> *output_row_types_;
  const common::ObIArray<ObObjMeta> *key_types_;
  // every row must be reset
  common::ObArenaAllocator output_row_alloc_;
  // for init output row, lifecycle is from open to close(called destroy)
  common::ObIAllocator *init_alloc_;
  bool inited_row_;
  common::ObNewRow convert_row_;
  ObCastCtx output_row_cast_ctx_;
  uint64_t base_table_id_;
  uint64_t cur_tenant_id_;
  const share::schema::ObTableSchema *table_schema_;
  const common::ObIArray<uint64_t> *output_column_ids_;
  common::ObArray<const share::schema::ObColumnSchemaV2 *> cols_schema_;
  uint64_t tenant_id_col_id_;
  int64_t max_col_cnt_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  bool has_tenant_id_col_;
  int64_t tenant_id_col_idx_;
};

class ObSqlFatalErrExtraInfoGuard : public common::ObFatalErrExtraInfoGuard
{
public:
  ObSqlFatalErrExtraInfoGuard() { reset(); }
  ~ObSqlFatalErrExtraInfoGuard() {};
  void reset() {
    tenant_id_ = OB_INVALID_TENANT_ID;
    cur_sql_.reset();
    plan_ = nullptr;
    exec_ctx_ = nullptr;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id;}
  void set_cur_sql(const common::ObString &cur_sql) { cur_sql_ = cur_sql; }
  void set_cur_plan(const ObPhysicalPlan *plan) { plan_ = plan; }
  void set_exec_context(ObExecContext *exec_ctx) { exec_ctx_ = exec_ctx; }
  DECLARE_TO_STRING;
private:
  uint64_t tenant_id_;
  common::ObString cur_sql_;
  const ObPhysicalPlan *plan_;
  ObExecContext *exec_ctx_;
};

enum PreCalcExprExpectResult {
  PRE_CALC_RESULT_NONE, // not init
  PRE_CALC_RESULT_NULL,
  PRE_CALC_RESULT_NOT_NULL,
  PRE_CALC_RESULT_TRUE,
  PRE_CALC_RESULT_FALSE,
  PRE_CALC_RESULT_NO_WILDCARD,
  PRE_CALC_ERROR,
  PRE_CALC_PRECISE,
  PRE_CALC_NOT_PRECISE,
  PRE_CALC_ROWID,
  PRE_CALC_LOSSLESS_CAST, // only used in rewrite, will be converted to cast(expr, type) = expr
};

struct ObExprConstraint
{
  ObExprConstraint() :
      pre_calc_expr_(NULL),
      expect_result_(PRE_CALC_RESULT_NONE),
      ignore_const_check_(false) {}
  ObExprConstraint(ObRawExpr *expr, PreCalcExprExpectResult expect_result) :
      pre_calc_expr_(expr),
      expect_result_(expect_result),
      ignore_const_check_(false) {}
  bool operator==(const ObExprConstraint &rhs) const;
  ObRawExpr *pre_calc_expr_;
  PreCalcExprExpectResult expect_result_;
  bool ignore_const_check_;
  TO_STRING_KV(KP_(pre_calc_expr),
               K_(expect_result),
               K_(ignore_const_check));
};

struct ObPreCalcExprConstraint : public common::ObDLinkBase<ObPreCalcExprConstraint>
{
  public:
    ObPreCalcExprConstraint(common::ObIAllocator &allocator):
      pre_calc_expr_info_(allocator),
      expect_result_(PRE_CALC_RESULT_NONE)
    {
    }
    virtual int assign(const ObPreCalcExprConstraint &other, common::ObIAllocator &allocator);
    virtual int check_is_match(const ObObjParam &obj_param, bool &is_match) const;
    ObPreCalcExprFrameInfo pre_calc_expr_info_;
    PreCalcExprExpectResult expect_result_;
};

struct ObRowidConstraint : public ObPreCalcExprConstraint
{
  public:
    ObRowidConstraint(common::ObIAllocator &allocator):
      ObPreCalcExprConstraint(allocator),
      rowid_version_(0),
      rowid_type_array_(allocator)
    {
    }
    virtual int assign(const ObPreCalcExprConstraint &other,
                       common::ObIAllocator &allocator) override;
    virtual int check_is_match(const ObObjParam &obj_param, bool &is_match) const override;
    uint8_t rowid_version_;
    ObFixedArray<ObObjType, common::ObIAllocator> rowid_type_array_;
};

//this guard use to link the exec context and session
class LinkExecCtxGuard
{
public:
  LinkExecCtxGuard(ObSQLSessionInfo &session, ObExecContext &exec_ctx)
    : session_(session),
      exec_ctx_(exec_ctx),
      is_linked_(false),
      is_autocommit_(false)
  {
    link_current_context();
  }
  ~LinkExecCtxGuard()
  {
    unlink_current_context();
  }
private:
  //bind current execute context with my session,
  //in order to access exec_ctx of the current statement through session
  void link_current_context();
  //unlink current execute context with my session to
  //forbid accessing this sql exec context through session
  //usually at the end of the sql execution
  void unlink_current_context();
private:
  ObSQLSessionInfo &session_;
  ObExecContext &exec_ctx_;
  bool is_linked_;
  bool is_autocommit_;
};

}
}

#endif
