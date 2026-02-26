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

#ifndef DEV_SRC_SQL_DAS_OB_CALC_GENERATED_COLUMN_UTILS_H_
#define DEV_SRC_SQL_DAS_OB_CALC_GENERATED_COLUMN_UTILS_H_
#include "sql/das/ob_das_define.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_resolver_utils.h"
namespace oceanbase
{
namespace sql
{

struct ObCalcGeneratedColumnInfo
{
public:
  ~ObCalcGeneratedColumnInfo() = default;
  ObCalcGeneratedColumnInfo(common::ObIAllocator &alloc)
    : column_define_(),
      table_id_(),
      column_id_(),
      column_flags_(),
      col_name_(),
      index_position_(),
      rowkey_position_(),
      srs_id_(),
      sub_type_(),
      accuracy_(),
      udt_set_id_(),
      meta_type_(),
      extended_type_info_(),
      flags_(0),
      is_generated_column_(false),
      dependcy_column_infos_(),
      dependcy_row_(nullptr),
      local_session_vars_(&alloc)
  { }

  TO_STRING_KV(K_(column_define),
               K_(table_id),
               K_(column_id),
               K_(index_position),
               K_(rowkey_position),
               K_(srs_id),
               K_(accuracy),
               K_(col_name),
               K_(extended_type_info),
               K_(dependcy_column_infos));

  inline common::ObCollationType get_collation_type() const { return meta_type_.get_collation_type(); }
  const ObCalcGeneratedColumnInfo *get_dependcy_column_by_column_id(int64_t column_id) const
  {
    int ret = OB_SUCCESS;
    bool founded = false;
    ObCalcGeneratedColumnInfo *dependcy_column_info = nullptr;
    for (int64_t i = 0; !founded && i < dependcy_column_infos_.count(); i++) {
      if (dependcy_column_infos_.at(i)->column_id_ == column_id) {
        dependcy_column_info = dependcy_column_infos_.at(i);
      }
    }
    return dependcy_column_info;
  }

  bool is_xmltype() const {
    return ((meta_type_.is_ext() || meta_type_.is_user_defined_sql_type()) && sub_type_ == T_OBJ_XML)
           || meta_type_.is_xml_sql_type();
  }
  bool is_enum_or_set() const { return meta_type_.is_enum_or_set(); }

  int32_t get_data_length() const
  {
    return (ob_is_accuracy_length_valid_tc(meta_type_.get_type()) ?
        accuracy_.get_length() : ob_obj_type_size(meta_type_.get_type()));
  }

  inline common::ColumnType get_data_type() const { return meta_type_.get_type(); }
  bool has_column_flag(int64_t flag) const { return column_flags_ & flag; }

  ObString column_define_;
  uint64_t table_id_;
  uint64_t column_id_;
  int64_t column_flags_;
  common::ObString col_name_; // 用于关联虚拟生成列
  uint64_t index_position_;
  uint64_t rowkey_position_;
  uint64_t srs_id_; // 不知道这玩意是干啥的，但是看到有，暂时先记下
  uint64_t sub_type_;
  common::ObAccuracy accuracy_;
  uint64_t udt_set_id_;
  common::ObObjMeta meta_type_;
  common::ObArrayHelper<common::ObString> extended_type_info_; // for enum or set type
  union
  {
    uint32_t flags_;
    struct
    {
      // is_update_属性貌似没用,trigger中update_columns_是通过ObDASUpdCtDef.updated_column_infos_
      // 中的column_name进行初始化的
      uint32_t is_autoincrement_:1;
      uint32_t is_not_null_for_read_:1;
      uint32_t is_not_null_for_write_:1;
      uint32_t is_not_null_validate_column_:1;
      uint32_t is_rowkey_column_:1;
      uint32_t is_index_column_:1;
      uint32_t is_zero_fill_:1;
      uint32_t is_hidden_:1;
      uint32_t reserved_:26;
    };
  };
  bool is_generated_column_;
  common::ObSEArray<ObCalcGeneratedColumnInfo *, 4> dependcy_column_infos_;
  ObNewRow *dependcy_row_; // 依赖列的所有的数据
  ObLocalSessionVar local_session_vars_;
};

class ObCalcGeneratedColumn
{
public:
  ObCalcGeneratedColumn()
    : inner_alloc_(),
      long_life_alloc_(),
      tz_info_map_(),
      inner_session_(nullptr),
      expr_factory_(nullptr),
      all_local_session_vars_(),
      sql_ctx_(),
      exec_ctx_(nullptr)
  {
  }

  ~ObCalcGeneratedColumn() { destroy(); }
  int init(); // 初始化session等数据结构
  int init_session_info();
  uint32_t calc_column_result_flag(const ObCalcGeneratedColumnInfo &generated_column_info);

  int found_column_ref_expr(const ObString &column_name, bool &founded, ObColumnRefRawExpr *&ref_expr);

  int found_dependcy_column_info(const ObCalcGeneratedColumnInfo &generated_column_info,
                                 const ObString &column_name,
                                 bool &founded,
                                 ObCalcGeneratedColumnInfo *&dependcy_column_info);
  int add_local_session_vars(ObIAllocator *alloc, const ObLocalSessionVar &local_session_var, int64_t &idx);

  static int build_pad_expr_recursively(ObRawExprFactory &expr_factory,
                                 const ObSQLSessionInfo &session,
                                 const ObCalcGeneratedColumnInfo &generated_column_info,
                                 ObRawExpr *&expr,
                                 const ObLocalSessionVar *local_vars,
                                 int64_t local_var_id);

  static int build_padding_expr(ObRawExprFactory &expr_factory,
                                const ObSQLMode sql_mode,
                                const sql::ObSQLSessionInfo *session_info,
                                const ObCalcGeneratedColumnInfo *generated_column_info,
                                ObRawExpr *&expr,
                                const ObLocalSessionVar *local_vars,
                                int64_t local_var_id);

  static int build_pad_expr(ObRawExprFactory &expr_factory,
                            bool is_char,
                            const ObCalcGeneratedColumnInfo *generated_column_info,
                            ObRawExpr *&expr,
                            const sql::ObSQLSessionInfo *session_info,
                            const ObLocalSessionVar *local_vars,
                            int64_t local_var_id);

  static int build_trim_expr(const ObCalcGeneratedColumnInfo *generated_column_info,
                      ObRawExprFactory &expr_factory,
                      const ObSQLSessionInfo *session_info,
                      ObRawExpr *&expr,
                      const ObLocalSessionVar *local_vars,
                      int64_t local_var_id);

  int resolve_collation_from_solidified_var(ObCalcGeneratedColumnInfo &generated_column_info,
                                            ObCollationType &cs_type);

  int resolve_sql_mode_from_solidified_var(ObCalcGeneratedColumnInfo &generated_column_info,
                                           ObSQLMode &sql_mode);

  int build_generated_column_expr_for_cdc(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo &session_info,
                                          ObSQLMode sql_mode,
                                          ObCollationType cs_type,
                                          ObCalcGeneratedColumnInfo &generated_column_info,
                                          ObColumnRefRawExpr *generated_column_expr,
                                          RowDesc &row_desc,
                                          ObRawExpr *&generated_ref_expr);
  int resolve_generated_column_expr(ObCalcGeneratedColumnInfo &generated_column_info,
                                    RowDesc &row_desc,
                                    ObColumnRefRawExpr *&generated_column_expr,
                                    ObRawExpr *&generated_ref_expr);
  int calc_generated_columns();
  int init_column_ref_expr(const ObCalcGeneratedColumnInfo *column_info,
                           const ObSQLSessionInfo &session_info,
                           ObColumnRefRawExpr *&column_expr);
  int print_diff_variables(ObLocalSessionVar &local_vars);
  int calc_generated_column(ObCalcGeneratedColumnInfo &column_info, common::ObIAllocator &alloc, ObTempExpr *&temp_expr, ObObj &result_obj);
  int calc_generated_column(ObTempExpr *temp_expr, const ObNewRow &row, ObObj &result_obj);
  int create_ctx_before_calc();
  int init_column_expr_subschema(const ObCalcGeneratedColumnInfo &column_info,
                                 const ObSQLSessionInfo *session_info,
                                 ObColumnRefRawExpr &column_expr);

  void reuse();
  common::ObArenaAllocator inner_alloc_;
  common::ObArenaAllocator long_life_alloc_;
  ObTZInfoMap tz_info_map_;
  sql::ObSQLSessionInfo *inner_session_;
  ObRawExprFactory *expr_factory_;
  common::ObSArray<ObLocalSessionVar, common::ModulePageAllocator, true> all_local_session_vars_;
  common::ObSEArray<ObColumnRefRawExpr *, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> dependcy_column_exprs_;
  ObSqlCtx sql_ctx_;
  ObExecContext *exec_ctx_;
private:
  void destroy();
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_DAS_OB_CALC_GENERATED_COLUMN_UTILS_H_ */
