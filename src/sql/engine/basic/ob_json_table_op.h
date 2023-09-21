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
 * This file contains interface support for the json table abstraction.
 */

#ifndef OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "lib/charset/ob_charset.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_path.h"
#include "lib/json_type/ob_json_bin.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{
struct JtScanCtx;
static const int64_t DEFAULT_STR_LENGTH = -1;
class ObExpr;
struct JtScanCtx;
class ObJsonTableSpec;

typedef enum JtJoinType {
  LEFT_TYPE,
  RIGHT_TYPE,
} JtJoinType;

typedef enum JtNodeType {
  REG_TYPE, // ordinality && reg type
  JOIN_TYPE, // join node
  SCAN_TYPE, // scan node
} JtNodeType;


typedef struct ObJtColInfo
{
  ObJtColInfo();
  ObJtColInfo(const ObJtColInfo& info);

  int32_t col_type_;
  int32_t truncate_;
  int32_t format_json_;
  int32_t wrapper_;
  int32_t allow_scalar_;
  int64_t output_column_idx_;
  int64_t empty_expr_id_;
  int64_t error_expr_id_;
  ObString col_name_;
  ObString path_;
  int32_t on_empty_;
  int32_t on_error_;
  int32_t on_mismatch_;
  int32_t on_mismatch_type_;
  int64_t res_type_;
  ObDataType data_type_;
  int32_t parent_id_;
  int32_t id_;

  int from_JtColBaseInfo(const ObJtColBaseInfo& info);
  int deep_copy(const ObJtColInfo& src, ObIAllocator* allocator);
  int serialize(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  TO_STRING_KV(K_(col_type), K_(format_json), K_(wrapper), K_(allow_scalar),
   K_(output_column_idx), K_(col_name), K_(path), K_(parent_id), K_(id));
} ObJtColInfo;

class JtColNode
{
public:
  JtColNode(const ObJtColInfo& col_info)
    : node_idx_(-1),
      total_(0),
      cur_pos_(0),
      in_(nullptr),
      curr_(nullptr),
      iter_(nullptr),
      js_path_(nullptr),
      is_evaled_(false),
      is_sub_evaled_(false),
      is_ord_node_(false),
      is_null_result_(false),
      is_nested_evaled_(false),
      is_emp_evaled_(false),
      is_err_evaled_(false),
      emp_val_(nullptr),
      err_val_(nullptr),
      emp_datum_(nullptr),
      err_datum_(nullptr) {
      new (&col_info_) ObJtColInfo(col_info);
      node_type_ = REG_TYPE;
      is_ord_node_ = col_info_.col_type_ == COL_TYPE_ORDINALITY;
    }
  virtual void destroy();
  JtColType type() { return static_cast<JtColType>(col_info_.col_type_); }
  JtNodeType node_type() { return node_type_; }
  void set_node_type(JtNodeType type) { node_type_ = type; }
  const ObJtColInfo& get_column_node_def() { return col_info_; }
  ObJtColInfo& get_column_def() { return col_info_; }
  bool is_ord_node() { return is_ord_node_;  }
  bool is_null_result() { return is_null_result_; }
  int init_js_path(JtScanCtx* ctx);
  void set_idx(int64_t idx) { node_idx_ = idx; }
  int special_proc_on_input_type(char* buf, size_t len, ObString& res);
  int check_default_cast_allowed(ObExpr* expr);
  int check_col_res_type(JtScanCtx* ctx);
  int set_val_on_empty(JtScanCtx* ctx, bool& need_cast_res);
  int set_val_on_empty_mysql(JtScanCtx* ctx, bool& need_cast_res);
  int process_default_value_pre_mysql(JtScanCtx* ctx);
  int wrapper2_json_array(JtScanCtx* ctx, ObJsonBaseVector &hit);
  int get_default_value_pre_mysql(ObExpr* default_expr,
                                  JtScanCtx* ctx,
                                  ObIJsonBase *&res,
                                  ObObjType &dst_type);
  int64_t node_idx() { return node_idx_; }
  virtual int open();
  virtual int get_next_row(ObIJsonBase* in, JtScanCtx* ctx, bool& is_null_value);


  void proc_query_on_error(JtScanCtx *ctx, int& err_code, bool& is_null);


  // fixed member
  int64_t node_idx_;
  JtNodeType node_type_;
  ObJtColInfo col_info_;

  /**
   * changable member
  */
  int32_t total_;
  int32_t cur_pos_;

  ObIJsonBase* in_;
  ObIJsonBase* curr_;
  ObIJsonBase* iter_;
  ObJsonPath *js_path_;

  bool is_evaled_;
  bool is_sub_evaled_;
  bool is_ord_node_;
  bool is_null_result_;
  bool is_nested_evaled_;
  bool is_emp_evaled_;
  bool is_err_evaled_;
  ObIJsonBase *emp_val_;
  ObIJsonBase *err_val_;
  int32_t ord_val_;
  ObDatum *emp_datum_;
  ObDatum *err_datum_;

  TO_STRING_KV(K_(node_type),
               K_(node_idx),
               K_(total),
               K_(cur_pos),
               K_(ord_val),
               K_(is_ord_node),
               K_(is_evaled),
               K_(is_sub_evaled),
               K_(node_type),
               K_(col_info));

};

class JtJoinNode : public JtColNode
{
public:
  JtJoinNode(const ObJtColInfo& col_info)
    : JtColNode(col_info),
      join_type_(LEFT_TYPE),
      left_idx_(-1),
      right_idx_(-1),
      left_(nullptr),
      right_(nullptr) {
        node_type_ = JOIN_TYPE;
      }
  void destroy();
  int open();
  int get_next_row(ObIJsonBase* in, JtScanCtx* ctx, bool& is_null_value);
  void set_join_type(JtJoinType join_type) { join_type_ = join_type; }
  void set_left(JtColNode* node) {
    left_ = node;
    left_idx_ = node->node_idx();
  }
  void set_right(JtColNode* node) {
    right_ = node;
    right_idx_ = node->node_idx();
  }
  JtColNode* left() { return left_; }
  JtColNode* right() { return right_; }
  JtColNode** left_addr() { return &left_; }
  JtColNode** right_addr() { return &right_; }
  JtJoinType get_join_type() { return join_type_; }
  int64_t left_idx() { return left_idx_; }
  int64_t right_idx() { return left_idx_; }

  TO_STRING_KV(K_(node_type),
               K_(node_idx),
               K_(join_type),
               K_(left_idx),
               K_(right_idx),
               KP_(left),
               KP_(right));

  JtJoinType join_type_;
  int64_t left_idx_;
  int64_t right_idx_;
  JtColNode *left_;
  JtColNode *right_;
};

class JtScanNode : public JtColNode
{
public:
  JtScanNode(const ObJtColInfo& col_info)
    : JtColNode(col_info),
      is_regular_done_(false),
      is_nested_done_(false),
      reg_col_defs_(),
      nest_col_def_(nullptr) {
        node_type_ = SCAN_TYPE;
      }
  void destroy();
  int open();
  int get_next_row(ObIJsonBase* in, JtScanCtx* ctx, bool& is_null_value);
  int assign(const JtScanNode& other);
  int add_reg_column_node(JtColNode* node, bool add_idx = false);
  int add_nest_column_node(JtColNode* node, bool add_idx = false) {
    nest_col_def_ = node;
    return add_idx ? child_idx_.push_back(node->node_idx()) : OB_SUCCESS;
  }
  size_t reg_column_count() { return reg_col_defs_.count(); }
  JtColNode* nest_col_node() { return nest_col_def_; }
  JtColNode* reg_col_node(size_t i) { return reg_col_defs_.at(i); }
  ObIArray<int64_t>& child_node_ref() { return child_idx_; }
  void reset_reg_columns(JtScanCtx* ctx);

  TO_STRING_KV(K_(node_type),
              K_(node_idx),
              K_(is_regular_done),
              K_(is_nested_done),
              KP_(nest_col_def),
              K(reg_col_defs_.count()));

  bool is_regular_done_;
  bool is_nested_done_;
  common::ObSEArray<JtColNode*, 4, common::ModulePageAllocator, true> reg_col_defs_;
  JtColNode* nest_col_def_;

  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> child_idx_;
};


class JtFuncHelpler
{
public:
  static int cast_to_res(JtScanCtx* ctx, ObIJsonBase* js_val, JtColNode& col_info, bool enable_error);
  static int cast_json_to_res(JtScanCtx* ctx, ObIJsonBase* js_val, JtColNode& col_node, ObDatum& res, bool enable_error = true);
  static int cast_to_json(common::ObIAllocator *allocator, ObIJsonBase *j_base, ObString &val);
  static int cast_to_bit(ObIJsonBase *j_base, uint64_t &val, common::ObAccuracy &accuracy);
  static int bit_length_check(const ObAccuracy &accuracy, uint64_t &value);
  static int cast_to_number(common::ObIAllocator *allocator,
                            ObIJsonBase *j_base,
                            common::ObAccuracy &accuracy,
                            ObObjType dst_type,
                            number::ObNumber &val);
  static int cast_to_double(ObIJsonBase *j_base, ObObjType dst_type, double &val);
  static int cast_to_float(ObIJsonBase *j_base, ObObjType dst_type, float &val);
  static int cast_to_year(ObIJsonBase *j_base, uint8_t &val);
  static int cast_to_time(ObIJsonBase *j_base, common::ObAccuracy &accuracy, int64_t &val);
  static int cast_to_date(ObIJsonBase *j_base, int32_t &val);
  static int cast_to_datetime(JtColNode* node,
                              ObIJsonBase *j_base,
                              common::ObIAllocator *allocator,
                              const ObBasicSessionInfo *session,
                              common::ObAccuracy &accuracy,
                              int64_t &val);
  static int cast_to_otimstamp(ObIJsonBase *j_base,
                               const ObBasicSessionInfo *session,
                               common::ObAccuracy &accuracy,
                               ObObjType dst_type,
                               ObOTimestampData &out_val);
  static bool type_cast_to_string(JtColNode* node,
                                  ObString &json_string,
                                  common::ObIAllocator *allocator,
                                  ObIJsonBase *j_base,
                                  ObAccuracy &accuracy);
  static int cast_to_string(JtColNode* node,
                            common::ObIAllocator *allocator,
                            ObIJsonBase *j_base,
                            ObCollationType in_cs_type,
                            ObCollationType dst_cs_type,
                            common::ObAccuracy &accuracy,
                            ObObjType dst_type,
                            ObString &val,
                            bool is_trunc = false,
                            bool is_quote = false,
                            bool is_const = false);
  static int padding_char_for_cast(int64_t padding_cnt, const ObCollationType &padding_cs_type,
                                    ObIAllocator &alloc, ObString &padding_res);
  static int time_scale_check(const ObAccuracy &accuracy, int64_t &value, bool strict = false);
  static int datetime_scale_check(const ObAccuracy &accuracy, int64_t &value, bool strict = false);
  static int number_range_check(const ObAccuracy &accuracy,
                                ObIAllocator *allocator,
                                number::ObNumber &val,
                                bool strict = false);
  static int check_default_val_accuracy(const ObAccuracy &accuracy,
                                        const ObObjType &type,
                                        const ObDatum *obj);
  static int check_default_value(JtScanCtx* ctx,
                                 ObJtColInfo &col_info_,
                                 ObExpr* col_expr);
  static int cast_to_uint(ObIJsonBase *j_base, ObObjType dst_type, uint64_t &val);
  static int cast_to_int(ObIJsonBase *j_base, ObObjType dst_type, int64_t &val);

  static int set_error_val(JtScanCtx* ctx, JtColNode& col_info, int& ret);
  static int set_error_val_mysql(JtScanCtx* ctx, JtColNode& col_info, int& ret);
  static int check_default_value_inner(JtScanCtx* ctx,
                                       ObJtColInfo &col_info,
                                       ObExpr* col_expr,
                                       ObExpr* default_expr);
  static int pre_default_value_check_mysql(JtScanCtx* ctx,
                                          ObIJsonBase* js_val,
                                          JtColNode& col_node);
};

struct JtColTreeNode {
  JtColTreeNode(const ObJtColInfo& info)
    : col_base_info_(info),
      regular_cols_(),
      nested_cols_() {}
  void destroy();
  ObJtColInfo col_base_info_;
  common::ObSEArray<JtColTreeNode*, 4, common::ModulePageAllocator, true> regular_cols_;
  common::ObSEArray<JtColTreeNode*, 4, common::ModulePageAllocator, true> nested_cols_;

  TO_STRING_KV(K_(col_base_info));
} ;

class ObJsonTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObJsonTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      value_expr_(nullptr),
      column_exprs_(alloc),
      emp_default_exprs_(alloc),
      err_default_exprs_(alloc),
      has_correlated_expr_(false),
      alloc_(&alloc),
      cols_def_(alloc) {}

  int dup_origin_column_defs(common::ObIArray<ObJtColBaseInfo*>& columns);
  int construct_tree(common::ObArray<JtColNode*> all_nodes, JtScanNode* parent);
  int construct_tree(common::ObArray<JtColNode*> all_nodes, JtJoinNode* parent);

  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_; // 列输出表达式
  common::ObFixedArray<ObExpr*, common::ObIAllocator> emp_default_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> err_default_exprs_;
  bool has_correlated_expr_; //是否是变量输入，用在算子rescan中，同function table
  ObIAllocator* alloc_;

  common::ObFixedArray<ObJtColInfo*, common::ObIAllocator> cols_def_;
};

class ObJsonTableOp;

struct JtScanCtx {
    JtScanCtx()
      : row_alloc_(),
        op_exec_alloc_(nullptr) {}

  ObJsonTableSpec* spec_ptr_;
  ObEvalCtx* eval_ctx_;
  ObExecContext* exec_ctx_;
  common::ObArenaAllocator row_alloc_;
  ObIAllocator *op_exec_alloc_;
  ObJsonTableOp* jt_op_;

  bool is_evaled_;
  bool is_cover_error_;
  bool is_need_end_;
  bool is_charset_converted_;
  bool is_const_input_;
  int error_code_;
  int32_t ord_val_;
  ObDatum* res_obj_;
  ObDatum** data_;
  char buf[OB_MAX_DECIMAL_PRECISION];

};

class ObJsonTableOp : public ObOperator
{
public:
  ObJsonTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      def_root_(nullptr),
      jt_root_(nullptr),
      allocator_(&exec_ctx.get_allocator()),
      is_inited_(false),
      is_evaled_(false),
      in_(nullptr),
      j_null_(),
      j_arr_(allocator_),
      j_obj_(allocator_)
  {
    const ObJsonTableSpec* spec_ptr = reinterpret_cast<const ObJsonTableSpec*>(&spec);
    col_count_ = spec_ptr->column_exprs_.count();
  }

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  //virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  ObJsonNull* get_js_null() { return &j_null_; }
  ObJsonArray* get_js_array() { return &j_arr_; }
  ObJsonObject* get_js_object() { return &j_obj_; }
  ObIJsonBase* get_root_param() { return in_; }
  JtScanNode* get_root_entry() { return jt_root_; }
  TO_STRING_KV(K_(is_inited),
               K_(col_count));

private:
  int init();
  int init_data_obj(); // allocate data_ array
  void reset_columns();
  int generate_column_trees(JtColTreeNode*& root);
  int find_column(int32_t id, JtColTreeNode* root, JtColTreeNode*& col);
  int generate_table_exec_tree();
  int generate_table_exec_tree(ObIAllocator* allocator, const JtColTreeNode& orig_col,
                               JtScanNode*& scan_col, int64_t& node_idx);

private:
  JtColTreeNode* def_root_;
  JtScanNode* jt_root_;
  common::ObIAllocator *allocator_;
  uint32_t col_count_;
  bool is_inited_;
  bool is_evaled_;
  ObIJsonBase* in_;
  JtScanCtx jt_ctx_;

private:
  ObJsonNull j_null_;
  ObJsonArray j_arr_;
  ObJsonObject j_obj_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_ */