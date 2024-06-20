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
#include "sql/engine/expr/ob_json_param_type.h"
#include "sql/engine/expr/ob_expr_json_utils.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "deps/oblib/src/lib/xml/ob_xml_util.h"

namespace oceanbase
{
namespace sql
{
struct JtScanCtx;
class ObExpr;
struct JtScanCtx;
class ObJsonTableSpec;
class MulModeTableFunc;
class JoinNode;
class ObRegCol;
class ScanNode;
class UnionNode;
class ObMultiModeTableNode;

enum table_type : int8_t {
  OB_INVALID_TABLE = 0,
  OB_JSON_TABLE = 1,
  OB_XML_TABLE = 2,
};

typedef enum JtNodeType {
  REG_TYPE, // ordinality && reg type
  SCAN_TYPE, // scan node type
  JOIN_TYPE, // join node
  UNION_TYPE, // scan node
} JtNodeType;

typedef enum ResultType {
  NOT_DATUM,
  EMPTY_DATUM,
  ERROR_DATUM
} ResultType;

typedef enum ScanType{  // symbol of execute stage, scan node or col node
  SCAN_NODE_TYPE,
  COL_NODE_TYPE,
} ScanType;

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
      cols_def_(alloc),
      table_type_(MulModeTableType::OB_ORA_JSON_TABLE_TYPE),
      namespace_def_(alloc) {}

  int dup_origin_column_defs(common::ObIArray<ObJtColBaseInfo*>& columns);
  int construct_tree(common::ObArray<ObMultiModeTableNode*> all_nodes, JoinNode* parent);
  int construct_tree(common::ObArray<ObMultiModeTableNode*> all_nodes, UnionNode* parent);
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_; // 列输出表达式
  common::ObFixedArray<ObExpr*, common::ObIAllocator> emp_default_exprs_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> err_default_exprs_;
  bool has_correlated_expr_; //是否是变量输入，用在算子rescan中，同function table
  ObIAllocator* alloc_;

  common::ObFixedArray<ObJtColInfo*, common::ObIAllocator> cols_def_;
  MulModeTableType table_type_;
  common::ObFixedArray<ObString, common::ObIAllocator> namespace_def_;
};

class ObJsonTableOp;

struct JtScanCtx {
    JtScanCtx()
      : row_alloc_(),
        op_exec_alloc_(nullptr),
        context(nullptr),
        table_func_(nullptr),
        default_ns() {}

  bool is_xml_table_func() {
    return spec_ptr_->table_type_ == OB_ORA_XML_TABLE_TYPE;
  }
  bool is_json_table_func() {
    return spec_ptr_->table_type_ == OB_ORA_JSON_TABLE_TYPE;
  }


  ObJsonTableSpec* spec_ptr_;
  ObEvalCtx* eval_ctx_;
  ObExecContext* exec_ctx_;
  common::ObArenaAllocator row_alloc_;
  ObMulModeMemCtx *mem_ctx_;
  ObMulModeMemCtx *xpath_ctx_;
  ObIAllocator *op_exec_alloc_;
  ObJsonTableOp* jt_op_;
  void *context;      // xml ns prefix_ns
  MulModeTableFunc *table_func_;
  ObString default_ns;

  bool is_evaled_;
  bool is_cover_error_;
  bool is_need_end_; // parse input json doc fail will affect by column
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
      allocator_(&exec_ctx.get_allocator()),
      is_inited_(false),
      is_evaled_(false),
      root_(nullptr),
      input_(nullptr),
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
  void* get_root_param() { return input_; }
  JoinNode* get_root_entry() { return root_; }
  TO_STRING_KV(K_(is_inited),
               K_(col_count));

private:
  int init();
  int reset_variable();  // reset var in rescan
  int init_data_obj(); // allocate data_ array
  int generate_column_trees(JtColTreeNode*& root);
  int find_column(int32_t id, JtColTreeNode* root, JtColTreeNode*& col);
  int generate_table_exec_tree();
  int generate_table_exec_tree(ObIAllocator* allocator, const JtColTreeNode& orig_col,
                               JoinNode*& join_col);
  int inner_get_next_row_jt();  // json table function inner

private:
  JtColTreeNode* def_root_;
  common::ObIAllocator *allocator_;
  uint32_t col_count_;
  bool is_inited_;
  bool is_evaled_;
  JoinNode* root_;  // mul mode table
  JtScanCtx jt_ctx_;

public:
  void *input_;   // mul mode table
  void reset_columns();

private:
  ObJsonNull j_null_;
  ObJsonArray j_arr_;
  ObJsonObject j_obj_;
};

class MulModeTableFunc {
public:
  MulModeTableFunc() {}
  ~MulModeTableFunc() {}

  virtual int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx) { return 0; } // init variable before seek value
  virtual int eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum) { return 0; }  // eval default value in inner open
  virtual int container_at(void* in, void *&out, int32_t pos) { return 0; }
  virtual int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx) { return 0; }  // get root input
  virtual int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value) { return 0; }
  virtual int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value) { return 0; };
  virtual int eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res) { return 0; }
  virtual int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx) { return 0; }
  virtual int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr) { return 0; }
  virtual int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null) { return 0; }
  virtual int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret) { return 0; }
  virtual int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true) { return 0; }
  virtual int set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx) { return 0; }
  virtual int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx) { return 0; } // init variable before seek value
};

class JsonTableFunc : public MulModeTableFunc {
public:
  JsonTableFunc()
  : MulModeTableFunc() {}
  ~JsonTableFunc() {}

  int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  int eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum);
  int container_at(void* in, void *&out, int32_t pos);
  int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);
  int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  int eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res);
  int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx);
  int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr);
  int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null);
  int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret);
  int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true);
  int set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx);
  int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx) { return OB_SUCCESS; }  // reset variable if variable is not const
};

class XmlTableFunc : public MulModeTableFunc {
public:
  XmlTableFunc()
  : MulModeTableFunc() {}
  ~XmlTableFunc() {}

  int init_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);
  int eval_default_value(JtScanCtx*& ctx, ObExpr*& default_expr, void*& res, bool need_datum);
  int container_at(void* in, void *&out, int32_t pos);
  int eval_input(ObJsonTableOp &jt, JtScanCtx& ctx, ObEvalCtx &eval_ctx);  // 获取跟节点输入 xml 解析namespace
  int reset_path_iter(ObRegCol &scan_node, void* in, JtScanCtx*& ctx, ScanType init_flag, bool &is_null_value);
  int get_iter_value(ObRegCol &col_node, JtScanCtx* ctx, bool &is_null_value);
  int eval_seek_col(ObRegCol &col_node, void* in, JtScanCtx* ctx, bool &is_null_value, bool &need_cast_res);
  int col_res_type_check(ObRegCol &col_node, JtScanCtx* ctx);
  int check_default_value(JtScanCtx* ctx, ObRegCol &col_node, ObExpr* expr);
  int set_on_empty(ObRegCol& col_node, JtScanCtx* ctx, bool &need_cast, bool& is_null);
  int set_on_error(ObRegCol& col_node, JtScanCtx* ctx, int& ret);
  int cast_to_result(ObRegCol& col_node, JtScanCtx* ctx, bool enable_error = false, bool is_pack_result = true);
  int set_expr_exec_param(ObRegCol& col_node, JtScanCtx* ctx) { return 0; }
  int reset_ctx(ObRegCol &scan_node, JtScanCtx*& ctx);  // reset variable if variable is not const
};

class ObMultiModeTableNode {
public:
  ObMultiModeTableNode()
    : in_(nullptr),
      is_evaled_(false),
      is_null_result_(false) {
      node_type_ = REG_TYPE;
    }
  virtual void destroy();
  virtual int reset(JtScanCtx* ctx) {
    UNUSED(ctx);
    is_evaled_ = false;
    is_null_result_ = false;
    return OB_SUCCESS;
  }
  JtNodeType node_type() { return node_type_; }
  virtual int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value) { return 0; }
  bool is_null_result() { return is_null_result_; }
  virtual int open(JtScanCtx* ctx) {
    return ObMultiModeTableNode::reset(ctx);
  }
  virtual void* get_curr_iter_value() { return nullptr; }

  JtNodeType node_type_;
  void* in_;
  bool is_evaled_;  // 节点是否被解析过
  bool is_null_result_;  // 节点是否返回空值

  TO_STRING_KV(K_(node_type),
               K_(is_evaled),
               K_(node_type));
};

class ObRegCol final
{
public:
  ObRegCol(const ObJtColInfo& col_info)
    : total_(0),
      path_(nullptr),
      curr_(nullptr),
      iter_(nullptr),
      cur_pos_(-1),
      ord_val_(-1),
      is_path_evaled_(false),
      is_emp_evaled_(false),
      is_err_evaled_(false),
      emp_val_(nullptr),
      err_val_(nullptr),
      res_flag_(NOT_DATUM),
      expr_param_() {
      new (&col_info_) ObJtColInfo(col_info);
      node_type_ = REG_TYPE;
      is_ord_node_ = col_info_.col_type_ == COL_TYPE_ORDINALITY;
    }
  void destroy();
  int reset(JtScanCtx* ctx);
  bool is_ord_node() { return is_ord_node_;  }
  const ObJtColInfo& get_column_node_def() { return col_info_; }
  ObJtColInfo& get_column_def() { return col_info_; }
  int open(JtScanCtx* ctx);
  int eval_regular_col(void *in, JtScanCtx* ctx, bool& is_null_value); // process regular column
  JtColType type() { return static_cast<JtColType>(col_info_.col_type_); }
  JtNodeType node_type() { return node_type_; }

  int32_t total_;
  void *path_;
  void* curr_;
  void* iter_;
  int32_t cur_pos_;
  int32_t ord_val_;
  bool is_path_evaled_;
  ObJtColInfo col_info_;
  bool is_ord_node_;
  bool is_emp_evaled_;
  bool is_err_evaled_;
  void *emp_val_;
  void *err_val_;
  // data in curr_ is datum type, not json/xml ,0 is not datum, 1 is empty datum, 2 is error datum.
  ResultType res_flag_;
  MulModeTableType tab_type_; // distinct xml path or json path , due to not common root
  JtNodeType node_type_;  // distinct regcol in scan node or regcol
  ObJsonExprParam expr_param_;
  TO_STRING_KV(K_(node_type),
               K_(cur_pos),
               K_(ord_val),
               K_(is_ord_node),
               K_(node_type),
               K_(col_info));

};

class ScanNode : public ObMultiModeTableNode
{
  public:
  ScanNode(const ObJtColInfo& col_info)
    : ObMultiModeTableNode(),
      reg_col_defs_(),
      child_idx_(),
      seek_node_(col_info)
      {
        new (&seek_node_) ObRegCol(col_info);
        seek_node_.node_type_ = SCAN_TYPE;
        node_type_ = SCAN_TYPE;
      }
  void destroy();
  int reset(JtScanCtx* ctx);
  int open(JtScanCtx* ctx);
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  int get_next_iter(void* in, JtScanCtx* ctx, bool& is_null_value);
  int assign(const ScanNode& other);
  int add_reg_column_node(ObRegCol* node, bool add_idx = false);
  size_t reg_column_count() { return reg_col_defs_.count(); }
  ObRegCol* reg_col_node(size_t i) { return reg_col_defs_.at(i); }
  ObIArray<int64_t>& child_node_ref() { return child_idx_; }
  void reset_reg_columns(JtScanCtx* ctx);
  void* get_curr_iter_value() {
    return seek_node_.iter_;
  }

  TO_STRING_KV(K_(node_type),
              K(reg_col_defs_.count()));
  common::ObSEArray<ObRegCol*, 4, common::ModulePageAllocator, true> reg_col_defs_;
  common::ObSEArray<int64_t, 4, common::ModulePageAllocator, true> child_idx_;
  ObRegCol seek_node_;
};

class UnionNode : public ObMultiModeTableNode
{
public:
  UnionNode()
    : ObMultiModeTableNode(), // useless
      left_(nullptr),
      right_(nullptr),
      is_left_iter_end_(false),
      is_right_iter_end_(true) {
        node_type_ = UNION_TYPE;
      }
  void destroy();
  int reset(JtScanCtx* ctx);
  int open(JtScanCtx* ctx);
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  void set_left(ObMultiModeTableNode* node) {
    left_ = node;
  }
  void set_right(ObMultiModeTableNode* node) {
    right_ = node;
  }
  ObMultiModeTableNode* left() { return left_; }
  ObMultiModeTableNode* right() { return right_; }
  void* get_curr_iter_value() {
    return left_->get_curr_iter_value();
  }

  TO_STRING_KV(K_(node_type),
               KP_(left),
               KP_(right));

  ObMultiModeTableNode *left_;
  ObMultiModeTableNode *right_;
  bool is_left_iter_end_;    // judge left child whether evaled_,
  bool is_right_iter_end_;  // judge right child whether evaled_,
};

class JoinNode : public UnionNode
{
  public:
  JoinNode()
    : UnionNode()
      {}
  int get_next_row(void* in, JtScanCtx* ctx, bool& is_null_value);
  int assign(const JoinNode& other);
  ScanNode* get_scan_node() { return dynamic_cast<ScanNode*>(left_); }
  // void* get_curr_iter_value() { return get_scan_node()->get_curr_iter_value(); }

  TO_STRING_KV(K_(node_type));
};

class RegularCol final{
public:
  static int eval_query_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_value_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_exist_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr, bool& is_null);
  static int eval_xml_scalar_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr);
  static int eval_xml_type_col(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* col_expr);
  static void proc_query_on_error(JtScanCtx* ctx, ObRegCol &col_node, int& ret, bool& is_null);
  static int check_default_val_cast_allowed(JtScanCtx* ctx, ObMultiModeTableNode &col_node, ObExpr* expr)  { return 0; }  // check type of default value
  static int set_val_on_empty(JtScanCtx* ctx, ObRegCol &col_node, bool& need_cast_res, bool& is_null);
  static bool check_cast_allowed(const ObObjType orig_type,
                                const ObCollationType orig_cs_type,
                                const ObObjType expect_type,
                                const ObCollationType expect_cs_type,
                                const bool is_explicit_cast);
  static int check_default_value_oracle(JtScanCtx* ctx, ObJtColInfo &col_info, ObExpr* expr);
  static int check_default_value_inner_oracle(JtScanCtx* ctx,
                                              ObJtColInfo &col_info,
                                              ObExpr* col_expr,
                                              ObExpr* default_expr);
  static int check_item_method_json(ObRegCol &col_node, JtScanCtx* ctx);
  static int check_default_value_inner_mysql(JtScanCtx* ctx,
                                             ObRegCol &col_node,
                                             ObExpr* default_expr,
                                             ObExpr* expr,
                                             ObIJsonBase* j_base);
  static int parse_default_value_2json(ObExpr* default_expr,
                                       JtScanCtx* ctx,
                                       ObDatum*& tmp_datum,
                                       ObIJsonBase *&res);
  static int check_default_value_mysql(ObRegCol &col_node, JtScanCtx* ctx, ObExpr* expr);
};


} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_JSON_TABLE_OP_H_ */