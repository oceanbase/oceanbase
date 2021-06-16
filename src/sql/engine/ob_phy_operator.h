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

#ifndef OCEANBASE_SQL_OB_PHY_OPERATOR_H
#define OCEANBASE_SQL_OB_PHY_OPERATOR_H
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/utility.h"
#include "share/ob_i_sql_expression.h"
#include "share/schema/ob_schema_struct.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/px/ob_px_op_size_factor.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/ob_sql_define.h"

#define DECLARE_PHY_OPERATOR_ASSIGN virtual int assign(const ObPhyOperator* other)

namespace oceanbase {
namespace common {
class ObIAllocator;
}
}  // namespace oceanbase

namespace oceanbase {
namespace sql {
class ObPhysicalPlan;
class ObSqlExpression;
template <class T>
class ObSqlExpressionNode;
class ObExecContext;
class ObColumnExpression;
class ObPhyOperator;
class ObTaskInfo;
class ObPhyOperatorVisitor;
enum OperatorOpenOrder {
  OPEN_CHILDREN_FIRST = 0,  // default open children first
  OPEN_SELF_FIRST = 1,
  OPEN_CHILDREN_LATER = 2,
  OPEN_SELF_LATER = 3,
  OPEN_SELF_ONLY = 4,
  OPEN_NONE = 5,
  OPEN_EXIT = 6
};
struct ObPhyOpSeriCtx {
public:
  ObPhyOpSeriCtx() : row_id_list_(NULL), exec_ctx_(NULL)
  {}
  const void* row_id_list_;
  const ObExecContext* exec_ctx_;
};
/**
 * @brief the interface of physical operator input param class,
 * each specified physical operator input param class must implement it
 * @note each specified physical operator input param class must be register in
 * function ObExecContext::alloc_operator_input_by_type() after it was defined
 * please use the micro REGISTER_PHY_OPERATOR_INPUT(input_class, op_type) to register it
 */
class ObIPhyOperatorInput {
  OB_UNIS_VERSION_PV();

public:
  ObIPhyOperatorInput()
  {}
  virtual ~ObIPhyOperatorInput()
  {}
  /**
   * brief fill info into input before sending it to dest svr for execution
   */
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op) = 0;
  /**
   * @brief every specified operator input parameter must correspond with it's physical operator
   * this function to get the operator type corresponding with the operator input parameter
   * @return the physical operator type
   */
  virtual ObPhyOperatorType get_phy_op_type() const = 0;
  /**
   * @brief set allocator which is used for deserialize, but not all objects will use allocator
   * while deserializing, so you can override it if you need.
   */
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator)
  {
    UNUSED(allocator);
  }
  // for compatibility
  virtual bool need_serialized() const
  {
    return true;
  }
  virtual void reset() = 0;
};

struct ColumnContent {
  OB_UNIS_VERSION(1);

public:
  ColumnContent()
      : projector_index_(0),
        auto_filled_timestamp_(false),
        is_nullable_(false),
        is_implicit_(false),
        column_name_(),
        column_type_(common::ObNullType),
        coll_type_(common::CS_TYPE_INVALID)
  //  column_id_(common::OB_INVALID_ID)
  {}

  TO_STRING_KV(N_INDEX, projector_index_, N_AUTO_FILL_TIMESTAMP, auto_filled_timestamp_, N_NULLABLE, is_nullable_,
      "implicit", is_implicit_, N_COLUMN_NAME, column_name_, K_(column_type), K_(coll_type));
  //               N_COLUMN_ID, column_id_);

  uint64_t projector_index_;
  bool auto_filled_timestamp_;
  bool is_nullable_;
  bool is_implicit_;
  common::ObString column_name_;       // only for error message.
  common::ObObjType column_type_;      // column data type
  common::ObCollationType coll_type_;  // column collation type
  //  uint64_t column_id_;
};

struct ObForeignKeyColumn {
  OB_UNIS_VERSION(1);

public:
  ObForeignKeyColumn() : name_(), idx_(-1), name_idx_(-1)
  {}
  inline void reset()
  {
    name_.reset();
    idx_ = -1;
    name_idx_ = -1;
  }
  TO_STRING_KV(N_COLUMN_NAME, name_, N_INDEX, idx_, N_INDEX, name_idx_);
  common::ObString name_;
  int32_t idx_;  // index of the column id in column_ids_ of ObTableModify. value column idx
  int32_t name_idx_;
};

class ObForeignKeyArg {
  OB_UNIS_VERSION(1);

public:
  ObForeignKeyArg()
      : ref_action_(share::schema::ACTION_INVALID), database_name_(), table_name_(), columns_(), is_self_ref_(false)
  {}
  inline void reset()
  {
    ref_action_ = share::schema::ACTION_INVALID;
    database_name_.reset();
    table_name_.reset();
    columns_.reset();
  }
  TO_STRING_KV(K_(ref_action), K_(database_name), K_(table_name), K_(columns), K_(is_self_ref));

public:
  share::schema::ObReferenceAction ref_action_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObSEArray<ObForeignKeyColumn, 4> columns_;
  bool is_self_ref_;
};

// record schema info for each operator
// such as for Sort operator, it's type of sort columns.
class ObOpSchemaObj {
  OB_UNIS_VERSION(1);

public:
  ObOpSchemaObj() : obj_type_(common::ObMaxType), is_not_null_(false), order_type_(default_asc_direction())
  {}
  ObOpSchemaObj(common::ObObjType obj_type)
      : obj_type_(obj_type), is_not_null_(false), order_type_(default_asc_direction())
  {}
  ObOpSchemaObj(common::ObObjType obj_type, ObOrderDirection order_type)
      : obj_type_(obj_type), is_not_null_(false), order_type_(order_type)
  {}

  bool is_null_first() const
  {
    return NULLS_FIRST_ASC == order_type_ || NULLS_FIRST_DESC == order_type_;
  }

  bool is_ascending_direction() const
  {
    return NULLS_FIRST_ASC == order_type_ || NULLS_LAST_ASC == order_type_;
  }
  TO_STRING_KV(K_(obj_type), K_(is_not_null), K_(order_type));

public:
  common::ObObjType obj_type_;
  bool is_not_null_;
  ObOrderDirection order_type_;
};

class ObPhyOperator {
public:
  static const int32_t FIRST_CHILD = 0;
  static const int32_t SECOND_CHILD = 1;
  static const int32_t THRID_CHIID = 2;
  static const int32_t FOURTH_CHILD = 3;
  class ObPhyOperatorCtx {
  public:
    static const int64_t BULK_COUNT = 500;  // get 500 rows when get_next_rows is called
  public:
    explicit ObPhyOperatorCtx(ObExecContext& ctx)
        : exec_ctx_(ctx),
          calc_mem_(NULL),
          calc_buf_(NULL),
          cur_row_(),
          cur_rows_(NULL),
          expr_ctx_(),
          is_filtered_(false),
          is_filtered_has_set_(false),
          exch_drained_(false),
          got_first_row_(false),
          op_monitor_info_(),
          check_times_(0),
          index_(0),
          row_count_(0),
          column_count_(0),
          projector_(NULL),
          projector_size_(0)
    {}
    virtual ~ObPhyOperatorCtx()
    {
      destroy_base();
    }

    // @note we don't call the destructor but this destroy() to free the resources for the performance purpose
    virtual void destroy() = 0;
    /**
     * @brief reset operator context to the state after call init_op_ctx()
     */
    /**
     * @brief create an common::ObObj array as cur row
     * @param column_count[in], the size of a row
     * @return if success, return OB_SUCCESS, otherwise, return errno
     */
    inline int create_cur_row(int64_t column_count, int32_t* projector, int64_t projector_size)
    {
      int ret = common::OB_SUCCESS;
      if (common::OB_SUCCESS == (ret = alloc_row_cells(column_count, cur_row_))) {
        cur_row_.projector_ = projector;
        cur_row_.projector_size_ = projector_size;
      }
      return ret;
    }

    /**
     * @brief create row_count common::ObObj array as cur rows
     * @param column_count[in], the size of a row
     * @return if success, return OB_SUCCESS, otherwise, return errno
     */
    int create_cur_rows(int64_t row_count, int64_t column_count, int32_t* projector, int64_t projector_size);

    /**
     * @brief create an common::ObObj array as row
     * @param column_count[in], the size of a row
     * @param row[out], the row space will be created
     * @return if success, return OB_SUCCESS, otherwise, return errno
     */
    int alloc_row_cells(const int64_t column_count, common::ObNewRow& row);

    /**
     * @brief create row_count common::ObObj array as rows
     * @param column_count[in], the size of a row
     * @param rows[out], the rows space will be created
     * @return if success, return OB_SUCCESS, otherwise, return errno
     */
    int alloc_rows_cells(const int64_t row_count, const int64_t column_count, common::ObNewRow* rows);

    /**
     * @brief materialize current row cells
     * @param row[in], storaged row
     * @return if success, return OB_SUCCESS, otherwise, return errno
     */
    inline int store_cur_row(const common::ObNewRow& row)
    {
      int ret = common::OB_SUCCESS;
      if (row.is_invalid() || cur_row_.is_invalid() || row.count_ > cur_row_.count_ || OB_ISNULL(calc_buf_)) {
        ret = common::OB_INVALID_ARGUMENT;
        SQL_ENG_LOG(WARN, "invalid argument", K_(row.cells), K_(row.count), K_(cur_row_.count));
      }
      for (int64_t i = 0; common::OB_SUCCESS == ret && i < row.count_; ++i) {
        SQL_ENG_LOG(WARN, "invalid argument", K_(row.cells), K_(row.count), K_(cur_row_.count));
      }
      for (int64_t i = 0; common::OB_SUCCESS == ret && i < row.count_; ++i) {
        if (OB_FAIL(ob_write_obj(*calc_buf_, row.cells_[i], cur_row_.cells_[i]))) {
          SQL_ENG_LOG(WARN, "write object failed", K(ret), "cell", row.cells_[i]);
        }
      }
      return ret;
    }
    /**
     * @brief get current row from phy operator context.
     */
    inline common::ObNewRow& get_cur_row()
    {
      return cur_row_;
    }
    /**
     * @brief get current rows from phy operator context.
     */
    inline common::ObNewRow* get_cur_rows()
    {
      return cur_rows_;
    }
    /**
     *@brief in order to reuse cur_rows when getting the next bulk of rows_
     */
    inline int64_t get_row_count()
    {
      return row_count_;
    }
    inline void rewind()
    {
      index_ = 0;
      for (int64_t i = 0; i < row_count_; i++) {
        cur_rows_[i].count_ = column_count_;
        cur_rows_[i].projector_ = projector_;
        cur_rows_[i].projector_size_ = projector_size_;
      }
    }
    /**
     * @brief get first row from phy operator context, for bulk call.
     */
    inline common::ObNewRow& get_first_row()
    {
      return cur_rows_[0];
    }

    /**
     * @brief get the calculate buffer of every physical operator
     * @return calc buffer
     */
    inline common::ObIAllocator& get_calc_buf()
    {
      return *calc_buf_;
    }
    void set_tenant_id(int64_t tenant_id)
    {
      op_monitor_info_.set_tenant_id(tenant_id);
    }
    void set_op_id(int64_t op_id)
    {
      op_monitor_info_.set_operator_id(op_id);
    }
    void set_op_type(ObPhyOperatorType type)
    {
      op_monitor_info_.set_operator_type(type);
    }
    ObPhyOperatorType get_op_type() const
    {
      return op_monitor_info_.get_operator_type();
    }
    ObMonitorNode& get_monitor_info()
    {
      return op_monitor_info_;
    }
    inline int init_base(uint64_t tenant_id);
    inline void destroy_base();

  protected:
    ObExecContext& exec_ctx_;
    lib::MemoryContext* calc_mem_;
    common::ObArenaAllocator* calc_buf_;
    common::ObNewRow cur_row_;
    common::ObNewRow* cur_rows_;

  public:
    common::ObExprCtx expr_ctx_;
    bool is_filtered_;
    bool is_filtered_has_set_;
    bool exch_drained_;
    bool got_first_row_;
    ObMonitorNode op_monitor_info_;
    int64_t check_times_;

  public:
    int64_t index_;
    int64_t row_count_;
    int64_t column_count_;
    int32_t* projector_;
    int64_t projector_size_;
  };

  OB_UNIS_VERSION_V(1);

public:
  explicit ObPhyOperator(common::ObIAllocator& alloc);
  virtual ~ObPhyOperator();

  void set_type(ObPhyOperatorType type)
  {
    type_ = type;
  }
  void set_plan_depth(int64_t depth)
  {
    plan_depth_ = depth;
  }
  /**
   * @brief reset function
   */
  virtual void reset();
  virtual void reuse();
  ObSqlExpressionFactory* get_sql_expression_factory()
  {
    return &sql_expression_factory_;
  }
  const ObSqlExpressionFactory* get_sql_expression_factory() const
  {
    return &sql_expression_factory_;
  }

  inline ObPhyOperatorType get_type() const
  {
    return type_;
  }
  inline int64_t get_plan_depth() const
  {
    return plan_depth_;
  }
  /**
   * @brief add child physical operator, some operators have multiple child operator
   *         but some operators don't have child operator
   * @param child_idx, child operator index
   */
  virtual int set_child(int32_t child_idx, ObPhyOperator& child_operator) = 0;
  virtual ObPhyOperator* get_child(int32_t child_idx) const = 0;

  virtual int32_t get_child_num() const = 0;
  inline void set_parent(ObPhyOperator* parent)
  {
    parent_op_ = parent;
  }
  inline ObPhyOperator* get_parent() const
  {
    return parent_op_;
  }
  int get_real_child(ObPhyOperator*& child, const int32_t idx) const;
  template <typename CompareObj>
  static int find_target_ops(
      const ObPhyOperator* root_op, const CompareObj& is_target_op, common::ObIArray<const ObPhyOperator*>& target_ops);
  /**
   * @brief Set the physical plan object who owns this operator.
   *
   * @param the_plan[in]
   */
  virtual void set_phy_plan(ObPhysicalPlan* the_plan);
  const ObPhysicalPlan* get_phy_plan() const;

  int check_status(ObExecContext& ctx) const;
  int try_check_status(ObExecContext& ctx) const;
  /**
   * @brief phy operator id which is unique in the physical plan, start with 0
   */
  void set_id(uint64_t id);
  uint64_t get_id() const;
  /**
   * @brief every operator output a row when calling get_next_row, the length of row is fixed
   * @return return the count of column
   */
  int64_t get_column_count() const
  {
    return column_count_;
  }
  void set_column_count(int64_t column_count)
  {
    column_count_ = column_count;
  }
  /**
   * @brief projector is an index array which the caller should use to access the row.
   * NOT every operator can consume rows with projector.
   */
  void set_projector(int32_t* projector, int64_t projector_size);
  void clear_projector();
  const int32_t* get_projector() const
  {
    return projector_;
  }
  int64_t get_projector_size() const
  {
    return projector_size_;
  }
  virtual int64_t get_output_count() const
  {
    return projector_size_;
  }
  virtual int create_child_array(int64_t child_op_size)
  {
    UNUSED(child_op_size);
    return common::OB_SUCCESS;
  }
  /**
   * @brief This function is called before that the job is sent to remote server.
   * By default, it does nothing. If you want to do something before
   * that the job is sent to remote server, you can override it
   */
  // virtual int prepare(ObExecContext &ctx) const;
  /**
   * @brief set expr_ctx and is_filtered.
   */
  int handle_op_ctx(ObExecContext& ctx) const;
  /**
   * @brief open operator, including children operators.
   */
  virtual int open(ObExecContext& ctx) const;
  virtual int process_expect_error(ObExecContext& ctx, int errcode) const;
  /**
   * @brief close operator, including children operators.
   */
  int close(ObExecContext& ctx) const;
  /**
   * @brief reopen the operator, with new params
   * if any operator needs rescan, code generator must have told 'him' that
   * where should 'he' get params, e.g. op_id
   */
  virtual int rescan(ObExecContext& ctx) const;
  virtual int switch_iterator(ObExecContext& ctx) const;
  /**
   * @brief get next row, call get_next to get a row,
   * if filters exist, call each filter in turn to filter the row,
   * if all rows are filtered, return OB_ITER_END,
   * if compute exprs exist, call each expr in turn to calculate
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;

  static int filter_row(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObIArray<common::ObISqlExpression*>& filters, bool& is_filtered);

  /**
   * @brief get next rows, call get_next_rows to get row_count rows,
   * if filters exist, call each filter in turn to filter the row,
   * if all rows are filtered, return OB_ITER_END,
   * if compute exprs exist, call each expr in turn to calculate
   * @param ctx[in], execute context
   * @param rows[out], ObSqlRow row_count obj arrays, row_size and row_count
   */
  virtual int get_next_rows(ObExecContext& ctx, const common::ObNewRow*& rows, int64_t& row_count) const;
  /**
   * @brief add filter expression to physical operator
   * @param expr[in], filter expression
   */
  virtual int add_filter(ObSqlExpression* expr, bool startup = false);

  virtual int add_virtual_column_expr(ObColumnExpression* expr);

  /**
   * @brief add compute expression to physical operator
   */
  virtual int add_compute(ObColumnExpression* expr);

  inline bool need_copy_row_for_compute() const
  {
    return calc_exprs_.get_size() > 0 || virtual_column_exprs_.get_size() > 0;
  }
  /**
   * @brief create operator input parameter,
   * only child operator can know it's specific operator type,
   * can be overwrited by child operator,
   * if the operator has no input param, no need to overwrite it
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int create_operator_input(ObExecContext& ctx) const;
  /**
   * @brief print content of this operator, use JSON style
   * @note DO NOT print children operators
   * @param buf [in] print string to log buffer
   * @param buf_len [in] buffer size
   * @return the length of to_string
   */
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;

  /* for material opertor, it need to oper after child open, otherwise it can't get data when open.
   * for px coord operator, it need to open before child open, since it need to schedule child dfo when open.
   */
  virtual OperatorOpenOrder get_operator_open_order(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return OPEN_CHILDREN_FIRST;
  }
  virtual int open_child(ObExecContext& ctx, const int32_t child_idx) const;
  const char* get_name() const
  {
    return get_phy_op_name(type_);
  }
  inline virtual int32_t get_name_length() const
  {
    return ((int32_t)strlen(get_name()));
  }

  int64_t get_rows() const
  {
    return rows_;
  }
  void set_rows(const int64_t rows)
  {
    rows_ = rows;
  }
  bool is_exact_rows() const
  {
    return is_exact_rows_;
  }
  void set_exact_rows(const bool is_exact_rows)
  {
    is_exact_rows_ = is_exact_rows;
  }
  int64_t get_cost() const
  {
    return cost_;
  }
  void set_cost(const int64_t cost)
  {
    cost_ = cost;
  }
  int64_t get_width() const
  {
    return width_;
  }
  void set_width(const int64_t width)
  {
    width_ = width;
  }
  PxOpSizeFactor get_px_est_size_factor()
  {
    return px_est_size_factor_;
  }
  void set_px_est_size_factor(PxOpSizeFactor px_est_size_factor)
  {
    px_est_size_factor_ = px_est_size_factor;
  }
  virtual bool is_dml_operator() const
  {
    return false;
  }
  virtual bool is_pdml_operator() const
  {
    return false;
  }
  virtual bool is_dml_without_output() const
  {
    return false;
  }
  virtual int serialize(char* buf, int64_t buf_len, int64_t& pos, ObPhyOpSeriCtx& seri_ctx) const
  {
    UNUSED(seri_ctx);
    return serialize(buf, buf_len, pos);
  }
  virtual int64_t get_serialize_size(const ObPhyOpSeriCtx& seri_ctx) const
  {
    UNUSED(seri_ctx);
    return get_serialize_size();
  }
  template <class T>
  int init_array_size(T& array, int64_t count)
  {
    int ret = common::OB_SUCCESS;
    if (count < 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(count));
    } else if (0 == count) {
      // nothing todo
    } else if (OB_FAIL(array.init(count))) {
      SQL_ENG_LOG(WARN, "fail to init array", K(ret), K(count));
    }
    return ret;
  }

  // Some output rows are associated with resources, resources need to reclaimed
  // when row consumed.
  // Only used by in index building to reclaim the intermediate macro blocks right now.
  typedef void (*reclaim_row_t)(const common::ObNewRow&);
  virtual reclaim_row_t reclaim_row_func() const
  {
    return NULL;
  }
  /**
   * @brief wrap the object of ObExprCtx, and reset calc_buf
   * @param exec_ctx[in], execute context
   * @param expr_ctx[out], sql expression calculate buffer context
   * @return if success, return OB_SUCCESS
   */
  virtual int wrap_expr_ctx(ObExecContext& exec_ctx, common::ObExprCtx& expr_ctx) const;
  const common::ObDList<ObSqlExpression>& get_virtual_column_exprs() const
  {
    return virtual_column_exprs_;
  }
  // register operator in datahub, for parallel execution.
  virtual int register_to_datahub(ObExecContext& ctx) const;

protected:
  OB_INLINE virtual bool need_filter_row() const
  {
    return true;
  }
  /**
   * @brief filter the specified row
   * @param expr_ctx[in], the sql expression calculate buffer context
   * @param row[in], ObSqlRow an obj array and row_size
   * @param is_filtered[out], if the specified row was filtered, set it to true
   * otherwise, set it to false
   */
  inline int filter_row(common::ObExprCtx& expr_ctx, const common::ObNewRow& row, bool& is_filtered) const;

  int filter_row(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObDList<ObSqlExpression>& filters, bool& is_filtered) const;

  int filter_row_for_check_cst(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObDList<ObSqlExpression>& filters, bool& is_filtered) const;

  int startup_filter(common::ObExprCtx& expr_ctx, bool& is_filtered) const;
  /**
   * @brief calculate the new row result
   * @param expr_ctx[in], the sql expression calculate buffer context
   * @param calc_row[in & out], the row is input row and the calculate result be set to it
   * @return if success, return OB_SUCCESS
   */
  int calculate_row(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row) const;
  int calculate_virtual_column(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row) const;
  int calculate_row_inner(common::ObExprCtx& expr_ctx, common::ObNewRow& calc_row,
      const common::ObDList<ObSqlExpression>& calc_exprs) const;
  int init_cur_row(ObPhyOperatorCtx& op_ctx, bool need_create_cells) const;
  int init_cur_rows(int64_t row_count, ObPhyOperatorCtx& op_ctx, bool need_create_cells) const;
  int copy_cur_row(ObPhyOperatorCtx& op_ctx, const common::ObNewRow*& row) const;
  int copy_cur_row_by_projector(ObPhyOperatorCtx& op_ctx, const common::ObNewRow*& row) const;
  int copy_cur_rows_by_projector(ObPhyOperatorCtx& op_ctx, const common::ObNewRow*& row) const;
  int copy_cur_rows(ObPhyOperatorCtx& op_ctx, const common::ObNewRow*& row, bool need_copy_cell) const;

  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const = 0;
  /**
   * @brief for child class to print it's member variable with json key-value format
   * @param buf[in] to string buffer
   * @param buf_len[in] buffer length
   * @return if success, return the length used by print string, otherwise return 0
   */
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   *         pure virtual function
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const = 0;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int after_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;

  // Drain exchange in data for PX, or producer DFO will be blocked.
  virtual int drain_exch(ObExecContext& ctx) const;

  // Try open and get operator ctx
  int try_open_and_get_operator_ctx(
      ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx, bool* has_been_opened = nullptr) const;

public:
  // check if this operator is some kind of table scan.
  // the default return value is false
  // Note: The default table scan operator ObTableScan overrides this
  //       method to return true.
  //       IT'S RECOMMENDED THAT any table scan operator derive from ObLogTableScan,
  //       or you MAY NEED TO CHANGE EVERY CALLER of this method.
  virtual bool is_table_scan() const
  {
    return false;
  }
  virtual bool is_receive_op() const
  {
    return false;
  }

public:
  int copy_cur_row_by_projector(common::ObNewRow& cur_row, const common::ObNewRow*& row) const;
  int copy_cur_row(common::ObNewRow& cur_row, const common::ObNewRow*& row, bool need_copy_cell) const;
  static int deep_copy_row(
      const common::ObNewRow& src_row, common::ObNewRow*& dst_row, common::ObIAllocator& allocator);

public:
  virtual int accept(ObPhyOperatorVisitor& visitor) const
  {
    UNUSED(visitor);
    return common::OB_SUCCESS;
  }

  static int filter_row_inner(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObISqlExpression* expr, bool& is_filtered);

  static int filter_row_inner_for_check_cst(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObISqlExpression* expr, bool& is_filtered);

  const common::ObIArray<ObOpSchemaObj>& get_op_schema_objs() const
  {
    return op_schema_objs_;
  }
  common::ObIArray<ObOpSchemaObj>& get_op_schema_objs_for_update()
  {
    return op_schema_objs_;
  }

private:
  static const int64_t CHECK_STATUS_MASK = 0x3FF;  // check_status for each 1024 rows
protected:
  ObSqlExpressionFactory sql_expression_factory_;
  /*
   * the physical plan object who owns this operator.
   */
  ObPhysicalPlan* my_phy_plan_;
  /* any operator can find its parent through this clue */
  ObPhyOperator* parent_op_;
  /* physical operator id */
  uint64_t id_;
  /* output row column count */
  int64_t column_count_;
  int64_t projector_size_;
  common::ObDList<ObSqlExpression> filter_exprs_;
  common::ObDList<ObSqlExpression> calc_exprs_;
  common::ObDList<ObSqlExpression> startup_exprs_;
  common::ObDList<ObSqlExpression> virtual_column_exprs_;
  int64_t rows_;
  int64_t cost_;
  int64_t width_;
  int32_t* projector_;
  PxOpSizeFactor px_est_size_factor_;
  bool is_exact_rows_;
  ObPhyOperatorType type_;  // for GDB debug purpose, no need to serialize
  int32_t plan_depth_;      // for plan cache explain
  common::ObSEArray<ObOpSchemaObj, 8> op_schema_objs_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPhyOperator);
};

class ObSingleChildPhyOperator;
class ObDoubleChildrenPhyOperator;
class ObMultiChildrenPhyOperator;
class ObNoChildrenPhyOperator;
class ObPhyOperatorVisitor {
public:
  virtual int pre_visit(const ObSingleChildPhyOperator& op) = 0;
  virtual int post_visit(const ObSingleChildPhyOperator& op) = 0;
  virtual int pre_visit(const ObDoubleChildrenPhyOperator& op) = 0;
  virtual int post_visit(const ObDoubleChildrenPhyOperator& op) = 0;
  virtual int pre_visit(const ObMultiChildrenPhyOperator& op) = 0;
  virtual int post_visit(const ObMultiChildrenPhyOperator& op) = 0;
  virtual int pre_visit(const ObNoChildrenPhyOperator& op) = 0;
  virtual int post_visit(const ObNoChildrenPhyOperator& op) = 0;
  virtual int pre_visit(const ObPhyOperator& op) = 0;
  virtual int post_visit(const ObPhyOperator& op) = 0;
};

inline void ObPhyOperator::set_projector(int32_t* projector, int64_t projector_size)
{
  projector_ = projector;
  projector_size_ = projector_size;
}

inline void ObPhyOperator::clear_projector()
{
  projector_ = NULL;
  projector_size_ = 0;
}

inline int ObPhyOperator::ObPhyOperatorCtx::init_base(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == calc_mem_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_mem_attr(tenant_id, common::ObModIds::OB_SQL_EXPR_CALC, common::ObCtxIds::DEFAULT_CTX_ID);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(calc_mem_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else {
      calc_buf_ = &calc_mem_->get_arena_allocator();
    }
  }
  return ret;
}

inline void ObPhyOperator::ObPhyOperatorCtx::destroy_base()
{
  if (OB_LIKELY(NULL != calc_mem_)) {
    DESTROY_CONTEXT(calc_mem_);
    calc_mem_ = NULL;
  }
}

inline void ObPhyOperator::set_phy_plan(ObPhysicalPlan* the_plan)
{
  my_phy_plan_ = the_plan;
}

inline const ObPhysicalPlan* ObPhyOperator::get_phy_plan() const
{
  return my_phy_plan_;
}

inline void ObPhyOperator::set_id(uint64_t id)
{
  id_ = id;
}

inline uint64_t ObPhyOperator::get_id() const
{
  return id_;
}

template <typename CompareObj>
int ObPhyOperator::find_target_ops(
    const ObPhyOperator* root_op, const CompareObj& is_targe_op, common::ObIArray<const ObPhyOperator*>& target_ops)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(root_op)) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "root op is null", K(ret));
  } else if (!root_op->is_receive_op()) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root_op->get_child_num(); ++i) {
      const ObPhyOperator* child_op = root_op->get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "child op is null", K(ret), K(i));
      } else if (OB_FAIL(ObPhyOperator::find_target_ops(child_op, is_targe_op, target_ops))) {
        SQL_ENG_LOG(WARN,
            "fail to find child target ops",
            K(ret),
            K(i),
            "op_id",
            root_op->get_id(),
            "child_id",
            child_op->get_id());
      }
    }
  }
  if (OB_SUCC(ret) && is_targe_op(*root_op)) {
    if (OB_FAIL(target_ops.push_back(root_op))) {
      SQL_ENG_LOG(WARN, "fail to push back target op", K(ret), KPC(root_op));
    }
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_OB_PHY_OPERATOR_H
