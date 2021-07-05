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

#ifndef _OCEANBASE_SQL_OPTIMIZER_OB_TABLE_LOCATION_H
#define _OCEANBASE_SQL_OPTIMIZER_OB_TABLE_LOCATION_H

#include "lib/hash/ob_pointer_hashmap.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"

namespace oceanbase {
namespace common {
class ObPartMgr;
}
namespace sql {
class ObRawExpr;
class ObPartHint;
class ObRawExprFactory;
class ObColumnRefRawExpr;
class ObExprEqualCheckContext;
typedef common::ObSEArray<int64_t, 1> RowkeyArray;
class ObPartIdRowMapManager {
public:
  ObPartIdRowMapManager() : manager_(), part_idx_(common::OB_INVALID_INDEX)
  {}
  typedef common::ObSEArray<int64_t, 12> ObRowIdList;
  struct MapEntry {
  public:
    MapEntry() : list_()
    {}
    TO_STRING_KV(K_(list));
    int assign(const MapEntry& entry);

  public:
    ObRowIdList list_;
  };
  typedef common::ObSEArray<MapEntry, 1> ObPartRowManager;
  int add_row_for_part(int64_t part_idx, int64_t row_id);
  const ObRowIdList* get_row_id_list(int64_t part_index);
  void reset()
  {
    manager_.reset();
    part_idx_ = common::OB_INVALID_INDEX;
  }
  int64_t get_part_count() const
  {
    return manager_.count();
  }
  int assign(const ObPartIdRowMapManager& other);
  int64_t get_part_idx() const
  {
    return part_idx_;
  }
  void set_part_idx(int64_t part_idx)
  {
    part_idx_ = part_idx;
  }
  const MapEntry& at(int64_t i) const
  {
    return manager_.at(i);
  }
  common::ObNewRow& get_part_row()
  {
    return part_row_;
  }
  TO_STRING_KV(K_(manager), K_(part_idx));

private:
  ObPartRowManager manager_;
  int64_t part_idx_;  // used for parameter pass only.
  common::ObNewRow part_row_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartIdRowMapManager);
};

class ObInsertStmt;
struct ObPartLocCalcNode {
  enum NodeType {
    INVALID = 0,
    CALC_AND,
    CALC_OR,
    QUERY_RANGE,
    FUNC_VALUE,
    COLUMN_VALUE,
  };

  ObPartLocCalcNode() : node_type_(INVALID)
  {}
  virtual ~ObPartLocCalcNode()
  {}

  inline void set_node_type(NodeType node_type)
  {
    node_type_ = node_type;
  }
  inline NodeType get_node_type() const
  {
    return node_type_;
  }

  inline bool is_and_node() const
  {
    return CALC_AND == node_type_;
  }
  inline bool is_or_node() const
  {
    return CALC_OR == node_type_;
  }
  inline bool is_query_range_node() const
  {
    return QUERY_RANGE == node_type_;
  }
  inline bool is_func_value_node() const
  {
    return FUNC_VALUE == node_type_;
  }
  inline bool is_column_value_node() const
  {
    return COLUMN_VALUE == node_type_;
  }

  static ObPartLocCalcNode* create_part_calc_node(common::ObIAllocator& allocator,
      common::ObIArray<ObPartLocCalcNode*>& calc_nodes, ObPartLocCalcNode::NodeType type);

  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const = 0;

  TO_STRING_KV(K_(node_type));

  NodeType node_type_;
};

struct ObPLAndNode : public ObPartLocCalcNode {
  ObPLAndNode() : left_node_(NULL), right_node_(NULL)
  {
    set_node_type(CALC_AND);
  }
  ObPLAndNode(common::ObIAllocator& allocator) : left_node_(NULL), right_node_(NULL)
  {
    UNUSED(allocator);
    set_node_type(CALC_AND);
  }

  virtual ~ObPLAndNode()
  {}
  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const;

  ObPartLocCalcNode* left_node_;
  ObPartLocCalcNode* right_node_;
};

struct ObPLOrNode : public ObPartLocCalcNode {
  ObPLOrNode() : left_node_(NULL), right_node_(NULL)
  {
    set_node_type(CALC_OR);
  }
  ObPLOrNode(common::ObIAllocator& allocator) : left_node_(NULL), right_node_(NULL)
  {
    UNUSED(allocator);
    set_node_type(CALC_OR);
  }

  virtual ~ObPLOrNode()
  {}
  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const;
  ObPartLocCalcNode* left_node_;
  ObPartLocCalcNode* right_node_;
};

struct ObPLQueryRangeNode : public ObPartLocCalcNode {
  ObPLQueryRangeNode() : pre_query_range_()
  {
    set_node_type(QUERY_RANGE);
  }

  ObPLQueryRangeNode(common::ObIAllocator& allocator) : pre_query_range_(allocator)
  {
    set_node_type(QUERY_RANGE);
  }
  virtual ~ObPLQueryRangeNode()
  {
    pre_query_range_.reset();
  }
  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const;
  int get_range(common::ObIAllocator& allocator, const ParamStore& params, ObQueryRangeArray& query_ranges,
      bool& is_all_single_value_ranges, bool& is_empty, const common::ObDataTypeCastParams& dtc_params) const;
  ObQueryRange pre_query_range_;
};

struct ObPLFuncValueNode : public ObPartLocCalcNode {
  struct ParamValuePair {
    ParamValuePair(int64_t param_idx, const common::ObObj obj_val) : param_idx_(param_idx), obj_value_(obj_val)
    {}
    ParamValuePair() : param_idx_(-1), obj_value_()
    {}
    TO_STRING_KV(K_(param_idx), K_(obj_value));
    int64_t param_idx_;
    common::ObObj obj_value_;
  };

  ObPLFuncValueNode() : res_type_(), param_idx_(-1)
  {
    set_node_type(FUNC_VALUE);
  }
  ObPLFuncValueNode(common::ObIAllocator& allocator) : res_type_(), param_idx_(-1)
  {
    UNUSED(allocator);
    set_node_type(FUNC_VALUE);
  }
  virtual ~ObPLFuncValueNode()
  {}
  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const;
  ObExprResType res_type_;
  int64_t param_idx_;
  common::ObObj value_;
  common::ObSEArray<ParamValuePair, 3, common::ModulePageAllocator, false> param_value_;
};

struct ObPLColumnValueNode : public ObPartLocCalcNode {
  ObPLColumnValueNode() : param_idx_(-1)
  {
    set_node_type(COLUMN_VALUE);
  }
  ObPLColumnValueNode(common::ObIAllocator& allocator) : param_idx_(-1)
  {
    UNUSED(allocator);
    set_node_type(COLUMN_VALUE);
  }

  virtual ~ObPLColumnValueNode()
  {}
  virtual int deep_copy(common::ObIAllocator& allocator, common::ObIArray<ObPartLocCalcNode*>& calc_nodes,
      ObPartLocCalcNode*& other) const;

  ObExprResType res_type_;
  int64_t param_idx_;
  common::ObObj value_;
};

struct ObListPartMapKey {
  common::ObNewRow row_;

  int64_t hash() const;
  bool operator==(const ObListPartMapKey& other) const;
  TO_STRING_KV(K_(row));
};

struct ObListPartMapValue {
  ObListPartMapKey key_;
  int64_t part_id_;

  TO_STRING_KV(K_(key), K_(part_id));
};

template <class T, class V>
struct ObGetListPartMapKey {
  void operator()(const T& t, const V& v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template <>
struct ObGetListPartMapKey<ObListPartMapKey, ObListPartMapValue*> {
  ObListPartMapKey operator()(const ObListPartMapValue* value) const
  {
    return value->key_;
  }
};

struct ObHashPartMapKey {
  int64_t part_idx_;

  int64_t hash() const;
  bool operator==(const ObHashPartMapKey& other) const;
  TO_STRING_KV(K_(part_idx));
};

struct ObHashPartMapValue {
  ObHashPartMapKey key_;
  int64_t part_id_;

  TO_STRING_KV(K_(key), K_(part_id));
};

template <class T, class V>
struct ObGetHashPartMapKey {
  void operator()(const T& t, const V& v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template <>
struct ObGetHashPartMapKey<ObHashPartMapKey, ObHashPartMapValue*> {
  ObHashPartMapKey operator()(const ObHashPartMapValue* value) const
  {
    return value->key_;
  }
};

struct TableLocationKey {
  uint64_t table_id_;
  uint64_t ref_table_id_;

  bool operator==(const TableLocationKey& other) const;
  bool operator!=(const TableLocationKey& other) const;

  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&ref_table_id_, sizeof(uint64_t), hash_ret);
    return hash_ret;
  }

  TO_STRING_KV(K_(table_id), K_(ref_table_id));
};

class ObOptimizerContext;
class ObTableLocation {
public:
  enum TablePartType {
    NONE,
    HASH,
    RANGE,
    LIST,
  };
  enum FastCalcType {   // Is it possible to quickly calculate the partition id
    NONE_FAST_CALC,     // Default initial value
    UNKNOWN_FAST_CALC,  // Insert statement: the value after the type matches; query statement: equivalent condition
                        // filtering;
    CANNOT_FAST_CALC,
    CAN_FAST_CALC,
  };
  typedef common::ObSEArray<ObSqlExpression*, 32, common::ModulePageAllocator, false> KeyExprs;
  typedef common::ObIArray<ObSqlExpression*> IKeyExprs;

  class PartProjector {
    OB_UNIS_VERSION(1);

  public:
    PartProjector(common::ObIAllocator& allocator, ObSqlExpressionFactory& sql_expr_factory,
        ObExprOperatorFactory& expr_op_factory)
        : allocator_(allocator), sql_expr_factory_(sql_expr_factory), expr_op_factory_(expr_op_factory)
    {
      reset();
    }
    inline void reset()
    {
      column_cnt_ = 0;
      part_projector_ = NULL;
      part_projector_size_ = 0;
      subpart_projector_ = NULL;
      subpart_projector_size_ = 0;
      virtual_column_exprs_.reset();
    }
    int deep_copy(const PartProjector& other);
    int init_part_projector(const ObRawExpr* part_expr, share::schema::ObPartitionLevel part_level, RowDesc& row_desc);
    int init_part_projector(const ObRawExpr* part_expr, RowDesc& row_desc);
    int init_subpart_projector(const ObRawExpr* part_expr, RowDesc& row_desc);
    inline void set_column_cnt(int64_t column_cnt)
    {
      column_cnt_ = column_cnt;
    }
    int calc_part_row(const stmt::StmtType& stmt_type, ObExecContext& ctx, const common::ObNewRow& input_row,
        common::ObNewRow*& part_row) const;
    void project_part_row(share::schema::ObPartitionLevel part_level, common::ObNewRow& part_row) const;
    TO_STRING_KV(K_(virtual_column_exprs), K_(column_cnt), "part_projector",
        common::ObArrayWrap<int32_t>(part_projector_, part_projector_size_), "subpart_projector",
        common::ObArrayWrap<int32_t>(subpart_projector_, subpart_projector_size_));

  private:
    int init_part_projector(
        const ObRawExpr* part_expr, RowDesc& row_desc, int32_t*& projector, int64_t& projector_size);

  private:
    common::ObDList<ObSqlExpression> virtual_column_exprs_;
    int64_t column_cnt_;
    int32_t* part_projector_;
    int64_t part_projector_size_;
    int32_t* subpart_projector_;
    int64_t subpart_projector_size_;
    common::ObIAllocator& allocator_;
    ObSqlExpressionFactory& sql_expr_factory_;
    ObExprOperatorFactory& expr_op_factory_;
  };

  static int get_location_type(const common::ObAddr& server,
      const ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list, ObTableLocationType& location_type);

  // get virtual talbe partition ids or fake id. ref_table_id should be partitioned virtual table
  //@param [in] ref_table_id partitioned virtual table
  //@param [out] partition ids. all partition ids
  //@param [out] fake id. Fake id, if has local partition return local part_id
  static int get_vt_partition_id(
      ObExecContext& exec_ctx, const uint64_t ref_table_id, common::ObIArray<int64_t>* partition_ids, int64_t* fake_id);
  OB_UNIS_VERSION(1);

public:
  // for array: new(&data_[count_]) T();
  // for normal usage, like plan set
  ObTableLocation()
      : inited_(false),
        table_id_(common::OB_INVALID_ID),
        ref_table_id_(common::OB_INVALID_ID),
        is_partitioned_(true),
        part_level_(share::schema::PARTITION_LEVEL_ZERO),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        part_get_all_(false),
        subpart_get_all_(false),
        is_col_part_expr_(false),
        is_col_subpart_expr_(false),
        is_oracle_temp_table_(false),
        tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
        inner_allocator_(common::ObModIds::OB_SQL_TABLE_LOCATION),
        allocator_(inner_allocator_),
        calc_node_(NULL),
        gen_col_node_(NULL),
        subcalc_node_(NULL),
        sub_gen_col_node_(NULL),
        sql_expression_factory_(allocator_),
        expr_op_factory_(allocator_),
        part_expr_(NULL),
        gen_col_expr_(NULL),
        subpart_expr_(NULL),
        sub_gen_col_expr_(NULL),
        simple_insert_(false),
        stmt_type_(stmt::T_NONE),
        literal_stmt_type_(stmt::T_NONE),
        hint_read_consistency_(common::INVALID_CONSISTENCY),
        is_contain_inner_table_(false),
        is_contain_select_for_update_(false),
        is_contain_mv_(false),
        key_exprs_(),
        subkey_exprs_(),
        num_values_(0),
        key_conv_exprs_(),
        subkey_conv_exprs_(),
        part_hint_ids_(),
        part_num_(1),
        direction_(MAX_DIR),
        index_table_id_(common::OB_INVALID_ID),
        part_col_type_(ObNullType),
        part_collation_type_(CS_TYPE_INVALID),
        subpart_col_type_(ObNullType),
        subpart_collation_type_(CS_TYPE_INVALID),
        first_partition_id_(-1),
        is_in_hit_(false),
        part_projector_(allocator_, sql_expression_factory_, expr_op_factory_),
        is_global_index_(false),
        related_part_expr_idx_(OB_INVALID_INDEX),
        related_subpart_expr_idx_(OB_INVALID_INDEX),
        use_list_part_map_(false),
        list_default_part_id_(OB_INVALID_ID),
        use_hash_part_opt_(false),
        use_range_part_opt_(false),
        fast_calc_part_opt_(NONE_FAST_CALC),
        duplicate_type_(ObDuplicateType::NOT_DUPLICATE),
        first_part_num_(1),
        partition_num_(1),
        range_obj_arr_(NULL),
        range_part_id_arr_(NULL),
        use_calc_part_by_rowid_(false),
        is_valid_range_columns_part_range_(false),
        is_valid_range_columns_subpart_range_(false)
  {}

  // Used in situations where the optimizer does not adjust the destructor, to ensure that
  // each member array is passed to the external allocator
  ObTableLocation(common::ObIAllocator& allocator)
      : inited_(false),
        table_id_(common::OB_INVALID_ID),
        ref_table_id_(common::OB_INVALID_ID),
        is_partitioned_(true),
        part_level_(share::schema::PARTITION_LEVEL_ZERO),
        part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
        part_get_all_(false),
        subpart_get_all_(false),
        is_col_part_expr_(false),
        is_col_subpart_expr_(false),
        is_oracle_temp_table_(false),
        tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
        allocator_(allocator),
        calc_node_(NULL),
        gen_col_node_(NULL),
        subcalc_node_(NULL),
        sub_gen_col_node_(NULL),
        calc_nodes_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        sql_expression_factory_(allocator_),
        expr_op_factory_(allocator_),
        part_expr_(NULL),
        gen_col_expr_(NULL),
        subpart_expr_(NULL),
        sub_gen_col_expr_(NULL),
        simple_insert_(false),
        stmt_type_(stmt::T_NONE),
        literal_stmt_type_(stmt::T_NONE),
        hint_read_consistency_(common::INVALID_CONSISTENCY),
        is_contain_inner_table_(false),
        is_contain_select_for_update_(false),
        is_contain_mv_(false),
        key_exprs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        subkey_exprs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        num_values_(0),
        key_conv_exprs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        subkey_conv_exprs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        part_hint_ids_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        part_num_(1),
        direction_(MAX_DIR),
        index_table_id_(OB_INVALID_ID),
        part_col_type_(ObNullType),
        part_collation_type_(CS_TYPE_INVALID),
        subpart_col_type_(ObNullType),
        subpart_collation_type_(CS_TYPE_INVALID),
        first_partition_id_(-1),
        is_in_hit_(false),
        part_projector_(allocator_, sql_expression_factory_, expr_op_factory_),
        is_global_index_(false),
        related_part_expr_idx_(OB_INVALID_INDEX),
        related_subpart_expr_idx_(OB_INVALID_INDEX),
        use_list_part_map_(false),
        list_default_part_id_(OB_INVALID_ID),
        use_hash_part_opt_(false),
        use_range_part_opt_(false),
        fast_calc_part_opt_(NONE_FAST_CALC),
        duplicate_type_(ObDuplicateType::NOT_DUPLICATE),
        first_part_num_(1),
        partition_num_(1),
        range_obj_arr_(NULL),
        range_part_id_arr_(NULL),
        list_part_map_(common::ModulePageAllocator(allocator_)),
        list_part_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        hash_part_map_(common::ModulePageAllocator(allocator_)),
        hash_part_array_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
        use_calc_part_by_rowid_(false),
        is_valid_range_columns_part_range_(false),
        is_valid_range_columns_subpart_range_(false)
  {}
  virtual ~ObTableLocation()
  {
    reset();
  }

  ObTableLocation(const ObTableLocation& other);

  ObTableLocation& operator=(const ObTableLocation& other);

  void reset();
  inline bool is_inited() const
  {
    return inited_;
  }
  // get preliminary_query_range and partition_expr. For sql with filters.
  template <typename SchemaGuardType>
  int init(SchemaGuardType& schema_guard, ObDMLStmt& stmt, ObSQLSessionInfo* session_info,
      const common::ObIArray<ObRawExpr*>& filter_exprs, const uint64_t table_id, const uint64_t ref_table_id,
      const ObPartHint* part_hint, const common::ObDataTypeCastParams& dtc_params, const bool is_dml_table,
      common::ObIArray<ObRawExpr*>* sort_exprs = NULL)
  {
    int ret = common::OB_SUCCESS;
    const share::schema::ObTableSchema* table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(ref_table_id, table_schema))) {
      SQL_OPT_LOG(WARN, "failed to get table schema", K(ret), K(ref_table_id));
    } else if (OB_FAIL(init(table_schema,
                   stmt,
                   session_info,
                   filter_exprs,
                   table_id,
                   ref_table_id,
                   part_hint,
                   dtc_params,
                   is_dml_table,
                   sort_exprs))) {
      SQL_OPT_LOG(WARN, "failed to init", K(ret), K(ref_table_id));
    }
    return ret;
  }
  int init(const share::schema::ObTableSchema* table_schema, ObDMLStmt& stmt, ObSQLSessionInfo* session_info,
      const common::ObIArray<ObRawExpr*>& filter_exprs, const uint64_t table_id, const uint64_t ref_table_id,
      const ObPartHint* part_hint, const common::ObDataTypeCastParams& dtc_params, const bool is_dml_table,
      common::ObIArray<ObRawExpr*>* sort_exprs = NULL);

  int get_is_weak_read(ObExecContext& exec_ctx, bool& is_weak_read) const;

  /**
   * Calculate the table's partition location list from the input parameters.
   *
   * This function is used after ObTableLocation inited.
   * The function assumes that:
   *    ObTableLocation has been inited.
   *
   * @param params[in]  SQL parameters
   * @param location_cache[in]  location cache
   * @param partition_location_list[out] the table's partition_location_list
   */
  int calculate_partition_location_infos(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      share::ObIPartitionLocationCache& location_cache, ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list,
      const common::ObDataTypeCastParams& dtc_params, bool nonblock = false) const;

  bool calculate_partition_ids_fast(
      ObExecContext& exec_ctx, const ParamStore& params, ObIArray<int64_t>& partition_ids) const;
  int calculate_partition_ids_fast_for_insert(ObExecContext& exec_ctx, const ParamStore& param_store,
      common::ObObj& result,    // Partition column value
      bool& has_optted) const;  // With or without optimization
  int calculate_partition_ids_fast_for_non_insert(ObExecContext& exec_ctx, const ParamStore& param_store,
      common::ObObj& result,    // Partition column value
      bool& has_optted) const;  // With or without optimization
  /**
   * Calculate table partition location ids from input parameters.
   *
   * This function is used by calculate, has the same assumes with calculate.
   *
   * @param params[in] SQL parameters
   * @param part_ids[out] partition location ids,
   *                      if part_ids' count == 0. it means error.
   * @return int, if success, return OB_SUCCESS, else return other error code
   */
  int calculate_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      common::ObIArray<int64_t>& part_ids, const common::ObDataTypeCastParams& dtc_params) const;
  // Pass in rowkey and table_id, and then calculate the part_ids
  // corresponding to rowkey and the rowkey_list corresponding to
  // part_id. The corresponding rules are: part_ids and rowkey_lists
  // are both arrays, and their subscripts correspond one-to-one.
  int calculate_partition_ids_by_rowkey(ObSQLSessionInfo& session_info,
      share::schema::ObSchemaGetterGuard& schema_guard, uint64_t table_id, const common::ObIArray<ObRowkey>& rowkeys,
      common::ObIArray<int64_t>& part_ids, common::ObIArray<RowkeyArray>& rowkey_lists);
  int init_table_location(ObSqlSchemaGuard& schema_guard, uint64_t table_id, uint64_t ref_table_id, ObDMLStmt& stmt,
      RowDesc& row_desc, const bool is_dml_table, const ObOrderDirection& direction = default_asc_direction());
  int init_table_location_with_rowkey(ObSqlSchemaGuard& schema_guard, uint64_t table_id, ObSQLSessionInfo& session_info,
      const bool is_dml_table = false);
  int calculate_partition_ids_by_row(ObExecContext& exec_ctx, ObPartMgr* part_mgr, const common::ObNewRow& row,
      ObIArray<int64_t>& part_ids, int64_t& part_idx) const;
  int calculate_partition_id_by_row(
      ObExecContext& exec_ctx, ObPartMgr* part_mgr, const common::ObNewRow& row, int64_t& part_id) const;
  /// Get the partition locations of partition ids
  static int set_partition_locations(ObExecContext& exec_ctx, share::ObIPartitionLocationCache& location_cache,
      const uint64_t ref_table_id, const common::ObIArray<int64_t>& partition_ids,
      ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list, bool nonblock = false);

  inline void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }

  inline uint64_t get_table_id() const
  {
    return table_id_;
  }

  inline uint64_t get_ref_table_id() const
  {
    return ref_table_id_;
  }

  inline uint64_t get_is_global_index() const
  {
    return is_global_index_;
  }

  void set_related_part_expr_idx(int32_t related_part_expr_idx)
  {
    related_part_expr_idx_ = related_part_expr_idx;
  }

  void set_related_subpart_expr_idx(int32_t related_subpart_expr_idx)
  {
    related_subpart_expr_idx_ = related_subpart_expr_idx;
  }

  int64_t get_partition_num() const
  {
    return part_num_;
  }

  bool is_partitioned() const
  {
    return is_partitioned_;
  }

  share::schema::ObPartitionLevel get_part_level() const
  {
    return part_level_;
  }

  bool can_fast_calc_part_ids() const
  {
    return CAN_FAST_CALC == fast_calc_part_opt_;
  }

  int64_t get_tablet_size()
  {
    return tablet_size_;
  }

  int get_ranges(ObIAllocator& allocator, const ParamStore& params, ObSQLSessionInfo* session_info,
      const ObPhyTableLocationInfo& phy_table_location, ObIArray<ObNewRange>& ranges) const;

  // ObQueryRange& get_preliminary_query_range() { return preliminary_query_range_; }
  const stmt::StmtType& get_stmt_type() const
  {
    return stmt_type_;
  }
  const ObConsistencyLevel& get_hint_read_consistency() const
  {
    return hint_read_consistency_;
  }
  bool is_contain_inner_table() const
  {
    return is_contain_inner_table_;
  }
  int set_log_op_infos(const uint64_t index_table_id, const ObOrderDirection& direction);
  // int set_pre_query_range(const ObQueryRange *pre_query_range);
  // int set_direction(const ObOrderDirection &direction);
  const ObOrderDirection& get_direction() const
  {
    return direction_;
  }

  int get_not_insert_dml_part_sort_expr(ObDMLStmt& stmt, common::ObIArray<ObRawExpr*>* sort_exprs) const;
  bool is_contain_mv() const
  {
    return is_contain_mv_;
  }
  bool has_generated_column() const
  {
    return NULL != gen_col_expr_ || NULL != sub_gen_col_expr_ || NULL != gen_col_node_ || NULL != sub_gen_col_node_;
  }
  inline ObSqlExpression* get_part_expr()
  {
    return part_expr_;
  }
  const inline ObSqlExpression* get_part_expr() const
  {
    return part_expr_;
  }
  inline ObSqlExpression* get_subpart_expr()
  {
    return subpart_expr_;
  }
  const inline ObSqlExpression* get_subpart_expr() const
  {
    return subpart_expr_;
  }
  static int get_phy_table_location_info(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
      bool is_weak, const common::ObIArray<int64_t>& part_ids, share::ObIPartitionLocationCache& location_cache,
      ObPhyTableLocationInfo& phy_loc_info);
  static int append_phy_table_location(ObExecContext& ctx, uint64_t table_location_key, uint64_t ref_table_id,
      bool is_weak, const common::ObIArray<int64_t>& part_ids, const ObOrderDirection& order_direction);

  int add_part_idx_to_part_id_map(const share::schema::ObTableSchema& table_schema);
  int get_part_id_from_part_idx(int64_t part_idx, int64_t& part_id) const;
  int add_list_part_schema(const share::schema::ObTableSchema& table_schema, const common::ObTimeZoneInfo* tz_info);
  int add_hash_part_schema(const share::schema::ObTableSchema& table_schema);
  int add_range_part_schema(const share::schema::ObTableSchema& table_schema);
  int get_list_part(const common::ObNewRow& row, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids,
      int64_t* part_idx) const;
  int get_list_part(
      common::ObObj& obj, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids, int64_t* part_idx) const;
  int get_range_part(const common::ObNewRow& obj, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids,
      int64_t* part_idx) const;
  int get_range_part(
      const common::ObNewRange* range, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids) const;
  int get_hash_part(const common::ObObj& obj, bool insert_or_replace, common::ObIArray<int64_t>& partition_ids,
      int64_t* part_idx) const;
  bool is_duplicate_table() const
  {
    return ObDuplicateType::NOT_DUPLICATE != duplicate_type_;
  }
  bool is_duplicate_table_not_in_dml() const
  {
    return ObDuplicateType::DUPLICATE == duplicate_type_;
  }
  void set_duplicate_type(ObDuplicateType v)
  {
    duplicate_type_ = v;
  }
  ObDuplicateType get_duplicate_type() const
  {
    return duplicate_type_;
  }
  const common::ObIArray<int64_t>& get_part_expr_param_idxs() const
  {
    return part_expr_param_idxs_;
  }
  int deal_dml_partition_selection(int64_t part_id) const;
  int add_part_hint_ids(const ObIArray<int64_t>& part_ids)
  {
    return append_array_no_dup(part_hint_ids_, part_ids);
  }

  int get_partition_ids_by_range(ObExecContext& exec_ctx, ObPartMgr* part_mgr, const ObNewRange* part_range,
      const ObNewRange* gen_range, ObIArray<int64_t>& partition_ids, const ObIArray<int64_t>* level_one_part_ids) const;

  int calc_partition_ids_by_range(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ObNewRange* range,
      ObIArray<int64_t>& partition_ids, bool& all_part, const ObIArray<int64_t>* part_ids,
      const ObSqlExpression* gen_col_expr = NULL) const;

  int init_table_location_with_row_desc(
      ObSqlSchemaGuard& schema_guard, uint64_t table_id, RowDesc& input_row_desc, ObSQLSessionInfo& session_info);

  int generate_row_desc_from_row_desc(ObDMLStmt& stmt, const uint64_t data_table_id, ObRawExprFactory& expr_factory,
      const RowDesc& input_row_desc, RowDesc& row_desc);

  int init_table_location_with_rowid(ObSqlSchemaGuard& schema_guard, const common::ObIArray<int64_t>& param_idxs,
      const uint64_t table_id, const uint64_t ref_table_id, ObSQLSessionInfo& session_info, const bool is_dml_table);

  int calc_partition_location_infos_with_rowid(ObExecContext& exec_ctx,
      share::schema::ObSchemaGetterGuard& schema_guard, const ParamStore& params,
      share::ObIPartitionLocationCache& location_cache, ObPhyPartitionLocationInfoIArray& phy_part_loc_info_list,
      bool nonblock = false) const;

  bool use_calc_part_by_rowid() const
  {
    return use_calc_part_by_rowid_;
  }

  int calculate_partition_ids_with_rowid(ObExecContext& exec_ctx, share::schema::ObSchemaGetterGuard& schema_guard,
      const ParamStore& params, common::ObIArray<int64_t>& part_ids) const;

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(part_num), K_(is_global_index), K_(duplicate_type),
      K_(part_expr_param_idxs), K_(part_projector), K_(part_expr), K_(gen_col_expr));

private:
  // get partition columns and generate partition_expr_
  // partition_columns:partition columns
  // gen_cols: columns dependented by generated partition column
  // partition_raw_expr: raw expr of partition
  // gen_col_expression: dependented expression of generated partition column
  int get_partition_column_info(ObDMLStmt& stmt, const share::schema::ObPartitionLevel part_level,
      common::ObIArray<ColumnItem>& partition_columns, common::ObIArray<ColumnItem>& gen_cols,
      const ObRawExpr*& partition_raw_expr, ObSqlExpression*& partition_expression,
      ObSqlExpression*& gen_col_expression, bool& is_col_part_expr);
  int generate_rowkey_desc(ObDMLStmt& stmt, const common::ObRowkeyInfo& rowkey_info, uint64_t data_table_id,
      ObRawExprFactory& expr_factory, RowDesc& row_desc);

  // Take the intersection of partition ids and to_inter_ids, and store the result in partition_ids
  int intersect_partition_ids(
      const common::ObIArray<int64_t>& to_inter_ids, common::ObIArray<int64_t>& partition_ids) const;
  // add partition columns.
  // For insert stmt with generated column,
  // the dependent columns would added to partitoin columns.
  // gen_cols: For non-insert statements, the partition expr of
  //           a single column stores the dependent column
  //           of the generated_column
  // gen_col_expr: generated_column's dependent expr
  int add_partition_columns(ObDMLStmt& stmt, const ObRawExpr* part_expr,
      common::ObIArray<ColumnItem>& partition_columns, common::ObIArray<ColumnItem>& gen_cols, ObRawExpr*& gen_col_expr,
      RowDesc& row_desc, RowDesc& gen_row_desc, const bool only_gen_cols = false);

  // add partition column
  // add column to row desc and partiton columns
  int add_partition_column(ObDMLStmt& stmt, const uint64_t table_id, const uint64_t column_id,
      common::ObIArray<ColumnItem>& partition_columns, RowDesc& row_desc);

  int record_not_insert_dml_partition_info(ObDMLStmt& stmt, const share::schema::ObTableSchema* table_schema,
      const common::ObIArray<ObRawExpr*>& filter_exprs, const common::ObDataTypeCastParams& dtc_params);

  int record_insert_partition_info(
      ObDMLStmt& stmt, const share::schema::ObTableSchema* table_schema, ObSQLSessionInfo* session_info);

  // record part info for insert stmt
  //@param[in]: partition_columns, the partition columns of some part level
  //@param[out]: key_exprs, record insert values for calc column conv expr of partition column
  //@params[out]: key_conv_exprs, record column conv expr of partition column
  //@params[out]: insert_up_exprs, record expr of partition column in insert_up assignments
  //@params[out]: update_set_key_num, record partition column num in insert_up assignment
  int record_insert_part_info(ObInsertStmt& insert_stmt, ObSQLSessionInfo* session_info,
      const common::ObIArray<ColumnItem>& partition_columns, IKeyExprs& key_exprs, IKeyExprs& key_conv_exprs);
  int gen_insert_part_row_projector(const ObRawExpr* partition_raw_expr,
      const common::ObIArray<ColumnItem>& partition_columns, share::schema::ObPartitionLevel part_level,
      share::schema::ObPartitionFuncType part_type);
  //
  int add_key_conv_expr(ObInsertStmt& insert_stmt, ObSQLSessionInfo* session_info, RowDesc& value_row_desc,
      RowDesc& extra_row_desc, const uint64_t column_id, IKeyExprs& key_conv_exprs);

  int add_key_col_idx_in_val_desc(const common::ObIArray<ObColumnRefRawExpr*>& value_desc, const uint64_t column_id,
      common::ObIArray<int64_t>& value_need_idx);

  /// Add partition key expr.
  int add_key_expr(IKeyExprs& key_exprs, ObRawExpr* expr);
  int add_key_expr_with_row_desc(IKeyExprs& key_exprs, RowDesc& row_desc, ObRawExpr* expr);
  //////end of

  int clear_columnlized_in_row_desc(RowDesc& row_desc);

  int calc_partition_ids_by_stored_expr(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
      common::ObIArray<int64_t>& partition_ids, const common::ObDataTypeCastParams& dtc_params) const;

  //  int calc_partition_ids_by_sub_select(ObExecContext &exec_ctx,
  //                                       common::ObPartMgr *part_mgr,
  //                                       common::ObIArray<int64_t> &partition_ids) const;

  int calc_partition_ids_by_ranges(ObExecContext& exec_ctx, const common::ObIArray<common::ObNewRange*>& ranges,
      common::ObIArray<int64_t>& partition_ids) const;

  // gen_col_node: partition information of dependented column of generated partition column
  // gen_col_expr: expression of dependented column of generated partition column
  int calc_partition_ids_by_calc_node(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPartLocCalcNode* calc_node, const ObPartLocCalcNode* gen_col_node, const ObSqlExpression* gen_col_expr,
      const bool part_col_get_all, common::ObIArray<int64_t>& partition_ids,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_partition_ids_by_calc_node(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPartLocCalcNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_and_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPLAndNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_or_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPLOrNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_query_range_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPLQueryRangeNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids,
      const ObSqlExpression* part_expr = NULL) const;

  int calc_func_value_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPLFuncValueNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_column_value_partition_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const ParamStore& params,
      const ObPLColumnValueNode* calc_node, common::ObIArray<int64_t>& partition_ids, bool& all_part,
      const common::ObDataTypeCastParams& dtc_params, const common::ObIArray<int64_t>* part_ids = NULL) const;

  int calc_partition_id_by_func_value(common::ObExprCtx& expr_ctx, common::ObPartMgr* part_mgr,
      const common::ObObj& func_value, const bool calc_oracle_hash, common::ObIArray<int64_t>& partition_ids,
      const common::ObIArray<int64_t>* part_ids = NULL, int64_t* part_idx = NULL) const;

  /// Get all partition ids with virtual table considered.
  int get_all_part_ids(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, common::ObIArray<int64_t>& partition_ids,
      const common::ObIArray<int64_t>* part_ids = NULL) const;

  /// Get partition key row desc for generating partition expr
  int deal_partition_selection(common::ObIArray<int64_t>& partition_ids) const;

  int get_location_calc_node(common::ObIArray<ColumnItem>& partition_columns, const ObRawExpr* partition_expr,
      const common::ObIArray<ObRawExpr*>& filter_exprs, ObPartLocCalcNode*& res_node, bool& get_all,
      const common::ObDataTypeCastParams& dtc_params);

  int analyze_filter(const common::ObIArray<ColumnItem>& partition_columns, const ObRawExpr* partition_expr,
      uint64_t column_id, const ObRawExpr* filter, bool& always_true, ObPartLocCalcNode*& calc_node,
      bool& cnt_func_expr, const common::ObDataTypeCastParams& dtc_params);

  int get_query_range_node(const ColumnIArray& range_columns, const common::ObIArray<ObRawExpr*>& filter_exprs,
      bool& always_true, ObPartLocCalcNode*& calc_node, const common::ObDataTypeCastParams& dtc_params);

  int extract_eq_op(const ObRawExpr* l_expr, const ObRawExpr* r_expr, const ObRawExpr* partition_expr,
      const uint64_t column_id, const ObExprResType& res_type, bool& cnt_func_expr, bool& always_true,
      ObPartLocCalcNode*& calc_node);

  int check_expr_equal(
      const ObRawExpr* partition_expr, const ObRawExpr* check_expr, bool& equal, ObExprEqualCheckContext& equal_ctx);

  int add_and_node(ObPartLocCalcNode* l_node, ObPartLocCalcNode*& r_node);

  int add_or_node(ObPartLocCalcNode* l_node, ObPartLocCalcNode*& r_node);

  int calc_partition_ids_by_ranges(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
      const common::ObIArray<common::ObNewRange*>& ranges, const bool is_all_single_value_ranges,
      common::ObIArray<int64_t>& partition_ids, bool& all_part, const common::ObIArray<int64_t>* part_ids,
      const ObSqlExpression* gen_col_expr) const;

  int get_part_ids_by_ranges(common::ObPartMgr* part_mgr, const common::ObIArray<common::ObNewRange*>& ranges,
      common::ObIArray<int64_t>& partition_ids, const common::ObIArray<int64_t>* part_ids) const;

  int calc_partition_id_by_row(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, common::ObNewRow& row,
      common::ObIArray<int64_t>& partition_ids, const common::ObIArray<int64_t>* part_ids = NULL) const;
  int calc_partition_ids_by_rowkey(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
      const common::ObIArray<ObRowkey>& rowkeys, common::ObIArray<int64_t>& part_ids,
      common::ObIArray<RowkeyArray>& rowkey_lists) const;

  int init_row(common::ObIAllocator& allocator, const int64_t column_num, common::ObNewRow& row) const;

  int calc_row(common::ObExprCtx& expr_ctx, const IKeyExprs& key_exprs, const int64_t key_count,
      const int64_t value_idx, common::ObNewRow& intput_row, common::ObNewRow& output_row) const;

  int calc_up_key_exprs_partition_id(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr, const IKeyExprs& key_exprs,
      const int64_t expect_idx, bool& cross_part, const int64_t part_idx = common::OB_INVALID_INDEX) const;
  int if_exprs_contain_column(const ObIArray<ObRawExpr*>& filter_exprs, bool& contain_column);

  int set_location_calc_node(ObDMLStmt& stmt, const common::ObIArray<ObRawExpr*>& filter_exprs,
      const share::schema::ObPartitionLevel part_level, const common::ObDataTypeCastParams& dtc_params,
      ObSqlExpression*& part_expr, ObSqlExpression*& gen_col_expr, bool& is_col_part_expr,
      ObPartLocCalcNode*& calc_node, ObPartLocCalcNode*& gen_col_node, bool& get_all);

  int calc_partition_ids_by_in_expr(ObExecContext& exec_ctx, common::ObPartMgr* part_mgr,
      ObIArray<int64_t>& partition_ids, const common::ObDataTypeCastParams& dtc_params) const;

  int record_in_dml_partition_info(ObDMLStmt& stmt, const common::ObIArray<ObRawExpr*>& filter_exprs, bool& hit,
      const share::schema::ObTableSchema* table_schema);
  int get_part_col_type(const ObRawExpr* expr, ObObjType& col_type, ObCollationType& collation_type,
      const share::schema::ObTableSchema* table_schema);
  int convert_row_obj_type(const ObNewRow& from, ObNewRow& to, ObObjType col_type, ObCollationType collation_type,
      const common::ObDataTypeCastParams& dtc_params, bool& is_all, bool& is_none) const;
  //  int calc_insert_select_partition_rule(share::schema::ObSchemaGetterGuard &schema_guard,
  //                                        const ObInsertStmt &insert_stmt, const ObSelectStmt &sub_select);
  //  int calc_sub_select_partition_ids(ObExecContext &exec_ctx, ObIArray<int64_t> &partition_ids) const;
  ObRawExpr* get_related_part_expr(
      const ObDMLStmt& stmt, share::schema::ObPartitionLevel part_level, uint64_t table_id, uint64_t index_tid) const;
  int init_fast_calc_info();
  int ob_write_row_with_cast_timestamp_ltz(common::ObIAllocator& allocator, const ObNewRow& src, ObNewRow& dst,
      const common::ObPartitionKeyInfo& part_key_info, const common::ObTimeZoneInfo* tz_info);
  bool is_simple_insert_or_replace() const
  {
    return simple_insert_;
  }

  void set_simple_insert_or_replace()
  {
    simple_insert_ = true;
  }
  int can_get_part_by_range_for_range_columns(const ObRawExpr* part_expr, bool& is_valid) const;
  int recursive_convert_generated_column(const ObIArray<ObColumnRefRawExpr*>& table_column,
      const ObIArray<ObRawExpr*>& column_conv_exprs, ObRawExpr*& expr);

private:
  bool inited_;
  uint64_t table_id_;
  uint64_t ref_table_id_;
  bool is_partitioned_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  bool part_get_all_;     // get all parts with information of partition column
  bool subpart_get_all_;  // get all subparts with information of sub-partition column
  bool is_col_part_expr_;
  bool is_col_subpart_expr_;
  // Whether it is a temporary table in oracle mode, call different hash calculation
  // functions according to this, because is_oracle_mode() is unreliable in inner SQL
  bool is_oracle_temp_table_;
  int64_t tablet_size_;
  common::ObArenaAllocator inner_allocator_;
  common::ObIAllocator& allocator_;      // used for deep copy other table location
  ObPartLocCalcNode* calc_node_;         // First-level partition partition calculation node
  ObPartLocCalcNode* gen_col_node_;      // query range node of columns dependented by generated column
  ObPartLocCalcNode* subcalc_node_;      // Secondary partition partition calculation Node
  ObPartLocCalcNode* sub_gen_col_node_;  // query range node of column dependented by sub partition generated column

  common::ObSEArray<ObPartLocCalcNode*, 5, common::ModulePageAllocator, false> calc_nodes_;
  ObSqlExpressionFactory sql_expression_factory_;
  ObExprOperatorFactory expr_op_factory_;
  ObSqlExpression* part_expr_;
  ObSqlExpression* gen_col_expr_;
  ObSqlExpression* subpart_expr_;
  ObSqlExpression* sub_gen_col_expr_;
  bool simple_insert_;  // insert [replace] into values ...
  stmt::StmtType stmt_type_;
  stmt::StmtType literal_stmt_type_;
  ObConsistencyLevel hint_read_consistency_;
  bool is_contain_inner_table_;
  bool is_contain_select_for_update_;
  bool is_contain_mv_;
  // stored for insert\update calc part_ids.
  // For insert stmt, key exprs stores the expr of values. It also needs key_conv_expr to do the corresponding
  // conversion
  KeyExprs key_exprs_;
  KeyExprs subkey_exprs_;
  int64_t num_values_;

  // For insert stmt
  KeyExprs key_conv_exprs_;     // record partition key column conv exprs
  KeyExprs subkey_conv_exprs_;  // record subpartition key column conv exprs
  common::ObSEArray<int64_t, 5, common::ModulePageAllocator, false> part_hint_ids_;
  int64_t part_num_;              // For virtual table and systable now.
  ObOrderDirection direction_;    // Just to save the direction in the plan cache
  ObQueryRange pre_query_range_;  // query range for the table scan
  uint64_t index_table_id_;
  ObObjType part_col_type_;
  ObCollationType part_collation_type_;
  ObObjType subpart_col_type_;
  ObCollationType subpart_collation_type_;
  int64_t first_partition_id_;
  bool is_in_hit_;
  PartProjector part_projector_;
  bool is_global_index_;
  int32_t related_part_expr_idx_;     // Get the number of expr from related_part_expr_arrays_ in ob_dml_stmt
  int32_t related_subpart_expr_idx_;  // Same as above, for secondary partition

  // Partition rules used to cache the list partition, so that the part_id of
  // the list partition does not need to be searched for the schema
  bool use_list_part_map_;
  int64_t list_default_part_id_;

  // The partitioning rules of the cache range/hash partition in oracle mode
  // are also optimized to no longer look up the schema. When the cache is
  // subsequently used, only equivalent condition search is currently supported.
  bool use_hash_part_opt_;
  bool use_range_part_opt_;
  // Single-key list/hash partition, if partition column=constant filter
  // in oracle mode, then the short path is directly entered when calculating the partition id
  FastCalcType fast_calc_part_opt_;
  ObDuplicateType duplicate_type_;
  int64_t first_part_num_;      // for hash part id calculation
  int64_t partition_num_;       // for range part id calculation
  ObObj* range_obj_arr_;        // Save the high val defined by the range partition
  int64_t* range_part_id_arr_;  // Save the part id of the range partition
                                // (the partition will no longer be 0~n after the partition is split)
  typedef common::hash::ObPointerHashMap<ObListPartMapKey, ObListPartMapValue*, ObGetListPartMapKey> ListPartMap;
  ListPartMap list_part_map_;
  common::ObArray<ObListPartMapValue> list_part_array_;

  typedef common::hash::ObPointerHashMap<ObHashPartMapKey, ObHashPartMapValue*, ObGetHashPartMapKey> HashPartMap;
  HashPartMap hash_part_map_;
  common::ObArray<ObHashPartMapValue> hash_part_array_;
  // When fast_calc_part_opt_ is satisfied, use this subscript to record the subscript
  // of the param_store parameter involved in the partition expression
  ObSEArray<int64_t, 4> part_expr_param_idxs_;

  bool use_calc_part_by_rowid_;
  bool is_valid_range_columns_part_range_;
  bool is_valid_range_columns_subpart_range_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
