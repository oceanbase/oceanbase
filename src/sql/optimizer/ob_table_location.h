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
#include "sql/das/ob_das_define.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/ob_phy_table_location.h"
#include "sql/code_generator/ob_column_index_provider.h"
#include "sql/optimizer/ob_phy_table_location_info.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "sql/das/ob_das_location_router.h"
//#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase
{
namespace share
{
  class ObLSLocation;
}
namespace sql
{
class ObRawExpr;
class ObRawExprFactory;
class ObColumnRefRawExpr;
class ObExprEqualCheckContext;
class ObDASTabletMapper;
class ObDASCtx;
class DASRelatedTabletMap;
typedef common::ObSEArray<int64_t, 1> RowkeyArray;
class ObPartIdRowMapManager
{
public:
  ObPartIdRowMapManager()
    : manager_(), part_idx_(common::OB_INVALID_INDEX) {}
  typedef common::ObSEArray<int64_t, 12> ObRowIdList;
  struct MapEntry
  {
  public:
    MapEntry(): list_() { }
    TO_STRING_KV(K_(list));
    int assign(const MapEntry &entry);
  public:
    ObRowIdList list_;
  };
  typedef common::ObSEArray<MapEntry, 1> ObPartRowManager;
  int add_row_for_part(int64_t part_idx, int64_t row_id);
  const ObRowIdList* get_row_id_list(int64_t part_index);
  void reset() { manager_.reset(); part_idx_ = common::OB_INVALID_INDEX; }
  int64_t get_part_count() const { return manager_.count(); }
  int assign(const ObPartIdRowMapManager &other);
  int64_t get_part_idx() const { return part_idx_; }
  void set_part_idx(int64_t part_idx) { part_idx_ = part_idx; }
  const MapEntry &at(int64_t i) const { return manager_.at(i); }
  common::ObNewRow &get_part_row() { return part_row_; }
  TO_STRING_KV(K_(manager), K_(part_idx));
private:
  ObPartRowManager manager_;
  int64_t part_idx_;//used for parameter pass only.
  common::ObNewRow part_row_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPartIdRowMapManager);
};

enum ValueExprType {
  INVILID_TYPE,
  QUESTMARK_TYPE,
  CONST_OBJ_TYPE,
  CONST_EXPR_TYPE
};

struct ValueItemExpr {

public:
  ValueItemExpr() : type_(INVILID_TYPE), idx_(0),
                    dst_type_(ObMaxType), dst_cs_type_(CS_TYPE_INVALID),
                    enum_set_values_cnt_(0), enum_set_values_(NULL)
  {}

  int deep_copy(common::ObIAllocator &allocator, ValueItemExpr &dst) const;
  TO_STRING_KV(K_(type), K_(idx), K_(dst_type), K_(dst_cs_type), K_(enum_set_values_cnt));

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &alloc, const char *buf,
                  const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

public:
  ValueExprType type_;
  union {
    uint64_t idx_;
    ObTempExpr *expr_;
  };
  ObObj obj_;

  ObObjType dst_type_;
  ObCollationType dst_cs_type_;
  uint16_t enum_set_values_cnt_;
  common::ObString *enum_set_values_;
};

class ObInsertStmt;
struct ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  enum NodeType
  {
    INVALID = 0,
    CALC_AND,
    CALC_OR,
    QUERY_RANGE,
    FUNC_VALUE,
    COLUMN_VALUE,
  };

  ObPartLocCalcNode (common::ObIAllocator &allocator): node_type_(INVALID), allocator_(allocator)
  { }
  virtual ~ObPartLocCalcNode()=default;

  inline void set_node_type(NodeType node_type) { node_type_ = node_type; }
  inline NodeType get_node_type() const { return node_type_; }

  inline bool is_and_node() const { return CALC_AND == node_type_; }
  inline bool is_or_node() const { return CALC_OR == node_type_; }
  inline bool is_query_range_node() const { return QUERY_RANGE == node_type_; }
  inline bool is_func_value_node() const { return FUNC_VALUE == node_type_; }
  inline bool is_column_value_node() const { return COLUMN_VALUE == node_type_; }

  static ObPartLocCalcNode *create_part_calc_node(common::ObIAllocator &allocator,
                                                  common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                                                  ObPartLocCalcNode::NodeType type);

  static int create_part_calc_node(common::ObIAllocator &allocator,
                                   ObPartLocCalcNode::NodeType type,
                                   ObPartLocCalcNode *&calc_node);

  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const = 0;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes) = 0;

  TO_STRING_KV(K_(node_type));

  NodeType node_type_;
  // for deserialize
  common::ObIAllocator &allocator_;
};

struct ObPLAndNode : public ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPLAndNode(common::ObIAllocator &allocator)
    : ObPartLocCalcNode(allocator), left_node_(NULL), right_node_(NULL)
  {
    set_node_type(CALC_AND);
  }

  virtual ~ObPLAndNode()
  { }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes);
  ObPartLocCalcNode *left_node_;
  ObPartLocCalcNode *right_node_;
};

struct ObPLOrNode : public ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPLOrNode(common::ObIAllocator &allocator) : ObPartLocCalcNode(allocator),
      left_node_(NULL), right_node_(NULL)
  {
    set_node_type(CALC_OR);
  }

  virtual ~ObPLOrNode()
  { }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes);
  ObPartLocCalcNode *left_node_;
  ObPartLocCalcNode *right_node_;
};

struct ObPLQueryRangeNode : public ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPLQueryRangeNode(common::ObIAllocator &allocator)
    : ObPartLocCalcNode(allocator), pre_query_range_(allocator)
  { set_node_type(QUERY_RANGE); }
  virtual ~ObPLQueryRangeNode()
  { pre_query_range_.reset(); }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes);
  ObQueryRange pre_query_range_;
};

struct ObPLFuncValueNode : public ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  struct ParamValuePair
  {
    OB_UNIS_VERSION_V(1);
  public:
    ParamValuePair(int64_t param_idx, const common::ObObj obj_val)
        : param_idx_(param_idx), obj_value_(obj_val)
    { }
    ParamValuePair() : param_idx_(-1), obj_value_()
    { }
    virtual ~ParamValuePair() {}
    TO_STRING_KV(K_(param_idx), K_(obj_value));
    int64_t param_idx_;
    common::ObObj obj_value_;
  };

  ObPLFuncValueNode(common::ObIAllocator &allocator)
    : ObPartLocCalcNode(allocator)
  {
    set_node_type(FUNC_VALUE);
  }
  virtual ~ObPLFuncValueNode()
  { }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes);
  ValueItemExpr vie_;
  common::ObSEArray<ParamValuePair, 3, common::ModulePageAllocator, false> param_value_;
};

struct ObPLColumnValueNode : public ObPartLocCalcNode
{
  OB_UNIS_VERSION_V(1);
public:
  ObPLColumnValueNode(common::ObIAllocator &allocator) :
      ObPartLocCalcNode(allocator)
  {
    set_node_type(COLUMN_VALUE);
  }

  virtual ~ObPLColumnValueNode()
  { }
  virtual int deep_copy(common::ObIAllocator &allocator,
                        common::ObIArray<ObPartLocCalcNode*> &calc_nodes,
                        ObPartLocCalcNode *&other) const;
  virtual int add_part_calc_node(common::ObIArray<ObPartLocCalcNode*> &calc_nodes);

  ValueItemExpr vie_;
};

struct ObListPartMapKey {
  common::ObNewRow row_;

  int hash(uint64_t &hash_val) const;
  bool operator==(const ObListPartMapKey &other) const;
  TO_STRING_KV(K_(row));
};

struct ObListPartMapValue
{
  ObListPartMapKey key_;
  int64_t part_id_;

  TO_STRING_KV(K_(key), K_(part_id));
};

template<class T, class V>
struct ObGetListPartMapKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};


template<>
struct ObGetListPartMapKey<ObListPartMapKey, ObListPartMapValue *>
{
  ObListPartMapKey operator()(const ObListPartMapValue *value) const
  {
    return value->key_;
  }
};

struct ObHashPartMapKey {
  int64_t part_idx_;

  int64_t hash() const;
  bool operator==(const ObHashPartMapKey &other) const;
  TO_STRING_KV(K_(part_idx));
};

struct ObHashPartMapValue
{
  ObHashPartMapKey key_;
  int64_t part_id_;

  TO_STRING_KV(K_(key), K_(part_id));
};

template<class T, class V>
struct ObGetHashPartMapKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetHashPartMapKey<ObHashPartMapKey, ObHashPartMapValue *>
{
  ObHashPartMapKey operator()(const ObHashPartMapValue *value) const
  {
    return value->key_;
  }
};

struct TableLocationKey
{
  uint64_t table_id_;
  uint64_t ref_table_id_;

  bool operator==(const TableLocationKey &other) const;
  bool operator!=(const TableLocationKey &other) const;

  inline uint64_t hash() const {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&ref_table_id_, sizeof(uint64_t), hash_ret);
    return hash_ret;
  }

  TO_STRING_KV(K_(table_id), K_(ref_table_id));
};

class ObOptimizerContext;
// 默认false=false，array等内存用array内部的分配器，必须显示析构此对象。
// 对于非plan set的调用, 指定为false没有区别，因为必须用传allocator给构造函数，
// 然后对array等在构造函数中将allocator也传入.
class ObSqlSchemaGuard;
class ObTableLocation
{
public:
  enum TablePartType {
    NONE,
    HASH,
    RANGE,
    LIST,
  };
  typedef common::ObIArray<ObTempExpr *> ISeKeyExprs;
  typedef common::ObIArray<ValueItemExpr> ISeValueItemExprs;
  typedef common::ObSEArray<ObTempExpr *, 32, common::ModulePageAllocator, false> SeKeyExprs;
  typedef common::ObSEArray<ValueItemExpr, 32, common::ModulePageAllocator, false> SeValueItemExprs;

  class PartProjector
  {
    OB_UNIS_VERSION(1);
  public:
    PartProjector(common::ObIAllocator &allocator)
      : allocator_(allocator)
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
      se_virtual_column_exprs_.reset();
      virtual_column_result_idx_.reset();
    }
    int deep_copy(const PartProjector &other);
    int init_part_projector(ObExecContext *exec_ctx,
                            const ObRawExpr *part_expr,
                            share::schema::ObPartitionLevel part_level,
                            RowDesc &row_desc);
    int init_part_projector(ObExecContext *exec_ctx,
                            const ObRawExpr *part_expr,
                            RowDesc &row_desc);
    int init_subpart_projector(ObExecContext *exec_ctx,
                               const ObRawExpr *part_expr,
                               RowDesc &row_desc);
    inline void set_column_cnt(int64_t column_cnt) { column_cnt_ = column_cnt; }
    int calc_part_row(ObExecContext &ctx,
                      const common::ObNewRow &input_row,
                      common::ObNewRow *&part_row) const;
    void project_part_row(share::schema::ObPartitionLevel part_level, common::ObNewRow &part_row) const;
    TO_STRING_KV(K_(column_cnt),
                 "part_projector", common::ObArrayWrap<int32_t>(part_projector_, part_projector_size_),
                 "subpart_projector", common::ObArrayWrap<int32_t>(subpart_projector_, subpart_projector_size_));
  private:

    int init_part_projector(ObExecContext *exec_ctx,
                            const ObRawExpr *part_expr,
                            RowDesc &row_desc,
                            int32_t *&projector,
                            int64_t &projector_size);
  private:
    int64_t column_cnt_;
    int32_t *part_projector_;
    int64_t part_projector_size_;
    int32_t *subpart_projector_;
    int64_t subpart_projector_size_;
    common::ObSEArray<ObTempExpr *, 2> se_virtual_column_exprs_;
    common::ObSEArray<int64_t, 2> virtual_column_result_idx_;

    common::ObIAllocator &allocator_;
  };

  int get_location_type(
      const common::ObAddr &server,
      const ObCandiTabletLocIArray &phy_part_loc_info_list,
      ObTableLocationType &location_type) const;

  //get virtual talbe partition ids or fake id. ref_table_id should be partitioned virtual table
  //@param [in] ref_table_id partitioned virtual table
  //@param [out] partition ids. all partition ids
  //@param [out] fake id. Fake id, if has local partition return local part_id
//  static int get_vt_partition_id(
//      ObExecContext &exec_ctx,
//      const uint64_t ref_table_id,
//      common::ObIArray<int64_t> *partition_ids,
//      int64_t *fake_id);
  OB_UNIS_VERSION(1);
public:
  // for array: new(&data_[count_]) T();
  // for normal usage, like plan set
  ObTableLocation()
  : inited_(false),
    is_partitioned_(true),
    part_level_(share::schema::PARTITION_LEVEL_ZERO),
    part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    part_get_all_(false),
    subpart_get_all_(false),
    is_col_part_expr_(false),
    is_col_subpart_expr_(false),
    is_oracle_temp_table_(false),
    table_type_(share::schema::MAX_TABLE_TYPE),
    inner_allocator_(common::ObModIds::OB_SQL_TABLE_LOCATION),
    allocator_(inner_allocator_),
    loc_meta_(inner_allocator_),
    calc_node_(NULL),
    gen_col_node_(NULL),
    subcalc_node_(NULL),
    sub_gen_col_node_(NULL),
    stmt_type_(stmt::T_NONE),
    se_part_expr_(NULL),
    se_gen_col_expr_(NULL),
    se_subpart_expr_(NULL),
    se_sub_gen_col_expr_(NULL),
    part_hint_ids_(allocator_),
    part_col_type_(ObNullType),
    part_collation_type_(CS_TYPE_INVALID),
    subpart_col_type_(ObNullType),
    subpart_collation_type_(CS_TYPE_INVALID),
    is_in_hit_(false),
    part_projector_(allocator_),
    is_valid_range_columns_part_range_(false),
    is_valid_range_columns_subpart_range_(false),
    has_dynamic_exec_param_(false),
    is_valid_temporal_part_range_(false),
    is_valid_temporal_subpart_range_(false),
    is_part_range_get_(false),
    is_subpart_range_get_(false),
    is_non_partition_optimized_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    object_id_(OB_INVALID_ID),
    related_list_(allocator_),
    check_no_partition_(false)
  {
  }

  // 用于优化器等不调析构函数的情况，保证每个成员数组都传入外部的allocator
  ObTableLocation(common::ObIAllocator &allocator)
  : inited_(false),
    is_partitioned_(true),
    part_level_(share::schema::PARTITION_LEVEL_ZERO),
    part_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    subpart_type_(share::schema::PARTITION_FUNC_TYPE_MAX),
    part_get_all_(false),
    subpart_get_all_(false),
    is_col_part_expr_(false),
    is_col_subpart_expr_(false),
    is_oracle_temp_table_(false),
    table_type_(share::schema::MAX_TABLE_TYPE),
    allocator_(allocator),
    loc_meta_(allocator),
    calc_node_(NULL),
    gen_col_node_(NULL),
    subcalc_node_(NULL),
    sub_gen_col_node_(NULL),
    calc_nodes_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
    stmt_type_(stmt::T_NONE),
    vies_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
    sub_vies_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator_)),
    se_part_expr_(NULL),
    se_gen_col_expr_(NULL),
    se_subpart_expr_(NULL),
    se_sub_gen_col_expr_(NULL),
    part_hint_ids_(allocator_),
    part_col_type_(ObNullType),
    part_collation_type_(CS_TYPE_INVALID),
    subpart_col_type_(ObNullType),
    subpart_collation_type_(CS_TYPE_INVALID),
    is_in_hit_(false),
    part_projector_(allocator_),
    is_valid_range_columns_part_range_(false),
    is_valid_range_columns_subpart_range_(false),
    has_dynamic_exec_param_(false),
    is_valid_temporal_part_range_(false),
    is_valid_temporal_subpart_range_(false),
    is_part_range_get_(false),
    is_subpart_range_get_(false),
    is_non_partition_optimized_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    object_id_(OB_INVALID_ID),
    related_list_(allocator_),
    check_no_partition_(false)
  {
  }
  virtual ~ObTableLocation() { reset(); }

  ObTableLocation(const ObTableLocation &other);

  ObTableLocation &operator=(const ObTableLocation &other);

  int assign(const ObTableLocation &other);

  void reset();
  inline bool is_inited() const { return inited_; }
  //get preliminary_query_range and partition_expr. For sql with filters.
  int init_location(ObSqlSchemaGuard *schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const common::ObIArray<ObRawExpr*> &filter_exprs,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const ObIArray<common::ObObjectID> *part_ids,
           const common::ObDataTypeCastParams &dtc_params,
           const bool is_dml_table,
           common::ObIArray<ObRawExpr*> *sort_exprs = NULL);
  int init(share::schema::ObSchemaGetterGuard &schema_guard,
      const ObDMLStmt &stmt,
      ObExecContext *exec_ctx,
      const common::ObIArray<ObRawExpr*> &filter_exprs,
      const uint64_t table_id,
      const uint64_t ref_table_id,
      const common::ObIArray<common::ObObjectID> *part_ids,
      const common::ObDataTypeCastParams &dtc_params,
      const bool is_dml_table,
      common::ObIArray<ObRawExpr*> *sort_exprs = NULL);
  int init(ObSqlSchemaGuard &schema_guard,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const common::ObIArray<ObRawExpr*> &filter_exprs,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const common::ObIArray<common::ObObjectID> *part_ids,
           const common::ObDataTypeCastParams &dtc_params,
           const bool is_dml_table,
           common::ObIArray<ObRawExpr*> *sort_exprs = NULL);
  int init(const share::schema::ObTableSchema *table_schema,
           const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const common::ObIArray<ObRawExpr*> &filter_exprs,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const common::ObIArray<common::ObObjectID> *part_ids,
           const common::ObDataTypeCastParams &dtc_params,
           const bool is_dml_table,
           common::ObIArray<ObRawExpr*> *sort_exprs = NULL);

  static int get_is_weak_read(const ObDMLStmt &dml_stmt,
                              const ObSQLSessionInfo *session_info,
                              const ObSqlCtx *sql_ctx,
                              bool &is_weak_read);

  int send_add_interval_partition_rpc(ObExecContext &exec_ctx,
                                      share::schema::ObSchemaGetterGuard *schema_guard,
                                      ObNewRow &row) const;

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
  int calculate_candi_tablet_locations(
      ObExecContext &exec_ctx,
      const ParamStore &params,
      ObCandiTabletLocIArray &candi_tablet_locs,
      const common::ObDataTypeCastParams &dtc_params) const;

  int calculate_single_tablet_partition(ObExecContext &exec_ctx,
                                        const ParamStore &params,
                                        const ObDataTypeCastParams &dtc_params) const;

  int calculate_final_tablet_locations(ObExecContext &exec_ctx,
                                       const ParamStore &params,
                                       const common::ObDataTypeCastParams &dtc_params) const;

  /**
   * Calculate tablet ids from input parameters.
   */
  int calculate_tablet_ids(ObExecContext &exec_ctx,
                           const ParamStore &params,
                           ObIArray<ObTabletID> &tablet_ids,
                           ObIArray<ObObjectID> &partition_ids,
                           ObIArray<ObObjectID> &first_level_part_ids,
                           const ObDataTypeCastParams &dtc_params) const;

  int init_partition_ids_by_rowkey2(ObExecContext &exec_ctx,
                                    ObSQLSessionInfo &session_info,
                                    ObSchemaGetterGuard &schema_guard,
                                    uint64_t table_id);

  int calculate_partition_ids_by_rows2(ObSQLSessionInfo &session_info,
                                        ObSchemaGetterGuard &schema_guard,
                                        uint64_t table_id,
                                        ObIArray<ObNewRow> &part_rows,
                                        ObIArray<ObTabletID> &tablet_ids,
                                        ObIArray<ObObjectID> &part_ids) const;//FIXME

   int calculate_partition_ids_by_rowkey(ObSQLSessionInfo &session_info,
                                         share::schema::ObSchemaGetterGuard &schema_guard,
                                         uint64_t table_id,
                                         const common::ObIArray<ObRowkey> &rowkeys,
                                         ObIArray<ObTabletID> &tablet_ids,
                                         ObIArray<ObObjectID> &part_ids);

  int calculate_tablet_id_by_row(ObExecContext &exec_ctx,
                                 uint64_t table_id,
                                 const common::ObIArray<uint64_t> &column_ids,
                                 common::ObNewRow &cur_row,
                                 common::ObIArray<common::ObTabletID> &tablet_ids,
                                 common::ObIArray<common::ObObjectID> &partition_ids);

  int get_tablet_locations(ObDASCtx &das_ctx,
                           const ObIArray<ObTabletID> &tablet_ids,
                           const ObIArray<ObObjectID> &partition_ids,
                           const ObIArray<ObObjectID> &first_level_part_ids,
                           ObCandiTabletLocIArray &candi_tablet_locs) const;

  int add_final_tablet_locations(ObDASCtx &das_ctx,
                                 const ObIArray<ObTabletID> &tablet_ids,
                                 const ObIArray<ObObjectID> &partition_ids,
                                 const ObIArray<ObObjectID> &first_level_part_ids) const;

  static int send_add_interval_partition_rpc_new_engine(ObIAllocator &allocator,
                                                        ObSQLSessionInfo *session,
                                                        ObSchemaGetterGuard *schema_guard,
                                                        const ObTableSchema *table_schema,
                                                        ObNewRow &row);

  inline void set_table_id(uint64_t table_id) { loc_meta_.table_loc_id_ = table_id; }

  inline uint64_t get_table_id() const { return loc_meta_.table_loc_id_; }

  inline uint64_t get_ref_table_id() const { return loc_meta_.ref_table_id_; }

  inline const ObDASTableLocMeta &get_loc_meta() const { return loc_meta_; }
  inline ObDASTableLocMeta &get_loc_meta() { return loc_meta_; }

  bool is_partitioned() const { return is_partitioned_; }

  void set_is_non_partition_optimized(bool is_non_partition_optimized) {
    is_non_partition_optimized_ = is_non_partition_optimized;
  }

  bool is_non_partition_optimized() const { return is_non_partition_optimized_; }

  share::schema::ObPartitionLevel get_part_level() const { return part_level_; }

  const stmt::StmtType &get_stmt_type() const { return stmt_type_; }
  int replace_ref_table_id(const uint64_t index_table_id, ObExecContext &exec_ctx);

  int get_not_insert_dml_part_sort_expr(const ObDMLStmt &stmt,
                                        common::ObIArray<ObRawExpr *> *sort_exprs) const;
  bool has_generated_column() const { return NULL != se_gen_col_expr_ || NULL != se_sub_gen_col_expr_ ||
                                             NULL != gen_col_node_ || NULL != sub_gen_col_node_; }
  static int get_full_leader_table_loc(ObDASLocationRouter &loc_router,
                                       ObIAllocator &allocator,
                                       uint64_t tenant_id,
                                       uint64_t table_id,
                                       uint64_t ref_table_id,
                                       ObDASTableLoc *&table_loc);
  bool is_duplicate_table() const { return loc_meta_.is_dup_table_; }
  bool is_duplicate_table_not_in_dml() const
  { return loc_meta_.is_dup_table_ && !loc_meta_.select_leader_; }
  void set_duplicate_type(ObDuplicateType v) { duplicate_type_to_loc_meta(v, loc_meta_); }
  ObDuplicateType get_duplicate_type() const { return loc_meta_to_duplicate_type(loc_meta_); }
  int deal_dml_partition_selection(ObObjectID part_id) const;
  int add_part_hint_ids(const ObIArray<ObObjectID> &part_ids) {
    return append_array_no_dup(part_hint_ids_, part_ids);
  }

  int get_partition_ids_by_range(ObExecContext &exec_ctx,
                                 const ObNewRange *part_range,
                                 const ObNewRange *gen_range,
                                 ObIArray<ObTabletID> &tablet_ids,
                                 ObIArray<ObObjectID> &partition_ids,
                                 const ObIArray<ObObjectID> *level_one_part_ids) const;

  int calc_partition_ids_by_range(ObExecContext &exec_ctx,
                                  ObDASTabletMapper &tablet_mapper,
                                  const ObNewRange *range,
                                  ObIArray<ObTabletID> &tablet_ids,
                                  ObIArray<ObObjectID> &partition_ids,
                                  bool &all_part,
                                  const ObIArray<ObObjectID> *part_ids,
                                  const ObTempExpr *se_gen_col_expr) const;

  int generate_row_desc_from_row_desc(const ObDMLStmt &stmt,
                                      const uint64_t data_table_id,
                                      ObRawExprFactory &expr_factory,
                                      const RowDesc &input_row_desc,
                                      RowDesc &row_desc);

  void set_use_das(bool use_das) { loc_meta_.use_dist_das_ = use_das; }
  bool use_das() const { return loc_meta_.use_dist_das_; }

  inline bool is_all_partition() const
  {
    return (part_level_ == share::schema::PARTITION_LEVEL_ZERO) ||
      (part_get_all_ && (part_level_ == share::schema::PARTITION_LEVEL_ONE)) ||
      (part_get_all_ && subpart_get_all_ && (part_level_ == share::schema::PARTITION_LEVEL_TWO));
  }

  inline bool is_part_or_subpart_all_partition() const
  {
    return (part_level_ == share::schema::PARTITION_LEVEL_ZERO) ||
           (part_level_ == share::schema::PARTITION_LEVEL_ONE && (part_get_all_ || !is_part_range_get_)) ||
           (part_level_ == share::schema::PARTITION_LEVEL_TWO && (subpart_get_all_ || part_get_all_ || !is_part_range_get_ || !is_subpart_range_get_));
  }

  void set_has_dynamic_exec_param(bool flag) {  has_dynamic_exec_param_ = flag; }
  bool get_has_dynamic_exec_param() const {  return has_dynamic_exec_param_; }

  int pruning_single_partition(int64_t partition_id,
      ObExecContext &exec_ctx, bool &pruning,
      common::ObIArray<int64_t> &partition_ids);

  int init_table_location_with_column_ids(ObSqlSchemaGuard &schema_guard,
                                          uint64_t table_id,
                                          const common::ObIArray<uint64_t> &column_ids,
                                          ObExecContext &exec_ctx,
                                          const bool is_dml_table = true);

  int calc_not_partitioned_table_ids(ObExecContext &exec_ctx);
  void set_check_no_partition(const bool check)
  {
    check_no_partition_ = check;
  }
  TO_STRING_KV(K_(loc_meta),
               K_(part_projector),
               K_(has_dynamic_exec_param),
               K_(part_hint_ids),
               K_(related_list));
private:
  int init_table_location(ObExecContext &exec_ctx,
                          ObSqlSchemaGuard &schema_guard,
                          uint64_t table_id,
                          uint64_t ref_table_id,
                          const ObDMLStmt &stmt,
                          const RowDesc &row_desc,
                          const bool is_dml_table,
                          const ObOrderDirection &direction = default_asc_direction());

  int init_table_location_with_rowkey(ObSqlSchemaGuard &schema_guard,
                                      uint64_t table_id,
                                      ObExecContext &exec_ctx,
                                      const bool is_dml_table = true);

  //gen_col_node: partition information of dependented column of generated partition column
  //gen_col_expr: expression of dependented column of generated partition column
  int calc_partition_ids_by_calc_node(ObExecContext &exec_ctx,
                                      ObDASTabletMapper &tablet_mapper,
                                      const ParamStore &params,
                                      const ObPartLocCalcNode *calc_node,
                                      const ObPartLocCalcNode *gen_col_node,
                                      const ObTempExpr *se_gen_col_expr,
                                      const bool part_col_get_all,
                                      common::ObIArray<common::ObTabletID> &tablet_ids,
                                      common::ObIArray<common::ObObjectID> &partition_ids,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;

  int calc_and_partition_ids(ObExecContext &exec_ctx,
                             ObDASTabletMapper &tablet_mapper,
                             const ParamStore &params,
                             const ObPLAndNode *calc_node,
                             common::ObIArray<common::ObTabletID> &tablet_ids,
                             common::ObIArray<common::ObObjectID> &partition_ids,
                             bool &all_part,
                             const common::ObDataTypeCastParams &dtc_params,
                             const common::ObIArray<ObObjectID> *part_ids = NULL) const;

  int calc_or_partition_ids(ObExecContext &exec_ctx,
                            ObDASTabletMapper &tablet_mapper,
                            const ParamStore &params,
                            const ObPLOrNode *calc_node,
                            common::ObIArray<common::ObTabletID> &tablet_ids,
                            common::ObIArray<common::ObObjectID> &partition_ids,
                            bool &all_part,
                            const common::ObDataTypeCastParams &dtc_params,
                            const common::ObIArray<ObObjectID> *part_ids = NULL) const;

  int calc_query_range_partition_ids(ObExecContext &exec_ctx,
                                     ObDASTabletMapper &tablet_mapper,
                                     const ParamStore &params,
                                     const ObPLQueryRangeNode *calc_node,
                                     common::ObIArray<common::ObTabletID> &tablet_ids,
                                     common::ObIArray<common::ObObjectID> &partition_ids,
                                     bool &all_part,
                                     const common::ObDataTypeCastParams &dtc_params,
                                     const common::ObIArray<common::ObObjectID> *part_ids,
                                     const ObTempExpr *temp_expr) const;

  int calc_func_value_partition_ids(ObExecContext &exec_ctx,
                                    ObDASTabletMapper &tablet_mapper,
                                    const ParamStore &params,
                                    const ObPLFuncValueNode *calc_node,
                                    common::ObIArray<common::ObTabletID> &tablet_ids,
                                    common::ObIArray<common::ObObjectID> &partition_ids,
                                    bool &all_part,
                                    const common::ObDataTypeCastParams &dtc_params,
                                    const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;

  int calc_column_value_partition_ids(ObExecContext &exec_ctx,
                                      ObDASTabletMapper &tablet_mapper,
                                      const ParamStore &params,
                                      const ObPLColumnValueNode *calc_node,
                                      common::ObIArray<common::ObTabletID> &tablet_ids,
                                      common::ObIArray<common::ObObjectID> &partition_ids,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      const common::ObIArray<ObObjectID> *part_ids = NULL) const;

  int calc_partition_id_by_func_value(ObDASTabletMapper &tablet_mapper,
                                      const common::ObObj &func_value,
                                      const bool calc_oracle_hash,
                                      common::ObIArray<common::ObTabletID> &tablet_ids,
                                      common::ObIArray<common::ObObjectID> &partition_ids,
                                      const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;


  int calc_partition_ids_by_calc_node(ObExecContext &exec_ctx,
                                      ObDASTabletMapper &tablet_mapper,
                                      const ParamStore &params,
                                      const ObPartLocCalcNode *calc_node,
                                      common::ObIArray<common::ObTabletID> &tablet_ids,
                                      common::ObIArray<common::ObObjectID> &partition_ids,
                                      bool &all_part,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;

  //get partition columns and generate partition_expr_
  //partition_columns:partition columns
  //gen_cols: columns dependented by generated partition column
  //partition_raw_expr: raw expr of partition
  //gen_col_expression: dependented expression of generated partition column
  int get_partition_column_info(const ObDMLStmt &stmt,
                                const share::schema::ObPartitionLevel part_level,
                                common::ObIArray<ColumnItem> &partition_columns,
                                common::ObIArray<ColumnItem> &gen_cols,
                                const ObRawExpr *&partition_raw_expr,
                                bool &is_col_part_expr,
                                ObTempExpr *&part_temp_expr,
                                ObTempExpr *&gen_col_temp_expr,
                                ObExecContext *exec_ctx);
  int generate_rowkey_desc(const ObDMLStmt &stmt,
                           const common::ObIArray<uint64_t> &column_ids,
                           uint64_t data_table_id,
                           ObRawExprFactory &expr_factory,
                           RowDesc &row_desc);

  //对partition ids和to_inter_ids取交集,结果存储在partition_ids中
  template <typename T>
  int intersect_partition_ids(const common::ObIArray<T> &to_inter_ids,
                              common::ObIArray<T> &partition_ids) const;

  //add partition columns.
  //For insert stmt with generated column,
  //the dependent columns would added to partitoin columns.
  //gen_cols: 对非insert语句，单column的partition expr存储generated_column的dependent column
  //gen_col_expr: generated_column的dependent expr
  int add_partition_columns(const ObDMLStmt &stmt,
                            const ObRawExpr *part_expr,
                            common::ObIArray<ColumnItem> &partition_columns,
                            common::ObIArray<ColumnItem> &gen_cols,
                            ObRawExpr *&gen_col_expr,
                            RowDesc &row_desc,
                            RowDesc &gen_row_desc,
                            const bool only_gen_cols = false);
  int check_can_replace(ObRawExpr *gen_col_expr,
                        ObRawExpr *col_expr,
                        bool &can_replace);
  //add partition column
  //add column to row desc and partition columns
  int add_partition_column(const ObDMLStmt &stmt,
                           const uint64_t table_id,
                           const uint64_t column_id,
                           common::ObIArray<ColumnItem> &partition_columns,
                           RowDesc &row_desc);

  int record_not_insert_dml_partition_info(const ObDMLStmt &stmt,
                                           ObExecContext *exec_ctx,
                                           const share::schema::ObTableSchema *table_schema,
                                           const common::ObIArray<ObRawExpr*> &filter_exprs,
                                           const common::ObDataTypeCastParams &dtc_params,
                                           const bool is_in_range_optimization_enabled);

  int add_se_value_expr(const ObRawExpr *value_expr,
                        RowDesc &value_row_desc,
                        int64_t expr_idx,
                        ObExecContext *exec_ctx,
                        ISeValueItemExprs &vies);

  int clear_columnlized_in_row_desc(RowDesc &row_desc);

  ///Get all partition ids with virtual table considered. by qianfu.zpf and tingshuai.yts
  int get_all_part_ids(ObDASTabletMapper &tablet_mapper,
                       common::ObIArray<common::ObTabletID> &tablet_ids,
                       common::ObIArray<common::ObObjectID> &partition_ids,
                       const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;

  ///Get partition key row desc for generating partition expr
  int deal_partition_selection(common::ObIArray<common::ObTabletID> &tablet_ids,
                               common::ObIArray<common::ObObjectID> &part_ids) const;

  int get_location_calc_node(const ObPartitionLevel part_level,
                             common::ObIArray<ColumnItem> &partition_columns,
                             const ObRawExpr *partition_expr,
                             const common::ObIArray<ObRawExpr*> &filter_exprs,
                             ObPartLocCalcNode *&res_node,
                             bool &get_all,
                             bool &is_range_get,
                             const common::ObDataTypeCastParams &dtc_params,
                             ObExecContext *exec_ctx,
                             const bool is_in_range_optimization_enabled);

  int analyze_filter(const common::ObIArray<ColumnItem> &partition_columns,
                     const ObRawExpr *partition_expr,
                     uint64_t column_id,
                     const ObRawExpr *filter,
                     bool &always_true,
                     ObPartLocCalcNode *&calc_node,
                     bool &cnt_func_expr,
                     const common::ObDataTypeCastParams &dtc_params,
                     ObExecContext *exec_ctx);

  int get_query_range_node(const ObPartitionLevel part_level,
                           const ColumnIArray &range_columns,
                           const common::ObIArray<ObRawExpr*> &filter_exprs,
                           bool &always_true,
                           ObPartLocCalcNode *&calc_node,
                           const common::ObDataTypeCastParams &dtc_params,
                           ObExecContext *exec_ctx,
                           const bool is_in_range_optimization_enabled);

  int extract_eq_op(ObExecContext *exec_ctx,
                    const ObRawExpr *l_expr,
                    const ObRawExpr *r_expr,
                    const uint64_t column_id,
                    const ObRawExpr *partition_expr,
                    const ObExprResType &res_type,
                    bool &cnt_func_expr,
                    bool &always_true,
                    ObPartLocCalcNode *&calc_node);

  int extract_value_item_expr(ObExecContext *exec_ctx,
                              const ObRawExpr *expr,
                              const ObRawExpr *dst_expr,
                              ValueItemExpr &vie);

  int check_expr_equal(const ObRawExpr *partition_expr,
                       const ObRawExpr *check_expr,
                       bool &equal,
                       ObExprEqualCheckContext &equal_ctx);

  int add_and_node(ObPartLocCalcNode *l_node,
                   ObPartLocCalcNode *&r_node);

  int add_or_node(ObPartLocCalcNode *l_node,
                  ObPartLocCalcNode *&r_node);

  int calc_partition_ids_by_ranges(ObExecContext &exec_ctx,
                                   ObDASTabletMapper &tablet_mapper,
                                   const common::ObIArray<common::ObNewRange*> &ranges,
                                   const bool is_all_single_value_ranges,
                                   common::ObIArray<common::ObTabletID> &tablet_ids,
                                   common::ObIArray<common::ObObjectID> &partition_ids,
                                   bool &all_part,
                                   const common::ObIArray<common::ObObjectID> *part_ids,
                                   const ObTempExpr *se_gen_col_expr) const;

  int get_part_ids_by_ranges(ObDASTabletMapper &tablet_mapper,
                             const common::ObIArray<common::ObNewRange*> &ranges,
                             common::ObIArray<common::ObTabletID> &tablet_ids,
                             common::ObIArray<common::ObObjectID> &partition_ids,
                             const common::ObIArray<ObObjectID> *part_ids) const;

  int calc_partition_id_by_row(ObExecContext &exec_ctx,
                               ObDASTabletMapper &tablet_mapper,
                               common::ObNewRow &row,
                               common::ObIArray<common::ObTabletID> &tablet_ids,
                               common::ObIArray<common::ObObjectID> &partition_ids,
                               const common::ObIArray<common::ObObjectID> *part_ids = NULL) const;

  int calc_partition_ids_by_rowkey(ObExecContext &exec_ctx,
                                   ObDASTabletMapper &tablet_mapper,
                                   const common::ObIArray<ObRowkey> &rowkeys,
                                   common::ObIArray<common::ObTabletID> &tablet_ids,
                                   common::ObIArray<common::ObObjectID> &part_ids) const;

  int init_row(common::ObIAllocator &allocator,
               const int64_t column_num,
               common::ObNewRow &row) const;

  int se_calc_value_item(ObCastCtx cast_ctx,
                         ObExecContext &exec_ctx,
                         const ParamStore &params,
                         const ValueItemExpr &vie,
                         ObNewRow &input_row,
                         ObObj &value) const;

  int se_calc_value_item_row(common::ObExprCtx &expr_ctx,
                             ObExecContext &exec_ctx,
                             const ISeValueItemExprs &vies,
                             const int64_t key_count,
                             const int64_t value_idx,
                             common::ObNewRow &input_row,
                             common::ObNewRow &output_row) const;

  int set_location_calc_node(const ObDMLStmt &stmt,
                             const common::ObIArray<ObRawExpr*> &filter_exprs,
                             const share::schema::ObPartitionLevel part_level,
                             const common::ObDataTypeCastParams &dtc_params,
                             ObExecContext *exec_ctx,
                             ObTempExpr *&se_part_expr,
                             ObTempExpr *&se_gen_col_expr,
                             bool &is_col_part_expr,
                             ObPartLocCalcNode *&calc_node,
                             ObPartLocCalcNode *&gen_col_node,
                             bool &get_all,
                             bool &is_range_get,
                             const bool is_in_range_optimization_enabled);

  int calc_partition_ids_by_in_expr(
                   ObExecContext &exec_ctx,
                   ObDASTabletMapper &tablet_mapper,
                   ObIArray<ObTabletID> &tablet_ids,
                   ObIArray<ObObjectID> &partition_ids,
                   const common::ObDataTypeCastParams &dtc_params) const;

  int record_in_dml_partition_info(const ObDMLStmt &stmt,
                                   ObExecContext *exec_ctx,
                                   const common::ObIArray<ObRawExpr*> &filter_exprs,
                                   bool &hit,
                                   const share::schema::ObTableSchema *table_schema);
  int get_part_col_type(const ObRawExpr *expr,
                        ObObjType &col_type,
                        ObCollationType &collation_type,
                        const share::schema::ObTableSchema *table_schema,
                        bool &is_valid);
  int convert_row_obj_type(const ObNewRow &from,
                           ObNewRow &to,
                           ObObjType col_type,
                           ObCollationType collation_type,
                           const common::ObDataTypeCastParams &dtc_params,
                           bool &is_all,
                           bool &is_none) const;

  int can_get_part_by_range_for_range_columns(const ObRawExpr *part_expr, bool &is_valid) const;
  int can_get_part_by_range_for_temporal_column(const ObRawExpr *part_expr, bool &is_valid) const;
  int calc_range_by_part_expr(ObExecContext &exec_ctx,
                              const common::ObIArray<common::ObNewRange*> &ranges,
                              const ObIArray<ObObjectID> *part_ids,
                              common::ObIAllocator &allocator,
                              common::ObIArray<common::ObNewRange*> &new_ranges,
                              bool &is_all_single_value_ranges) const;
  int try_split_integer_range(const common::ObIArray<common::ObNewRange*> &ranges,
                              common::ObIAllocator &allocator,
                              common::ObIArray<common::ObNewRange*> &new_ranges,
                              bool &all_part) const;
  bool is_include_physical_rowid_range(const ObIArray<ObNewRange*> &ranges) const;
  int get_tablet_and_object_id_with_phy_rowid(ObNewRange &range,
                                              ObDASTabletMapper &tablet_mapper,
                                              ObIArray<ObTabletID> &tablet_ids,
                                              ObIArray<ObObjectID> &partition_ids) const;

  int fill_related_tablet_and_object_ids(ObDASTabletMapper &tablet_mapper,
                                         const uint64_t tenant_id,
                                         const ObTabletID src_tablet_id,
                                         const int64_t idx) const;
private:
  bool inited_;
  bool is_partitioned_;
  share::schema::ObPartitionLevel part_level_;
  share::schema::ObPartitionFuncType part_type_;
  share::schema::ObPartitionFuncType subpart_type_;
  bool part_get_all_;//get all parts with information of partition column
  bool subpart_get_all_;//get all subparts with information of sub-partition column
  bool is_col_part_expr_;
  bool is_col_subpart_expr_;
  bool is_oracle_temp_table_;//是否为oracle模式下的临时表, 根据此调用不同的hash计算函数, 因为内部sql时
                             //is_oracle_mode()不可靠
  share::schema::ObTableType table_type_;
  common::ObArenaAllocator inner_allocator_;
  common::ObIAllocator &allocator_; //used for deep copy other table location
  ObDASTableLocMeta loc_meta_;
  ObPartLocCalcNode *calc_node_;//一级分区分区计算node
  ObPartLocCalcNode *gen_col_node_;//query range node of columns dependented by generated column
  ObPartLocCalcNode *subcalc_node_;//二级分区分区计算Node
  ObPartLocCalcNode *sub_gen_col_node_;//query range node of column dependented by sub partition generated column

  common::ObSEArray<ObPartLocCalcNode *, 5, common::ModulePageAllocator, false> calc_nodes_;
  stmt::StmtType stmt_type_;

  SeValueItemExprs vies_;
  SeValueItemExprs sub_vies_;

  ObTempExpr *se_part_expr_;
  ObTempExpr *se_gen_col_expr_;
  ObTempExpr *se_subpart_expr_;
  ObTempExpr *se_sub_gen_col_expr_;

  common::ObFixedArray<common::ObObjectID, common::ObIAllocator> part_hint_ids_;
  ObQueryRange pre_query_range_; // query range for the table scan
  ObObjType part_col_type_;
  ObCollationType part_collation_type_;
  ObObjType subpart_col_type_;
  ObCollationType subpart_collation_type_;
  bool is_in_hit_;
  PartProjector part_projector_;

  bool is_valid_range_columns_part_range_;
  bool is_valid_range_columns_subpart_range_;
  bool has_dynamic_exec_param_;

  //mysql enable partition pruning by query range if part expr is temporal func like year(date)
  bool is_valid_temporal_part_range_;
  bool is_valid_temporal_subpart_range_;
  bool is_part_range_get_;
  bool is_subpart_range_get_;

  bool is_non_partition_optimized_;
  ObTabletID tablet_id_;
  ObObjectID object_id_;
  common::ObList<DASRelatedTabletMap::MapEntry, common::ObIAllocator> related_list_;
  bool check_no_partition_;
};

}
}
#endif
