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
#ifndef OCEANBASE_SQL_REWRITE_OB_QUERY_RANGE_DEFINE_H_
#define OCEANBASE_SQL_REWRITE_OB_QUERY_RANGE_DEFINE_H_

#include "lib/list/ob_obj_store.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "lib/geo/ob_geo_common.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/rewrite/ob_query_range.h"



namespace oceanbase
{
namespace sql
{
class ObExecContext;

static const int64_t OB_RANGE_MAX_VALUE = INT64_MAX;
static const int64_t OB_RANGE_MIN_VALUE = INT64_MAX - 1;
static const int64_t OB_RANGE_EMPTY_VALUE = INT64_MAX - 2;
static const int64_t OB_RANGE_NULL_VALUE = INT64_MAX - 3;
static const int64_t OB_RANGE_EXTEND_VALUE = INT64_MAX - 4;
#define PHYSICAL_ROWID_IDX UINT8_MAX

static const uint32_t OB_FINAL_EXPR_WITH_LOB_TRUNCATE = 1;

class ObPreRangeGraph;
typedef common::ObIArray<ObExprConstraint> ExprConstrantArray;
class ObRangeNode
{
  OB_UNIS_VERSION(1);
public:
  ObRangeNode(common::ObIAllocator &allocator)
      : allocator_(allocator),
        flags_(0),
        column_cnt_(0),
        start_keys_(nullptr),
        end_keys_(nullptr),
        min_offset_(-1),
        max_offset_(-1),
        in_param_count_(0),
        node_id_(-1),
        or_next_(nullptr),
        and_next_(nullptr) {}
  ~ObRangeNode() { reset(); }
  void reset();
  void set_always_true();
  void set_always_false();
  int deep_copy(const ObRangeNode &other);
  DECLARE_TO_STRING;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRangeNode);
public:
  common::ObIAllocator &allocator_;
  union {
    uint32_t flags_;
    struct {
      uint32_t always_true_:    1;
      uint32_t always_false_:   1;
      uint32_t include_start_:  1;
      uint32_t include_end_:    1;
      uint32_t contain_in_:     1;
      uint32_t is_phy_rowid_:   1;
      uint32_t is_domain_node_: 1; // FARM COMPAT WHITELIST
      uint32_t is_not_in_node_: 1;
      uint32_t reserved_:      24;
  };
  };
  int64_t column_cnt_;
  int64_t* start_keys_;
  int64_t* end_keys_;
  int64_t min_offset_;
  int64_t max_offset_;
  union {
    int64_t in_param_count_;
    struct {
      uint32_t srid_;
      int32_t  domain_releation_type_;
    } domain_extra_;
  };
  int64_t node_id_;
  //list member
  ObRangeNode *or_next_;
  ObRangeNode *and_next_;
};

typedef common::ObFixedArray<int64_t, common::ObIAllocator> InParam;

struct ObRangeMap
{
  OB_UNIS_VERSION(1);
public:
  struct ExprFinalInfo {
    ExprFinalInfo()
    : flags_(0),
      param_idx_(OB_INVALID_ID)
    {}
    union {
      uint32_t flags_;
      struct {
        uint32_t is_param_:       1;
        uint32_t is_const_:       1;
        uint32_t is_expr_:        1;
        uint32_t null_safe_:      1;
        /**
         * rowid_idx_ =
         *  0     : current final info is not rowid
         *  1-129 : current final info is logical rowid
         *  255   : current final info is physical rowid
        */
        uint32_t rowid_idx_:      8;
        uint32_t is_not_first_col_in_row_: 1;
        uint32_t reserved_:      19;
      };
    };
    union {
      int64_t param_idx_;
      common::ObObj* const_val_;
      ObTempExpr *temp_expr_;
    };
    TO_STRING_KV(K_(flags));
  };
public:
  ObRangeMap(common::ObIAllocator &alloc)
  : allocator_(alloc),
    expr_final_infos_(alloc),
    in_params_(alloc)
  {}
  TO_STRING_KV(K_(expr_final_infos));
  common::ObIAllocator &allocator_;
  common::ObFixedArray<ExprFinalInfo, common::ObIAllocator> expr_final_infos_;
  common::ObFixedArray<InParam*, common::ObIAllocator> in_params_;
};

class ObRangeColumnMeta
{
  OB_UNIS_VERSION(1);
public:
  ObRangeColumnMeta()
  : column_type_()
  {}

  ObRangeColumnMeta(ObExprResType type)
  : column_type_(type)
  {}

  TO_STRING_KV(N_COLUMN_TYPE, column_type_);

  ObExprResType column_type_;
};

struct ObQueryRangeCtx
{
  ObQueryRangeCtx(ObExecContext *exec_ctx)
    : column_cnt_(0),
      need_final_extract_(false),
      cur_is_precise_(false),
      phy_rowid_for_table_loc_(false),
      ignore_calc_failure_(false),
      refresh_max_offset_(false),
      exec_ctx_(exec_ctx),
      expr_constraints_(nullptr),
      params_(nullptr),
      expr_factory_(nullptr),
      session_info_(nullptr),
      geo_column_id_map_(nullptr),
      max_mem_size_(128*1024*1024),
      enable_not_in_range_(true),
      optimizer_features_enable_version_(0),
      index_prefix_(-1),
      is_geo_range_(false),
      can_range_get_(true),
      contail_geo_filters_(false),
      unique_index_column_num_(-1) {}
  ~ObQueryRangeCtx() {}
  int init(ObPreRangeGraph *pre_range_graph,
           const ObIArray<ColumnItem> &range_columns,
           ExprConstrantArray *expr_constraints,
           const ParamStore *params,
           ObRawExprFactory *expr_factory,
           const bool phy_rowid_for_table_loc,
           const bool ignore_calc_failure,
           const int64_t index_prefix,
           const ColumnIdInfoMap *geo_column_id_map,
           const ObTableSchema *index_schema);
  int64_t column_cnt_;
  // 131 is the next prime number larger than OB_MAX_ROWKEY_COLUMN_NUMBER
  common::hash::ObPlacementHashMap<int64_t, int64_t, 131> range_column_map_;
  bool need_final_extract_;
  // current expr can generate precise range
  bool cur_is_precise_;
  bool phy_rowid_for_table_loc_;
  bool ignore_calc_failure_;
  bool refresh_max_offset_;
  ObExecContext *exec_ctx_;
  ExprConstrantArray *expr_constraints_;
  const common::ParamStore *params_;
  ObRawExprFactory *expr_factory_;
  ObSQLSessionInfo *session_info_;
  common::ObSEArray<const ObRawExpr *, 16> final_exprs_;
  common::ObSEArray<uint32_t, 16> final_exprs_flag_;
  common::ObSEArray<InParam*, 4> in_params_;
  common::ObSEArray<int64_t, 16> null_safe_value_idxs_;
  common::ObSEArray<ObRangeColumnMeta*, 4> column_metas_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 16> rowid_idxs_;
  common::ObSEArray<int64_t, 4> column_flags_;
  common::ObSEArray<int64_t, 16> non_first_in_row_value_idxs_;
  const ColumnIdInfoMap *geo_column_id_map_;
  int64_t max_mem_size_;
  bool enable_not_in_range_;
  uint64_t optimizer_features_enable_version_;
  int64_t index_prefix_;
  bool is_geo_range_;
  bool can_range_get_;
  bool contail_geo_filters_;
  int64_t unique_index_column_num_;
};

class ObPreRangeGraph : public ObQueryRangeProvider
{
OB_UNIS_VERSION(1);
public:
  ObPreRangeGraph(ObIAllocator &alloc)
  : allocator_(alloc),
    table_id_(OB_INVALID_ID),
    column_count_(0),
    range_size_(0),
    node_count_(0),
    node_head_(nullptr),
    is_standard_range_(false),
    is_precise_get_(false),
    is_equal_range_(false),
    is_get_(false),
    contain_exec_param_(false),
    column_metas_(alloc),
    range_map_(alloc),
    skip_scan_offset_(-1),
    range_exprs_(alloc),
    ss_range_exprs_(alloc),
    unprecise_range_exprs_(alloc),
    total_range_sizes_(alloc),
    range_expr_max_offsets_(alloc),
    flags_(0) {}

  virtual ~ObPreRangeGraph() { reset(); }

  void reset();
  virtual inline bool is_new_query_range() const { return true; }
  virtual int deep_copy(const ObPreRangeGraph &other);

  int deep_copy_range_graph(ObRangeNode *src_node);
  int inner_deep_copy_range_graph(ObRangeNode *range_node,
                                  ObIArray<ObRangeNode*> &range_nodes,
                                  ObIArray<std::pair<int64_t, int64_t>> &ptr_pairs);
  int deep_copy_column_metas(const ObIArray<ObRangeColumnMeta*> &src_metas);
  int deep_copy_range_map(const ObRangeMap &src_range_map);

  virtual int preliminary_extract_query_range(const ObIArray<ColumnItem> &range_columns,
                                      const ObIArray<ObRawExpr*> &root_exprs,
                                      ObExecContext *exec_ctx,
                                      ExprConstrantArray *expr_constraints = NULL,
                                      const ParamStore *params = NULL,
                                      const bool phy_rowid_for_table_loc = false,
                                      const bool ignore_calc_failure = true,
                                      const int64_t index_prefix = -1,
                                      const ObTableSchema *index_schema = NULL,
                                      const ColumnIdInfoMap *geo_column_id_map = NULL);
  virtual int get_tablet_ranges(common::ObIAllocator &allocator,
                                ObExecContext &exec_ctx,
                                ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params) const;
  virtual int get_tablet_ranges(ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params);
  virtual int get_tablet_ranges(common::ObIAllocator &allocator,
                                ObExecContext &exec_ctx,
                                ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params,
                                ObIArray<common::ObSpatialMBR> &mbr_filters) const;
  virtual int get_fast_nlj_tablet_ranges(ObFastFinalNLJRangeCtx &fast_nlj_range_ctx,
                                         common::ObIAllocator &allocator,
                                         ObExecContext &exec_ctx,
                                         const ParamStore &param_store,
                                         void *range_buffer,
                                         ObQueryRangeArray &ranges,
                                         const common::ObDataTypeCastParams &dtc_params) const;
  int fill_column_metas(const ObIArray<ColumnItem> &range_columns);
  virtual int get_ss_tablet_ranges(common::ObIAllocator &allocator,
                                   ObExecContext &exec_ctx,
                                   ObQueryRangeArray &ss_ranges,
                                   const common::ObDataTypeCastParams &dtc_params) const;
  virtual bool is_precise_whole_range() const { return (nullptr == node_head_) || (node_head_->always_true_); }
  virtual int is_get(bool &is_get) const
  {
    is_get = is_get_;
    return common::OB_SUCCESS;
  }
  virtual bool is_precise_get() const { return is_precise_get_; }
  bool is_standard_range() const { return is_standard_range_; }
  bool is_equal_range() const { return is_equal_range_; }
  virtual int64_t get_column_count() const { return column_count_; }
  virtual bool has_exec_param() const { return contain_exec_param_; }
  virtual bool is_ss_range() const { return skip_scan_offset_ > -1; }
  virtual int64_t get_skip_scan_offset() const { return skip_scan_offset_; }
  virtual int reset_skip_scan_range()
  {
    skip_scan_offset_ = -1;
    return common::OB_SUCCESS;
  }
  virtual inline bool has_range() const { return column_count_ > 0; }
  virtual bool is_contain_geo_filters() const { return contain_geo_filters_; }
  inline void reset_range_exprs() { range_exprs_.reset(); }
  virtual const common::ObIArray<ObRawExpr*> &get_range_exprs() const { return range_exprs_; }
  virtual const common::ObIArray<ObRawExpr*> &get_ss_range_exprs() const { return ss_range_exprs_; }
  virtual const common::ObIArray<ObRawExpr*> &get_unprecise_range_exprs() const { return unprecise_range_exprs_; }
  virtual int get_prefix_info(int64_t &equal_prefix_count,
                              int64_t &range_prefix_count,
                              bool &contain_always_false) const;
  virtual int get_total_range_sizes(common::ObIArray<uint64_t> &total_range_sizes) const;

  const ObIArray<uint64_t>& get_range_sizes() const { return total_range_sizes_; }
  virtual bool is_fast_nlj_range() const { return fast_nlj_range_; }
  int get_prefix_info(const ObRangeNode *range_node,
                      bool* equals,
                      int64_t &equal_prefix_count) const;
  int get_new_equal_idx(const ObRangeNode *range_node,
                        bool* equals,
                        ObIArray<int64_t> &new_idx) const;

  ObRangeNode* get_range_head() { return node_head_; }
  const ObRangeNode* get_range_head() const { return node_head_; }
  uint64_t get_table_id() const { return table_id_; }
  uint64_t get_range_size() const { return range_size_; }
  uint64_t get_node_count() const { return node_count_; }
  int64_t get_column_cnt() const { return column_count_; }
  ObIArray<ObRangeColumnMeta*>& get_column_metas() { return column_metas_; }
  ObRangeColumnMeta* get_column_meta(int64_t idx) { return column_metas_.at(idx); }
  const ObRangeColumnMeta* get_column_meta(int64_t idx) const { return column_metas_.at(idx); }
  ObRangeMap& get_range_map() { return range_map_; }
  const ObRangeMap& get_range_map() const { return range_map_; }
  void set_table_id(const uint64_t v) { table_id_ = v; }
  void set_range_size(const uint64_t v) { range_size_ = v; }
  void set_node_count(const uint64_t v) { node_count_ = v; }
  void set_range_head(ObRangeNode* head) { node_head_ = head; }
  void set_is_standard_range(const bool v) { is_standard_range_ = v; }
  void set_is_precise_get(const bool v) { is_precise_get_ = v; }
  void set_is_equal_range(const bool v) { is_equal_range_ = v; }
  void set_is_get(const bool v) { is_get_ = v; }
  void set_contain_exec_param(const bool v) { contain_exec_param_ = v; }
  void set_skip_scan_offset(int64_t v) { skip_scan_offset_ = v; }
  int set_range_exprs(ObIArray<ObRawExpr*> &range_exprs) { return range_exprs_.assign(range_exprs); }
  int set_ss_range_exprs(ObIArray<ObRawExpr*> &range_exprs) { return ss_range_exprs_.assign(range_exprs); }
  int set_unprecise_range_exprs(ObIArray<ObRawExpr*> &range_exprs) { return unprecise_range_exprs_.assign(range_exprs); }
  void set_fast_nlj_range(bool v) { fast_nlj_range_ = v; }

  int serialize_range_graph(ObRangeNode *range_node, char *buf,
                            int64_t buf_len, int64_t &pos) const;
  int deserialize_range_graph(ObRangeNode *&range_node, const char *buf,
                              int64_t data_len, int64_t &pos);
  int64_t get_serialize_size_range_graph(const ObRangeNode *range_node) const;
  DECLARE_TO_STRING;
  int64_t range_graph_to_string(char *buf, const int64_t buf_len,
                                ObRangeNode *range_node) const;
  int64_t set_total_range_sizes(uint64_t* total_range_sizes, int64_t count);
  void set_contain_geo_filters(bool v) { contain_geo_filters_ = v; }
  int64_t set_range_expr_max_offsets(const ObIArray<int64_t> &max_offsets) { return range_expr_max_offsets_.assign(max_offsets); }
  const ObIArray<int64_t>& get_range_expr_max_offsets() const { return range_expr_max_offsets_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPreRangeGraph);
private:
  common::ObIAllocator &allocator_;
  uint64_t table_id_;
  int64_t column_count_;
  uint64_t range_size_;
  uint64_t node_count_;
  ObRangeNode *node_head_;
  // one range for all columns
  bool is_standard_range_;
  // one equal range for all columns. e.g. [1,2,3; 1,2,3]
  bool is_precise_get_;
  // one or more equal range for partial columns. e.g. (1,2,min; 1,2,max), (3,4,min; 3,4,max)
  bool is_equal_range_;
  // one or more equal range for all columns. e.g. [1,2,3; 1,2,3], [4,5,6; 4,5,6]
  bool is_get_;
  bool contain_exec_param_;
  common::ObFixedArray<ObRangeColumnMeta*, common::ObIAllocator> column_metas_;
  ObRangeMap range_map_;
  int64_t skip_scan_offset_;
  // only used by optimizer, don't need to serialize it
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> range_exprs_;
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> ss_range_exprs_;
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> unprecise_range_exprs_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> total_range_sizes_;
  common::ObFixedArray<int64_t, common::ObIAllocator> range_expr_max_offsets_;
  union {
    uint32_t flags_;
    struct {
      uint32_t contain_geo_filters_  :  1;
      uint32_t fast_nlj_range_       :  1;
      uint32_t reserved_             : 30;
    };
  };
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_OB_QUERY_RANGE_DEFINE_H_
