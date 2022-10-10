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

#ifndef OCEANBASE_SQL_REWRITE_QUERY_RANGE_
#define OCEANBASE_SQL_REWRITE_QUERY_RANGE_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_obj_store.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "lib/geo/ob_s2adapter.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "sql/rewrite/ob_key_part.h"
#include "sql/resolver/ob_schema_checker.h"
#include "objit/common/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace common
{
class ObDataTypeCastParams;
}
namespace sql
{
class ObRawExpr;
class ObConstRawExpr;
class ObOpRawExpr;
class ObColumnRefRawExpr;
struct ObGeoColumnInfo
{
  uint32_t srid_;
  uint64_t cellid_columnId_;
};

typedef common::ObDList<ObKeyPart> ObKeyPartList;
typedef common::ParamStore ParamsIArray;
typedef common::ObIArray<ObExprConstraint> ExprConstrantArray;
typedef common::ObIArray<ObRawExpr *> ExprIArray;
typedef common::ObIArray<ColumnItem> ColumnIArray;
typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint64_t, ObGeoColumnInfo>::AllocType,
                                  common::ObWrapperAllocator> ColumnIdInfoMapAllocer;
typedef hash::ObHashMap<uint64_t, ObGeoColumnInfo, common::hash::LatchReadWriteDefendMode,
  common::hash::hash_func<uint64_t>, common::hash::equal_to<uint64_t>, ColumnIdInfoMapAllocer,
  common::hash::NormalPointer, common::ObWrapperAllocator> ColumnIdInfoMap;
typedef common::ObList<common::ObSpatialMBR, common::ObIAllocator> MbrFilterArray;
static const uint32_t OB_DEFAULT_SRID_BUKER = 4;

class ObQueryRange : public ObQueryRangeProvider
{
  OB_UNIS_VERSION(4);
private:
  struct ObRangeExprItem
  {
    const ObRawExpr *cur_expr_;
    common::ObSEArray<int64_t, 16> cur_pos_;
    DECLARE_TO_STRING;
  };

  struct ArrayParamInfo
  {
    ArrayParamInfo()
      : param_index_(OB_INVALID_ID),
        array_param_(NULL)
    {
    }
    TO_STRING_KV(K_(param_index), KPC_(array_param));
    int64_t param_index_;
    ObSqlArrayObj *array_param_;
  };

  struct ObQueryRangeCtx
  {
    ObQueryRangeCtx(ObExecContext *exec_ctx,
                    ExprConstrantArray *expr_constraints,
                    const ParamsIArray *params)
      : need_final_extract_(false),
        max_valid_offset_(-1),
        cur_expr_is_precise_(false),
        phy_rowid_for_table_loc_(false),
        ignore_calc_failure_(false),
        range_optimizer_max_mem_size_(100*1024*1024),
        exec_ctx_(exec_ctx),
        expr_constraints_(expr_constraints),
        params_(params),
        use_in_optimization_(false),
        row_in_offsets_()
    {
    }
    ~ObQueryRangeCtx()
    {
    }
    //131的原因是最大的rowkey个数是128，距离128最近的素数是131
    common::hash::ObPlacementHashMap<ObKeyPartId, ObKeyPartPos*, 131> key_part_map_;
    bool need_final_extract_;
    int64_t max_valid_offset_;
    bool cur_expr_is_precise_; //当前正在被抽取的表达式是精确的范围，没有被放大
    bool phy_rowid_for_table_loc_;
    bool ignore_calc_failure_;
    int64_t range_optimizer_max_mem_size_;
    common::ObSEArray<ObRangeExprItem, 4, common::ModulePageAllocator, true> precise_range_exprs_;
    ObExecContext *exec_ctx_;
    ExprConstrantArray *expr_constraints_;
    const ParamsIArray *params_;
    common::ObSEArray<const ObRawExpr *, 16> final_exprs_;
    ObSEArray<ObKeyPartPos*, 8> key_part_pos_array_;
    bool use_in_optimization_;
    ObSEArray<int64_t, 4> row_in_offsets_;
  };
public:
  enum ObQueryRangeState
  {
    NEED_INIT = 0,
    NEED_TARGET_CND,
    NEED_PREPARE_PARAMS,
    CAN_READ,
  };

  enum ObRowBorderType
  {
    OB_FROM_NONE,
    OB_FROM_LEFT,
    OB_FROM_RIGHT,
  };

  enum ObRangeKeyType
  {
    T_GET,
    T_SCAN,
    T_FULL,
    T_EMPTY,
  };

  struct ObRangeKeyInfo
  {
    ObRangeKeyType key_type_;
    TO_STRING_KV(N_TYPE, static_cast<int32_t>(key_type_));
  };

  struct ObRangeWrapper
  {
    common::ObNewRange *range_;

    ObRangeWrapper()
      : range_(NULL)
    {
    }

    ObRangeWrapper(const ObRangeWrapper &other)
      : range_(other.range_)
    {
    }

    ~ObRangeWrapper()
    {
      range_ = NULL;
    }

    uint64_t hash() const
    {
      uint64_t uval = 0;
      if (NULL == range_) {
        SQL_REWRITE_LOG_RET(WARN, common::OB_NOT_INIT, "range_ is not inited.");
      } else {
        uval = range_->hash();
      }
      return uval;
    }

    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

    bool operator== (const ObRangeWrapper &other) const
    {
      bool is_equal = false;
      if (NULL != range_) {
        is_equal = range_->equal2(*other.range_);
      } else if (NULL == other.range_) {
        is_equal = true;
      } else {
        // do nothing
      }
      return is_equal;
    }
  };

  struct ObEqualOff
  {
    OB_UNIS_VERSION_V(1);
  public :
    ObEqualOff()
      : only_pos_(false), param_idx_(0), pos_off_(0), pos_type_(ObNullType)
    {
      pos_value_.reset();
    }
    virtual ~ObEqualOff() = default;
    TO_STRING_KV(K_(only_pos), K_(param_idx), K_(pos_off), K_(pos_type), K_(pos_value));
    bool only_pos_;
    int64_t param_idx_;
    int64_t pos_off_;
    ObObjType pos_type_;
    ObObj pos_value_;
  };

private:
  struct ObSearchState
  {
    ObSearchState(common::ObIAllocator &allocator)
      : start_(NULL),
        end_(NULL),
        include_start_(NULL),
        include_end_(NULL),
        table_id_(common::OB_INVALID_ID),
        depth_(0),
        max_exist_index_(0),
        last_include_start_(false),
        last_include_end_(false),
        produce_range_(false),
        is_equal_range_(false),
        is_empty_range_(false),
        valid_offsets_(),
        allocator_(allocator),
        range_set_(),
        is_phy_rowid_range_(false)
    {
    }

    int init_search_state(int64_t column_count, bool init_as_full_range, uint64_t table_id);
    bool has_intersect(const common::ObObj &start,
                       bool include_start,
                       const common::ObObj &end,
                       bool include_end) const
    {
      bool bret = true;
      if (NULL != start_ && NULL != end_ && include_start_ != NULL && include_end_ != NULL && depth_ >= 0) {
        common::ObObj &s1 = start_[depth_];
        common::ObObj &e1 = end_[depth_];
        bool include_s1 = include_start_[depth_];
        bool include_e1 = include_end_[depth_];
        int cmp_s2_e1 = 0;
        int cmp_e2_s1 = 0;
        if ((cmp_s2_e1 = start.compare(e1)) > 0
            || (cmp_e2_s1 = end.compare(s1)) < 0
            || (0 == cmp_s2_e1 && (!include_start || !include_e1))
            || (0 == cmp_e2_s1 && (!include_end || !include_s1))) {
          bret = false;
        }
      } else {
        bret = false;
      }
      return bret;
    }

    int intersect(const common::ObObj &start,
                  bool include_start,
                  const common::ObObj &end,
                  bool include_end);
    int tailor_final_range(int64_t column_count);
    common::ObObj *start_;
    common::ObObj *end_;
    //和start_数组对应，记录每个range的start边界
    bool *include_start_;
    //和end_数组对应，记录每个range的end边界
    bool *include_end_;
    uint64_t table_id_;
    int depth_;
    int64_t max_exist_index_;
    bool last_include_start_;
    bool last_include_end_;
    bool produce_range_;
    bool is_equal_range_;
    bool is_empty_range_;
    ObSqlBitSet<> valid_offsets_;
    common::ObIAllocator &allocator_;
    common::hash::ObHashSet<ObRangeWrapper, common::hash::NoPthreadDefendMode> range_set_;
    bool is_phy_rowid_range_;
  };

  struct ObRangeGraph
  {
    ObRangeGraph()
        : key_part_head_(NULL),
          is_equal_range_(false),
          is_standard_range_(true),
          is_precise_get_(false),
          skip_scan_offset_(-1)
    {
      //将is_standard_range_初始化为true的原因是我们认为当表达式条件为空的时候也是一个简单range
    }

    void reset()
    {
      key_part_head_ = NULL;
      is_equal_range_ = false;
      is_standard_range_ = true;
      is_precise_get_ = false;
      skip_scan_offset_ = -1;
    }

    int assign(const ObRangeGraph &other)
    {
      int ret = common::OB_SUCCESS;
      key_part_head_ = other.key_part_head_;
      is_equal_range_ = other.is_equal_range_;
      is_standard_range_ = other.is_standard_range_;
      is_precise_get_ = other.is_precise_get_;
      skip_scan_offset_ = other.skip_scan_offset_;
      return ret;
    }

    ObKeyPart *key_part_head_;
    bool is_equal_range_;
    bool is_standard_range_;
    bool is_precise_get_;
    int64_t skip_scan_offset_;
  };

  struct ExprFinalInfo {
    ExprFinalInfo() : param_idx_(OB_INVALID_ID), flags_(0) {}
    union {
      int64_t param_idx_;
      ObTempExpr *temp_expr_;
    };
    union {
      uint16_t flags_;
      struct {
        uint16_t is_param_:       1;
        uint16_t cnt_exec_param_: 1;
        uint16_t expr_exists_:    1;
        uint16_t reserved_:      13;
      };
    };
    TO_STRING_KV(K_(flags));
  };

public:
  ObQueryRange();
  explicit ObQueryRange(common::ObIAllocator &alloc);
  virtual ~ObQueryRange();
  ObQueryRange &operator=(const ObQueryRange &other);

  //  After reset(), the caller need to re-init the query range

  void reset();

  //  preliminary_extract_query_range will preliminary extract query range
  //  from query conditions, which is only occurred in generating the physical plan.
  //  During this stage, some consts are not really known, for example,
  //  prepared params, session variables, global variables, now(), current_timestamp(),
  //  utc_timestamp, etc..

  //  final extraction may be need in physical plan open.
  /**
   * @brief ATTENTION!!! Only used in unittest
   */
  int preliminary_extract_query_range(const ColumnIArray &range_columns,
                                      const ObRawExpr *expr_root,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      ObExecContext *exec_ctx,
                                      ExprConstrantArray *expr_constraints = NULL,
                                      const ParamsIArray *params = NULL,
                                      const bool use_in_optimization = false);
  /**
   * @brief
   * @param range_columns: columns used to extract range, index column or partition column
   * @param root_exprs: predicates used to extract range
   * @param dtc_params: data type cast params
   * @param exec_ctx: exec context, used to eval exprs and cg final expr
   * @param expr_constraints: used to add precise constraint and calc failure constraint
   * @param params: whether calc the expr with param at preliminary stage
   * @param phy_rowid_for_table_loc: whether use rowid when extract range for table location
   * @param ignore_calc_failure: whether ignore the calc failure
   *  table scan range: ignore calc failure and add constraint
   *  calc selectivity: ignore calc failure and not add constraint
   *  table location: not ignore calc failure
   */
  int preliminary_extract_query_range(const ColumnIArray &range_columns,
                                      const ExprIArray &root_exprs,
                                      const common::ObDataTypeCastParams &dtc_params,
                                      ObExecContext *exec_ctx,
                                      ExprConstrantArray *expr_constraints = NULL,
                                      const ParamsIArray *params = NULL,
                                      const bool phy_rowid_for_table_loc = false,
                                      const bool ignore_calc_failure = true,
                                      const bool use_in_optimization = false);

  //  final_extract_query_range extracts the final query range of its physical plan.
  //  It will get the real-time value of some const which are unknown during physical plan generating.
  //  Query range can not be used until this function is called.

  int final_extract_query_range(ObExecContext &exec_ctx,
                                const common::ObDataTypeCastParams &dtc_params);

  // get_tablet_ranges gets range of a index.
  // This function can not be used until physical plan is opened.

  virtual int get_tablet_ranges(ObQueryRangeArray &ranges,
                                bool &all_single_value_ranges,
                                const common::ObDataTypeCastParams &dtc_params);

  int direct_get_tablet_ranges(common::ObIAllocator &allocator,
                              ObExecContext &exec_ctx,
                              ObQueryRangeArray &ranges,
                              bool &all_single_value_ranges,
                              const common::ObDataTypeCastParams &dtc_params) const;
  int get_ss_tablet_ranges(common::ObIAllocator &allocator,
                           ObExecContext &exec_ctx,
                           ObQueryRangeArray &ss_ranges,
                           const ObDataTypeCastParams &dtc_params) const;
  int get_tablet_ranges(common::ObIAllocator &allocator,
                        ObExecContext &exec_ctx,
                        ObQueryRangeArray &ranges,
                        bool &all_single_value_ranges,
                        const common::ObDataTypeCastParams &dtc_params) const;
  // deep copy query range except the pointer of phy_plan_
  int deep_copy(const ObQueryRange &other, const bool copy_for_final = false);
  // necessary condition:
  //  true returned, all ranges are get-conditions, becareful, final range maybe (max, min);
  //  false returned, maybe all ranges are scan-conditions,
  //  maybe some get-condition(s) and some scan-condition(s)
  //  or maybe all ranges are get-conditions after final extraction.

  // USE only in test.
  bool is_precise_whole_range() const
  {
    bool bret = false;
    if (NULL == table_graph_.key_part_head_) {
      bret = true;
    } else if (table_graph_.key_part_head_->is_always_true()) {
      bret = true;
    } else if (table_graph_.key_part_head_->pos_.offset_ > 0) {
      bret = true;
    }
    return bret;
  }
  int is_at_most_one_row(bool &is_one_row) const;
  int is_get(bool &is_get) const;
  int is_get(int64_t column_count, bool &is_get) const;
  bool is_precise_get() const { return table_graph_.is_precise_get_; }
  common::ObGeoRelationType get_geo_relation(ObItemType type) const;
  const common::ObIArray<ObRawExpr*> &get_range_exprs() const { return range_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_ss_range_exprs() const { return ss_range_exprs_; }
  int check_graph_type(ObKeyPart &key_part_head);
  int check_skip_scan_range(ObKeyPart *key_part_head,
                            const bool is_standard_range,
                            const int64_t max_precise_pos,
                            ObKeyPart *&ss_head,
                            int64_t &skip_scan_offset,
                            int64_t &ss_max_precise_pos);
  int reset_skip_scan_range();
  bool is_precise_get(const ObKeyPart &key_part_head,
                      int64_t &max_precise_pos,
                      bool ignore_head = false);
  int fill_range_exprs(const int64_t max_precise_pos,
                       const int64_t ss_offset,
                       const int64_t ss_max_precise_pos);
  bool is_ss_range() const {  return table_graph_.skip_scan_offset_ > -1; }
  int64_t get_skip_scan_offset() const {  return table_graph_.skip_scan_offset_; }

  static bool can_be_extract_range(ObItemType cmp_type, const ObExprResType &col_type,
                            const ObExprCalcType &res_type, common::ObObjType data_type,
                            bool &always_true);

  // that mean the query range contain non-standard range graph,
  // need copy from ObTableScan operator to physical operator context to extract query range

  bool need_deep_copy() const { return !table_graph_.is_standard_range_; }
  inline bool has_range() const { return column_count_ > 0; }
  inline int64_t get_column_count() const { return column_count_; }
  const ObRangeGraph &get_table_grapth() const { return table_graph_; }
  int get_result_value(common::ObObj &val, ObExecContext &exec_ctx, ObIAllocator *allocator) const;
  int get_result_value_with_rowid(const ObKeyPart &key_part,
                                  ObObj &val,
                                  ObExecContext &exec_ctx,
                                  bool &is_inconsistent_rowid,
                                  ObIAllocator *allocator = NULL) const;
  bool inline has_exec_param() const { return has_exec_param_; }
  bool inline get_is_equal_and() const { return is_equal_and_; }
  void inline set_is_equal_and(int64_t is_equal_and) { is_equal_and_ = is_equal_and; }
  const common::ObIArray<ObEqualOff> &get_raw_equal_offs() const { return equal_offs_; }
  common::ObIArray<ObEqualOff> &get_equal_offs() { return equal_offs_; }
  DECLARE_TO_STRING;
  // check a pattern str is precise range or imprecise range
  static int is_precise_like_range(const ObObjParam &pattern, char escape, bool &is_precise);
  const MbrFilterArray &get_mbr_filter() const { return mbr_filters_; }
  const ColumnIdInfoMap &get_columnId_map() const {return columnId_map_; }
  int init_columnId_map();
  int set_columnId_map(uint64_t columnId, const ObGeoColumnInfo &column_info);
  MbrFilterArray &ut_get_mbr_filter() { return mbr_filters_; }
  ColumnIdInfoMap &ut_get_columnId_map() { return columnId_map_; }
  bool is_contain_geo_filters() const { return contain_geo_filters_; }
private:

  int init_query_range_ctx(common::ObIAllocator &allocator,
                           const ColumnIArray &range_columns,
                           ObExecContext *exec_ctx,
                           ExprConstrantArray *expr_constraints,
                           const ParamsIArray *params,
                           const bool phy_rowid_for_table_loc,
                           const bool ignore_calc_failure,
                           const bool use_in_optimization);
  void destroy_query_range_ctx(common::ObIAllocator &allocator);
  int add_expr_offsets(ObIArray<int64_t> &cur_pos, const ObKeyPart *cur_key);
  int extract_valid_exprs(const ExprIArray &root_exprs,
                          ObIArray<ObRawExpr *> &candi_exprs);
  int check_cur_expr(const ObRawExpr *cur_expr,
                     ObIArray<int64_t> &offsets,
                     bool &need_extract_const,
                     bool &is_valid_expr);
  int extract_row_info(const ObRawExpr *l_expr,
                      const ObRawExpr *r_expr,
                      const ObItemType &cmp_type,
                      ObIArray<int64_t> &offsets,
                      bool &need_extract_const,
                      bool &is_valid_expr);
  int extract_basic_info(const ObRawExpr *l_expr,
                      const ObRawExpr *r_expr,
                      const ObItemType &cmp_type,
                      ObIArray<int64_t> &offsets,
                      bool &need_extract_const,
                      bool &is_valid_expr);
  int check_can_extract_rowid(const ObIArray<const ObColumnRefRawExpr *> &pk_column_items,
                              const bool is_physical_rowid,
                              const uint64_t table_id,
                              const uint64_t part_column_id,
                              ObIArray<int64_t> &offsets,
                              bool &is_valid_expr);

  // @brief escape_expr only be used when cmp_type == T_OP_LIKE

  int get_basic_query_range(const ObRawExpr *l_expr,
                            const ObRawExpr *r_expr,
                            const ObRawExpr *escape_expr,
                            ObItemType cmp_type,
                            const ObExprResType &result_type,
                            ObKeyPart *&out_key_part,
                            const common::ObDataTypeCastParams &dtc_params,
                            bool &is_bound_modified);
  int get_const_key_part(const ObRawExpr *l_expr,
                         const ObRawExpr *r_expr,
                         const ObRawExpr *escape_expr,
                         ObItemType cmp_type,
                         const ObExprResType &result_type,
                         ObKeyPart *&out_key_part,
                         const common::ObDataTypeCastParams &dtc_params);
  int get_column_key_part(const ObRawExpr *l_expr,
                          const ObRawExpr *r_expr,
                          const ObRawExpr *escape_expr,
                          ObItemType cmp_type,
                          const ObExprResType &result_type,
                          ObKeyPart *&out_key_part,
                          const common::ObDataTypeCastParams &dtc_params,
                          bool &is_bound_modified);
  int get_rowid_key_part(const ObRawExpr *l_expr,
                         const ObRawExpr *r_expr,
                         const ObRawExpr *escape_expr,
                         ObItemType cmp_type,
                         ObKeyPart *&out_key_part,
                         const ObDataTypeCastParams &dtc_params);
  int get_normal_cmp_keypart(ObItemType cmp_type,
                             const common::ObObj &val,
                             ObKeyPart &out_keypart) const;
  int get_geo_single_keypart(const ObObj &val_start, const ObObj &val_end, ObKeyPart &out_keypart) const;
  int get_geo_intersects_keypart(uint32_t input_srid,
                                 const common::ObString &wkb,
                                 const common::ObGeoRelationType op_type,
                                 ObKeyPart *out_key_part);
  int get_geo_coveredby_keypart(uint32_t input_srid,
                                const common::ObString &wkb,
                                const common::ObGeoRelationType op_type,
                                ObKeyPart *out_key_part);
  int set_geo_keypart_whole_range(ObKeyPart &out_key_part);
  int get_row_key_part(const ObRawExpr *l_expr,
                       const ObRawExpr *r_expr,
                       ObItemType cmp_type,
                       const ObExprResType &result_type,
                       ObKeyPart *&out_key_part,
                       const common::ObDataTypeCastParams &dtc_params);
  int check_row_bound(ObKeyPart *key_part,
                  const ObDataTypeCastParams &dtc_params,
                  const ObRawExpr *const_expr,
                  bool &is_bound_modified);
  int add_row_item(ObKeyPart *&row_tail, ObKeyPart *key_part);
  int add_and_item(ObKeyPartList &and_storage, ObKeyPart *key_part);
  int add_or_item(ObKeyPartList &or_storage, ObKeyPart *key_part);
  int preliminary_extract(const ObRawExpr *node,
                          ObKeyPart *&out_key_part,
                          const common::ObDataTypeCastParams &dtc_params,
                          const bool is_single_in = false);
  int pre_extract_basic_cmp(const ObRawExpr *node,
                            ObKeyPart *&out_key_part,
                            const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_ne_op(const ObOpRawExpr *t_expr,
                        ObKeyPart *&out_key_part,
                        const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_is_op(const ObOpRawExpr *t_expr,
                        ObKeyPart *&out_key_part,
                        const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_btw_op(const ObOpRawExpr *t_expr,
                         ObKeyPart *&out_key_part,
                         const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_not_btw_op(const ObOpRawExpr *t_expr,
                             ObKeyPart *&out_key_part,
                             const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_single_in_op(const ObOpRawExpr *b_expr,
                               ObKeyPart *&out_key_part,
                               const ObDataTypeCastParams &dtc_params);
  int pre_extract_complex_in_op(const ObOpRawExpr *b_expr,
                                ObKeyPart *&out_key_part,
                                const ObDataTypeCastParams &dtc_params);
  int pre_extract_in_op(const ObOpRawExpr *b_expr,
                        ObKeyPart *&out_key_part,
                        const ObDataTypeCastParams &dtc_params,
                        const bool is_single_in);
  int pre_extract_in_op_with_opt(const ObOpRawExpr *b_expr,
                        ObKeyPart *&out_key_part,
                        const common::ObDataTypeCastParams &dtc_params);
  int check_row_in_need_in_optimization(const ObOpRawExpr *b_expr,
                                        const bool is_single_in,
                                        bool &use_in_optimization);
  int pre_extract_not_in_op(const ObOpRawExpr *b_expr,
                            ObKeyPart *&out_key_part,
                            const ObDataTypeCastParams &dtc_params);
  int pre_extract_and_or_op(const ObOpRawExpr *m_expr,
                            ObKeyPart *&out_key_part,
                            const common::ObDataTypeCastParams &dtc_params);
  int pre_extract_const_op(const ObRawExpr *node,
                           ObKeyPart *&out_key_part);
  int pre_extract_geo_op(const ObOpRawExpr *geo_expr,
                         ObKeyPart *&out_key_part,
                         const ObDataTypeCastParams &dtc_params);
  int prepare_multi_in_info(const ObOpRawExpr *l_expr,
                            const ObOpRawExpr *r_expr,
                            ObKeyPart *&tmp_key_part,
                            bool &has_rowid,
                            common::hash::ObHashMap<int64_t, ObKeyPartPos*> &idx_pos_map,
                            common::hash::ObHashMap<int64_t, InParamMeta *> &idx_param_map,
                            common::hash::ObHashMap<int64_t, int64_t> &expr_idx_param_idx_map,
                            const ObDataTypeCastParams &dtc_params);
  int get_multi_in_key_part(const ObOpRawExpr *l_expr,
                            const ObOpRawExpr *r_expr,
                            const ObExprResType &res_type,
                            ObKeyPart *&out_key_part,
                            const ObDataTypeCastParams &dtc_params);
  int check_const_val_valid(const ObRawExpr *l_expr,
                            const ObRawExpr *r_expr,
                            const ObExprResType &res_type,
                            const ObDataTypeCastParams &dtc_params,
                            bool &is_valid);

  int get_single_in_key_part(const ObColumnRefRawExpr *col_expr,
                             const ObOpRawExpr *r_expr,
                             const ObExprResType &res_type,
                             ObKeyPart *&out_key_part,
                             const ObDataTypeCastParams &dtc_params);
  int get_rowid_in_key_part(const ObRawExpr *l_expr,
                            const ObOpRawExpr *r_expr,
                            const int64_t idx,
                            ObKeyPart *&out_key_part,
                            const ObDataTypeCastParams &dtc_params);
  int check_rowid_val(const ObIArray<const ObColumnRefRawExpr *> &pk_column_items,
                      const ObObj &val, const bool is_physical_rowid);
  int get_param_value(ObInKeyPart *in_key,
                      InParamMeta *param_meta,
                      const ObKeyPartPos &key_pos,
                      const ObRawExpr *const_expr,
                      const ObDataTypeCastParams &dtc_params,
                      bool &is_val_valid);
  int check_expr_precise(ObKeyPart *key_part,
                         const ObRawExpr *const_expr,
                         const ObExprCalcType &calc_type,
                         const ObKeyPartPos &key_pos);
  int is_key_part(const ObKeyPartId &id, ObKeyPartPos *&pos, bool &is_key_part);
  int split_general_or(ObKeyPart *graph, ObKeyPartList &or_storage);
  int split_or(ObKeyPart *graph, ObKeyPartList &or_list);
  int deal_not_align_keypart(ObKeyPart *l_key_part,
                             ObKeyPart *r_key_part,
                             ObKeyPart *&rest);
  int intersect_border_from(const ObKeyPart *l_key_part,
                            const ObKeyPart *r_key_part,
                            ObRowBorderType &start_border_type,
                            ObRowBorderType &end_border_type,
                            bool &is_always_false,
                            bool &has_special_key);
  int set_partial_row_border(ObKeyPart *l_gt,
                             ObKeyPart *r_gt,
                             ObRowBorderType start_border_type,
                             ObRowBorderType end_border_type,
                             ObKeyPart *&result);
//  int link_item(ObKeyPart *l_gt, ObKeyPart *r_gt);
  int do_key_part_node_and(ObKeyPart *l_key_part, ObKeyPart *r_key_part, ObKeyPart *&res_key_part);
  int deep_copy_key_part_and_items(const ObKeyPart *src_key_part, ObKeyPart *&dest_key_part);
  int deep_copy_expr_final_info(const ObIArray<ExprFinalInfo> &final_info);
  int shallow_copy_expr_final_info(const ObIArray<ExprFinalInfo> &final_info);
  int and_single_gt_head_graphs(ObKeyPartList &l_array,
                                ObKeyPartList &r_array,
                                ObKeyPartList &res_array);
  int and_range_graph(ObKeyPartList &ranges, ObKeyPart *&out_key_part);

  int do_row_gt_and(ObKeyPart *l_gt, ObKeyPart *r_gt, ObKeyPart *&res_gt);
  int do_gt_and(ObKeyPart *l_gt, ObKeyPart *r_gt, ObKeyPart *&res_gt);
  int link_or_graphs(ObKeyPartList &storage, ObKeyPart *&out_key_part);
  int definite_key_part(ObKeyPart *key_part, ObExecContext &exec_ctx,
                        const common::ObDataTypeCastParams &dtc_params,
                        bool &is_bound_modified);
  int replace_unknown_value(ObKeyPart *root, ObExecContext &exec_ctx,
                            const common::ObDataTypeCastParams &dtc_params,
                            bool &is_bound_modified);
  int or_single_head_graphs(ObKeyPartList &or_list, ObExecContext *exec_ctx,
                            const common::ObDataTypeCastParams &dtc_params);
  int union_in_with_in(ObKeyPartList &or_list,
                       ObKeyPart *cur1,
                       ObKeyPart *cur2,
                       ObExecContext *exec_ctx,
                       const ObDataTypeCastParams &dtc_params,
                       bool &has_union);
  int union_in_with_normal(ObKeyPart *cur1,
                            ObKeyPart *cur2,
                            ObExecContext *exec_ctx,
                            const ObDataTypeCastParams &dtc_params,
                            bool &is_unioned,
                            bool &need_remove_normal);
  int union_single_equal_cond(ObKeyPart *cur1,
                              ObKeyPart *cur2,
                              const ObObj &val,
                              ObExecContext *exec_ctx,
                              const ObDataTypeCastParams &dtc_params,
                              bool &need_remove_val,
                              bool &need_remove_normal);
  int union_single_equal_cond(ObExecContext *exec_ctx,
                              const common::ObDataTypeCastParams &dtc_params,
                              ObKeyPart *cur1,
                              ObKeyPart *cur2);
  int or_range_graph(ObKeyPartList &ranges, ObExecContext *exec_ctx, ObKeyPart *&out_key_part,
                     const common::ObDataTypeCastParams &dtc_params);
  int definite_in_range_graph(ObExecContext &exec_ctx, ObKeyPart *&root, bool &has_scan_key,
                              const common::ObDataTypeCastParams &dtc_params);

  int set_valid_offsets(const ObKeyPart *cur, ObSqlBitSet<> &offsets) const;
  int remove_cur_offset(const ObKeyPart *cur, ObSqlBitSet<> &offsets) const;
  int remove_and_next_offset(ObKeyPart *cur, ObSqlBitSet<> &offsets) const;
  int64_t get_max_valid_offset(const ObSqlBitSet<> &offsets) const;
  // find all single range
  int and_first_search(ObSearchState &search_state,
                       ObKeyPart *cur,
                       ObQueryRangeArray &ranges,
                       bool &all_single_value_ranges,
                       const common::ObDataTypeCastParams &dtc_params);
  int and_first_in_key(ObSearchState &search_state,
                       ObKeyPart *cur,
                       ObQueryRangeArray &ranges,
                       bool &all_single_value_ranges,
                       const ObDataTypeCastParams &dtc_params);
  int generate_cur_range(ObSearchState &search_state,
                         const int64_t copy_depth,
                         const bool copy_produce_range,
                         ObQueryRangeArray &ranges,
                         bool &all_single_value_ranges,
                         const bool is_phy_rowid_range);
  inline int generate_single_range(ObSearchState &search_state,
                                   int64_t column_num,
                                   common::ObNewRange *&range,
                                   bool &is_get_range) const;
  inline int generate_true_or_false_range(const ObKeyPart *cur,
                                          common::ObIAllocator &allocator,
                                          common::ObNewRange *&range) const;
  int store_range(common::ObNewRange *range,
                  bool is_get_range,
                  ObSearchState &search_state,
                  ObQueryRangeArray &ranges,
                  bool &all_single_value_ranges);
  int alloc_empty_key_part(ObKeyPart *&out_key_part);
  int alloc_full_key_part(ObKeyPart *&out_key_part);
  int deep_copy_range_graph(ObKeyPart *src, ObKeyPart *&dest);
  int serialize_range_graph(const ObKeyPart *cur,
                            char *buf,
                            int64_t buf_len,
                            int64_t &pos) const;
  int serialize_cur_keypart(const ObKeyPart &cur, char *buf, int64_t buf_len, int64_t &pos) const;
  int serialize_expr_final_info(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize_range_graph(ObKeyPart *&cur, const char *buf, int64_t data_len, int64_t &pos);
  int deserialize_cur_keypart(ObKeyPart *&cur, const char *buf, int64_t data_len, int64_t &pos);
  int deserialize_expr_final_info(const char *buf, int64_t data_len, int64_t &pos);
  int get_range_graph_serialize_size(const ObKeyPart *cur, int64_t &all_size) const;
  int get_cur_keypart_serialize_size(const ObKeyPart &cur, int64_t &all_size) const;
  int64_t get_expr_final_info_serialize_size() const;
  int serialize_srid_map(char *buf, int64_t buf_len, int64_t &pos) const;
  int deserialize_srid_map(int64_t count, const char *buf, int64_t data_len, int64_t &pos);
  int64_t get_columnId_map_size() const;
  ObKeyPart *create_new_key_part();
  ObKeyPart *deep_copy_key_part(ObKeyPart *key_part);
  void print_keypart(const ObKeyPart *keypart, const ObString &prefix) const;
  int64_t range_graph_to_string(char *buf, const int64_t buf_len, ObKeyPart *key_part) const;
  bool is_get_graph(int deepth, ObKeyPart *key_part);
  int get_like_range(const common::ObObj &pattern, const common::ObObj &escape,
                     ObKeyPart &out_key_part, const ObDataTypeCastParams &dtc_params);
  int get_geo_range(const common::ObObj &wkb, const common::ObGeoRelationType op_type, ObKeyPart *out_key_part);
  int get_dwithin_item(const ObRawExpr *expr, const ObConstRawExpr *&extra_item);
  int get_like_const_range(const ObRawExpr *text,
                           const ObRawExpr *pattern,
                           const ObRawExpr *escape,
                           common::ObCollationType cmp_cs_type,
                           ObKeyPart *&out_key_part,
                           const common::ObDataTypeCastParams &dtc_params);
  int get_in_expr_res_type(const ObRawExpr *in_expr, int64_t val_idx, ObExprResType &res_type) const;
  inline bool is_standard_graph(const ObKeyPart *root) const;
  bool is_strict_in_graph(const ObKeyPart *root, const int64_t start_pos = 0) const;
  int is_strict_equal_graph(const ObKeyPart *root, const int64_t cur_pos, int64_t &max_pos, bool &is_strict_equal) const;
  inline int get_single_key_value(const ObKeyPart *key,
                                  ObExecContext &exec_ctx,
                                  ObSearchState &search_state,
                                  const common::ObDataTypeCastParams &dtc_params,
                                  int64_t skip_offset = 0) const;
  int gen_simple_get_range(const ObKeyPart &root,
                           common::ObIAllocator &allocator,
                           ObExecContext &exec_ctx,
                           ObQueryRangeArray &ranges,
                           bool &all_single_value_ranges,
                           const common::ObDataTypeCastParams &dtc_params) const;
  int gen_simple_scan_range(common::ObIAllocator &allocator,
                            ObExecContext &exec_ctx,
                            ObQueryRangeArray &ranges,
                            bool &all_single_value_ranges,
                            const common::ObDataTypeCastParams &dtc_params) const;

  const ObKeyPart* get_ss_key_part_head() const;

  int gen_skip_scan_range(ObIAllocator &allocator,
                          ObExecContext &exec_ctx,
                          const ObDataTypeCastParams &dtc_params,
                          const ObKeyPart *ss_root,
                          int64_t post_column_count,
                          ObQueryRangeArray &ss_ranges) const;

  int cold_cast_cur_node(const ObKeyPart *cur,
                         common::ObIAllocator &allocator,
                         const common::ObDataTypeCastParams &dtc_params,
                         common::ObObj &cur_val,
                         bool &always_false) const;
  int remove_precise_range_expr(int64_t offset);
  bool is_general_graph(const ObKeyPart &keypart) const;
  bool has_scan_key(const ObKeyPart &keypart) const;
  bool is_min_range_value(const common::ObObj &obj) const;
  bool is_max_range_value(const common::ObObj &obj) const;
  int check_is_get(ObKeyPart &key_part,
                   const int64_t column_count,
                   bool &bret,
                   ObSqlBitSet<> &valid_offsets) const;
  static bool check_like_range_precise(const ObString &pattern_str,
                                       const char *min_str_buf,
                                       const size_t min_str_len,
                                       const char escape);
  int cast_like_obj_if_needed(const ObObj &string_obj,
                              ObObj &buf_obj,
                              const ObObj *&obj_ptr,
                              ObKeyPart &out_key_part,
                              const ObDataTypeCastParams &dtc_params);
  int refine_large_range_graph(ObKeyPart *&key_part, bool use_in_optimization = false);
  int compute_range_size(const ObIArray<ObKeyPart*> &key_parts,
                         const ObIArray<uint64_t> &or_count,
                         ObIArray<ObKeyPart*> &next_key_parts,
                         ObIArray<uint64_t> &next_or_count,
                         uint64_t &range_size);
  int remove_useless_range_graph(ObKeyPart *key_part, ObSqlBitSet<> &valid_offsets);
  bool is_and_next_useless(ObKeyPart *cur_key, ObKeyPart *and_next, const int64_t max_valid_offset);
  int get_extract_rowid_range_infos(const ObRawExpr *calc_urowid_expr,
                                    ObIArray<const ObColumnRefRawExpr*> &pk_columns,
                                    bool &is_physical_rowid,
                                    uint64_t &table_id,
                                    uint64_t &part_column_id);
  int get_calculable_expr_val(const ObRawExpr *expr, ObObj &val, bool &is_valid, const bool ignore_error = true);
  int add_precise_constraint(const ObRawExpr *expr, bool is_precise);
  int add_prefix_pattern_constraint(const ObRawExpr *expr);
  int get_final_expr_val(const ObRawExpr *expr, ObObj &val);
  int generate_expr_final_info();
  int check_null_param_compare_in_row(const ObRawExpr *l_expr,
                                      const ObRawExpr *r_expr,
                                      ObKeyPart *&out_key_part);
private:
  static const int64_t RANGE_BUCKET_SIZE = 1000;
  static const int64_t MAX_RANGE_SIZE_OLD = 10000;
  static const int64_t MAX_RANGE_SIZE_NEW = 100000;
  static const int64_t MAX_NOT_IN_SIZE = 10; //do not extract range for not in row over this size
  typedef common::ObObjStore<ObKeyPart*, common::ObIAllocator&> KeyPartStore;
private:
  ObRangeGraph table_graph_;
  ObQueryRangeState state_;
  int64_t range_size_;
  int64_t column_count_;
  bool contain_row_;
  bool contain_in_;
  ColumnIdInfoMap columnId_map_;
  bool contain_geo_filters_;
  //not need serialize
  common::ObArenaAllocator inner_allocator_;
  common::ObIAllocator &allocator_;
  common::ObWrapperAllocator bucket_allocator_wrapper_;
  ColumnIdInfoMapAllocer map_alloc_;
  ObQueryRangeCtx *query_range_ctx_;
  KeyPartStore key_part_store_;
  //this flag used by optimizer, so don't need to serialize it
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> range_exprs_;
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> ss_range_exprs_;
  MbrFilterArray mbr_filters_;
  bool has_exec_param_;
  bool is_equal_and_;
  common::ObFixedArray<ObEqualOff, common::ObIAllocator> equal_offs_;
  common::ObFixedArray<ExprFinalInfo, common::ObIAllocator> expr_final_infos_;
  // NOTE: following two members are not allowed to be serialize or deep copy
  int64_t mem_used_;
  bool is_reach_mem_limit_;
  friend class ObKeyPart;
};
} // namespace sql-
} //namespace oceanbase
#endif // OCEANBASE_SQL_REWRITE_QUERY_RANGE_
