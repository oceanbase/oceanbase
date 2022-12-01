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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_
#include "lib/container/ob_array.h"
#include "common/object/ob_object.h"
#include "share/ob_simple_batch.h"
#include "share/ob_rpc_struct.h"
#include "sql/rewrite/ob_query_range_provider.h"
#include "sql/optimizer/ob_opt_default_stat.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "share/stat/ob_opt_ds_stat.h"

namespace oceanbase
{
namespace sql
{
struct OrderItem;
struct ObExprSelPair;
struct JoinFilterInfo;
class OptTableMetas;
class OptSelectivityCtx;

enum RowCountEstMethod
{
  INVALID_METHOD = 0,
  DEFAULT_STAT,
  BASIC_STAT,    //use min/max/ndv to estimate row count
  STORAGE_STAT, //use storage layer to estimate row count
  DYNAMIC_SAMPLING_STAT //use dynamic sampling to estimate row count
};

// all the table meta info need to compute cost
struct ObTableMetaInfo
{
  ObTableMetaInfo(uint64_t ref_table_id)
    : ref_table_id_(ref_table_id),
      schema_version_(share::OB_INVALID_SCHEMA_VERSION),
      part_count_(0),
      micro_block_size_(0),
      table_column_count_(0),
      table_rowkey_count_(0),
      table_row_count_(0),
      part_size_(0),
      average_row_size_(0),
      row_count_(0),
      has_opt_stat_(false),
      micro_block_count_(-1),
      table_type_(share::schema::MAX_TABLE_TYPE)
  { }
  virtual ~ObTableMetaInfo()
  { }

  void assign(const ObTableMetaInfo &table_meta_info);
  double get_micro_block_numbers() const;
  TO_STRING_KV(K_(ref_table_id), K_(part_count), K_(micro_block_size),
               K_(part_size), K_(average_row_size), K_(table_column_count),
               K_(table_rowkey_count), K_(table_row_count), K_(row_count),
               K_(micro_block_count), K_(table_type));

  /// the following fields come from schema info
  uint64_t ref_table_id_; //ref table id
  int64_t schema_version_; // schema version
  int64_t part_count_;  //partition count
  int64_t micro_block_size_;  //main table micro block size
  int64_t table_column_count_; // table column count
  int64_t table_rowkey_count_; // table rowkey count, used in index_back cost calc.index_back时候会从索引表获取主键

  /// the following fields come from access path estimation
  int64_t table_row_count_; // table row count in stat.
  double part_size_;  //main table best partition data size
  double average_row_size_; //main table best partition average row size

  double row_count_;  // row count after filters, estimated by stat manager
  bool has_opt_stat_;
  int64_t micro_block_count_;  // main table micro block count
  share::schema::ObTableType table_type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableMetaInfo);
};

// all the index meta info need to compute cost
struct ObIndexMetaInfo
{
  ObIndexMetaInfo(uint64_t ref_table_id, uint64_t index_id)
    : ref_table_id_(ref_table_id),
      index_id_(index_id),
      index_micro_block_size_(0),
      index_part_count_(1),
      index_part_size_(0),
      index_column_count_(0),
      is_index_back_(false),
      is_unique_index_(false),
      is_global_index_(false),
      is_geo_index_(false),
      index_micro_block_count_(-1)
  { }
  virtual ~ObIndexMetaInfo()
  { }
  void assign(const ObIndexMetaInfo &index_meta_info);
  double get_micro_block_numbers() const;
  TO_STRING_KV(K_(ref_table_id), K_(index_id), K_(index_micro_block_size),
               K_(index_part_count), K_(index_part_size),
               K_(index_column_count), K_(is_index_back),
               K_(is_unique_index), K_(index_micro_block_count));
  uint64_t ref_table_id_; // ref table id
  uint64_t index_id_; // index id
  int64_t index_micro_block_size_; //index micro block size, same as main table when path is primary
  uint64_t index_part_count_;
  double index_part_size_; //index table partition(0) data size, same as main table when path is primary
  int64_t index_column_count_; //index column count
  bool is_index_back_; // is index back
  bool is_unique_index_; // is unique index
  bool is_global_index_; // whether is global index
  bool is_geo_index_; // whether is spatial index
  int64_t index_micro_block_count_;  // micro block count from table static info
private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexMetaInfo);
};

struct ObBasicCostInfo
{
  ObBasicCostInfo() : rows_(0), cost_(0), width_(0), exchange_allocated_(false) {}
  ObBasicCostInfo(double rows, double cost, double width, bool exchange_allocated = false)
  : rows_(rows), cost_(cost), width_(width), exchange_allocated_(exchange_allocated)
  {}
  TO_STRING_KV(K_(rows), K_(cost), K_(width), K_(exchange_allocated));
  double rows_;
  double cost_;
  double width_;
  bool exchange_allocated_;
};

struct ObTwoNodeCostInfo
{
  ObTwoNodeCostInfo(double left_rows, double left_width,
                    double right_rows, double right_width,
                    OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
  : left_rows_(left_rows),
    left_width_(left_width),
    right_rows_(right_rows),
    right_width_(right_width),
    table_metas_(table_metas),
    sel_ctx_(sel_ctx)
  { }
  const double left_rows_;
  const double left_width_;
  const double right_rows_;
  const double right_width_;
  OptTableMetas *table_metas_;
  OptSelectivityCtx *sel_ctx_;
};

/*
 * store all the info needed to cost table scan
 */
struct ObCostTableScanInfo
{
  ObCostTableScanInfo(uint64_t table_id, uint64_t ref_table_id, uint64_t index_id)
   : table_id_(table_id),
     ref_table_id_(ref_table_id),
     index_id_(index_id),
     table_meta_info_(NULL),
     index_meta_info_(ref_table_id, index_id),
     is_virtual_table_(is_virtual_table(ref_table_id)),
     is_unique_(false),
     is_inner_path_(false),
     can_use_batch_nlj_(false),
     ranges_(),
     ss_ranges_(),
     range_columns_(),
     prefix_filters_(),
     pushdown_prefix_filters_(),
     ss_postfix_range_filters_(),
     postfix_filters_(),
     table_filters_(),
     table_metas_(NULL),
     sel_ctx_(NULL),
     row_est_method_(RowCountEstMethod::INVALID_METHOD),
     prefix_filter_sel_(1.0),
     pushdown_prefix_filter_sel_(1.0),
     postfix_filter_sel_(1.0),
     table_filter_sel_(1.0),
     join_filter_sel_(1.0),
     ss_prefix_ndv_(1.0),
     ss_postfix_range_filters_sel_(1.0),
     batch_type_(common::ObSimpleBatch::ObBatchType::T_NONE)
  { }
  virtual ~ObCostTableScanInfo()
  { }

  int assign(const ObCostTableScanInfo &other_est_cost_info);

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(index_id),
               K_(table_meta_info), K_(index_meta_info),
               K_(access_column_items),
               K_(is_virtual_table), K_(is_unique),
               K_(is_inner_path), K_(can_use_batch_nlj),
               K_(prefix_filter_sel), K_(pushdown_prefix_filter_sel),
               K_(postfix_filter_sel), K_(table_filter_sel),
               K_(ss_prefix_ndv), K_(ss_postfix_range_filters_sel));
  // the following information need to be set before estimating cost
  uint64_t table_id_; // table id
  uint64_t ref_table_id_; // ref table id
  uint64_t index_id_;  // index_id
  ObTableMetaInfo *table_meta_info_; // table related meta info
  ObIndexMetaInfo index_meta_info_; // index related meta info
  bool is_virtual_table_; // is virtual table
  bool is_unique_;  // whether query range is unique
  bool is_inner_path_;
  bool can_use_batch_nlj_;
  ObRangesArray ranges_;  // all the ranges
  ObRangesArray ss_ranges_;  // skip scan ranges
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> range_columns_; // all the range columns
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> access_column_items_; // all the access columns
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> index_access_column_items_; // all the access columns

  //这几个filter的分类参考OptimizerUtil::classify_filters()
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> prefix_filters_; // filters match index prefix
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> pushdown_prefix_filters_; // filters match index prefix along pushed down filter
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> ss_postfix_range_filters_;  // range conditions extract postfix range for skip scan
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> postfix_filters_; // filters evaluated before index back, but not index prefix
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> table_filters_;  // filters evaluated after index back

  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> access_columns_;

  OptTableMetas *table_metas_;
  OptSelectivityCtx *sel_ctx_;
  // the following information are useful when estimating cost
  RowCountEstMethod row_est_method_; // row_est_method
  double prefix_filter_sel_;
  double pushdown_prefix_filter_sel_;
  double postfix_filter_sel_;
  double table_filter_sel_;
  double join_filter_sel_;
  double ss_prefix_ndv_;  // skip scan prefix columns NDV
  double ss_postfix_range_filters_sel_;
  common::ObSimpleBatch::ObBatchType batch_type_;
  SampleInfo sample_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCostTableScanInfo);
};

struct ObCostBaseJoinInfo : public ObTwoNodeCostInfo
{
  ObCostBaseJoinInfo(double left_rows, double left_width,
                     double right_rows, double right_width,
                     ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
                     const common::ObIArray<ObRawExpr *> &equal_join_conditions,
                     const common::ObIArray<ObRawExpr *> &other_join_conditions,
                     const common::ObIArray<ObRawExpr *> &filters,
                     OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
    : ObTwoNodeCostInfo(left_rows, left_width,
                        right_rows, right_width,
                        table_metas, sel_ctx),
      left_ids_(left_ids),
      right_ids_(right_ids),
      join_type_(join_type),
      equal_join_conditions_(equal_join_conditions),
      other_join_conditions_(other_join_conditions),
      filters_(filters)
  { }
  virtual ~ObCostBaseJoinInfo() { };

  TO_STRING_KV(K_(left_rows), K_(right_rows),
               K_(left_width), K_(right_width),
               K_(left_ids), K_(right_ids), K(join_type_),
               K_(equal_join_conditions), K_(other_join_conditions));
  ObRelIds left_ids_;
  ObRelIds right_ids_;
  ObJoinType join_type_;
  const common::ObIArray<ObRawExpr*> &equal_join_conditions_;
  const common::ObIArray<ObRawExpr*> &other_join_conditions_;
  // for outer join, denote where condition
  const common::ObIArray<ObRawExpr*> &filters_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCostBaseJoinInfo);
};

struct ObCostNLJoinInfo : public ObCostBaseJoinInfo
{
  ObCostNLJoinInfo(double left_rows, double left_cost, double left_width,
                   double right_rows, double right_cost, double right_width,
                   ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
                   double anti_or_semi_match_sel,
                   bool with_nl_param,
                   bool need_mat,
                   bool right_has_px_rescan,
                   int64_t parallel,
                   const common::ObIArray<ObRawExpr *> &equal_join_conditions,
                   const common::ObIArray<ObRawExpr *> &other_join_conditions,
                   const common::ObIArray<ObRawExpr *> &filters,
                   OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
   : ObCostBaseJoinInfo(left_rows, left_width,
                        right_rows, right_width,
                        left_ids, right_ids, join_type,
                        equal_join_conditions,
                        other_join_conditions,
                        filters,
                        table_metas,
                        sel_ctx),
      left_cost_(left_cost),
      right_cost_(right_cost),
      anti_or_semi_match_sel_(anti_or_semi_match_sel),
      parallel_(parallel),
      with_nl_param_(with_nl_param),
      need_mat_(need_mat),
      right_has_px_rescan_(right_has_px_rescan)
  { }
  virtual ~ObCostNLJoinInfo() { }
  TO_STRING_KV(K_(left_rows), K_(left_cost), K_(right_rows), K_(right_cost),
               K_(left_width), K_(right_width),
               K_(left_ids), K_(right_ids), K_(join_type),
               K_(with_nl_param), K_(need_mat), K_(right_has_px_rescan),
               K_(equal_join_conditions), K_(other_join_conditions), K_(filters));
  double left_cost_;
  double right_cost_;
  double anti_or_semi_match_sel_;
  int64_t parallel_;
  bool with_nl_param_;
  bool need_mat_;
  bool right_has_px_rescan_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCostNLJoinInfo);
};

/**
 * so far hash join info only contain variables from base class,
 * in future, we will change cost model and may need additional information
 */
struct ObCostMergeJoinInfo : public ObCostBaseJoinInfo
{
  ObCostMergeJoinInfo(double left_rows, double left_width,
                      double right_rows, double right_width,
                      ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
                      const common::ObIArray<ObRawExpr *> &equal_join_conditions,
                      const common::ObIArray<ObRawExpr *> &other_join_conditions,
                      const common::ObIArray<ObRawExpr *> &filters,
                      double equal_cond_sel, double other_cond_sel,
                      OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
   : ObCostBaseJoinInfo(left_rows, left_width,
                        right_rows, right_width,
                        left_ids, right_ids, join_type,
                        equal_join_conditions, other_join_conditions, filters,
                        table_metas, sel_ctx),
     equal_cond_sel_(equal_cond_sel),
     other_cond_sel_(other_cond_sel)
  { }
  virtual ~ObCostMergeJoinInfo() { };
  TO_STRING_KV(K_(left_rows), K_(right_rows),
               K_(left_width), K_(right_width),
               K_(left_ids), K_(right_ids), K_(join_type),
               K_(equal_join_conditions), K_(other_join_conditions), K_(filters));
  double equal_cond_sel_;
  double other_cond_sel_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCostMergeJoinInfo);
};

/**
 * so far hash join info only contain variables from base class,
 * in future, we will change cost model and may need additional information
 */
struct ObCostHashJoinInfo : public ObCostBaseJoinInfo
{
  ObCostHashJoinInfo(double left_rows, double left_width,
                     double right_rows, double right_width,
                     ObRelIds left_ids, ObRelIds right_ids, ObJoinType join_type,
                     const common::ObIArray<ObRawExpr *> &equal_join_conditions,
                     const common::ObIArray<ObRawExpr *> &other_join_conditions,
                     const common::ObIArray<ObRawExpr *> &filters,
                     const ObIArray<JoinFilterInfo> &join_filter_infos,
                     double equal_cond_sel, double other_cond_sel,
                     OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
   : ObCostBaseJoinInfo(left_rows, left_width,
                        right_rows, right_width,
                        left_ids, right_ids, join_type,
                        equal_join_conditions, other_join_conditions, filters,
                        table_metas, sel_ctx),
     join_filter_infos_(join_filter_infos),
     equal_cond_sel_(equal_cond_sel),
     other_cond_sel_(other_cond_sel)
  { };
  TO_STRING_KV(K_(left_rows), K_(right_rows),
               K_(left_width), K_(right_width),
               K_(left_ids), K_(right_ids), K_(join_type),
               K_(equal_join_conditions), K_(other_join_conditions), K_(filters));
  virtual ~ObCostHashJoinInfo() { };
  const ObIArray<JoinFilterInfo> &join_filter_infos_;
  double equal_cond_sel_;
  double other_cond_sel_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCostHashJoinInfo);
};

struct ObSubplanFilterCostInfo
{
  ObSubplanFilterCostInfo(const ObIArray<ObBasicCostInfo> &children,
                          const ObBitSet<> &onetime_idxs,
                          const ObBitSet<> &initplan_idxs)
    : children_(children), onetime_idxs_(onetime_idxs), initplan_idxs_(initplan_idxs)
  { }
  TO_STRING_KV(K_(children), K_(onetime_idxs), K_(initplan_idxs));

  const ObIArray<ObBasicCostInfo> &children_;
  const ObBitSet<> &onetime_idxs_;
  const ObBitSet<> &initplan_idxs_;
};

struct ObCostMergeSetInfo
{
  ObCostMergeSetInfo(const ObIArray<ObBasicCostInfo> &children,
                     int64_t op, int64_t num_select_items)
    : children_(children), op_(op), num_select_items_(num_select_items){}

  const ObIArray<ObBasicCostInfo> &children_;
  int64_t op_;
  int64_t num_select_items_;
};

struct ObCostHashSetInfo : public ObTwoNodeCostInfo
{
  ObCostHashSetInfo(double left_rows,
                    double left_width,
                    double right_rows,
                    double right_width,
                    int64_t op,
                    const ObIArray<ObRawExpr *> &hash_columns,
                    OptTableMetas *table_metas, OptSelectivityCtx *sel_ctx)
    : ObTwoNodeCostInfo(
        left_rows,
        left_width,
        right_rows,
        right_width,
        table_metas,
        sel_ctx),
      op_(op),
      hash_columns_(hash_columns) {}

  int64_t op_;
  const ObIArray<ObRawExpr *> &hash_columns_;
};

struct ObSortCostInfo
{
  ObSortCostInfo(double rows,
                 double width,
                 int64_t prefix_pos,
                 const ObIArray<OrderItem> &order_items,
                 const bool is_local_order,
                 OptTableMetas *table_metas = NULL,
                 OptSelectivityCtx *sel_ctx = NULL,
                 double topn = -1,
                 int64_t part_cnt = 0)
  : rows_(rows),
    width_(width),
    prefix_pos_(prefix_pos),
    order_items_(order_items),
    is_local_merge_sort_(is_local_order),
    table_metas_(table_metas),
    sel_ctx_(sel_ctx),
    topn_(topn),
    part_cnt_(part_cnt)
  {}
  TO_STRING_KV(K_(rows), K_(width), K_(prefix_pos), K_(order_items),
               K_(is_local_merge_sort), K_(topn), K_(part_cnt));
  double rows_;
  double width_;
  // not prefix sort if prefix_pos_ <= 0
  int64_t prefix_pos_;
  const ObIArray<OrderItem> &order_items_;
  bool is_local_merge_sort_;
  // used to calculate ndv in prefix sort
  OptTableMetas *table_metas_;
  OptSelectivityCtx *sel_ctx_;
  // not top-n sort if topn_ < 0
  double topn_;
  // not hash_based sort if part_cnt <= 0
  int64_t part_cnt_;
};

struct ObDelUpCostInfo
{
  ObDelUpCostInfo(double affect_rows,
                  double index_count,
                  double constraint_count)
  :affect_rows_(affect_rows),
  index_count_(index_count),
  constraint_count_(constraint_count)
  {}

  TO_STRING_KV(
  K_(affect_rows),
  K_(index_count),
  K_(constraint_count)
  );

  double affect_rows_;
  double index_count_;
  double constraint_count_;
};

struct ObExchCostInfo
{
  ObExchCostInfo(double rows,
                   double width,
                   ObPQDistributeMethod::Type dist_method,
                   int64_t out_parallel,
                   int64_t in_parallel,
                   bool is_local_order,
                   const ObIArray<OrderItem> &sort_keys,
                   int64_t in_server_cnt)
    : sort_keys_(sort_keys),
      rows_(rows),
      width_(width),
      dist_method_(dist_method),
      out_parallel_(out_parallel),
      in_parallel_(in_parallel),
      is_local_order_(is_local_order),
      in_server_cnt_(in_server_cnt)
    { }
   const ObIArray<OrderItem> &sort_keys_;
   double rows_;
   double width_;
   ObPQDistributeMethod::Type dist_method_;
   int64_t out_parallel_;
   int64_t in_parallel_;
   bool is_local_order_;
   int64_t in_server_cnt_;
};

struct ObExchInCostInfo
{
  ObExchInCostInfo(double rows,
                   double width,
                   ObPQDistributeMethod::Type dist_method,
                   int64_t parallel,
                    int64_t server_cnt,
                   bool is_local_order,
                   const ObIArray<OrderItem> &sort_keys)
  : sort_keys_(sort_keys),
    rows_(rows),
    width_(width),
    dist_method_(dist_method),
    parallel_(parallel),
    server_cnt_(server_cnt),
    is_local_order_(is_local_order)
  {}
  const ObIArray<OrderItem> &sort_keys_;
  double rows_;
  double width_;
  ObPQDistributeMethod::Type dist_method_;
  int64_t parallel_;
  int64_t server_cnt_;
  bool is_local_order_;
};

struct ObExchOutCostInfo
{
  ObExchOutCostInfo(double rows,
                    double width,
                    ObPQDistributeMethod::Type dist_method,
                    int64_t parallel,
                    int64_t server_cnt)
  : rows_(rows),
    width_(width),
    dist_method_(dist_method),
    parallel_(parallel),
    server_cnt_(server_cnt)
  {}
  double rows_;
  double width_;
  ObPQDistributeMethod::Type dist_method_;
  int64_t parallel_;
  int64_t server_cnt_;
};

class ObOptEstCostModel
{
public:
  const static int64_t DEFAULT_LOCAL_ORDER_DEGREE;
  const static int64_t DEFAULT_MAX_STRING_WIDTH;
  const static int64_t DEFAULT_FIXED_OBJ_WIDTH;

  struct ObCostParams
  {
    explicit ObCostParams(
    const double DEFAULT_CPU_TUPLE_COST,
    const double DEFAULT_TABLE_SCAN_CPU_TUPLE_COST,
    const double DEFAULT_MICRO_BLOCK_SEQ_COST,
    const double DEFAULT_MICRO_BLOCK_RND_COST,
    const double DEFAULT_PROJECT_COLUMN_SEQ_INT_COST,
    const double DEFAULT_PROJECT_COLUMN_SEQ_NUMBER_COST,
    const double DEFAULT_PROJECT_COLUMN_SEQ_CHAR_COST,
    const double DEFAULT_PROJECT_COLUMN_RND_INT_COST,
    const double DEFAULT_PROJECT_COLUMN_RND_NUMBER_COST,
    const double DEFAULT_PROJECT_COLUMN_RND_CHAR_COST,
    const double DEFAULT_FETCH_ROW_RND_COST,
    const double DEFAULT_CMP_INT_COST,
    const double DEFAULT_CMP_NUMBER_COST,
    const double DEFAULT_CMP_CHAR_COST,
    const double DEFAULT_CMP_GEO_COST,
    const double INVALID_CMP_COST,
    const double DEFAULT_HASH_INT_COST,
    const double DEFAULT_HASH_NUMBER_COST,
    const double DEFAULT_HASH_CHAR_COST,
    const double INVALID_HASH_COST,
    const double DEFAULT_MATERIALIZE_PER_BYTE_WRITE_COST,
    const double DEFAULT_READ_MATERIALIZED_PER_ROW_COST,
    const double DEFAULT_PER_AGGR_FUNC_COST,
    const double DEFAULT_PER_WIN_FUNC_COST,
    const double DEFAULT_CPU_OPERATOR_COST,
    const double DEFAULT_JOIN_PER_ROW_COST,
    const double DEFAULT_BUILD_HASH_PER_ROW_COST,
    const double DEFAULT_PROBE_HASH_PER_ROW_COST,
    const double DEFAULT_RESCAN_COST,
    const double DEFAULT_NETWORK_SER_PER_BYTE_COST,
    const double DEFAULT_NETWORK_DESER_PER_BYTE_COST,
    const double DEFAULT_NETWORK_TRANS_PER_BYTE_COST,
    const double DEFAULT_PX_RESCAN_PER_ROW_COST,
    const double DEFAULT_PX_BATCH_RESCAN_PER_ROW_COST,
    const double DEFAULT_NL_SCAN_COST,
    const double DEFAULT_BATCH_NL_SCAN_COST,
    const double DEFAULT_NL_GET_COST,
    const double DEFAULT_BATCH_NL_GET_COST,
    const double DEFAULT_TABLE_LOOPUP_PER_ROW_RPC_COST,
    const double DEFAULT_INSERT_PER_ROW_COST,
    const double DEFAULT_INSERT_INDEX_PER_ROW_COST,
    const double DEFAULT_INSERT_CHECK_PER_ROW_COST,
    const double DEFAULT_UPDATE_PER_ROW_COST,
    const double DEFAULT_UPDATE_INDEX_PER_ROW_COST,
    const double DEFAULT_UPDATE_CHECK_PER_ROW_COST,
    const double DEFAULT_DELETE_PER_ROW_COST,
    const double DEFAULT_DELETE_INDEX_PER_ROW_COST,
    const double DEFAULT_DELETE_CHECK_PER_ROW_COST,
    const double DEFAULT_SPATIAL_PER_ROW_COST,
    const double DEFAULT_RANGE_COST
    )
    : CPU_TUPLE_COST(DEFAULT_CPU_TUPLE_COST),
      TABLE_SCAN_CPU_TUPLE_COST(DEFAULT_TABLE_SCAN_CPU_TUPLE_COST),
      MICRO_BLOCK_SEQ_COST(DEFAULT_MICRO_BLOCK_SEQ_COST),
      MICRO_BLOCK_RND_COST(DEFAULT_MICRO_BLOCK_RND_COST),
      PROJECT_COLUMN_SEQ_INT_COST(DEFAULT_PROJECT_COLUMN_SEQ_INT_COST),
      PROJECT_COLUMN_SEQ_NUMBER_COST(DEFAULT_PROJECT_COLUMN_SEQ_NUMBER_COST),
      PROJECT_COLUMN_SEQ_CHAR_COST(DEFAULT_PROJECT_COLUMN_SEQ_CHAR_COST),
      PROJECT_COLUMN_RND_INT_COST(DEFAULT_PROJECT_COLUMN_RND_INT_COST),
      PROJECT_COLUMN_RND_NUMBER_COST(DEFAULT_PROJECT_COLUMN_RND_NUMBER_COST),
      PROJECT_COLUMN_RND_CHAR_COST(DEFAULT_PROJECT_COLUMN_RND_CHAR_COST),
      FETCH_ROW_RND_COST(DEFAULT_FETCH_ROW_RND_COST),
      CMP_DEFAULT_COST(DEFAULT_CMP_NUMBER_COST),
      CMP_INT_COST(DEFAULT_CMP_INT_COST),
      CMP_NUMBER_COST(DEFAULT_CMP_NUMBER_COST),
      CMP_CHAR_COST(DEFAULT_CMP_CHAR_COST),
      CMP_SPATIAL_COST(DEFAULT_CMP_GEO_COST),
      HASH_DEFAULT_COST(DEFAULT_HASH_INT_COST),
      HASH_INT_COST(DEFAULT_HASH_INT_COST),
      HASH_NUMBER_COST(DEFAULT_HASH_NUMBER_COST),
      HASH_CHAR_COST(DEFAULT_HASH_CHAR_COST),
      MATERIALIZE_PER_BYTE_WRITE_COST(DEFAULT_MATERIALIZE_PER_BYTE_WRITE_COST),
      READ_MATERIALIZED_PER_ROW_COST(DEFAULT_READ_MATERIALIZED_PER_ROW_COST),
      PER_AGGR_FUNC_COST(DEFAULT_PER_AGGR_FUNC_COST),
      PER_WIN_FUNC_COST(DEFAULT_PER_WIN_FUNC_COST),
      CPU_OPERATOR_COST(DEFAULT_CPU_OPERATOR_COST),
      JOIN_PER_ROW_COST(DEFAULT_JOIN_PER_ROW_COST),
      BUILD_HASH_PER_ROW_COST(DEFAULT_BUILD_HASH_PER_ROW_COST),
      PROBE_HASH_PER_ROW_COST(DEFAULT_PROBE_HASH_PER_ROW_COST),
      RESCAN_COST(DEFAULT_RESCAN_COST),
      NETWORK_SER_PER_BYTE_COST(DEFAULT_NETWORK_SER_PER_BYTE_COST),
      NETWORK_DESER_PER_BYTE_COST(DEFAULT_NETWORK_DESER_PER_BYTE_COST),
      NETWORK_TRANS_PER_BYTE_COST(DEFAULT_NETWORK_TRANS_PER_BYTE_COST),
      PX_RESCAN_PER_ROW_COST(DEFAULT_PX_RESCAN_PER_ROW_COST),
      PX_BATCH_RESCAN_PER_ROW_COST(DEFAULT_PX_BATCH_RESCAN_PER_ROW_COST),
      NL_SCAN_COST(DEFAULT_NL_SCAN_COST),
      BATCH_NL_SCAN_COST(DEFAULT_BATCH_NL_SCAN_COST),
      NL_GET_COST(DEFAULT_NL_GET_COST),
      BATCH_NL_GET_COST(DEFAULT_BATCH_NL_GET_COST),
      TABLE_LOOPUP_PER_ROW_RPC_COST(DEFAULT_TABLE_LOOPUP_PER_ROW_RPC_COST),
      INSERT_PER_ROW_COST(DEFAULT_INSERT_PER_ROW_COST),
      INSERT_INDEX_PER_ROW_COST(DEFAULT_INSERT_INDEX_PER_ROW_COST),
      INSERT_CHECK_PER_ROW_COST(DEFAULT_INSERT_CHECK_PER_ROW_COST),
      UPDATE_PER_ROW_COST(DEFAULT_UPDATE_PER_ROW_COST),
      UPDATE_INDEX_PER_ROW_COST(DEFAULT_UPDATE_INDEX_PER_ROW_COST),
      UPDATE_CHECK_PER_ROW_COST(DEFAULT_UPDATE_CHECK_PER_ROW_COST),
      DELETE_PER_ROW_COST(DEFAULT_DELETE_PER_ROW_COST),
      DELETE_INDEX_PER_ROW_COST(DEFAULT_DELETE_INDEX_PER_ROW_COST),
      DELETE_CHECK_PER_ROW_COST(DEFAULT_DELETE_CHECK_PER_ROW_COST),
      SPATIAL_PER_ROW_COST(DEFAULT_SPATIAL_PER_ROW_COST),
      RANGE_COST(DEFAULT_RANGE_COST)
    {}
    /** 读取一行的CPU开销，基本上只包括get_next_row()操作 */
    double CPU_TUPLE_COST;
    /** 存储层吐出一行的代价 **/
    double TABLE_SCAN_CPU_TUPLE_COST;
    /** 顺序读取一个微块并反序列化的开销 */
    double MICRO_BLOCK_SEQ_COST;
    /** 随机读取一个微块并反序列化的开销 */
    double MICRO_BLOCK_RND_COST;
    /** 顺序读取时投影一列int类型的开销 */
    double PROJECT_COLUMN_SEQ_INT_COST;
    /** 顺序读取时投影一列number类型的开销 */
    double PROJECT_COLUMN_SEQ_NUMBER_COST;
    /** 顺序读取时投影一个字符的开销 */
    double PROJECT_COLUMN_SEQ_CHAR_COST;
    /** 随机读取时投影一列int类型的开销 */
    double PROJECT_COLUMN_RND_INT_COST;
    /** 随机读取时投影一列number类型的开销 */
    double PROJECT_COLUMN_RND_NUMBER_COST;
    /** 随机读取时投影一个字符的开销 */
    double PROJECT_COLUMN_RND_CHAR_COST;
    /** 随机读取中定位某一行所在位置的开销 */
    double FETCH_ROW_RND_COST;
    /** 对于无法获取数据类型的情况，默认的比较代价 */
    double CMP_DEFAULT_COST;
    /** 比较一次整型变量的代价 */
    double CMP_INT_COST;
    /** 比较一次ObNumber的代价 */
    double CMP_NUMBER_COST;
    /** 比较一次字符串的代价 */
    double CMP_CHAR_COST;
    /** 比较一次空间数据的代价 */
    double CMP_SPATIAL_COST;
    /** 对于无法获取数据类型的情况，默认的hash代价 */
    double HASH_DEFAULT_COST;
    /** 计算一个整型变量的hash值的代价 */
    double HASH_INT_COST;
    /** 计算一个ObNumber的hash值的代价 */
    double HASH_NUMBER_COST;
    /** 计算一个字符串的hash值的代价 */
    double HASH_CHAR_COST;
    /** 物化一个字节的代价 */
    double MATERIALIZE_PER_BYTE_WRITE_COST;
    /** 读取物化后的行的代价，即对物化后数据结构的get_next_row() */
    double READ_MATERIALIZED_PER_ROW_COST;
    /** 一次聚集函数计算的代价 */
    double PER_AGGR_FUNC_COST;
    /** 一次窗口函数计算的代价 */
    double PER_WIN_FUNC_COST;
    /** 一次操作的基本代价 */
    double CPU_OPERATOR_COST;
    /** 连接两表的一行的基本代价 */
    double JOIN_PER_ROW_COST;
    /** 构建hash table时每行的均摊代价 */
    double BUILD_HASH_PER_ROW_COST;
    /** 查询hash table时每行的均摊代价 */
    double PROBE_HASH_PER_ROW_COST;
    double RESCAN_COST;
    /*network serialization cost for one byte*/
    double NETWORK_SER_PER_BYTE_COST;
    /*network de-serialization cost for one byte*/
    double NETWORK_DESER_PER_BYTE_COST;
    /** 网络传输1个字节的代价 */
    double NETWORK_TRANS_PER_BYTE_COST;
    /*additional px-rescan cost*/
    double PX_RESCAN_PER_ROW_COST;
    double PX_BATCH_RESCAN_PER_ROW_COST;
    //条件下压nestloop join右表扫一次的代价
    double NL_SCAN_COST;
    //条件下压batch nestloop join右表扫一次的代价
    double BATCH_NL_SCAN_COST;
    //条件下压nestloop join右表GET一次的代价
    double NL_GET_COST;
    //条件下压batch nestloop join右表GET一次的代价
    double BATCH_NL_GET_COST;
    //table look up一行的rpc代价
    double TABLE_LOOPUP_PER_ROW_RPC_COST;
    //insert一行主表的代价
    double INSERT_PER_ROW_COST;
    //insert一行索引表的代价
    double INSERT_INDEX_PER_ROW_COST;
    //insert单个约束检查代价
    double INSERT_CHECK_PER_ROW_COST;
    //update一行主表的代价
    double UPDATE_PER_ROW_COST;
    //update一行索引表的代价
    double UPDATE_INDEX_PER_ROW_COST;
    //update单个约束检查代价
    double UPDATE_CHECK_PER_ROW_COST;
    //delete一行主表的代价
    double DELETE_PER_ROW_COST;
    //delete一行索引表的代价
    double DELETE_INDEX_PER_ROW_COST;
    //delete单个约束检查代价
    double DELETE_CHECK_PER_ROW_COST;
    //空间索引扫描的线性参数
    double SPATIAL_PER_ROW_COST;
    //存储层切换一次range的代价
    double RANGE_COST;
  };

	ObOptEstCostModel(
			const double (&comparison_params)[common::ObMaxTC + 1],
			const double (&hash_params)[common::ObMaxTC + 1],
			const ObCostParams &cost_params)
		:comparison_params_(comparison_params),
		hash_params_(hash_params),
		cost_params_(cost_params)
	{}

  virtual ~ObOptEstCostModel()=default;

  int cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
										double &cost,
										common::ObIArray<ObExprSelPair> &all_predicate_sel);

  int cost_mergejoin(const ObCostMergeJoinInfo &est_cost_info,
                    double &cost);

  int cost_hashjoin(const ObCostHashJoinInfo &est_cost_info,
                    double &cost);

  int cost_sort_and_exchange(OptTableMetas *table_metas,
														OptSelectivityCtx *sel_ctx,
														const ObPQDistributeMethod::Type dist_method,
														const bool is_distributed,
														const bool input_local_order,
														const double input_card,
														const double input_width,
														const double input_cost,
														const int64_t out_parallel,
														const int64_t in_server_cnt,
														const int64_t in_parallel,
														const ObIArray<OrderItem> &expected_ordering,
														const bool need_sort,
														const int64_t prefix_pos,
														double &cost);

  // 对外提供两个估算排序算子代价的接口，一个使用ObRawExpr表示sort key，另一个使用
  // OrderItem。
  // 其它的参数信息通过cost_info传入，内部基于参数信息应该采用哪种排序
  // 算法（目前包括普通排序、top-n 排序、前缀排序），对外暂不暴露实际排序算法的估算接口
  int cost_sort(const ObSortCostInfo &cost_info,
                double &cost);

  int cost_exchange(const ObExchCostInfo &cost_info,
                    double &ex_cost);

  int cost_exchange_in(const ObExchInCostInfo &cost_info,
                      double &cost);

  int cost_exchange_out(const ObExchOutCostInfo &cost_info,
                        double &cost);

  double cost_merge_group(double rows,
													double res_rows,
													double width,
													const ObIArray<ObRawExpr *> &group_columns,
													int64_t agg_col_count);

  double cost_hash_group(double rows,
												double res_rows,
												double width,
												const ObIArray<ObRawExpr *> &group_columns,
												int64_t agg_col_count);

  double cost_scalar_group(double rows,
                          int64_t agg_col_count);

  double cost_merge_distinct(double rows,
														double res_rows,
														double width,
														const ObIArray<ObRawExpr *> &distinct_columns);

  double cost_hash_distinct(double rows,
														double res_rows,
														double width,
														const ObIArray<ObRawExpr *> &disinct_columns);

  double cost_get_rows(double rows);

  double cost_sequence(double rows, double uniq_sequence_cnt);

  double cost_material(const double rows, const double average_row_size);

  double cost_read_materialized(const double rows);

  double cost_filter_rows(double rows, ObIArray<ObRawExpr*> &filters);

  int cost_subplan_filter(const ObSubplanFilterCostInfo &info, double &cost);

  int cost_union_all(const ObCostMergeSetInfo &info, double &cost);

  int cost_merge_set(const ObCostMergeSetInfo &info, double &cost);

  int cost_hash_set(const ObCostHashSetInfo &info, double &cost);

  int cost_project(double rows, const ObIArray<ColumnItem> &columns, bool is_seq, double &cost);

  int cost_full_table_scan_project(double rows, const ObCostTableScanInfo &est_cost_info, double &cost);

  double cost_quals(double rows, const ObIArray<ObRawExpr *> &quals, bool need_scale = true);

  double cost_hash(double rows, const ObIArray<ObRawExpr *> &hash_exprs);

  double cost_late_materialization_table_get(int64_t column_cnt);

  void cost_late_materialization_table_join(double left_card,
																						double left_cost,
																						double right_card,
																						double right_cost,
																						double &op_cost,
																						double &cost);
  void cost_late_materialization(double left_card,
																double left_cost,
																int64_t column_count,
																double &cost);

  int get_sort_cmp_cost(const common::ObIArray<sql::ObExprResType> &types, double &cost);

  int cost_window_function(double rows, double width, double win_func_cnt, double &cost);

  int cost_insert(ObDelUpCostInfo& cost_info, double &cost);

  int cost_update(ObDelUpCostInfo& cost_info, double &cost);
	
  int cost_delete(ObDelUpCostInfo& cost_info, double &cost);

	  /*
   * entry point for estimating table access cost
   */
  int cost_table(const ObCostTableScanInfo &est_cost_info,
								int64_t parallel,
								double query_range_row_count,
								double phy_query_range_row_count,
								double &cost,
								double &index_back_cost);

  int cost_table_for_parallel(const ObCostTableScanInfo &est_cost_info,
                              const int64_t parallel,
                              const double part_cnt_per_dop,
                              double query_range_row_count,
                              double phy_query_range_row_count,
                              double &px_cost,
                              double &cost);

  int cost_px(int64_t parallel, double &px_cost);

  int cost_range_scan(const ObTableMetaInfo& table_meta_info,
                      const ObIArray<ObRawExpr *> &filters,
                      int64_t index_column_count,
                      int64_t range_count,
                      double range_sel,
                      double &cost);

protected:
  int cost_sort(const ObSortCostInfo &cost_info,
								const common::ObIArray<ObExprResType> &order_col_types,
								double &cost);

  int cost_part_sort(const ObSortCostInfo &cost_info,
											const ObIArray<ObRawExpr *> &order_exprs,
											const ObIArray<ObExprResType> &order_col_types,
											double &cost);

  int cost_prefix_sort(const ObSortCostInfo &cost_info,
											const ObIArray<ObRawExpr *> &order_exprs,
											const int64_t topn_count,
											double &cost);

  int cost_topn_sort(const ObSortCostInfo &cost_info,
										const ObIArray<ObExprResType> &types,
										double &cost);

  int cost_local_order_sort(const ObSortCostInfo &cost_info,
														const ObIArray<ObExprResType> &types,
														double &cost);

  int cost_topn_sort_inner(const ObIArray<ObExprResType> &types,
													double rows,
													double n,
													double &cost);

  //calculate real sort cost (std::sort)
  int cost_sort_inner(const common::ObIArray<sql::ObExprResType> &types,
											double row_count,
											double &cost);

  int cost_local_order_sort_inner(const common::ObIArray<sql::ObExprResType> &types,
																	double row_count,
																	double &cost);

  // estimate cost for non-virtual table
  int cost_normal_table(const ObCostTableScanInfo &est_cost_info,
												int64_t parallel,
												const double query_range_row_count,
												const double phy_query_range_row_count,
												double &cost,
												double &index_back_cost);

  //estimate one batch
  int cost_table_one_batch(const ObCostTableScanInfo &est_cost_info,
                          const double part_cnt_per_dop,
													const common::ObSimpleBatch::ObBatchType &type,
													const double logical_output_row_count,
													const double physical_output_row_count,
													double &cost,
													double &index_back_cost);

  // estimate one batch table scan cost
  int cost_table_scan_one_batch(const ObCostTableScanInfo &est_cost_info,
																const double logical_output_row_count,
																const double physical_output_row_count,
																double &cost,
																double &index_back_cost);

  //estimate one batch table get cost
  int cost_table_get_one_batch(const ObCostTableScanInfo &est_cost_info,
															const double output_row_count,
															double &cost,
															double &index_back_cost);

  // estimate the network transform and rpc cost for global index
  int cost_table_lookup_rpc(double row_count,
														const ObCostTableScanInfo &est_cost_info,
														double &cost);

  // estimate the spatial calculation and sort cost for spatial index
  int cost_table_get_one_batch_spatial(double row_count,
                                       const ObCostTableScanInfo &est_cost_info,
                                       double &cost);

  // @param[in] is_index_back 仅在对索引扫描的回表扫描进行估计时为true
  //estimate one batch table get cost, truly estimation function
  int cost_table_get_one_batch_inner(double row_count,
																		const ObCostTableScanInfo &est_cost_info,
																		bool is_scan_index,
																		double &res);

  // @param[in] is_index_back 仅在对索引扫描的回表扫描进行估计时为true
  //estimate one batch table scan cost, truly estimation function
  virtual int cost_table_scan_one_batch_inner(double row_count,
                                              const ObCostTableScanInfo &est_cost_info,
                                              bool is_scan_index,
                                              double &res);
  int cost_table_scan_one_batch_io_cost(double row_count,
                                        const ObCostTableScanInfo &est_cost_info,
                                        double &io_cost);
  int cost_table_get_one_batch_io_cost(const double row_count,
                                       const ObCostTableScanInfo &est_cost_info,
																			 bool is_scan_index,
                                       double &io_cost);

  int cost_skip_scan_prefix_scan_one_row(const ObCostTableScanInfo &est_cost_info,
                                         double &cost);
protected:
  const double (&comparison_params_)[common::ObMaxTC + 1];
  const double (&hash_params_)[common::ObMaxTC + 1];
  const ObCostParams &cost_params_;
  DISALLOW_COPY_AND_ASSIGN(ObOptEstCostModel);
};

}
}

#endif /* OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_ */
