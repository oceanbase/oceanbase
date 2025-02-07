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
class ObOptCostModelParameter;
class OptSystemStat;

enum RowCountEstMethod { INVALID_METHOD = 0 }; // deprecated
enum ObBaseTableEstBasicMethod
{
  EST_INVALID   = 0,
  EST_DEFAULT   = 1 << 0,
  EST_STAT      = 1 << 1,
  EST_STORAGE   = 1 << 2,
  EST_DS_BASIC  = 1 << 3,
  EST_DS_FULL   = 1 << 4,
};
typedef uint64_t ObBaseTableEstMethod;

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
      table_type_(share::schema::MAX_TABLE_TYPE),
      is_broadcast_table_(false)
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
  bool is_broadcast_table_;
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
      is_fulltext_index_(false),
      is_multivalue_index_(false),
      is_vector_index_(false),
      index_micro_block_count_(-1)
  { }
  virtual ~ObIndexMetaInfo()
  { }
  void assign(const ObIndexMetaInfo &index_meta_info);
  double get_micro_block_numbers() const;
  inline bool is_domain_index() const { return is_geo_index_ || is_fulltext_index_ || is_multivalue_index_ || is_vector_index_;}
  TO_STRING_KV(K_(ref_table_id), K_(index_id), K_(index_micro_block_size),
               K_(index_part_count), K_(index_part_size),
               K_(index_column_count), K_(is_index_back),
               K_(is_unique_index), K_(is_fulltext_index), K_(index_micro_block_count));
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
  bool is_fulltext_index_; // is fulltext index
  bool is_multivalue_index_; // is multivalue index
  bool is_vector_index_;   // is vector index
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

struct ObCostColumnGroupInfo {
  ObCostColumnGroupInfo()
  :micro_block_count_(0.0),
  filter_sel_(1.0),
  skip_rate_(1.0),
  skip_filter_sel_(1.0)
  {
  }
  int assign(const ObCostColumnGroupInfo& info);

  TO_STRING_KV(
    K_(filters),
    K_(access_column_items),
    K_(column_id),
    K_(micro_block_count),
    K_(filter_sel),
    K_(skip_rate),
    K_(skip_filter_sel)
  );
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> filters_;
  common::ObSEArray<ColumnItem, 4, common::ModulePageAllocator, true> access_column_items_;
  uint64_t column_id_;
  int64_t micro_block_count_;
  double filter_sel_;
  double skip_rate_;
  double skip_filter_sel_;
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
     is_das_scan_(false),
     is_rescan_(false),
     is_batch_rescan_(false),
     ranges_(),
     total_range_cnt_(0),
     ss_ranges_(),
     range_columns_(),
     prefix_filters_(),
     pushdown_prefix_filters_(),
     ss_postfix_range_filters_(),
     postfix_filters_(),
     table_filters_(),
     table_metas_(NULL),
     sel_ctx_(NULL),
     est_method_(EST_INVALID),
     prefix_filter_sel_(1.0),
     pushdown_prefix_filter_sel_(1.0),
     postfix_filter_sel_(1.0),
     table_filter_sel_(1.0),
     join_filter_sel_(1.0),
     ss_prefix_ndv_(1.0),
     ss_postfix_range_filters_sel_(1.0),
     logical_query_range_row_count_(0.0),
     phy_query_range_row_count_(0.0),
     index_back_row_count_(0.0),
     output_row_count_(0.0),
     batch_type_(common::ObSimpleBatch::ObBatchType::T_NONE),
     use_column_store_(false),
     at_most_one_range_(false),
     index_back_with_column_store_(false),
     rescan_left_server_list_(NULL),
     rescan_server_list_(NULL),
     limit_rows_(-1.0)
  { }
  virtual ~ObCostTableScanInfo()
  { }

  int assign(const ObCostTableScanInfo &other_est_cost_info);
  int has_exec_param(bool &bool_ret) const;
  int has_exec_param(const ObIArray<ObRawExpr *> &exprs, bool &bool_ret) const;

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(index_id),
               K_(table_meta_info), K_(index_meta_info),
               K_(access_column_items),
               K_(is_virtual_table), K_(is_unique),
               K_(is_das_scan), K_(is_rescan), K_(is_batch_rescan), K_(est_method),
               K_(prefix_filter_sel), K_(pushdown_prefix_filter_sel),
               K_(postfix_filter_sel), K_(table_filter_sel),
               K_(ss_prefix_ndv), K_(ss_postfix_range_filters_sel),
               K_(limit_rows), K_(total_range_cnt),
               K_(use_column_store),
               K_(index_back_with_column_store),
               K_(index_scan_column_group_infos),
               K_(index_back_column_group_infos));
  // the following information need to be set before estimating cost
  uint64_t table_id_; // table id
  uint64_t ref_table_id_; // ref table id
  uint64_t index_id_;  // index_id
  ObTableMetaInfo *table_meta_info_; // table related meta info
  ObIndexMetaInfo index_meta_info_; // index related meta info
  bool is_virtual_table_; // is virtual table
  bool is_unique_;  // whether query range is unique
  bool is_das_scan_;
  bool is_rescan_;
  bool is_batch_rescan_;
  ObRangesArray ranges_;  // all the ranges
  int64_t total_range_cnt_;
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
  ObBaseTableEstMethod est_method_;
  double prefix_filter_sel_;
  double pushdown_prefix_filter_sel_;
  double postfix_filter_sel_;
  double table_filter_sel_;
  double join_filter_sel_;
  double ss_prefix_ndv_;  // skip scan prefix columns NDV
  double ss_postfix_range_filters_sel_;
  double logical_query_range_row_count_;// 估计出的抽出的query range中所包含的行数(logical)
  double phy_query_range_row_count_;// 估计出的抽出的query range中所包含的行数(physical)
  double index_back_row_count_;// 估计出的需要回表的行数
  double output_row_count_;
  common::ObSimpleBatch::ObBatchType batch_type_;
  SampleInfo sample_info_;
  bool use_column_store_;
  bool at_most_one_range_;
  bool index_back_with_column_store_;
  common::ObSEArray<ObCostColumnGroupInfo, 4, common::ModulePageAllocator, true> index_scan_column_group_infos_;
  common::ObSEArray<ObCostColumnGroupInfo, 4, common::ModulePageAllocator, true> index_back_column_group_infos_;
  const common::ObIArray<common::ObAddr> *rescan_left_server_list_;
  const common::ObIArray<common::ObAddr> *rescan_server_list_;

  double limit_rows_;
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
                   double other_cond_sel,
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
      other_cond_sel_(other_cond_sel),
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
  double other_cond_sel_;
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

	ObOptEstCostModel(const ObOptCostModelParameter &cost_params,
                    const OptSystemStat &stat)
		:cost_params_(cost_params),
    sys_stat_(stat)
	{}

  virtual ~ObOptEstCostModel()=default;

  int cost_nestloop(const ObCostNLJoinInfo &est_cost_info,
										double &cost);

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

  int cost_project(double rows,
                   const ObIArray<ColumnItem> &columns,
                   bool is_get,
                   bool use_column_store,
                   double &cost);

  int cost_project(double rows,
                   const ObIArray<ObRawExpr*> &columns,
                   bool is_get,
                   bool use_column_store,
                   double &cost);

  int cost_full_table_scan_project(double rows,
                                   const ObCostTableScanInfo &est_cost_info,
                                   bool is_get,
                                   double &cost);

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
								double &cost);

  int cost_table_for_parallel(const ObCostTableScanInfo &est_cost_info,
                              const int64_t parallel,
                              const double part_cnt_per_dop,
                              double &px_cost,
                              double &cost);

  int cost_px(int64_t parallel, double &px_cost);

  int calc_range_cost(const ObTableMetaInfo& table_meta_info,
                      const ObIArray<ObRawExpr *> &filters,
                      int64_t index_column_count,
                      int64_t range_count,
                      double range_sel,
                      double &cost);
  int calc_pred_cost_per_row(const ObRawExpr *expr,
                            double card,
                            double &cost);

protected:
  int cost_sort(const ObSortCostInfo &cost_info,
								const common::ObIArray<ObExprResType> &order_col_types,
								double &cost);

  int cost_part_sort(const ObSortCostInfo &cost_info,
											const ObIArray<ObRawExpr *> &order_exprs,
											const ObIArray<ObExprResType> &order_col_types,
											double &cost);
  int cost_part_topn_sort(const ObSortCostInfo &cost_info,
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
  int cost_basic_table(const ObCostTableScanInfo &est_cost_info,
                        const double part_cnt_per_dop,
												double &cost);

  int cost_index_scan(const ObCostTableScanInfo &est_cost_info,
                      double row_count,
                      double &cost);

  int cost_index_back(const ObCostTableScanInfo &est_cost_info,
                      double row_count,
                      double limit_count,
                      double &cost);
  int cost_column_store_index_scan(const ObCostTableScanInfo &est_cost_info,
                                    double row_count,
                                    double &cost);

  int cost_column_store_index_back(const ObCostTableScanInfo &est_cost_info,
                                    double row_count,
                                    double limit_count,
                                    double &cost);
  int cost_row_store_index_scan(const ObCostTableScanInfo &est_cost_info,
                                double row_count,
                                double &cost);

  int cost_row_store_index_back(const ObCostTableScanInfo &est_cost_info,
                                double row_count,
                                double limit_count,
                                double &cost);
  int calc_das_rpc_cost(const ObCostTableScanInfo &est_cost_info,
                        double &das_rpc_cost);
  int get_rescan_rpc_cnt(const ObIArray<common::ObAddr> *left_server_list,
                         const ObIArray<common::ObAddr> *right_server_list,
                         double &remote_rpc_cnt,
                         double &local_rpc_cnt);
  // estimate the network transform and rpc cost for global index
  int cost_global_index_back_with_rp(double row_count,
                                    const ObCostTableScanInfo &est_cost_info,
                                    double &cost);

  int cost_range_scan(const ObCostTableScanInfo &est_cost_info,
                      bool is_scan_index,
                      double row_count,
                      double &cost);

  int cost_range_get(const ObCostTableScanInfo &est_cost_info,
                    bool is_scan_index,
                    double row_count,
                    double &cost);

  int range_get_io_cost(const ObCostTableScanInfo &est_cost_info,
                        bool is_scan_index,
                        double row_count,
                        double &cost);

  int range_scan_io_cost(const ObCostTableScanInfo &est_cost_info,
                        bool is_scan_index,
                        double row_count,
                        double &cost);

  int range_scan_cpu_cost(const ObCostTableScanInfo &est_cost_info,
                          bool is_scan_index,
                          double row_count,
                          bool is_get,
                          double &cost);

protected:
  const ObOptCostModelParameter &cost_params_;
  const OptSystemStat &sys_stat_;
  DISALLOW_COPY_AND_ASSIGN(ObOptEstCostModel);
};

class ObCostTableScanSimpleInfo {
  OB_UNIS_VERSION_V(1);
public:
  ObCostTableScanSimpleInfo()
  :is_index_back_(false),
  is_global_index_(false),
  part_count_(1),
  table_micro_blocks_(0),
  index_micro_blocks_(0),
  range_count_(1),
  table_row_count_(1),
  postix_filter_qual_cost_per_row_(0),
  table_filter_qual_cost_per_row_(0),
  index_scan_project_cost_per_row_(0),
  index_back_project_cost_per_row_(0),
  row_width_(0),
  is_spatial_index_(false),
  index_id_(0)
  { }
  ~ObCostTableScanSimpleInfo()=default;
  int init(const ObCostTableScanInfo &est_cost_info);
  int calculate_table_dop(double range_row_count,
                          double index_back_row_count,
                          int64_t part_cnt,
                          int64_t cost_threshold_us,
                          int64_t parallel_degree_limit,
                          int64_t &table_dop) const;
  int64_t get_range_columns_count() const { return range_count_; }
  bool get_is_spatial_index() const { return is_spatial_index_; }
  int64_t get_index_id() const { return index_id_; }

private:
  double calculate_table_scan_cost(double range_row_count,
                                   double index_back_row_count,
                                   int64_t part_cnt,
                                   int64_t parallel) const;
  bool is_index_back_;
  bool is_global_index_;
  int64_t part_count_;
  int64_t table_micro_blocks_;
  int64_t index_micro_blocks_;
  int64_t range_count_;
  double table_row_count_;
  double postix_filter_qual_cost_per_row_;
  double table_filter_qual_cost_per_row_;
  double index_scan_project_cost_per_row_;
  double index_back_project_cost_per_row_;
  double row_width_;
  bool is_spatial_index_;
  int64_t index_id_;
};

}
}

#endif /* OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_COST_MODEL_ */
