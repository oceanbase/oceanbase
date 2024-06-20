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

#ifndef _OB_DYNAMIC_SAMPLING_H_
#define _OB_DYNAMIC_SAMPLING_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/printer/ob_raw_expr_printer.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_opt_ds_stat_cache.h"
namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObEstSelInfo;
class ObRawExprPrinter;
class ObExecContext;
class OptTableMeta;
}  // end of namespace sql
namespace common {
class ObServerConfig;
class ObMySQLProxy;

struct ObDSFailTabInfo
{
  ObDSFailTabInfo () : table_id_(OB_INVALID_ID), part_ids_() {}
  uint64_t table_id_;
  ObSEArray<int64_t, 1, common::ModulePageAllocator, true> part_ids_;
  TO_STRING_KV(K(table_id_),
               K(part_ids_));
};

struct ObDSTableParam
{
  ObDSTableParam () :
    tenant_id_(0),
    table_id_(OB_INVALID_ID),
    db_name_(),
    table_name_(),
    alias_name_(),
    is_virtual_table_(false),
    ds_level_(ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING),
    sample_block_cnt_(0),
    max_ds_timeout_(0),
    degree_(1),
    need_specify_partition_(false),
    partition_infos_()
 {}

  bool is_valid() const { return tenant_id_ != 0 &&
                                 table_id_ != OB_INVALID_ID &&
                                 ds_level_ != ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING &&
                                 max_ds_timeout_ > 0; }
  uint64_t tenant_id_;
  uint64_t table_id_;
  ObString db_name_;
  ObString table_name_;
  ObString alias_name_;
  bool is_virtual_table_;
  int64_t ds_level_;
  int64_t sample_block_cnt_;
  int64_t max_ds_timeout_;
  int64_t degree_;
  bool need_specify_partition_;
  ObSEArray<PartInfo, 4, common::ModulePageAllocator, true> partition_infos_;

  TO_STRING_KV(K(tenant_id_),
               K(table_id_),
               K(db_name_),
               K(table_name_),
               K(alias_name_),
               K(is_virtual_table_),
               K(ds_level_),
               K(max_ds_timeout_),
               K(degree_),
               K(sample_block_cnt_),
               K(need_specify_partition_),
               K(partition_infos_));
};

enum ObDSResultItemType
{
  OB_DS_INVALID_STAT = -1,
  OB_DS_BASIC_STAT,//basic table stat, like table rowcount、column ndv、column num null
  OB_DS_OUTPUT_STAT,
  OB_DS_FILTER_OUTPUT_STAT//match filters output
};

enum ObDSStatItemType
{
  OB_DS_INVALID_TYPE = -1,
  OB_DS_ROWCOUNT,
  OB_DS_OUTPUT_COUNT,
  OB_DS_FILTER_OUTPUT,
  OB_DS_COLUMN_NUM_DISTINCT,
  OB_DS_COLUMN_NUM_NULL
};

struct ObDSResultItem
{
  ObDSResultItem():
    type_(OB_DS_INVALID_STAT),
    index_id_(OB_INVALID_ID),
    exprs_(),
    stat_key_(),
    stat_handle_(),
    stat_(NULL)
  {}
  ObDSResultItem(ObDSResultItemType type, uint64_t index_id):
    type_(type),
    index_id_(index_id),
    exprs_(),
    stat_key_(),
    stat_handle_(),
    stat_(NULL)
  {}
  TO_STRING_KV(K(type_),
               K(index_id_),
               K(exprs_),
               K(stat_key_),
               KPC(stat_handle_.stat_),
               KPC(stat_));
  ObDSResultItemType type_;
  uint64_t index_id_;
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> exprs_;
  ObOptDSStat::Key stat_key_;
  ObOptDSStatHandle stat_handle_;
  ObOptDSStat *stat_;
};

class ObDSStatItem
{
public:
  ObDSStatItem() :
    result_item_(NULL),
    filter_string_(),
    column_expr_(NULL),
    type_(OB_DS_INVALID_TYPE)
  {}
  ObDSStatItem(ObDSResultItem *result_item,
               const ObString &filter_string,
               ObDSStatItemType type) :
    result_item_(result_item),
    filter_string_(filter_string),
    column_expr_(NULL),
    type_(type)
  {}
  ObDSStatItem(ObDSResultItem *result_item,
               const ObString &filter_string,
               const ObColumnRefRawExpr *column_expr,
               ObDSStatItemType type) :
    result_item_(result_item),
    filter_string_(filter_string),
    column_expr_(column_expr),
    type_(type)
  {}
  void reset() {
    result_item_ = NULL;
    filter_string_.reset();
    column_expr_ = NULL;
    type_ = OB_DS_INVALID_TYPE;
  }
  virtual ~ObDSStatItem() { reset(); }
  virtual bool is_needed() const { return true; }//TODO, need refine??
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos);
  virtual int decode(double sample_ratio, ObObj &obj);
  ObDSStatItemType get_type() { return type_; }
  int cast_int(const ObObj &obj, int64_t &ret_value);
  TO_STRING_KV(KPC(result_item_),
               K(filter_string_),
               KPC(column_expr_),
               K(type_));
  ObDSResultItem *result_item_;
  ObString filter_string_;
  const ObColumnRefRawExpr *column_expr_;
  ObDSStatItemType type_;
};

template <class T>
static T *copy_ds_stat_item(ObIAllocator &allocator, const T &src)
{
  T *ret = NULL;
  void *ptr = allocator.alloc(sizeof(T));
  if (NULL != ptr) {
    ret = new (ptr) T();
    *ret = src;
  }
  return ret;
}

const int64_t OB_DS_BASIC_SAMPLE_MICRO_CNT = 32;
const int64_t OB_DS_MAX_FILTER_EXPR_COUNT = 10000;
const int64_t OB_DS_MIN_QUERY_TIMEOUT = 1000;//Dynamic sampling requires a minimum timeout of 1ms.
const int64_t OB_DS_MAX_BASIC_SAMPLE_MICRO_CNT = 1000000;
//const int64_t OB_OPT_DS_ADAPTIVE_SAMPLE_MICRO_CNT = 200;
//const int64_t OB_OPT_DS_MAX_TIMES = 7;

class ObDynamicSampling
{
public:
  explicit ObDynamicSampling(ObOptimizerContext &ctx, ObIAllocator &allocator) :
    ctx_(&ctx),
    allocator_(allocator),
    db_name_(),
    table_name_(),
    alias_name_(),
    partition_list_(),
    macro_block_num_(0),
    micro_block_num_(0),
    sstable_row_count_(0),
    memtable_row_count_(0),
    sample_block_ratio_(0.0),
    seed_(0),
    sample_block_(),
    basic_hints_(),
    where_conditions_(),
    ds_stat_items_(),
    results_(),
    is_big_table_(false),
    sample_big_table_rown_cnt_(0),
    table_clause_()
  {}

  int estimate_table_rowcount(const ObDSTableParam &param,
                              ObIArray<ObDSResultItem> &ds_result_items,
                              bool &throw_ds_error);
  int add_table_info(const ObString &db_name,
                     const ObString &table_name,
                     const ObString &alias_name);
  int add_basic_hint_info(ObSqlString &basic_hint_str,
                          int64_t query_timeout,
                          int64_t degree);
  int add_block_sample_info(const double &sample_block_ratio,
                            const int64_t seed,
                            ObSqlString &sample_str);
  int add_filter_infos(const ObIArray<ObRawExpr*> &filter_exprs,
                       bool only_column_namespace,
                       ObSqlString &filter_sql_str,
                       ObString &filter_str);
  int calc_table_sample_block_ratio(const ObDSTableParam &param);
  int add_partition_info(const ObIArray<PartInfo> &partition_infos,
                         ObSqlString &partition_sql_str,
                         ObString &partition_str);
  static int print_filter_exprs(const ObSQLSessionInfo *session_info,
                                ObSchemaGetterGuard *schema_guard,
                                const ParamStore *param_store,
                                const ObIArray<ObRawExpr*> &filter_exprs,
                                bool only_column_namespace,
                                ObSqlString &expr_str);
  static inline double revise_between_0_100(double num) {
    return num < 0 ? 0 : (num > 100.0 ? 100.0 : num); }
  const ObIArray<ObDSStatItem*> &get_ds_items() const { return ds_stat_items_; }
  int64_t get_ds_item_size() const { return ds_stat_items_.count(); }
  int add_result(ObObj &obj) { return results_.push_back(obj); }
  int64_t get_micro_block_num() const { return micro_block_num_; }

template <class T>
  int add_ds_stat_item(const T &item);
private:
  int do_estimate_table_rowcount(const ObDSTableParam &param, bool &throw_ds_error);
  int get_ds_table_result_from_cache(const ObDSTableParam &param,
                                     ObOptDSStat::Key &key,
                                     ObOptDSStatHandle &ds_stat_handle,
                                     int64_t &cur_modified_dml_cnt);
  int do_estimate_rowcount(ObSQLSessionInfo *session_info, const ObSqlString &raw_sql);
  int estimte_rowcount(int64_t max_ds_timeout, int64_t degree, bool &throw_ds_error);
  int pack(ObSqlString &raw_sql_str);
  int gen_select_filed(ObSqlString &select_fields);
  int estimate_table_block_count_and_row_count(const ObDSTableParam &param);
  int get_all_tablet_id_and_object_id(const ObDSTableParam &param,
                                      ObIArray<ObTabletID> &tablet_ids,
                                      ObIArray<ObObjectID> &partition_ids);
  int decode(double sample_ratio);
  int construct_ds_stat_key(const ObDSTableParam &param,
                            ObDSResultItemType type,
                            const ObIArray<ObRawExpr*> &filter_exprs,
                            ObOptDSStat::Key &key);
  int gen_partition_str(const ObIArray<PartInfo> &partition_infos, ObSqlString &partition_str);
  int add_ds_stat_items_by_dml_info(const ObDSTableParam &param,
                                    const int64_t cur_modified_dml_cnt,
                                    const double stale_percent_threshold,
                                    ObIArray<ObDSResultItem> &ds_result_items);
  int do_add_ds_stat_item(const ObDSTableParam &param,
                          ObDSResultItem &result_item,
                          int64_t ds_column_cnt);
  int add_ds_col_stat_item(const ObDSTableParam &param,
                           ObDSResultItem &result_item,
                           int64_t ds_column_cnt);
  bool all_ds_col_stats_are_gathered(const ObDSTableParam &param,
                                     const ObIArray<ObRawExpr*> &column_exprs,
                                     const ObOptDSStat::DSColStats &ds_col_stats,
                                     int64_t &ds_column_cnt);
  int add_ds_table_stat_item(const ObIArray<ObRawExpr*> &filters,
                             ObDSStatItemType stat_item_type,
                             ObOptDSStat &ds_stat,
                             ObSqlString &filters_str);
  int64_t get_dynamic_sampling_micro_block_num(const ObDSTableParam &param);
  int get_table_dml_info(const uint64_t tenant_id,
                         const uint64_t table_id,
                         int64_t &cur_modified_dml_cnt,
                         double &stale_percent_threshold);
  int add_ds_result_cache(ObIArray<ObDSResultItem> &ds_result_items);
  int add_block_info_for_stat_items();
  int get_ds_stat_items(const ObDSTableParam &param,
                        ObIArray<ObDSResultItem> &ds_result_items);
  int prepare_and_store_session(ObSQLSessionInfo *session,
                                sql::ObSQLSessionInfo::StmtSavedValue *&session_value,
                                int64_t &nested_count,
                                bool &is_no_backslash_escapes,
                                transaction::ObTxDesc *&tx_desc);
  int restore_session(ObSQLSessionInfo *session,
                      sql::ObSQLSessionInfo::StmtSavedValue *session_value,
                      int64_t nested_count,
                      bool is_no_backslash_escapes,
                      transaction::ObTxDesc *tx_desc);
  int add_table_clause(ObSqlString &table_str);

private:
  ObOptimizerContext *ctx_;
  ObIAllocator &allocator_;
  ObString db_name_;
  ObString table_name_;
  ObString alias_name_;
  ObString partition_list_;
  int64_t macro_block_num_;
  int64_t micro_block_num_;
  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  double sample_block_ratio_;
  int64_t seed_;
  ObString sample_block_;
  ObString basic_hints_;
  ObString where_conditions_;
  ObSEArray<ObDSStatItem *, 4, common::ModulePageAllocator, true> ds_stat_items_;
  ObSEArray<ObObj, 4, common::ModulePageAllocator, true> results_;
  bool is_big_table_;
  int64_t sample_big_table_rown_cnt_;
  ObString table_clause_;
  //following members will be used for dynamic sampling join in the future
  //ObString join_type_;
  //ObString join_conditions_;
  //int64_t micro_total_count2_;
  //bool is_left_sample_;
};

class ObDynamicSamplingUtils
{
public:
  static int get_valid_dynamic_sampling_level(const ObSQLSessionInfo *session_info,
                                              const ObTableDynamicSamplingHint *table_ds_hint,
                                              const int64_t global_ds_level,
                                              int64_t &ds_level,
                                              int64_t &sample_block_cnt,
                                              bool &specify_ds);

  static int get_ds_table_param(ObOptimizerContext &ctx,
                                const ObLogPlan *log_plan,
                                const OptTableMeta *table_meta,
                                ObDSTableParam &ds_table_param,
                                bool &specify_ds);

  static int check_ds_can_use_filters(const ObIArray<ObRawExpr*> &filters,
                                      bool &no_use);

  static const ObDSResultItem *get_ds_result_item(ObDSResultItemType type,
                                                  uint64_t index_id,
                                                  const ObIArray<ObDSResultItem> &ds_result_items);

  static int64_t get_dynamic_sampling_max_timeout(ObOptimizerContext &ctx);

  static int add_failed_ds_table_list(const uint64_t table_id,
                                      const common::ObIArray<int64_t> &used_part_id,
                                      common::ObIArray<ObDSFailTabInfo> &failed_list);

  static bool is_ds_virtual_table(const int64_t table_id);

  static int get_ds_table_degree(ObOptimizerContext &ctx,
                                 const ObLogPlan *log_plan,
                                 const uint64_t table_id,
                                 const uint64_t ref_table_id,
                                 int64_t &degree);

  static bool check_is_failed_ds_table(const uint64_t table_id,
                                       const common::ObIArray<int64_t> &used_part_id,
                                       const common::ObIArray<ObDSFailTabInfo> &failed_list);

private:
  static int check_ds_can_use_filter(const ObRawExpr *filter,
                                     bool &no_use,
                                     int64_t &total_expr_cnt);

  static int get_ds_table_part_info(ObOptimizerContext &ctx,
                                    const uint64_t ref_table_id,
                                    const common::ObIArray<ObTabletID> &used_tablets,
                                    bool &need_specify_partition,
                                    ObIArray<PartInfo> &partition_infos);

}
;


// struct ObOptDSJoinParam {
//   ObOptDSJoinParam() :
//     left_table_param_(),
//     right_table_param_(),
//     join_type_(UNKNOWN_JOIN),
//     max_ds_timeout_(0),
//     join_conditions_(NULL)
//   {}

//   bool is_valid() const { return left_table_param_.is_valid() &&
//                                  right_table_param_.is_valid() &&
//                                  join_type_ != UNKNOWN_JOIN &&
//                                  max_ds_timeout_ > 0 &&
//                                  join_conditions_ != NULL; }
//   ObOptDSBaseParam left_table_param_;
//   ObOptDSBaseParam right_table_param_;
//   ObJoinType join_type_;
//   int64_t max_ds_timeout_;
//   const ObIArray<ObRawExpr*> *join_conditions_;

//   TO_STRING_KV(K(left_table_param_),
//                K(right_table_param_),
//                K(join_type_),
//                K(max_ds_timeout_),
//                KPC(join_conditions_));
// };

}  // end of namespace common
}  // end of namespace oceanbase
#endif /* _OB_DYNAMIC_SAMPLING_H_ */
