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

#define USING_LOG_PREFIX COMMON
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/utility.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "ob_dynamic_sampling.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_opt_ds_stat_cache.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_log_plan.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase {
namespace common {

template<class T>
int ObDynamicSampling::add_ds_stat_item(const T &item)
{
  int ret = OB_SUCCESS;
  ObDSStatItem *cpy = NULL;
  if (!item.is_needed()) {
    // do nothing
  } else if (OB_ISNULL(cpy = copy_ds_stat_item(allocator_, item))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to copy stat item", K(ret));
  } else if (OB_FAIL(ds_stat_items_.push_back(cpy))) {
    LOG_WARN("failed to push back stat item", K(ret));
  }
  return ret;
}

int ObDynamicSampling::estimate_table_rowcount(const ObDSTableParam &param,
                                               ObIArray<ObDSResultItem> &ds_result_items,
                                               bool &throw_ds_error)
{
  int ret = OB_SUCCESS;
  throw_ds_error = false;
  LOG_TRACE("begine to estimate table rowcount", K(param), K(ds_result_items));
  if (OB_FAIL(get_ds_stat_items(param, ds_result_items))) {
    LOG_WARN("failed to get ds stat items");
  } else if (get_ds_item_size() == 0) {
    //all ds item can get from cache.
    LOG_TRACE("succeed to get ds item from cache", K(param));
  } else if (OB_FAIL(do_estimate_table_rowcount(param, throw_ds_error))) {
    LOG_WARN("failed to do estimate table rowcount", K(ret));
  } else if (OB_FAIL(add_ds_result_cache(ds_result_items))) {
    LOG_WARN("failed to ds result cache", K(ret));
  }
  return ret;
}

int ObDynamicSampling::add_ds_result_cache(ObIArray<ObDSResultItem> &ds_result_items)
{
  int ret = OB_SUCCESS;
  int64_t logical_idx = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_result_items.count(); ++i) {
    if (OB_ISNULL(ctx_->get_opt_stat_manager()) ||
        OB_UNLIKELY(ds_result_items.at(i).stat_handle_.stat_ != NULL &&
                    ds_result_items.at(i).stat_ != NULL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(ds_result_items.at(i)));
    } else if (ds_result_items.at(i).stat_handle_.stat_ != NULL) {//stat from cache
      if (ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_BASIC_STAT) {
        logical_idx = i;
      }
    } else if (ds_result_items.at(i).stat_ != NULL) {
      ds_result_items.at(i).stat_->set_stat_expired_time(ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL);
      if (ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_OUTPUT_STAT &&
          ds_result_items.at(i).exprs_.empty()) {
        //no need add, assign the logical count hanle
        if (OB_UNLIKELY(logical_idx < 0 || logical_idx >= ds_result_items.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(logical_idx), K(ds_result_items));
        } else {
          ds_result_items.at(i).stat_handle_ =  ds_result_items.at(logical_idx).stat_handle_;
          ds_result_items.at(i).stat_ = NULL;
        }
      } else if (OB_FAIL(ctx_->get_opt_stat_manager()->add_ds_stat_cache(ds_result_items.at(i).stat_key_,
                                                                         *ds_result_items.at(i).stat_,
                                                                         ds_result_items.at(i).stat_handle_))) {
        LOG_WARN("failed to add ds stat cache", K(ret));
      } else {
        ds_result_items.at(i).stat_ = NULL;//reset and the memory will free togther after ds.
        if (ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_BASIC_STAT) {
          logical_idx = i;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(ds_result_items.at(i)));
    }
  }
  return ret;
}

int ObDynamicSampling::get_ds_stat_items(const ObDSTableParam &param,
                                         ObIArray<ObDSResultItem> &ds_result_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_->get_opt_stat_manager()) ||
      OB_ISNULL(ctx_->get_session_info()) ||
      OB_ISNULL(ctx_->get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_->get_opt_stat_manager()),
                                    K(ctx_->get_session_info()), K(ctx_->get_exec_ctx()));
  } else {
    int64_t ds_column_cnt = 0;
    bool need_dml_info = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ds_result_items.count(); ++i) {
      ObOptDSStat::Key &key = ds_result_items.at(i).stat_key_;
      ObOptDSStatHandle &handle = ds_result_items.at(i).stat_handle_;
      if (OB_FAIL(construct_ds_stat_key(param, ds_result_items.at(i).type_,
                                        ds_result_items.at(i).exprs_, key))) {
        LOG_WARN("failed to construct ds stat key", K(ret));
      } else if (OB_FAIL(ctx_->get_opt_stat_manager()->get_ds_stat(key, handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get ds stat failed", K(ret), K(key), K(param));
        } else {
          ret = OB_SUCCESS;
          need_dml_info |= true;
        }
      } else if (OB_ISNULL(handle.stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(handle.stat_));
      } else if (!all_ds_col_stats_are_gathered(param,
                                                ds_result_items.at(i).exprs_,
                                                handle.stat_->get_ds_col_stats(),
                                                ds_column_cnt)) {
        need_dml_info |= true;
      } else if (handle.stat_->is_arrived_expired_time()) {
        need_dml_info |= true;
      } else {
        need_dml_info |= param.degree_ > handle.stat_->get_ds_degree();
      }
    }
    if (OB_SUCC(ret)) {
      if (need_dml_info) {
        int64_t cur_modified_dml_cnt = 0;
        double stale_percent_threshold = OPT_DEFAULT_STALE_PERCENT;
        if (OB_FAIL(get_table_dml_info(param.tenant_id_, param.table_id_,
                                       cur_modified_dml_cnt, stale_percent_threshold))) {
          LOG_WARN("failed to get table dml info", K(ret));
        } else if (OB_FAIL(add_ds_stat_items_by_dml_info(param,
                                                         cur_modified_dml_cnt,
                                                         stale_percent_threshold,
                                                         ds_result_items))) {
          LOG_WARN("failed to try add ds stat item by dml info", K(ret));
        } else {/*do nothing*/}
      } else {
        OPT_TRACE("succeed to get ds table result from cache");
      }
    }
  }
  return ret;
}

int ObDynamicSampling::add_ds_stat_items_by_dml_info(const ObDSTableParam &param,
                                                     const int64_t cur_modified_dml_cnt,
                                                     const double stale_percent_threshold,
                                                     ObIArray<ObDSResultItem> &ds_result_items)
{
  int ret = OB_SUCCESS;
  int64_t ds_column_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_result_items.count(); ++i) {
    bool need_add = ds_result_items.at(i).stat_handle_.stat_ == NULL;
    bool need_process_col = ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_BASIC_STAT;
    if (!need_add) {
      int64_t origin_modified_count = ds_result_items.at(i).stat_handle_.stat_->get_dml_cnt();
      int64_t inc_modified_cnt = cur_modified_dml_cnt - origin_modified_count;
      double inc_ratio = origin_modified_count == 0 ? 0 : inc_modified_cnt * 1.0 / origin_modified_count;
      bool use_col_stat_cache = false;
      if (inc_ratio <= stale_percent_threshold && need_process_col) {
        use_col_stat_cache = all_ds_col_stats_are_gathered(param,
                                                           ds_result_items.at(i).exprs_,
                                                           ds_result_items.at(i).stat_handle_.stat_->get_ds_col_stats(),
                                                           ds_column_cnt);
        need_process_col = false;
      }
      if (inc_ratio <= stale_percent_threshold &&
          use_col_stat_cache &&
          param.degree_ <= ds_result_items.at(i).stat_handle_.stat_->get_ds_degree()) {
        const_cast<ObOptDSStat*>(ds_result_items.at(i).stat_handle_.stat_)->set_stat_expired_time(
                         ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL);
      } else if (inc_ratio <= stale_percent_threshold && ds_result_items.at(i).type_ == ObDSResultItemType::OB_DS_BASIC_STAT &&
                 OB_FAIL(ds_result_items.at(i).stat_handle_.stat_->deep_copy(allocator_, ds_result_items.at(i).stat_))) {
        LOG_WARN("failed to deep copy", K(ret));
      } else {
        ds_result_items.at(i).stat_handle_.reset();
        need_add = true;
      }
    }
    if (OB_SUCC(ret) && need_process_col) {
      for (int64_t j = 0; j < ds_result_items.at(i).exprs_.count(); ++j) {
        if (ds_result_items.at(i).exprs_.at(j) != NULL &&
            ds_result_items.at(i).exprs_.at(j)->is_column_ref_expr() &&
            ObColumnStatParam::is_valid_opt_col_type(ds_result_items.at(i).exprs_.at(j)->get_data_type())) {
          ++ ds_column_cnt;
        }
      }
    }
    if (OB_SUCC(ret) && need_add) {
      if (ds_result_items.at(i).stat_ == NULL) {//need allocate new one
        char *buf = NULL;
        if (OB_ISNULL(buf = static_cast<char*>(allocator_.alloc(sizeof(ObOptDSStat))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          ds_result_items.at(i).stat_ = new (buf) ObOptDSStat();
          ds_result_items.at(i).stat_->init(ds_result_items.at(i).stat_key_);
          ds_result_items.at(i).stat_->set_dml_cnt(cur_modified_dml_cnt);
          ds_result_items.at(i).stat_->set_ds_degree(param.degree_);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(do_add_ds_stat_item(param, ds_result_items.at(i), ds_column_cnt))) {
          LOG_WARN("failed to do add ds stat item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDynamicSampling::do_add_ds_stat_item(const ObDSTableParam &param,
                                           ObDSResultItem &result_item,
                                           int64_t ds_column_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString filters_str;
  switch (result_item.type_) {
    case ObDSResultItemType::OB_DS_BASIC_STAT: {
      if (OB_ISNULL(result_item.stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if ((result_item.stat_->get_rowcount() == 0 ||
                  param.degree_ > result_item.stat_->get_ds_degree()) &&
                 OB_FAIL(add_ds_stat_item(ObDSStatItem(&result_item,
                                                       filters_str.string(),
                                                       ObDSStatItemType::OB_DS_ROWCOUNT)))) {
        LOG_WARN("failed to add ds stat item", K(ret));
      } else if (OB_FAIL(add_ds_col_stat_item(param, result_item, ds_column_cnt))) {
        LOG_WARN("failed to add ds col stat item", K(ret));
      } else if (param.degree_ > result_item.stat_->get_ds_degree()) {
        result_item.stat_->set_ds_degree(param.degree_);
      }
      break;
    }
    case ObDSResultItemType::OB_DS_OUTPUT_STAT:
    case ObDSResultItemType::OB_DS_FILTER_OUTPUT_STAT: {
      ObString tmp_str;
      if (OB_ISNULL(ctx_->get_sql_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(print_filter_exprs(ctx_->get_session_info(),
                                            ctx_->get_sql_schema_guard()->get_schema_guard(),
                                            ctx_->get_params(),
                                            result_item.exprs_,
                                            true,
                                            filters_str))) {
        LOG_WARN("failed to print filter exprs", K(ret));
      } else if (OB_FAIL(ob_write_string(allocator_, filters_str.string(), tmp_str))) {
        LOG_WARN("failed to write string", K(ret), K(tmp_str));
      } else if (OB_FAIL(add_ds_stat_item(ObDSStatItem(&result_item,
                                                       tmp_str,
                                                       result_item.type_ == OB_DS_OUTPUT_STAT ? ObDSStatItemType::OB_DS_OUTPUT_COUNT :
                                                       ObDSStatItemType::OB_DS_FILTER_OUTPUT)))) {
        LOG_WARN("failed to add ds stat item", K(ret));
      } else {/*do nothing*/}
      break;
    }
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(result_item.type_));
    break;
  }
  return ret;
}

int ObDynamicSampling::add_ds_col_stat_item(const ObDSTableParam &param,
                                            ObDSResultItem &result_item,
                                            int64_t ds_column_cnt)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  ObArrayWrap<ObOptDSColStat> tmp_col_stats;
  if (ds_column_cnt == 0) {
    //do nothing
  } else if (OB_ISNULL(result_item.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(tmp_col_stats.allocate_array(allocator_, ds_column_cnt))) {
    LOG_WARN("failed to prepare allocate count", K(ret));
  } else {
    int64_t idx = 0;
    //add ds column item
    for (int64_t i = 0; i < result_item.exprs_.count(); ++i) {
      if (OB_ISNULL(result_item.exprs_.at(i)) ||
          OB_UNLIKELY(!result_item.exprs_.at(i)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(result_item.exprs_));
      } else {
        const ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(result_item.exprs_.at(i));
        bool found_it = false;
        for (int64_t j = 0; !found_it && j < result_item.stat_->get_ds_col_stats().count(); ++j) {
          if (col_expr->get_column_id() == result_item.stat_->get_ds_col_stats().at(j).column_id_ &&
              param.degree_ <= result_item.stat_->get_ds_col_stats().at(j).degree_) {
            found_it = true;
          }
        }
        if (!found_it) {
          if (!ObColumnStatParam::is_valid_opt_col_type(col_expr->get_data_type())) {
            //do nothing, only ds fulfill with column stats type.
          } else if (OB_FAIL(add_ds_stat_item(ObDSStatItem(&result_item,
                                                           tmp_str,
                                                           col_expr,
                                                           ObDSStatItemType::OB_DS_COLUMN_NUM_DISTINCT)))) {
            LOG_WARN("failed to add num distinct stat item", K(ret));
          } else if (OB_FAIL(add_ds_stat_item(ObDSStatItem(&result_item,
                                                           tmp_str,
                                                           col_expr,
                                                           ObDSStatItemType::OB_DS_COLUMN_NUM_NULL)))) {
            LOG_WARN("failed to add num null stat item", K(ret));
          } else if (OB_UNLIKELY(idx >= tmp_col_stats.count())) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WARN("get unexpected error", K(ret), K(idx), K(result_item), K(ds_column_cnt));
          } else {
            tmp_col_stats.at(idx).column_id_ = col_expr->get_column_id();
            tmp_col_stats.at(idx).degree_ = param.degree_;
            ++ idx;
          }
        }
      }
    }
    //add no ds column item
    for (int64_t i = 0; OB_SUCC(ret) && i < result_item.stat_->get_ds_col_stats().count(); ++i) {
      bool found_it = false;
      for (int64_t j = 0; !found_it && j < result_item.exprs_.count(); ++j) {
        const ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(result_item.exprs_.at(j));
        if (result_item.stat_->get_ds_col_stats().at(i).column_id_ == col_expr->get_column_id()) {
          found_it = true;
          if (param.degree_ <= result_item.stat_->get_ds_col_stats().at(i).degree_) {
            if (OB_UNLIKELY(idx >= tmp_col_stats.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(ret), K(idx), K(result_item), K(ds_column_cnt));
            } else {
              tmp_col_stats.at(idx++) = result_item.stat_->get_ds_col_stats().at(i);
            }
          }
        }
      }
      if (OB_SUCC(ret) && !found_it) {
        if (OB_UNLIKELY(idx >= tmp_col_stats.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(idx), K(result_item), K(ds_column_cnt));
        } else {
          tmp_col_stats.at(idx++) = result_item.stat_->get_ds_col_stats().at(i);
        }
      }
    }
    //assign new col stats
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(idx != ds_column_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(idx), K(ds_column_cnt), K(param), K(result_item));
      } else {
        result_item.stat_->get_ds_col_stats().assign(tmp_col_stats);
      }
    }
  }
  return ret;
}

int ObDynamicSampling::get_table_dml_info(const uint64_t tenant_id,
                                          const uint64_t table_id,
                                          int64_t &cur_modified_dml_cnt,
                                          double &stale_percent_threshold)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_->get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_->get_exec_ctx()));
  } else if (OB_FAIL(ObBasicStatsEstimator::estimate_modified_count(*ctx_->get_exec_ctx(),
                                                                    tenant_id,
                                                                    table_id,
                                                                    cur_modified_dml_cnt,
                                                                    false))) {
    LOG_WARN("failed to estimate modified count", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::get_table_stale_percent_threshold(*ctx_->get_exec_ctx(),
                                                                        tenant_id,
                                                                        table_id,
                                                                        stale_percent_threshold))) {
    LOG_WARN("failed to get table stale percent threshold", K(ret));
  } else {
    LOG_TRACE("succeed to get table dml info", K(tenant_id), K(table_id), K(cur_modified_dml_cnt),
                                               K(stale_percent_threshold));
  }
  return ret;
}

int ObDynamicSampling::construct_ds_stat_key(const ObDSTableParam &param,
                                             ObDSResultItemType type,
                                             const ObIArray<ObRawExpr*> &filter_exprs,
                                             ObOptDSStat::Key &key)
{
  int ret = OB_SUCCESS;
  ObSqlString expr_str;
  ObSqlString partition_str;
  int64_t sample_micro_cnt = 0;
  ObSEArray<ObRawExpr*,1> empty_exprs;
  if (OB_ISNULL(ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(print_filter_exprs(ctx_->get_session_info(),
                                        ctx_->get_sql_schema_guard()->get_schema_guard(),
                                        NULL,
                                        type == ObDSResultItemType::OB_DS_BASIC_STAT ? empty_exprs : filter_exprs,
                                        true,
                                        expr_str))) {
    LOG_WARN("failed to print filter exprs", K(ret));
  } else if (param.need_specify_partition_ &&
             OB_FAIL(gen_partition_str(param.partition_infos_, partition_str))) {
    LOG_WARN("failed to print filter exprs", K(ret));
  } else if (OB_UNLIKELY((sample_micro_cnt = get_dynamic_sampling_micro_block_num(param)) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else {
    key.tenant_id_ = param.tenant_id_;
    key.table_id_ = param.table_id_;
    key.partition_hash_ = murmurhash64A(partition_str.ptr(), partition_str.length(), 0);
    key.ds_level_ = param.ds_level_;
    key.sample_block_ = sample_micro_cnt;
    key.expression_hash_ = murmurhash64A(expr_str.ptr(), expr_str.length(), 0);
    LOG_TRACE("succeed to construct ds stat key", K(key), K(expr_str), K(partition_str));
  }
  return ret;
}

int ObDynamicSampling::do_estimate_table_rowcount(const ObDSTableParam &param, bool &throw_ds_error)
{
  int ret = OB_SUCCESS;
  double sample_ratio = 0.0;
  ObSqlString partition_str;
  ObSqlString where_str;
  ObSqlString pre_filters_string;
  ObSqlString postfix_filters_string;
  ObSqlString all_filters_string;
  ObString tmp_str;
  ObSEArray<ObRawExpr*, 4> tmp_filters;
  throw_ds_error = false;
  LOG_TRACE("begin estimate table rowcount", K(param));
  if (OB_FAIL(add_table_info(param.db_name_,
                             param.table_name_,
                             param.alias_name_))) {
    LOG_WARN("failed to add table info", K(ret));
  } else if (param.need_specify_partition_ &&
             OB_FAIL(add_partition_info(param.partition_infos_,
                                        partition_str,
                                        partition_list_))) {
    LOG_WARN("failed to add partition info", K(ret));
  } else if (OB_FAIL(calc_table_sample_block_ratio(param))) {
    LOG_WARN("failed to calc sample block ratio", K(ret));
  } else if (OB_FAIL(add_block_info_for_stat_items())) {
    LOG_WARN("failed to add block info for stat items", K(ret));
  } else if (OB_FAIL(estimte_rowcount(param.max_ds_timeout_, param.degree_, throw_ds_error))) {
    LOG_WARN("failed to estimate rowcount", K(ret));
  }
  return ret;
}

int ObDynamicSampling::estimte_rowcount(int64_t max_ds_timeout,
                                        int64_t degree,
                                        bool &throw_ds_error)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql_str;
  ObSqlString sample_str;
  ObSqlString basic_hint_str;
  ObSqlString table_str;
  bool is_no_backslash_escapes = false;
  int64_t nested_count = -1;
  sql::ObSQLSessionInfo::StmtSavedValue *session_value = NULL;
  int64_t start_time = ObTimeUtility::current_time();
  ObSQLSessionInfo *session_info = ctx_->get_session_info();
  bool need_restore_session = false;
  transaction::ObTxDesc *tx_desc = NULL;
  if (!is_big_table_ && OB_FAIL(add_block_sample_info(sample_block_ratio_, seed_, sample_str))) {
    LOG_WARN("failed to add block sample info", K(ret));
  } else if (OB_FAIL(add_basic_hint_info(basic_hint_str, max_ds_timeout, is_big_table_ ? 1 : degree))) {
    LOG_WARN("failed to add basic hint info", K(ret));
  } else if (OB_FAIL(add_table_clause(table_str))) {
    LOG_WARN("failed to add table clause", K(ret));
  } else if (OB_FAIL(pack(raw_sql_str))) {
    LOG_WARN("failed to pack dynamic sampling", K(ret));
  } else if (OB_FAIL(prepare_and_store_session(session_info, session_value,
                                               nested_count, is_no_backslash_escapes, tx_desc))) {
    throw_ds_error = true;//here we must throw error, because the session may be unavailable.
    LOG_WARN("failed to prepare and store session", K(ret));
  } else {
    need_restore_session = true;
  }
  if (OB_SUCC(ret)) {
    //do not trace dynamic sample sql execute
    STOP_OPT_TRACE;
    if (OB_FAIL(do_estimate_rowcount(session_info, raw_sql_str))) {
      LOG_WARN("failed to do estimate rowcount", K(ret));
    }
    RESUME_OPT_TRACE;
  }
  //regardless of whether the above behavior is successful or not, we need to restore session.
  if (need_restore_session) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = restore_session(session_info, session_value,
                                                 nested_count, is_no_backslash_escapes, tx_desc))) {
      throw_ds_error = true;//here we must throw error, because the session may be unavailable.
      ret = COVER_SUCC(tmp_ret);
      LOG_WARN("failed to restore session", K(tmp_ret));
    }
    if (OB_NOT_NULL(session_value)) {
      session_value->reset();
    }
  }
  LOG_TRACE("go to dynamic sample one time", K(sample_block_ratio_), K(ret),
            K(raw_sql_str), K(max_ds_timeout), K(start_time), K(ObTimeUtility::current_time() - start_time));
  OPT_TRACE("dynamic sample cost time:", ObTimeUtility::current_time()-start_time,
            "us, max sample time:", max_ds_timeout, "us");
  return ret;
}

int ObDSStatItem::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObSqlString expr_str;
  switch (type_) {
      case OB_DS_ROWCOUNT:
      case OB_DS_OUTPUT_COUNT:
      case OB_DS_FILTER_OUTPUT: {
      if (filter_string_.empty()) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "COUNT(*)"))) {
          LOG_WARN("failed to print buf", K(ret));
        } else {/*do nothing*/}
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "SUM(CASE WHEN %.*s THEN 1 ELSE 0 END)",
                                         filter_string_.length(),
                                         filter_string_.ptr()))) {
        LOG_WARN("failed to print buf", K(ret));
      }
      break;
    }
    case OB_DS_COLUMN_NUM_DISTINCT: {
      if (OB_ISNULL(column_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(column_expr_));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                         lib::is_oracle_mode() ? "APPROX_COUNT_DISTINCT(\"%.*s\")" :
                                                                 "APPROX_COUNT_DISTINCT(`%.*s`)",
                                         column_expr_->get_column_name().length(),
                                         column_expr_->get_column_name().ptr()))) {
        LOG_WARN("failed to print buf", K(ret));
      }
      break;
    }
    case OB_DS_COLUMN_NUM_NULL: {
      if (OB_ISNULL(column_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(column_expr_));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                         lib::is_oracle_mode() ? "SUM(CASE WHEN \"%.*s\" IS NULL THEN 1 ELSE 0 END)" :
                                                                 "SUM(CASE WHEN `%.*s` IS NULL THEN 1 ELSE 0 END)",
                                          column_expr_->get_column_name().length(),
                                          column_expr_->get_column_name().ptr()))) {
        LOG_WARN("failed to print buf", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(type_), K(ret));
      break;
    }
  }
  return ret;
}

//select /*+hint_string*/count(*) as logical_rowcount,
//       sum(case when post_filter_str then 1 else 0 end) as indexback_rowcount,
//       sum(case when table_filter_str then 1 else 0 end) as output_rowcount,
//  from table_name sample block(sample_ratio) where prefix_filter_str.
//
int ObDynamicSampling::pack(ObSqlString &raw_sql_str)
{
  int ret = OB_SUCCESS;
  ObSqlString select_fields;
  if (OB_FAIL(gen_select_filed(select_fields))) {
    LOG_WARN("failed to generate select filed", K(ret));
  } else if (OB_FAIL(raw_sql_str.append_fmt(lib::is_oracle_mode() ?
                                            "SELECT %.*s %.*s FROM %.*s %s %.*s" :
                                            "SELECT %.*s %.*s FROM %.*s %s %.*s" ,
                                            basic_hints_.length(),
                                            basic_hints_.ptr(),
                                            static_cast<int32_t>(select_fields.length()),
                                            select_fields.ptr(),
                                            table_clause_.length(),
                                            table_clause_.ptr(),
                                            where_conditions_.empty() ? " " : "WHERE",
                                            where_conditions_.length(),
                                            where_conditions_.ptr()))) {
    LOG_WARN("failed to build query sql stmt", K(ret));
  } else {
    LOG_TRACE("OptStat: dynamic sampling query sql", K(raw_sql_str));
    OPT_TRACE("dynamic sampling query sql:", raw_sql_str.string());
  }
  return ret;
}

int ObDynamicSampling::gen_select_filed(ObSqlString &select_fields)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_stat_items_.count(); ++i) {
    int64_t pos = 0;
    SMART_VAR(char[OB_MAX_SQL_LENGTH], buf) {
      if (i != 0 && OB_FAIL(select_fields.append(", "))) {
        LOG_WARN("failed to append delimiter", K(ret));
      } else if (OB_FAIL(ds_stat_items_.at(i)->gen_expr(buf, OB_MAX_SQL_LENGTH, pos))) {
        LOG_WARN("failed to gen select expr", K(ret));
      } else if (OB_FAIL(select_fields.append(buf, pos))) {
        LOG_WARN("failed to append stat item expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDynamicSampling::add_table_info(const ObString &db_name,
                                      const ObString &table_name,
                                      const ObString &alias_name)
{
  int ret = OB_SUCCESS;
  db_name_ = db_name;
  table_name_ = table_name;
  alias_name_ = alias_name;
  return ret;
}

int ObDynamicSampling::add_basic_hint_info(ObSqlString &basic_hint_str,
                                           int64_t query_timeout,
                                           int64_t degree)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(basic_hint_str.append("/*+ NO_REWRITE"))) {//hint begin
    LOG_WARN("failed to append", K(ret));
  //add suite degree
  } else if (degree <= 1 && OB_FAIL(basic_hint_str.append(" NO_PARALLEL "))) {
    LOG_WARN("failed to append", K(ret));
  } else if (degree > 1 && OB_FAIL(basic_hint_str.append_fmt(" PARALLEL(%ld) ", degree))) {
    LOG_WARN("failed to append", K(ret));
  //Dynamic Sampling SQL shouldn't dynamic sampling
  } else if (OB_FAIL(basic_hint_str.append(" DYNAMIC_SAMPLING(0) "))) {
    LOG_WARN("failed to append", K(ret));
  //add query timeout control Dynamic Sampling SQL execute time.
  } else if (OB_FAIL(basic_hint_str.append_fmt(" QUERY_TIMEOUT(%ld) ", query_timeout))) {
    LOG_WARN("failed to append", K(ret));
  //use defualt stat
  } else if (OB_FAIL(basic_hint_str.append(" OPT_PARAM(\'USE_DEFAULT_OPT_STAT\',\'TRUE\') "))) {
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(basic_hint_str.append("*/"))) {//hint end
    LOG_WARN("failed to append", K(ret));
  } else {
    basic_hints_ = basic_hint_str.string();
  }
  return ret;
}

int ObDynamicSampling::add_partition_info(const ObIArray<PartInfo> &partition_infos,
                                          ObSqlString &partition_sql_str,
                                          ObString &partition_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_sql_str.append("partition("))) {//partition begin
    LOG_WARN("failed to append", K(ret));
  } else if (OB_FAIL(gen_partition_str(partition_infos, partition_sql_str))) {
    LOG_WARN("failed to gen partition str", K(ret));
  } else if (OB_FAIL(partition_sql_str.append(")"))) {//partition end
    LOG_WARN("failed to append", K(ret));
  } else {
    partition_str = partition_sql_str.string();
  }
  return ret;
}

int ObDynamicSampling::print_filter_exprs(const ObSQLSessionInfo *session_info,
                                          ObSchemaGetterGuard *schema_guard,
                                          const ParamStore *param_store,
                                          const ObIArray<ObRawExpr*> &filter_exprs,
                                          bool only_column_namespace,
                                          ObSqlString &expr_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("ObOptDS");
  ObRawExprFactory expr_factory(tmp_allocator);
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    ObRawExpr *new_expr = NULL;
    if (OB_FAIL(ObRawExprCopier::copy_expr(expr_factory, filter_exprs.at(i), new_expr))) {
      LOG_WARN("failed to copy raw expr", K(ret));
    } else if (OB_ISNULL(new_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(new_expr));
    } else {
      HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf) {
        MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
        int64_t pos = 0;
        ObRawExprPrinter expr_printer(expr_str_buf,
                                      OB_MAX_DEFAULT_VALUE_LENGTH, &pos,
                                      schema_guard,
                                      TZ_INFO(session_info),
                                      param_store);
        if (OB_FAIL(expr_printer.do_print(new_expr, T_WHERE_SCOPE, only_column_namespace))) {
          LOG_WARN("failed to print expr", KPC(new_expr), K(ret));
        } else if (OB_FAIL(expr_str.append_fmt("%s%.*s", i == 0 ? " " : " and ",
                                               static_cast<int32_t>(pos), expr_str_buf))) {
          LOG_WARN("failed to append fmt", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  LOG_TRACE("succeed to print filter exprs", K(filter_exprs), K(expr_str));
  return ret;
}

int ObDynamicSampling::add_filter_infos(const ObIArray<ObRawExpr*> &filter_exprs,
                                        bool only_column_namespace,
                                        ObSqlString &filter_sql_str,
                                        ObString &filter_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_->get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(filter_exprs), K(ret));
  } else if (OB_FAIL(print_filter_exprs(ctx_->get_session_info(),
                                        ctx_->get_sql_schema_guard()->get_schema_guard(),
                                        ctx_->get_params(),
                                        filter_exprs,
                                        only_column_namespace,
                                        filter_sql_str))) {
    LOG_WARN("failed to print filter exprs", K(ret));
  } else {
    filter_str = filter_sql_str.string();
  }
  return ret;
}

/*
 * virtual table choose full table scan
*/
int ObDynamicSampling::calc_table_sample_block_ratio(const ObDSTableParam &param)
{
  int ret = OB_SUCCESS;
  int64_t sample_micro_cnt = param.sample_block_cnt_;
  const int64_t MAX_FULL_SCAN_ROW_COUNT = 100000;
  int64_t macro_threshold = 200;
  if (param.is_virtual_table_) {
    sample_block_ratio_ = 100.0;
    seed_ = param.degree_ > 1 ? 0 : 1;
  } else if (OB_UNLIKELY((sample_micro_cnt = get_dynamic_sampling_micro_block_num(param)) < 1 ||
                         (micro_block_num_ > 0 && memtable_row_count_ + sstable_row_count_ <= 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (OB_FAIL(estimate_table_block_count_and_row_count(param))) {
    LOG_WARN("failed to estimate table block count and row count", K(ret));
  } else {
    int64_t max_allowed_multiple = sample_micro_cnt > OB_DS_BASIC_SAMPLE_MICRO_CNT ? sample_micro_cnt / OB_DS_BASIC_SAMPLE_MICRO_CNT : 1;
    if (micro_block_num_ > OB_DS_MAX_BASIC_SAMPLE_MICRO_CNT * max_allowed_multiple &&
        MAGIC_MAX_AUTO_SAMPLE_SIZE * max_allowed_multiple < memtable_row_count_ + sstable_row_count_) {
      is_big_table_ = true;
      sample_big_table_rown_cnt_ = MAGIC_MAX_AUTO_SAMPLE_SIZE * max_allowed_multiple;
      sample_block_ratio_ = 100.0 * sample_big_table_rown_cnt_ / (memtable_row_count_ + sstable_row_count_);
      sample_block_ratio_ = sample_block_ratio_ < 100.0 ? sample_block_ratio_ : 100.0;
    } else {
      //1.retire to memtable sample
      if (sstable_row_count_ < memtable_row_count_) {
        double sample_row_cnt = MAGIC_MAX_AUTO_SAMPLE_SIZE * (1.0 * sample_micro_cnt / OB_DS_BASIC_SAMPLE_MICRO_CNT);
        if (memtable_row_count_ < sample_row_cnt) {
          sample_block_ratio_ = 100.0;
        } else {
          sample_block_ratio_ = 100.0 * sample_row_cnt / (memtable_row_count_ + sstable_row_count_);
        }
      } else {
      //2.use the block sample
        if (sample_micro_cnt >= micro_block_num_) {
          sample_block_ratio_ = 100.0;
        } else {
          sample_block_ratio_ = 100.0 * sample_micro_cnt / micro_block_num_;
        }
      }
      //3.try adjust sample block ratio according to the degree
      if (param.degree_ > 1 && sample_block_ratio_ < 100.0) {//adjust sample ratio according to the degree.
        sample_block_ratio_ = sample_block_ratio_ * param.degree_;
        sample_block_ratio_ = sample_block_ratio_ < 100.0 ? sample_block_ratio_ : 100.0;
      }
      sample_block_ratio_ = sample_block_ratio_ < 0.000001 ? 0.000001 : sample_block_ratio_;
      //4.adjust the seed.
      seed_ = (param.degree_ > 1 || param.partition_infos_.count() > 1) ? 0 : 1;
    }
  }
  LOG_TRACE("succeed to calc table sample block ratio", K(param), K(seed_), K(sample_micro_cnt), K(is_big_table_),
                                                        K(sample_block_ratio_), K(micro_block_num_),
                                                        K(sstable_row_count_), K(memtable_row_count_));
  return ret;
}

int ObDynamicSampling::add_block_info_for_stat_items()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_stat_items_.count(); ++i) {
    if (OB_ISNULL(ds_stat_items_.at(i)) || OB_ISNULL(ds_stat_items_.at(i)->result_item_) ||
        OB_ISNULL(ds_stat_items_.at(i)->result_item_->stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), KPC(ds_stat_items_.at(i)));
    } else {
      ds_stat_items_.at(i)->result_item_->stat_->set_macro_block_num(macro_block_num_);
      ds_stat_items_.at(i)->result_item_->stat_->set_micro_block_num(micro_block_num_);
      ds_stat_items_.at(i)->result_item_->stat_->set_sample_block_ratio(sample_block_ratio_);
    }
  }
  return ret;
}

int64_t ObDynamicSampling::get_dynamic_sampling_micro_block_num(const ObDSTableParam &param)
{
  int64_t sample_micro_cnt = 0;
  if (param.sample_block_cnt_ > 0) {
    sample_micro_cnt = param.sample_block_cnt_;
  } else {
    sample_micro_cnt = OB_DS_BASIC_SAMPLE_MICRO_CNT;
  }
  return sample_micro_cnt;
}

int ObDynamicSampling::estimate_table_block_count_and_row_count(const ObDSTableParam &param)
{
  int ret = OB_SUCCESS;
  macro_block_num_ = 0;
  micro_block_num_ = 0;
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<ObObjectID, 4> partition_ids;
  ObSEArray<EstimateBlockRes, 4> estimate_result;
  ObArray<uint64_t> column_group_ids;
  if (OB_ISNULL(ctx_->get_exec_ctx()) || OB_ISNULL(ctx_->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_->get_exec_ctx()), K(ctx_->get_session_info()));
  } else if (OB_FAIL(get_all_tablet_id_and_object_id(param, tablet_ids, partition_ids))) {
    LOG_WARN("failed to get all tablet id and object id", K(ret));
  } else if (OB_FAIL(ObBasicStatsEstimator::do_estimate_block_count_and_row_count(*ctx_->get_exec_ctx(),
                                                                                  ctx_->get_session_info()->get_effective_tenant_id(),
                                                                                  param.table_id_,
                                                                                  tablet_ids,
                                                                                  partition_ids,
                                                                                  column_group_ids,
                                                                                  estimate_result))) {
    LOG_WARN("failed to do estimate block count and row count", K(ret));
  } else {
    for (int64_t i = 0; i < estimate_result.count(); ++i) {
      macro_block_num_ += estimate_result.at(i).macro_block_count_;
      micro_block_num_ += estimate_result.at(i).micro_block_count_;
      sstable_row_count_ += estimate_result.at(i).sstable_row_count_;
      memtable_row_count_ += estimate_result.at(i).memtable_row_count_;
    }
    LOG_TRACE("Succeed to estimate micro block count", K(micro_block_num_), K(macro_block_num_),
                                                       K(tablet_ids), K(partition_ids),
                                                       K(estimate_result), K(param),
                                                       K(sstable_row_count_), K(memtable_row_count_));
  }
  return ret;
}

int ObDynamicSampling::get_all_tablet_id_and_object_id(const ObDSTableParam &param,
                                                       ObIArray<ObTabletID> &tablet_ids,
                                                       ObIArray<ObObjectID> &partition_ids)
{
  int ret = OB_SUCCESS;
  if (param.partition_infos_.empty()) {
    ObDASTabletMapper tablet_mapper;
    ObSEArray<ObTabletID, 1> tmp_tablet_ids;
    ObSEArray<ObObjectID, 1> tmp_part_ids;
    if (OB_ISNULL(ctx_->get_exec_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(ctx_->get_exec_ctx()));
    } else if (OB_FAIL(ctx_->get_exec_ctx()->get_das_ctx().get_das_tablet_mapper(param.table_id_,
                                                                                tablet_mapper))) {
      LOG_WARN("fail to get das tablet mapper", K(ret));
    } else if (OB_FAIL(tablet_mapper.get_non_partition_tablet_id(tmp_tablet_ids, tmp_part_ids))) {
      LOG_WARN("failed to get non partition tablet id", K(ret));
    } else if (OB_UNLIKELY(tmp_part_ids.count() != 1 || tmp_tablet_ids.count() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(tmp_part_ids), K(tmp_tablet_ids));
    } else if (OB_FAIL(tablet_ids.push_back(tmp_tablet_ids.at(0)))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(partition_ids.push_back(tmp_part_ids.at(0)))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.partition_infos_.count(); ++i) {
      if (OB_FAIL(tablet_ids.push_back(param.partition_infos_.at(i).tablet_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(partition_ids.push_back(static_cast<ObObjectID>(param.partition_infos_.at(i).part_id_)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObDynamicSampling::add_block_sample_info(const double &sample_block_ratio,
                                             const int64_t seed,
                                             ObSqlString &sample_str)
{
  int ret = OB_SUCCESS;
  ObSqlString seed_str;
  if (OB_UNLIKELY(sample_block_ratio <= 0 || sample_block_ratio > 100.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(sample_block_ratio));
  } else if (sample_block_ratio == 100.0) {
    //do nothing
    sample_block_.reset();
  } else if (seed > 0 && OB_FAIL(seed_str.append_fmt(" SEED(%ld) ", seed))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(sample_str.append_fmt(" SAMPLE BLOCK(%lf) %s ",
                                           sample_block_ratio,
                                           seed_str.empty() ? " " : seed_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    sample_block_ = sample_str.string();
  }
  return ret;
}

int ObDynamicSampling::do_estimate_rowcount(ObSQLSessionInfo *session_info,
                                            const ObSqlString &raw_sql)
{
  int ret = OB_SUCCESS;
  common::ObOracleSqlProxy oracle_proxy;
  ObCommonSqlProxy *sql_proxy = NULL;
  if (OB_ISNULL(session_info) ||
      OB_ISNULL(ctx_->get_exec_ctx()) ||
      OB_ISNULL(sql_proxy = ctx_->get_exec_ctx()->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(ctx_->get_exec_ctx()), K(sql_proxy), K(session_info));
  } else if (lib::is_oracle_mode()) {
    if (OB_FAIL(oracle_proxy.init(sql_proxy->get_pool()))) {
      LOG_WARN("failed to init oracle proxy", K(ret));
    } else {
      sql_proxy = &oracle_proxy;
    }
  }
  if (OB_SUCC(ret)) {
    observer::ObInnerSQLConnectionPool *pool =
                            static_cast<observer::ObInnerSQLConnectionPool*>(sql_proxy->get_pool());
    sqlclient::ObISQLConnection *conn = NULL;
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_UNLIKELY(raw_sql.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected empty", K(ret));
      } else if (OB_FAIL(pool->acquire(session_info, conn, lib::is_oracle_mode()))) {
        LOG_WARN("failed to acquire inner connection", K(ret));
      } else if (OB_ISNULL(conn)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("conn is null", K(ret));
      } else if (OB_FAIL(conn->execute_read(session_info->get_effective_tenant_id(),
                                            raw_sql.ptr(),
                                            proxy_result))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        int64_t result_cnt = 0;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          ++ result_cnt;
          if (OB_UNLIKELY(result_cnt > 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(result_cnt), K(raw_sql));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < get_ds_item_size(); ++i) {
            ObObj tmp;
            ObObj val;
            if (OB_FAIL(client_result->get_obj(i, tmp))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(ob_write_obj(allocator_, tmp, val))) {
              LOG_WARN("failed to write object", K(ret));
            } else if (OB_FAIL(add_result(val))) {
              LOG_WARN("failed to add result", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(decode(sample_block_ratio_ / 100.0))) {
              LOG_WARN("failed to decode results", K(ret));
            } else {
              results_.reset();
            }
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get result", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }
    if (conn != NULL && sql_proxy != NULL) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = sql_proxy->close(conn, true))) {
        LOG_WARN("close result set failed", K(ret), K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObDynamicSampling::prepare_and_store_session(ObSQLSessionInfo *session,
                                                 sql::ObSQLSessionInfo::StmtSavedValue *&session_value,
                                                 int64_t &nested_count,
                                                 bool &is_no_backslash_escapes,
                                                 transaction::ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session));
  } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(sql::ObSQLSessionInfo::StmtSavedValue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for saved session value", K(ret));
  } else {
    session_value = new(ptr)sql::ObSQLSessionInfo::StmtSavedValue();
    if (OB_FAIL(session->save_session(*session_value))) {
      LOG_WARN("failed to save session", K(ret));
    } else {
      ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
      nested_count = session->get_nested_count();
      IS_NO_BACKSLASH_ESCAPES(session->get_sql_mode(), is_no_backslash_escapes);
      session->set_sql_mode(session->get_sql_mode() & ~SMO_NO_BACKSLASH_ESCAPES);
      session->set_query_start_time(ObTimeUtility::current_time());
      session->set_inner_session();
      session->set_nested_count(-1);
      //bug:
      session->set_autocommit(session_value->inc_autocommit_);
      //ac is true, dynamic sampling select query no need tx desc.
      if (session_value->inc_autocommit_ && session->get_tx_desc() != NULL) {
        tx_desc = session->get_tx_desc();
        session->get_tx_desc() = NULL;
      }
    }
  }
  return ret;
}

int ObDynamicSampling::restore_session(ObSQLSessionInfo *session,
                                       sql::ObSQLSessionInfo::StmtSavedValue *session_value,
                                       int64_t nested_count,
                                       bool is_no_backslash_escapes,
                                       transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(session_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session), K(session_value));
  } else if (OB_FAIL(session->restore_session(*session_value))) {
    LOG_WARN("failed to restore session", K(ret));
  } else {
    ObSQLSessionInfo::LockGuard data_lock_guard(session->get_thread_data_lock());
    session->set_nested_count(nested_count);
    if (is_no_backslash_escapes) {
      session->set_sql_mode(session->get_sql_mode() | SMO_NO_BACKSLASH_ESCAPES);
    }
    if (tx_desc != NULL) {//reset origin tx desc.
      // release curr
      if (OB_NOT_NULL(session->get_tx_desc())) {
        auto txs = MTL(transaction::ObTransService*);
        if (OB_ISNULL(txs)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("can not acquire MTL TransService", KR(ret));
          session->get_tx_desc()->dump_and_print_trace();
        } else {
          txs->release_tx(*session->get_tx_desc());
        }
      }
      session->get_tx_desc() = tx_desc;
    }
  }
  return ret;
}

int ObDynamicSampling::decode(double sample_ratio)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ds_stat_items_.count() != results_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size does not match", K(ret), K(ds_stat_items_.count()), K(results_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ds_stat_items_.count(); ++i) {
    if (OB_FAIL(ds_stat_items_.at(i)->decode(sample_ratio, results_.at(i)))) {
      LOG_WARN("failed to decode statistic result", K(ret));
    }
  }
  return ret;
}

int ObDSStatItem::decode(double sample_ratio, ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t res = 0;
  if (OB_ISNULL(result_item_) || OB_ISNULL(result_item_->stat_) ||
      OB_UNLIKELY(sample_ratio <= 0.0 || sample_ratio > 1.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(result_item_->stat_), K(sample_ratio));
  } else if (OB_FAIL(cast_int(obj, res))) {
    LOG_WARN("failed to cast int", K(ret));
  } else {
    switch (type_) {
      case OB_DS_ROWCOUNT:
      case OB_DS_OUTPUT_COUNT:
      case OB_DS_FILTER_OUTPUT: {
        result_item_->stat_->set_rowcount(static_cast<int64_t>(res / sample_ratio));
        break;
      }
      case OB_DS_COLUMN_NUM_DISTINCT:
      case OB_DS_COLUMN_NUM_NULL: {
        bool found_it = false;
        ObOptDSStat::DSColStats &ds_col_stats = result_item_->stat_->get_ds_col_stats();
        for (int64_t i = 0; !found_it && i < ds_col_stats.count(); ++i) {
          if (ds_col_stats.at(i).column_id_ == column_expr_->get_column_id()) {
            found_it = true;
            if (type_ == OB_DS_COLUMN_NUM_DISTINCT) {
              int64_t total_row_cnt = result_item_->stat_->get_rowcount();
              int64_t cur_row_cnt = result_item_->stat_->get_rowcount() * sample_ratio;
              ds_col_stats.at(i).num_distinct_ = static_cast<int64_t>(ObOptSelectivity::scale_distinct(total_row_cnt,
                                                                                                       cur_row_cnt,
                                                                                                       res));
            } else {
              ds_col_stats.at(i).num_null_ = static_cast<int64_t>(res / sample_ratio);
            }
          }
        }
        if (OB_UNLIKELY(!found_it)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected type", K(ret), KPC(column_expr_), K(ds_col_stats));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected type", K(ret), K(type_));
        break;
    }
  }
  return ret;
}

int ObDSStatItem::cast_int(const ObObj &obj, int64_t &ret_value)
{
  int ret = OB_SUCCESS;
  ObObj dest_obj;
  ObArenaAllocator calc_buffer("ObOptDS");
  ObCastCtx cast_ctx(&calc_buffer, NULL, CM_NONE, ObCharset::get_system_collation());
  if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, obj, dest_obj))) {
    LOG_WARN("cast to int value failed", K(ret), K(obj));
  } else if (dest_obj.is_null()) {
    /*do nothing*/
  } else if (OB_FAIL(dest_obj.get_int(ret_value))) {
    LOG_WARN("get int value failed", K(ret), K(dest_obj), K(obj));
  }
  return ret;
}

int ObDynamicSampling::gen_partition_str(const ObIArray<PartInfo> &partition_infos,
                                         ObSqlString &partition_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(partition_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(partition_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
      char prefix = i == 0 ? ' ' : ',';
      if (OB_FAIL(partition_str.append_fmt("%c%.*s", prefix, partition_infos.at(i).part_name_.length(),
                                           partition_infos.at(i).part_name_.ptr()))) {
        LOG_WARN("failed to append fmt", K(ret));
      }
    }
  }
  return ret;
}

bool ObDynamicSampling::all_ds_col_stats_are_gathered(const ObDSTableParam &param,
                                                      const ObIArray<ObRawExpr*> &column_exprs,
                                                      const ObOptDSStat::DSColStats &ds_col_stats,
                                                      int64_t &ds_column_cnt)
{
  bool res = true;
  ds_column_cnt = ds_col_stats.count();
  for (int64_t i = 0; i < column_exprs.count(); ++i) {
    bool found_it = false;
    for (int64_t j = 0; !found_it && j < ds_col_stats.count(); ++j) {
      if (column_exprs.at(i) != NULL && column_exprs.at(i)->is_column_ref_expr() &&
          static_cast<ObColumnRefRawExpr*>(column_exprs.at(i))->get_column_id() == ds_col_stats.at(j).column_id_) {
        if (param.degree_ > ds_col_stats.at(j).degree_) {
          found_it = false;
          -- ds_column_cnt;
        } else {
          found_it = true;
        }
      }
    }
    if (!found_it && column_exprs.at(i) != NULL && column_exprs.at(i)->is_column_ref_expr() &&
        ObColumnStatParam::is_valid_opt_col_type(column_exprs.at(i)->get_data_type())) {
      ++ ds_column_cnt;
      res = false;
    }
  }
  return res;
}

int ObDynamicSampling::add_table_clause(ObSqlString &table_str)
{
  int ret = OB_SUCCESS;
  int64_t big_table_row = 100000;
  if (OB_UNLIKELY(is_big_table_ && sample_big_table_rown_cnt_ < MAGIC_MAX_AUTO_SAMPLE_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(sample_big_table_rown_cnt_), K(is_big_table_));
  } else if (is_big_table_ && OB_FAIL(table_str.append_fmt(lib::is_oracle_mode() ? "(SELECT * FROM \"%.*s\".\"%.*s\" %.*s %.*s FETCH NEXT %ld ROWS ONLY) %s%.*s%s" :
                                                                                   "(SELECT * FROM `%.*s`.`%.*s` %.*s %.*s LIMIT %ld) %s%.*s%s" ,
                                                           db_name_.length(),
                                                           db_name_.ptr(),
                                                           table_name_.length(),
                                                           table_name_.ptr(),
                                                           partition_list_.length(),
                                                           partition_list_.ptr(),
                                                           sample_block_.length(),
                                                           sample_block_.ptr(),
                                                           sample_big_table_rown_cnt_,
                                                           alias_name_.empty() ? " " : (lib::is_oracle_mode() ? "\"" : "`"),
                                                           alias_name_.length(),
                                                           alias_name_.ptr(),
                                                           alias_name_.empty() ? " " : (lib::is_oracle_mode() ? "\"" : "`")))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (!is_big_table_ && OB_FAIL(table_str.append_fmt(lib::is_oracle_mode() ? "\"%.*s\".\"%.*s\" %.*s %.*s %s%.*s%s" :
                                                                                      "`%.*s`.`%.*s` %.*s %.*s %s%.*s%s" ,
                                                            db_name_.length(),
                                                            db_name_.ptr(),
                                                            table_name_.length(),
                                                            table_name_.ptr(),
                                                            partition_list_.length(),
                                                            partition_list_.ptr(),
                                                            sample_block_.length(),
                                                            sample_block_.ptr(),
                                                            alias_name_.empty() ? " " : (lib::is_oracle_mode() ? "\"" : "`"),
                                                            alias_name_.length(),
                                                            alias_name_.ptr(),
                                                            alias_name_.empty() ? " " : (lib::is_oracle_mode() ? "\"" : "`")))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    table_clause_ = table_str.string();
  }
  return ret;
}

int ObDynamicSamplingUtils::get_valid_dynamic_sampling_level(const ObSQLSessionInfo *session_info,
                                                             const ObTableDynamicSamplingHint *table_ds_hint,
                                                             const int64_t global_ds_level,
                                                             int64_t &ds_level,
                                                             int64_t &sample_block_cnt,
                                                             bool &specify_ds)
{
  int ret = OB_SUCCESS;
  uint64_t session_ds_level = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
  ds_level = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
  sample_block_cnt = 0;
  specify_ds = false;
  //first see table hint.
  if (table_ds_hint != NULL &&
      (table_ds_hint->get_dynamic_sampling() != ObGlobalHint::UNSET_DYNAMIC_SAMPLING)) {
    ds_level = table_ds_hint->get_dynamic_sampling();
    sample_block_cnt = table_ds_hint->get_sample_block_cnt();
    specify_ds = true;
  //then see global hint.
  } else if (global_ds_level != ObGlobalHint::UNSET_DYNAMIC_SAMPLING) {
    ds_level = global_ds_level;
    specify_ds = true;
  //last see user session variable.
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info));
  } else if (session_info->is_user_session() && OB_FAIL(session_info->get_opt_dynamic_sampling(session_ds_level))) {
    LOG_WARN("failed to get opt dynamic sampling level", K(ret));
  } else if (session_ds_level == ObDynamicSamplingLevel::BASIC_DYNAMIC_SAMPLING) {
    ds_level = session_ds_level;
  }
  LOG_TRACE("get valid dynamic sampling level", KPC(table_ds_hint), K(global_ds_level), K(specify_ds),
                                                K(session_ds_level), K(ds_level), K(sample_block_cnt));
  return ret;
}

int ObDynamicSamplingUtils::get_ds_table_param(ObOptimizerContext &ctx,
                                               const ObLogPlan *log_plan,
                                               const OptTableMeta *table_meta,
                                               ObDSTableParam &ds_table_param,
                                               bool &specify_ds)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  bool is_valid = true;
  int64_t ds_level = ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING;
  int64_t sample_block_cnt = 0;
  if (OB_ISNULL(log_plan) || OB_ISNULL(table_meta) ||
      OB_ISNULL(table_item = log_plan->get_stmt()->get_table_item_by_id(table_meta->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(log_plan), KPC(table_meta), KPC(table_item));
  } else if (OB_UNLIKELY(!log_plan->get_stmt()->is_select_stmt()) ||
             OB_UNLIKELY(ctx.use_default_stat()) ||
             OB_UNLIKELY(is_virtual_table(table_meta->get_ref_table_id()) && !is_ds_virtual_table(table_meta->get_ref_table_id())) ||
             OB_UNLIKELY(table_meta->get_table_type() == EXTERNAL_TABLE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(log_plan), KPC(table_meta), KPC(table_item));
  } else if (OB_FAIL(get_valid_dynamic_sampling_level(ctx.get_session_info(),
                                                      log_plan->get_log_plan_hint().get_dynamic_sampling_hint(table_meta->get_table_id()),
                                                      ctx.get_global_hint().get_dynamic_sampling(),
                                                      ds_level,
                                                      sample_block_cnt,
                                                      specify_ds))) {
    LOG_WARN("failed to get valid dynamic sampling level", K(ret));
  } else if (ds_level == ObDynamicSamplingLevel::BASIC_DYNAMIC_SAMPLING) {
    if (OB_FAIL(get_ds_table_part_info(ctx, table_meta->get_ref_table_id(),
                                       table_meta->get_all_used_tablets(),
                                       ds_table_param.need_specify_partition_,
                                       ds_table_param.partition_infos_))) {
      LOG_WARN("failed to get ds table part info", K(ret));
    } else if (check_is_failed_ds_table(table_meta->get_ref_table_id(),
                                        table_meta->get_all_used_parts(),
                                        ctx.get_failed_ds_tab_list())) {
      LOG_TRACE("get faile ds table, not use dynamic sampling", K(*table_meta), K(ctx.get_failed_ds_tab_list()));
    } else if (OB_FAIL(get_ds_table_degree(ctx, log_plan,
                                           table_meta->get_table_id(),
                                           table_meta->get_ref_table_id(),
                                           ds_table_param.degree_))) {
      LOG_WARN("failed to get ds table degree", K(ret));
    } else if ((ds_table_param.max_ds_timeout_ = get_dynamic_sampling_max_timeout(ctx)) > 0) {
      ds_table_param.tenant_id_ = ctx.get_session_info()->get_effective_tenant_id();
      ds_table_param.table_id_ = table_meta->get_ref_table_id();
      ds_table_param.ds_level_ = ds_level;
      ds_table_param.sample_block_cnt_ = sample_block_cnt;
      ds_table_param.is_virtual_table_ = is_virtual_table(table_meta->get_ref_table_id()) &&
                                    !share::is_oracle_mapping_real_virtual_table(table_meta->get_ref_table_id());
      ds_table_param.db_name_ = table_item->database_name_;
      ds_table_param.table_name_ = table_item->table_name_;
      ds_table_param.alias_name_ = table_item->alias_name_;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid ds level", K(ret), K(ds_level));
  }
  return ret;
}

int ObDynamicSamplingUtils::check_ds_can_use_filters(const ObIArray<ObRawExpr*> &filters,
                                                     bool &no_use)
{
  int ret = OB_SUCCESS;
  no_use = false;
  int64_t total_expr_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && !no_use && i < filters.count(); ++i) {
    if (OB_ISNULL(filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(filters.at(i)));
    } else if (OB_FAIL(check_ds_can_use_filter(filters.at(i), no_use, total_expr_cnt))) {
      LOG_WARN("failed to check ds can use filter", K(ret));
    }
  }
  return ret;
}

int ObDynamicSamplingUtils::check_ds_can_use_filter(const ObRawExpr *filter,
                                                    bool &no_use,
                                                    int64_t &total_expr_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(filter));
  } else if (filter->has_flag(CNT_DYNAMIC_PARAM) ||
             filter->has_flag(CNT_SUB_QUERY) ||
             filter->has_flag(CNT_RAND_FUNC) ||
             filter->has_flag(CNT_DYNAMIC_USER_VARIABLE) ||
             filter->has_flag(CNT_PL_UDF) ||
             filter->has_flag(CNT_SO_UDF) ||
             filter->has_flag(CNT_MATCH_EXPR) ||
             filter->get_expr_type() == T_FUN_SET_TO_STR ||
             filter->get_expr_type() == T_FUN_ENUM_TO_STR ||
             filter->get_expr_type() == T_OP_GET_PACKAGE_VAR ||
             (filter->get_expr_type() >= T_FUN_SYS_IS_JSON &&
              filter->get_expr_type() <= T_FUN_SYS_TREAT) ||
             filter->get_expr_type() == T_FUN_GET_TEMP_TABLE_SESSID) {
    no_use = true;
  } else if (filter->get_expr_type() == T_FUN_SYS_LNNVL) {
    const ObRawExpr *real_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(filter->get_param_expr(0), real_expr))) {
      LOG_WARN("fail to get real expr without cast", K(ret));
    } else if (OB_ISNULL(real_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real expr is invalid", K(ret), K(real_expr));
    } else if (real_expr->get_expr_type() == T_OP_OR ||
               real_expr->get_expr_type() == T_OP_AND ||
               real_expr->get_expr_type() == T_OP_NOT ||
               real_expr->get_expr_type() == T_OP_BOOL) {
      no_use = true;
    } else {/*do nothing*/}
  } else if (filter->is_column_ref_expr()) {
    //Dynamic Sampling of columns with LOB-related types is prohibited, as projecting such type columns is particularly slow.
    //bug:
    if (!ObColumnStatParam::is_valid_opt_col_type(filter->get_data_type())) {
      no_use = true;
    } else if (ob_obj_type_class(filter->get_data_type()) == ColumnTypeClass::ObTextTC &&
               filter->get_data_type() != ObTinyTextType) {
      no_use = true;
    }
  } else {/*do nothing*/}
  if (OB_SUCC(ret) && !no_use) {
    ++ total_expr_cnt;
    no_use = total_expr_cnt > OB_DS_MAX_FILTER_EXPR_COUNT;
    for (int64_t i = 0; !no_use && OB_SUCC(ret) && i < filter->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_ds_can_use_filter(filter->get_param_expr(i), no_use, total_expr_cnt)))) {
        LOG_WARN("failed to check ds can use filter", K(ret));
      }
    }
  }
  return ret;
}

int ObDynamicSamplingUtils::get_ds_table_part_info(ObOptimizerContext &ctx,
                                                   const uint64_t ref_table_id,
                                                   const common::ObIArray<ObTabletID> &used_tablets,
                                                   bool &need_specify_partition,
                                                   ObIArray<PartInfo> &partition_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartInfo, 4> tmp_part_infos;
  ObSEArray<PartInfo, 4> tmp_subpart_infos;
  ObSEArray<int64_t, 4> tmp_part_ids;
  ObSEArray<int64_t, 4> tmp_subpart_ids;
  const ObTableSchema *table_schema = NULL;
  const ObSqlSchemaGuard *schema_guard = NULL;
  need_specify_partition = false;
  if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(table_schema));
  } else if (!table_schema->is_partitioned_table() ||
             (is_virtual_table(ref_table_id) && !share::is_oracle_mapping_real_virtual_table(ref_table_id))) {
    /*do nothing*/
  } else if (OB_FAIL(ObDbmsStatsUtils::get_part_infos(*table_schema,
                                                      ctx.get_allocator(),
                                                      tmp_part_infos,
                                                      tmp_subpart_infos,
                                                      tmp_part_ids,
                                                      tmp_subpart_ids))) {
    LOG_WARN("failed to get table part infos", K(ret));
  } else if ((table_schema->get_part_level() == share::schema::PARTITION_LEVEL_ONE &&
              tmp_part_infos.count() == used_tablets.count()) ||
             (table_schema->get_part_level() == share::schema::PARTITION_LEVEL_TWO &&
              tmp_subpart_infos.count() == used_tablets.count())) {
    if ((table_schema->get_part_level() == share::schema::PARTITION_LEVEL_ONE &&
         OB_FAIL(append(partition_infos, tmp_part_infos))) ||
        (table_schema->get_part_level() == share::schema::PARTITION_LEVEL_TWO &&
         OB_FAIL(append(partition_infos, tmp_subpart_infos)))) {
      LOG_WARN("failed to append", K(ret));
    }
  } else {
    need_specify_partition = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < used_tablets.count(); ++i) {
      bool found_it = false;
      if (table_schema->get_part_level() == share::schema::PARTITION_LEVEL_ONE) {
        for (int64_t j = 0; OB_SUCC(ret) && !found_it && j < tmp_part_infos.count(); ++j) {
          if (tmp_part_infos.at(j).tablet_id_ == used_tablets.at(i)) {
            found_it = true;
            if (OB_FAIL(partition_infos.push_back(tmp_part_infos.at(j)))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
      if (table_schema->get_part_level() == share::schema::PARTITION_LEVEL_TWO) {
        for (int64_t j = 0; OB_SUCC(ret) && !found_it && j < tmp_subpart_infos.count(); ++j) {
          if (tmp_subpart_infos.at(j).tablet_id_ == used_tablets.at(i)) {
            found_it = true;
            if (OB_FAIL(partition_infos.push_back(tmp_subpart_infos.at(j)))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && !found_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(used_tablets.at(i)), K(tmp_part_infos), K(tmp_subpart_infos));
      }
    }
  }
  return ret;
}

int ObDynamicSamplingUtils::get_ds_table_degree(ObOptimizerContext &ctx,
                                                const ObLogPlan *log_plan,
                                                const uint64_t table_id,
                                                const uint64_t ref_table_id,
                                                int64_t &degree)
{
  int ret = OB_SUCCESS;
  degree = ObGlobalHint::DEFAULT_PARALLEL;
  const ObTableSchema *table_schema = NULL;
  const ObSqlSchemaGuard *schema_guard = NULL;
  if (OB_ISNULL(schema_guard = ctx.get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(table_schema));
  } else if (ObGlobalHint::DEFAULT_PARALLEL <= (degree = log_plan->get_log_plan_hint().get_parallel(table_id))) {
    // use table parallel hint
  } else if (ctx.is_use_table_dop()) {
    // use table dop
    degree = table_schema->get_dop();
  } else {
    degree = std::max(degree, ctx.get_parallel());
  }
  LOG_TRACE("succeed to get ds table degree", K(degree));
  return ret;
}

const ObDSResultItem *ObDynamicSamplingUtils::get_ds_result_item(ObDSResultItemType type,
                                                                 uint64_t index_id,
                                                                 const ObIArray<ObDSResultItem> &ds_result_items)
{
  const ObDSResultItem *item = NULL;
  bool found_it = false;
  for (int64_t i = 0; !found_it && i < ds_result_items.count(); ++i) {
    if (ds_result_items.at(i).type_ == type &&
        ds_result_items.at(i).index_id_ == index_id) {
      item = &ds_result_items.at(i);
      found_it = true;
    }
  }
  return item;
}

int64_t ObDynamicSamplingUtils::get_dynamic_sampling_max_timeout(ObOptimizerContext &ctx)
{
  int64_t max_ds_timeout = 0;
  if (THIS_WORKER.get_timeout_remain() / 10 >= OB_DS_MIN_QUERY_TIMEOUT) {
    max_ds_timeout = THIS_WORKER.get_timeout_remain() / 10;//default ds time can't exceed 10% of current sql remain timeout
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(ctx.get_session_info()->get_effective_tenant_id()));
    if (tenant_config.is_valid()) {
      int64_t ds_maximum_time = tenant_config->_optimizer_ads_time_limit * 1000000;
      if (max_ds_timeout > ds_maximum_time) {//can't exceed the max ds timeout for single table
        max_ds_timeout = ds_maximum_time;
      }
    }
  }
  return max_ds_timeout;
}

int ObDynamicSamplingUtils::add_failed_ds_table_list(const uint64_t table_id,
                                                     const common::ObIArray<int64_t> &used_part_id,
                                                     common::ObIArray<ObDSFailTabInfo> &failed_list)
{
  int ret = OB_SUCCESS;
  ObDSFailTabInfo info;
  info.table_id_ = table_id;
  if (OB_FAIL(info.part_ids_.assign(used_part_id))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(failed_list.push_back(info))) {
    LOG_WARN("failed to push back");
  } else {/*do nothing*/}
  return ret;
}

bool ObDynamicSamplingUtils::check_is_failed_ds_table(const uint64_t table_id,
                                                      const common::ObIArray<int64_t> &used_part_id,
                                                      const common::ObIArray<ObDSFailTabInfo> &failed_list)
{
  bool found_it = false;
  for (int64_t i = 0; !found_it && i < failed_list.count(); ++i) {
    if (table_id == failed_list.at(i).table_id_) {
      if (used_part_id.empty() && !failed_list.at(i).part_ids_.empty()) {
        found_it = true;
      } else {
        found_it = true;
        for (int64_t j = 0; found_it && j < used_part_id.count(); ++j) {
          bool in_it = false;
          for (int64_t k = 0; !in_it && k < failed_list.at(i).part_ids_.count(); ++k) {
            if (used_part_id.at(j) == failed_list.at(i).part_ids_.at(k)) {
              in_it = true;
            }
          }
          found_it = in_it;
        }
      }
    }
  }
  return found_it;
}

bool ObDynamicSamplingUtils::is_ds_virtual_table(const int64_t table_id)
{
  return is_virtual_table(table_id) &&
         (share::is_oracle_mapping_real_virtual_table(table_id) ||
          table_id == share::OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID ||
          table_id == share::OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID ||
          table_id == share::OB_ALL_VIRTUAL_DML_STATS_TID ||
          table_id == share::OB_ALL_VIRTUAL_DATA_TYPE_TID ||
          table_id == share::OB_ALL_VIRTUAL_TENANT_MYSQL_SYS_AGENT_TID ||
          table_id == share::OB_ALL_VIRTUAL_TENANT_INFO_TID ||
          table_id == share::OB_ALL_VIRTUAL_CORE_META_TABLE_TID ||
          table_id == share::OB_TENANT_VIRTUAL_COLLATION_TID ||
          table_id == share::OB_ALL_VIRTUAL_CORE_ALL_TABLE_ORA_TID ||
          table_id == share::OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_ORA_TID ||
          table_id == share::OB_ALL_VIRTUAL_DML_STATS_ORA_TID ||
          table_id == share::OB_ALL_VIRTUAL_DATA_TYPE_ORA_TID ||
          table_id == share::OB_ALL_VIRTUAL_TENANT_INFO_ORA_TID ||
          table_id == share::OB_TENANT_VIRTUAL_COLLATION_ORA_TID);
}

//following function used to dynamic sampling join in the future.

// int ObDynamicSampling::estimate_join_rowcount(const ObOptDSJoinParam &param,
//                                                  uint64_t &join_output_cnt)
// {
//   int ret = OB_SUCCESS;
//   bool get_result = false;
//   ObOptDSStat::Key key;
//   int64_t sample_tab_modified_count = 0;
//   int64_t other_tab_modified_count = 0;
//   if (OB_FAIL(get_ds_join_result_from_cache(param, key, join_output_cnt,
//                                             sample_tab_modified_count,
//                                             other_tab_modified_count,
//                                             get_result))) {
//     LOG_WARN("failed to get ds table result from cache", K(ret));
//   } else if (get_result) {
//     //do nothing
//   } else {
//     ObOptDSStat ds_stat;
//     ds_stat.init(key, ObOptDSType::JOIN_DYNAMIC_SAMPLE);
//     ds_stat.set_sample_tab_dml_cnt(sample_tab_modified_count);
//     ds_stat.set_other_tab_dml_cnt(other_tab_modified_count);
//     ObOptDSStatHandle ds_stat_handle;
//     if (OB_FAIL(do_estimate_join_rowcount(param, ds_stat, get_result))) {
//       LOG_WARN("failed to do estimate table rowcount", K(ret));
//     } else if (!get_result) {
//       //do nothing
//     } else if (OB_FAIL(ctx_->get_opt_stat_manager()->add_ds_stat_cache(key, ds_stat, ds_stat_handle))) {
//       LOG_WARN("get ds stat failed", K(ret), K(key), K(param), K(ds_stat));
//     } else {
//       join_output_cnt = ds_stat.get_output_rowcount();
//     }
//   }
//   return ret;
// }

// int ObDynamicSampling::do_estimate_join_rowcount(const ObOptDSJoinParam &param,
//                                                     ObOptDSStat &ds_stat,
//                                                     bool &get_result)
// {
//   int ret = OB_SUCCESS;
//   double sample_ratio = 0.0;
//   ObSqlString partition_str;
//   ObSqlString partition_str2;
//   ObSqlString where_str;
//   ObSqlString join_cond_str;
//   ObSqlString join_type_str;
//   ObSEArray<ObRawExpr*, 4> tmp_filters;
//   get_result = false;
//   ObString empty_str;
//   LOG_TRACE("begin estimate join rowcount", K(param));
//   if (OB_FAIL(add_table_info(param.left_table_param_.db_name_,
//                              param.left_table_param_.table_name_,
//                              param.left_table_param_.alias_name_))) {
//     LOG_WARN("failed to add table info", K(ret));
//   } else if (OB_FAIL(add_table_info(param.right_table_param_.db_name_,
//                                     param.right_table_param_.table_name_,
//                                     param.right_table_param_.alias_name_,
//                                     false))) {
//     LOG_WARN("failed to add table info", K(ret));
//   } else if (OB_FAIL(add_partition_info(param.left_table_param_.specified_partition_names_,
//                                         partition_str,
//                                         partition_list_))) {
//     LOG_WARN("failed to add partition info", K(ret));
//   } else if (OB_FAIL(add_partition_info(param.right_table_param_.specified_partition_names_,
//                                         partition_str2,
//                                         partition_list2_))) {
//     LOG_WARN("failed to add partition info", K(ret));
//   } else if (OB_FAIL(add_join_type(param.join_type_, join_type_str))) {
//     LOG_WARN("failed to add join type", K(ret));
//   } else if (OB_FAIL(add_ds_stat_item(ObDSStatItem(&ds_stat,
//                                                    empty_str,
//                                                    OB_DS_JOIN_OUTPUT_ROWCOUNT)))) {
//     LOG_WARN("failed to add ds logical rowcount stat item", K(ret));
//   } else if (OB_FAIL(append(tmp_filters, param.left_table_param_.all_table_filters_))) {
//     LOG_WARN("failed to append", K(ret));
//   } else if (OB_FAIL(append(tmp_filters, param.right_table_param_.all_table_filters_))) {
//     LOG_WARN("failed to append", K(ret));
//   } else if (OB_FAIL(add_filter_infos(tmp_filters, false, where_str, where_conditions_))) {
//     LOG_WARN("failed to add where condition", K(ret));
//   } else if (param.join_conditions_ != NULL &&
//              OB_FAIL(add_filter_infos(*param.join_conditions_, false, join_cond_str, join_conditions_))) {
//     LOG_WARN("failed to add where condition", K(ret));
//   } else if (OB_FAIL(calc_join_sample_block_ratio(param))) {
//     LOG_WARN("failed to calc sample block ratio", K(ret));
//   } else if (OB_FAIL(estimte_rowcount(get_result, ObDynamicSamplingLevel::ADS_DYNAMIC_SAMPLING))) {
//     LOG_WARN("failed to estimate rowcount", K(ret));
//   } else if (get_result) {
//     ds_stat.set_stat_expired_time(ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL);
//     OPT_TRACE("succeed to do estimate join rowcount");
//   }
//   return ret;
// }

// int ObDynamicSampling::pack_join_ds_sql(ObSqlString &raw_sql_str)
// {
//   int ret = OB_SUCCESS;
//   ObSqlString select_fields;
//   if (OB_FAIL(gen_select_filed(select_fields))) {
//     LOG_WARN("failed to generate select filed", K(ret));
//   } else if (OB_FAIL(raw_sql_str.append_fmt(lib::is_oracle_mode() ?
//                                             "SELECT %.*s %.*s FROM \"%.*s\".\"%.*s\" %.*s %.*s %.*s %.*s \"%.*s\".\"%.*s\" %.*s %.*s %.*s %s %.*s %s %.*s" :
//                                             "SELECT %.*s %.*s FROM `%.*s`.`%.*s` %.*s %.*s %.*s %.*s `%.*s`.`%.*s` %.*s %.*s %.*s %s %.*s %s %.*s",
//                                             basic_hints_.length(),
//                                             basic_hints_.ptr(),
//                                             static_cast<int32_t>(select_fields.length()),
//                                             select_fields.ptr(),
//                                             db_name_.length(),
//                                             db_name_.ptr(),
//                                             table_name_.length(),
//                                             table_name_.ptr(),
//                                             partition_list_.length(),
//                                             partition_list_.ptr(),
//                                             is_left_sample_ ? sample_block_.length() : 0,
//                                             is_left_sample_ ? sample_block_.ptr() : NULL,
//                                             alias_name_.length(),
//                                             alias_name_.ptr(),
//                                             join_type_.length(),
//                                             join_type_.ptr(),
//                                             db_name2_.length(),
//                                             db_name2_.ptr(),
//                                             table_name2_.length(),
//                                             table_name2_.ptr(),
//                                             partition_list2_.length(),
//                                             partition_list2_.ptr(),
//                                             is_left_sample_ ? 0 : sample_block_.length(),
//                                             is_left_sample_ ? NULL : sample_block_.ptr(),
//                                             alias_name2_.length(),
//                                             alias_name2_.ptr(),
//                                             join_conditions_.empty() ? " " : "ON",
//                                             join_conditions_.length(),
//                                             join_conditions_.ptr(),
//                                             where_conditions_.empty() ? " " : "WHERE",
//                                             where_conditions_.length(),
//                                             where_conditions_.ptr()))) {
//     LOG_WARN("failed to build query sql stmt", K(ret));
//   } else {
//     LOG_TRACE("OptStat: dynamic sampling query sql", K(raw_sql_str));
//     OPT_TRACE("OptStat: dynamic sampling query sql", raw_sql_str.string());
//   }
//   return ret;
// }

/*
 * virtual table choose full table scan
*/
// int ObDynamicSampling::calc_join_sample_block_ratio(const ObOptDSJoinParam &param)
// {
//   int ret = OB_SUCCESS;
//   int64_t sample_micro_cnt = OB_OPT_DS_ADAPTIVE_SAMPLE_MICRO_CNT;//default
//   micro_block_num_ = param.left_table_param_.micro_block_count_;
//   micro_total_count2_ = param.right_table_param_.micro_block_count_;
//   if (param.left_table_param_.is_virtual_table_ || param.right_table_param_.is_virtual_table_) {
//     if (param.left_table_param_.is_virtual_table_ && param.right_table_param_.is_virtual_table_) {
//       sample_block_ratio_ = 100.0;
//     } else if (param.left_table_param_.is_virtual_table_) {
//       is_left_sample_ = false;
//       sample_block_ratio_ = sample_micro_cnt > micro_total_count2_ ? 100.0 : 100.0 * sample_micro_cnt / micro_total_count2_;
//     } else {
//       is_left_sample_ = true;
//       sample_block_ratio_ = sample_micro_cnt > micro_block_num_ ? 100.0 : 100.0 * sample_micro_cnt / micro_block_num_;
//     }
//   } else {
//     is_left_sample_ = micro_block_num_ >= micro_total_count2_;
//     int64_t micro_total_count = micro_block_num_ >= micro_total_count2_ ? micro_block_num_ : micro_total_count2_;
//     if (micro_total_count == 0 || sample_micro_cnt >= micro_total_count) {
//       sample_block_ratio_ = 100.0;
//     } else {
//       sample_block_ratio_ = 100.0 * sample_micro_cnt / micro_total_count;
//     }
//     LOG_TRACE("succeed to calc table sample block ratio", K(param), K(micro_total_count),
//         K(micro_block_num_), K(micro_total_count2_), K(sample_micro_cnt), K(sample_block_ratio_));
//   }
//   return ret;
// }

// int ObDynamicSampling::get_ds_join_result_from_cache(const ObOptDSJoinParam &param,
//                                                         ObOptDSStat::Key &key,
//                                                         uint64_t &join_output_cnt,
//                                                         int64_t &sample_tab_modified_count,
//                                                         int64_t &other_tab_modified_count,
//                                                         bool &get_result)
// {
//   int ret = OB_SUCCESS;
//   ObOptDSStatHandle ds_stat_handle;
//   join_output_cnt = 0;
//   sample_tab_modified_count = 0;
//   other_tab_modified_count = 0;
//   double sample_stale_percent_threshold = OPT_DEFAULT_STALE_PERCENT;
//   double other_stale_percent_threshold = OPT_DEFAULT_STALE_PERCENT;
//   if (OB_ISNULL(ctx_->get_opt_stat_manager()) ||
//       OB_ISNULL(ctx_->get_session_info()) ||
//       OB_ISNULL(ctx_->get_exec_ctx())) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected null", K(ret), K(ctx_->get_opt_stat_manager()),
//                                     K(ctx_->get_session_info()));
//   } else if (OB_FAIL(get_ds_join_stat_info(ctx_->get_session_info()->get_effective_tenant_id(),
//                                            param, key))) {
//     LOG_WARN("failed to gen ds stat key", K(ret), K(param));
//   } else if (OB_FAIL(ctx_->get_opt_stat_manager()->get_ds_stat(key, ds_stat_handle))) {
//     if (OB_ENTRY_NOT_EXIST != ret) {
//       LOG_WARN("get ds stat failed", K(ret), K(key), K(param));
//     } else {
//       ret = OB_SUCCESS;
//       LOG_TRACE("ds stat not exists in cache", K(key));
//     }
//   } else if (OB_ISNULL(ds_stat_handle.stat_)) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected null", K(ret), K(ds_stat_handle.stat_));
//   } else if (!ds_stat_handle.stat_->is_arrived_expired_time()) {
//     get_result = true;
//     join_output_cnt = ds_stat_handle.stat_->get_output_rowcount();
//     OPT_TRACE("succeed to get ds join result from cache");
//   } else if (OB_FAIL(ObBasicStatsEstimator::estimate_modified_count(*ctx_->get_exec_ctx(),
//                                                                     ctx_->get_session_info()->get_effective_tenant_id(),
//                                                                     is_left_sample_ ? param.left_table_param_.table_id_ :
//                                                                                       param.right_table_param_.table_id_,
//                                                                     sample_tab_modified_count,
//                                                                     false))) {
//     LOG_WARN("failed to estimate modified count", K(ret));
//   } else if (OB_FAIL(ObBasicStatsEstimator::estimate_modified_count(*ctx_->get_exec_ctx(),
//                                                                     ctx_->get_session_info()->get_effective_tenant_id(),
//                                                                     !is_left_sample_ ? param.left_table_param_.table_id_ :
//                                                                                        param.right_table_param_.table_id_,
//                                                                     other_tab_modified_count,
//                                                                     false))) {
//     LOG_WARN("failed to estimate modified count", K(ret));
//   } else if (OB_FAIL(pl::ObDbmsStats::get_table_stale_percent_threshold(*ctx_->get_exec_ctx(),
//                                                                         ctx_->get_session_info()->get_effective_tenant_id(),
//                                                                         is_left_sample_ ? param.left_table_param_.table_id_ :
//                                                                                           param.right_table_param_.table_id_,
//                                                                         sample_stale_percent_threshold))) {
//     LOG_WARN("failed to get table stale percent threshold", K(ret));
//   } else if (OB_FAIL(pl::ObDbmsStats::get_table_stale_percent_threshold(*ctx_->get_exec_ctx(),
//                                                                         ctx_->get_session_info()->get_effective_tenant_id(),
//                                                                         !is_left_sample_ ? param.left_table_param_.table_id_ :
//                                                                                            param.right_table_param_.table_id_,
//                                                                         other_stale_percent_threshold))) {
//     LOG_WARN("failed to get table stale percent threshold", K(ret));
//   } else {
//     int64_t origin_sample_dml_cnt = ds_stat_handle.stat_->get_sample_tab_dml_cnt();
//     int64_t origin_other_dml_cnt = ds_stat_handle.stat_->get_table_logical_rowcount();
//     int64_t sample_inc_cnt = sample_tab_modified_count - origin_sample_dml_cnt;
//     int64_t other_inc_cnt = other_tab_modified_count - origin_other_dml_cnt;
//     double sample_inc_ratio = origin_sample_dml_cnt == 0 ? 0 : sample_inc_cnt * 1.0 / origin_sample_dml_cnt;
//     double other_inc_ratio = origin_other_dml_cnt == 0 ? 0 : other_inc_cnt * 1.0 / origin_other_dml_cnt;
//     if (sample_inc_ratio <= sample_stale_percent_threshold && other_inc_ratio <= other_stale_percent_threshold) {
//       get_result = true;
//       join_output_cnt = ds_stat_handle.stat_->get_output_rowcount();
//       const_cast<ObOptDSStat*>(ds_stat_handle.stat_)->set_stat_expired_time(ObTimeUtility::current_time() + ObOptStatMonitorCheckTask::CHECK_INTERVAL);
//       OPT_TRACE("succeed to get ds join result from cache");
//     } else if (OB_FAIL(ctx_->get_opt_stat_manager()->erase_ds_stat(key))) {
//       LOG_WARN("failed to erase ds stat", K(ret));
//     }
//     LOG_TRACE("get ds join result from cache", K(get_result), K(param), K(key), KPC(ds_stat_handle.stat_),
//                                                K(other_tab_modified_count), K(sample_inc_ratio),
//                                                K(sample_tab_modified_count), K(other_inc_ratio),
//                                                K(sample_stale_percent_threshold), K(other_stale_percent_threshold));
//   }
//   return ret;
// }

// int ObDynamicSampling::get_ds_join_stat_info(const uint64_t tenant_id,
//                                                 const ObOptDSJoinParam &param,
//                                                 ObOptDSStat::Key &key)
// {
//   int ret = OB_SUCCESS;
//   ObSEArray<ObRawExpr*, 8> all_filters;
//   ObSqlString expr_str;
//   if (OB_ISNULL(param.join_conditions_) || OB_ISNULL(ctx_->get_sql_schema_guard())) {
//     ret = OB_ERR_UNEXPECTED;
//     LOG_WARN("get unexpected null", K(ret));
//   } else if (OB_FAIL(append(all_filters, *param.join_conditions_)) ||
//              OB_FAIL(append(all_filters, param.left_table_param_.all_table_filters_)) ||
//              OB_FAIL(append(all_filters, param.right_table_param_.all_table_filters_))) {
//     LOG_WARN("failed to append", K(ret));
//   } else if (OB_FAIL(print_filter_exprs(ctx_->get_session_info(),
//                                         ctx_->get_sql_schema_guard()->get_schema_guard(),
//                                         ctx_->get_params(),
//                                         all_filters,
//                                         false,
//                                         expr_str))) {
//     LOG_WARN("failed to print filter exprs", K(ret));
//   } else if (OB_FAIL(calc_join_sample_block_ratio(param))) {
//     LOG_WARN("failed to calc sample block ratio", K(ret));
//   } else {
//     const ObOptDSBaseParam &sample_ds_param = is_left_sample_ ? param.left_table_param_ : param.right_table_param_;
//     const ObOptDSBaseParam &other_ds_param = !is_left_sample_ ? param.left_table_param_ : param.right_table_param_;
//     ObSqlString sample_partition_str;
//     ObSqlString other_partition_str;
//     if (OB_FAIL(gen_partition_str(sample_ds_param, sample_partition_str)) ||
//         OB_FAIL(gen_partition_str(other_ds_param, other_partition_str))) {
//        LOG_WARN("failed to print filter exprs", K(ret));
//     } else {
//       key.tenant_id_ = tenant_id;
//       key.sample_tab_id_ = sample_ds_param.table_id_;
//       key.sample_index_id_ = sample_ds_param.index_id_;
//       key.sample_partition_hash_ = murmurhash64A(sample_partition_str.ptr(), sample_partition_str.length(), 0);
//       key.ds_level_ = sample_ds_param.ds_level_;
//       key.other_tab_id_ = other_ds_param.table_id_;
//       key.other_index_id_ = other_ds_param.index_id_;
//       key.other_partition_hash_ = murmurhash64A(other_partition_str.ptr(), other_partition_str.length(), 0);
//       key.expression_hash_ = murmurhash64A(expr_str.ptr(), expr_str.length(), 0);
//       LOG_TRACE("succeed to gen ds tab stat key", K(key), K(expr_str));
//     }
//   }
//   return ret;
// }

// int ObDynamicSampling::add_join_type(ObJoinType join_type, ObSqlString &join_type_str)
// {
//   int ret = OB_SUCCESS;
//   switch (join_type) {
//     case INNER_JOIN: {
//       if (OB_FAIL(join_type_str.append(" INNER JOIN "))) {
//         LOG_WARN("failed to append", K(ret));
//       }
//       break;
//     }
//     case LEFT_OUTER_JOIN: {
//       if (OB_FAIL(join_type_str.append(" LEFT OUTER JOIN "))) {
//         LOG_WARN("failed to append", K(ret));
//       }
//       break;
//     }
//     case RIGHT_OUTER_JOIN: {
//       if (OB_FAIL(join_type_str.append(" RIGHT OUTER JOIN "))) {
//         LOG_WARN("failed to append", K(ret));
//       }
//       break;
//     }
//     case FULL_OUTER_JOIN: {
//       if (OB_FAIL(join_type_str.append(" FULL OUTER JOIN "))) {
//         LOG_WARN("failed to append", K(ret));
//       }
//       break;
//     }
//     default:
//       ret = OB_NOT_SUPPORTED;
//       LOG_WARN("get not supported join dynamic sampling", K(ret), K(join_type));
//   }
//   if (OB_SUCC(ret)) {
//     join_type_ = join_type_str.string();
//   }
//   return ret;
// }

}  // end of namespace common
}  // end of namespace oceanbase
