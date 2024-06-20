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

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/ob_index_stats_estimator.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_opt_stat_manager.h"
namespace oceanbase
{
namespace common
{

ObIndexStatsEstimator::ObIndexStatsEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObBasicStatsEstimator(ctx, allocator)
{}

int ObIndexStatsEstimator::estimate(const ObOptStatGatherParam &param,
                                    ObIArray<ObOptStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnStatParam> &column_params = param.column_params_;
  ObString no_rewrite("NO_REWRITE USE_PLAN_CACHE(NONE) DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', 256)");
  ObString calc_part_id_str;
  ObOptTableStat tab_stat;
  ObOptStat src_opt_stat;
  src_opt_stat.table_stat_ = &tab_stat;
  ObOptTableStat *src_tab_stat = src_opt_stat.table_stat_;
  ObIArray<ObOptColumnStat*> &src_col_stats = src_opt_stat.column_stats_;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  ObSqlString raw_sql;
  int64_t duration_time = -1;

  // Note that there are dependences between different kinds of statistics
  //            1. RowCount should be added at the first
  //            2. NumDistinct should be estimated before TopKHist
  //            3. AvgRowLen should be added at the last
  if (OB_UNLIKELY(dst_opt_stats.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(dst_opt_stats.empty()));
  } else if (OB_FAIL(ObDbmsStatsUtils::init_col_stats(allocator,
                                                      column_params.count(),
                                                      src_col_stats))) {
    LOG_WARN("failed init col stats", K(ret));
  } else if (OB_FAIL(add_hint(no_rewrite, ctx_.get_allocator()))) {
    LOG_WARN("failed to add no_rewrite", K(ret));
  } else if (OB_FAIL(add_from_table(param.db_name_, param.data_table_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_index_info(ctx_.get_allocator(),
                                     param.data_table_name_,
                                     param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_parallel_info(ctx_.get_allocator(), param.degree_))) {
    LOG_WARN("failed to add query sql parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(ctx_.get_allocator(), duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (dst_opt_stats.count() > 1 &&
             OB_FAIL(fill_index_group_by_info(ctx_.get_allocator(), param, calc_part_id_str))) {
    LOG_WARN("failed to group by info", K(ret));
  } else if (OB_FAIL(add_stat_item(ObStatRowCount(src_tab_stat)))) {
    LOG_WARN("failed to add row count", K(ret));
  } else if (calc_part_id_str.empty()) {
    if (OB_FAIL(fill_partition_condition(ctx_.get_allocator(), param,
                                         dst_opt_stats.at(0).table_stat_->get_partition_id()))) {
      LOG_WARN("failed to add fill partition condition", K(ret));
    } else if (OB_UNLIKELY(dst_opt_stats.count() != 1) ||
               OB_ISNULL(dst_opt_stats.at(0).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(dst_opt_stats.count()));
    } else {
      src_tab_stat->set_partition_id(dst_opt_stats.at(0).table_stat_->get_partition_id());
    }
  } else if (OB_FAIL(add_stat_item(ObPartitionId(src_tab_stat, calc_part_id_str, -1)))) {
    LOG_WARN("failed to add partition id", K(ret));
  } else {/*do nothing*/}
  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    const ObColumnStatParam *col_param = &column_params.at(i);
    if (OB_FAIL(add_stat_item(ObStatAvgLen(col_param, src_col_stats.at(i))))) {
      LOG_WARN("failed to add statistic item", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_stat_item(ObStatAvgRowLen(src_tab_stat, src_col_stats)))) {
      LOG_WARN("failed to add avg row size estimator", K(ret));
    } else if (OB_FAIL(pack(raw_sql))) {
      LOG_WARN("failed to pack raw sql", K(ret));
    } else if (OB_FAIL(do_estimate(param.tenant_id_, raw_sql.string(), true, src_opt_stat, dst_opt_stats))) {
      LOG_WARN("failed to evaluate basic stats", K(ret));
    } else {
      LOG_TRACE("index stats is collected", K(dst_opt_stats.count()));
    }
  }
  return ret;
}

int ObIndexStatsEstimator::fill_index_info(common::ObIAllocator &alloc,
                                           const ObString &table_name,
                                           const ObString &index_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_name.empty() || index_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_name), K(index_name));
  } else {
    const char *fmt_str = "INDEX(%.*s %.*s)";
    char *buf = NULL;
    int64_t buf_len = table_name.length() + index_name.length() + strlen(fmt_str);
    if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(buf), K(buf_len));
    } else {
      int64_t real_len = sprintf(buf, fmt_str, table_name.length(), table_name.ptr(),
                                               index_name.length(), index_name.ptr());
      if (OB_UNLIKELY(real_len < 0 || real_len > buf_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(real_len));
      } else {
        ObString index_str;
        index_str.assign_ptr(buf, real_len);
        if (OB_FAIL(add_hint(index_str, alloc))) {
          LOG_WARN("failed to add hint", K(ret));
        } else {
          LOG_TRACE("succeed to fill index info", K(index_str));
        }
      }
    }
  }
  return ret;
}

//Specify the partition name or non-partition table don't use group by.
int ObIndexStatsEstimator::fill_index_group_by_info(ObIAllocator &allocator,
                                                    const ObOptStatGatherParam &param,
                                                    ObString &calc_part_id_str)
{
  int ret = OB_SUCCESS;
  const char *fmt_str = lib::is_oracle_mode() ? "GROUP BY CALC_PARTITION_ID(\"%.*s\", \"%.*s\", %.*s)"
                                                : "GROUP BY CALC_PARTITION_ID(`%.*s`, `%.*s`, %.*s)";
  char *buf = NULL;
  ObString type_str;
  if (param.stat_level_ == PARTITION_LEVEL) {
    type_str = ObString(4, "PART");
  } else if (param.stat_level_ == SUBPARTITION_LEVEL) {
    type_str = ObString(7, "SUBPART");
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(param.stat_level_), K(ret));
  }
  if (OB_SUCC(ret)) {
    const int64_t len = strlen(fmt_str) +
                        param.data_table_name_.length() +
                        param.tab_name_.length() +
                        type_str.length() ;
    int32_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len));
    } else {
      real_len = sprintf(buf, fmt_str, param.data_table_name_.length(),param.data_table_name_.ptr(),
                                       param.tab_name_.length(), param.tab_name_.ptr(),
                                       type_str.length(), type_str.ptr());
      if (OB_UNLIKELY(real_len < 0 || real_len > len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to print partition hint", K(ret), K(real_len), K(len), K(param));
      } else {
        group_by_string_.assign(buf, real_len);
        //"GROUP BY CALC_PARTITION_ID(xxxxx)"
        const int64_t len_group_by = strlen("GROUP BY ");
        calc_part_id_str.assign_ptr(group_by_string_.ptr() + len_group_by,
                                    group_by_string_.length() - len_group_by);
        LOG_TRACE("Succeed to fill group by info", K(group_by_string_), K(calc_part_id_str));
      }
    }
  }
  return ret;
}

//Specify the partition name need add filter.
int ObIndexStatsEstimator::fill_partition_condition(ObIAllocator &allocator,
                                                    const ObOptStatGatherParam &param,
                                                    const int64_t dst_partition_id)
{
  int ret = OB_SUCCESS;
  if (param.stat_level_ == PARTITION_LEVEL || param.stat_level_ == SUBPARTITION_LEVEL) {
    const char *fmt_str = lib::is_oracle_mode() ? "WHERE CALC_PARTITION_ID(\"%.*s\", \"%.*s\", %.*s) = %ld"
                                                  : "WHERE CALC_PARTITION_ID(`%.*s`, `%.*s`, %.*s) = %ld";
    char *buf = NULL;
    ObString type_str;
    if (param.stat_level_ == PARTITION_LEVEL) {
      type_str = ObString(4, "PART");
    } else if (param.stat_level_ == SUBPARTITION_LEVEL) {
      type_str = ObString(7, "SUBPART");
    }
    const int64_t len = strlen(fmt_str) +
                        param.data_table_name_.length() +
                        param.tab_name_.length() +
                        type_str.length() + 30;
    int32_t real_len = -1;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(len));
    } else {
      real_len = sprintf(buf, fmt_str, param.data_table_name_.length(),param.data_table_name_.ptr(),
                                       param.tab_name_.length(), param.tab_name_.ptr(),
                                       type_str.length(), type_str.ptr(),
                                       dst_partition_id);
      if (OB_UNLIKELY(real_len < 0 || real_len > len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to print partition hint", K(ret), K(real_len), K(len), K(param));
      } else {
        where_string_.assign(buf, real_len);
        LOG_TRACE("Succeed to fill group by info", K(where_string_));
      }
    }
  }
  return ret;
}

int ObIndexStatsEstimator::fast_gather_index_stats(ObExecContext &ctx,
                                                   const ObTableStatParam &data_param,
                                                   const ObTableStatParam &index_param,
                                                   bool &is_fast_gather)
{
  int ret = OB_SUCCESS;
  ObOptStatManager &mgr = ObOptStatManager::get_instance();
  ObSEArray<int64_t, 4> gather_part_ids;
  ObSEArray<ObOptTableStat, 4> data_table_stats;
  ObSEArray<ObOptTableStat *, 4> index_table_stats;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  PartitionIdBlockMap partition_id_block_map;
  bool use_column_store = false;
  bool use_split_part = false;
  is_fast_gather = false;
  LOG_TRACE("begin to fast gather index stats", K(data_param), K(index_param));
  if (OB_FAIL(get_all_need_gather_partition_ids(data_param, index_param, gather_part_ids))) {
    LOG_WARN("failed to get all need gather partition ids", K(ret));
  } else if (gather_part_ids.empty()) {
    //do nothing
  } else if (OB_UNLIKELY(index_param.is_global_index_ && gather_part_ids.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(index_param.is_global_index_), K(gather_part_ids.count()));
  } else if (OB_FAIL(mgr.get_table_stat(data_param.tenant_id_, data_param.table_id_, gather_part_ids, data_table_stats))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (index_param.need_estimate_block_ &&
             OB_FAIL(partition_id_block_map.create(10000,
                                                   ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                   ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                   index_param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (index_param.need_estimate_block_ &&
             OB_FAIL(ObBasicStatsEstimator::estimate_block_count(ctx, index_param,
                                                                 partition_id_block_map,
                                                                 use_column_store,
                                                                 use_split_part))) {
    LOG_WARN("failed to estimate block count", K(ret));
  } else {
    bool is_continued = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_continued && i < data_table_stats.count(); ++i) {
      ObOptTableStat &data_tab_stat = data_table_stats.at(i);
      int64_t idx_part_id = -1;
      if (OB_FAIL(get_index_part_id(data_tab_stat.get_partition_id(),
                                    data_param,
                                    index_param,
                                    idx_part_id))) {
        LOG_WARN("failed to get index part id", K(ret));
      } else if (data_tab_stat.get_last_analyzed() > 0) {
        void *ptr = NULL;
        if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptTableStat)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(ptr));
        } else {
          ObOptTableStat *index_stat = new (ptr) ObOptTableStat();
          index_stat->set_table_id(index_param.table_id_);
          index_stat->set_partition_id(idx_part_id);
          index_stat->set_object_type(data_tab_stat.get_object_type());
          index_stat->set_stattype_locked(index_param.stattype_);
          index_stat->set_row_count(data_tab_stat.get_row_count());
          int64_t avg_len = 0;
          if (OB_FAIL(fast_get_index_avg_len(data_tab_stat.get_partition_id(),
                                             data_param,
                                             index_param,
                                             is_continued,
                                             avg_len))) {
            LOG_WARN("failed to fast get index avg len", K(ret));
          } else if (!is_continued) {
            //do nothing
          } else {
            index_stat->set_avg_row_size(avg_len);
            BlockNumStat *block_num_stat = NULL;
            if (OB_FAIL(partition_id_block_map.get_refactored(idx_part_id, block_num_stat))) {
              if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get refactored", K(ret));
              }
            } else if (OB_ISNULL(block_num_stat)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(ret), K(block_num_stat));
            } else {
              index_stat->set_macro_block_num(block_num_stat->tab_macro_cnt_);
              index_stat->set_micro_block_num(block_num_stat->tab_micro_cnt_);
              index_stat->set_sstable_row_count(block_num_stat->sstable_row_cnt_);
              index_stat->set_memtable_row_count(block_num_stat->memtable_row_cnt_);
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(index_table_stats.push_back(index_stat))) {
                LOG_WARN("failed to push back", K(ret));
              } else {
                LOG_TRACE("Succeed to fast gather index stat", K(index_table_stats));
              }
            }
          }
        }
      } else {
        is_continued = false;
      }
    }
    if (OB_SUCC(ret) && is_continued && !index_table_stats.empty()) {
      ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(ctx.get_sql_proxy(), index_param.tenant_id_))) {
          LOG_WARN("fail to start transaction", K(ret));
      } else if (OB_FAIL(mgr.update_table_stat(index_param.tenant_id_,
                                               trans.get_connection(),
                                               index_table_stats,
                                               index_param.is_index_stat_))) {
        LOG_WARN("failed to update table stats", K(ret));
      } else {
        is_fast_gather = true;
        LOG_TRACE("Succeed to fast gather index stats", K(data_param), K(index_param),
                                                        K(index_table_stats));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true))) {
          LOG_WARN("fail to commit transaction", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("fail to roll back transaction", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObIndexStatsEstimator::fast_get_index_avg_len(const int64_t data_partition_id,
                                                  const ObTableStatParam &data_param,
                                                  const ObTableStatParam &index_param,
                                                  bool &is_fast_get,
                                                  int64_t &avg_len)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> column_ids;
  avg_len = 0;
  is_fast_get = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_param.column_params_.count(); ++i) {
    bool is_found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < data_param.column_params_.count(); ++j) {
      if (0 == index_param.column_params_.at(i).column_name_.case_compare(
                                                    data_param.column_params_.at(j).column_name_)) {
        if (OB_FAIL(column_ids.push_back(data_param.column_params_.at(j).column_id_))) {
          LOG_WARN("failed to push back", K(ret));
        } else {
          is_found = true;
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!is_found)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(is_found), K(data_param), K(index_param));
    }
  }
  if (OB_SUCC(ret) && !column_ids.empty()) {
    bool is_all_valid = true;
    //need refine
    ObSEArray<ObOptColumnStatHandle, 4> col_stat_handles;
    ObSEArray<int64_t, 4> partition_ids;
    ObOptStatManager &mgr = ObOptStatManager::get_instance();
    if (OB_FAIL(partition_ids.push_back(data_partition_id))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(mgr.get_column_stat(data_param.tenant_id_,
                                           data_param.table_id_,
                                           partition_ids,
                                           column_ids,
                                           col_stat_handles))) {
      LOG_WARN("failed to get column stat", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && is_all_valid && i < col_stat_handles.count(); ++i) {
        if (OB_ISNULL(col_stat_handles.at(i).stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(col_stat_handles.at(i).stat_), K(ret));
        } else if (col_stat_handles.at(i).stat_->get_last_analyzed() > 0) {
          avg_len += col_stat_handles.at(i).stat_->get_avg_len();
        } else {
          is_all_valid = false;
        }
      }
      if (OB_SUCC(ret) && is_all_valid) {
        is_fast_get = true;
      }
      LOG_TRACE("fast get index avg len", K(avg_len), K(column_ids), K(is_fast_get),
                                          K(data_param), K(index_param));
    }
  }
  return ret;
}

int ObIndexStatsEstimator::get_all_need_gather_partition_ids(const ObTableStatParam &data_param,
                                                             const ObTableStatParam &index_param,
                                                             ObIArray<int64_t> &gather_part_ids)
{
  int ret = OB_SUCCESS;
  if (index_param.global_stat_param_.need_modify_) {
    if (OB_FAIL(gather_part_ids.push_back(data_param.global_part_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_SUCC(ret) && index_param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_param.part_infos_.count(); ++i) {
      if (OB_FAIL(gather_part_ids.push_back(data_param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < data_param.approx_part_infos_.count(); ++i) {
      if (OB_FAIL(gather_part_ids.push_back(data_param.approx_part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && index_param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(gather_part_ids.push_back(data_param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObIndexStatsEstimator::get_index_part_id(const int64_t data_tab_partition_id,
                                             const ObTableStatParam &data_param,
                                             const ObTableStatParam &index_param,
                                             int64_t &index_partition_id)
{
  int ret = OB_SUCCESS;
  if (data_tab_partition_id == data_param.global_part_id_) {
    index_partition_id = index_param.global_part_id_;
  } else if (OB_UNLIKELY(data_param.part_level_ != index_param.part_level_ ||
                         data_param.all_part_infos_.count() != index_param.all_part_infos_.count() ||
                         data_param.all_subpart_infos_.count() != index_param.all_subpart_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(data_param), K(index_param));
  } else {
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < data_param.all_part_infos_.count(); ++i) {
      if (data_tab_partition_id == data_param.all_part_infos_.at(i).part_id_) {
        index_partition_id = index_param.all_part_infos_.at(i).part_id_;
        is_found = true;
      }
    }
    if (!is_found) {
      for (int64_t i = 0; !is_found && i < data_param.all_subpart_infos_.count(); ++i) {
        if (data_tab_partition_id == data_param.all_subpart_infos_.at(i).part_id_) {
          index_partition_id = index_param.all_subpart_infos_.at(i).part_id_;
          is_found = true;
        }
      }
    }
    if (!is_found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(data_tab_partition_id), K(data_param), K(index_param));
    }
  }
  return ret;
}


} // end of common
} // end of oceanbase
