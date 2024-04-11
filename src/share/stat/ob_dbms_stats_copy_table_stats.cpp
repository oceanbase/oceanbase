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
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_dbms_stats_copy_table_stats.h"
#include "share/stat/ob_dbms_stats_history_manager.h"

int CopyTableStatHelper::copy_part_stat(ObIArray<ObOptTableStat *> &table_stats)
{
  int ret = OB_SUCCESS;
  ObOptTableStat *dst_part_stat = NULL;
  if (OB_ISNULL(src_part_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(dst_part_stat = OB_NEWx(ObOptTableStat, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mem", K(ret));
  } else {
    dst_part_stat->set_table_id(table_id_);
    dst_part_stat->set_partition_id(dst_part_id_);
    dst_part_stat->set_object_type(src_part_stat_->get_object_type());
    dst_part_stat->set_row_count(static_cast<int64_t>(src_part_stat_->get_row_count() * scale_factor_));
    dst_part_stat->set_avg_row_size(src_part_stat_->get_avg_row_size());
    dst_part_stat->set_sstable_row_count(static_cast<int64_t>(src_part_stat_->get_sstable_row_count() * scale_factor_));
    dst_part_stat->set_memtable_row_count(static_cast<int64_t>(src_part_stat_->get_memtable_row_count() * scale_factor_));
    dst_part_stat->set_data_size(src_part_stat_->get_data_size());
    dst_part_stat->set_macro_block_num(static_cast<int64_t>(src_part_stat_->get_macro_block_num() * scale_factor_));
    dst_part_stat->set_micro_block_num(static_cast<int64_t>(src_part_stat_->get_micro_block_num() * scale_factor_));
    dst_part_stat->set_sstable_avg_row_size(src_part_stat_->get_sstable_avg_row_size());
    dst_part_stat->set_memtable_avg_row_size(src_part_stat_->get_memtable_avg_row_size());
    dst_part_stat->set_data_version(src_part_stat_->get_data_version());
    dst_part_stat->set_last_analyzed(src_part_stat_->get_last_analyzed());
    // dst_part_stat->set_stattype_locked(src_part_stat_->get_stattype_locked()); no need to copy
    dst_part_stat->set_sample_size(src_part_stat_->get_sample_size());
    dst_part_stat->set_tablet_id(src_part_stat_->get_tablet_id());
    dst_part_stat->set_stat_expired_time(src_part_stat_->get_stat_expired_time());
    //TODO:set stale_stats to true for hash/list partitions
    if (dst_part_stat->get_row_count() < 0
        || dst_part_stat->get_sstable_row_count() < 0
        || dst_part_stat->get_memtable_row_count() < 0
        || dst_part_stat->get_macro_block_num() < 0
        || dst_part_stat->get_micro_block_num() < 0) {
      ret = OB_DATA_OUT_OF_RANGE;
      LOG_WARN("data of dst_part_stat out of range", K(ret), K(scale_factor_), KPC(dst_part_stat));
    } else if (OB_FAIL(table_stats.push_back(dst_part_stat))) {
      LOG_WARN("failed to push back table stats", K(ret));
    } else {
      LOG_TRACE("succeed to copy part stat", KPC(dst_part_stat), K(scale_factor_));
    }
  }
  return ret;
}

int CopyTableStatHelper::copy_col_stat(bool is_subpart,
                                       const ObIArray<ObOptColumnStatHandle> &col_handles,
                                       ObIArray<ObOptColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  bool is_src_part_equal_to_lower_bound = true;
  if (OB_ISNULL(src_part_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(check_range_part(is_subpart, col_handles, is_src_part_equal_to_lower_bound))) {
    LOG_WARN("failed to check range part", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < col_handles.count(); ++i) {
      const ObOptColumnStat *src_col_stat = col_handles.at(i).stat_;
      ObOptColumnStat *dst_col_stat = NULL;
      ObCopyPartInfo *dst_part_info = NULL;
      if (OB_ISNULL(src_col_stat) || OB_ISNULL(allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), KP(allocator_), KP(src_col_stat));
      } else if (src_col_stat->get_partition_id() != src_part_stat_->get_partition_id()) {
        LOG_TRACE("partition id not match, do nothing", K(src_col_stat->get_partition_id()),
                                                        K(src_part_stat_->get_partition_id()));
      } else if (0 == src_col_stat->get_last_analyzed()) {
        //do nothing
      } else {
        if (OB_ISNULL(dst_col_stat = ObOptColumnStat::malloc_new_column_stat(*allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate mem", K(ret));
        } else if ((!is_subpart && !is_contain(part_column_ids_, src_col_stat->get_column_id()))
                   || (is_subpart && !is_contain(subpart_column_ids_, src_col_stat->get_column_id()))) {
          if (OB_FAIL(dst_col_stat->deep_copy(*src_col_stat))) {
            LOG_WARN("failed to deep copy col stat", K(ret));
          } else {
            dst_col_stat->set_partition_id(dst_part_id_);
          }
          LOG_TRACE("not partition key, directly copy", K(part_column_ids_),
                                                        K(subpart_column_ids_),
                                                        K(src_col_stat->get_column_id()));
        } else if (OB_FAIL(dst_part_map_.get_refactored(src_col_stat->get_column_id(), dst_part_info))) {
          LOG_WARN("failed to get dst part info", K(ret), K(src_col_stat->get_column_id()));
        } else if (OB_ISNULL(dst_part_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          if (dst_part_info->is_normal_range_part_ && is_src_part_equal_to_lower_bound) {
            dst_part_info->min_res_type_ = CopyPrePartUpperBound;
            dst_part_info->max_res_type_ = CopyPrePartUpperBound;
          }
          dst_col_stat->set_table_id(table_id_);
          dst_col_stat->set_partition_id(dst_part_id_);
          dst_col_stat->set_column_id(src_col_stat->get_column_id());
          dst_col_stat->set_stat_level(src_col_stat->get_stat_level());
          dst_col_stat->set_version(src_col_stat->get_version());
          dst_col_stat->set_num_null(src_col_stat->get_num_null());
          dst_col_stat->set_num_not_null(src_part_stat_->get_row_count() - src_col_stat->get_num_null());
          dst_col_stat->set_num_distinct(src_col_stat->get_num_distinct());
          dst_col_stat->set_avg_len(src_col_stat->get_avg_len());
          dst_col_stat->set_last_analyzed(src_col_stat->get_last_analyzed());
          dst_col_stat->set_collation_type(src_col_stat->get_collation_type());
          dst_col_stat->set_total_col_len(src_col_stat->get_total_col_len());
          if (OB_FAIL(dst_col_stat->deep_copy_llc_bitmap(src_col_stat->get_llc_bitmap(),
                                                         src_col_stat->get_llc_bitmap_size()))) {
            LOG_WARN("failed to deep copy llc bitmap", K(ret));
          } else if (0 == dst_col_stat->get_num_distinct()) {
            dst_col_stat->get_min_value().set_null();
            dst_col_stat->get_max_value().set_null();
          } else if (OB_FAIL(copy_min_val(src_col_stat->get_min_value(),
                                          dst_col_stat->get_min_value(),
                                          dst_part_info))) {
            LOG_WARN("failed to update min val", K(ret));
          } else if (OB_FAIL(copy_max_val(src_col_stat->get_max_value(),
                                          dst_col_stat->get_max_value(),
                                          dst_part_info))) {
            LOG_WARN("failed to update min val", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (dst_part_info->is_hash_part_) {
            if (OB_FAIL(dst_col_stat->deep_copy_histogram(src_col_stat->get_histogram()))) {
              LOG_WARN("failed to deep copy histogram", K(ret));
            }
          } else {
            dst_col_stat->get_histogram().set_sample_size(src_part_stat_->get_row_count() - src_col_stat->get_num_null());
          }
        }
        if (0 != dst_col_stat->get_max_value().compare(dst_col_stat->get_min_value())
            && dst_col_stat->get_num_distinct() <= 1) {
          dst_col_stat->set_num_distinct(2);
        }
        LOG_TRACE("succeed to copy col stat", KPC(dst_col_stat), KPC(dst_part_info));
        if (OB_SUCC(ret) && OB_FAIL(column_stats.push_back(dst_col_stat))) {
          LOG_WARN("failed to push back column stat", K(ret));
        }
      }
    }
  }
  return ret;
}

int CopyTableStatHelper::check_range_part(bool is_subpart,
                                          const ObIArray<ObOptColumnStatHandle> &col_handles,
                                          bool &flag)
{
  int ret = OB_SUCCESS;
  flag = true;
  for (int64_t i = 0; OB_SUCC(ret) && flag && i < col_handles.count(); ++i) {
    const ObOptColumnStat *src_col_stat = col_handles.at(i).stat_;
    ObOptColumnStat *dst_col_stat = NULL;
    ObCopyPartInfo *dst_part_info = NULL;
    if (OB_ISNULL(allocator_) || OB_ISNULL(src_col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KP(allocator_), KP(src_col_stat));
    } else if (src_col_stat->get_partition_id() != src_part_stat_->get_partition_id()) {
    } else if ((!is_subpart && !is_contain(part_column_ids_, src_col_stat->get_column_id()))
               || (is_subpart && !is_contain(subpart_column_ids_, src_col_stat->get_column_id()))) {
      //do nothing
    } else {
      if (OB_FAIL(dst_part_map_.get_refactored(src_col_stat->get_column_id(), dst_part_info))) {
        LOG_WARN("failed to get dst part info", K(ret), K(src_col_stat->get_column_id()));
      } else if (OB_ISNULL(dst_part_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (0 != src_col_stat->get_min_value().compare(dst_part_info->src_part_lower_bound_) ||
                 0 != src_col_stat->get_max_value().compare(dst_part_info->src_part_lower_bound_) ||
                 1 != src_col_stat->get_num_distinct()) {
        flag = false;
      }
    }
  }
  return ret;
}

int CopyTableStatHelper::copy_min_val(const common::ObObj &src_min_val,
                                      common::ObObj &dst_min_val,
                                      ObCopyPartInfo *dst_part_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(dst_part_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(allocator_), KP(dst_part_info));
  } else {
    switch (dst_part_info->min_res_type_) {
      case CopyDstPartLowerBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->part_lower_bound_, dst_min_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopyDstPartUpperBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->part_upper_bound_, dst_min_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopyPrePartUpperBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->pre_part_upper_bound_, dst_min_val))) {
          LOG_WARN("failed to copy max val", K(ret));
        }
        break;
      }
      case CopySrcPartMinVal: {
        if (OB_FAIL(ob_write_obj(*allocator_, src_min_val, dst_min_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopySrcPartMaxVal:
      case CopyMaxSrcValDstBound:
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid argument", K(ret));
      }
    }
  }
  return ret;
}

int CopyTableStatHelper::copy_max_val(const common::ObObj &src_max_val,
                                      common::ObObj &dst_max_val,
                                      ObCopyPartInfo *dst_part_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(dst_part_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(allocator_), KP(dst_part_info));
  } else {
    switch (dst_part_info->max_res_type_) {
      case CopyDstPartLowerBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->part_lower_bound_, dst_max_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopyDstPartUpperBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->part_upper_bound_, dst_max_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopyPrePartUpperBound: {
        if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->pre_part_upper_bound_, dst_max_val))) {
          LOG_WARN("failed to copy max val", K(ret));
        }
        break;
      }
      case CopySrcPartMaxVal: {
        if (OB_FAIL(ob_write_obj(*allocator_, src_max_val, dst_max_val))) {
          LOG_WARN("failed to copy min val", K(ret));
        }
        break;
      }
      case CopyMaxSrcValDstBound: {
        if (src_max_val.compare(dst_part_info->part_upper_bound_)  > 0) {
          if (OB_FAIL(ob_write_obj(*allocator_, src_max_val, dst_max_val))) {
            LOG_WARN("failed to copy min val", K(ret));
          }
        } else {
          if (OB_FAIL(ob_write_obj(*allocator_, dst_part_info->part_upper_bound_, dst_max_val))) {
            LOG_WARN("failed to copy min val", K(ret));
          }
        }
        break;
      }
      case CopySrcPartMinVal:
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get invalid argument", K(ret));
      }
    }
  }
  return ret;
}

int CopyTableStatHelper::copy_part_col_stat(bool is_subpart,
                                           const ObIArray<ObOptColumnStatHandle> &col_handles,
                                           ObIArray<ObOptTableStat *> &table_stats,
                                           ObIArray<ObOptColumnStat *> &column_stats)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_part_stat(table_stats))) {
    LOG_WARN("failed to copy table stat", K(ret));
  } else if (OB_FAIL(copy_col_stat(is_subpart, col_handles, column_stats))) {
    LOG_WARN("failed to copy column stat", K(ret));
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::extract_partition_column_ids(CopyTableStatHelper &copy_stat_helper,
                                              const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> column_ids;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      bool is_part = false;
      if (table_schema->is_partitioned_table()) {
        if (OB_FAIL(table_schema->get_partition_key_info().is_rowkey_column(column_ids.at(i), is_part))) {
          LOG_WARN("check is partition key failed", K(ret), K(column_ids));
        } else if (is_part &&
                 OB_FAIL(add_var_to_array_no_dup(copy_stat_helper.part_column_ids_, column_ids.at(i)))) {
          LOG_WARN("failed to push back part column ids", K(ret));
        }
        if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
          bool is_subpart = false;
          if (OB_FAIL(table_schema->get_subpartition_key_info().is_rowkey_column(column_ids.at(i), is_subpart))) {
            LOG_WARN("check is partition key failed", K(ret), K(column_ids));
          } else if (is_subpart &&
                  OB_FAIL(add_var_to_array_no_dup(copy_stat_helper.subpart_column_ids_, column_ids.at(i)))) {
            LOG_WARN("failed to push back part column ids", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::check_parts_valid(sql::ObExecContext &ctx,
                                   const CopyTableStatHelper &helper,
                                   const ObTableStatParam &table_stat_param,
                                   ObCopyLevel &copy_level)
{
  int ret = OB_SUCCESS;
  PartInfo dummy_part;
  const ObString &srcpart_name = helper.srcpart_name_;
  const ObString &dstpart_name = helper.dstpart_name_;
  if (table_stat_param.part_infos_.empty()) {
    ret = OB_WRONG_PARTITION_NAME;
    LOG_WARN("The table is not partitioned, so the partition name is invalid", K(ret));
    LOG_ORACLE_USER_ERROR(OB_WRONG_PARTITION_NAME, srcpart_name.length(), srcpart_name.ptr());
  } else {
    bool is_src_onepart = ObDbmsStatsUtils::find_part(table_stat_param.all_part_infos_,
                                                      srcpart_name,
                                                      lib::is_oracle_mode(),
                                                      dummy_part);
    bool is_src_twopart = ObDbmsStatsUtils::find_part(table_stat_param.all_subpart_infos_,
                                                      srcpart_name,
                                                      lib::is_oracle_mode(),
                                                      dummy_part);
    bool is_dst_onepart = ObDbmsStatsUtils::find_part(table_stat_param.all_part_infos_,
                                                      dstpart_name,
                                                      lib::is_oracle_mode(),
                                                      dummy_part);
    bool is_dst_twopart = ObDbmsStatsUtils::find_part(table_stat_param.all_subpart_infos_,
                                                      dstpart_name,
                                                      lib::is_oracle_mode(),
                                                      dummy_part);
    if ((!is_src_onepart && !is_src_twopart) ||
        (!is_dst_onepart && !is_dst_twopart)) {
      ret = OB_WRONG_PARTITION_NAME;
      LOG_WARN("invalid src/dst part name", K(ret), K(is_src_onepart), K(is_src_twopart),
               K(is_dst_onepart), K(is_dst_twopart), K(srcpart_name), K(dstpart_name));
      if (!is_dst_onepart && !is_dst_twopart) {
        LOG_ORACLE_USER_ERROR(OB_WRONG_PARTITION_NAME, dstpart_name.length(), dstpart_name.ptr());
      } else {
        LOG_ORACLE_USER_ERROR(OB_WRONG_PARTITION_NAME, srcpart_name.length(), srcpart_name.ptr());
      }
    } else if ((is_src_twopart && is_dst_onepart)
               || (is_src_onepart && is_dst_twopart)) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("src partition and dst partition type are different", K(ret));
      LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "src partition and dst partition type are different");
    } else if (is_src_onepart && is_dst_onepart) {
      copy_level = CopyOnePartLevel;
    } else if (is_src_twopart && is_dst_twopart) {
      copy_level = CopyTwoPartLevel;
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::copy_tab_col_stats(sql::ObExecContext &ctx,
                                    ObTableStatParam &table_stat_param,
                                    CopyTableStatHelper &copy_stat_helper)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStatHandle, 4> tab_handles;
  ObSEArray<ObOptColumnStatHandle, 4> col_handles;
  ObSEArray<ObOptTableStat *, 1> table_stats;
  ObSEArray<ObOptColumnStat *, 4> column_stats;
  if (OB_FAIL(ObDbmsStatsUtils::get_current_opt_stats(table_stat_param,
                                                      tab_handles,
                                                      col_handles))) {
    LOG_WARN("failed to get history stat handles", K(ret));
  } else if (OB_FAIL(find_src_tab_stat(table_stat_param, tab_handles, copy_stat_helper.src_part_stat_))) {
    LOG_WARN("failed to find src table stat", K(ret));
  } else if (OB_ISNULL(copy_stat_helper.src_part_stat_)
             || 0 == copy_stat_helper.src_part_stat_->get_last_analyzed()) {
    // the src part does not have stat, do nothing
    LOG_WARN("src table stat is not analyzed", K(table_stat_param.part_infos_.at(0).part_id_));
  } else if (OB_FAIL(copy_stat_helper.copy_part_col_stat(table_stat_param.is_subpart_name_, col_handles, table_stats, column_stats))) {
    LOG_WARN("failed to copy table column stat", K(ret), KPC(copy_stat_helper.src_part_stat_));
  }
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    //begin trans
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), table_stat_param.tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(ctx, trans, table_stat_param, ObTimeUtility::current_time()))) {
      LOG_WARN("failed to backup opt stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(), table_stats, column_stats))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
    //end trans
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
  return ret;
}

int ObDbmsStatsCopyTableStats::find_src_tab_stat(const ObTableStatParam &table_stat_param,
                                   const ObIArray<ObOptTableStatHandle> &tab_handles,
                                   ObOptTableStat *&src_tab_stat)
{
  int ret = OB_SUCCESS;
  src_tab_stat = NULL;
  if (OB_UNLIKELY(table_stat_param.part_infos_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should only have one part info", K(ret), K(table_stat_param.part_infos_.count()));
  } else if (table_stat_param.is_subpart_name_
             && OB_UNLIKELY(table_stat_param.subpart_infos_.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should only have one subpart info", K(ret), K(table_stat_param.subpart_infos_.count()));
  } else {
    int64_t src_part_id = OB_INVALID_ID;
    if (table_stat_param.is_subpart_name_) {
      src_part_id = table_stat_param.subpart_infos_.at(0).part_id_;
    } else {
      src_part_id = table_stat_param.part_infos_.at(0).part_id_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && src_tab_stat == NULL && i < tab_handles.count(); ++i) {
      ObOptTableStat * cur_tab_stat = const_cast<ObOptTableStat *>(tab_handles.at(i).stat_);
      if (OB_ISNULL(cur_tab_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(cur_tab_stat), K(table_stat_param.part_infos_));
      } else if (src_part_id == cur_tab_stat->get_partition_id()) {
        src_tab_stat = cur_tab_stat;
        LOG_TRACE("succeed to find src part stat", KPC(src_tab_stat));
      }
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_dst_part_infos(const ObTableStatParam &table_stat_param,
                                    CopyTableStatHelper &helper,
                                    const ObTableSchema *table_schema,
                                    const ObCopyLevel copy_level,
                                    bool &is_found)
{
  int ret = OB_SUCCESS;
  is_found = false;
  bool found_dst = false;
  bool found_src = false;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!table_schema->is_partitioned_table()) {
    // do nothing
  } else if (OB_FAIL(helper.dst_part_map_.create(7, "DstPartBucket", "DstPartNode", helper.tenant_id_))) {
    LOG_WARN("failed to create dst part map", K(ret));
  } else if (copy_level == CopyOnePartLevel) {
    const ObPartition *part = NULL;
    const ObPartition *pre_part = NULL;
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartIterator iter(*table_schema, check_partition_mode);
    found_src = !table_schema->is_range_part();
    while (OB_SUCC(ret) && !is_found && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null partition", K(ret), K(part));
      } else if (ObCharset::case_insensitive_equal(helper.dstpart_name_, part->get_part_name())) {
        if (OB_FAIL(get_dst_part_info(table_schema, helper, part, pre_part, copy_level))) {
          LOG_WARN("failed to get dst part info", K(ret));
        } else {
          helper.dst_part_id_ = part->get_part_id();
          found_dst = true;
        }
      } else if (table_schema->is_range_part() &&
                 ObCharset::case_insensitive_equal(helper.srcpart_name_, part->get_part_name())) {
        if (OB_FAIL(get_src_part_info(table_schema, helper, part, pre_part, copy_level))) {
          LOG_WARN("failed to get src part info", K(ret));
        } else {
          found_src = true;
        }
      }
      pre_part = part;
      is_found = found_src && found_dst;
    }
  } else if (copy_level == CopyTwoPartLevel) {
    const ObPartition *part = NULL;
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    ObPartIterator iter(*table_schema, check_partition_mode);
    found_src = !table_schema->is_range_subpart();
    while (OB_SUCC(ret) && !is_found && OB_SUCC(iter.next(part))) {
      if (OB_ISNULL(part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part is null", KR(ret));
      } else {
        ObSubPartIterator sub_iter(*table_schema, *part, check_partition_mode);
        const ObSubPartition *subpart = NULL;
        const ObSubPartition *pre_subpart = NULL;
        while (OB_SUCC(ret) && !is_found && OB_SUCC(sub_iter.next(subpart))) {
          if (OB_ISNULL(subpart)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null subpartition", K(ret));
          } else if (ObCharset::case_insensitive_equal(helper.dstpart_name_, subpart->get_part_name())) {
            if (OB_FAIL(get_dst_part_info(table_schema, helper, subpart, pre_subpart, copy_level))) {
              LOG_WARN("failed to get dst part info", K(ret));
            } else {
              helper.dst_part_id_ = subpart->get_sub_part_id();
              found_dst = true;
            }
          } else if (table_schema->is_range_subpart() &&
                     ObCharset::case_insensitive_equal(helper.srcpart_name_, subpart->get_part_name())) {
            if (OB_FAIL(get_src_part_info(table_schema, helper, subpart, pre_subpart, copy_level))) {
              LOG_WARN("failed to get src part info", K(ret));
            } else {
              found_src = true;
            }
          }
          pre_subpart = subpart;
          is_found = found_src && found_dst;
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          break;
        }
      }
    }
  }
  ret = (ret == OB_ITER_END ? OB_SUCCESS : ret);
  return ret;
}

int ObDbmsStatsCopyTableStats::get_dst_part_info(const ObTableSchema *table_schema,
                                   CopyTableStatHelper &helper,
                                   const ObBasePartition *part,
                                   const ObBasePartition *pre_part,
                                   const ObCopyLevel copy_level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", KPC(part), K(table_schema));
  } else if ((copy_level == CopyOnePartLevel &&
              is_hash_like_part(table_schema->get_part_option().get_part_func_type())) ||
             (copy_level == CopyTwoPartLevel &&
              is_hash_like_part(table_schema->get_sub_part_option().get_part_func_type()))) {
    if (OB_FAIL(get_hash_or_default_part_info(table_schema,
                                              helper,
                                              copy_level,
                                              true))) {
      LOG_WARN("failed to get hash or list default part info", K(ret));
    }
  } else if ((copy_level == CopyOnePartLevel && table_schema->is_list_part())
             || (copy_level == CopyTwoPartLevel && table_schema->is_list_subpart())) {
    int64_t list_val_cnt = part->get_list_row_values().count();
    if (OB_UNLIKELY(list_val_cnt == 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid argument", K(ret), KPC(part));
    } else if (list_val_cnt == 1 && part->get_list_row_values().at(0).get_cell(0).is_max_value()) {
      // default part
      if (OB_FAIL(get_hash_or_default_part_info(table_schema,
                                                helper,
                                                copy_level,
                                                false))) {
        LOG_WARN("failed to get hash or list default part info", K(ret));
      }
    } else if (OB_FAIL(get_normal_list_part_info(table_schema,
                                                 helper,
                                                 part,
                                                 copy_level,
                                                 list_val_cnt))) {
      LOG_WARN("failed to get normal list part info", K(ret));
    }
  } else if ((copy_level == CopyOnePartLevel && table_schema->is_range_part())
             || (copy_level == CopyTwoPartLevel && table_schema->is_range_subpart())) {
    const ObRowkey &high_bound_val = part->get_high_bound_val();
    int64_t rowkey_cnt = high_bound_val.get_obj_cnt();
    if (OB_UNLIKELY(rowkey_cnt == 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid rowkey cnt");
    } else if (OB_FAIL(get_range_part_info(table_schema,
                                            helper,
                                            part,
                                            pre_part,
                                            copy_level,
                                            rowkey_cnt))) {
      LOG_WARN("failed to get range part info", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_hash_or_default_part_info(const ObTableSchema *table_schema,
                                               CopyTableStatHelper &helper,
                                               const ObCopyLevel copy_level,
                                               bool is_hash)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    int64_t sz = 0;
    if (copy_level == CopyOnePartLevel) {
      sz = table_schema->get_partition_key_info().get_size();
    } else if (copy_level == CopyTwoPartLevel) {
      sz = table_schema->get_subpartition_key_info().get_size();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sz; ++i) {
      ObCopyPartInfo *dst_part_info = NULL;
      if (OB_FAIL(get_copy_part_info(table_schema, copy_level, i, helper, dst_part_info))) {
        LOG_WARN("failed to get copy part info", K(ret));
      } else if (OB_ISNULL(dst_part_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        dst_part_info->min_res_type_ = CopySrcPartMinVal;
        dst_part_info->max_res_type_ = CopySrcPartMaxVal;
        dst_part_info->is_hash_part_ = is_hash;
      }
      LOG_TRACE("succeed to get hash or default list part info", KPC(dst_part_info), K(copy_level), K(i));
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_normal_list_part_info(const ObTableSchema *table_schema,
                                           CopyTableStatHelper &helper,
                                           const ObBasePartition *part,
                                           const ObCopyLevel copy_level,
                                           int64_t list_val_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(part));
  } else {
    const common::ObNewRow &low_row = part->get_list_row_values().at(0);
    const common::ObNewRow &high_row = part->get_list_row_values().at(list_val_cnt - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < low_row.get_count(); ++i) {
      ObCopyPartInfo *dst_part_info = NULL;
      if (OB_FAIL(get_copy_part_info(table_schema, copy_level, i, helper, dst_part_info))) {
        LOG_WARN("failed to get copy part info", K(ret));
      } else if (OB_ISNULL(dst_part_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ob_write_obj(*helper.allocator_,
                                      low_row.get_cell(i),
                                      dst_part_info->part_lower_bound_))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(*helper.allocator_,
                                      high_row.get_cell(i),
                                      dst_part_info->part_upper_bound_))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else {
        dst_part_info->min_res_type_ = CopyDstPartLowerBound;
        dst_part_info->max_res_type_ = CopyDstPartUpperBound;
      }
      LOG_TRACE("succeed to get normal list part info",
                KPC(dst_part_info), K(copy_level), K(i), K(part->get_list_row_values()));
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_range_part_info(const ObTableSchema *table_schema,
                                            CopyTableStatHelper &helper,
                                            const ObBasePartition *part,
                                            const ObBasePartition *pre_part,
                                            const ObCopyLevel copy_level,
                                            int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(table_schema), K(part));
  } else {
    const ObRowkey &high_bound_val = part->get_high_bound_val();
    const ObRowkey &low_bound_val = part->get_low_bound_val();

    ObCopyPartInfo *dst_part_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      if (OB_FAIL(get_copy_part_info(table_schema,
                                      copy_level, i,
                                      helper,
                                      dst_part_info))) {
        LOG_WARN("failed to get copy part info", K(ret));
      } else if (OB_ISNULL(dst_part_info)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (NULL != high_bound_val.get_obj_ptr()
                  && OB_FAIL(ob_write_obj(*helper.allocator_,
                                      high_bound_val.get_obj_ptr()[i],
                                      dst_part_info->part_upper_bound_))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (NULL != low_bound_val.get_obj_ptr()
                  && OB_FAIL(ob_write_obj(*helper.allocator_,
                                      low_bound_val.get_obj_ptr()[i],
                                      dst_part_info->part_lower_bound_))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (NULL != pre_part) {
        const ObRowkey &pre_high_bound_val = pre_part->get_high_bound_val();
        if (NULL != pre_high_bound_val.get_obj_ptr()
                  && OB_FAIL(ob_write_obj(*helper.allocator_,
                                      pre_high_bound_val.get_obj_ptr()[i],
                                      dst_part_info->pre_part_upper_bound_))) {
          LOG_WARN("failed to copy obj", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (NULL == pre_part) {
          // first range part
          dst_part_info->min_res_type_ = CopyDstPartUpperBound;
        } else {
          dst_part_info->min_res_type_ = CopyPrePartUpperBound;
          dst_part_info->is_normal_range_part_ = true;
        }
        if (high_bound_val.get_obj_ptr()[i].is_max_value()) {
          dst_part_info->max_res_type_ = CopyPrePartUpperBound;
        } else {
          dst_part_info->max_res_type_ = CopyDstPartUpperBound;
          if (i != 0) {
            dst_part_info->max_res_type_ = CopyMaxSrcValDstBound;
            if (pre_part != NULL
                && high_bound_val.get_obj_ptr()[i - 1] ==
                pre_part->get_high_bound_val().get_obj_ptr()[i - 1]) {
              dst_part_info->max_res_type_ = CopyDstPartUpperBound;
            }
          }
        }
      }
      LOG_TRACE("succeed to get range part info", KPC(dst_part_info),
              K(copy_level), K(i), KP(pre_part), K(high_bound_val), K(low_bound_val));
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_copy_part_info(const ObTableSchema *table_schema,
                                    const ObCopyLevel copy_level,
                                    const int64_t idx,
                                    CopyTableStatHelper &helper,
                                    ObCopyPartInfo *&dst_part_info)
{
  int ret = OB_SUCCESS;
  ObRowkeyColumn column;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (copy_level == CopyOnePartLevel &&
            OB_FAIL(table_schema->get_partition_key_info().get_column(idx, column))) {
    LOG_WARN("failed to get partition key info", K(ret));
  } else if (copy_level == CopyTwoPartLevel &&
            OB_FAIL(table_schema->get_subpartition_key_info().get_column(idx, column))) {
    LOG_WARN("failed to get subpartition key info", K(ret));
  } else if (OB_FAIL(helper.dst_part_map_.get_refactored(column.column_id_, dst_part_info))) {
    if (OB_UNLIKELY(ret != OB_HASH_NOT_EXIST)) {
      LOG_WARN("failed to get copy part info", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_ISNULL(dst_part_info = OB_NEWx(ObCopyPartInfo, helper.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate part info", K(ret));
      } else if (OB_FAIL(helper.dst_part_map_.set_refactored(column.column_id_, dst_part_info))) {
        LOG_WARN("failed to set part info", K(ret));
      } else {
        LOG_DEBUG("add dist part info", K(copy_level), K(column.column_id_));
      }
    }
  }
  return ret;
}

int ObDbmsStatsCopyTableStats::get_src_part_info(const ObTableSchema *table_schema,
                                   CopyTableStatHelper &helper,
                                   const ObBasePartition *part,
                                   const ObBasePartition *pre_part,
                                   const ObCopyLevel copy_level)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(part), K(table_schema));
  } else if (OB_UNLIKELY(!table_schema->is_partitioned_table()
            || (CopyOnePartLevel == copy_level && !table_schema->is_range_part())
            || (CopyTwoPartLevel == copy_level && !table_schema->is_range_subpart()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only range partitioned table need to get src part info", K(ret));
  } else {
    const ObRowkey &high_bound_val = part->get_high_bound_val();
    const ObRowkey &low_bound_val = NULL != pre_part ? pre_part->get_high_bound_val()
                                                       : part->get_low_bound_val();
    int64_t rowkey_cnt = high_bound_val.get_obj_cnt();
    if (OB_UNLIKELY(rowkey_cnt == 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid rowkey cnt", K(ret), K(high_bound_val));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
        ObCopyPartInfo *dst_part_info = NULL;
        if (OB_FAIL(get_copy_part_info(table_schema,
                                       copy_level, i,
                                       helper,
                                       dst_part_info))) {
          LOG_WARN("failed to get copy part info", K(ret));
        } else if (OB_ISNULL(dst_part_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else if (NULL != low_bound_val.get_obj_ptr()
                   && OB_FAIL(ob_write_obj(*helper.allocator_,
                                        low_bound_val.get_obj_ptr()[i],
                                        dst_part_info->src_part_lower_bound_))) {
          LOG_WARN("failed to copy obj", K(ret));
        }
        LOG_TRACE("succeed to get src part info", KPC(dst_part_info),
                K(copy_level), K(i), K(high_bound_val), K(low_bound_val));
      }
    }
  }
  return ret;
}
