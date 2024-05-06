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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_analyze_stmt.h"
#include "pl/sys_package/ob_dbms_stats.h"

namespace oceanbase
{
namespace sql
{

ObAnalyzeTableInfo::ObAnalyzeTableInfo()
  : database_name_(),
    database_id_(common::OB_INVALID_ID),
    table_name_(),
    table_id_(common::OB_INVALID_ID),
    part_level_(share::schema::PARTITION_LEVEL_ZERO),
    partition_name_(),
    partition_infos_(),
    subpartition_infos_(),
    column_params_(),
    column_group_params_(),
    ref_table_type_(share::schema::ObTableType::MAX_TABLE_TYPE),
    gather_subpart_hist_(false),
    is_sepcify_subpart_(false)
{
}

ObAnalyzeTableInfo::~ObAnalyzeTableInfo()
{
}

int ObAnalyzeTableInfo::assign(const ObAnalyzeTableInfo &other)
{
  int ret = OB_SUCCESS;
  database_name_ = other.database_name_;
  database_id_ = other.database_id_;
  table_name_ = other.table_name_;
  table_id_ = other.table_id_;
  part_level_ = other.part_level_;
  partition_name_ = other.partition_name_;
  ref_table_type_ = other.ref_table_type_;
  gather_subpart_hist_ = other.gather_subpart_hist_;
  is_sepcify_subpart_ = other.is_sepcify_subpart_;
  if (OB_FAIL(partition_infos_.assign(other.partition_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(subpartition_infos_.assign(other.subpartition_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(column_params_.assign(other.column_params_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(column_group_params_.assign(other.column_group_params_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(all_partition_infos_.assign(other.all_partition_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(all_subpartition_infos_.assign(other.all_subpartition_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {/*do nothing*/}
  return ret;
}

ObAnalyzeStmt::ObAnalyzeStmt()
  : ObStmt(NULL, stmt::T_ANALYZE),
    tenant_id_(common::OB_INVALID_ID),
    statistic_type_(InvalidStatistics),
    sample_info_(),
    parallel_degree_(1),
    is_drop_(false)
{
}

ObAnalyzeStmt::~ObAnalyzeStmt()
{
}

int ObAnalyzeTableInfo::set_column_params(const ObIArray<ObColumnStatParam> &col_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_params_.assign(col_params))) {
    LOG_WARN("failed to assign column params", K(ret));
  }
  return ret;
}

ObColumnStatParam *ObAnalyzeTableInfo::get_column_param(const uint64_t column_id)
{
  ObColumnStatParam *ret = NULL;
  for (int64_t i = 0; NULL == ret && i < column_params_.count(); ++i) {
    if (column_params_.at(i).column_id_ == column_id) {
      ret = &column_params_.at(i);
    }
  }
  return ret;
}

int ObAnalyzeStmt::fill_table_stat_params(ObExecContext &ctx, ObIArray<common::ObTableStatParam> &params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(params.prepare_allocate(tables_.count()))) {
    LOG_WARN("prepare allocate failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
    params.at(i).tenant_id_ = tenant_id_;
    params.at(i).sample_info_ = sample_info_;
    params.at(i).degree_ = parallel_degree_;
    if (OB_FAIL(tables_.at(i).fill_table_stat_param(ctx, params.at(i)))) {
      LOG_WARN("fill table stat param failed", K(ret));
    }
  }
  return ret;
}

int ObAnalyzeTableInfo::fill_table_stat_param(ObExecContext &ctx, common::ObTableStatParam &param)
{
  int ret = OB_SUCCESS;

  param.db_name_ = database_name_;
  param.db_id_ = database_id_;

  param.tab_name_ = table_name_;
  param.table_id_ = table_id_;
  param.ref_table_type_ = ref_table_type_;
  param.part_level_ = part_level_;

  param.part_name_ = partition_name_;

  OZ (param.part_infos_.assign(partition_infos_));
  OZ (param.subpart_infos_.assign(subpartition_infos_));
  OZ (param.column_params_.assign(column_params_));
  OZ (param.column_group_params_.assign(column_group_params_));
  OZ (pl::ObDbmsStats::set_param_global_part_id(ctx, param));
  OZ (param.all_part_infos_.assign(all_partition_infos_));
  OZ (param.all_subpart_infos_.assign(all_subpartition_infos_));

  if (OB_SUCC(ret)) {
    //analyze stmt default use granularity is based partition type(oracle 12c),maybe refine it later
    param.global_stat_param_.need_modify_ = partition_name_.empty();
    param.part_stat_param_.need_modify_ = !partition_infos_.empty() && !is_sepcify_subpart_;
    param.subpart_stat_param_.need_modify_ = !subpartition_infos_.empty() && (partition_name_.empty() || is_sepcify_subpart_);
    param.subpart_stat_param_.gather_histogram_ = gather_subpart_hist_;
    if (param.global_stat_param_.need_modify_ && param.part_stat_param_.need_modify_) {
      param.global_stat_param_.gather_approx_ = true;
    }
    if (param.part_stat_param_.need_modify_ && param.subpart_stat_param_.need_modify_) {
      param.part_stat_param_.can_use_approx_ = true;
    }
    bool no_estimate_block = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
    if (no_estimate_block) {
      param.need_estimate_block_ = false;
    }
  }

  LOG_TRACE("link bug", K(param));
  return ret;
}

int ObAnalyzeTableInfo::set_part_infos(ObIArray<PartInfo> &part_infos)
{
  return partition_infos_.assign(part_infos);
}

int ObAnalyzeTableInfo::set_subpart_infos(ObIArray<PartInfo> &subpart_infos)
{
  return subpartition_infos_.assign(subpart_infos);
}

int ObAnalyzeTableInfo::set_all_part_infos(ObIArray<PartInfo> &all_part_infos)
{
  return all_partition_infos_.assign(all_part_infos);
}

int ObAnalyzeTableInfo::set_all_subpart_infos(ObIArray<PartInfo> &all_subpart_infos)
{
  return all_subpartition_infos_.assign(all_subpart_infos);
}

int ObAnalyzeStmt::add_table(const ObString database_name,
                            const uint64_t database_id,
                            const ObString table_name,
                            const uint64_t table_id,
                            const share::schema::ObTableType table_type)
{
  int ret = OB_SUCCESS;
  ObAnalyzeTableInfo table;
  if (is_mysql_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_.count(); ++i) {
      if (tables_.at(i).get_table_id() == table_id
          && tables_.at(i).get_database_id() == database_id) {
        ret = OB_ERR_NONUNIQ_TABLE;
        LOG_USER_ERROR(OB_ERR_NONUNIQ_TABLE, table_name.length(), table_name.ptr());
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObAnalyzeTableInfo *table = NULL;
    if (OB_ISNULL(table = tables_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate table info from array error", K(ret));
    } else {
      table->set_database_name(database_name);
      table->set_database_id(database_id);
      table->set_table_name(table_name);
      table->set_table_id(table_id);
      table->set_ref_table_type(table_type);
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
