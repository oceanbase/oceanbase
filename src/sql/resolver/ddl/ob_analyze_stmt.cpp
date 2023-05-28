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

ObAnalyzeStmt::ObAnalyzeStmt()
  : ObStmt(NULL, stmt::T_ANALYZE),
    tenant_id_(common::OB_INVALID_ID),
    database_name_(),
    database_id_(common::OB_INVALID_ID),
    table_name_(),
    table_id_(common::OB_INVALID_ID),
    part_level_(share::schema::PARTITION_LEVEL_ZERO),
    total_part_cnt_(0),
    partition_name_(),
    partition_infos_(),
    subpartition_infos_(),
    column_params_(),
    statistic_type_(InvalidStatistics),
    sample_info_(),
    parallel_degree_(1),
    is_drop_(false),
    part_ids_(),
    subpart_ids_(),
    ref_table_type_(share::schema::ObTableType::MAX_TABLE_TYPE),
    gather_subpart_hist_(false)
{
}

ObAnalyzeStmt::~ObAnalyzeStmt()
{
}

int ObAnalyzeStmt::set_column_params(const ObIArray<ObColumnStatParam> &col_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(column_params_.assign(col_params))) {
    LOG_WARN("failed to assign column params", K(ret));
  }
  return ret;
}

ObColumnStatParam *ObAnalyzeStmt::get_column_param(const uint64_t column_id)
{
  ObColumnStatParam *ret = NULL;
  for (int64_t i = 0; NULL == ret && i < column_params_.count(); ++i) {
    if (column_params_.at(i).column_id_ == column_id) {
      ret = &column_params_.at(i);
    }
  }
  return ret;
}

int ObAnalyzeStmt::fill_table_stat_param(ObExecContext &ctx, common::ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  param.tenant_id_ = tenant_id_;

  param.db_name_ = database_name_;
  param.db_id_ = database_id_;

  param.tab_name_ = table_name_;
  param.table_id_ = table_id_;
  param.ref_table_type_ = ref_table_type_;
  param.part_level_ = part_level_;
  param.total_part_cnt_ = total_part_cnt_;

  param.part_name_ = partition_name_;

  OZ (param.part_infos_.assign(partition_infos_));
  OZ (param.subpart_infos_.assign(subpartition_infos_));
  OZ (param.column_params_.assign(column_params_));
  OZ (param.part_ids_.assign(part_ids_));
  OZ (param.subpart_ids_.assign(subpart_ids_));
  OZ (pl::ObDbmsStats::set_param_global_part_id(ctx, param));
  OZ (param.all_part_infos_.assign(all_partition_infos_));
  OZ (param.all_subpart_infos_.assign(all_subpartition_infos_));

  if (OB_SUCC(ret)) {
    param.sample_info_ = sample_info_;
    param.degree_ = parallel_degree_;
    //analyze stmt default use granularity is based partition type(oracle 12c),maybe refine it later
    param.global_stat_param_.need_modify_ = partition_name_.empty();
    param.part_stat_param_.need_modify_ = !partition_infos_.empty();
    param.subpart_stat_param_.need_modify_ = !subpartition_infos_.empty();
    param.subpart_stat_param_.gather_histogram_ = gather_subpart_hist_;
  }

  LOG_TRACE("link bug", K(param));
  return ret;
}

int ObAnalyzeStmt::set_part_infos(ObIArray<PartInfo> &part_infos)
{
  return partition_infos_.assign(part_infos);
}

int ObAnalyzeStmt::set_subpart_infos(ObIArray<PartInfo> &subpart_infos)
{
  return subpartition_infos_.assign(subpart_infos);
}

int ObAnalyzeStmt::set_part_ids(ObIArray<int64_t> &part_ids)
{
  return part_ids_.assign(part_ids);
}

int ObAnalyzeStmt::set_subpart_ids(ObIArray<int64_t> &subpart_ids)
{
  return subpart_ids_.assign(subpart_ids);
}

int ObAnalyzeStmt::set_all_part_infos(ObIArray<PartInfo> &all_part_infos)
{
  return all_partition_infos_.assign(all_part_infos);
}

int ObAnalyzeStmt::set_all_subpart_infos(ObIArray<PartInfo> &all_subpart_infos)
{
  return all_subpartition_infos_.assign(all_subpart_infos);
}

} /* namespace sql */
} /* namespace oceanbase */
