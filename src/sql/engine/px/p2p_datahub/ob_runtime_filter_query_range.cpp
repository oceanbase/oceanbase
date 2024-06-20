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

#include "sql/engine/px/p2p_datahub/ob_runtime_filter_query_range.h"

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObPxRFStaticInfo, is_inited_, is_shared_, p2p_dh_ids_);
OB_SERIALIZE_MEMBER(ObPxQueryRangeInfo, table_id_, range_column_cnt_, prefix_col_idxs_,
                    prefix_col_obj_metas_);

int ObPxRFStaticInfo::init(const ObIArray<int64_t> &p2p_dh_ids, bool is_shared)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("twice init bf static info", K(ret));
  } else if (OB_FAIL(p2p_dh_ids_.assign(p2p_dh_ids))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    is_shared_ = is_shared;
    is_inited_ = true;
  }
  return ret;
}

int ObPxRFStaticInfo::assign(const ObPxRFStaticInfo &other)
{
  int ret = OB_SUCCESS;
  is_inited_ = other.is_inited_;
  is_shared_ = other.is_shared_;
  if (OB_FAIL(p2p_dh_ids_.assign(other.p2p_dh_ids_))) {
    LOG_WARN("failed to assign p2p_dh_ids_");
  }
  return ret;
}

int ObPxQueryRangeInfo::init(int64_t table_id, int64_t range_column_cnt,
                             const ObIArray<int64_t> &prefix_col_idxs,
                             const ObIArray<ObObjMeta> &prefix_col_obj_metas)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  range_column_cnt_ = range_column_cnt;
  if (OB_FAIL(prefix_col_idxs_.assign(prefix_col_idxs))) {
    LOG_WARN("failed to assign prefix_col_idxs");
  } else if (OB_FAIL(prefix_col_obj_metas_.assign(prefix_col_obj_metas))) {
    LOG_WARN("failed to assign prefix_col_obj_metas");
  }
  return ret;
}

int ObPxQueryRangeInfo::assign(const ObPxQueryRangeInfo &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  range_column_cnt_ = other.range_column_cnt_;
  if (OB_FAIL(prefix_col_idxs_.assign(other.prefix_col_idxs_))) {
    LOG_WARN("failed to assign prefix_col_idxs");
  } else if (OB_FAIL(prefix_col_obj_metas_.assign(other.prefix_col_obj_metas_))) {
    LOG_WARN("failed to assign prefix_col_obj_metas");
  }
  return ret;
}

} // namespace sql

} // namespace oceanbase
