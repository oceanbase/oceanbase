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

#define USING_LOG_PREFIX SQL_EXE

#include "sql/executor/ob_transmit.h"
#include "sql/executor/ob_job.h"
#include "sql/executor/ob_shuffle_service.h"
#include "share/schema/ob_part_mgr_util.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER(ObTransmitInput);

OB_SERIALIZE_MEMBER_INHERIT(ObHashColumn, ObColumnInfo, expr_idx_, cmp_type_);

ObTransmit::ObTransmit(ObIAllocator& alloc)
    : ObSingleChildPhyOperator(alloc),
      split_task_count_(0),
      parallel_server_count_(0),
      server_parallel_thread_count_(0),
      repartition_type_(OB_REPARTITION_NO_REPARTITION),
      repartition_table_id_(OB_INVALID_ID),
      repart_columns_(alloc),
      repart_sub_columns_(alloc),
      repart_func_(alloc),
      repart_sub_func_(alloc),
      px_dop_(0),
      px_single_(false),
      dfo_id_(common::OB_INVALID_ID),
      px_id_(common::OB_INVALID_ID),
      dist_method_(ObPQDistributeMethod::MAX_VALUE),
      unmatch_row_dist_method_(ObPQDistributeMethod::MAX_VALUE),
      hash_dist_columns_(alloc),
      dist_exprs_(alloc),
      slave_mapping_type_(SlaveMappingType::SM_NONE),
      job_conf_(),
      has_lgi_(false),
      partition_id_idx_(common::OB_INVALID_INDEX)
{}

ObTransmit::~ObTransmit()
{}

void ObTransmit::reset()
{
  split_task_count_ = 0;
  parallel_server_count_ = 0;
  server_parallel_thread_count_ = 0;
  job_conf_.reset();
  repartition_type_ = OB_REPARTITION_NO_REPARTITION;
  repartition_table_id_ = OB_INVALID_ID;
  repart_columns_.reset();
  repart_sub_columns_.reset();
  px_dop_ = 0;
  px_single_ = false;
  dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  unmatch_row_dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  hash_dist_columns_.reset();
  dist_exprs_.reset();
  slave_mapping_type_ = SlaveMappingType::SM_NONE;
  partition_id_idx_ = common::OB_INVALID_INDEX;
  ObSingleChildPhyOperator::reset();
}

void ObTransmit::reuse()
{
  split_task_count_ = 0;
  parallel_server_count_ = 0;
  server_parallel_thread_count_ = 0;
  job_conf_.reset();
  repartition_type_ = OB_REPARTITION_NO_REPARTITION;
  repartition_table_id_ = OB_INVALID_ID;
  repart_columns_.reset();
  repart_sub_columns_.reset();
  px_dop_ = 0;
  px_single_ = false;
  dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  unmatch_row_dist_method_ = ObPQDistributeMethod::MAX_VALUE;
  hash_dist_columns_.reset();
  dist_exprs_.reset();
  slave_mapping_type_ = SlaveMappingType::SM_NONE;
  partition_id_idx_ = common::OB_INVALID_INDEX;
  ObSingleChildPhyOperator::reset();
}

int ObTransmit::add_repart_column(const int64_t column_index, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(column_index), K(ret));
  } else {
    ObTransmitRepartColumn repart_column;
    repart_column.index_ = column_index;
    repart_column.cs_type_ = cs_type;
    ret = repart_columns_.push_back(repart_column);
  }
  return ret;
}

int ObTransmit::add_repart_sub_column(const int64_t column_index, ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(column_index), K(ret));
  } else {
    ObTransmitRepartColumn repart_sub_column;
    repart_sub_column.index_ = column_index;
    repart_sub_column.cs_type_ = cs_type;
    ret = repart_sub_columns_.push_back(repart_sub_column);
  }
  return ret;
}

int ObTransmit::add_hash_dist_column(const bool is_expr, const int64_t idx, const ObObjMeta& cmp_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(idx), K(is_expr), K(ret));
  } else {
    ObHashColumn c;
    c.cmp_type_ = cmp_type.get_type();
    c.cs_type_ = cmp_type.get_collation_type();
    if (is_expr) {
      c.expr_idx_ = idx;
    } else {
      c.index_ = idx;
    }
    ret = hash_dist_columns_.push_back(c);
  }
  return ret;
}

int ObTransmit::set_dist_exprs(ObIArray<ObSqlExpression*>& exprs)
{
  return dist_exprs_.assign(exprs);
}

void ObTransmit::need_repart(bool& repart_part, bool& repart_subpart) const
{
  repart_part = repart_columns_.count() > 0;
  repart_subpart = repart_sub_columns_.count() > 0;
}

// ObTransmit::open did everything, no need to use get_next_row/close
int ObTransmit::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  UNUSED(ctx);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

int ObTransmit::add_compute(ObColumnExpression* expr)
{
  UNUSED(expr);
  return OB_NOT_SUPPORTED;
}

int ObTransmit::add_filter(ObSqlExpression* expr)
{
  UNUSED(expr);
  return OB_NOT_SUPPORTED;
}

int ObTransmit::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_op_ctx(ctx))) {
    LOG_WARN("init operator ctx failed", K(ret));
  } else if (OB_FAIL(handle_op_ctx(ctx))) {
    LOG_WARN("fail handle op ctx", K(ret));
  }
  return ret;
}

int ObTransmit::init_repart_columns(int64_t repart_count, int64_t repart_sub_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_size(repart_columns_, repart_count))) {
    LOG_WARN("failed to init repart_columns_", K(repart_count), K(repart_sub_count), K(ret));
  } else if (OB_FAIL(init_array_size(repart_sub_columns_, repart_sub_count))) {
    LOG_WARN("failed to init repart_sub_columns_", K(repart_count), K(repart_sub_count), K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransmit::init_hash_dist_columns(const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_array_size(hash_dist_columns_, count))) {
    LOG_WARN("init fixed array failed", K(ret), K(count));
  }
  return ret;
}

int ObTransmit::get_slice_idx_by_partition_ids(
    int64_t part_id, int64_t subpart_id, const share::schema::ObTableSchema& table_schema, int64_t& slice_idx)
{
  int ret = OB_SUCCESS;
  int64_t phy_partition_id = OB_INVALID_INDEX_INT64;
  if (ObShuffleService::NO_MATCH_PARTITION == part_id || ObShuffleService::NO_MATCH_PARTITION == subpart_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the code should not be here", K(ret), K(part_id), K(subpart_id));
  } else if (part_id < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part id should not less than 0", K(part_id), K(subpart_id), K(ret));
  } else {
    phy_partition_id = generate_phy_part_id(part_id, subpart_id, table_schema.get_part_level());
    if (OB_FAIL(
            share::schema::ObPartMgrUtils::get_partition_idx_by_id(table_schema, false, phy_partition_id, slice_idx))) {
      LOG_WARN("fail get parttion idx by phy part id", K(part_id), K(subpart_id), K(ret));
    }
  }
  LOG_DEBUG("get slice idx",
      K(part_id),
      K(subpart_id),
      K(phy_partition_id),
      K(slice_idx),
      K(table_schema.get_table_name_str()));
  return ret;
}

OB_DEF_SERIALIZE(ObTransmit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSingleChildPhyOperator::serialize(buf, buf_len, pos))) {
    LOG_WARN("single operator serialize fail", K(ret));
  } else {
    const int64_t dist_exprs_size = dist_exprs_.count();
    LST_DO_CODE(OB_UNIS_ENCODE,
        split_task_count_,
        parallel_server_count_,
        server_parallel_thread_count_,
        repartition_type_,
        repartition_table_id_,
        repart_columns_,
        repart_sub_columns_,
        repart_func_,
        repart_sub_func_,
        px_dop_,
        px_single_,
        dist_method_,
        unmatch_row_dist_method_,
        hash_dist_columns_,
        dist_exprs_size);
  }
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(e, dist_exprs_, OB_SUCC(ret))
    {
      if (OB_ISNULL(*e)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expression", K(ret));
      } else if (OB_FAIL((*e)->serialize(buf, buf_len, pos))) {
        LOG_WARN("expression serialize failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, dfo_id_, px_id_, partition_id_idx_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTransmit)
{
  int ret = OB_SUCCESS;
  int64_t len = ObSingleChildPhyOperator::get_serialize_size();
  const int64_t dist_exprs_size = dist_exprs_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      split_task_count_,
      parallel_server_count_,
      server_parallel_thread_count_,
      repartition_type_,
      repartition_table_id_,
      repart_columns_,
      repart_sub_columns_,
      repart_func_,
      repart_sub_func_,
      px_dop_,
      px_single_,
      dist_method_,
      unmatch_row_dist_method_,
      hash_dist_columns_,
      dist_exprs_size);
  FOREACH_CNT(e, dist_exprs_)
  {
    if (OB_ISNULL(*e)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL expression", K(ret));
    } else {
      len += (*e)->get_serialize_size();
    }
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, dfo_id_, px_id_, partition_id_idx_);
  return len;
}

OB_DEF_DESERIALIZE(ObTransmit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSingleChildPhyOperator::deserialize(buf, data_len, pos))) {
    LOG_WARN("single child phy operator deserialize failed", K(ret));
  } else {
    int64_t dist_exprs_size = 0;
    LST_DO_CODE(OB_UNIS_DECODE,
        split_task_count_,
        parallel_server_count_,
        server_parallel_thread_count_,
        repartition_type_,
        repartition_table_id_,
        repart_columns_,
        repart_sub_columns_,
        repart_func_,
        repart_sub_func_,
        px_dop_,
        px_single_,
        dist_method_,
        unmatch_row_dist_method_,
        hash_dist_columns_,
        dist_exprs_size);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_array_size(dist_exprs_, dist_exprs_size))) {
      LOG_WARN("fixed array init failed", K(ret));
    } else {
      for (int64_t i = 0; i < dist_exprs_size && OB_SUCC(ret); i++) {
        ObSqlExpression* expr = NULL;
        if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(my_phy_plan_, expr))) {
          LOG_WARN("make sql expression failed", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL expr returned", K(ret));
        } else if (OB_FAIL(expr->deserialize(buf, data_len, pos))) {
          LOG_WARN("expression deserialize failed", K(ret));
        } else if (OB_FAIL(dist_exprs_.push_back(expr))) {
          LOG_WARN("array push back failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, dfo_id_, px_id_, partition_id_idx_);
    }
  }
  return ret;
}

int64_t ObTransmit::to_string_kv(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(repartition_type), K_(repartition_table_id), K_(px_dop), K_(px_single), K_(dist_method));
  return pos;
}

}  // namespace sql
}  // namespace oceanbase
