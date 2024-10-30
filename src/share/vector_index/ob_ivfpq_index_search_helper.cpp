/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX COMMON

#include "share/vector_index/ob_ivfpq_index_search_helper.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace share
{
int ObIvfpqIndexSearchHelper::init(
  const int64_t tenant_id,
  const int64_t index_table_id,
  const int64_t ann_k,
  const int64_t probes,
  const int64_t segment,
  const ObTypeVector &qvector,
  const common::ObIArray<int32_t> &output_projector,
  common::sqlclient::ObISQLConnection *conn,
  sql::ObSQLSessionInfo *session) {
  int ret = ObIvfIndexSearchHelper::init(tenant_id, index_table_id, ann_k, probes, qvector, output_projector, conn, session);
  if (OB_SUCC(ret)) {
    pq_seg_ = segment;
    pq_centers_dis_array_.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
    is_inited_ = true;
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::get_vector_probes(const sql::ObSQLSessionInfo *session, uint64_t &probes) const {
  return session->get_vector_ivfpq_probes(probes);
}

int ObIvfpqIndexSearchHelper::set_partition_name(common::ObTabletID &tablet_id) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIvfIndexSearchHelper::set_partition_name(tablet_id))) {
    LOG_WARN("failed to set partition name", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObTenantPQCenterCache::set_partition_name(
      tenant_id_, index_table_id_, tablet_id, allocator_, partition_name_, partition_idx_))) {
    LOG_WARN("failed to set pq partition name", K(ret));
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::reset_pq_centers()
{
  int ret = OB_SUCCESS;
  pq_centers_ = nullptr; // reset
  if (OB_FAIL(MTL(ObTenantPQCenterCache *)->get(index_table_id_, partition_idx_, pq_centers_))) {
    LOG_WARN("failed to get center cache", K(ret), K(index_table_id_), K(partition_idx_));
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::get_pq_centers_by_cache()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pq_centers_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null centers", K(ret));
  } else {
    LOG_TRACE("start to get pq centers by cache");
    ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
    double distance = 0;
    int64_t offset = 0;
    for (int64_t i = 0; i < pq_centers_->get_seg(); ++i) {
      int64_t center_count = pq_centers_->get_seg_center_count()[i];
      for (int64_t j = 0; OB_SUCC(ret) && j < center_count; ++j) {
        const ObTypeVector &tmp_vec = pq_centers_->at(offset + j);
        if (OB_SUCC(ret) && OB_FAIL(update_pq_center_heap(j, i, tmp_vec, distance_type))) {
          LOG_WARN("failed to update pq center heap", K(ret), K(j), K(i), K(tmp_vec));
        }
      }
      offset += center_count;
    }
    LOG_TRACE("success to get centers by cache", K(ret), K(center_heap_.count()));
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::split_vector(ObArray<ObTypeVector*> &vector_list, const ObTypeVector &vector, const int64_t center_idx, const int64_t segment, const int64_t pq_dim) {
  int ret = OB_SUCCESS;
  ObTypeVector *new_vector = nullptr;
  if (OB_ISNULL(centers_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null cached centers", K(ret));
  } else if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, centers_->at(center_idx), new_vector))) {
    LOG_WARN("failed to alloc and copy vector", K(ret));
  } else if (OB_FAIL(vector_list.prepare_allocate(pq_seg_))) {
    LOG_WARN("failed to prepare allocate splitted vector", K(ret), K(pq_seg_));
  } else if (OB_FAIL(new_vector->subtract(vector))) {
    LOG_WARN("failed to subtract vector", K(ret), K(new_vector));
  } else if (segment <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected segment", K(ret), K(segment));
  } else if (vector_list.count() < segment) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector list count", K(ret), K(vector_list.count()), K(segment));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < segment; ++i) {
      if (OB_FAIL(new_vector->split(vector_list.at(i), pq_dim, i))) {
        LOG_WARN("failed to split vector", K(ret));
      }
    }
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::update_pq_center_heap(const int64_t pq_center_idx,
    const int64_t seg_idx, const ObTypeVector &tmp_vec, const ObVectorDistanceType &distance_type) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret) || pq_center_idx < 0 || seg_idx < 0 || seg_idx >= pq_seg_) {
    LOG_WARN("failed to get center idx", K(ret), K(pq_center_idx), K(seg_idx));
  } else {
    if (pq_centers_dis_array_.count() < 1 &&
        OB_FAIL(pq_centers_dis_array_.prepare_allocate(center_heap_.count()))) {
      LOG_WARN("failed to prepare allocate pq_centers_dis_array", K(ret), K(n_probes_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pq_centers_dis_array_.count(); ++i) {
      int64_t center_idx = center_heap_.at(i).center_idx_;
      ObArray<ObTypeVector*> splitted_vector;
      splitted_vector.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
      if (OB_FAIL(split_vector(splitted_vector, qvector_, center_idx, pq_seg_, tmp_vec.dims()))) {
        LOG_WARN("failed to split vector into PQ segments", K(ret), K(qvector_));
      }
      ObArray<ObArray<double>> &pq_center_dis_array = pq_centers_dis_array_.at(i);
      pq_center_dis_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
      if (OB_FAIL(ret)) {
      } else if (pq_center_dis_array.count() < 1 && OB_FAIL(pq_center_dis_array.prepare_allocate(pq_seg_))) {
        LOG_WARN("failed to prepare allocate pq center heap", K(ret), K(center_idx));
      } else {
        ObArray<double> &pq_dis_array = pq_center_dis_array.at(seg_idx);
        pq_dis_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
        ObTypeVector *qvector_seg = splitted_vector.at(seg_idx);
        double distance = 0;
        if (OB_ISNULL(qvector_seg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null qvector seg", K(ret), K(center_idx), K(seg_idx));
        } else {
          if (L2 == distance_type && OB_FAIL(tmp_vec.cal_l2_square(*qvector_seg, distance))) {
            LOG_WARN("failed to cal l2 distance", K(ret), K(tmp_vec), K(qvector_seg));
          } else if (L2 != distance_type && OB_FAIL(tmp_vec.cal_distance(distance_type, *qvector_seg, distance))) {
            LOG_WARN("failed to cal distance", K(ret), K(tmp_vec), K(qvector_seg));
          }
          if (pq_dis_array.count() <= pq_center_idx && OB_FAIL(pq_dis_array.prepare_allocate(pq_center_idx + 1))) {
            LOG_WARN("failed to prepare allocate pq distance array", K(ret), K(pq_center_idx));
          } else if (FALSE_IT(pq_dis_array.at(pq_center_idx) = distance)) {
            LOG_WARN("failed to store pq distance array", K(ret));
          }
        }
        LOG_TRACE("success to get center", K(ret), K(pq_center_idx), K(seg_idx), KPC(qvector_seg), K(tmp_vec), K(distance), K_(pq_centers_dis_array), K(center_idx), K_(qvector));
      }
    }
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::generate_pq_centers_by_storage(
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(output_exprs) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exprs", K(ret), K(output_exprs), K(eval_ctx));
  } else if (output_exprs->count() != 3) { // froce defence
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(output_exprs));
  } else {
    ObVectorDistanceType distance_type = distance_type_ == INNER_PRODUCT ? COSINE : distance_type_;
    ObTypeVector tmp_vec;
    ObArray<int64_t> idx_array;
    idx_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
    int64_t pq_center_idx = -1;
    int64_t seg_idx = -1;
    LOG_TRACE("start to get centers", K(distance_type), KPC(output_exprs));
    if (OB_FAIL(parse_output_expr(output_exprs, eval_ctx, idx_array, tmp_vec))) {
      LOG_WARN("failed to parse container expr", K(ret), KPC(output_exprs));
    } else if (idx_array.count() != 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx array", K(ret), K(idx_array));
    } else if (FALSE_IT(pq_center_idx = idx_array.at(0))) {
    } else if (FALSE_IT(seg_idx = idx_array.at(1))) {
    } else if (OB_FAIL(update_pq_center_heap(pq_center_idx, seg_idx, tmp_vec, distance_type))) {
      LOG_WARN("failed to update pq center heap", K(ret), K(pq_center_idx), K(seg_idx), K(tmp_vec));
    }
  }
  return ret;
}

int ObIvfpqIndexSearchHelper::get_rows_by_storage(
    const sql::ExprFixedArray *extra_access_exprs,
    const sql::ExprFixedArray *output_exprs,
    sql::ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(extra_access_exprs) || OB_ISNULL(output_exprs) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exprs", K(ret), K(extra_access_exprs), K(output_exprs), K(eval_ctx));
  } else if (extra_access_exprs->count() != 2) { // froce defence
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(extra_access_exprs));
  } else if (projector_cnt_ != output_exprs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs", K(ret), KPC(output_exprs));
  } else {
    HeapRowItem row_item;
    ObObj tmp_obj;
    ObTypeVector tmp_vec;
    ObArray<int64_t> idx_array;
    idx_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
    int64_t center_idx = -1;
    double distance = DBL_MAX;
    LOG_TRACE("start to get centers", KPC(extra_access_exprs));
    if (OB_FAIL(parse_output_expr(extra_access_exprs, eval_ctx, idx_array, tmp_vec))) {
      LOG_WARN("failed to parse expr", K(ret), KPC(extra_access_exprs));
    } else if (idx_array.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx array", K(ret), K(idx_array));
    } else if (FALSE_IT(center_idx = idx_array.at(0))) {}
    int index = -1;
    for (int i = 0; OB_SUCC(ret) && i < center_heap_.count(); ++i) {
      if (center_heap_.at(i).center_idx_ == center_idx) {
        index = i;
        distance = center_heap_.at(i).distance_;
        break;
      }
    }
    if (index < 0 || index >= pq_centers_dis_array_.count()) {
      LOG_WARN("center idx not found in pq_centers_dis_array, probably empty pq_centers", K(ret), K(center_idx), K(center_heap_));
    } else {
      ObArray<ObArray<double>> &pq_center_dis_array = pq_centers_dis_array_.at(index);
      pq_center_dis_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
      for (int i = 0; OB_SUCC(ret) && i < tmp_vec.dims(); ++i) {
        int seg_center_idx;
        if (FALSE_IT(seg_center_idx = tmp_vec.at(i))) {
          LOG_WARN("failed to get distance", K(ret), K(tmp_vec), K(qvector_));
        } else if (seg_center_idx < 0) {
          LOG_TRACE("skip zero vector segment", K(seg_center_idx));
          continue;  
        } else {
          ObArray<double> &pq_array = pq_center_dis_array.at(i);
          pq_array.set_attr(ObMemAttr(MTL_ID(), "PQSearch"));
          if (pq_array.count() <= seg_center_idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected pq heap nullptr", K(ret), K(pq_array));
          } else {
            distance += pq_array.at(seg_center_idx);
          }
        }
      }
    }
    if (row_heap_.count() < ann_k_ || distance < row_heap_.top().distance_) {
      if (OB_FAIL(alloc_row_item(output_exprs, distance, eval_ctx, row_item))) {
        LOG_WARN("failed to alloc row item", K(ret));
      }
    }
    if (FAILEDx(update_row_heap(row_item, distance))) {
      LOG_WARN("failed to update row heap");
    }
    LOG_TRACE("get row by storage", K(ret), K(tmp_vec), K(distance), K(center_idx), K_(ann_k), K(row_heap_.top().distance_), K(row_heap_.count()),
              K(center_idx), K(index), K_(center_heap), K_(pq_centers_dis_array));
  }
  return ret;
}

} // share
} // oceanbase
