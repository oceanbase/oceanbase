/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX RS

#include "share/vector_index/ob_hnsw_index_builder.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/object/ob_obj_cast.h"
#include "share/vector_index/ob_hnsw_index_cache.h"
#include <queue>
#include <random>
#include <set>

namespace oceanbase {
namespace share {

int ObHNSWElement::set_level(int64_t level, ObIAllocator *allocator) {
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i <= level; ++i) {
    OB_ASSERT(neighbors_per_level_[i] == nullptr);
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator->alloc(sizeof(ObHNSWLevelNeighbors)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObHNSWLevelNeighbors)));
    } else {
      neighbors_per_level_[i] = new (buf) ObHNSWLevelNeighbors();
    }
  }
  return ret;
}

int ObHNSWElement::alloc_and_deep_copy_cpool_ptr(ObHNSWCPoolPtr &dest_ptr,
                                                 const ObHNSWCPoolPtr &src_ptr,
                                                 ObIAllocator *allocator) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_ptr.objs_)) {
    void *new_ptr = nullptr;
    ObObj *new_objs = nullptr;
    const char *buf_ptr = src_ptr.buf_str_.ptr();
    int64_t buf_size = src_ptr.buf_str_.length();
    int64_t obj_count = src_ptr.n_objs_;
    if (OB_ISNULL(new_ptr = allocator->alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(buf_size));
    } else if (OB_ISNULL(new_objs = static_cast<ObObj *>(
                             allocator->alloc(obj_count * sizeof(ObObj))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(obj_count * sizeof(ObObj)));
    } else {
      dest_ptr.n_objs_ = obj_count;
      dest_ptr.objs_ = new_objs;
      dest_ptr.buf_str_.assign_ptr(static_cast<char *>(new_ptr), buf_size);
    }
  } else if (dest_ptr.buf_str_.length() < src_ptr.buf_str_.length()) {
    void *tmp_ptr = dest_ptr.buf_str_.ptr();
    int64_t old_size = dest_ptr.buf_str_.length();
    int64_t buf_size = src_ptr.buf_str_.length();
    if (OB_ISNULL(tmp_ptr = allocator->realloc(tmp_ptr, old_size, buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to realloc memory", K(ret), K(buf_size));
    } else {
      dest_ptr.buf_str_.assign_ptr(static_cast<char *>(tmp_ptr), buf_size);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    int64_t obj_count = src_ptr.n_objs_;
    int64_t buf_size = src_ptr.buf_str_.length();
    OB_ASSERT(dest_ptr.n_objs_ == obj_count &&
              dest_ptr.buf_str_.length() == buf_size);
    ObObj *other_objs = src_ptr.objs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < obj_count; ++i) {
      if (!other_objs[i].need_deep_copy()) {
        other_objs[i].copy_value_or_obj(dest_ptr.objs_[i], true);
        MEMCPY(dest_ptr.buf_str_.ptr() + pos, (&(other_objs[i].v_)),
               sizeof(ObObjValue));
        pos += sizeof(ObObjValue);
      } else if (OB_FAIL(dest_ptr.objs_[i].deep_copy(
                     other_objs[i], dest_ptr.buf_str_.ptr(), buf_size, pos))) {
        LOG_WARN("fail to deep copy obj value", K(ret), K(pos), K(buf_size));
      }
    }
  }
  return ret;
}

int ObHNSWElement::deep_copy(const ObHNSWElement &other,
                             ObIAllocator *allocator, int skip_cnt,
                             int64_t cache_epoch) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy element is not valid", K(ret));
  } else if (!is_valid()) {
    // first time
    // vector mem alloc
    void *vector_data = nullptr;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(vector_data = allocator->alloc(other.vector_.dims() *
                                                   sizeof(float)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret),
                 K(other.vector_.dims() * sizeof(float)));
      } else {
        vector_.assign(static_cast<float *>(vector_data), other.vector_.dims());
      }
    }
  }
  if (OB_SUCC(ret) && ObHNSWElement::alloc_and_deep_copy_cpool_ptr(
                          pk_ptr_, other.pk_ptr_, allocator)) {
    LOG_WARN("fail to alloc and deep copy cpool ptr", K(ret));
  }

  if (OB_SUCC(ret)) {
    skip_cnt_ = skip_cnt;
    cache_epoch_ = cache_epoch;
  }

  if (OB_SUCC(ret)) {
    vector_.assign(other.vector_);
    MEMSET(neighbors_per_level_, 0, sizeof(neighbors_per_level_));
    for (int64_t i = 0; OB_SUCC(ret) && i < ObHNSWOption::HNSW_MAX_LEVEL + 1 &&
                        OB_NOT_NULL(other.neighbors_per_level_[i]);
         ++i) {
      if (OB_ISNULL(neighbors_per_level_[i])) {
        void *npl_ptr = nullptr;
        if (OB_ISNULL(npl_ptr =
                          allocator->alloc(sizeof(ObHNSWLevelNeighbors)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret),
                   K(sizeof(ObHNSWLevelNeighbors)));
        } else if (OB_FALSE_IT(neighbors_per_level_[i] =
                                   new (npl_ptr) ObHNSWLevelNeighbors())) {
        }
      }

      for (int64_t nb_idx = 0;
           OB_SUCC(ret) && nb_idx < other.neighbors_per_level_[i]->n_nb_;
           ++nb_idx) {
        // copy & alloc ObHNSWCPoolPtr
        ObHNSWCPoolPtr tmp_ptr;
        const ObHNSWNeighbor &nb = other.neighbors_per_level_[i]->nbs_[nb_idx];
        if (OB_FAIL(ObHNSWElement::alloc_and_deep_copy_cpool_ptr(
                tmp_ptr, nb.pk_ptr_, allocator))) {
          LOG_WARN("fail to alloc and deep copy cpool ptr", K(ret));
        } else {
          neighbors_per_level_[i]->set_lv_neighbor(nb_idx, nb.distance_,
                                                   tmp_ptr);
        }
      }
    }
  }
  return ret;
}

//
//  **************** ObHNSWIndexBuilder ********************
//
int ObHNSWIndexBuilder::init(bool insert_mode) {
  int ret = OB_SUCCESS;
  srand((unsigned)time(NULL));
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHNSWIndexBuilder is inited", K(ret));
  } else if (OB_FAIL(sr_cache_.init(insert_mode))) {
    LOG_WARN("fail to init ObHNSWIndexSwitchableRingedCache", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObHNSWIndexBuilder::reset() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sr_cache_.prune_out_ring_cache(trans_))) {
    LOG_WARN("fail to prune out ring cache", K(ret));
  }
  return ret;
}

// TODO: shk
// Maybe adding a hidden increment pk for vector table is better.
int ObHNSWIndexBuilder::copy_objs_to_const_pool(sqlclient::ObMySQLResult *res,
                                                int64_t start_idx,
                                                int64_t end_idx,
                                                ObHNSWCPoolPtr &pk_ptr) {
  int ret = OB_SUCCESS;
  pk_ptr.objs_ = nullptr;
  ObObj tmp_obj;
  int64_t objs_count = end_idx - start_idx;
  int64_t buf_size = 0;
  char *buf = nullptr;
  if (OB_ISNULL(pk_ptr.objs_ = static_cast<ObObj *>(
                    allocator_->alloc(objs_count * sizeof(ObObj))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObObj array", K(ret), K(pkey_count_));
  } else {
    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      if (OB_FAIL(res->get_obj(i, tmp_obj))) {
        LOG_WARN("get obj failed", K(ret), K(i));
      } else if (tmp_obj.need_deep_copy()) {
        buf_size += tmp_obj.get_deep_copy_size();
      } else {
        tmp_obj.copy_value_or_obj(pk_ptr.objs_[i - start_idx], true);
        buf_size += sizeof(ObObjValue);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(buf = static_cast<char *>(
                        allocator_->alloc(buf_size * sizeof(char))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buffer", K(ret), K(buf_size));
      } else {
        int64_t pos = 0;
        for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
          if (OB_FAIL(res->get_obj(i, tmp_obj))) {
            LOG_WARN("get obj failed", K(ret), K(i));
          } else if (!tmp_obj.need_deep_copy()) {
            MEMCPY(buf + pos, (&(tmp_obj.v_)), sizeof(ObObjValue));
            pos += sizeof(ObObjValue);
          } else if (OB_FAIL(pk_ptr.objs_[i - start_idx].deep_copy(
                         tmp_obj, buf, buf_size, pos))) {
            LOG_WARN("fail to deep copy obj value", K(ret), K(pos),
                     K(buf_size));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      pk_ptr.n_objs_ = objs_count;
      pk_ptr.buf_str_.assign_ptr(buf, buf_size);
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::borrow_vector_from_mysql_res(
    sqlclient::ObMySQLResult *res, int64_t idx, common::ObTypeVector &vector) {
  int ret = OB_SUCCESS;
  ObObj tmp_obj;
  if (OB_FAIL(res->get_obj(idx, tmp_obj))) {
    LOG_WARN("get obj failed", K(ret), K(idx));
  } else if (OB_UNLIKELY(ObVectorType != tmp_obj.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a vector", K(ret), K(tmp_obj.get_type()));
  } else {
    vector.assign(reinterpret_cast<float *>(tmp_obj.get_deep_copy_obj_ptr()),
                  tmp_obj.get_val_len() / sizeof(float));
  }
  return ret;
}

int ObHNSWIndexBuilder::copy_vector_to_const_pool(
    sqlclient::ObMySQLResult *res, int64_t idx, common::ObTypeVector &vector) {
  int ret = OB_SUCCESS;
  ObObj tmp_obj;
  char *buf = nullptr;

  if (OB_FAIL(res->get_obj(idx, tmp_obj))) {
    LOG_WARN("get obj failed", K(ret), K(idx));
  } else if (OB_UNLIKELY(ObVectorType != tmp_obj.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a vector", K(ret), K(tmp_obj.get_type()));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(
                           tmp_obj.get_val_len() * sizeof(char))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buffer", K(ret), K(tmp_obj.get_val_len()));
  } else {
    MEMCPY(buf, tmp_obj.get_deep_copy_obj_ptr(), tmp_obj.get_val_len());
    vector.assign(reinterpret_cast<float *>(buf),
                  tmp_obj.get_val_len() / sizeof(float));
    if (OB_UNLIKELY(-1 == vector_dim_)) {
      vector_dim_ = vector.dims();
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::hnsw_get_or_alloc_lv_neighbors(
    ObHNSWElement *element, int64_t e_lv, ObHNSWLevelNeighbors *&lv_neighbors) {
  int ret = OB_SUCCESS;
  void *lv_neighbors_buf = nullptr;
  if (OB_NOT_NULL(element->neighbors_per_level_[e_lv])) {
    lv_neighbors = element->neighbors_per_level_[e_lv];
  } else if (OB_ISNULL(lv_neighbors_buf =
                           allocator_->alloc(sizeof(ObHNSWLevelNeighbors)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObHNSWLevelNeighbors", K(ret));
  } else if (OB_FALSE_IT(element->neighbors_per_level_[e_lv] =
                             new (lv_neighbors_buf) ObHNSWLevelNeighbors())) {
  } else {
    lv_neighbors = element->neighbors_per_level_[e_lv];
  }
  return ret;
}

int ObHNSWIndexBuilder::check_element_closerV2(
    ObHNSWPruneCandidate cd, common::ObArray<ObHNSWPruneCandidate> &r,
    int64_t lv, bool &is_closer) {
  int ret = OB_SUCCESS;
  is_closer = true;
  OB_ASSERT(cd.ele_ != nullptr);
  for (int64_t i = 0; OB_SUCC(ret) && i < r.count(); ++i) {
    OB_ASSERT(r.at(i).ele_ != nullptr);
    double distance = 0.0;
    if (OB_FAIL(get_vector_distance_with_cache(cd.ele_, r.at(i).ele_, lv,
                                               distance))) {
      LOG_WARN("fail to calc distance", K(ret));
    } else if (distance <= cd.distance_) {
      is_closer = false;
      break;
    }
  }
  return ret;
}

// for newly added vector
int ObHNSWIndexBuilder::hnsw_select_neighbors_simple(
    common::ObArray<ObHNSWCandidate> &cds, int64_t m,
    common::ObArray<ObHNSWCandidate> &selected_neighbors,
    int64_t &min_skip_cnt) {
  int ret = OB_SUCCESS;
  if (cds.count() > m) {
    std::priority_queue<ObHNSWCandidate, std::vector<ObHNSWCandidate>,
                        std::greater<ObHNSWCandidate>>
        near_cds;

    for (int64_t i = 0; OB_SUCC(ret) && i < cds.count(); ++i) {
      near_cds.push(cds.at(i));
    }

    while (OB_SUCC(ret) && !near_cds.empty() &&
           selected_neighbors.count() < m) {
      ObHNSWCandidate n_cd = near_cds.top();
      min_skip_cnt = OB_MIN(min_skip_cnt, n_cd.skip_cnt_);
      near_cds.pop();
      if (OB_FAIL(selected_neighbors.push_back(n_cd))) {
        LOG_WARN("push element to cds failed", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cds.count(); ++i) {
      ObHNSWCandidate n_cd = cds.at(i);
      min_skip_cnt = OB_MIN(min_skip_cnt, n_cd.skip_cnt_);
      if (OB_FAIL(selected_neighbors.push_back(n_cd))) {
        LOG_WARN("fail to push back ObHNSWCandidate", K(ret));
      }
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::hnsw_search_layer(common::ObTypeVector &q,
                                          common::ObArray<ObHNSWCandidate> &ep,
                                          int64_t ef, int64_t lv) {
  int ret = OB_SUCCESS;
  std::priority_queue<ObHNSWCandidate, std::vector<ObHNSWCandidate>,
                      std::greater<ObHNSWCandidate>>
      near_cds;
  std::priority_queue<ObHNSWCandidate, std::vector<ObHNSWCandidate>,
                      std::less<ObHNSWCandidate>>
      far_cds;
  std::set<ObHNSWElement *> visit_ele;
  int cd_cnt = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < ep.count(); ++i) {
    ObHNSWCandidate &cd = ep.at(i);
    visit_ele.insert(cd.element_);
    near_cds.push(cd);
    far_cds.push(cd);
    cd_cnt++;
  }

  while (!near_cds.empty() && OB_SUCC(ret)) {
    ObHNSWCandidate n_cd = near_cds.top();
    near_cds.pop();
    ObHNSWCandidate f_cd = far_cds.top();

    if (n_cd.candi_distance_ > f_cd.candi_distance_) {
      break;
    }
    // get neighbors of n_cd
    ObHNSWLevelNeighbors *neighbors = nullptr;
    if (OB_FAIL(sr_cache_.get_neighbors(n_cd.element_, lv, neighbors))) {
      LOG_WARN("fail to get neighbors", K(ret), K(lv));
    }

    ObHNSWElement *neighbor_e = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < neighbors->n_nb_; ++i) {
      if (OB_FAIL(sr_cache_.get_element_in_neighbor(
              trans_, &(neighbors->nbs_[i]), n_cd.element_->skip_cnt_,
              neighbor_e))) {
        LOG_WARN("fail to get element in neighbor", K(ret), K(i));
      } else if (visit_ele.find(neighbor_e) != visit_ele.end()) {
        // visited
        continue;
      } else {
        visit_ele.insert(neighbor_e);
        f_cd = far_cds.top();
        double dis_neighbor_e_to_q = 0.0;
        if (OB_FAIL(get_vector_distance(q, neighbor_e->vector_,
                                        dis_neighbor_e_to_q))) {
          LOG_WARN("fail to calc distance", K(ret));
        } else if (dis_neighbor_e_to_q < f_cd.candi_distance_ || cd_cnt < ef) {
          near_cds.push(ObHNSWCandidate{neighbor_e, dis_neighbor_e_to_q,
                                        n_cd.skip_cnt_ + 1});
          far_cds.push(ObHNSWCandidate{neighbor_e, dis_neighbor_e_to_q,
                                       n_cd.skip_cnt_ + 1});
          cd_cnt++;
          if (cd_cnt > ef) {
            far_cds.pop();
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ep.reset();
    while (!far_cds.empty() && OB_SUCC(ret)) {
      ObHNSWCandidate f_cd = far_cds.top();
      far_cds.pop();
      if (OB_FAIL(ep.push_back(f_cd))) {
        LOG_WARN("fail to add new candidate", K(ret));
      }
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::write_all_updates() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sr_cache_.write_all_updates(trans_))) {
    LOG_WARN("fail to write all updates", K(ret));
  }
  return ret;
}

int ObHNSWIndexBuilder::get_vector_distance_with_cache(ObHNSWElement *e1,
                                                       ObHNSWElement *e2,
                                                       int64_t lv,
                                                       double &distance) {
  int ret = OB_SUCCESS;
  ObHNSWLevelNeighbors *neighbors_e1 = nullptr;
  ObHNSWLevelNeighbors *neighbors_e2 = nullptr;
  bool cache_match = false;
  distance = 0.0;
  if (OB_FAIL(sr_cache_.get_neighbors(e1, lv, neighbors_e1))) {
    LOG_WARN("fail to get neighbors", K(ret), K(lv));
  } else if (OB_FAIL(sr_cache_.get_neighbors(e2, lv, neighbors_e2))) {
    LOG_WARN("fail to get neighbors", K(ret), K(lv));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !cache_match && i < neighbors_e1->n_nb_;
       ++i) {
    if (neighbors_e1->nbs_[i].element_ == e2) {
      cache_match = true;
      distance = neighbors_e1->nbs_[i].distance_;
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !cache_match && i < neighbors_e2->n_nb_;
       ++i) {
    if (neighbors_e2->nbs_[i].element_ == e1) {
      cache_match = true;
      distance = neighbors_e2->nbs_[i].distance_;
      break;
    }
  }
  if (OB_SUCC(ret) && !cache_match &&
      OB_FAIL(opt_.dfunc_(e1->vector_, e2->vector_, distance))) {
    LOG_WARN("fail to calc distance", K(ret));
  }
  return ret;
}

int ObHNSWIndexBuilder::get_vector_distance(const ObTypeVector &v1,
                                            const ObTypeVector &v2,
                                            double &distance) {
  int ret = OB_SUCCESS;
  distance = 0.0;
  if (OB_FAIL(opt_.dfunc_(v1, v2, distance))) {
    LOG_WARN("fail to calc distance", K(ret));
  }
  return ret;
}

int ObHNSWIndexBuilder::prune_connections(
    ObHNSWIndexRingedCache *ringed_cache, uint64_t global_cache_epoch,
    ObHNSWLevelNeighbors *lv_nbs, int64_t lv, ObHNSWElement *new_cand_e,
    double distance, int cur_skip_cnt, int64_t &prune_idx, int64_t debug_lv) {
  int ret = OB_SUCCESS;
  prune_idx = -1;
  std::priority_queue<ObHNSWPruneCandidate, std::vector<ObHNSWPruneCandidate>,
                      std::less<ObHNSWPruneCandidate>>
      far_cds;
  std::priority_queue<ObHNSWPruneCandidate, std::vector<ObHNSWPruneCandidate>,
                      std::greater<ObHNSWPruneCandidate>>
      near_cds;
  common::ObArray<ObHNSWPruneCandidate> r;
  int64_t m = lv_nbs->n_nb_;
  for (int64_t i = 0; OB_SUCC(ret) && i < lv_nbs->n_nb_; ++i) {
    ObHNSWElement *ele = nullptr;
    if (OB_FAIL(ringed_cache->get_element_in_neighbor(
            trans_, &(lv_nbs->nbs_[i]), cur_skip_cnt, global_cache_epoch,
            ele))) {
      LOG_WARN("fail to get element in neighbor", K(ret), K(i),
               K(cur_skip_cnt));
    } else {
      far_cds.push(ObHNSWPruneCandidate{i, ele, lv_nbs->nbs_[i].distance_});
    }
  }
  far_cds.push(ObHNSWPruneCandidate{-1, new_cand_e, distance});

  while (!far_cds.empty() && r.count() < m && OB_SUCC(ret)) {
    ObHNSWPruneCandidate cd = far_cds.top();
    far_cds.pop();
    bool is_closer = false;
    if (OB_FAIL(check_element_closerV2(cd, r, lv, is_closer))) {
      LOG_WARN("fail to check element closer", K(ret));
    } else if (is_closer) {
      r.push_back(cd);
    } else {
      near_cds.push(cd);
    }
  }

  while (OB_SUCC(ret) && !near_cds.empty() && r.count() < m) {
    ObHNSWPruneCandidate cd = near_cds.top();
    near_cds.pop();
    r.push_back(cd);
  }

  if (!near_cds.empty()) {
    prune_idx = near_cds.top().idx_;
  } else {
    OB_ASSERT(!far_cds.empty());
    prune_idx = far_cds.top().idx_;
  }
  return ret;
}

int ObHNSWIndexBuilder::hnsw_insert_elementV2(common::ObTypeVector &q,
                                              int64_t e_lv, ObHNSWElement *ep,
                                              sqlclient::ObMySQLResult *res,
                                              ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  int64_t ele_max_level = e_lv;
  int64_t ep_lv = cur_max_level_;
  e_lv = OB_MIN(e_lv, ep_lv);
  ele = nullptr;

  common::ObArray<ObHNSWCandidate> cds;
  double tmp_dis = 0.0;
  if (OB_FAIL(get_vector_distance(q, ep->vector_, tmp_dis))) {
    LOG_WARN("fail to calc distance", K(ret));
  } else {
    cds.push_back(ObHNSWCandidate{ep, tmp_dis, 0});
  }

  /* 1st phase: greedy search to insert level */
  for (int64_t lv = ep_lv; OB_SUCC(ret) && lv >= e_lv + 1; --lv) {
    if (OB_FAIL(hnsw_search_layer(q, cds, 1, lv))) {
      LOG_WARN("fail to search layer", K(ret), K(lv));
    }
  }

  /* 2nd phase */
  int64_t min_skip_cnt = INT64_MAX;
  common::ObArray<common::ObArray<ObHNSWCandidate>> selected_neighbors_lv;
  for (int64_t lv = e_lv; OB_SUCC(ret) && lv >= 0; --lv) {
    common::ObArray<ObHNSWCandidate> selected_neighbors;
    int64_t lm = (lv == 0) ? opt_.m_ * 2 : opt_.m_;

    if (OB_FAIL(hnsw_search_layer(q, cds, opt_.ef_construction_, lv))) {
      LOG_WARN("fail to search layer", K(ret), K(opt_.ef_construction_), K(lv));
    } else if (OB_FAIL(hnsw_select_neighbors_simple(cds, lm, selected_neighbors,
                                                    min_skip_cnt))) {
      // select neighbors (`lm` of `ef_construction`)
      LOG_WARN("fail to select neighbors", K(ret), K(lm));
    } else if (OB_FAIL(selected_neighbors_lv.push_back(selected_neighbors))) {
      LOG_WARN("fail to push back selected_neighbors", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ele_max_level <= cur_max_level_) {
    if (OB_FAIL(sr_cache_.set_element_for_insert(
            this, res, ele, ele_max_level, static_cast<int>(min_skip_cnt + 1),
            selected_neighbors_lv))) {
      LOG_WARN("fail to set element for insert", K(ret), K(ele_max_level),
               K(min_skip_cnt + 1));
    }
  } else {
    // ele_max_level > cur_max_level_
    if (OB_FAIL(sr_cache_.set_new_ep(this, res, ele, ele_max_level,
                                     selected_neighbors_lv))) {
      LOG_WARN("fail to set new entry point", K(ret), K(ele_max_level));
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::insert_vector(sqlclient::ObMySQLResult *res) {
  int ret = OB_SUCCESS;
  //
  // static int64_t static_lv[23] = {
  // 1,0,2,2,5,6,1,3,0,0,1,5,5,5,2,4,4,2,0,1,2,3,3 };
  //
  int64_t new_level = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObHNSWIndexBuilder not inited", K(ret));
  } else {
    ObHNSWElement *ele = nullptr;
    // new_level = static_lv[(lv_idx_++) % 23];
    std::uniform_real_distribution<double> distribution(0.0, 1.0);
    new_level = OB_MIN(
        static_cast<int>(-log(distribution(level_generator_)) * opt_.ml_),
        ObHNSWOption::HNSW_MAX_LEVEL);
    if (OB_ISNULL(ep_)) {
      common::ObArray<common::ObArray<ObHNSWCandidate>> empty_cd;
      if (OB_FAIL(sr_cache_.set_element_for_insert(this, res, ele, new_level, 0,
                                                   empty_cd))) {
        LOG_WARN("fail to set element for insert", K(ret), K(new_level));
      } else {
        ep_ = ele;
        cur_max_level_ = new_level;
      }
    } else {
      common::ObTypeVector vector;
      if (OB_FAIL(borrow_vector_from_mysql_res(res, pkey_count_, vector))) {
        LOG_WARN("fail to borrow vector from mysql res", K(ret),
                 K(pkey_count_));
      } else if (OB_FAIL(
                     hnsw_insert_elementV2(vector, new_level, ep_, res, ele))) {
        LOG_WARN("fail to insert element", K(ret), K(new_level));
      } else if (OB_UNLIKELY(cur_max_level_ < new_level)) {
        // if ele level is higher
        ep_ = ele;
        cur_max_level_ = new_level;
      }
    }
  }
  return ret;
}

int ObHNSWIndexBuilder::delete_vector(sqlclient::ObMySQLResult *res) {
  int ret = OB_SUCCESS;

  return ret;
}

} // namespace share
} // namespace oceanbase
