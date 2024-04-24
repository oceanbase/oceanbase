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

#include "share/vector_index/ob_hnsw_index_reader.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/object/ob_obj_cast.h"
#include <queue>
#include <set>

namespace oceanbase {
namespace share {

const double ObHNSWIndexReader::ann_search_mem_percentage = 0.5;

int ObHNSWIndexReader::init() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHNSWIndexReader is inited", K(ret));
  } else if (OB_FAIL(sr_cache_.init(false))) {
    LOG_WARN("fail to init ObHNSWIndexSwitchableRingedCache", K(ret));
  } else {
    mem_allow_limit_ = lib::get_tenant_memory_limit(tenant_id_);
    is_inited_ = true;
  }
  return ret;
}

int ObHNSWIndexReader::reset() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sr_cache_.prune_out_ring_cache(nullptr))) {
    LOG_WARN("fail to prune out ring cache", K(ret));
  }
  return ret;
}

int ObHNSWIndexReader::init_ep_for_search(common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  OB_ASSERT(OB_ISNULL(ep_));
  if (OB_FAIL(get_ep_with_sql_for_search(trans))) {
    LOG_WARN("fail to get ep", K(ret), K(cur_max_level_));
  } else if (-1 != cur_max_level_ && OB_ISNULL(ep_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("entry point can not be null", K(ret));
  }
  return ret;
}

int ObHNSWIndexReader::get_ep_with_sql_for_search(
    common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_LIKELY(-1 != cur_max_level_)) {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("select %.*s,level,n_neighbors,distance",
                               static_cast<int>(vector_column_name_.length()),
                               vector_column_name_.ptr()))) {
      LOG_WARN("failed to append sql", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_count_; ++i) {
      if (OB_FAIL(sql.append_fmt(",base_pk%ld", i))) {
        LOG_WARN("failed to append sql", K(ret), K(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_count_; ++i) {
      if (OB_FAIL(sql.append_fmt(",ref_pk%ld", i))) {
        LOG_WARN("fail to append sql", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.append_fmt(
                   " from `%.*s`.`%.*s` where ",
                   static_cast<int>(index_db_name_.length()),
                   index_db_name_.ptr(),
                   static_cast<int>(hnsw_index_table_name_.length()),
                   hnsw_index_table_name_.ptr()))) {
      LOG_WARN("failed to append sql", K(ret), K(index_db_name_),
               K(hnsw_index_table_name_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_count_; ++i) {
      const ObString &ep_pkey_str = ep_pkeys_for_search_->at(i);
      if (OB_FAIL(sql.append_fmt(
              i == 0 ? "base_pk%ld=%.*s " : "and base_pk%ld=%.*s ", i,
              static_cast<int>(ep_pkey_str.length()), ep_pkey_str.ptr()))) {
        LOG_WARN("failed to append sql", K(ret), K(i));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sr_cache_.load_ep_element(trans, sql, ep_))) {
      LOG_WARN("fail to load ep element", K(ret));
    }
  } else {
    ep_ = nullptr;
  }
  return ret;
}

int ObHNSWIndexReader::get_vector_distance(const ObTypeVector &v1,
                                           const ObTypeVector &v2,
                                           double &distance) {
  int ret = OB_SUCCESS;
  distance = 0.0;
  if (OB_FAIL(opt_.dfunc_(v1, v2, distance))) {
    LOG_WARN("fail to calc distance", K(ret));
  }
  return ret;
}

int ObHNSWIndexReader::hnsw_search_layer(common::ObMySQLTransaction *trans,
                                         common::ObTypeVector &q,
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
              trans, &(neighbors->nbs_[i]), n_cd.element_->skip_cnt_,
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

int ObHNSWIndexReader::search_vector(common::ObMySQLTransaction *trans,
                                     ObTypeVector &q, int64_t k,
                                     int64_t ef_search,
                                     ObArray<ObHNSWCandidate> &res) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ep_)) {
    // do nothing
  } else {
    common::ObArray<ObHNSWCandidate> cds;
    std::priority_queue<ObHNSWCandidate, std::vector<ObHNSWCandidate>,
                        std::greater<ObHNSWCandidate>>
        near_cds;
    double tmp_dis = 0.0;
    if (OB_FAIL(get_vector_distance(q, ep_->vector_, tmp_dis))) {
      LOG_WARN("fail to calc distance", K(ret));
    } else {
      cds.push_back(ObHNSWCandidate{ep_, tmp_dis, 0});
    }

    for (int64_t lv = cur_max_level_; OB_SUCC(ret) && lv >= 1; --lv) {
      if (OB_FAIL(hnsw_search_layer(trans, q, cds, 1, lv))) {
        LOG_WARN("fail to search layer", K(ret), K(lv));
      }
    }
    if (OB_SUCC(ret) &&
        OB_FAIL(hnsw_search_layer(trans, q, cds, ef_search, 0))) {
      LOG_WARN("fail to search layer", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < cds.count(); ++i) {
      near_cds.push(cds.at(i));
    }

    while (OB_SUCC(ret) && !near_cds.empty() && res.count() < k) {
      ObHNSWCandidate cd = near_cds.top();
      near_cds.pop();
      res.push_back(cd);
    }

    if (sr_cache_.get_cur_cache_mem_usage() >
            ann_search_mem_percentage * mem_allow_limit_ &&
        OB_FAIL(sr_cache_.prune_out_ring_cache(trans))) {
      LOG_WARN("fail to prune out ring cache", K(ret));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
