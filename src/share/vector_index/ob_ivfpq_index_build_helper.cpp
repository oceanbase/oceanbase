/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#include "lib/oblog/ob_log_module.h"
#define USING_LOG_PREFIX COMMON
#include "share/vector_index/ob_ivfpq_index_build_helper.h"
#include "share/vector_index/ob_tenant_pq_center_cache.h"

namespace oceanbase {
namespace share {
/*
 * ObIvfpqIndexBuildHelper Impl
 */
int ObIvfpqIndexBuildHelper::init(const int64_t tenant_id,
                                    const int64_t lists,
                                    const int64_t segment,
                                    const ObVectorDistanceType distance_type) {
  int ret = ObIvfIndexBuildHelper::init(tenant_id, lists, distance_type);
  if (OB_SUCC(ret)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_FAIL(center_helper_.init(tenant_id, lists, distance_type,
                  tenant_config->vector_ivfpq_iters_count,
                   tenant_config->vector_ivfpq_elkan))) {
      LOG_WARN("failed to init center helper");
    } else {
      pq_samples_.set_attr(ObMemAttr(MTL_ID(), "PQSamples"));
      pq_kmeans_helpers_.set_attr(ObMemAttr(MTL_ID(), "PQSamples"));
      pq_seg_ = segment;
      is_init_ = true;
    }
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::split_vector(ObArray<ObTypeVector*> &vector_list, const ObTypeVector &vector, const int64_t nearest_center_idx, const int64_t segment) {
  int ret = OB_SUCCESS;
  ObTypeVector *new_vector = nullptr;
  if (OB_FAIL(ObTypeVector::alloc_and_copy_vector(allocator_, *center_helper_.get_center_vectors().at(nearest_center_idx), new_vector))) {
    LOG_WARN("failed to alloc and copy vector", K(ret));
  } else if (OB_FAIL(vector_list.prepare_allocate(pq_seg_))) {
    LOG_WARN("failed to prepare allocate splitted vector", K(ret), K(pq_seg_));
  } else if (OB_FAIL(new_vector->subtract(vector))) {
    LOG_WARN("failed to subtract vector", K(ret), K(vector));
  } else if (segment <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected segment", K(ret), K(segment));
  } else if (vector_list.count() < segment) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector list count", K(ret), K(vector_list.count()), K(segment));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < segment; ++i) {
      if (OB_FAIL(new_vector->split(vector_list.at(i), pq_dim_, i))) {
        LOG_WARN("failed to split vector", K(ret));
      }
    }
  }
  return ret;
}


void ObIvfpqIndexBuildHelper::destroy_pq_samples() {
  while (!pq_samples_.empty()) {
    ObArray<ObTypeVector*> &pq_sample = pq_samples_.at(pq_samples_.count() - 1);
    while (!pq_sample.empty()) {
      ObTypeVector* vector = pq_sample.at(pq_sample.count() - 1);
      pq_sample.pop_back();
      if (OB_NOT_NULL(vector)) {
        vector->destroy(allocator_);
        allocator_.free(vector);
      }
    }
    pq_samples_.pop_back();
  }
}

void ObIvfpqIndexBuildHelper::destroy_pq_kmeans_helpers() {
  while (!pq_kmeans_helpers_.empty()) {
    ObKMeansCenterHelper *helper = pq_kmeans_helpers_.at(pq_kmeans_helpers_.count() - 1);
    pq_kmeans_helpers_.pop_back();
    if (OB_NOT_NULL(helper)) {
      helper->destroy();
      allocator_.free(helper);
    }
  }
}

void ObIvfpqIndexBuildHelper::destroy() {
  destroy_pq_samples();
  destroy_pq_kmeans_helpers();
  ObIvfIndexBuildHelper::destroy();
}


void ObIvfpqIndexBuildHelper::reuse() {
  destroy_pq_samples();
  destroy_pq_kmeans_helpers();
  ObIvfIndexBuildHelper::reuse();
  status_ = IVF_CENTERS;
}

int ObIvfpqIndexBuildHelper::build() {
  int ret = OB_SUCCESS;
  switch (status_) {
    case IVF_CENTERS:
      if (OB_FAIL(center_helper_.build())) {
        LOG_WARN("failed to build center helper");
      }
      if (center_helper_.is_finish()) {
        status_ = PQ_PREPARE_CENTERS;
      }
      break;
    case PQ_PREPARE_CENTERS:
      if (center_helper_.skip_insert()) {
        status_ = COMPLETE;
      } else if (OB_FAIL(pq_prepare_centers())) {
        LOG_WARN("failed to prepare pq centers", K(ret));
      } else {
        status_ = PQ_KMEANS;
      }
      break;
    case PQ_KMEANS:
      if (OB_FAIL(pq_kmeans())) {
        LOG_WARN("failed to run pq kmeans", K(ret));
      } else {
        status_ = COMPLETE;
      }
      break;
    case COMPLETE:
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(ret), K(status_));
      break;
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::pq_prepare_centers() {
  int ret = OB_SUCCESS;

  ObIndexSampleCache *cache = center_helper_.get_cache();
  cache->reuse();
  int64_t vector_dim = center_helper_.get_center_vectors().at(0)->dims();
  // TODO: Handle the case when the dimension of the center vector cannot be divisible by the pq_seg_
  pq_dim_ = vector_dim / pq_seg_;
  if (pq_seg_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected segment", K(ret), K(pq_seg_), K(pq_dim_));
  } else if (vector_dim % pq_seg_ != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected vector dim", K(ret), K(vector_dim), K(pq_seg_));
  } else if (OB_FAIL(pq_samples_.prepare_allocate(pq_seg_))) {
    LOG_WARN("failed to prepare allocate pq samples", K(ret), K(pq_seg_));
  } else {
    while (OB_SUCC(ret)) {
      ObArray<ObTypeVector*> splitted_vector;
      splitted_vector.set_attr(ObMemAttr(MTL_ID(), "PQBuild"));
      ObTypeVector vector;
      int64_t nearest_center_idx = -1;
      double min_distance = DBL_MAX;
      if (OB_FAIL(cache->get_next_vector(vector))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next vector", K(ret));
        }
      } else if (vector.dims() != vector_dim) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected vector dims", K(ret), K(vector.dims()), K(vector_dim), K(vector));
      } else if (OB_FAIL(center_helper_.get_nearest_center(vector, nearest_center_idx,
                                                        min_distance))) {
        LOG_WARN("failed to get nearest center", K(ret), K(vector));
      } else if (nearest_center_idx < 0 || nearest_center_idx >= center_helper_.get_center_vectors().count()) {
        LOG_WARN("unexpected nearest center idx", K(ret), K(nearest_center_idx),
                  K(center_helper_.get_center_vectors().count()));
      } else if (OB_FAIL(split_vector(splitted_vector, vector, nearest_center_idx, pq_seg_))) {
        LOG_WARN("failed to split vector into PQ segments", K(ret), K(vector));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < pq_seg_; ++i) {
        ObTypeVector *splitted = splitted_vector.at(i);
        if (splitted->is_zero() || OB_FAIL(pq_samples_.at(i).push_back(splitted))) {
          LOG_WARN("failed to push back splitted vector", K(ret), K(i), KPC(splitted));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < pq_seg_; ++i) {
      ObKMeansCenterHelper *helper = nullptr;
      void *buf = nullptr;
      if (nullptr == (buf = allocator_.alloc(sizeof(ObKMeansCenterHelper)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc ObTypeVector", K(ret));
      } else if (FALSE_IT(helper = new (buf) ObKMeansCenterHelper())) {
      } else if (OB_FAIL(helper->init(tenant_id_, center_helper_.get_lists(), center_helper_.get_distance_type(),
                                center_helper_.get_max_iterate_times(), center_helper_.is_using_elkan_kmeans()))) {
        LOG_WARN("failed to init kmeans center helper", K(ret));
        helper->destroy();
        allocator_.free(helper);
      } else if (OB_FAIL(pq_kmeans_helpers_.push_back(helper))) {
        LOG_WARN("failed to push back kmeans helper", K(ret));
      }
    }
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::pq_kmeans() {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pq_seg_; ++i) {
    ObKMeansCenterHelper *helper = pq_kmeans_helpers_.at(i);
    ObIndexSampleCache cache;
    if (OB_FAIL(cache.init(tenant_id_, pq_samples_.at(i), "IvfpqSampsSeg"))) {
      LOG_WARN("failed to init sample cache for pq segment", K(ret), K(i));
    } else {
      helper->set_cache(&cache);
      if (!helper->is_inited()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected helper status", K(ret), K(*helper));
      } else {
        while ((OB_SUCC(ret) || OB_NEED_RETRY == ret) && !helper->is_finish()) {
          LOG_TRACE("build ivf index", K(ret), K(helper));
          if (OB_FAIL(helper->build())) {
            LOG_WARN("failed to build ivf index", K(ret));
          }
        }
      }
    }
    LOG_TRACE("build pq kmeans", K(ret), K(i), K(pq_seg_), K(*pq_kmeans_helpers_.at(i)), K(*pq_kmeans_helpers_.at(i)->get_cache()));
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::set_partition_name(common::ObTabletID &tablet_id, uint64_t base_table_id) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIvfIndexBuildHelper::set_partition_name(tablet_id, base_table_id))) {
    LOG_WARN("failed to set ivf partition name", K(ret), K(tenant_id_), K(base_table_id));
  } else if (OB_FAIL(ObTenantPQCenterCache::set_partition_name(
        tenant_id_, base_table_id, tablet_id, allocator_for_partition_name_,
        partition_name_, partition_idx_))) {
    LOG_WARN("failed to set partition name", K(ret), K(tenant_id_), K(base_table_id));
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::set_center_cache(const int64_t table_id) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIvfIndexBuildHelper::set_center_cache(table_id))) {
    LOG_WARN("failed to set ivf center cache", K(ret), K(table_id));
  } else if (!is_finish()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(status_));
  } else {
    MTL_SWITCH(tenant_id_) {
      ObArray<common::ObIArray<ObTypeVector *> *> pq_centers;
      pq_centers.set_attr(ObMemAttr(MTL_ID(), "PQBuild"));
      for (int64_t i = 0; OB_SUCC(ret) && i < pq_seg_; ++i) {
        ObKMeansCenterHelper *helper = pq_kmeans_helpers_.at(i);
        if (OB_FAIL(pq_centers.push_back(&helper->get_center_vectors()))) {
          LOG_WARN("failed to push back centers", K(ret));
        }
      }
      if (OB_FAIL(MTL(ObTenantPQCenterCache *)
                      ->put(table_id, partition_idx_, center_helper_.get_distance_type(),
                            pq_centers))) {
        LOG_WARN("failed to put int cache", K(ret));
      }
    }
  }
  return ret;
}

int ObIvfpqIndexBuildHelper::construct_batch_insert_second_container_sql(
      common::ObSqlString &second_container_string, const int64_t dest_table_id,
      const int64_t second_container_table_id) {
  int ret = OB_SUCCESS;
  if (COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(status_));
  } else if (OB_FAIL(construct_batch_insert_container_sql_simple(
                  second_container_string, dest_table_id, second_container_table_id, true))) { 
    LOG_WARN("failed to construct batch insert container sql simple", K(ret));
  } else {
    ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
    allocator.set_attr(ObMemAttr(tenant_id_, "IvfArena"));
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE,
                        ObCharset::get_system_collation());
    ObObj obj;
    ObObj cast_obj;
    ObString tmp_str;
    ObString patch_str;
    if (OB_FAIL(get_patch_pkeys_for_center_dummy_pkeys_array(patch_str))) {
      LOG_WARN("fail to get patch pkeys string", K(ret));
    }
    // construct container_string
    bool empty = true;
    for (int64_t i = 0;
          OB_SUCC(ret) && i < pq_seg_; ++i) {
      if (pq_kmeans_helpers_.at(i)->get_center_vectors().count() <= 0) {
        continue;
      }
      empty = false;
      for (int64_t j = 0;
          OB_SUCC(ret) && j < pq_kmeans_helpers_.at(i)->get_center_vectors().count(); ++j) {
        obj.reset();
        ObTypeVector *center = pq_kmeans_helpers_.at(i)->get_center_vectors().at(j);
        obj.set_vector(center->ptr(), center->dims());
        if (i == 0 && 0 == j) {
          if (OB_FAIL(second_container_string.append("("))) {
            LOG_WARN("fail to append sql string", K(ret));
          }
        } else if (OB_FAIL(second_container_string.append(",("))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
        if (FAILEDx(ObObjCaster::to_type(ObVarcharType, cast_ctx, obj,
                                        cast_obj))) {
          LOG_WARN("failed to cast to varchar", K(ret));
        } else if (OB_FAIL(cast_obj.get_string(tmp_str))) {
          LOG_WARN("failed to get string", K(ret), K(cast_obj));
        } else if (OB_FAIL(second_container_string.append_fmt(
                      "%ld,%ld,%.*s,'%.*s'", i , j, patch_str.length(),
                      patch_str.ptr(), tmp_str.length(), tmp_str.ptr()))) {
          LOG_WARN("failed to append sql string", K(ret),
                  K(second_container_string));
        } else if (OB_FAIL(second_container_string.append(")"))) {
          LOG_WARN("fail to append sql string", K(ret));
        }
      }
    }
    if (empty) {
      // if there is no center, just keep the sql string empty
      second_container_string.assign("SELECT 1");
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("success to construct insert container table sql", K(ret),
                K(second_container_string));
    }
  }
  LOG_TRACE("construct_batch_insert_second_container_sql", K(ret), K(second_container_string), K(dest_table_id), K(second_container_table_id));
  return ret;
}

int ObIvfpqIndexBuildHelper::construct_batch_insert_index_sql(sqlclient::ObMySQLResult &result,
      common::ObSqlString &index_string,
      int64_t &row_count, int64_t &idx) {
  int ret = OB_SUCCESS;
  if (COMPLETE != status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("status not match", K(ret), K(status_));
  } else {
    row_count = 0;
    int64_t nearest_center_idx = -1;
    double min_distance = DBL_MAX;
    ObArenaAllocator allocator(ObModIds::BLOCK_ALLOC);
    allocator.set_attr(ObMemAttr(tenant_id_, "IvfArena"));
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE,
                       ObCharset::get_system_collation());
    bool first_row = true;
    // construct index_string
    while (OB_SUCC(ret) && row_count < BATCH_INSERT_SIZE) {
      ObTypeVector vector;
      int64_t column_cnt;
      ObArray<ObTypeVector*> splitted_vector;
      splitted_vector.set_attr(ObMemAttr(MTL_ID(), "PQBuild"));
      if (OB_FAIL(pre_fill_data_insert_index_sql(result, vector, index_string,
                  column_cnt, min_distance, nearest_center_idx, first_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to pre fill data insert index sql", K(ret));
        }
        break;
      } else if (nearest_center_idx < 0 || nearest_center_idx >= center_helper_.get_center_vectors().count()) {
        LOG_WARN("unexpected nearest center idx", K(ret), K(nearest_center_idx),
                  K(center_helper_.get_center_vectors().count()));
      } else if (OB_FAIL(split_vector(splitted_vector, vector, nearest_center_idx, pq_seg_))) {
        LOG_WARN("failed to split vector into PQ segments", K(ret), K(vector));
      }
      ObSqlString vector_sql_str;
      if (FAILEDx(vector_sql_str.append("["))) {
        LOG_WARN("failed to append vector string", K(ret), K(vector_sql_str));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < pq_seg_; ++i) {
        int64_t nearest_seg_center_idx = -1;
        double min_seg_distance = DBL_MAX;
        if (splitted_vector.at(i)->is_zero()) {
          LOG_WARN("the vector is all zero", K(ret), KPC(splitted_vector.at(i)));
          nearest_seg_center_idx = -1;
        } else if (OB_FAIL(pq_kmeans_helpers_.at(i)->get_nearest_center(*splitted_vector.at(i), nearest_seg_center_idx, min_seg_distance))) {
          LOG_WARN("failed to get nearest center", K(ret));
        }
        if (FAILEDx(vector_sql_str.append_fmt("%ld", nearest_seg_center_idx))) {
          LOG_WARN("failed to append vector string", K(ret), K(vector_sql_str));
        } else if (i == pq_seg_ - 1) {
          if (FAILEDx(vector_sql_str.append("]"))) {
            LOG_WARN("failed to append vector string", K(ret), K(vector_sql_str));
          }
        } else {
          if (FAILEDx(vector_sql_str.append(","))) {
            LOG_WARN("failed to append vector string", K(ret), K(vector_sql_str));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if(OB_FAIL(post_fill_data_insert_index_sql(cast_ctx, result, index_string,
                   vector_sql_str.string(), column_cnt, nearest_center_idx))) {
          LOG_WARN("failed to post fill data insert index sql", K(ret));
        }
        ++row_count;
        ++idx;
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("success to construct batch insert index table sql", K(ret),
              K(index_string), K(row_count));
  }
  LOG_TRACE("construct_batch_insert_index_sql", K(ret), K(index_string), K(row_count), K(idx));
  return ret;
}

} // namespace share
} // namespace oceanbase
