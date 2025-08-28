/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SHARE
#include "lib/geo/ob_s2adapter.h" // for htonll
#include "ob_vector_common_util.h"
#include "observer/ob_inner_sql_connection_pool.h"

namespace oceanbase {
namespace share {

// ------------------ ObVectorNormalize implement ------------------

// norm_vector = data means update inplace
int ObVectorNormalize::L2_normalize_vector(const int64_t dim, float *data, float *norm_vector, bool *do_normalize /*= nullptr*/)
{
  int ret = OB_SUCCESS;
  if (0 >= dim || OB_ISNULL(data) || OB_ISNULL(norm_vector)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), KP(data), KP(norm_vector));
  } else {
    const float float_accuracy = 0.00001;
    float norm_l2_sqr = ObVectorL2Distance<float>::l2_norm_square(data, dim);
    
    if (norm_l2_sqr > 0 && fabs(1.0f - norm_l2_sqr) > float_accuracy) {
      float norm_l2 = sqrt(norm_l2_sqr);
      for (int64_t i = 0; i < dim; ++i) {
        norm_vector[i] = data[i] / norm_l2;
      }
    } else if (data != norm_vector) {
      MEMCPY(norm_vector, data, dim * sizeof(float));
    }
    if (OB_NOT_NULL(do_normalize)) {
      *do_normalize = norm_l2_sqr > 0 && fabs(1.0f - norm_l2_sqr) > float_accuracy;
    }
  }
  return ret;
}

int ObVectorNormalize::L2_normalize_vectors(const int64_t dim, const int64_t count, float *datas, float *norm_vectors)
{
  int ret = OB_SUCCESS;
  if (0 >= dim || 0 >= count || OB_ISNULL(datas) || OB_ISNULL(norm_vectors)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(count), KP(datas), KP(norm_vectors));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      float *data = datas + i * dim;
      float *norm_vector = norm_vectors + i * dim;
      if (OB_FAIL(ObVectorNormalize::L2_normalize_vector(dim, data, norm_vector))) {
        SHARE_LOG(WARN, "failed to calc l2 normalize", K(ret));
      }
    }
  }
  return ret;
}

// ------------------ ObVectorNormalizeInfo implement ------------------
int ObVectorNormalizeInfo::normalize_vectors(const int64_t dim, const int64_t count, float *datas, float *norm_vectors)
{
  int ret = OB_SUCCESS;
  if (0 >= dim || 0 >= count || OB_ISNULL(datas) || OB_ISNULL(norm_vectors)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), K(count), KP(datas), KP(norm_vectors));
  } else if (OB_ISNULL(normalize_func_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "nullptr func ptr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      float *data = datas + i * dim;
      float *norm_vector = norm_vectors + i * dim;
      if (OB_FAIL(normalize_func_(dim, data, norm_vector, nullptr))) {
        SHARE_LOG(WARN, "failed to calc normalize", K(ret));
      }
    }
  }
  return ret;
}

// ------------------ ObVectorClusterHelper implement ------------------
// search centers[l_idx, r_idx)
int ObVectorClusterHelper::get_nearest_probe_centers(
    float *vector,
    const int64_t dim,
    ObIArray<float*> &centers,
    const int64_t nprobe,
    ObIAllocator &allocator,
    share::ObVectorNormalizeInfo *norm_info/* = nullptr*/,
    int l_idx/* = 0*/,
    int r_idx/* = -1*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vector) || 0 >= dim || 0 >= nprobe) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dim), K(nprobe), KP(vector));
  } else if (centers.empty()) {
    // do nothing
  } else {
    float distance = FLT_MAX;
    // normalize if needed
    float *norm_vector = nullptr;
    if (OB_NOT_NULL(norm_info)) {
      if (OB_ISNULL(norm_vector = static_cast<float*>(allocator.alloc(dim * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc norm vector", K(ret));
      } else if (FALSE_IT(MEMSET(norm_vector, 0, dim * sizeof(float)))) {
      } else if (OB_FAIL(norm_info->normalize_func_(dim, vector, norm_vector, nullptr))) {
        LOG_WARN("failed to normalize vector", K(ret));
      }
    }
    // get the nearest nprobe centers
    float *data = norm_vector == nullptr ? vector : norm_vector;
    r_idx = r_idx < 0 ? centers.count() : r_idx;
    for (int64_t i = l_idx; OB_SUCC(ret) && i < r_idx; ++i) {
      // cosine/inner_product use l2_normalize + l2_distance to replace
      distance = ObVectorL2Distance<float>::l2_square_flt_func(data, centers.at(i), dim);
      if (max_heap_.count() < nprobe) {
        if (OB_FAIL(max_heap_.push(HeapCenterItem(distance, i)))) {
          LOG_WARN("failed to push center heap", K(ret), K(i), K(distance));
        }
      } else {
        const HeapCenterItem &top = max_heap_.top();
        HeapCenterItem tmp(distance, i);
        if (max_compare_(tmp, top)) {
          HeapCenterItem tmp(distance, i);
          if (OB_FAIL(max_heap_.replace_top(tmp))) {
            LOG_WARN("failed to replace top", K(ret), K(tmp));
          }
        }
      }
    }
    // free norm vector
    if (OB_NOT_NULL(norm_vector)) {
      allocator.free(norm_vector);
    }
  }
  
  return ret;
}

int ObVectorClusterHelper::get_center_idx(const int64_t idx, int64_t &center_id)
{
  int ret = OB_SUCCESS;
  if (max_heap_.empty()) {
    center_id = 1; // default center_id for empty center_id_table
  } else if (0 > idx || max_heap_.count() <= idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    center_id = max_heap_.at(idx).center_idx_ + 1; // idx 0 is id 1
  }
  return ret;
}

int ObVectorClusterHelper::get_pq_center_idx(const int64_t idx, const int64_t pq_center_num, int64_t &center_id)
{
  int ret = OB_SUCCESS;
  if (max_heap_.empty()) {
    center_id = 1; // default center_id for empty center_id_table
  } else if (0 > idx || max_heap_.count() <= idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    center_id = (max_heap_.at(idx).center_idx_ % pq_center_num) + 1; // idx 0 is id 1
  }
  return ret;
}

int ObVectorClusterHelper::get_center_vector(const int64_t idx, const ObIArray<float *> &centers, float*& center_vector)
{
  int ret = OB_SUCCESS;
  center_vector = nullptr;
  if (max_heap_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty max heap", K(ret), K(idx), K(max_heap_.count()));
  } else if (0 > idx || max_heap_.count() <= idx) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(max_heap_.count()));
  } else {
    int64_t center_idx = max_heap_.at(idx).center_idx_;
    if (OB_FAIL(centers.at(center_idx, center_vector))) {
      LOG_WARN("failed to get center vector", K(ret), K(center_idx), K(centers.count()));
    }
  }
  return ret;
}

void ObVectorClusterHelper::reset()
{
  max_heap_.reset();
}

int ObVectorClusterHelper::get_center_id_from_string(
    ObCenterId &center_id, 
    const ObString &str, 
    uint8_t flag/* = IVF_PARSE_CENTER*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() < OB_DOC_ID_COLUMN_BYTE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cluster center id str", K(ret), KP(str.ptr()), K(str.length()));
  } else {
    const ObCenterId *center_id_ptr = reinterpret_cast<const ObCenterId *>(str.ptr());
    if (flag & IVF_PARSE_TABLET_ID) {
      center_id.tablet_id_ = ntohll(center_id_ptr->tablet_id_);
    }
    if (flag & IVF_PARSE_CENTER_ID) {
      center_id.center_id_ = ntohll(center_id_ptr->center_id_);
    }
  }
  return ret;
}

int ObVectorClusterHelper::set_center_id_to_string(const ObCenterId &center_id, ObString &str, ObIAllocator *allocator/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  if (!center_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cluster center id", K(ret), K(center_id));
  } else if (OB_NOT_NULL(allocator)) {
    char *buf = nullptr;
    if (OB_ISNULL(buf = static_cast<char*>(allocator->alloc(OB_DOC_ID_COLUMN_BYTE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc cid", K(ret));
    } else {
      str.assign(buf, OB_DOC_ID_COLUMN_BYTE_LENGTH);
      // assign will set length = buffer_size
      str.set_length(0);
    }
  }
  
  if (OB_SUCC(ret)) {
    ObCenterId tmp;
    tmp.tablet_id_ = htonll(center_id.tablet_id_);
    tmp.center_id_ = htonll(center_id.center_id_);
    if (OB_DOC_ID_COLUMN_BYTE_LENGTH != str.write(reinterpret_cast<const char *>(&tmp), OB_DOC_ID_COLUMN_BYTE_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed write data to string", K(ret));
    }
  }
  return ret;
}

int ObVectorClusterHelper::get_pq_center_id_from_string(
    ObPqCenterId &pq_center_id, 
    const ObString &str, 
    uint8_t flag/* = IVF_PARSE_PQ_CENTER*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr()) || OB_UNLIKELY(str.length() < OB_DOC_ID_COLUMN_BYTE_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cluster center id str", K(ret), K(str));
  } else {
    const ObPqCenterId *pq_center_id_ptr = reinterpret_cast<const ObPqCenterId *>(str.ptr());
    if (flag & IVF_PARSE_TABLET_ID) {
      pq_center_id.tablet_id_ = ntohll(pq_center_id_ptr->tablet_id_);
    }
    if (flag & IVF_PARSE_M_ID) {
      pq_center_id.m_id_ = ntohl(pq_center_id_ptr->m_id_);
    }
    if (flag & IVF_PARSE_CENTER_ID) {
      pq_center_id.center_id_ = ntohl(pq_center_id_ptr->center_id_);
    }
  }
  return ret;
}


int ObVectorClusterHelper::set_pq_center_id_to_string(
    const ObPqCenterId &pq_center_id, 
    ObString &str, 
    ObIAllocator *alloc/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  
  if (!pq_center_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid cluster center id", K(ret), K(pq_center_id));
  } else if (OB_NOT_NULL(alloc)) {
    char *c_ptr = nullptr;
    if (OB_ISNULL(c_ptr = reinterpret_cast<char*>(alloc->alloc(sizeof(char) * OB_DOC_ID_COLUMN_BYTE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      str.assign(c_ptr, OB_DOC_ID_COLUMN_BYTE_LENGTH);
      // assign will set length = buffer_size
      str.set_length(0);
    }
  }
  
  if (OB_SUCC(ret)) {
    ObPqCenterId tmp;
    tmp.tablet_id_ = htonll(pq_center_id.tablet_id_);
    tmp.m_id_ = htonl(pq_center_id.m_id_);
    tmp.center_id_ = htonl(pq_center_id.center_id_);
    if (OB_DOC_ID_COLUMN_BYTE_LENGTH != str.write(reinterpret_cast<const char *>(&tmp), OB_DOC_ID_COLUMN_BYTE_LENGTH)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed write data to string", K(ret), K(str), KP(alloc));
    }
  }
  return ret;
}


int ObVectorClusterHelper::create_inner_session(
    const bool is_oracle_mode,
    sql::ObFreeSessionCtx &free_session_ctx,
    sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  uint32_t sid = sql::ObSQLSessionInfo::INVALID_SESSID;
  uint64_t proxy_sid = 0;
  const schema::ObTenantSchema *tenant_info = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(GCTX.session_mgr_->create_sessid(sid))) {
    LOG_WARN("Failed to create sess id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.session_mgr_->create_session(
             tenant_id, sid, proxy_sid, ObTimeUtility::current_time(), session))) {
    GCTX.session_mgr_->mark_sessid_unused(sid);
    session = nullptr;
    LOG_WARN("Failed to create session", K(ret), K(sid), K(tenant_id));
  } else {
    free_session_ctx.sessid_ = sid;
    free_session_ctx.proxy_sessid_ = proxy_sid;
  }
  if (FAILEDx(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
     LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id));
  } else if (OB_FAIL(session->set_default_database(OB_SYS_DATABASE_NAME))) {
    LOG_WARN("Failed to set default database", K(ret), K(tenant_id));
  } else {
    session->set_inner_session();
    session->set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    InnerDDLInfo ddl_info;
    ddl_info.set_is_dummy_ddl_for_inner_visibility(true);
    session->set_database_id(OB_SYS_DATABASE_ID);
    session->set_query_start_time(ObTimeUtil::current_time());
    if (OB_FAIL(session->get_ddl_info().init(ddl_info, 0 /*session_id*/))) {
      LOG_WARN("fail to init ddl info", KR(ret), K(ddl_info));
    } else {
      LOG_INFO("[VECTOR INDEX]: Succ to create inner session", K(ret), K(tenant_id), KP(session));
    }
  }
  if (OB_FAIL(ret)) {
    release_inner_session(free_session_ctx, session);
  }
  return ret;
}

void ObVectorClusterHelper::release_inner_session(sql::ObFreeSessionCtx &free_session_ctx, sql::ObSQLSessionInfo *&session)
{
  if (nullptr != session) {
    LOG_INFO("[VECTOR INDEX]: Release inner session", KP(session));
    session->get_ddl_info().reset();
    session->set_session_sleep();
    GCTX.session_mgr_->revert_session(session);
    GCTX.session_mgr_->free_session(free_session_ctx);
    GCTX.session_mgr_->mark_sessid_unused(free_session_ctx.sessid_);
    session = nullptr;
  }
}

int ObVectorClusterHelper::create_inner_connection(sql::ObSQLSessionInfo *session, common::sqlclient::ObISQLConnection *&connection)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnectionPool *conn_pool = static_cast<observer::ObInnerSQLConnectionPool *>(GCTX.sql_proxy_->get_pool());
  if (OB_FAIL(conn_pool->acquire(session, connection))) {
    LOG_WARN("Failed to acquire conn_", K(ret));
  } else {
    LOG_INFO("[v]: Succ to create inner connection", K(ret), KP(session), KP(connection));
  }
  return ret;
}

void ObVectorClusterHelper::release_inner_connection(common::sqlclient::ObISQLConnection *&connection)
{
  if (nullptr != connection) {
    LOG_INFO("[VECTOR INDEX]: Release inner connection", KP(connection));
    GCTX.sql_proxy_->get_pool()->release(connection, true);
    connection = nullptr;
  }
}

// ------------------ ObCentersBuffer implement ------------------
template <>
int ObCentersBuffer<float>::divide(const int64_t idx, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (0 >= count || idx < 0 || idx >= total_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(idx), K(count), K_(total_cnt));
  } else {
    float *raw_vector = vectors_.at(idx);
    for (int64_t i = 0; i < dim_; ++i) {
      if (OB_UNLIKELY(0 != ::isinf(raw_vector[i]))) {
        raw_vector[i] = raw_vector[i] > 0 ? FLT_MAX : -FLT_MAX;
      }
    }
    if (OB_FAIL(ObVectorDiv::calc(raw_vector, static_cast<float>(count), dim_))) {
      LOG_WARN("fail to div count", K(ret), K(count), K(dim_));
    }
  }
  return ret;
}

template <>
int ObCentersBuffer<float>::get_nearest_center(const int64_t dim, float *vector, int64_t &center_idx)
{
  int ret = OB_SUCCESS;
  if (dim != dim_ || OB_ISNULL(vector)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret));
  } else {
    // TODO(@jingshui): only use l2 distance now
    double min_distance = DBL_MAX;
    double distance = DBL_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt_; ++i) {
      if (OB_FAIL(ObVectorL2Distance<float>::l2_square_func(vector, this->at(i), dim, distance))) {
        SHARE_LOG(WARN, "failed to calc l2 square", K(ret));
      } else if (distance < min_distance) {
        min_distance = distance;
        center_idx = i;
      }
    }
  }
  return ret;
}

template <>
int ObCentersBuffer<float>::add(const int64_t idx, const int64_t dim, float *vector)
{
  int ret = OB_SUCCESS;
  if (dim != dim_ || nullptr == vector || idx < 0 || idx >= total_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dim), KP(vector), K(idx), K_(total_cnt));
  } else if (OB_FAIL(ObVectorAdd::calc(vectors_.at(idx), vector, dim))) {
    LOG_WARN("fail to calc vectors add", K(ret), K(dim), K(idx), K(total_cnt_));
  }
  return ret;
}

template <>
int ObCenterWithBuf<ObRowkey>::new_from_src(const ObRowkey &src_rowkey)
{
  int ret = OB_SUCCESS;
  int64_t need_size = src_rowkey.get_deep_copy_size();
  if (buf_size_ >= need_size && OB_NOT_NULL(buf_)) {
    reset();
  } else {
    if (OB_ISNULL(alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "alloc_ is null", K(ret));
    } else {
      free_buf();
      if (OB_ISNULL(buf_ = (char *)alloc_->alloc(need_size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "allocate mem for buf failed.", K(need_size), K(ret));
      } else {
        buf_size_ = need_size;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ret = src_rowkey.deep_copy(center_, buf_, buf_size_);
  }
  return ret;
}

template <>
int ObCenterWithBuf<ObString>::new_from_src(const ObString &src_cid)
{
  int ret = OB_SUCCESS;
  int need_size = src_cid.length();
  if (buf_size_ >= need_size && OB_NOT_NULL(buf_)) {
    reset();
  } else {
    if (alloc_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "alloc_ is null", K(ret));
    } else {
      free_buf();
      if (NULL == (buf_ = static_cast<char *>(alloc_->alloc(need_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "allocate mem for buf failed.", K(need_size), K(ret));
      } else {
        buf_size_ = need_size;
      }
    }
  }

  if (OB_SUCC(ret)) {
    MEMCPY(buf_, src_cid.ptr(), need_size);
    center_.assign_ptr(buf_, need_size);
  }
  return ret;
}

template <>
int ObCenterWithBuf<ObCenterId>::new_from_src(const ObCenterId &src_cid)
{
  int ret = OB_SUCCESS;
  center_ = src_cid;
  return ret;
}
// ------------------------------- ObVectorCenterClusterHelper implement --------------------------------
template <>
int ObVectorCenterClusterHelper<float, ObCenterId>::get_nearest_probe_center_ids_dist(ObArrayWrap<bool> &nearest_cid_dist)
{
  int ret = OB_SUCCESS;
  if (heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
  }
  while(OB_SUCC(ret) && !heap_.empty()) {
    const HeapCenterItemTemp &cur_top = heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
    } else {
      const ObCenterId &center_id = cur_top.center_with_buf_->get_center();
      if (OB_UNLIKELY(center_id.center_id_ >= nearest_cid_dist.count())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_id is not less than nearest_cid_dist", K(ret), K(center_id), K(nearest_cid_dist.count()));
      } else if (OB_FALSE_IT(nearest_cid_dist.at(center_id.center_id_) = true)) {
      } else if (OB_FAIL(heap_.pop())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to pop max heap", K(ret));
      }
    }
  }
  return ret;
}

template <>
int ObVectorCenterClusterHelper<float, ObCenterId>::get_nearest_probe_centers_ptrs(ObArrayWrap<float *> &nearest_cid_dist)
{
  int ret = OB_SUCCESS;
  if (heap_.count() > nprobe_) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "max heap count is not equal to nprobe", K(ret), K(heap_.count()), K(nprobe_));
  }
  while(OB_SUCC(ret) && !heap_.empty()) {
    const HeapCenterItemTemp &cur_top = heap_.top();
    if (OB_ISNULL(cur_top.center_with_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(WARN, "center_with_buf is null", K(ret), K(cur_top));
    } else {
      const ObCenterId &center_id = cur_top.center_with_buf_->get_center();
      if (OB_UNLIKELY(center_id.center_id_ >= nearest_cid_dist.count())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "center_id is not less than nearest_cid_dist", K(ret), K(center_id), K(nearest_cid_dist.count()));
      } else if (OB_FALSE_IT(nearest_cid_dist.at(center_id.center_id_) = cur_top.vec_dim_.vec_)) {
      } else if (OB_FAIL(heap_.pop())) {
        ret = OB_ERR_UNEXPECTED;
        SHARE_LOG(WARN, "failed to pop max heap", K(ret));
      }
    }
  }
  return ret;
}
}
}
