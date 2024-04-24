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

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/object/ob_obj_cast.h"
#include "share/vector_index/ob_hnsw_index_builder.h"

namespace oceanbase {
namespace share {

int ObHNSWIndexCache::init() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHNSWIndexCache inits twice", K(ret));
  } else if (OB_FAIL(
                 element_index_.create(10240, "HNSW", "HNSW", tenant_id_))) {
    LOG_WARN("fail to create element index", K(ret), K(tenant_id_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObHNSWIndexCache::reset() {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexCache is not inited", K(ret));
  } else {
    allocator_->clear();
    element_index_.reuse();
  }
  return ret;
}

int ObHNSWIndexCache::copy_objs_to_const_pool(sqlclient::ObMySQLResult *res,
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

int ObHNSWIndexCache::copy_vector_to_const_pool(sqlclient::ObMySQLResult *res,
                                                int64_t idx,
                                                common::ObTypeVector &vector) {
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
  }
  return ret;
}

int ObHNSWIndexCache::get_or_alloc_lv_neighbors(
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

int ObHNSWIndexCache::load_ep_element(common::ObMySQLTransaction *trans,
                                      ObSqlString &sql, ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  common::ObTypeVector vector;
  void *ele_buf = nullptr;
  ele = nullptr;
  SMART_VAR(ObMySQLProxy::MySQLResult, read_res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(trans->read(read_res, tenant_id_, sql.ptr()))) {
      LOG_WARN("fail to scan index table", K(ret), K(tenant_id_));
    } else if (OB_ISNULL(result = read_res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", K(ret), KP(result));
    } else {
      int64_t level = 0;
      int64_t n_neighbors = 0;
      double distance = 0.0;
      ObHNSWCPoolPtr pkey_ptr;
      ObHNSWCPoolPtr ref_pkey_ptr;
      bool is_base_inited = false;
      if (OB_ISNULL(ele_buf = allocator_->alloc(sizeof(ObHNSWElement)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for ObHNSWElement", K(ret));
      } else {
        ele = new (ele_buf) ObHNSWElement();
      }
      while (OB_SUCC(ret)) {
        ObHNSWLevelNeighbors *lv_nbs = nullptr;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            if (OB_UNLIKELY(!is_base_inited)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("entry point does not exist", K(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "level", level, int);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(get_or_alloc_lv_neighbors(ele, level, lv_nbs))) {
            LOG_WARN("fail to get or alloc ObHNSWLevelNeighbors", K(ret));
          } else if (!is_base_inited &&
                     OB_FAIL(copy_vector_to_const_pool(result, 0, vector))) {
            LOG_WARN("fail to copy vector to constant pool", K(ret));
          } else if (!is_base_inited &&
                     OB_FAIL(copy_objs_to_const_pool(result, 4, 4 + pkey_count_,
                                                     pkey_ptr))) {
            LOG_WARN("fail to copy objs to constant pool", K(ret),
                     K(pkey_count_));
          }
          if (OB_SUCC(ret) && !is_base_inited) {
            ele->pk_ptr_ = pkey_ptr;
            ele->vector_ = vector;
            ele->skip_cnt_ = 0;
            ele->cache_epoch_ = 0;
            is_base_inited = true;
          }
          EXTRACT_INT_FIELD_MYSQL(*result, "n_neighbors", n_neighbors, int);
          if (OB_FAIL(ret)) {
            break;
          }
          if (n_neighbors == 0) {
            continue;
          }
          EXTRACT_DOUBLE_FIELD_MYSQL(*result, "distance", distance, double);
          if (OB_FAIL(ret)) {
            break;
          } else if (OB_FAIL(copy_objs_to_const_pool(
                         result, 4 + pkey_count_, 4 + pkey_count_ + pkey_count_,
                         ref_pkey_ptr))) {
            LOG_WARN("fail to copy objs to constant pool", K(ret),
                     K(pkey_count_));
          }
          if (OB_SUCC(ret)) {
            lv_nbs->add_origin_neighbor(distance, ref_pkey_ptr,
                                        ObHNSWNeighbor::ORIGINAL);
          }
        }
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(element_index_.set_refactored(ele->pk_ptr_.buf_str_, ele))) {
        LOG_WARN("fail to add new element to element_index_v2", K(ret));
      }
    }
  }
  return ret;
}

int ObHNSWIndexCache::load_element(common::ObMySQLTransaction *trans,
                                   ObHNSWCPoolPtr pkey_ptr, int skip_cnt,
                                   int64_t cache_epoch, ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  lib::ObMemAttr mattr(tenant_id_, "HNSW");
  ObArenaAllocator tmp_allocator(mattr);
  if (OB_FAIL(sql.append_fmt("select %.*s,level,n_neighbors,distance",
                             static_cast<int>(vector_column_name_.length()),
                             vector_column_name_.ptr()))) {
    LOG_WARN("failed to append sql", K(ret));
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
  } else {
    ObCastCtx cast_ctx(&tmp_allocator, NULL, CM_NONE,
                       ObCharset::get_system_collation());
    ObObj tmp_obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_count_; ++i) {
      if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx,
                                       pkey_ptr.objs_[i], tmp_obj))) {
        LOG_WARN("fail to cast obj to varchar", K(pkey_ptr.objs_[i]));
      } else if (OB_FAIL(sql.append_fmt(
                     i == 0 ? "base_pk%ld = %.*s " : "and base_pk%ld = %.*s ",
                     i, // TODO:shk
                     static_cast<int>(tmp_obj.get_string().length()),
                     tmp_obj.get_string().ptr()))) {
        LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
      }
    }
  }
  // excute sql
  if (OB_SUCC(ret)) {
    common::ObTypeVector vector;
    void *ele_buf = nullptr;
    SMART_VAR(ObMySQLProxy::MySQLResult, read_res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(trans->read(read_res, tenant_id_, sql.ptr()))) {
        LOG_WARN("fail to scan index table", K(ret), K(tenant_id_));
      } else if (OB_ISNULL(result = read_res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(ret), KP(result));
      } else {
        int64_t level = 0;
        int64_t n_neighbors = 0;
        double distance = 0.0;
        ObHNSWCPoolPtr ref_pkey_ptr;
        bool is_base_inited = false;
        if (OB_ISNULL(ele_buf = allocator_->alloc(sizeof(ObHNSWElement)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory for ObHNSWElement", K(ret));
        } else {
          ele = new (ele_buf) ObHNSWElement();
        }
        while (OB_SUCC(ret)) {
          ObHNSWLevelNeighbors *lv_nbs = nullptr;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("fail to get next row", K(ret));
            }
          } else {
            EXTRACT_INT_FIELD_MYSQL(*result, "level", level, int);
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(get_or_alloc_lv_neighbors(ele, level, lv_nbs))) {
              LOG_WARN("fail to get or alloc ObHNSWLevelNeighbors", K(ret));
            } else if (!is_base_inited &&
                       OB_FAIL(copy_vector_to_const_pool(result, 0, vector))) {
              LOG_WARN("fail to copy vector to constant pool", K(ret));
            }
            if (OB_SUCC(ret) && !is_base_inited) {
              ele->skip_cnt_ = skip_cnt;
              ele->cache_epoch_ = cache_epoch;
              ele->pk_ptr_ = pkey_ptr;
              ele->vector_ = vector;
              is_base_inited = true;
            }
            EXTRACT_INT_FIELD_MYSQL(*result, "n_neighbors", n_neighbors, int);
            if (OB_FAIL(ret)) {
              break;
            }
            if (n_neighbors == 0) {
              continue;
            }
            EXTRACT_DOUBLE_FIELD_MYSQL(*result, "distance", distance, double);
            if (OB_FAIL(ret)) {
              break;
            } else if (OB_FAIL(copy_objs_to_const_pool(
                           result, 4, 4 + pkey_count_, ref_pkey_ptr))) {
              LOG_WARN("fail to copy objs to constant pool", K(ret),
                       K(pkey_count_));
            }
            if (OB_SUCC(ret)) {
              lv_nbs->add_origin_neighbor(distance, ref_pkey_ptr,
                                          ObHNSWNeighbor::ORIGINAL);
            }
          }
        }
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(element_index_.set_refactored(ele->pk_ptr_.buf_str_, ele))) {
        LOG_WARN("fail to add new element to element_index", K(ret));
      }
    }
  }
  return ret;
}

int ObHNSWIndexCache::get_element_in_neighbor_without_load(
    ObHNSWNeighbor *nb, ObHNSWElement *&ele) {
  // if ObHNSWNeighbor::cache_lv_ > ObHNSWElement::cache_lv_ && cache_epoch_ <
  // global_cache_epoch_
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexCache is not inited", K(ret));
  } else if (OB_FAIL(
                 element_index_.get_refactored(nb->pk_ptr_.buf_str_, ele))) {
    if (OB_UNLIKELY(ret != OB_HASH_NOT_EXIST)) {
      LOG_WARN("fail to get element from element_index", K(ret));
    }
  }
  return ret;
}

int ObHNSWIndexCache::set_element_for_insert(sqlclient::ObMySQLResult *res,
                                             ObHNSWElement *&ele,
                                             int64_t ele_max_level) {
  int ret = OB_SUCCESS;
  void *ele_buf = nullptr;
  // init insert vector's ObHNSWElement
  // res pkey is guaranteed to be different.
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexCache is not inited", K(ret));
  } else if (OB_ISNULL(ele_buf = allocator_->alloc(sizeof(ObHNSWElement)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED; // TODO: memset 0
    LOG_WARN("fail to alloc HNSWElement", K(ret));
  } else if (OB_FALSE_IT(ele = new (ele_buf) ObHNSWElement())) {
  } else if (OB_FAIL(
                 copy_objs_to_const_pool(res, 0, pkey_count_, ele->pk_ptr_))) {
    LOG_WARN("fail to copy objs to const pool", K(ret), K(pkey_count_));
  } else if (OB_UNLIKELY(!ele->pk_ptr_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("objs is null", K(ret));
  } else if (OB_FAIL(
                 copy_vector_to_const_pool(res, pkey_count_, ele->vector_))) {
    LOG_WARN("fail to copy vector to const pool", K(ret), K(pkey_count_));
  } else if (OB_FAIL(ele->set_level(ele_max_level, allocator_))) {
    LOG_WARN("fail to set level", K(ret));
  } else if (OB_FAIL(
                 element_index_.set_refactored(ele->pk_ptr_.buf_str_, ele))) {
    LOG_WARN("fail to insert new vector", K(ret));
  }
  return ret;
}

int ObHNSWIndexCache::set_deep_copy_element(const ObHNSWElement &other,
                                            int skip_cnt, int64_t cache_epoch,
                                            ObHNSWElement *&new_ele) {
  int ret = OB_SUCCESS;
  new_ele = nullptr;
  void *ele_buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexCache is not inited", K(ret));
  } else if (OB_FAIL(element_index_.get_refactored(other.pk_ptr_.buf_str_,
                                                   new_ele))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      if (OB_ISNULL(ele_buf = allocator_->alloc(sizeof(ObHNSWElement)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED; // TODO: memset 0
        LOG_WARN("fail to alloc HNSWElement", K(ret));
      } else if (OB_FALSE_IT(new_ele = new (ele_buf) ObHNSWElement())) {
      } else if (OB_FAIL(new_ele->deep_copy(other, allocator_, skip_cnt,
                                            cache_epoch))) {
        LOG_WARN("fail to copy element", K(ret));
      } else if (OB_FAIL(element_index_.set_refactored(
                     new_ele->pk_ptr_.buf_str_, new_ele))) {
        LOG_WARN("fail to insert entry point vector", K(ret));
      }
    } else {
      LOG_WARN("fail to get element from element_index", K(ret));
    }
  }
  return ret;
}

bool ObHNSWIndexRingedCache::BatchTransInsertFunctor::operator()(
    const AddrInt &key, const ObHNSWSQLInsertRow &value) {
  int ret = OB_SUCCESS;
  ObSqlTransQueryStashDesc *stash_desc;
  if (OB_FAIL(trans_->get_stash_query(ring_cache_->in_ring_cache_.tenant_id_,
                                      "hnsw_index_tb", stash_desc))) {
    LOG_WARN("get_stash_query fail", K(ret),
             K(ring_cache_->in_ring_cache_.tenant_id_));
  } else if (stash_desc->get_stash_query().empty()) {
    if (OB_FAIL(stash_desc->get_stash_query().append_fmt(
            "insert into `%.*s`.`%.*s` %.*s values ",
            static_cast<int>(
                ring_cache_->in_ring_cache_.index_db_name_.length()),
            ring_cache_->in_ring_cache_.index_db_name_.ptr(),
            static_cast<int>(
                ring_cache_->in_ring_cache_.hnsw_index_table_name_.length()),
            ring_cache_->in_ring_cache_.hnsw_index_table_name_.ptr(),
            static_cast<int>(ring_cache_->insert_column_sql_str_.length()),
            ring_cache_->insert_column_sql_str_.ptr()))) {
      LOG_WARN("fail to append insert sql", K(ret),
               K(ring_cache_->in_ring_cache_.index_db_name_),
               K(ring_cache_->in_ring_cache_.hnsw_index_table_name_));
    } else if (OB_FAIL(ring_cache_->parse_insert_row_to_insert_values(
                   value, stash_desc->get_stash_query(), true))) {
      LOG_WARN("fail to parse insert row to insert values", K(ret));
    }
  } else if (OB_FAIL(ring_cache_->parse_insert_row_to_insert_values(
                 value, stash_desc->get_stash_query(), false))) {
    LOG_WARN("fail to parse insert row to insert values", K(ret));
  }
  if (OB_SUCC(ret)) {
    stash_desc->add_row_cnt(1);
    if (OB_FAIL(trans_->do_stash_query(static_cast<int>(batch_size_)))) {
      LOG_WARN("do_stash_query fail", K(ret));
    } else {
      reinterpret_cast<ObHNSWNeighbor *>(
          reinterpret_cast<void *>(key.addr_int_))
          ->status_ = ObHNSWNeighbor::ORIGINAL;
    }
  }
  ret_code_ = ret;
  return ret == OB_SUCCESS;
}

int ObHNSWIndexRingedCache::init(bool insert_mode) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHNSWIndexRingedCache inits twice", K(ret));
  } else if (OB_FAIL(in_ring_cache_.init())) {
    LOG_WARN("fail to init in-ring cache", K(ret));
  } else if (OB_FAIL(out_ring_cache_.init())) {
    LOG_WARN("fail to init out-ring cache", K(ret));
  } else if (OB_FAIL(insert_mode &&
                     OB_FAIL(inserts_v2_.create(10240, "HNSW", "HNSW",
                                                in_ring_cache_.tenant_id_)))) {
    LOG_WARN("fail to init insert_v2 index", K(ret));
  } else if (OB_FAIL(insert_mode && OB_FAIL(init_insert_column_sql_str()))) {
    LOG_WARN("fail to init insert column sql str", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObHNSWIndexRingedCache::init_insert_column_sql_str() {
  int ret = OB_SUCCESS;
  // vector data
  if (OB_FAIL(insert_column_sql_str_.append_fmt(
          "(%.*s",
          static_cast<int>(in_ring_cache_.vector_column_name_.length()),
          in_ring_cache_.vector_column_name_.ptr()))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  // base pkey
  for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
    if (OB_FAIL(insert_column_sql_str_.append_fmt(",base_pk%ld", i))) {
      LOG_WARN("fail to append sql", K(ret), K(i));
    }
  }
  // level
  if (OB_SUCC(ret) && OB_FAIL(insert_column_sql_str_.append_fmt(",level"))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  // ref pkey
  for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
    if (OB_FAIL(insert_column_sql_str_.append_fmt(",ref_pk%ld", i))) {
      LOG_WARN("fail to append sql", K(ret), K(i));
    }
  }
  // is_valid & distance
  if (OB_SUCC(ret) &&
      OB_FAIL(insert_column_sql_str_.append_fmt(",n_neighbors,distance)"))) {
    LOG_WARN("fail to append sql", K(ret));
  }
  return ret;
}

int ObHNSWIndexRingedCache::all_reset(common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexRingedCache is not inited", K(ret));
  } else if (OB_FAIL(write_all_updates(trans))) {
    LOG_WARN("fail to write all updates", K(ret));
  } else if (OB_FAIL(out_ring_cache_.reset())) {
    LOG_WARN("fail to reset out-ring cache", K(ret));
  } else if (OB_FAIL(in_ring_cache_.reset())) {
    LOG_WARN("fail to reset in-ring cache", K(ret));
  }
  return ret;
}

int ObHNSWIndexRingedCache::partial_reset(common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexRingedCache is not inited", K(ret));
  } else if (OB_FAIL(write_all_updates(trans))) {
    LOG_WARN("fail to write all updates", K(ret));
  } else if (OB_FAIL(out_ring_cache_.reset())) {
    LOG_WARN("fail to reset out-ring cache", K(ret));
  }
  return ret;
}

bool ObHNSWIndexRingedCache::valid_neighbor_element_ptr(
    ObHNSWNeighbor *nb, int cur_ele_skip_cnt, uint64_t global_cache_epoch) {
  int cur_cache_lv = get_cache_lv(cur_ele_skip_cnt);
  OB_ASSERT(-1 != nb->skip_cnt_);
  int nb_cache_lv = get_cache_lv(nb->skip_cnt_);
  if (0 == cur_cache_lv && 1 == nb_cache_lv &&
      global_cache_epoch != nb->cache_epoch_) {
    return false;
  }
  return true;
}

int ObHNSWIndexRingedCache::get_element_in_neighbor(
    common::ObMySQLTransaction *trans, ObHNSWNeighbor *nb, int cur_ele_skip_cnt,
    uint64_t global_cache_epoch, ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexRingedCache is not inited", K(ret));
  } else if (OB_NOT_NULL(nb->element_) &&
             valid_neighbor_element_ptr(nb, cur_ele_skip_cnt,
                                        global_cache_epoch)) {
    ele = nb->element_;
  } else if (OB_UNLIKELY(!nb->pk_ptr_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pkey ptr in ObHNSWNeighbor cannot be invalid", K(ret));
  } else {
    if (OB_FAIL(in_ring_cache_.get_element_in_neighbor_without_load(nb, ele))) {
      if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
        if (OB_FAIL(out_ring_cache_.get_element_in_neighbor_without_load(
                nb, ele))) {
          if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
            if (0 == get_cache_lv(cur_ele_skip_cnt + 1)) {
              if (OB_FAIL(in_ring_cache_.load_element(
                      trans, nb->pk_ptr_, cur_ele_skip_cnt + 1,
                      global_cache_epoch, nb->element_))) {
                LOG_WARN("fail to load element", K(ret));
              } else {
                ele = nb->element_;
                nb->skip_cnt_ = cur_ele_skip_cnt + 1;
                nb->cache_epoch_ = global_cache_epoch;
              }
            } else {
              if (OB_FAIL(out_ring_cache_.load_element(
                      trans, nb->pk_ptr_, cur_ele_skip_cnt + 1,
                      global_cache_epoch, nb->element_))) {
                LOG_WARN("fail to load element", K(ret));
              } else {
                ele = nb->element_;
                nb->skip_cnt_ = cur_ele_skip_cnt + 1;
                nb->cache_epoch_ = global_cache_epoch;
              }
            }
          } else {
            LOG_WARN(
                "fail to get element in neighbor without load (out_ring_cache)",
                K(ret));
          }
        } else {
          nb->element_ = ele; // modify nb->element_
          nb->skip_cnt_ = ele->skip_cnt_;
          nb->cache_epoch_ = ele->cache_epoch_;
        }
      } else {
        LOG_WARN("fail to get element in neighbor without load (in_ring_cache)",
                 K(ret));
      }
    } else {
      nb->element_ = ele; // modify nb->element_
      nb->skip_cnt_ = ele->skip_cnt_;
      nb->cache_epoch_ = ele->cache_epoch_;
    }
    // TODO:
  }
  return ret;
}

int ObHNSWIndexRingedCache::set_element_for_insert(
    ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
    ObHNSWElement *&ele, int64_t ele_max_level, int skip_cnt,
    uint64_t global_cache_epoch,
    common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexRingedCache is not inited", K(ret));
  } else if (skip_cnt <= ring_radius_) {
    // insert into in_ring_cache_
    if (OB_FAIL(
            in_ring_cache_.set_element_for_insert(res, ele, ele_max_level))) {
      LOG_WARN("fail to set element in in_ring_cache_", K(ret),
               K(ele_max_level));
    }
  } else {
    // insert into out_ring_cache_
    if (OB_FAIL(
            out_ring_cache_.set_element_for_insert(res, ele, ele_max_level))) {
      LOG_WARN("fail to set element in out_ring_cache_", K(ret),
               K(ele_max_level));
    }
  }
  if (OB_SUCC(ret)) {
    ele->skip_cnt_ = skip_cnt;
    ele->cache_epoch_ = global_cache_epoch;
  }

  /* add_connection_for_inserted_element */
  for (int64_t i = 0; OB_SUCC(ret) && i < selected_neighbors_lv.count(); ++i) {
    common::ObArray<ObHNSWCandidate> &selected_neighbors =
        selected_neighbors_lv.at(i);
    int64_t lv = selected_neighbors_lv.count() - 1 - i;
    if (OB_FAIL(
            add_connection_for_inserted_element(ele, selected_neighbors, lv))) {
      LOG_WARN("fail to add connection for inserted element", K(ret), K(lv));
    }
  }

  /* construct update sql */
  if (OB_SUCC(ret) &&
      OB_FAIL(update_connection_for_neighbors(
          idx_builder, skip_cnt, global_cache_epoch, ele, ele_max_level))) {
    LOG_WARN("fail to update connection fro inserted element", K(ret),
             K(skip_cnt), K(global_cache_epoch), K(ele_max_level));
  }

  /* try to do delete sql */
  if (OB_SUCC(ret) &&
      OB_FAIL(do_deletes(idx_builder->get_trans(), delete_batch_threshold_))) {
    LOG_WARN("fail to do sql", K(ret));
  }
  return ret;
}

int ObHNSWIndexRingedCache::print_vector_to_sql(ObSqlString &sql,
                                                const ObTypeVector &vector,
                                                bool print_comma) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt(print_comma ? ",'[" : "'["))) {
    LOG_WARN("fail to append sql", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < vector.dims(); ++i) {
      if (OB_FAIL(sql.append_fmt(i == 0 ? "%f" : ",%f", vector.at(i)))) {
        LOG_WARN("fail to append sql", K(ret), K(vector.at(i)));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt("]'"))) {
      LOG_WARN("fail to append sql", K(ret));
    }
  }
  return ret;
}

int ObHNSWIndexRingedCache::parse_insert_row_to_insert_values(
    const ObHNSWSQLInsertRow &insert_row, ObSqlString &sql,
    bool first_value_list) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.append_fmt(first_value_list ? "(" : ",("))) {
    LOG_WARN("fail to append sql", K(ret));
  } else {
    lib::ObMemAttr mattr(in_ring_cache_.tenant_id_, "HNSW");
    ObArenaAllocator tmp_allocator(mattr);
    ObCastCtx cast_ctx(&tmp_allocator, NULL, CM_NONE,
                       ObCharset::get_system_collation());
    // vector data
    if (OB_SUCC(ret) &&
        OB_FAIL(print_vector_to_sql(sql, insert_row.data_, false))) {
      LOG_WARN("fail to append sql", K(ret));
    }
    // base pkey
    ObObj tmp_obj;
    for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
      const bool is_str =
          insert_row.base_pk_ptr_.objs_[i].is_string_or_lob_locator_type();
      if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx,
                                       insert_row.base_pk_ptr_.objs_[i],
                                       tmp_obj))) {
        LOG_WARN("fail to cast obj to varchar", K(ret));
      } else if (OB_FAIL(sql.append_fmt(
                     is_str ? ",'%.*s'" : ",%.*s",
                     static_cast<int>(tmp_obj.get_string().length()),
                     tmp_obj.get_string().ptr()))) {
        LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
      }
    }
    // level
    if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(",%ld", insert_row.level_))) {
      LOG_WARN("fail to append sql", K(ret), K(insert_row.level_));
    }
    // ref pkey
    for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
      const bool is_str =
          (insert_row.ref_pk_ptr_.objs_ == nullptr || !insert_row.is_valid_)
              ? insert_row.base_pk_ptr_.objs_[i].is_string_or_lob_locator_type()
              : insert_row.ref_pk_ptr_.objs_[i].is_string_or_lob_locator_type();
      if (OB_FAIL(ObObjCaster::to_type(
              ObVarcharType, cast_ctx,
              (insert_row.ref_pk_ptr_.objs_ == nullptr || !insert_row.is_valid_)
                  ? insert_row.base_pk_ptr_.objs_[i]
                  : insert_row.ref_pk_ptr_.objs_[i],
              tmp_obj))) {
        LOG_WARN("fail to cast obj to varchar", K(ret));
      } else if (OB_FAIL(sql.append_fmt(
                     is_str ? ",'%.*s'" : ",%.*s",
                     static_cast<int>(tmp_obj.get_string().length()),
                     tmp_obj.get_string().ptr()))) {
        LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
      }
    }
    // is_valid
    if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(",%d", insert_row.is_valid_))) {
      LOG_WARN("fail to append sql", K(ret), K(insert_row.is_valid_));
    }
    // distance
    if (OB_SUCC(ret) && OB_FAIL(sql.append_fmt(",%lf", insert_row.distance_))) {
      LOG_WARN("fail to append sql", K(ret), K(insert_row.distance_));
    }

    if (OB_FAIL(sql.append_fmt(")"))) {
      LOG_WARN("fail to append sql", K(ret));
    }
  }
  return ret;
}

int ObHNSWIndexRingedCache::parse_delete_row_to_delete_where_clause(
    ObHNSWSQLDeleteRow &delete_row, ObSqlString &sql) {
  int ret = OB_SUCCESS;
  lib::ObMemAttr mattr(in_ring_cache_.tenant_id_, "HNSW");
  ObArenaAllocator tmp_allocator(mattr);
  ObCastCtx cast_ctx(&tmp_allocator, NULL, CM_NONE,
                     ObCharset::get_system_collation());
  // base pkey
  ObObj tmp_obj;
  for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx,
                                     delete_row.base_pk_ptr_.objs_[i],
                                     tmp_obj))) {
      LOG_WARN("fail to cast obj to varchar", K(ret));
    } else if (OB_FAIL(sql.append_fmt(
                   i == 0 ? "base_pk%ld = %.*s " : "and base_pk%ld = %.*s ",
                   i, // TODO:shk
                   static_cast<int>(tmp_obj.get_string().length()),
                   tmp_obj.get_string().ptr()))) {
      LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
    }
  }
  // level
  if (OB_SUCC(ret) &&
      OB_FAIL(sql.append_fmt("and level = %ld ", delete_row.level_))) {
    LOG_WARN("fail to append sql", K(ret), K(delete_row.level_));
  }
  // ref pkey
  for (int64_t i = 0; OB_SUCC(ret) && i < in_ring_cache_.pkey_count_; ++i) {
    OB_ASSERT(OB_NOT_NULL(delete_row.ref_pk_ptr_.objs_));
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx,
                                     delete_row.ref_pk_ptr_.objs_[i],
                                     tmp_obj))) {
      LOG_WARN("fail to cast obj to varchar", K(ret));
    } else if (OB_FAIL(sql.append_fmt(
                   "and ref_pk%ld = %.*s ", i, // TODO:shk
                   static_cast<int>(tmp_obj.get_string().length()),
                   tmp_obj.get_string().ptr()))) {
      LOG_WARN("fail to append sql", K(ret), K(tmp_obj.get_string()));
    }
  }
  return ret;
}

int ObHNSWIndexRingedCache::do_deletes(common::ObMySQLTransaction *trans,
                                       int64_t real_delete_bach_sz) {
  int ret = OB_SUCCESS;
  int64_t delete_cnt = deletes_.count();
  if (real_delete_bach_sz > delete_cnt) {
    // do nothing
  } else {
    LOG_INFO("############ hnsw do_deletes start", K(ret), K(deletes_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < deletes_.count(); ++i) {
      ObSqlString delete_sql;
      int64_t affected_row = 0;
      if (OB_FAIL(delete_sql.append_fmt(
              "delete from `%.*s`.`%.*s` where ",
              static_cast<int>(in_ring_cache_.index_db_name_.length()),
              in_ring_cache_.index_db_name_.ptr(),
              static_cast<int>(in_ring_cache_.hnsw_index_table_name_.length()),
              in_ring_cache_.hnsw_index_table_name_.ptr()))) {
        LOG_WARN("fail to append delete sql", K(ret),
                 K(in_ring_cache_.index_db_name_),
                 K(in_ring_cache_.hnsw_index_table_name_));
      } else if (OB_FAIL(parse_delete_row_to_delete_where_clause(deletes_.at(i),
                                                                 delete_sql))) {
        LOG_WARN("fail to parse delete_row to delete where clause", K(ret),
                 K(deletes_.at(i)));
      } else if (OB_FAIL(trans->write(in_ring_cache_.tenant_id_,
                                      delete_sql.ptr(), affected_row))) {
        LOG_WARN("fail to execute delete", K(ret), K(delete_sql));
      }
    }
    if (OB_SUCC(ret)) {
      deletes_.reset();
    }
    LOG_INFO("############ hnsw do_deletes end", K(ret));
  }
  return ret;
}

int ObHNSWIndexRingedCache::do_inserts(common::ObMySQLTransaction *trans,
                                       int64_t real_insert_bach_sz) {
  int ret = OB_SUCCESS;
  BatchTransInsertFunctor functor(trans, this, real_insert_bach_sz);
  LOG_INFO("############ hnsw do_inserts start", K(ret), K(inserts_v2_.size()));
  int64_t inserts_cnt = 0;
  for (auto &it : inserts_v2_) {
    if (!functor(it.first, it.second)) {
      ret = functor.get_ret_code();
      LOG_WARN("fail to do batch trans insert", K(ret));
      break;
    }
    ++inserts_cnt;
    if (0 == inserts_cnt % 1000) {
      LOG_INFO("############ hnsw do_inserts cnt", K(ret), K(inserts_cnt));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans->do_stash_query(1))) {
    LOG_WARN("fail to trans stash query", K(ret));
  } else {
    inserts_v2_.reuse();
  }
  LOG_INFO("############ hnsw do_inserts finish", K(ret));
  return ret;
}

int ObHNSWIndexRingedCache::write_all_updates(
    common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_deletes(trans, 1))) {
    LOG_WARN("fail to do remain deletes", K(ret));
  } else if (OB_FAIL(do_inserts(trans, insert_batch_threshold_))) {
    LOG_WARN("fail to do inserts", K(ret), K(insert_batch_threshold_));
  } else if (OB_FAIL(trans->do_stash_query(1))) {
    LOG_WARN("fail to do remain inserts", K(ret));
  }
  LOG_INFO("############ hnsw do_stash_query finish", K(ret));
  return ret;
}

int ObHNSWIndexRingedCache::add_connection_for_inserted_element(
    ObHNSWElement *element, common::ObArray<ObHNSWCandidate> &cds, int64_t lv) {
  int ret = OB_SUCCESS;
  OB_ASSERT(element->neighbors_per_level_[lv] != nullptr);
  for (int64_t i = 0; OB_SUCC(ret) && i < cds.count(); ++i) {
    OB_ASSERT(cds.at(i).element_->pk_ptr_.is_valid());
    ObHNSWCPoolPtr pk_ptr;
    if (is_escape_connection(element->skip_cnt_,
                             cds.at(i).element_->skip_cnt_)) {
      if (OB_FAIL(ObHNSWElement::alloc_and_deep_copy_cpool_ptr(
              pk_ptr, cds.at(i).element_->pk_ptr_,
              in_ring_cache_.allocator_))) {
        LOG_WARN("fail to alloc and deep copy cpool ptr", K(ret));
      }
    } else {
      pk_ptr = cds.at(i).element_->pk_ptr_;
    }
    if (OB_SUCC(ret)) {
      element->neighbors_per_level_[lv]->add_origin_neighbor(
          cds.at(i).candi_distance_, pk_ptr, ObHNSWNeighbor::ADDED,
          cds.at(i).element_, cds.at(i).element_->skip_cnt_,
          cds.at(i).element_->cache_epoch_);
    }
  }
  return ret;
}

int ObHNSWIndexRingedCache::get_neighbors(ObHNSWElement *ele, int64_t lv,
                                          ObHNSWLevelNeighbors *&neighbors) {
  int ret = OB_SUCCESS;
  neighbors = nullptr;
  if (OB_NOT_NULL(ele->neighbors_per_level_[lv])) {
    neighbors = ele->neighbors_per_level_[lv];
  } else {
    ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("fail to load hnsw neighbors with sql", K(ret), K(lv));
    LOG_WARN("neighbors_per_level_ cannot be null", K(ret), K(lv));
  }
  return ret;
}

int ObHNSWIndexRingedCache::update_connection_for_neighbors(
    ObHNSWIndexBuilder *idx_builder, int cur_ele_skip_cnt,
    uint64_t global_cache_epoch, ObHNSWElement *element, int64_t lv) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i <= lv; ++i) {
    ObHNSWLevelNeighbors *lv_nbs = nullptr;
    if (OB_ISNULL(lv_nbs = element->neighbors_per_level_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("element neighbors is null", K(ret), K(i));
    } else if (lv_nbs->n_nb_ == 0) {
      if (OB_FAIL(inserts_v2_.set_refactored(
              AddrInt(lv_nbs->nbs_ + ObHNSWLevelNeighbors::MAX_NEIGHBORS_NUM),
              ObHNSWSQLInsertRow{element->pk_ptr_, i, ObHNSWCPoolPtr(),
                                 element->vector_, 0, 0.0}))) {
        LOG_WARN("fail to insert ObHNSWSQLInsertRow", K(ret));
      }
    } else {
      for (int64_t nb_idx = 0; OB_SUCC(ret) && nb_idx < lv_nbs->n_nb_;
           ++nb_idx) {
        if (OB_FAIL(inserts_v2_.set_refactored(
                AddrInt(lv_nbs->nbs_ + nb_idx),
                ObHNSWSQLInsertRow{
                    element->pk_ptr_, i, lv_nbs->nbs_[nb_idx].pk_ptr_,
                    element->vector_, 1, lv_nbs->nbs_[nb_idx].distance_}))) {
          LOG_WARN("fail to insert ObHNSWSQLInsertRow", K(ret));
        }
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i <= lv; ++i) {
    ObHNSWLevelNeighbors *lv_nbs = nullptr;
    if (OB_ISNULL(lv_nbs = element->neighbors_per_level_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("element neighbors is null", K(ret), K(i));
    } else if (lv_nbs->n_nb_ == 0) {
    } else {
      ObHNSWElement *neighbor_e = nullptr;
      ObHNSWLevelNeighbors *lv_neighbors = nullptr;
      bool is_esc_conn = false;
      // for each neighbor on level `lv`
      for (int64_t nb_idx = 0; OB_SUCC(ret) && nb_idx < lv_nbs->n_nb_;
           ++nb_idx) {
        if (OB_FAIL(get_element_in_neighbor(
                idx_builder->get_trans(), &(lv_nbs->nbs_[nb_idx]),
                cur_ele_skip_cnt, global_cache_epoch, neighbor_e))) {
          LOG_WARN("fail to get element in neighbor", K(ret), K(nb_idx));
        } else if (OB_FAIL(get_neighbors(neighbor_e, i, lv_neighbors))) {
          LOG_WARN("fail to get neighbors in element", K(ret), K(i));
        } else if (OB_FALSE_IT(is_esc_conn =
                                   is_escape_connection(neighbor_e->skip_cnt_,
                                                        element->skip_cnt_))) {
        } else if (lv_neighbors->n_nb_ >=
                   idx_builder->get_opt().get_max_m_on_lv(i)) {
          int64_t prune_idx = -1;
          if (OB_FAIL(idx_builder->prune_connections(
                  this, global_cache_epoch, lv_neighbors, i, element,
                  lv_nbs->nbs_[nb_idx].distance_, neighbor_e->skip_cnt_,
                  prune_idx, lv))) {
            LOG_WARN("fail to prune connections", K(ret));
          } else if (-1 == prune_idx) {
          } else {
            // check neighbor status
            // ADDED : just update kv in inserts_
            // ORIGINAL : 1. add record to deletes_
            //            2. add record to inserts_
            ObHNSWNeighbor::NeighborStatus stat =
                lv_neighbors->nbs_[prune_idx].status_;
            if (ObHNSWNeighbor::ADDED == stat) {
              ObHNSWSQLInsertRow assert_tmp;
              OB_ASSERT(inserts_v2_.get_refactored(
                            AddrInt(lv_neighbors->nbs_ + prune_idx),
                            assert_tmp) == OB_SUCCESS);
              if (OB_FAIL(inserts_v2_.set_refactored(
                      AddrInt(lv_neighbors->nbs_ + prune_idx),
                      ObHNSWSQLInsertRow{neighbor_e->pk_ptr_, i,
                                         element->pk_ptr_, neighbor_e->vector_,
                                         1, lv_nbs->nbs_[nb_idx].distance_},
                      1))) {
                LOG_WARN("fail to update inserts_v2", K(ret));
              }
            } else if (ObHNSWNeighbor::ORIGINAL == stat) {
              if (OB_FAIL(deletes_.push_back(ObHNSWSQLDeleteRow{
                      neighbor_e->pk_ptr_, i,
                      lv_neighbors->nbs_[prune_idx].pk_ptr_}))) {
                LOG_WARN("fail to insert deletes_", K(ret));
              } else if (OB_FAIL(inserts_v2_.set_refactored(
                             AddrInt(lv_neighbors->nbs_ + prune_idx),
                             ObHNSWSQLInsertRow{
                                 neighbor_e->pk_ptr_, i, element->pk_ptr_,
                                 neighbor_e->vector_, 1,
                                 lv_nbs->nbs_[nb_idx].distance_}))) {
                LOG_WARN("fail to insert inserts_v2", K(ret));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected neighbor status", K(ret), K(stat));
            }
            if (OB_SUCC(ret)) {
              // replace it
              // TODO: maybe copy element pk_ptr
              ObHNSWCPoolPtr pk_ptr;
              if (is_esc_conn) {
                if (OB_FAIL(ObHNSWElement::alloc_and_deep_copy_cpool_ptr(
                        pk_ptr, element->pk_ptr_, in_ring_cache_.allocator_))) {
                  LOG_WARN("fail to alloc and deep copy cpool ptr", K(ret));
                }
              } else {
                pk_ptr = element->pk_ptr_;
              }
              if (OB_SUCC(ret)) {
                lv_neighbors->replace_neighbor_with_ele(
                    prune_idx, element, pk_ptr, lv_nbs->nbs_[nb_idx].distance_);
              }
            }
          }
        } else {
          // add element as new neighbor
          // TODO: maybe copy element pk_ptr
          ObHNSWCPoolPtr pk_ptr;
          if (is_esc_conn) {
            if (OB_FAIL(ObHNSWElement::alloc_and_deep_copy_cpool_ptr(
                    pk_ptr, element->pk_ptr_, in_ring_cache_.allocator_))) {
              LOG_WARN("fail to alloc and deep copy cpool ptr", K(ret));
            }
          } else {
            pk_ptr = element->pk_ptr_;
          }
          lv_neighbors->add_origin_neighbor(
              lv_nbs->nbs_[nb_idx].distance_, pk_ptr, ObHNSWNeighbor::ADDED,
              element, element->skip_cnt_, element->cache_epoch_);
          if (OB_FAIL(inserts_v2_.set_refactored(
                  AddrInt(lv_neighbors->nbs_ + lv_neighbors->n_nb_ - 1),
                  ObHNSWSQLInsertRow{neighbor_e->pk_ptr_, i, element->pk_ptr_,
                                     neighbor_e->vector_, 1,
                                     lv_nbs->nbs_[nb_idx].distance_}))) {
            LOG_WARN("fail to insert inserts_v2_", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::init(bool insert_mode) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache inits twice", K(ret));
  } else if (OB_FAIL(cache1_.init(insert_mode))) {
    LOG_WARN("fail to init cache1", K(ret));
  } else if (OB_FAIL(cache2_.init(insert_mode))) {
    LOG_WARN("fail to init cache2", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::write_all_updates(
    common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(caches_[cur_cache_id_]->write_all_updates(trans))) {
    LOG_WARN("fail to write all updates", K(ret));
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::all_reset(
    common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(caches_[cur_cache_id_ ^ 1]->all_reset(trans))) {
    LOG_WARN("fail to all_set ObHNSWIndexRingedCache", K(ret));
  } else if (OB_FAIL(caches_[cur_cache_id_]->all_reset(trans))) {
    LOG_WARN("fail to all_set ObHNSWIndexRingedCache", K(ret));
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::partial_reset(
    int cache_idx, common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(caches_[cache_idx]->all_reset(trans))) {
    LOG_WARN("fail to all_set ObHNSWIndexRingedCache", K(ret));
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::prune_out_ring_cache(
    common::ObMySQLTransaction *trans) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_NOT_NULL(trans) &&
             OB_FAIL(caches_[cur_cache_id_]->partial_reset(trans))) {
    LOG_WARN("fail to partial reset ringed cache", K(ret));
  } else {
    ++global_cache_epoch_;
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::get_neighbors(
    ObHNSWElement *ele, int64_t lv, ObHNSWLevelNeighbors *&neighbors) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(
                 caches_[cur_cache_id_]->get_neighbors(ele, lv, neighbors))) {
    LOG_WARN("fail to get element in neighbor", K(ret));
  }
  return ret;
}

/**
 *  nb: the neighbor meta info in current visited element
 *  cur_ele_skip_cnt: the skip_cnt of current visited element
 */
int ObHNSWIndexSwitchableRingedCache::get_element_in_neighbor(
    common::ObMySQLTransaction *trans, ObHNSWNeighbor *nb, int cur_ele_skip_cnt,
    ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(caches_[cur_cache_id_]->get_element_in_neighbor(
                 trans, nb, cur_ele_skip_cnt, global_cache_epoch_, ele))) {
    LOG_WARN("fail to get element in neighbor", K(ret));
  }
  return ret;
}

/**
 *  skip_cnt: the skip_cnt from entry point in the newly inserted element
 */
int ObHNSWIndexSwitchableRingedCache::set_element_for_insert(
    ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
    ObHNSWElement *&ele, int64_t ele_max_level, int skip_cnt,
    common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(caches_[cur_cache_id_]->set_element_for_insert(
                 idx_builder, res, ele, ele_max_level, skip_cnt,
                 global_cache_epoch_, selected_neighbors_lv))) {
    LOG_WARN("fail to set element for insert", K(ret));
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::set_new_ep(
    ObHNSWIndexBuilder *idx_builder, sqlclient::ObMySQLResult *res,
    ObHNSWElement *&ele, int64_t ele_max_level,
    common::ObArray<common::ObArray<ObHNSWCandidate>> &selected_neighbors_lv) {
  int ret = OB_SUCCESS;
  ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
  ObHNSWIndexRingedCache *new_epoch_cache = caches_[cur_cache_id_ ^ 1];
  int64_t new_epoch = global_cache_epoch_ + 1;
  if (OB_FAIL(new_epoch_cache->in_ring_cache_.set_element_for_insert(
          res, ele, ele_max_level))) {
    LOG_WARN("fail to set new entry point", K(ret));
  } else {
    ele->skip_cnt_ = 0;
    ele->cache_epoch_ = new_epoch;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < selected_neighbors_lv.count(); ++i) {
    common::ObArray<ObHNSWCandidate> &selected_neighbors =
        selected_neighbors_lv.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < selected_neighbors.count(); ++j) {
      ObHNSWCandidate cd = selected_neighbors.at(j);
      ObHNSWElement *new_ele = nullptr;
      int rad = new_epoch_cache->ring_radius_;
      if (1 <= rad &&
          OB_FAIL(new_epoch_cache->in_ring_cache_.set_deep_copy_element(
              *cd.element_, 1, new_epoch, new_ele))) {
        LOG_WARN("fail to set deep copy element", K(ret), K(new_epoch));
      } else if (1 > rad &&
                 OB_FAIL(new_epoch_cache->out_ring_cache_.set_deep_copy_element(
                     *cd.element_, 1, new_epoch, new_ele))) {
        LOG_WARN("fail to set deep copy element", K(ret), K(new_epoch));
      } else {
        selected_neighbors.at(j).element_ = new_ele;
        selected_neighbors.at(j).skip_cnt_ = 1;
      }
    }
  }

  // write updates before loading element in new cache
  if (OB_SUCC(ret) && partial_reset(cur_cache_id_, idx_builder->get_trans())) {
    LOG_WARN("fail to partial reset", K(ret), K(cur_cache_id_));
  }

  /* add_connection_for_inserted_element */
  for (int64_t i = 0; OB_SUCC(ret) && i < selected_neighbors_lv.count(); ++i) {
    common::ObArray<ObHNSWCandidate> &selected_neighbors =
        selected_neighbors_lv.at(i);
    int64_t lv = selected_neighbors_lv.count() - 1 - i;
    if (OB_FAIL(new_epoch_cache->add_connection_for_inserted_element(
            ele, selected_neighbors, lv))) {
      LOG_WARN("fail to add connection for inserted element", K(ret), K(lv));
    }
  }

  /* construct update sql */
  if (OB_SUCC(ret) && OB_FAIL(new_epoch_cache->update_connection_for_neighbors(
                          idx_builder, 0, new_epoch, ele, ele_max_level))) {
    LOG_WARN("fail to update connection fro inserted element", K(ret),
             K(new_epoch), K(ele_max_level));
  }

  /* try to do delete sql */
  if (OB_SUCC(ret) && OB_FAIL(new_epoch_cache->do_deletes(
                          idx_builder->get_trans(),
                          new_epoch_cache->delete_batch_threshold_))) {
    LOG_WARN("fail to do sql", K(ret));
  }

  if (OB_SUCC(ret)) {
    ++global_cache_epoch_;
    cur_cache_id_ ^= 1;
  }
  return ret;
}

int ObHNSWIndexSwitchableRingedCache::load_ep_element(
    common::ObMySQLTransaction *trans, ObSqlString &sql, ObHNSWElement *&ele) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWIndexSwitchableRingedCache is not inited", K(ret));
  } else if (OB_FAIL(
                 caches_[cur_cache_id_]->load_ep_element(trans, sql, ele))) {
    LOG_WARN("fail to load ep element", K(ret));
  }
  return ret;
}

void ObHNSWIndexSwitchableRingedCache::show_cache_info() {
  ObHNSWIndexRingedCache *cur_cache = caches_[cur_cache_id_];
  int64_t in_ring_vector_cnt = cur_cache->in_ring_cache_.element_index_.size();
  int64_t out_ring_vector_cnt =
      cur_cache->out_ring_cache_.element_index_.size();
  int64_t in_ring_mem_use = cur_cache->in_ring_cache_.allocator_->used();
  int64_t out_ring_mem_use = cur_cache->out_ring_cache_.allocator_->used();
  LOG_INFO("@@@@@@ hnsw cache info @@@@@@@@", K(global_cache_epoch_),
           K(in_ring_vector_cnt), K(in_ring_mem_use), K(out_ring_vector_cnt),
           K(out_ring_mem_use));
}

} // namespace share
} // namespace oceanbase