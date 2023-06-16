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

#define USING_LOG_PREFIX SQL_SESSION
#include "sql/session/ob_session_val_map.h"

#include "lib/hash/ob_hashutils.h"
#include "share/schema/ob_schema_struct.h"
using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace share;
using namespace share::schema;

ObSessionValMap::ObSessionValMap()
    : block_allocator_(SMALL_BLOCK_SIZE, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                       ObMalloc(ObModIds::OB_SQL_SESSION_VAR_MAP)),
    var_name_val_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
    str_buf1_(ObModIds::OB_SQL_SESSION_VAR_MAP, 64 * 1024),
    str_buf2_(ObModIds::OB_SQL_SESSION_VAR_MAP, 64 * 1024),
    current_buf_index_(0),
    bucket_allocator_(ObModIds::OB_SQL_SESSION_VAR_MAP),
    bucket_allocator_wrapper_(&bucket_allocator_),
    str_buf_free_threshold_(0),
    next_free_mem_point_(str_buf_free_threshold_)
{
  str_buf_[0] = &str_buf1_;
  str_buf_[1] = &str_buf2_;
}

ObSessionValMap::ObSessionValMap(const int64_t block_size,
                                 const ObWrapperAllocator &block_allocator,
                                 const int64_t tenant_id)
    : block_allocator_(SMALL_BLOCK_SIZE, common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                       ObMalloc(ObMemAttr(tenant_id, ObModIds::OB_SQL_SESSION_VAR_MAP))),
    var_name_val_map_allocer_(block_size, block_allocator),
    str_buf1_(ObModIds::OB_SQL_SESSION_VAR_MAP),
    str_buf2_(ObModIds::OB_SQL_SESSION_VAR_MAP),
    current_buf_index_(0),
    bucket_allocator_(ObMemAttr(tenant_id, ObModIds::OB_SQL_SESSION_VAR_MAP)),
    bucket_allocator_wrapper_(&bucket_allocator_),
    str_buf_free_threshold_(0),
    next_free_mem_point_(str_buf_free_threshold_)
{
  str_buf_[0] = &str_buf1_;
  str_buf_[1] = &str_buf2_;
}

ObSessionValMap::~ObSessionValMap()
{
}

int ObSessionValMap::init(int64_t free_threshold,
                          int64_t bucket_num,
                          common::ObWrapperAllocator *bucket_allocator)
{
  int ret = OB_SUCCESS;
  str_buf_free_threshold_ = free_threshold;
  next_free_mem_point_ = str_buf_free_threshold_;
  if (NULL != bucket_allocator) {
    bucket_allocator_wrapper_.set_alloc(bucket_allocator);
  }

  if (OB_FAIL(map_.create(bucket_num, &var_name_val_map_allocer_,
                                       &bucket_allocator_wrapper_))) {
    _OB_LOG(WARN, "fail to create hashmap:ret[%d]", ret);
  }
  return ret;
}

void ObSessionValMap::reuse()
{
  map_.clear();
  str_buf1_.reset();
  str_buf2_.reset();
}

void ObSessionValMap::reset()
{
  reuse();
  map_.destroy();
  var_name_val_map_allocer_.reset();
  bucket_allocator_.reset();
  block_allocator_.reset();
}

// 成功则返回OB_SUCCESS
// 其他返回码均表示不成功
int ObSessionValMap::set_refactored(const common::ObString &name, const ObSessionVariable &sess_var)
{
  int ret = 0;
  ObString tmp_name;
  ObSessionVariable tmp_sess_var;
  if (current_buf_index_ >= 2 || current_buf_index_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid current_buf_index_", K(current_buf_index_), K(name), K(sess_var));
  } else if (OB_ISNULL(str_buf_[current_buf_index_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid internal args", KP(str_buf_[current_buf_index_]), K(name), K(sess_var));
  } else if (OB_FAIL(free_mem())) {
    LOG_WARN("fail to free mem", K(ret));
  } else if (OB_FAIL(str_buf_[current_buf_index_]->write_string(name, &tmp_name))) {
    LOG_WARN("fail to write name to string_buf", K(ret), K(current_buf_index_),
             K(name), K(sess_var));
  } else if (OB_FAIL(str_buf_[current_buf_index_]->write_obj(sess_var.value_,
                                                             &tmp_sess_var.value_))) {
    LOG_WARN("fail to write obj to string_buf", K(ret), K(current_buf_index_),
             K(name), K(sess_var));
  } else if (FALSE_IT(tmp_sess_var.meta_ = sess_var.meta_)) {
  } else if (OB_FAIL(map_.set_refactored(tmp_name, tmp_sess_var, 1))) {
    LOG_WARN("fail to add item to hash", K(ret), K(tmp_name), K(tmp_sess_var));
  }
  return ret;
}

int ObSessionValMap::get_refactored(const ObString &name, ObSessionVariable &sess_var) const
{
  return map_.get_refactored(name, sess_var);
}

const ObSessionVariable *ObSessionValMap::get(const common::ObString &name) const
{
  return map_.get(name);
}

int ObSessionValMap::erase_refactored(const ObString &key, ObSessionVariable *sess_var)
{
  return map_.erase_refactored(key, sess_var);
}

int ObSessionValMap::assign(const ObSessionValMap &other)
{
  int ret = OB_SUCCESS;
  reuse();
  if (other.size() > 0) {
    for (VarNameValMap::const_iterator iter = other.map_.begin();
       (OB_SUCCESS ==ret ) && iter != other.map_.end(); iter++) {
       const ObString &key = iter->first;
       const ObSessionVariable &value = iter->second;
       ret = set_refactored(key, value);
    }
  }
  return ret;
}

int ObSessionValMap::free_mem()
{
  int ret = OB_SUCCESS;
  if (current_buf_index_ >= 2 || current_buf_index_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid current_buf_index_", K(current_buf_index_));
  } else if (OB_ISNULL(str_buf_[current_buf_index_])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid internal args", KP(str_buf_[current_buf_index_]));
  } else if (str_buf_[current_buf_index_]->used() > next_free_mem_point_) {
    _OB_LOG(DEBUG, "string buf[%d] is large[%ld], switch string buf", current_buf_index_,
              str_buf_[current_buf_index_]->used());
    int32_t next_index = (current_buf_index_ == 0 ? 1 : 0);

    int64_t count = 0;
    if (map_.size() > 0) {
      VarNameValMap::iterator iter;
      for (iter = map_.begin(); OB_SUCC(ret) && iter != map_.end(); ++ iter) {
        count ++;
        if (OB_FAIL(str_buf_[next_index]->write_string(iter->first, &(iter->first)))) {
          _OB_LOG(WARN, "fail to write string:ret[%d]", ret);
        } else if (OB_FAIL(str_buf_[next_index]->write_obj(iter->second.value_,
                                                           &(iter->second.value_)))) {
          _OB_LOG(WARN, "fail to write obj:ret[%d]", ret);
        }
      }
    }
    _OB_LOG(DEBUG, "[%ld] val migrate", count);
    if (OB_SUCC(ret)) {
      str_buf_[current_buf_index_]->reuse();
      current_buf_index_ = next_index;
      next_free_mem_point_ = std::max(2 * str_buf_[next_index]->used(), str_buf_free_threshold_);
      _OB_LOG(DEBUG, "next_free_mem_point_[%ld]", next_free_mem_point_);
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObSessionValMap)
{
  int ret = OB_SUCCESS;
  int64_t num_of_entries = size();
  OB_UNIS_ENCODE(num_of_entries);
  if (OB_SUCC(ret) && num_of_entries > 0) {
    for (VarNameValMap::const_iterator iter = map_.begin();
        (OB_SUCC(ret)) && iter != map_.end(); iter++) {
      const ObString &key = iter->first;
      const ObSessionVariable &value = iter->second;
      OB_UNIS_ENCODE(key);
      OB_UNIS_ENCODE(value.meta_);
      OB_UNIS_ENCODE(value.value_);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSessionValMap)
{
  int ret = OB_SUCCESS;
  int64_t num_of_entries = 0;
  reuse();
  OB_UNIS_DECODE(num_of_entries);
  ObString key;
  ObSessionVariable value;
  if (num_of_entries > 0 && !map_.created()) {
    if (OB_FAIL(init(256 * 1024, 64, NULL))) {
      SQL_ENG_LOG(WARN, "init map failed", K(ret));
    }
  }
  for (int64_t i = 0; (OB_SUCC(ret)) && (i < num_of_entries); ++i) {
    key.reset();
    value.reset();
    OB_UNIS_DECODE(key);
    OB_UNIS_DECODE(value.meta_);
    OB_UNIS_DECODE(value.value_);
    ret = this->set_refactored(key,value);
    if (OB_FAIL(ret)) {
      SQL_ENG_LOG(WARN, "Insert value into map failed", K(ret));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSessionValMap)
{
  int64_t len = 0;
  int64_t num_of_entries = size();
  OB_UNIS_ADD_LEN(num_of_entries);
  if (num_of_entries > 0) {
    for (VarNameValMap::const_iterator iter = map_.begin();
        (iter != map_.end()); iter++) {
      const ObString &key = iter->first;
      const ObSessionVariable &value = iter->second;
      OB_UNIS_ADD_LEN(key);
      OB_UNIS_ADD_LEN(value.meta_);
      OB_UNIS_ADD_LEN(value.value_);
    }
  }
  return len;
}
