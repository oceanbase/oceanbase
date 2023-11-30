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
#include "sql/engine/ob_subschema_ctx.h"
#include "deps/oblib/src/lib/udt/ob_udt_type.h"
#include "src/share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace sql
{
// implement of subschema mapping

// Add New de/serialize functions for schema value when new subschema types are added.
// Signature is identify of subschema value, using for reverse search of subschema id
// for sql udt, signature is original udt id
#define DEF_SUBSCHEMA_ENTRY(SUBSCHEMATYPE)         \
  {                                                \
    subschema_value_serialize<SUBSCHEMATYPE>,      \
    subschema_value_deserialize<SUBSCHEMATYPE>,    \
    subschema_value_serialize_size<SUBSCHEMATYPE>, \
    subschema_value_get_signature<SUBSCHEMATYPE>,  \
    subschema_value_deep_copy<SUBSCHEMATYPE>,      \
  }

template<>
int subschema_value_serialize<OB_SUBSCHEMA_UDT_TYPE>(void *value, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null sql udt value for seriazlie", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
  } else {
    const ObSqlUDTMeta* udt_meta = reinterpret_cast<ObSqlUDTMeta *>(value);
    if (OB_FAIL(udt_meta->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to do sql udt meta seriazlie", K(ret), K(*udt_meta));
    }
  }

  return ret;
}

template <>
int subschema_value_deserialize<OB_SUBSCHEMA_UDT_TYPE>(void *value, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null sql udt value for deseriazlie", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
  } else {
    ObSqlUDTMeta* udt_meta = reinterpret_cast<ObSqlUDTMeta *>(value);
    if (OB_FAIL(udt_meta->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to do sql udt meta deseriazlie", K(ret), KP(buf), K(data_len));
    }
  }
  return ret;
}

template <>
int64_t subschema_value_serialize_size<OB_SUBSCHEMA_UDT_TYPE>(void *value)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(value)) {
  } else {
    ObSqlUDTMeta* udt_meta = reinterpret_cast<ObSqlUDTMeta *>(value);
    len += udt_meta->get_serialize_size();
  }
  return len;
}

template <>
int subschema_value_get_signature<OB_SUBSCHEMA_UDT_TYPE>(void *value, uint64_t &signature)
{
  int ret = OB_SUCCESS;
  signature = 0;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subschema value", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
  } else {
    const ObSqlUDTMeta* udt_meta = reinterpret_cast<ObSqlUDTMeta *>(value);
    signature = udt_meta->udt_id_;
  }
  return ret;
}

template <>
int subschema_value_deep_copy<OB_SUBSCHEMA_UDT_TYPE>(const void *src_value, void *&dst_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subschema value for deep copy", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
  } else {
    const ObSqlUDTMeta* udt_meta = reinterpret_cast<const ObSqlUDTMeta *>(src_value);
    ObSqlUDTMeta* copy_meta = NULL;
    if (OB_FAIL(udt_meta->deep_copy(allocator, copy_meta))) {
      LOG_WARN("failed to deep copy udt meta", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
    } else if (OB_ISNULL(copy_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deep copy udt meta result is null", K(ret), K(OB_SUBSCHEMA_UDT_TYPE));
    } else {
      dst_value = static_cast<void *>(copy_meta);
    }
  }
  return ret;
}

ObSubSchemaFuncs SUBSCHEMA_FUNCS[OB_SUBSCHEMA_MAX_TYPE] =
{
  DEF_SUBSCHEMA_ENTRY(OB_SUBSCHEMA_UDT_TYPE),
};

int ObSubSchemaValue::deep_copy_value(const void *src_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null value for deep copy subschema value", K(ret));
  } else if (OB_FAIL(SUBSCHEMA_FUNCS[type_].deep_copy(src_value, value_, allocator))) {
    LOG_WARN("failed deep copy subschema value", K(ret), K(*this), KP(src_value));
  }
  return ret;
}

// Implementation of de/serialize of ObSubschemaValue
OB_DEF_SERIALIZE(ObSubSchemaValue)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              type_,
              signature_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to serialize subschema type info", K(ret), K(type_), K(signature_));
  } else if (OB_FAIL(SUBSCHEMA_FUNCS[type_].value_serialize(value_, buf, buf_len, pos))) {
    LOG_WARN("fail to serialize subschema data", K(ret), K(type_), K(signature_));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSubSchemaValue)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              type_,
              signature_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to deserialize subschema type info", K(ret), K(type_), K(signature_));
  } else if (OB_FAIL(SUBSCHEMA_FUNCS[type_].value_deserialize(value_, buf, data_len, pos))) {
    LOG_WARN("fail to deserialize subschema data", K(ret), K(type_), K(signature_));
  }
  return ret;
}

// serialize size cannot return error code
OB_DEF_SERIALIZE_SIZE(ObSubSchemaValue)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              type_,
              signature_);
  len += SUBSCHEMA_FUNCS[type_].get_value_serialize_size(value_);
  return len;
}

// Implementation of de/serialize of ObSubschemaValue of ObSubSchemaCtx
OB_DEF_SERIALIZE(ObSubSchemaCtx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || subschema_map_.empty()) { // do nothing
  } else { // subschema count more then zero
    uint32_t subschema_count = subschema_map_.size();
    OB_UNIS_ENCODE(subschema_count);
    OB_UNIS_ENCODE(used_subschema_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to serialize subschema ctx", K(ret));
    } else {
      ObSubSchemaMap::const_iterator iter = subschema_map_.begin();
      while (OB_SUCC(ret) && iter != subschema_map_.end()) {
        OB_UNIS_ENCODE(iter->first);
        OB_UNIS_ENCODE(iter->second);
        iter++;
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObSubSchemaCtx)
{
  int ret = OB_SUCCESS;
  uint32_t subschema_count = 0;
  OB_UNIS_DECODE(subschema_count);
  if (OB_FAIL(ret)) {
  } else if (subschema_count > 0) {
    if (!is_inited_ && OB_FAIL(init())) {
      LOG_WARN("fail to init subschema ctx", K(ret));
    } else {
      OB_UNIS_DECODE(used_subschema_id_);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to deserialize subschema ctx", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < subschema_count; i++) {
          uint64_t subschema_id = 0;
          ObSubSchemaValue value;
          ObSqlUDTMeta udt_meta;
          value.value_ = &udt_meta;
          OB_UNIS_DECODE(subschema_id);
          OB_UNIS_DECODE(value);
          if (OB_FAIL(ret)) { // copy value from buffer to local memory
          } else if (OB_FAIL(value.deep_copy_value(value.value_, allocator_))) {
            LOG_WARN("deep copy value failed", K(ret), K(subschema_id), K(value));
          } else if (OB_FAIL(set_subschema(subschema_id, value))) {
             LOG_WARN("fail to set subschema", K(ret), K(subschema_id), K(value));
          }
        }
      }
    }
  }
  return ret;
}

// serialize size cannot return error code
OB_DEF_SERIALIZE_SIZE(ObSubSchemaCtx)
{
  int64_t len = 0;
  if (!is_inited_ || subschema_map_.empty()) { // do nothing
  } else { // subschema count more then zero
    uint32_t subschema_count = subschema_map_.size();
    OB_UNIS_ADD_LEN(subschema_count);
    OB_UNIS_ADD_LEN(used_subschema_id_);
    ObSubSchemaMap::const_iterator iter = subschema_map_.begin();
    while (iter != subschema_map_.end()) {
      OB_UNIS_ADD_LEN(iter->first);
      OB_UNIS_ADD_LEN(iter->second);
      iter++;
    }
  }
  return len;
}

int ObSubSchemaCtx::assgin(const ObSubSchemaCtx &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_inited() || other.get_subschema_count() == 0) {
    // no subschema, do nothing
  } else if (is_inited() && get_subschema_count() > 0) {
    reset();
    LOG_INFO("subschema context reset due to assgin other", K(*this), K(lbt()));
  }
  if (!is_inited() && OB_FAIL(init())) {
    LOG_WARN("fail to init subschema ctx", K(ret));
  } else {
    ObSubSchemaMap::const_iterator iter = other.get_subschema_map().begin();
    while (OB_SUCC(ret) && iter != other.get_subschema_map().end()) {
      uint64_t subschema_id = iter->first;
      ObSubSchemaValue value = iter->second;
      if (OB_FAIL(value.deep_copy_value(iter->second.value_, allocator_))) {
        LOG_WARN("deep copy value failed", K(ret), K(subschema_id), K(value));
      } else if (OB_FAIL(set_subschema(subschema_id, value))) {
        LOG_WARN("fail to set subschema", K(ret), K(subschema_id), K(value));
      }
      iter++;
    }
    used_subschema_id_ = other.used_subschema_id_;
  }
  return ret;
}

int ObSubSchemaCtx::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema ctx already inited", K(ret), K(*this));
  } else if (OB_FAIL(subschema_map_.create(SUBSCHEMA_BUCKET_NUM,
                                    "SubSchemaHash",
                                    "SubSchemaHash",
                                    MTL_ID()))) {
    LOG_WARN("fail to create subschema map", K(ret));
  } else if (OB_FAIL(subschema_reverse_map_.create(SUBSCHEMA_BUCKET_NUM,
                                    "SubSchemaRev",
                                    "SubSchemaRev",
                                    MTL_ID()))) {
    LOG_WARN("fail to create subschema map", K(ret));
  } else {
    is_inited_ = true;
    used_subschema_id_ = MAX_NON_RESERVED_SUBSCHEMA_ID;
  }
  return ret;
}

void ObSubSchemaCtx::reset() {
  // content in subschema value is alloc from plan object allocator? need a new allocator?
  if (is_inited_) {
    subschema_map_.destroy();
    subschema_reverse_map_.destroy();
    is_inited_ = false;
    used_subschema_id_ = MAX_NON_RESERVED_SUBSCHEMA_ID;
    reserved_ = 0;
    fields_ = NULL;
    LOG_INFO("subschema ctx reset", KP(this), KP(this), K(lbt()));
  }
}

uint32_t ObSubSchemaCtx::get_subschema_count() const
{
  uint32_t subschema_count = 0;
  if (!is_inited_) {
  } else {
    subschema_count = subschema_map_.size();
  }
  return subschema_count;
}

int ObSubSchemaCtx::get_new_subschema_id(uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  subschema_id = used_subschema_id_++;
  if (used_subschema_id_ == UINT16_MAX) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "more then 64K different subschema", K(ret), K(subschema_id), K(lbt()));
  } else {
    SQL_ENG_LOG(INFO, "new subschema id", KP(this), K(subschema_id), K(lbt()));
  }
  return ret;
}
int ObSubSchemaCtx::get_subschema_id_from_fields(uint64_t udt_id, uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  subschema_id = ObInvalidSqlType;
  bool is_found = false;
  if (OB_NOT_NULL(fields_)) {
    for (uint32_t i = 0; is_found == false && OB_SUCC(ret) && i < fields_->count(); i++) {
      if ((fields_->at(i).type_.is_user_defined_sql_type()
            || fields_->at(i).type_.is_collection_sql_type())
          && fields_->at(i).accuracy_.get_accuracy() == udt_id) {
        subschema_id = fields_->at(i).type_.get_udt_subschema_id();
        is_found = true;
      }
    }
  }
  return ret;
}

int ObSubSchemaCtx::set_subschema(uint16_t subschema_id, ObSubSchemaValue &value)
{
  int ret = OB_SUCCESS;
  uint64_t key = subschema_id;
  ObSubSchemaValue tmp_value;
  if (OB_FAIL(subschema_map_.get_refactored(key, tmp_value))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get subschema", K(ret), K(key), K(tmp_value), K(value));
    } else { // not exist
      ret = OB_SUCCESS;
      ObSubSchemaReverseKey rev_key(value.type_, value.signature_);
      LOG_INFO("add new subschema", K(ret), K(subschema_id), K(value));
      if (OB_FAIL(subschema_map_.set_refactored(key, value))) {
        LOG_WARN("set subschema map failed", K(ret), K(subschema_id));
      } else if (OB_FAIL(subschema_reverse_map_.set_refactored(rev_key, key))) {
        LOG_WARN("set subschema map failed", K(ret), K(rev_key));
        int tmp_ret = subschema_map_.erase_refactored(subschema_id);
        if (tmp_ret != OB_SUCCESS) {
          LOG_WARN("erase subschema map failed", K(ret), K(tmp_ret), K(subschema_id));
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subschema id already exist", KP(this), K(ret), K(subschema_id), K(value));
  }
  return ret;
}

int ObSubSchemaCtx::get_subschema(uint16_t subschema_id, ObSubSchemaValue &value) const
{
  uint64_t key = subschema_id;
  return subschema_map_.get_refactored(key, value);
}

int ObSubSchemaCtx::get_subschema_id(uint64_t value_signature,
                                     ObSubSchemaType type,
                                     uint16_t &subschema_id) const
{
  ObSubSchemaReverseKey rev_key(type, value_signature);
  uint64_t value = ObMaxSystemUDTSqlType; // init invalid subschema value
  int ret = subschema_reverse_map_.get_refactored(rev_key, value);
  subschema_id = value;
  return ret;
}

} //sql
} //oceanbase
