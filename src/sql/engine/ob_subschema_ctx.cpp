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
#include "deps/oblib/src/lib/udt/ob_array_utils.h"
#include "lib/enumset/ob_enum_set_meta.h"
#include "share/rc/ob_tenant_base.h"

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
#define DEF_SUBSCHEMA_ENTRY(SUBSCHEMATYPE, CLZ)         \
  {                                                     \
    subschema_value_serialize<SUBSCHEMATYPE, CLZ>,      \
    subschema_value_deserialize<SUBSCHEMATYPE, CLZ>,    \
    subschema_value_serialize_size<SUBSCHEMATYPE, CLZ>, \
    subschema_value_get_signature<SUBSCHEMATYPE, CLZ>,  \
    subschema_value_deep_copy<SUBSCHEMATYPE, CLZ>,      \
    subschema_value_init<SUBSCHEMATYPE, CLZ>,           \
  }

template<ObSubSchemaType TYPE, typename CLZ>
int subschema_value_serialize(void *value, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null sql subschema value for serialize", K(ret), K(TYPE));
  } else {
    const CLZ *subschema_value = reinterpret_cast<CLZ *>(value);
    if (OB_FAIL(subschema_value->serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to do sql subschema value serialize", K(ret), K(*subschema_value));
    }
  }

  return ret;
}

template<ObSubSchemaType TYPE, typename CLZ>
int subschema_value_deserialize(void *value, const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null sql subschema value for deserialize", K(ret), K(TYPE));
  } else {
    CLZ *subschema_value = reinterpret_cast<CLZ *>(value);
    if (OB_FAIL(subschema_value->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to do sql subschema value deserialize", K(ret), KP(buf), K(data_len));
    }
  }
  return ret;
}

template<ObSubSchemaType TYPE, typename CLZ>
int64_t subschema_value_serialize_size(void *value)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(value)) {
  } else {
    const CLZ *subschema_value = reinterpret_cast<CLZ *>(value);
    len += subschema_value->get_serialize_size();
  }
  return len;
}

template<ObSubSchemaType TYPE, typename CLZ>
int subschema_value_get_signature(void *value, uint64_t &signature)
{
  int ret = OB_SUCCESS;
  signature = 0;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subschema value", K(ret), K(TYPE));
  } else {
    const CLZ *subschema_value = reinterpret_cast<CLZ *>(value);
    signature = subschema_value->get_signature();
  }
  return ret;
}

template<ObSubSchemaType TYPE, typename CLZ>
int subschema_value_deep_copy(const void *src_value, void *&dst_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subschema value for deep copy", K(ret), K(TYPE));
  } else {
    const CLZ *src_subschema_value = reinterpret_cast<const CLZ *>(src_value);
    CLZ* copy_value = NULL;
    if (OB_FAIL(src_subschema_value->deep_copy(allocator, copy_value))) {
      LOG_WARN("failed to deep copy subschema value", K(ret), K(TYPE));
    } else if (OB_ISNULL(copy_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deep copy subschema value result is null", K(ret), K(TYPE));
    } else {
      dst_value = static_cast<void *>(copy_value);
    }
  }
  return ret;
}

template<ObSubSchemaType TYPE, typename CLZ>
int subschema_value_init(void *&value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  void *mem = value;
  if (OB_NOT_NULL(mem)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("value is not null", K(ret), KP(value));
  } else if (OB_ISNULL(mem = allocator.alloc(sizeof(CLZ)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc value", K(ret), K(TYPE));
  } else {
    CLZ *meta = new(mem)CLZ(&allocator);
    value = meta;
  }
  return ret;
}

ObSubSchemaFuncs SUBSCHEMA_FUNCS[OB_SUBSCHEMA_MAX_TYPE] =
{
  DEF_SUBSCHEMA_ENTRY(OB_SUBSCHEMA_UDT_TYPE, ObSqlUDTMeta),
  DEF_SUBSCHEMA_ENTRY(OB_SUBSCHEMA_ENUM_SET_TYPE, ObEnumSetMeta),
  DEF_SUBSCHEMA_ENTRY(OB_SUBSCHEMA_COLLECTION_TYPE, ObSqlCollectionInfo),
};

int ObSubSchemaValue::deep_copy_value(const void *src_value, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (!is_valid_type(type_)) {
    // do nothing
  } else if (OB_ISNULL(src_value)) {
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
  } else if (is_valid_type(type_) &&
      OB_FAIL(SUBSCHEMA_FUNCS[type_].value_serialize(value_, buf, buf_len, pos))) {
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
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(type_), K(signature_));
  } else if (is_valid_type(type_)) {
    ObIAllocator *alloc = allocator_;
    void *value_meta = NULL;
    if (OB_FAIL(SUBSCHEMA_FUNCS[type_].init(value_meta, *alloc))) {
      LOG_WARN("fail to init value", K(ret));
    } else if (OB_FAIL(SUBSCHEMA_FUNCS[type_].value_deserialize(value_meta, buf, data_len, pos))) {
      LOG_WARN("fail to deserialize subschema data", K(ret), K(type_), K(signature_));
    } else {
      value_ = value_meta;
    }
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
  if (is_valid_type(type_)) {
    len += SUBSCHEMA_FUNCS[type_].get_value_serialize_size(value_);
  }
  return len;
}

// Implementation of de/serialize of ObSubschemaValue of ObSubSchemaCtx
OB_DEF_SERIALIZE(ObSubSchemaCtx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || subschema_array_.empty()) { // do nothing
  } else { // subschema count more then zero
    const uint32_t subschema_count = subschema_array_.count();
    OB_UNIS_ENCODE(subschema_count);
    OB_UNIS_ENCODE(used_subschema_id_);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to serialize subschema ctx", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < subschema_array_.count(); ++i) {
        if (OB_LIKELY(subschema_array_.at(i).is_valid())) {
          OB_UNIS_ENCODE(i);
          OB_UNIS_ENCODE(subschema_array_.at(i));
        }
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
          value.allocator_ = &allocator_;
          OB_UNIS_DECODE(subschema_id);
          OB_UNIS_DECODE(value);
          if (OB_FAIL(ret)) { // copy value from buffer to local memory
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
  if (!is_inited_ || subschema_array_.empty()) { // do nothing
  } else { // subschema count more then zero
    uint32_t subschema_count = subschema_array_.count();
    OB_UNIS_ADD_LEN(subschema_count);
    OB_UNIS_ADD_LEN(used_subschema_id_);
    for (int64_t i = 0; i < subschema_array_.count(); ++i) {
      if (OB_LIKELY(subschema_array_.at(i).is_valid())) {
        OB_UNIS_ADD_LEN(i);
        OB_UNIS_ADD_LEN(subschema_array_.at(i));
      }
    }
  }
  return len;
}

int ObSubSchemaCtx::assgin(const ObSubSchemaCtx &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_inited() || other.get_subschema_count() == 0) {
    // no subschema, do nothing
  } else {
    if (is_inited() && get_subschema_count() > 0) {
      reset();
      LOG_INFO("subschema context reset due to assgin other", K(*this), K(lbt()));
    }
    if (!is_inited() && OB_FAIL(init())) {
      LOG_WARN("fail to init subschema ctx", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.get_subschema_array().count(); ++i) {
        uint64_t subschema_id = i;
        const ObSubSchemaValue src = other.get_subschema_array().at(subschema_id);
        ObSubSchemaValue dst = src;
        if (OB_FAIL(dst.deep_copy_value(src.value_, allocator_))) {
          LOG_WARN("deep copy value failed", K(ret), K(i), K(dst));
        } else if (OB_FAIL(set_subschema(i, dst))) {
          LOG_WARN("fail to set subschema", K(ret), K(i), K(dst));
        }
      }
      used_subschema_id_ = other.used_subschema_id_;
    }
  }
  return ret;
}

int ObSubSchemaCtx::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub schema ctx already inited", K(ret), K(*this));
  } else {
    uint64_t tenant_id = MTL_ID();
    if (tenant_id == OB_INVALID_TENANT_ID) {
      tenant_id = OB_SERVER_TENANT_ID;
    }
    if (OB_FAIL(subschema_reverse_map_.create(SUBSCHEMA_BUCKET_NUM,
                                      "SubSchemaRev",
                                      "SubSchemaRev",
                                      tenant_id))) {
      LOG_WARN("fail to create subschema map", K(ret));
    } else if (OB_FAIL(enum_set_meta_reverse_map_.create(SUBSCHEMA_BUCKET_NUM,
                                                              "SubSchemaRev",
                                                              "SubSchemaRev",
                                                               tenant_id))) {
    } else {
      subschema_array_.set_attr(ObMemAttr(MTL_ID(), "SubSchemaHash"));
      is_inited_ = true;
      used_subschema_id_ = MAX_NON_RESERVED_SUBSCHEMA_ID;
    }
  }
  return ret;
}

void ObSubSchemaCtx::reset() {
  // content in subschema value is alloc from plan object allocator? need a new allocator?
  if (is_inited_) {
    subschema_array_.destroy();
    subschema_reverse_map_.destroy();
    enum_set_meta_reverse_map_.destroy();
    is_inited_ = false;
    used_subschema_id_ = MAX_NON_RESERVED_SUBSCHEMA_ID;
    reserved_ = 0;
    fields_ = NULL;
    LOG_DEBUG("subschema ctx reset", KP(this), K(lbt()));
  }
}

uint32_t ObSubSchemaCtx::get_subschema_count() const
{
  uint32_t subschema_count = 0;
  if (!is_inited_) {
  } else {
    subschema_count = subschema_array_.count();
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
int ObSubSchemaCtx::ensure_array_capacity(const uint16_t count)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(count >= subschema_array_.get_capacity()) &&
        OB_FAIL(subschema_array_.reserve(next_pow2(count)))) {
    LOG_WARN("fail to reserve array capacity", K(ret), K(count), K_(subschema_array));
  } else if (OB_FAIL(subschema_array_.prepare_allocate(count))) {
    LOG_WARN("fail to prepare allocate array", K(ret), K(count), K_(subschema_array));
  }
  return ret;
}

int ObSubSchemaCtx::set_subschema(uint16_t subschema_id, ObSubSchemaValue &value)
{
  int ret = OB_SUCCESS;
  uint64_t key = subschema_id;
  ObSubSchemaValue tmp_value;
  ObSubSchemaReverseKey rev_key(value.type_, value.signature_);
  if (OB_FAIL(get_subschema(key, tmp_value))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get subschema", K(ret), K(key), K(tmp_value), K(value));
    } else if (value.type_ == ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
      ObSqlCollectionInfo *meta_info = static_cast<ObSqlCollectionInfo *>(value.value_);
      rev_key.str_signature_ = meta_info->get_def_string();
    }
    if (OB_HASH_NOT_EXIST == ret) {
      // not exist
      ret = OB_SUCCESS;
      LOG_INFO("add new subschema", K(ret), K(subschema_id), K(value), K(subschema_array_.count()));
      if (OB_FAIL(ensure_array_capacity(subschema_id + 1))) {
        LOG_WARN("fail to ensure array capacity", K(ret));
      } else if (FALSE_IT(subschema_array_.at(subschema_id) = value)) {
      } else if (OB_FAIL(subschema_reverse_map_.set_refactored(rev_key, key))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("set subschema map failed", K(ret), K(rev_key));
          subschema_array_.at(subschema_id).reset();
        }
      }
      if (OB_SUCC(ret) && value.type_ == OB_SUBSCHEMA_ENUM_SET_TYPE) {
        const ObEnumSetMeta* enumset_meta = reinterpret_cast<const ObEnumSetMeta*>(value.value_);
        ObEnumSetMetaReverseKey meta_rev_key(value.type_, enumset_meta);
        if (OB_FAIL(enum_set_meta_reverse_map_.set_refactored(meta_rev_key, key))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("set subschema map failed", K(ret), K(rev_key));
            subschema_array_.at(subschema_id).reset();
            subschema_reverse_map_.erase_refactored(rev_key);
          }
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
  int ret = OB_SUCCESS;
  if (OB_LIKELY(subschema_id < subschema_array_.count() &&
      subschema_array_.at(subschema_id).is_valid())) {
    value = subschema_array_.at(subschema_id);
  } else {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
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

int ObSubSchemaCtx::get_subschema_id_by_typedef(const ObString &type_def,
                                                uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  ObSubSchemaReverseKey rev_key(OB_SUBSCHEMA_COLLECTION_TYPE, type_def);
  uint64_t tmp_subid = ObMaxSystemUDTSqlType;
  if (OB_FAIL(subschema_reverse_map_.get_refactored(rev_key, tmp_subid))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get subschemaid from reverse map", K(ret));
    } else {
      ObSqlCollectionInfo *buf = NULL;
      uint16_t new_tmp_id;
      char *name_def = NULL;
      // construnct collection meta
      if (OB_ISNULL(buf = OB_NEWx(ObSqlCollectionInfo, &allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create ObSqlCollectionInfo buffer", K(ret));
      } else if (OB_FAIL(get_new_subschema_id(new_tmp_id))) {
        LOG_WARN("fail to get new subschema id", K(ret));
      } else if (OB_ISNULL(name_def = static_cast<char *>(allocator_.alloc(type_def.length())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create ObSqlCollectionInfo buffer", K(ret));
      } else {
        tmp_subid = new_tmp_id;
        ObString type_info;
        type_info.assign_buffer(name_def, type_def.length());
        type_info.write(type_def.ptr(), type_def.length());
        buf->set_name(type_info);
        if (OB_FAIL(buf->parse_type_info())) {
          LOG_WARN("fail to parse ObSqlCollectionInfo", K(ret));
        } else {
          ObSubSchemaValue value;
          value.type_ = OB_SUBSCHEMA_COLLECTION_TYPE;
          value.value_ = static_cast<void *>(buf);
          uint64_t key = tmp_subid;
          rev_key.str_signature_.assign_ptr(type_info.ptr(), type_info.length());
          if (OB_FAIL(ensure_array_capacity(key + 1))) {
            LOG_WARN("fail to ensure array capacity", K(ret));
          } else if (FALSE_IT(subschema_array_.at(key) = value)) {
          } else if (OB_FAIL(subschema_reverse_map_.set_refactored(rev_key, key))) {
            LOG_WARN("set subschema map failed", K(ret), K(rev_key), K(key));
            subschema_array_.at(key).reset();
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    subschema_id = tmp_subid;
  }
  return ret;
}

int ObSubSchemaCtx::get_subschema_id_by_typedef(const ObString &type_def,
                                                uint16_t &subschema_id) const
{
  int ret = OB_SUCCESS;
  ObSubSchemaReverseKey rev_key(OB_SUBSCHEMA_COLLECTION_TYPE, type_def);
  uint64_t tmp_subid = ObMaxSystemUDTSqlType;
  if (OB_FAIL(subschema_reverse_map_.get_refactored(rev_key, tmp_subid))) {
    LOG_WARN("failed to get subschemaid from reverse map", K(ret));
  } else {
    subschema_id = tmp_subid;
  }
  return ret;
}

int ObSubSchemaCtx::get_subschema_id_by_typedef(ObNestedType coll_type,
                                                const ObDataType &elem_type,
                                                uint16_t &subschema_id)
{
  int ret = OB_SUCCESS;
  const int MAX_LEN = 256;
  char tmp[MAX_LEN] = {0};
  if (OB_FAIL(ObArrayUtil::get_type_name(coll_type, elem_type, tmp, MAX_LEN))) {
    LOG_WARN("failed to convert len to string", K(ret));
  } else {
    ObString tmp_def(strlen(tmp), tmp);
    if (OB_FAIL(get_subschema_id_by_typedef(tmp_def, subschema_id))) {
      LOG_WARN("failed to get subschemaid by typedef", K(ret));
    }
  }
  return ret;
}

int ObSubSchemaCtx::get_subschema_id_by_typedef(ObNestedType coll_type,
                                                const ObDataType &elem_type,
                                                uint16_t &subschema_id) const
{
  int ret = OB_SUCCESS;
  const int MAX_LEN = 256;
  int64_t pos = 0;
  char tmp[MAX_LEN] = {0};
  if (OB_FAIL(ObArrayUtil::get_type_name(coll_type, elem_type, tmp, MAX_LEN))) {
    LOG_WARN("failed to convert len to string", K(ret));
  } else {
    ObString tmp_def(strlen(tmp), tmp);
    if (OB_FAIL(get_subschema_id_by_typedef(tmp_def, subschema_id))) {
      LOG_WARN("failed to get subschemaid by typedef", K(ret));
    }
  }
  return ret;
}

int ObSubSchemaCtx::get_subschema_id(const ObSubSchemaType type,
                                     const ObEnumSetMeta &meta,
                                     uint16_t &subschema_id) const
{
  ObEnumSetMetaReverseKey rev_key(type, &meta);
  uint64_t value = ObMaxSystemUDTSqlType; // init invalid subschema value
  int ret = enum_set_meta_reverse_map_.get_refactored(rev_key, value);
  subschema_id = value;
  return ret;
}

} //sql
} //oceanbase
