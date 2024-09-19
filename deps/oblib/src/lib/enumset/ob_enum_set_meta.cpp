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

#define USING_LOG_PREFIX LIB
#include "ob_enum_set_meta.h"
#include "lib/container/ob_array_helper.h"
#include "share/ob_define.h"


namespace oceanbase {
namespace common {

bool ObEnumSetMeta::is_same(const ObObjMeta &obj_meta, const ObStrValues &str_value) const
{
  bool is_same_meta = true;
  if (OB_LIKELY(!is_valid())) {
    is_same_meta = false;
  } else if (obj_meta != obj_meta_) {
    is_same_meta = false;
  } else if (str_value.count() != str_values_->count()) {
    is_same_meta = false;
  } else {
    for (int64_t i = 0; is_same_meta && i < str_value.count(); ++i) {
      const ObString &str1 = str_value.at(i);
      const ObString &str2 = str_values_->at(i);
      if (str1.length() != str2.length()) {
        is_same_meta = false;
      } else {
        is_same_meta = (0 == MEMCMP(str1.ptr(), str2.ptr(), str1.length()));
      }
    }
  }
  return is_same_meta;
}

bool ObEnumSetMeta::is_same(const ObEnumSetMeta &other) const
{
  bool is_same_meta = true;
  if (this == &other) {
    is_same_meta = true;
  } else if (other.is_valid()) {
    is_same_meta = is_same(other.get_obj_meta(), *other.get_str_values());
  }
  return is_same_meta;
}

uint64_t ObEnumSetMeta::hash() const
{
  uint64_t hash_val = 0;
  if (is_valid()) {
    const uint64_t subschema_id = obj_meta_.get_subschema_id();
    hash_val = obj_meta_.is_set() ? subschema_id : ~subschema_id;
    for (int64_t i = 0; i < str_values_->count(); ++i) {
      hash_val = str_values_->at(i).hash(hash_val);
    }
  }
  return hash_val;
}

int ObEnumSetMeta::deep_copy(ObIAllocator &allocator, ObEnumSetMeta *&dst) const
{
  int ret = OB_SUCCESS;
  void *mem = NULL;
  const int64_t mem_size = sizeof(ObEnumSetMeta) + sizeof(ObFixedArray<ObString, ObIAllocator>);
  ObEnumSetMeta *meta = NULL;
  ObFixedArray<ObString, ObIAllocator> *str_values = NULL;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src meta is invalid", K(ret), KPC(this));
  } else if (OB_ISNULL(mem = allocator.alloc(mem_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc udt meta", K(ret), K(mem_size));
  } else {
    meta = new(mem)ObEnumSetMeta();
    void *array_ptr = static_cast<char*>(mem) + sizeof(ObEnumSetMeta);
    str_values = new(array_ptr) ObFixedArray<ObString, ObIAllocator>(allocator);
    if (OB_FAIL(str_values->init(str_values_->count()))) {
      LOG_WARN("fail to init array", K(ret));
    } else {
      // deep copy string
      for (int64_t i = 0; OB_SUCC(ret) && i < str_values_->count(); ++i) {
        ObString dst_str;
        if (OB_FAIL(ob_write_string(allocator, str_values_->at(i), dst_str))) {
          LOG_WARN("fail to deep copying string", K(ret));
        } else if (OB_FAIL(str_values->push_back(dst_str))) {
          LOG_WARN("push_back failed", K(ret));
          // free memory avoid memory leak
          for (int64_t j = 0; j < str_values->count(); ++j) {
            allocator.free(str_values->at(j).ptr());
          }
          allocator.free(dst_str.ptr());
        }
      }
    }
    if (OB_FAIL(ret)) {
      allocator.free(mem);
      mem = NULL;
    } else {
      meta->obj_meta_.set_meta(obj_meta_);
      meta->str_values_ = str_values;
      dst = meta;
    }
  }
  return ret;
}

static_assert(ObEnumSetMeta::MetaState::UNINITIALIZED == SCALE_UNKNOWN_YET,
                "make sure the value of uninitialized state always be equal to scale unkown yet");

OB_DEF_SERIALIZE(ObEnumSetMeta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src meta is invalid", K(ret), KPC(this));
  } else {
    OB_UNIS_ENCODE(obj_meta_);
    OB_UNIS_ENCODE_ARRAY(str_values_->get_data(), str_values_->count());
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObEnumSetMeta)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(obj_meta_);
  OB_UNIS_DECODE(count);
  const int64_t array_size = sizeof(ObArrayHelper<ObString>); // for array container
  const int64_t strings_size = count * sizeof(ObString); // for string data
  void *mem = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is need for deserialize", K(ret), K(lbt()));
  } else if (OB_UNLIKELY(0 == count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("string values count is zero", K(ret));
  } else if (OB_ISNULL(mem = allocator_->alloc(strings_size + array_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mem", K(ret), K(strings_size), K(array_size));
  } else if (FALSE_IT(MEMSET(mem, 0, strings_size + array_size))) {
  } else {
    ObArrayHelper<ObString> *array_helper = new(mem)(ObArrayHelper<ObString>);
    ObString *strings = reinterpret_cast<ObString*>(static_cast<char*>(mem) + array_size);
    OB_UNIS_DECODE_ARRAY(strings, count);
    if (OB_SUCC(ret)) {
      array_helper->init(count, strings, count);
      str_values_ = array_helper;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(mem)) {
    allocator_->free(mem);
    str_values_ = NULL;
    mem = NULL;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObEnumSetMeta)
{
  int64_t len = 0;
  if (is_valid()) {
    OB_UNIS_ADD_LEN(obj_meta_);
    OB_UNIS_ADD_LEN_ARRAY(str_values_->get_data(), str_values_->count());
  }
  return len;
}

} // namespace common
} // namespace oceanbase