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

#ifndef OCEANBASE_SQL_OB_SUBSCHEMA_CTX_H
#define OCEANBASE_SQL_OB_SUBSCHEMA_CTX_H

#include "lib/oblog/ob_log_module.h"
#include "lib/udt/ob_collection_type.h"
#include "common/ob_field.h"
#include "lib/enumset/ob_enum_set_meta.h"

namespace oceanbase
{
namespace sql
{

// implement of subschema mapping
enum ObSubSchemaType {
  OB_SUBSCHEMA_UDT_TYPE = 0,
  OB_SUBSCHEMA_ENUM_SET_TYPE = 1,
  OB_SUBSCHEMA_COLLECTION_TYPE = 2,
  OB_SUBSCHEMA_MAX_TYPE
};

// key must be subschema (uint16_t)
// signature for value (refer to value serialize)

class ObSubSchemaValue
{
OB_UNIS_VERSION(1);
public:
  ObSubSchemaValue() : type_(OB_SUBSCHEMA_MAX_TYPE), signature_(0), value_(NULL),
                       allocator_(NULL) {}
  ~ObSubSchemaValue() {}
  int deep_copy_value(const void *src_value, ObIAllocator &allocator);
  static bool is_valid_type(ObSubSchemaType type)
  { return type >= OB_SUBSCHEMA_UDT_TYPE && type < OB_SUBSCHEMA_MAX_TYPE; }
  inline bool is_valid() const { return is_valid_type(type_); }
  inline void reset()
  {
    type_ = OB_SUBSCHEMA_MAX_TYPE;
    signature_ = 0;
    value_ = NULL;
    allocator_ = NULL;
  }
  TO_STRING_KV(K_(type), K_(signature), KP_(value));
public:
  ObSubSchemaType type_;
  uint64_t signature_;
  void *value_;
  common::ObIAllocator *allocator_; // for deserialize
};

typedef int (*ob_subschema_value_serialize)(void *value, char* buf, const int64_t buf_len, int64_t& pos);
typedef int (*ob_subschema_value_deserialize)(void *value, const char* buf, const int64_t data_len, int64_t& pos);
typedef int64_t (*ob_subschema_value_serialize_size)(void *value);
typedef int (*ob_subschema_value_get_signature)(void *value, uint64_t &signature);

typedef int (*ob_subschema_value_deep_copy)(const void *src_value, void *&dst_value, ObIAllocator &allocator);
typedef int (*ob_subschema_value_init)(void *&value, ObIAllocator &allocator);

struct ObSubSchemaFuncs
{
  ob_subschema_value_serialize value_serialize;
  ob_subschema_value_deserialize value_deserialize;
  ob_subschema_value_serialize_size get_value_serialize_size;
  ob_subschema_value_get_signature get_signature;

  ob_subschema_value_deep_copy deep_copy;
  ob_subschema_value_init init;
};

template <ObSubSchemaType TYPE, typename CLZ>
int subschema_value_serialize(void *value, char* buf, const int64_t buf_len, int64_t& pos);
template <ObSubSchemaType TYPE, typename CLZ>
int subschema_value_deserialize(void *value, const char* buf, const int64_t data_len, int64_t& pos);
template <ObSubSchemaType TYPE, typename CLZ>
int64_t subschema_value_serialize_size(void *value);
template <ObSubSchemaType TYPE, typename CLZ>
int subschema_value_get_signature(void *value, uint64_t &signature);
template <ObSubSchemaType TYPE, typename CLZ>
int subschema_value_deep_copy(const void *src_value, void *&dst_value, ObIAllocator &allocator);

template <ObSubSchemaType TYPE, typename CLZ>
int subschema_value_init(void *&value, ObIAllocator &allocator);

class ObSubSchemaReverseKey
{
  public:
  ObSubSchemaReverseKey() : type_(OB_SUBSCHEMA_MAX_TYPE), signature_(0), str_signature_() {}
  ObSubSchemaReverseKey(ObSubSchemaType type, uint64_t signature) : type_(type), signature_(signature), str_signature_() {}
  ObSubSchemaReverseKey(ObSubSchemaType type, ObString type_info) : type_(type), signature_(0), str_signature_(type_info) {}
  ~ObSubSchemaReverseKey() {}

  uint64_t hash() const
  {
    return type_ == OB_SUBSCHEMA_UDT_TYPE ? signature_ + static_cast<uint64_t>(type_)
      : murmurhash(str_signature_.ptr(), static_cast<int32_t>(str_signature_.length()), 0) + static_cast<uint64_t>(type_);
  }

  int hash(uint64_t &res) const
  {
    res = hash();
    return OB_SUCCESS;
  }

  bool operator==(const ObSubSchemaReverseKey &other) const
  {
    return (other.type_ == this->type_
            && other.signature_ == this->signature_
            && other.str_signature_ == this->str_signature_);
  }

  TO_STRING_KV(K_(type), K_(signature), K_(str_signature));
  ObSubSchemaType type_;
  uint64_t signature_;
  ObString str_signature_;
};

class ObEnumSetMetaReverseKey
{
public:
  ObEnumSetMetaReverseKey() : type_(OB_SUBSCHEMA_MAX_TYPE), meta_(NULL) {}
  ObEnumSetMetaReverseKey(const ObSubSchemaType type, const ObEnumSetMeta *meta) :
      type_(type), meta_(meta) {}
  ~ObEnumSetMetaReverseKey() {}

  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    if (OB_NOT_NULL(meta_)) {
      hash_val = meta_->hash() + static_cast<uint64_t>(type_);
    }
    return hash_val;
  }

  inline int hash(uint64_t &res) const
  {
    res = hash();
    return OB_SUCCESS;
  }

  inline bool operator==(const ObEnumSetMetaReverseKey &other) const
  {
    bool eq_ret = true;
    if (other.type_ != this->type_) {
      eq_ret = false;
    } else if (meta_ == NULL || other.meta_ == NULL) {
      eq_ret = (meta_ == other.meta_);
    } else {
      eq_ret = (*meta_ == *other.meta_);
    }
    return eq_ret;
  }

  TO_STRING_KV(K_(type), KP_(meta));

  ObSubSchemaType type_;
  const ObEnumSetMeta *meta_;
};

class ObSubSchemaCtx
{
OB_UNIS_VERSION(1);
  static const uint16_t MAX_NON_RESERVED_SUBSCHEMA_ID = ObInvalidSqlType + 1;
  static const uint32_t SUBSCHEMA_BUCKET_NUM = 64;
  typedef common::ObSEArray<ObSubSchemaValue, SUBSCHEMA_BUCKET_NUM> ObSubSchemaArray;
  // reverse map is used for conflict check and reverse search, reverse key is a signature from value;
  typedef common::hash::ObHashMap<ObSubSchemaReverseKey, uint64_t, common::hash::NoPthreadDefendMode> ObSubSchemaReverseMap;
  typedef common::hash::ObHashMap<ObEnumSetMetaReverseKey, uint64_t, common::hash::NoPthreadDefendMode> ObEnumSetMetaReverseMap;

public:
  ObSubSchemaCtx(ObIAllocator &allocator) :
    is_inited_(false), used_subschema_id_(MAX_NON_RESERVED_SUBSCHEMA_ID),
    reserved_(0), fields_(NULL), allocator_(allocator) {}

  ~ObSubSchemaCtx() { reset(); }

  int init();
  bool is_inited() const { return is_inited_; }
  void reset();
  void destroy() { reset(); }
  int assgin(const ObSubSchemaCtx &other);

  uint32_t get_subschema_count() const;

  int get_new_subschema_id(uint16_t &subschema_id);
  int get_subschema_id_from_fields(uint64_t udt_id, uint16_t &subschema_id);

  int set_subschema(uint16_t subschema_id, ObSubSchemaValue &value);
  int get_subschema(uint16_t subschema_id, ObSubSchemaValue &value) const;
  ObSubSchemaArray &get_subschema_array() { return subschema_array_; }
  const ObSubSchemaArray &get_subschema_array() const { return subschema_array_; }

  int get_subschema_id(uint64_t value_signature, ObSubSchemaType type, uint16_t &subschema_id) const;
  int get_subschema_id_by_typedef(ObNestedType coll_type, const ObDataType &elem_type, uint16_t &subschema_id);
  int get_subschema_id_by_typedef(ObNestedType coll_type, const ObDataType &elem_type, uint16_t &subschema_id) const;
  int get_subschema_id_by_typedef(const ObString &type_def, uint16_t &subschema_id);
  int get_subschema_id_by_typedef(const ObString &type_def, uint16_t &subschema_id) const;
  int get_subschema_id(const ObSubSchemaType type, const ObEnumSetMeta &meta,
                       uint16_t &subschema_id) const;

  void set_fields(const common::ObIArray<common::ObField> *fields) { fields_ = fields; }
  ObIAllocator &get_allocator() { return allocator_; }

  TO_STRING_KV(K_(is_inited), K_(used_subschema_id),
               K(subschema_array_.count()), K(subschema_reverse_map_.size()));
private:
  inline int ensure_array_capacity(const uint16_t count);
  static bool is_type_info_subschema(const ObSubSchemaType type)
  {
    return type == OB_SUBSCHEMA_ENUM_SET_TYPE;
  }

private:
  bool is_inited_;
  uint16_t used_subschema_id_;
  uint32_t reserved_;
  const common::ObIArray<common::ObField> *fields_; // resultset fields, no need to serialize

  ObIAllocator &allocator_;
  ObSubSchemaArray subschema_array_; // subschema id (as array index) mapping to subschema (e.g. udt meta)
  ObSubSchemaReverseMap subschema_reverse_map_; // subschema type+signature (e.g. udt_id) mapping to subschema id
  ObEnumSetMetaReverseMap enum_set_meta_reverse_map_; // subschema type + meta_ mapping to subschema id
};

}
}
#endif //OCEANBASE_SQL_OB_SUBSCHEMA_CTX_H
