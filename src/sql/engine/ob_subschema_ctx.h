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
#include "common/ob_field.h"

namespace oceanbase
{
namespace sql
{

// implement of subschema mapping
enum ObSubSchemaType {
  OB_SUBSCHEMA_UDT_TYPE = 0,
  OB_SUBSCHEMA_MAX_TYPE
};

// key must be subschema (uint16_t)
// signature for value (refer to value serialize)

class ObSubSchemaValue
{
OB_UNIS_VERSION(1);
public:
  ObSubSchemaValue() : type_(OB_SUBSCHEMA_MAX_TYPE), signature_(0), value_(NULL) {}
  ~ObSubSchemaValue() {}
  int deep_copy_value(const void *src_value, ObIAllocator &allocator);
  TO_STRING_KV(K_(type), K_(signature), KP_(value));
public:
  ObSubSchemaType type_;
  uint64_t signature_;
  void *value_;
};

typedef int (*ob_subschema_value_serialize)(void *value, char* buf, const int64_t buf_len, int64_t& pos);
typedef int (*ob_subschema_value_deserialize)(void *value, const char* buf, const int64_t data_len, int64_t& pos);
typedef int64_t (*ob_subschema_value_serialize_size)(void *value);
typedef int (*ob_subschema_value_get_signature)(void *value, uint64_t &signature);

typedef int (*ob_subschema_value_deep_copy)(const void *src_value, void *&dst_value, ObIAllocator &allocator);

struct ObSubSchemaFuncs
{
  ob_subschema_value_serialize value_serialize;
  ob_subschema_value_deserialize value_deserialize;
  ob_subschema_value_serialize_size get_value_serialize_size;
  ob_subschema_value_get_signature get_signature;

  ob_subschema_value_deep_copy deep_copy;

};

template <ObSubSchemaType type>
    int subschema_value_serialize(void *value, char* buf, const int64_t buf_len, int64_t& pos);
template <ObSubSchemaType type>
    int subschema_value_deserialize(void *value, const char* buf, const int64_t data_len, int64_t& pos);
template <ObSubSchemaType type> int64_t subschema_value_serialize_size(void *value);
template <ObSubSchemaType type>
    int subschema_value_get_signature(void *value, uint64_t &signature);
template <ObSubSchemaType type>
    int subschema_value_deep_copy(const void *src_value, void *&dst_value, ObIAllocator &allocator);

class ObSubSchemaReverseKey
{
  public:
  ObSubSchemaReverseKey() : type_(OB_SUBSCHEMA_MAX_TYPE), signature_(0) {}
  ObSubSchemaReverseKey(ObSubSchemaType type, uint64_t signature) : type_(type), signature_(signature) {}
  ~ObSubSchemaReverseKey() {}

  uint64_t hash() const
  {
    return signature_ + static_cast<uint64_t>(type_);
  }

  int hash(uint64_t &res) const
  {
    res = hash();
    return OB_SUCCESS;
  }

  bool operator==(const ObSubSchemaReverseKey &other) const
  {
    return (other.type_ == this->type_
            && other.signature_ == this->signature_);
  }

  TO_STRING_KV(K_(type), K_(signature));
  ObSubSchemaType type_;
  uint64_t signature_;
};

class ObSubSchemaCtx
{
OB_UNIS_VERSION(1);
  typedef common::hash::ObHashMap<uint64_t, ObSubSchemaValue, common::hash::NoPthreadDefendMode> ObSubSchemaMap;
  // reverse map is used for confilict check and reverse search, reverse key is a signature from value;
  typedef common::hash::ObHashMap<ObSubSchemaReverseKey, uint64_t, common::hash::NoPthreadDefendMode> ObSubSchemaReverseMap;
  static const uint16_t MAX_NON_RESERVED_SUBSCHEMA_ID = ObInvalidSqlType + 1;
  static const uint32_t SUBSCHEMA_BUCKET_NUM = 64;

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
  ObSubSchemaMap &get_subschema_map() { return subschema_map_; }
  const ObSubSchemaMap &get_subschema_map() const { return subschema_map_; }

  int get_subschema_id(uint64_t value_signature, ObSubSchemaType type, uint16_t &subschema_id) const;

  void set_fields(const common::ObIArray<common::ObField> *fields) { fields_ = fields; }
  ObIAllocator &get_allocator() { return allocator_; }

  TO_STRING_KV(K_(is_inited), K_(used_subschema_id),
               K(subschema_map_.size()), K(subschema_reverse_map_.size()));

private:
  bool is_inited_;
  uint16_t used_subschema_id_;
  uint32_t reserved_;
  const common::ObIArray<common::ObField> *fields_; // resultset fields, no need to serialize

  ObIAllocator &allocator_;
  ObSubSchemaMap subschema_map_; // subschema id mapping to subschema (e.g. udt meta)
  ObSubSchemaReverseMap subschema_reverse_map_; // subschema type+signature (e.g. udt_id) mapping to subschema id
};

}
}
#endif //OCEANBASE_SQL_OB_SUBSCHEMA_CTX_H
