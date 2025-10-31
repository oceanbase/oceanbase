/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/avro_schema_util.h"

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/ob_define.h"
#include "spec/schema_field.h"
#include "spec/type.h"

#include <avro/NodeImpl.hh>
#include <charconv>
#include <s2/base/casts.h>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

SchemaProjection::SchemaProjection(ObIAllocator &allocator) : fields_(allocator)
{
}

int AvroSchemaProjectionUtils::project(ObIAllocator &allocator,
                                       const StructType &expected_schema,
                                       const avro::NodePtr &avro_node,
                                       SchemaProjection &schema_projection)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;

  // 我们只需要他的 children 节点，所以这里用临时 allocator
  FieldProjection tmp_field_projection(tmp_allocator);
  if (OB_FAIL(AvroSchemaProjectionUtils::project_nested(allocator,
                                                        &expected_schema,
                                                        avro_node,
                                                        tmp_field_projection))) {
    LOG_WARN("AvroSchemaProjectionUtils::project failed", K(ret));
  } else if (OB_FAIL(schema_projection.fields_.reserve(tmp_field_projection.children_.count()))) {
    LOG_WARN("failed to reserve size", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_field_projection.children_.count(); i++) {
      OZ(schema_projection.fields_.push_back(tmp_field_projection.children_[i]));
    }
  }
  return ret;
}

int AvroSchemaProjectionUtils::project_nested(ObIAllocator &allocator,
                                              const Type *expected_schema,
                                              const avro::NodePtr &avro_node,
                                              FieldProjection &field_projection)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expected_schema) || OB_ISNULL(avro_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null input parameters", K(ret));
  } else if (!expected_schema->is_nested()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expected schema must be nested type", K(ret));
  } else {
    switch (expected_schema->type_id()) {
      case TypeId::kStruct: {
        if (OB_FAIL(AvroSchemaProjectionUtils::project_struct(
                allocator,
                down_cast<const StructType *>(expected_schema),
                avro_node,
                field_projection))) {
          LOG_WARN("fail to project struct", K(ret));
        }
        break;
      }
      case TypeId::kList: {
        if (OB_FAIL(AvroSchemaProjectionUtils::project_list(
                allocator,
                down_cast<const ListType *>(expected_schema),
                avro_node,
                field_projection))) {
          LOG_WARN("fail to project list", K(ret));
        }
        break;
      }
      case TypeId::kMap: {
        if (OB_FAIL(
                AvroSchemaProjectionUtils::project_map(allocator,
                                                       down_cast<const MapType *>(expected_schema),
                                                       avro_node,
                                                       field_projection))) {
          LOG_WARN("fail to project map", K(ret));
        }
        break;
      }
      default:
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid nested type", K(ret), K(expected_schema->type_id()));
    }
  }
  return ret;
}

int AvroSchemaProjectionUtils::project_struct(ObIAllocator &allocator,
                                              const StructType *expected_schema,
                                              const avro::NodePtr &avro_node,
                                              FieldProjection &field_projection)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expected_schema) || avro::AVRO_RECORD != avro_node->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input parameters", K(ret), KP(expected_schema), K(avro_node->type()));
  }

  hash::ObHashMap<int32_t, ::avro::NodePtr> field_id_2_avro_node_info_map;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(field_id_2_avro_node_info_map.create(10, "avro_mapping"))) {
      LOG_WARN("fail to create map", K(ret));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < avro_node->leaves(); idx++) {
        const avro::NodePtr &leaf_node = avro_node->leafAt(idx);
        int32_t field_id = -1;
        if (OB_FAIL(AvroSchemaProjectionUtils::get_field_id(avro_node, idx, field_id))) {
          LOG_WARN("fail to get leaf node's field-id", K(ret), K(field_id));
        } else if (OB_FAIL(field_id_2_avro_node_info_map.set_refactored(field_id, leaf_node))) {
          LOG_WARN("fail to set refactored", K(ret), K(field_id));
        }
      }
    }
  }

  // 检查 ExpectedSchema 里面的每一个 required SchemaField 都是在 avro data schema 里面存在的
  if (OB_SUCC(ret)) {
    avro::NodePtr tmp_found_avro_node;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < expected_schema->fields_count(); idx++) {
      const SchemaField *expected_field = expected_schema->get_field_by_index(idx);
      if (OB_ISNULL(expected_field)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("null expected field", K(ret));
      } else if (OB_FAIL(field_id_2_avro_node_info_map.get_refactored(expected_field->field_id(),
                                                                      tmp_found_avro_node))) {
        // not found
        if (OB_HASH_NOT_EXIST == ret) {
          if (expected_field->optional()) {
            ret = OB_SUCCESS;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("required schema field not existed", K(ret), K(expected_field->field_id()));
          }
        }
      }
    }
  }

  struct SchemaFieldInfo
  {
    int64_t field_idx_ = -1; // 对应这个 SchemaField 在 ExpectedSchemaField 里面的 idx
    const SchemaField *schema_field = NULL;
  };
  hash::ObHashMap<int32_t, SchemaFieldInfo> field_id_2_schema_field_map;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(field_id_2_schema_field_map.create(10, "avro_mapping"))) {
      LOG_WARN("fail to create map", K(ret));
    } else {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < expected_schema->fields_count(); idx++) {
        const SchemaField *schema_field = expected_schema->get_field_by_index(idx);
        if (OB_ISNULL(schema_field)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("null schema field", K(ret));
        } else if (OB_FAIL(field_id_2_schema_field_map.set_refactored(
                       schema_field->field_id(),
                       SchemaFieldInfo{idx, schema_field}))) {
          LOG_WARN("fail to set refactored", K(ret), K(schema_field->field_id()));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(field_projection.children_.reserve(avro_node->leaves()))) {
      LOG_WARN("fail to reserve children", K(ret));
    }

    for (int64_t idx = 0; OB_SUCC(ret) && idx < avro_node->leaves(); idx++) {
      const avro::NodePtr &leaf_node = avro_node->leafAt(idx);
      int32_t field_id = -1;
      SchemaFieldInfo tmp_schema_field_info;
      if (OB_FAIL(AvroSchemaProjectionUtils::get_field_id(avro_node, idx, field_id))) {
        LOG_WARN("fail to get leaf node's field-id", K(ret), K(field_id));
      } else if (OB_FAIL(
                     field_id_2_schema_field_map.get_refactored(field_id, tmp_schema_field_info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to get refactored", K(ret), K(field_id));
        }
      }

      if (OB_SUCC(ret) || OB_HASH_NOT_EXIST == ret) {
        FieldProjection *child_field_projection = NULL;
        if (OB_ISNULL(child_field_projection = OB_NEWx(FieldProjection, &allocator, allocator))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate child", K(ret));
        } else if (OB_HASH_NOT_EXIST == ret) {
          child_field_projection->kind_ = FieldProjection::Kind::NOT_EXISTED;
          child_field_projection->avro_node_ = leaf_node;
          ret = OB_SUCCESS;
        } else {
          child_field_projection->kind_ = FieldProjection::Kind::Projected;
          child_field_projection->field_id_ = field_id;
          child_field_projection->avro_node_ = leaf_node;

          if (tmp_schema_field_info.schema_field->type()->is_nested()) {
            // 如果 schema field 是 nested，需要递归进去
            avro::NodePtr tmp_unwarp_avro_node = NULL;
            if (OB_FAIL(AvroUtils::unwarp_union(leaf_node, tmp_unwarp_avro_node))) {
              LOG_WARN("fail to unwarp union avro node", K(ret));
            } else if (OB_FAIL(AvroSchemaProjectionUtils::project_nested(
                           allocator,
                           tmp_schema_field_info.schema_field->type(),
                           tmp_unwarp_avro_node,
                           *child_field_projection))) {
              LOG_WARN("fail to project nested", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          OZ(field_projection.children_.push_back(child_field_projection));
        }
      }
    }
  }

  return ret;
}

int AvroSchemaProjectionUtils::project_list(ObIAllocator &allocator,
                                            const ListType *expected_schema,
                                            const avro::NodePtr &avro_node,
                                            FieldProjection &field_projection)
{
  int ret = OB_SUCCESS;
  int32_t element_field_id = -1;
  const SchemaField *expected_element_schema_field = NULL;
  FieldProjection *element_field_projection = NULL;
  if (OB_UNLIKELY(avro::AVRO_ARRAY != avro_node->type() || 1 != avro_node->leaves())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid avro array node", K(avro_node->type()));
  } else if (OB_FAIL(get_element_id(avro_node, element_field_id))) {
    LOG_WARN("fail to get element id", K(ret));
  } else if (OB_ISNULL(expected_element_schema_field = expected_schema->element())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null expected element", KP(expected_element_schema_field));
  } else if (OB_UNLIKELY(element_field_id != expected_element_schema_field->field_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unmatched element id",
             K(expected_element_schema_field->field_id()),
             K(element_field_id));
  } else if (OB_FAIL(field_projection.children_.reserve(1))) {
    LOG_WARN("fail to reserve children", K(ret));
  } else if (OB_ISNULL(element_field_projection
                       = OB_NEWx(FieldProjection, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate field projection", K(ret));
  } else if (expected_element_schema_field->type()->is_nested()) {
    avro::NodePtr tmp_unwarp_element_avro_node = NULL;
    if (OB_FAIL(AvroUtils::unwarp_union(avro_node->leafAt(0), tmp_unwarp_element_avro_node))) {
      LOG_WARN("fail to unwarp union", K(ret));
    } else if (OB_FAIL(
                   AvroSchemaProjectionUtils::project_nested(allocator,
                                                             expected_element_schema_field->type(),
                                                             tmp_unwarp_element_avro_node,
                                                             *element_field_projection))) {
      LOG_WARN("fail to project nested", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    element_field_projection->kind_ = FieldProjection::Kind::Projected;
    element_field_projection->field_id_ = expected_element_schema_field->field_id();
    element_field_projection->avro_node_ = avro_node->leafAt(0);
    OZ(field_projection.children_.push_back(element_field_projection));
  }
  return ret;
}

int AvroSchemaProjectionUtils::project_map(ObIAllocator &allocator,
                                           const MapType *expected_schema,
                                           const avro::NodePtr &avro_node,
                                           FieldProjection &field_projection)
{
  int ret = OB_SUCCESS;
  const SchemaField *expected_key_schema = expected_schema->key();
  const SchemaField *expected_value_schema = expected_schema->value();
  avro::NodePtr avro_map_node = NULL;
  avro::NodePtr unwarp_avro_map_key_node = NULL;
  avro::NodePtr unwarp_avro_map_value_node = NULL;
  int32_t key_field_id = -1;
  int32_t value_field_id = -1;
  FieldProjection *key_field_projection = NULL;
  FieldProjection *value_field_projection = NULL;

  if (OB_UNLIKELY(OB_ISNULL(expected_key_schema) || OB_ISNULL(expected_value_schema))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null expected key/value schema",
             K(ret),
             K(expected_key_schema),
             K(expected_value_schema));
  } else if (OB_UNLIKELY(avro::AVRO_ARRAY != avro_node->type() || 1 != avro_node->leaves())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("array-backed map type must have exactly one node", K(ret), K(avro_node->type()));
  } else if (OB_FALSE_IT(avro_map_node = avro_node->leafAt(0))) {
  } else if (OB_UNLIKELY(avro::AVRO_RECORD != avro_map_node->type()
                         || 2 != avro_map_node->leaves())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("array-backed map type must have a record node with two fields",
             K(ret),
             K(avro_map_node->leaves()));
  } else if (OB_FAIL(AvroSchemaProjectionUtils::get_field_id(avro_map_node, 0, key_field_id))) {
    LOG_WARN("fail to get key id", K(ret));
  } else if (OB_FAIL(AvroSchemaProjectionUtils::get_field_id(avro_map_node, 1, value_field_id))) {
    LOG_WARN("fail to get value id", K(ret));
  } else if (expected_key_schema->field_id() != key_field_id
             || expected_value_schema->field_id() != value_field_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unmatched field id",
             K(ret),
             K(expected_key_schema->field_id()),
             K(expected_value_schema->field_id()),
             K(key_field_id),
             K(value_field_id));
  } else if (OB_FAIL(field_projection.children_.reserve(2))) {
    LOG_WARN("fail to reserve field projection", K(ret));
  } else if (OB_FAIL(AvroUtils::unwarp_union(avro_map_node->leafAt(0), unwarp_avro_map_key_node))) {
    LOG_WARN("fail to unwarp union", K(ret));
  } else if (OB_FAIL(
                 AvroUtils::unwarp_union(avro_map_node->leafAt(1), unwarp_avro_map_value_node))) {
    LOG_WARN("fail to unwarp union", K(ret));
  } else if (OB_ISNULL(key_field_projection = OB_NEWx(FieldProjection, &allocator, allocator))
             || OB_ISNULL(value_field_projection
                          = OB_NEWx(FieldProjection, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate field projection", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (expected_key_schema->type()->is_nested()) {
      if (OB_FAIL(AvroSchemaProjectionUtils::project_nested(allocator,
                                                            expected_key_schema->type(),
                                                            unwarp_avro_map_key_node,
                                                            *key_field_projection))) {
        LOG_WARN("failed to project nested key", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (expected_value_schema->type()->is_nested()) {
      if (OB_FAIL(AvroSchemaProjectionUtils::project_nested(allocator,
                                                            expected_value_schema->type(),
                                                            unwarp_avro_map_value_node,
                                                            *value_field_projection))) {
        LOG_WARN("failed to project nested value", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    key_field_projection->kind_ = FieldProjection::Kind::Projected;
    key_field_projection->field_id_ = expected_key_schema->field_id();
    key_field_projection->avro_node_ = avro_map_node->leafAt(0);

    value_field_projection->kind_ = FieldProjection::Kind::Projected;
    value_field_projection->field_id_ = expected_value_schema->field_id();
    value_field_projection->avro_node_ = avro_map_node->leafAt(1);
    OZ(field_projection.children_.push_back(key_field_projection));
    OZ(field_projection.children_.push_back(value_field_projection));
  }
  return ret;
}

int AvroSchemaProjectionUtils::get_id(const avro::NodePtr &avro_node,
                                      const ObString &attr_name,
                                      const int64_t field_idx,
                                      int32_t &id)
{
  int ret = OB_SUCCESS;
  id = -1;
  std::optional<std::string> id_str = std::nullopt;

  if (field_idx >= avro_node->customAttributes()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid field idx", K(ret), K(field_idx), K(avro_node->customAttributes()));
  } else {
    id_str = avro_node->customAttributesAt(field_idx).getAttribute(
        std::string(attr_name.ptr(), attr_name.length()));
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!id_str.has_value())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expected attr not found", K(ret), K(attr_name));
    } else {
      const std::string &id_value = id_str.value();
      std::from_chars_result result
          = std::from_chars(id_value.data(), id_value.data() + id_value.size(), id);
      if (OB_UNLIKELY(result.ec != std::errc())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid attr name", K(ret), K(attr_name));
      }
    }
  }

  return ret;
}

int AvroSchemaProjectionUtils::get_element_id(const avro::NodePtr &avro_node, int32_t &id)
{
  return AvroSchemaProjectionUtils::get_id(avro_node, "element-id", 0, id);
}

int AvroSchemaProjectionUtils::get_key_id(const avro::NodePtr &avro_node, int32_t &id)
{
  return AvroSchemaProjectionUtils::get_id(avro_node, "key-id", 0, id);
}

int AvroSchemaProjectionUtils::get_value_id(const avro::NodePtr &avro_node, int32_t &id)
{
  return AvroSchemaProjectionUtils::get_id(avro_node, "value-id", 0, id);
}

int AvroSchemaProjectionUtils::get_field_id(const avro::NodePtr &avro_node,
                                            const int64_t field_idx,
                                            int32_t &id)
{
  return get_id(avro_node, "field-id", field_idx, id);
}

int AvroUtils::skip_decode(const avro::NodePtr &avro_node, avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;

  switch (avro_node->type()) {
    case avro::AVRO_STRING:
      decoder.skipString();
      break;
    case avro::AVRO_BYTES:
      decoder.skipBytes();
      break;
    case avro::AVRO_INT:
      decoder.decodeInt();
      break;
    case avro::AVRO_LONG:
      decoder.decodeLong();
      break;
    case avro::AVRO_FLOAT:
      decoder.decodeFloat();
      break;
    case avro::AVRO_DOUBLE:
      decoder.decodeDouble();
      break;
    case avro::AVRO_BOOL:
      decoder.decodeBool();
      break;
    case avro::AVRO_NULL:
      decoder.decodeNull();
      break;
    case avro::AVRO_RECORD: {
      for (int64_t i = 0; OB_SUCC(ret) && i < avro_node->leaves(); i++) {
        if (OB_FAIL(skip_decode(avro_node->leafAt(i), decoder))) {
          LOG_WARN("failed to skip avro record", K(ret), K(i));
        }
      }
      break;
    }
    case avro::AVRO_ENUM: {
      decoder.decodeEnum();
      break;
    }
    case avro::AVRO_ARRAY: {
      const avro::NodePtr &element_node_type = avro_node->leafAt(0);
      for (size_t n = decoder.arrayStart(); OB_SUCC(ret) && n != 0; n = decoder.arrayNext()) {
        for (size_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          if (OB_FAIL(AvroUtils::skip_decode(element_node_type, decoder))) {
            LOG_WARN("failed to skip array's element", K(ret), K(i));
          }
        }
      }
      break;
    }
    case avro::AVRO_MAP: {
      ret = OB_NOT_SUPPORTED;
      break;
    }
    case avro::AVRO_UNION: {
      size_t union_index = decoder.decodeUnionIndex();
      if (union_index >= avro_node->leaves()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid avro union index", K(ret), K(union_index), K(avro_node->leaves()));
      } else if (OB_FAIL(skip_decode(avro_node->leafAt(union_index), decoder))) {
        LOG_WARN("failed to skip avro union index", K(ret), K(union_index));
      }
      break;
    }
    case avro::AVRO_FIXED: {
      decoder.skipFixed(avro_node->fixedSize());
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid avro type", K(ret));
    }
  }
  return ret;
}

int AvroUtils::unwarp_union(const avro::NodePtr &avro_node, avro::NodePtr &result)
{
  int ret = OB_SUCCESS;
  if (avro::AVRO_UNION != avro_node->type()) {
    result = avro_node;
  } else {
    if (avro_node->leaves() != 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Union type must have exactly two branches", K(ret));
    } else {
      avro::NodePtr branch_0 = avro_node->leafAt(0);
      avro::NodePtr branch_1 = avro_node->leafAt(1);
      if (branch_0->type() == ::avro::AVRO_NULL) {
        result = branch_1;
      } else if (branch_1->type() == ::avro::AVRO_NULL) {
        result = branch_0;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Union type must have exactly one null branch", K(ret));
      }
    }
  }
  return ret;
}

int AvroUtils::decode_binary(ObIAllocator &allocator,
                             const avro::NodePtr &avro_node,
                             avro::Decoder &decoder,
                             ObString &value)
{
  int ret = OB_SUCCESS;
  switch (avro_node->type()) {
    case avro::Type::AVRO_STRING: {
      std::string str = decoder.decodeString();
      OZ(ob_write_string(allocator, ObString(str.size(), str.data()), value, true));
      break;
    }
    case avro::Type::AVRO_BYTES: {
      std::vector<uint8_t> bytes = decoder.decodeBytes();
      OZ(ob_write_string(allocator,
                         ObString(bytes.size(), reinterpret_cast<const char *>(bytes.data())),
                         value,
                         true));
      break;
    }
    case avro::Type::AVRO_FIXED: {
      std::vector<uint8_t> bytes = decoder.decodeFixed(avro_node->fixedSize());
      OZ(ob_write_string(allocator,
                         ObString(bytes.size(), reinterpret_cast<const char *>(bytes.data())),
                         value,
                         true));
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported binary type", K(ret), K(avro_node->type()));
    }
  }
  return ret;
}

int AvroUtils::decode_binary(ObIAllocator &allocator,
                             const avro::NodePtr &avro_node,
                             avro::Decoder &decoder,
                             std::optional<ObString> &value)
{
  int ret = OB_SUCCESS;
  avro::NodePtr target_avro_node = NULL;
  if (avro::Type::AVRO_UNION == avro_node->type()) {
    size_t union_index = decoder.decodeUnionIndex();
    if (OB_UNLIKELY(union_index >= avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid union_index", K(ret), K(union_index), K(avro_node->leaves()));
    } else if (avro::Type::AVRO_NULL == avro_node->leafAt(union_index)->type()) {
      decoder.decodeNull();
      value = std::nullopt;
      target_avro_node = NULL;
    } else {
      target_avro_node = avro_node->leafAt(union_index);
    }
  } else {
    target_avro_node = avro_node;
  }

  if (OB_SUCC(ret) && NULL != target_avro_node) {
    ObString tmp_value;
    if (OB_FAIL(decode_binary(allocator, target_avro_node, decoder, tmp_value))) {
      LOG_WARN("failed to decode binary", K(ret));
    } else {
      value = tmp_value;
    }
  }
  return ret;
}

template <typename T>
typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                              || std::is_same_v<T, bool> || std::is_same_v<T, float>
                              || std::is_same_v<T, double>,
                          int>
AvroUtils::decode_primitive(const avro::NodePtr &avro_node, avro::Decoder &decoder, T &value)
{
  int ret = OB_SUCCESS;
  switch (avro_node->type()) {
    case avro::Type::AVRO_INT: {
      value = decoder.decodeInt();
      break;
    }
    case avro::Type::AVRO_LONG: {
      value = decoder.decodeLong();
      break;
    }
    case avro::Type::AVRO_FLOAT: {
      value = decoder.decodeFloat();
      break;
    }
    case avro::Type::AVRO_DOUBLE: {
      value = decoder.decodeDouble();
      break;
    }
    case avro::Type::AVRO_BOOL: {
      value = decoder.decodeBool();
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported primitive type", K(ret), K(avro_node->type()));
    }
  }
  return ret;
}

template <typename T>
std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, bool>
                     || std::is_same_v<T, float> || std::is_same_v<T, double>,
                 int>
AvroUtils::decode_primitive(const avro::NodePtr &avro_node,
                            avro::Decoder &decoder,
                            std::optional<T> &value)
{
  int ret = OB_SUCCESS;
  avro::NodePtr target_avro_node = NULL;
  if (avro::Type::AVRO_UNION == avro_node->type()) {
    size_t union_index = decoder.decodeUnionIndex();
    if (OB_UNLIKELY(union_index >= avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("union_index out of range", K(ret), K(union_index), K(avro_node->leaves()));
    } else if (avro::Type::AVRO_NULL == avro_node->leafAt(union_index)->type()) {
      decoder.decodeNull();
      value = std::nullopt;
      target_avro_node = NULL;
    } else {
      target_avro_node = avro_node->leafAt(union_index);
    }
  } else {
    target_avro_node = avro_node;
  }

  if (OB_SUCC(ret) && NULL != target_avro_node) {
    T tmp_value;
    if (OB_FAIL(decode_primitive(target_avro_node, decoder, tmp_value))) {
      LOG_WARN("failed to decode primitive", K(ret));
    } else {
      value = tmp_value;
    }
  }
  return ret;
}

template <typename K, typename V>
std::enable_if_t<std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>
                     || std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>
                     || std::is_same_v<V, bool>,
                 int>
AvroUtils::decode_primitive_map(const avro::NodePtr &avro_node,
                                avro::Decoder &decoder,
                                ObIArray<std::pair<K, V>> &value)
{
  int ret = OB_SUCCESS;
  avro::NodePtr target_avro_node = NULL;
  if (avro::Type::AVRO_UNION == avro_node->type()) {
    size_t union_index = decoder.decodeUnionIndex();
    if (OB_UNLIKELY(union_index >= avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid union index", K(ret), K(union_index));
    } else if (avro::Type::AVRO_NULL == avro_node->leafAt(union_index)->type()) {
      decoder.decodeNull();
    } else {
      target_avro_node = avro_node->leafAt(union_index);
    }
  } else {
    target_avro_node = avro_node;
  }

  if (OB_SUCC(ret) && NULL != target_avro_node) {
    // ARRAY<RECORD<K, V>>
    avro::NodePtr element_record_node_type = NULL;
    if (OB_UNLIKELY(avro::Type::AVRO_ARRAY != target_avro_node->type()
                    || 1 != target_avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid avro node",
               K(ret),
               K(target_avro_node->type()),
               K(target_avro_node->leaves()));
    } else if (OB_FALSE_IT(element_record_node_type = target_avro_node->leafAt(0))) {
    } else if (OB_UNLIKELY(avro::Type::AVRO_RECORD != element_record_node_type->type()
                           || 2 != element_record_node_type->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid element avro node",
               K(ret),
               K(element_record_node_type->type()),
               K(element_record_node_type->leaves()));
    } else {
      ObArray<std::pair<K, V>> tmp_array;
      const avro::NodePtr &map_key_node_type = element_record_node_type->leafAt(0);
      const avro::NodePtr &map_value_node_type = element_record_node_type->leafAt(1);
      for (size_t n = decoder.arrayStart(); OB_SUCC(ret) && n != 0; n = decoder.arrayNext()) {
        for (size_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          K tmp_key;
          V tmp_value;
          if (OB_FAIL(AvroUtils::decode_primitive(map_key_node_type, decoder, tmp_key))) {
            LOG_WARN("failed to decode map key", K(ret));
          } else if (OB_FAIL(
                         AvroUtils::decode_primitive(map_value_node_type, decoder, tmp_value))) {
            LOG_WARN("failed to decode map value", K(ret));
          } else if (OB_FAIL(tmp_array.push_back(std::make_pair(tmp_key, tmp_value)))) {
            LOG_WARN("failed to push back array", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(value.assign(tmp_array))) {
          LOG_WARN("failed to assign array", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename K, typename V>
std::enable_if_t<(std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>)
                     && std::is_same_v<V, ObString>,
                 int>
AvroUtils::decode_binary_map(ObIAllocator &allocator,
                             const avro::NodePtr &avro_node,
                             avro::Decoder &decoder,
                             ObIArray<std::pair<K, V>> &value)
{
  int ret = OB_SUCCESS;
  avro::NodePtr target_avro_node = NULL;
  if (avro::Type::AVRO_UNION == avro_node->type()) {
    size_t union_index = decoder.decodeUnionIndex();
    if (OB_UNLIKELY(union_index >= avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid union index", K(ret), K(union_index));
    } else if (avro::Type::AVRO_NULL == avro_node->leafAt(union_index)->type()) {
      decoder.decodeNull();
    } else {
      target_avro_node = avro_node->leafAt(union_index);
    }
  } else {
    target_avro_node = avro_node;
  }

  if (OB_SUCC(ret) && NULL != target_avro_node) {
    // ARRAY<RECORD<K, V>>
    avro::NodePtr element_record_node_type = NULL;
    if (OB_UNLIKELY(avro::Type::AVRO_ARRAY != target_avro_node->type()
                    || 1 != target_avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid avro node",
               K(ret),
               K(target_avro_node->type()),
               K(target_avro_node->leaves()));
    } else if (OB_FALSE_IT(element_record_node_type = target_avro_node->leafAt(0))) {
    } else if (OB_UNLIKELY(avro::Type::AVRO_RECORD != element_record_node_type->type()
                           || 2 != element_record_node_type->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid element avro node",
               K(ret),
               K(element_record_node_type->type()),
               K(element_record_node_type->leaves()));
    } else {
      ObArray<std::pair<K, V>> tmp_array;
      const avro::NodePtr &map_key_node_type = element_record_node_type->leafAt(0);
      const avro::NodePtr &map_value_node_type = element_record_node_type->leafAt(1);
      for (size_t n = decoder.arrayStart(); OB_SUCC(ret) && n != 0; n = decoder.arrayNext()) {
        for (size_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          K tmp_key;
          V tmp_value;
          if (OB_FAIL(AvroUtils::decode_primitive(map_key_node_type, decoder, tmp_key))) {
            LOG_WARN("failed to decode map key", K(ret));
          } else if (OB_FAIL(AvroUtils::decode_binary(allocator,
                                                      map_value_node_type,
                                                      decoder,
                                                      tmp_value))) {
            LOG_WARN("failed to decode map value", K(ret));
          } else if (OB_FAIL(tmp_array.push_back(std::make_pair(tmp_key, tmp_value)))) {
            LOG_WARN("failed to push back array", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(value.assign(tmp_array))) {
          LOG_WARN("failed to assign array", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename V>
std::enable_if_t<std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>, int>
AvroUtils::decode_primitive_array(const avro::NodePtr &avro_node,
                                  avro::Decoder &decoder,
                                  ObIArray<V> &value)
{
  int ret = OB_SUCCESS;
  avro::NodePtr target_avro_node = NULL;
  if (avro::Type::AVRO_UNION == avro_node->type()) {
    size_t union_index = decoder.decodeUnionIndex();
    if (OB_UNLIKELY(union_index >= avro_node->leaves())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid union index", K(ret), K(union_index));
    } else if (avro::Type::AVRO_NULL == avro_node->leafAt(union_index)->type()) {
      decoder.decodeNull();
    } else {
      target_avro_node = avro_node->leafAt(union_index);
    }
  } else {
    target_avro_node = avro_node;
  }

  if (OB_SUCC(ret) && NULL != target_avro_node) {
    // 如果 target_avro_node 不为空，意味着存在数据，需要真的进行解析
    ObArray<V> tmp_array;
    for (size_t n = decoder.arrayStart(); OB_SUCC(ret) && n != 0; n = decoder.arrayNext()) {
      for (size_t i = 0; OB_SUCC(ret) && i < n; ++i) {
        V tmp_value;
        if (OB_FAIL(AvroUtils::decode_primitive(target_avro_node->leafAt(0), decoder, tmp_value))) {
          LOG_WARN("failed to decode array's element", K(ret), K(i));
        } else if (OB_FAIL(tmp_array.push_back(tmp_value))) {
          LOG_WARN("failed to push back array", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(value.assign(tmp_array))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    }
  }
  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase