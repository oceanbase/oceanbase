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

#ifndef OCEANBASE_AVRO_SCHEMA_UTIL_H
#define OCEANBASE_AVRO_SCHEMA_UTIL_H

#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"

#include <avro/Decoder.hh>
#include <avro/Types.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class FieldProjection
{
public:
  enum class Kind
  {
    Invalid = 0,
    Projected,
    NOT_EXISTED, // 意味着在 avro data schema 里面存在，但是在 ManifestEntry 里面没有对应的字段
  };
  explicit FieldProjection(ObIAllocator &allocator) : children_(allocator) {};

  TO_STRING_KV(K_(field_id), K_(kind));
  avro::NodePtr avro_node_ = NULL;
  int32_t field_id_ = -1; // 对应 ManifestFile/ManifestEntry 里面字段的 field_id
  Kind kind_ = Kind::Invalid;
  ObFixedArray<FieldProjection *, ObIAllocator> children_;
};

class SchemaProjection
{
public:
  explicit SchemaProjection(ObIAllocator &allocator);
  ObFixedArray<FieldProjection *, ObIAllocator> fields_;
};

class AvroSchemaProjectionUtils
{
public:
  static int project(ObIAllocator &allocator,
                     const StructType &expected_schema,
                     const avro::NodePtr &avro_node,
                     SchemaProjection &schema_projection);

private:
  static int project_nested(ObIAllocator &allocator,
                            const Type *expected_schema,
                            const avro::NodePtr &avro_node,
                            FieldProjection &field_projection);

  static int project_struct(ObIAllocator &allocator,
                            const StructType *expected_schema,
                            const avro::NodePtr &avro_node,
                            FieldProjection &field_projection);

  static int project_map(ObIAllocator &allocator,
                         const MapType *expected_schema,
                         const avro::NodePtr &avro_node,
                         FieldProjection &field_projection);

  static int project_list(ObIAllocator &allocator,
                          const ListType *expected_schema,
                          const avro::NodePtr &avro_node,
                          FieldProjection &field_projection);

  static int get_id(const avro::NodePtr &avro_node,
                    const ObString &attr_name,
                    const int64_t field_idx,
                    int32_t &id);
  static int get_element_id(const avro::NodePtr &avro_node, int32_t &id);
  static int get_key_id(const avro::NodePtr &avro_node, int32_t &id);
  static int get_value_id(const avro::NodePtr &avro_node, int32_t &id);
  static int get_field_id(const avro::NodePtr &avro_node, const int64_t field_idx, int32_t &id);
};

class AvroUtils
{
public:
  static int skip_decode(const avro::NodePtr &avro_node, avro::Decoder &decoder);

  static int unwarp_union(const avro::NodePtr &avro_node, avro::NodePtr &result);

  static int decode_binary(ObIAllocator &allocator,
                           const avro::NodePtr &avro_node,
                           avro::Decoder &decoder,
                           ObString &value);

  static int decode_binary(ObIAllocator &allocator,
                           const avro::NodePtr &avro_node,
                           avro::Decoder &decoder,
                           std::optional<ObString> &value);

  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool> || std::is_same_v<T, float>
                                       || std::is_same_v<T, double>,
                                   int>
  decode_primitive(const avro::NodePtr &avro_node, avro::Decoder &decoder, T &value);

  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool> || std::is_same_v<T, float>
                                       || std::is_same_v<T, double>,
                                   int>
  decode_primitive(const avro::NodePtr &avro_node, avro::Decoder &decoder, std::optional<T> &value);

  // 因为 avro 的 map 类型的 key 列只能是 string，所以 iceberg 使用 Array<Record<Key, Value>> 来存储
  // map 因此这里解析按照 AVRO_ARRAY 类型进行处理
  template <typename K, typename V>
  static typename std::enable_if_t<std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>
                                       || std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>
                                       || std::is_same_v<V, bool>,
                                   int>
  decode_primitive_map(const avro::NodePtr &avro_node,
                       avro::Decoder &decoder,
                       ObIArray<std::pair<K, V>> &value);

  template <typename K, typename V>
  static typename std::enable_if_t<(std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>)
                                       && std::is_same_v<V, ObString>,
                                   int>
  decode_binary_map(ObIAllocator &allocator,
                    const avro::NodePtr &avro_node,
                    avro::Decoder &decoder,
                    ObIArray<std::pair<K, V>> &value);

  template <typename V>
  static typename std::enable_if_t<std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>, int>
  decode_primitive_array(const avro::NodePtr &avro_node,
                         avro::Decoder &decoder,
                         ObIArray<V> &value);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_AVRO_SCHEMA_UTIL_H
