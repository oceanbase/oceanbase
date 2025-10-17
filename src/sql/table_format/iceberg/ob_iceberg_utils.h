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

#ifndef OB_ICEBERG_UTILS_H
#define OB_ICEBERG_UTILS_H

#include "lib/hash/ob_hashmap.h"
#include "lib/json_type/ob_json_parse.h"
#include "sql/engine/cmd/ob_load_data_parser.h"

#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>
#include <avro/Types.hh>
#include <optional>
#include <s2/base/casts.h>

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

class ObIcebergUtils
{
public:
  template <typename K, typename V>
  static bool get_map_value(ObIArray<std::pair<K, V>> &map, const K &key, V &value)
  {
    bool find = false;
    for (int64_t i = 0; !find && i < map.count(); ++i) {
      std::pair<K, V> &pair = map.at(i);
      if (pair.first == key) {
        find = true;
        value = pair.second;
      }
    }
    return find;
  }

  static int deep_copy_optional_string(ObIAllocator &allocator,
                                       const std::optional<ObString> &src,
                                       std::optional<ObString> dst);
  static int deep_copy_map_string(ObIAllocator &allocator,
                                  const ObIArray<std::pair<ObString, ObString>> &src,
                                  ObIArray<std::pair<ObString, ObString>> &dst);
  template <typename T>
  static int deep_copy_array_object(ObIAllocator &allocator,
                                    const ObIArray<T *> &src,
                                    ObIArray<T *> &dst);
  static uint64_t get_ob_column_id(int32_t iceberg_field_id);
  static int32_t get_iceberg_field_id(uint64_t ob_column_id);
};

class ObIcebergFileIOUtils
{
public:
  static int read_table_metadata(ObIAllocator &allocator,
                                 const ObString &filename,
                                 const ObString &access_info,
                                 char *&buf,
                                 int64_t &read_size);

  static int read(ObIAllocator &allocator,
                  const ObString &filename,
                  const ObString &access_info,
                  char *&buf,
                  int64_t &read_size,
                  bool enable_cache = true);

  static int is_exist(const ObString &filename, const ObString &access_info, bool &existed);
};

class ObCatalogJsonUtils
{
public:
  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool>,
                                   int>
  get_primitive(const ObJsonObject &json_object, const ObString &key, std::optional<T> &value);

  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool>,
                                   int>
  get_primitive(const ObJsonObject &json_object, const ObString &key, T &value);

  template <typename T>
  static typename std::enable_if<std::is_same<T, int32_t>::value || std::is_same<T, int64_t>::value,
                                 int>::type
  get_primitive_array(const ObJsonObject &json_object, const ObString &key, ObIArray<T> &value);

  static int get_string(ObIAllocator &allocator,
                        const ObJsonObject &json_object,
                        const ObString &key,
                        std::optional<ObString> &value);

  static int get_string(ObIAllocator &allocator,
                        const ObJsonObject &json_object,
                        const ObString &key,
                        ObString &value);

  // convert to template when needed
  static int convert_json_object_to_map(ObIAllocator &allocator,
                                        const ObJsonObject &json_object,
                                        ObIArray<std::pair<ObString, ObString>> &values);

  static int get_string_array(ObIAllocator &allocator,
                              const ObJsonObject &json_object,
                              const ObString &key,
                              ObIArray<ObString> &values);
};

class ObCatalogAvroUtils
{
public:
  template <avro::Type T>
  static int get_binary(ObIAllocator &allocator,
                        const avro::GenericRecord &avro_record,
                        const ObString &key,
                        ObString &value);

  template <avro::Type T>
  static int get_binary(ObIAllocator &allocator,
                        const avro::GenericRecord &avro_record,
                        const ObString &key,
                        std::optional<ObString> &value);

  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool>,
                                   int>
  get_primitive(const avro::GenericRecord &avro_record, const ObString &key, T &value);

  template <typename T>
  static typename std::enable_if_t<std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>
                                       || std::is_same_v<T, bool>,
                                   int>
  get_primitive(const avro::GenericRecord &avro_record,
                const ObString &key,
                std::optional<T> &value);

  // 因为 avro 的 map 类型的 key 列只能是 string，所以 iceberg 使用 Array<Record<Key, Value>> 来存储
  // map 因此这里解析按照 AVRO_ARRAY 类型进行处理
  template <typename K, typename V>
  static typename std::enable_if_t<std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>
                                       || std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>
                                       || std::is_same_v<V, bool>,
                                   int>
  get_primitive_map(const avro::GenericRecord &avro_record,
                    const ObString &key,
                    ObIArray<std::pair<K, V>> &value);

  template <avro::Type AVRO_TYPE, typename K, typename V>
  static typename std::enable_if_t<(std::is_same_v<K, int32_t> || std::is_same_v<K, int64_t>)
                                       && std::is_same_v<V, ObString>,
                                   int>
  get_binary_map(ObIAllocator &allocator,
                 const avro::GenericRecord &avro_record,
                 const ObString &key,
                 ObIArray<std::pair<K, V>> &value);

  template <typename V>
  static typename std::enable_if_t<std::is_same_v<V, int32_t> || std::is_same_v<V, int64_t>, int>
  get_primitive_array(const avro::GenericRecord &avro_record,
                      const ObString &key,
                      ObIArray<V> &value);

  template <avro::Type T>
  static int get_value(const avro::GenericRecord &avro_record,
                       const ObString &key,
                       const avro::GenericDatum *&value);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // OB_ICEBERG_UTILS_H
