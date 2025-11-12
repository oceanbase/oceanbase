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

#ifndef PARTITION_H
#define PARTITION_H

#include "lib/container/ob_array.h"
#include "lib/json/ob_json.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/table_format/iceberg/spec/spec.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

// Transform types used for partitioning
enum class TransformType
{
  Invalid = 0,
  Identity,
  Bucket,
  Truncate,
  Year,
  Month,
  Day,
  Hour,
  Void,
};

class Transform
{
public:
  TransformType transform_type = TransformType::Invalid;
  std::optional<int32_t> param = std::nullopt;
  int init_from_string(const ObString &transform_str);
  int assign(const Transform &other);
  int get_result_type(const ObObjType source_type, ObObjType &result_type) const;
  static int get_result_type(TransformType transform_type, const ObObjType source_type, ObObjType &result_type);
  static int get_part_expr(const Transform &transform, const share::schema::ObColumnSchemaV2 &col_schema,
                           ObIAllocator &allocator, ObString &str);

  static constexpr const char *IDENTITY = "identity";
  static constexpr const char *BUCKET = "bucket";
  static constexpr const char *TRUNCATE = "truncate";
  static constexpr const char *YEAR = "year";
  static constexpr const char *MONTH = "month";
  static constexpr const char *DAY = "day";
  static constexpr const char *HOUR = "hour";
  static constexpr const char *VOID = "void";
};

class PartitionField : public SpecWithAllocator
{
public:
  PartitionField(ObIAllocator &allocator);
  int init_from_json(const ObJsonObject &json_object,
                     std::optional<int32_t> next_partition_field_id = std::nullopt);
  int assign(const PartitionField &other);
  int32_t source_id;
  int32_t field_id;
  ObString name;
  Transform transform;

  TO_STRING_EMPTY();

  static constexpr const char *SOURCE_ID = "source-id";
  static constexpr const char *FIELD_ID = "field-id";
  static constexpr const char *NAME = "name";
  static constexpr const char *TRANSFORM = "transform";
};

class PartitionSpec : public SpecWithAllocator
{
public:
  explicit PartitionSpec(ObIAllocator &allocator);
  int init_from_json(const ObJsonObject &json_object);
  int init_from_v1_json(int32_t next_partition_field_id, const ObJsonArray &json_partition_fields);
  int convert_to_unpartitioned(); // 转换为 unpartitioned PartitionSpec,主要用于 UT
  int assign(const PartitionSpec &other);
  // 浅拷贝，因为 一个 Manifest 下面的 ManifestEntry 其实都是同一个 PartitionSpec，这里浅拷贝即可
  int shallow_assign(const PartitionSpec &other);
  bool is_partitioned() const;
  bool is_unpartitioned() const;
  int get_partition_field_by_field_id(int32_t field_id, const PartitionField *&partition_field) const;
  int32_t spec_id;
  int32_t last_assigned_field_id; // 非持久化字段
  ObFixedArray<PartitionField *, ObIAllocator> fields;
  TO_STRING_KV(K(spec_id));
  static constexpr const char *SPEC_ID = "spec-id";
  static constexpr const char *FIELDS = "fields";
};

class PartitionKey
{
public:
  int assign(const PartitionKey &other);
  int init_from_manifest_entry(const ManifestEntry &manifest_entry);
  int hash(uint64_t &hash_val) const;
  bool operator== (const PartitionKey &other) const;

  int32_t partition_spec_id;
  ObArray<ObObj> partition_values;
};

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

#endif // PARTITION_H
