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

#include "sql/table_format/iceberg/spec/partition.h"

#include "lib/string/ob_sql_string.h"
#include "share/ob_define.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "sql/table_format/iceberg/spec/manifest.h"

#include <regex>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int Transform::init_from_string(const ObString &transform_str)
{
  int ret = OB_SUCCESS;
  if (0 == transform_str.case_compare(IDENTITY)) {
    transform_type = TransformType::Identity;
  } else if (0 == transform_str.case_compare(YEAR)) {
    transform_type = TransformType::Year;
  } else if (0 == transform_str.case_compare(MONTH)) {
    transform_type = TransformType::Month;
  } else if (0 == transform_str.case_compare(DAY)) {
    transform_type = TransformType::Day;
  } else if (0 == transform_str.case_compare(HOUR)) {
    transform_type = TransformType::Hour;
  } else if (0 == transform_str.case_compare(VOID)) {
    transform_type = TransformType::Void;
  } else {
    // Match bucket[16] or truncate[4]
    static const std::regex param_regex(R"((bucket|truncate)\[(\d+)\])");
    std::string str(transform_str.ptr(), transform_str.length());
    std::smatch match;
    if (std::regex_match(str, match, param_regex)) {
      const std::string type_str = match[1];
      const ObString type_ob_str = ObString(type_str.length(), type_str.data());
      const int32_t param_value = std::stoi(match[2]);

      if (0 == type_ob_str.case_compare(BUCKET)) {
        transform_type = TransformType::Bucket;
        param = param_value;
      } else if (0 == type_ob_str.case_compare(TRUNCATE)) {
        transform_type = TransformType::Truncate;
        param = param_value;
      }
    }
  }

  if (TransformType::Invalid == transform_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid transform type", K(ret), K(transform_str));
  }
  return ret;
}

int Transform::assign(const Transform &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    transform_type = other.transform_type;
    param = other.param;
  }
  return ret;
}

int Transform::get_result_type(const ObObjType source_type, ObObjType &result_type) const
{
  return get_result_type(transform_type, source_type, result_type);
}

int Transform::get_result_type(TransformType transform_type, const ObObjType source_type, ObObjType &result_type)
{
  int ret = OB_SUCCESS;
  switch (transform_type) {
    case TransformType::Identity: {
      result_type = source_type;
      break;
    }
    case TransformType::Bucket: {
      switch (source_type) {
        case ObTinyIntType: // bool
        case ObInt32Type: // int
        case ObIntType: // long
        case ObDecimalIntType: // decimal
        case ObDateType: // date
        case ObTimeType: // time
        case ObDateTimeType:
        case ObTimestampType:
        case ObVarcharType:
          result_type = ObInt32Type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Truncate: {
      switch (source_type) {
        case ObInt32Type:
        case ObIntType:
        case ObDecimalIntType:
        case ObVarcharType:
          result_type = source_type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Year: {
      switch (source_type) {
        case ObDateType:
        case ObDateTimeType:
        case ObTimestampType:
          result_type = ObInt32Type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Month: {
      switch (source_type) {
        case ObDateType:
        case ObDateTimeType:
        case ObTimestampType:
          result_type = ObInt32Type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Day: {
      switch (source_type) {
        case ObDateType:
        case ObDateTimeType:
        case ObTimestampType:
          result_type = ObInt32Type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Hour: {
      switch (source_type) {
        case ObDateTimeType:
        case ObTimestampType:
          result_type = ObInt32Type;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
      }
      break;
    }
    case TransformType::Void: {
      result_type = source_type;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported transform type", K(ret), K(transform_type));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("unsupported type", K(ret), K(source_type), K(transform_type));
  }
  return ret;
}

int Transform::get_part_expr(const Transform &transform, const share::schema::ObColumnSchemaV2 &col_schema,
                             ObIAllocator &allocator, ObString &str)
{
  int ret = OB_SUCCESS;
  ObSqlString trans_str;
  bool trunc_or_bucket = false;
  int param_val = 0;
  switch (transform.transform_type) {
    case TransformType::Identity: {
      // do nothing
      break;
    }
    case TransformType::Bucket:
    case TransformType::Truncate: {
      if (TransformType::Bucket == transform.transform_type) {
        OZ (trans_str.append(BUCKET));
      } else {
        OZ (trans_str.append(TRUNCATE));
      }
      if (transform.param.has_value()) {
        param_val = transform.param.value();
      }
      trunc_or_bucket = true;
      break;
    }
    case TransformType::Year: {
      OZ (trans_str.append(YEAR));
      break;
    }
    case TransformType::Month: {
      OZ (trans_str.append(MONTH));
      break;
    }
    case TransformType::Day: {
      OZ (trans_str.append(DAY));
      break;
    }
    case TransformType::Hour: {
      OZ (trans_str.append(HOUR));
      break;
    }
    case TransformType::Void: {
      OZ (trans_str.append(VOID));
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported transform type", K(ret), K(transform.transform_type));
    }
  }

  ObSqlString part_expr;
  ObString col_name = col_schema.get_column_name_str();
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!trunc_or_bucket) {
    if (!trans_str.empty() && OB_FAIL(part_expr.append_fmt("%.*s(%.*s)", trans_str.length(), trans_str.ptr(),
                                                           col_name.length(), col_name.ptr()))) {
      LOG_WARN("append fmt failed", K(ret));
    } else if (trans_str.empty() && OB_FAIL(part_expr.append_fmt("%.*s", col_name.length(), col_name.ptr()))) {
      LOG_WARN("append fmt failed", K(ret));
    }
  } else if (trunc_or_bucket && OB_FAIL(part_expr.append_fmt("%.*s(%.*s, %d)", trans_str.length(), trans_str.ptr(),
                                                        col_name.length(), col_name.ptr(), param_val))) {
    LOG_WARN("append fmt failed", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, part_expr.string(), str))) {
    LOG_WARN("write string failed", K(ret));
  }
  return ret;
}

PartitionField::PartitionField(ObIAllocator &allocator) : SpecWithAllocator(allocator) {}

int PartitionField::init_from_json(const ObJsonObject &json_object,
                                   std::optional<int32_t> next_partition_field_id)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObString transform_str;
  if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator_, json_object, NAME, name))) {
    LOG_WARN("failed to get name", K(ret));
  } else if (OB_FAIL(ObCatalogJsonUtils::get_string(tmp_allocator,
                                                    json_object,
                                                    TRANSFORM,
                                                    transform_str))) {
    LOG_WARN("failed to get transform");
  } else if (OB_FAIL(transform.init_from_string(transform_str))) {
    LOG_WARN("failed to init transform", K(ret));
  } else if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SOURCE_ID, source_id))) {
    LOG_WARN("failed to get source-id", K(ret));
  } else {
    std::optional<int32_t> tmp_field_id;
    if (OB_FAIL(OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, FIELD_ID, tmp_field_id)))) {
      LOG_WARN("failed to get field-id", K(ret));
    } else if (tmp_field_id.has_value()) {
      field_id = tmp_field_id.value();
    } else if (next_partition_field_id.has_value()) {
      // 在老的标准里面，PartitionField 里面的 field_id 可能为空，所以这里赋值 INVALID_FIELD_ID
      // 然后在外部重新进行 field_id 赋值。 其需要从 PARTITION_DATA_ID_START 开始递增推算
      // field_id
      field_id = next_partition_field_id.value();
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there is no valid field id", K(ret));
    }
  }
  return ret;
}

int PartitionField::assign(const PartitionField &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    source_id = other.source_id;
    field_id = other.field_id;
    OZ(transform.assign(other.transform));
    OZ(ob_write_string(allocator_, other.name, name));
  }
  return ret;
}

PartitionSpec::PartitionSpec(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      fields(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int PartitionSpec::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_fields = json_object.get_value(FIELDS);
   if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SPEC_ID, spec_id))) {
    LOG_WARN("failed to get spec-id", K(ret));
  } else if (NULL == json_fields || ObJsonNodeType::J_ARRAY != json_fields->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fields", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < json_fields->element_count(); i++) {
      ObIJsonBase *json_field = NULL;
      PartitionField *partition_field = NULL;
      if (OB_FAIL(json_fields->get_array_element(i, json_field))) {
        LOG_WARN("failed to get field", K(ret));
      } else if (ObJsonNodeType::J_OBJECT != json_field->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid field", K(ret));
      } else if (OB_ISNULL(partition_field = OB_NEWx(PartitionField, &allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc", K(ret));
      } else if (OB_FAIL(partition_field->init_from_json(*down_cast<ObJsonObject *>(json_field)))) {
        LOG_WARN("failed to parse field", K(ret));
      } else {
        OZ(fields.push_back(partition_field));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 如果 PartitionFields 为空，则 last_assigned_field_id 值不修改。否则遍历 PartitionFields
    // 匹配最大的 field id
    last_assigned_field_id = PARTITION_DATA_ID_START - 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < fields.count(); i++) {
      last_assigned_field_id = std::max(last_assigned_field_id, fields.at(i)->field_id);
    }
  }

  return ret;
}

int PartitionSpec::init_from_v1_json(int32_t next_partition_field_id, const ObJsonArray &json_partition_fields)
{
  int ret = OB_SUCCESS;
  // v1 spec id always = 0
  spec_id = INITIAL_SPEC_ID;
  int32_t last_assigned_partition_field_id = next_partition_field_id - 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < json_partition_fields.element_count(); i++) {
    PartitionField *partition_field = NULL;
    ObIJsonBase *json_field = NULL;
    if (OB_FAIL(json_partition_fields.get_array_element(i, json_field))) {
      LOG_WARN("failed to get json field", K(ret));
    } else if (ObJsonNodeType::J_OBJECT != json_field->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid field", K(ret));
    } else if (OB_ISNULL(partition_field = OB_NEWx(PartitionField, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_FAIL(partition_field->init_from_json(*down_cast<ObJsonObject *>(json_field),
                                                       next_partition_field_id))) {
      LOG_WARN("failed to parse field");
    } else if (OB_FAIL(fields.push_back(partition_field))) {
      LOG_WARN("failed to add partition field", K(ret));
    } else {
      last_assigned_partition_field_id
          = std::max(last_assigned_partition_field_id, partition_field->field_id);
      next_partition_field_id++;
    }
  }
  OX(last_assigned_field_id = last_assigned_partition_field_id);
  return ret;
}

int PartitionSpec::convert_to_unpartitioned()
{
  int ret = OB_SUCCESS;
  spec_id = INITIAL_SPEC_ID;
  last_assigned_field_id = PARTITION_DATA_ID_START - 1;
  fields.reset();
  return ret;
}

int PartitionSpec::assign(const PartitionSpec &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    spec_id = other.spec_id;
    last_assigned_field_id = other.last_assigned_field_id;
    OZ(ObIcebergUtils::deep_copy_array_object(allocator_, other.fields, fields));
  }
  return ret;
}

bool PartitionSpec::is_partitioned() const
{
  bool has_valid_partition = false;
  for (int64_t i = 0; !has_valid_partition && i < fields.count(); i++) {
    if (TransformType::Void != fields.at(i)->transform.transform_type) {
      has_valid_partition = true;
    }
  }
  return has_valid_partition;
}

bool PartitionSpec::is_unpartitioned() const
{
  return !is_partitioned();
}

int PartitionKey::assign(const PartitionKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    partition_spec_id = other.partition_spec_id;
    OZ(partition_values.assign(other.partition_values));
  }
  return ret;
}

int PartitionKey::init_from_manifest_entry(const ManifestEntry &manifest_entry)
{
  int ret = OB_SUCCESS;
  partition_spec_id = manifest_entry.partition_spec_id;
  if (OB_FAIL(partition_values.assign(manifest_entry.data_file.partition))) {
    LOG_WARN("failed to assign part values");
  }
  return ret;
}

int PartitionKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = do_hash(partition_spec_id, hash_val);
  for (int64_t i = 0; i < partition_values.count(); ++i) {
    hash_val = partition_values.at(i).hash(hash_val);
  }
  return ret;
}

bool PartitionKey::operator==(const PartitionKey &other) const
{
  bool is_equal = false;
  if (partition_spec_id == other.partition_spec_id
      && partition_values.count() == other.partition_values.count()) {
    is_equal = true;
    for (int64_t i = 0; is_equal && i < partition_values.count(); ++i) {
      if (!partition_values.at(i).is_equal(other.partition_values.at(i))) {
        is_equal = false;
      }
    }
  }
  return is_equal;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase