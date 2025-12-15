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

#include "sql/table_format/iceberg/spec/manifest.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "storage/lob/ob_lob_manager.h"

#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

OB_SERIALIZE_MEMBER(ObSerializableDataFile,
                    content_,
                    file_format_,
                    record_count_,
                    file_size_in_bytes_,
                    file_path_)

ManifestMetadata::ManifestMetadata(ObIAllocator &allocator)
    : SpecWithAllocator(allocator), schema(allocator), partition_spec(allocator)
{
}

int ManifestMetadata::init_from_metadata(
    const std::map<std::string, std::vector<uint8_t>> &metadata)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(SCHEMA);
    if (it == metadata.end()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema not existed", K(ret));
    } else if (OB_FAIL(init_schema_from_metadata(
                   ObString(it->second.size(), reinterpret_cast<const char *>(it->second.data())),
                   schema))) {
      LOG_WARN("failed to init schema", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(SCHEMA_ID);
    if (it == metadata.end()) {
      schema_id = DEFAULT_SCHEMA_ID;
    } else {
      std::string tmp(it->second.begin(), it->second.end());
      schema_id = std::stoi(tmp);
    }
  }

  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(PARTITION_SPEC);
    if (it == metadata.end()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("partition-spec not existed", K(ret));
    } else if (OB_FAIL(init_partition_fields_from_metadata(
                   ObString(it->second.size(), reinterpret_cast<const char *>(it->second.data())),
                   partition_spec))) {
      LOG_WARN("failed to init partition-spec", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(PARTITION_SPEC_ID);
    if (it == metadata.end()) {
      partition_spec_id = INITIAL_SPEC_ID;
    } else {
      std::string tmp(it->second.begin(), it->second.end());
      partition_spec_id = std::stoi(tmp);
    }
  }

  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(FORMAT_VERSION);
    if (it == metadata.end()) {
      format_version = FormatVersion::V1;
    } else {
      std::string tmp(it->second.begin(), it->second.end());
      int32_t format_version_num = std::stoi(tmp);
      if (format_version_num < static_cast<int32_t>(FormatVersion::V1)
          || format_version_num > static_cast<int32_t>(FormatVersion::V3)
          || (format_version_num == static_cast<int32_t>(FormatVersion::V3)
              && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_5_1_0)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported iceberg version", K(ret), K(format_version_num));
      } else {
        format_version = static_cast<FormatVersion>(format_version_num);
      }
    }
  }

  if (OB_SUCC(ret)) {
    const auto &it = metadata.find(CONTENT);
    if (it == metadata.end()) {
      // v1 always use DATA
      content = ManifestContent::DATA;
    } else {
      ObString manifest_content_type_str
          = ObString(it->second.size(), reinterpret_cast<const char *>(it->second.data()));
      if (0 == manifest_content_type_str.case_compare("data")) {
        content = ManifestContent::DATA;
      } else if (0 == manifest_content_type_str.case_compare("deletes")) {
        content = ManifestContent::DELETES;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported manifest content type", K(ret), K(manifest_content_type_str));
      }
    }
  }

  return ret;
}

int ManifestMetadata::init_schema_from_metadata(const ObString &metadata, Schema &schema)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObJsonNode *json_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&tmp_allocator, metadata, json_node))) {
    LOG_WARN("failed to parse json", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_OBJECT != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json object", K(ret));
  } else if (OB_FAIL(schema.init_from_json(*down_cast<ObJsonObject *>(json_node)))) {
    LOG_WARN("failed to init schema", K(ret));
  }
  return ret;
}

int ManifestMetadata::init_partition_fields_from_metadata(const ObString &metadata,
                                                          PartitionSpec &partition_spec)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObJsonNode *json_node = NULL;
  if (OB_FAIL(ObJsonParser::get_tree(&tmp_allocator, metadata, json_node))) {
    LOG_WARN("failed to parse json", K(ret));
  } else if (OB_ISNULL(json_node) || ObJsonNodeType::J_ARRAY != json_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid json array", K(ret));
  } else if (OB_FAIL(partition_spec.init_from_v1_json(PARTITION_DATA_ID_START,
                                                      *down_cast<ObJsonArray *>(json_node)))) {
    LOG_WARN("failed to init partition-spec", K(ret));
  }
  return ret;
}

DataFile::DataFile(ObIAllocator &allocator)
    : SpecWithAllocator(allocator), partition(allocator_), column_sizes(allocator_),
      value_counts(allocator_), null_value_counts(allocator_), nan_value_counts(allocator_),
      lower_bounds(allocator_), upper_bounds(allocator_), split_offsets(allocator_),
      equality_ids(allocator_)
{
}

int DataFile::decode_field(const ManifestMetadata &manifest_metadata,
                           const FieldProjection &field_projection,
                           avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  switch (field_projection.field_id_) {
    case CONTENT_FIELD_ID: {
      std::optional<int32_t> tmp_data_file_content;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_data_file_content))) {
        LOG_WARN("failed to get content", K(ret));
      } else if (!tmp_data_file_content.has_value()) {
        // all v1 files are data files
        content = DataFileContent::DATA;
      } else if (tmp_data_file_content.value() < static_cast<int32_t>(DataFileContent::DATA)
                 || tmp_data_file_content.value()
                        > static_cast<int32_t>(DataFileContent::EQUALITY_DELETES)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid data file content", K(ret));
      } else {
        content = static_cast<DataFileContent>(tmp_data_file_content.value());
      }
      break;
    }
    case FILE_PATH_FIELD_ID: {
      // file_path
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           file_path))) {
        LOG_WARN("fail to get file_path", K(ret));
      }
      break;
    }
    case FILE_FORMAT_FIELD_ID: {
      // file_format
      ObString tmp_file_format;
      ObArenaAllocator tmp_allocator;
      if (OB_FAIL(AvroUtils::decode_binary(tmp_allocator,
                                           field_projection.avro_node_,
                                           decoder,
                                           tmp_file_format))) {
        LOG_WARN("failed to get file_format", K(ret));
      } else if (0 == tmp_file_format.case_compare("AVRO")) {
        file_format = DataFileFormat::AVRO;
      } else if (0 == tmp_file_format.case_compare("ORC")) {
        file_format = DataFileFormat::ORC;
      } else if (0 == tmp_file_format.case_compare("PARQUET")) {
        file_format = DataFileFormat::PARQUET;
      } else if (0 == tmp_file_format.case_compare("PUFFIN")) {
        file_format = DataFileFormat::PUFFIN;
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported data file format", K(ret), K(tmp_file_format));
      }
      break;
    }
    case PARTITION_FIELD_ID: {
      // partition
      if (OB_FAIL(decode_partitions_(manifest_metadata, field_projection, decoder))) {
        LOG_WARN("fail to get partition", K(ret));
      }
      break;
    }
    case RECORD_COUNT_FIELD_ID: {
      // record_count
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, record_count))) {
        LOG_WARN("fail to get record_count", K(ret));
      }
      break;
    }
    case FILE_SIZE_IN_BYTES_FIELD_ID: {
      // file_size_in_bytes
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              file_size_in_bytes))) {
        LOG_WARN("failed to get file_size_in_bytes", K(ret));
      }
      break;
    }
    case COLUMN_SIZE_FIELD_ID: {
      // column sizes
      if (OB_FAIL(AvroUtils::decode_primitive_map(field_projection.avro_node_,
                                                  decoder,
                                                  column_sizes))) {
        LOG_WARN("fail to get column_sizes", K(ret));
      }
      break;
    }
    case VALUE_COUNTS_FIELD_ID: {
      // value_counts
      if (OB_FAIL(AvroUtils::decode_primitive_map(field_projection.avro_node_,
                                                  decoder,
                                                  value_counts))) {
        LOG_WARN("failed to get value_counts", K(ret));
      }
      break;
    }
    case NULL_VALUE_COUNTS_FIELD_ID: {
      // null_value_counts
      if (OB_FAIL(AvroUtils::decode_primitive_map(field_projection.avro_node_,
                                                  decoder,
                                                  null_value_counts))) {
        LOG_WARN("failed to get null_value_counts", K(ret));
      }
      break;
    }
    case NAN_VALUE_COUNTS_FIELD_ID: {
      // nan_value_counts
      if (OB_FAIL(AvroUtils::decode_primitive_map(field_projection.avro_node_,
                                                  decoder,
                                                  nan_value_counts))) {
        LOG_WARN("failed to get nan_value_counts", K(ret));
      }
      break;
    }
    case LOWER_BOUNDS_FIELD_ID: {
      // lower_bounds
      if (OB_FAIL(AvroUtils::decode_binary_map(allocator_,
                                               field_projection.avro_node_,
                                               decoder,
                                               lower_bounds))) {
        LOG_WARN("failed to get lower_bounds", K(ret));
      }
      break;
    }
    case UPPER_BOUNDS_FIELD_ID: {
      // upper_bounds
      if (OB_FAIL(AvroUtils::decode_binary_map(allocator_,
                                               field_projection.avro_node_,
                                               decoder,
                                               upper_bounds))) {
        LOG_WARN("failed to get upper_bounds", K(ret));
      }
      break;
    }
    case KEY_METADATA_FIELD_ID: {
      // key_metadata
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           key_metadata))) {
        LOG_WARN("failed to get key_metadata", K(ret));
      }
      break;
    }
    case SPLIT_OFFSETS_FIELD_ID: {
      // split_offsets
      if (OB_FAIL(AvroUtils::decode_primitive_array(field_projection.avro_node_,
                                                    decoder,
                                                    split_offsets))) {
        LOG_WARN("failed to get split_offsets", K(ret));
      }
      break;
    }
    case EQUALITY_IDS_FIELD_ID: {
      // equality_deletes
      if (OB_FAIL(AvroUtils::decode_primitive_array(field_projection.avro_node_,
                                                    decoder,
                                                    equality_ids))) {
        LOG_WARN("failed to get equality_ids", K(ret));
      } else if (OB_UNLIKELY(DataFileContent::EQUALITY_DELETES == content
                             && equality_ids.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("eq delete must have equality_deletes field", K(ret));
      }
      break;
    }
    case SORT_ORDER_ID_FIELD_ID: {
      // sort_order_id
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, sort_order_id))) {
        LOG_WARN("failed to get sort_order_id", K(ret));
      }
      break;
    }
    case REFERENCED_DATA_FILE_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           referenced_data_file))) {
        LOG_WARN("failed to get referenced_data_file", K(ret));
      }
      break;
    }
    case CONTENT_OFFSET_FIELD_ID: {
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, content_offset))) {
        LOG_WARN("failed to get content_offset", K(ret));
      }
      break;
    }
    case CONTENT_SIZE_IN_BYTES_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              content_size_in_bytes))) {
        LOG_WARN("failed to get content_size_in_bytes", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unreachable", K(ret));
    }
  }
  return ret;
}

int DataFile::decode_partitions_(const ManifestMetadata &manifest_metadata,
                                 const FieldProjection &field_projection,
                                 avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(avro::Type::AVRO_RECORD != field_projection.avro_node_->type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is illegal type", K(ret), K(field_projection.avro_node_->type()));
  } else if (OB_FAIL(DataFile::read_partition_values_from_avro(allocator_,
                                                               manifest_metadata,
                                                               field_projection,
                                                               decoder,
                                                               partition))) {
    LOG_WARN("fail to get partition values", K(ret));
  }
  return ret;
}

int DataFile::read_partition_values_from_avro(ObIAllocator &allocator,
                                              const ManifestMetadata &manifest_metadata,
                                              const FieldProjection &field_projection,
                                              avro::Decoder &decoder,
                                              ObIArray<ObObj> &partition_values)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(field_projection.children_.count()
                  != manifest_metadata.partition_spec.fields.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("PartitionField counts not matched",
             K(ret),
             K(field_projection.children_.count()),
             K(manifest_metadata.partition_spec.fields.count()));
  } else if (OB_FAIL(partition_values.reserve(manifest_metadata.partition_spec.fields.count()))) {
    LOG_WARN("fail to reserve values", K(ret));
  }

  // todo 将来需要考虑 PartitionField 和 Avro Data Schema 里面列顺序不一样的情况
  for (int64_t i = 0; OB_SUCC(ret) && i < field_projection.children_.count(); i++) {
    const FieldProjection &child_field_projection = field_projection.children_[i];
    const PartitionField *partition_field = NULL;
    const schema::ObColumnSchemaV2 *column_schema = NULL;
    if (OB_UNLIKELY(FieldProjection::Kind::Invalid == child_field_projection.kind_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid field projection", K(ret));
    } else if (FieldProjection::Kind::NOT_EXISTED == child_field_projection.kind_) {
      // skip
      if (OB_FAIL(AvroUtils::skip_decode(child_field_projection.avro_node_, decoder))) {
        LOG_WARN("fail to skip decode", K(ret));
      }
    } else if (OB_FAIL(manifest_metadata.partition_spec.get_partition_field_by_field_id(
                   child_field_projection.field_id_,
                   partition_field))) {
      LOG_WARN("fail to get partition field", K(ret));
    } else if (OB_FAIL(manifest_metadata.schema.get_column_schema_by_field_id(
                   partition_field->source_id,
                   column_schema))) {
      LOG_WARN("failed to get column_schema", K(ret), K(partition_field->source_id));
    } else {
      ObObj partition_value;
      if (OB_FAIL(read_partition_value_from_avro(allocator,
                                                 partition_field,
                                                 column_schema,
                                                 child_field_projection.avro_node_,
                                                 decoder,
                                                 partition_value))) {
        LOG_WARN("failed to get partition value", K(ret));
      } else if (OB_FAIL(partition_values.push_back(partition_value))) {
        LOG_WARN("failed to add partition value", K(ret));
      }
    }
  }
  return ret;
}

int DataFile::read_partition_value_from_avro(ObIAllocator &allocator,
                                             const PartitionField *partition_field,
                                             const schema::ObColumnSchemaV2 *column_schema,
                                             const avro::NodePtr &avro_node,
                                             avro::Decoder &decoder,
                                             ObObj &obj)
{
  int ret = OB_SUCCESS;
  const Transform &transform = partition_field->transform;
  const ObString &partition_column_name = partition_field->name;
  ObObjType result_type;
  if (OB_FAIL(transform.get_result_type(column_schema->get_data_type(), result_type))) {
    LOG_WARN("failed to get result_type", K(ret));
  } else {
    switch (result_type) {
      case ObTinyIntType: {
        std::optional<bool> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode bool", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_bool(value.value());
        }
        break;
      }
      case ObInt32Type: {
        std::optional<int32_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int32", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_int32(value.value());
        }
        break;
      }
      case ObIntType: {
        std::optional<int64_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int64", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_int(value.value());
        }
        break;
      }
      case ObFloatType: {
        std::optional<float> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode float", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_float(value.value());
        }
        break;
      }
      case ObDoubleType: {
        std::optional<double> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode double", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_double(value.value());
        }
        break;
      }
      case ObDecimalIntType: {
        std::optional<ObString> value;
        if (OB_FAIL(AvroUtils::decode_binary(allocator, avro_node, decoder, value))) {
          LOG_WARN("failed to decode fixed", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else if (column_schema->get_data_precision() <= 0
                   || column_schema->get_data_scale() <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid decimal type", K(ret));
        } else {
          int32_t buffer_size = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
              column_schema->get_data_precision());
          uint8_t *buf = static_cast<uint8_t *>(allocator.alloc(buffer_size));
          memset(buf, 0, buffer_size);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            const ObString &bytes = value.value();
            for (int32_t i = 0; i < bytes.length(); i++) {
              const uint8_t &tmp = bytes.ptr()[i];
              buf[buffer_size - 1 - i] = tmp;
            }
            ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
            obj.set_decimal_int(buffer_size, column_schema->get_data_scale(), decint);
          }
        }
        break;
      }
      case ObDateType: {
        std::optional<int32_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int32", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_date(value.value());
        }
        break;
      }
      case ObTimeType: {
        std::optional<int64_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int64", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_time(value.value());
        }
        break;
      }
      case ObDateTimeType: {
        // aka iceberg timestamp
        std::optional<int64_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int64", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_datetime(value.value());
        }
        break;
      }
      case ObTimestampType: {
        // aka iceberg timestamptz
        std::optional<int64_t> value;
        if (OB_FAIL(AvroUtils::decode_primitive(avro_node, decoder, value))) {
          LOG_WARN("failed to decode int64", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          obj.set_timestamp(value.value());
        }
        break;
      }
      case ObMediumTextType: {
        // iceberg binary/string
        ObArenaAllocator tmp_allocator;
        std::optional<ObString> value;
        if (OB_FAIL(AvroUtils::decode_binary(tmp_allocator, avro_node, decoder, value))) {
          LOG_WARN("failed to decode binary", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          ObString bytes = value.value();
          ObString lob_with_header;
          OZ(storage::ObLobManager::fill_lob_header(allocator, bytes, lob_with_header));
          OX(obj.set_string(ObObjType::ObMediumTextType, lob_with_header));
          OX(obj.set_has_lob_header());
          OX(obj.set_collation_type(column_schema->get_collation_type()));
        }
        break;
      }
      case ObVarcharType: {
        // fixed/uuid
        std::optional<ObString> value;
        if (OB_FAIL(AvroUtils::decode_binary(allocator, avro_node, decoder, value))) {
          LOG_WARN("failed to decode binary", K(ret));
        } else if (!value.has_value()) {
          obj.set_null();
        } else {
          ObString bytes = value.value();
          OX(obj.set_varbinary(bytes));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported column type", K(ret), K(result_type));
      }
    }
  }
  return ret;
}

int DataFile::get_read_expected_schema(ObIAllocator &allocator,
                                       const ManifestMetadata &manifest_metadata,
                                       StructType *&struct_type)
{
  int ret = OB_SUCCESS;

  SchemaField *partition_schema_field = NULL;
  StructType *partition_struct_type = NULL;
  // setup partition_spec's schema
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(partition_struct_type = OB_NEWx(StructType, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else if (OB_ISNULL(partition_schema_field = OB_NEWx(SchemaField,
                                                          &allocator,
                                                          PARTITION_FIELD_ID,
                                                          PARTITION,
                                                          partition_struct_type))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    }
    for (int32_t i = 0; OB_SUCC(ret) && i < manifest_metadata.partition_spec.fields.count(); i++) {
      const PartitionField *partition_field = manifest_metadata.partition_spec.fields[i];
      SchemaField *schema_field = NULL;
      // todo 因为现在 schema project 只是根据 field name
      // 映射，不考虑类型，所以这里所有分区列都直接用 StringType
      if (OB_ISNULL(schema_field = OB_NEWx(SchemaField,
                                           &allocator,
                                           partition_field->field_id,
                                           partition_field->name,
                                           StringType::get_instance(),
                                           true,
                                           ""))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_FAIL(partition_struct_type->add_schema_field(schema_field))) {
        LOG_WARN("failed to add schema field", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(struct_type = OB_NEWx(StructType,
                                        &allocator,
                                        {&OPTIONAL_CONTENT_FIELD,
                                         &FILE_PATH_FIELD,
                                         &FILE_FORMAT_FIELD,
                                         partition_schema_field,
                                         &RECORD_COUNT_FIELD,
                                         &FILE_SIZE_FIELD,
                                         &COLUMN_SIZES_FIELD,
                                         &VALUE_COUNTS_FIELD,
                                         &NULL_VALUE_COUNTS_FIELD,
                                         &NAN_VALUE_COUNTS_FIELD,
                                         &LOWER_BOUNDS_FIELD,
                                         &UPPER_BOUNDS_FIELD,
                                         &KEY_METADATA_FIELD,
                                         &SPLIT_OFFSETS_FIELD,
                                         &EQUALITY_IDS_FIELD,
                                         &SORT_ORDER_ID_FIELD,
                                         &REFERENCED_DATA_FILE_FIELD,
                                         &CONTENT_OFFSET_FIELD,
                                         &CONTENT_SIZE_IN_BYTES_FIELD}))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    }
  }
  return ret;
}

ManifestEntry::ManifestEntry(ObIAllocator &allocator)
    : SpecWithAllocator(allocator), data_file(allocator), partition_spec(allocator)
{
}

int ManifestEntry::apply(const ManifestMetadata &manifest_metadata)
{
  int ret = OB_SUCCESS;
  // 这里浅拷贝 PartitionSpec 就行了（PartitionSpec 的内存来自于 ManifestMetadata）
  if (OB_FAIL(partition_spec.shallow_assign(manifest_metadata.partition_spec))) {
    LOG_WARN("failed to shallow assign partition spec", K(ret));
  } else {
    partition_spec_id = manifest_metadata.partition_spec_id;
    // ManifestMetadata 里面的 PartitionSpec 解析出来时没有 spec_id
    // 的(v1的PartitionSpec标准)，所以这里需要重新 reassign() reassign partition_spec's spec_id to
    // correct value
    partition_spec.spec_id = partition_spec_id;

    if (FormatVersion::V1 == manifest_metadata.format_version) {
      // v1 里面下面这三个字段不存在，需要 reset 一下默认值
      data_file.content = DataFileContent::DATA;
      sequence_number = 0L;
      file_sequence_number = 0L;
    }
  }
  return ret;
}

int ManifestEntry::decode_field(const ManifestFile &parent_manifest_file,
                                const ManifestMetadata &manifest_metadata,
                                const FieldProjection &field_projection,
                                avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  switch (field_projection.field_id_) {
    case STATUS_FIELD_ID: {
      // status
      int32_t tmp_status = -1;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_, decoder, tmp_status))) {
        LOG_WARN("failed to get status", K(ret));
      } else {
        switch (tmp_status) {
          case 0:
            status = ManifestEntryStatus::EXISTING;
            break;
          case 1:
            status = ManifestEntryStatus::ADDED;
            break;
          case 2:
            status = ManifestEntryStatus::DELETED;
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid status", K(ret), K(tmp_status));
        }
      }
      break;
    }
    case SNAPSHOT_ID_FIELD_ID: {
      // snapshot-id
      std::optional<int64_t> tmp_snapshot_id;
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, tmp_snapshot_id))) {
        LOG_WARN("failed to get snapshot-id", K(ret));
      } else if (tmp_snapshot_id.has_value()) {
        snapshot_id = tmp_snapshot_id.value();
      } else {
        // Inherited when null.
        snapshot_id = parent_manifest_file.added_snapshot_id;
      }
      break;
    }
    case SEQUENCE_NUMBER_FIELD_ID: {
      // sequence_number
      // in v1 tables, the data sequence number is not persisted and can be safely defaulted to 0
      // in v2 tables, the data sequence number should be inherited iff the entry status is ADDED
      std::optional<int64_t> tmp_data_sequence_number;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_data_sequence_number))) {
        LOG_WARN("failed to get sequence_number", K(ret));
      } else if (tmp_data_sequence_number.has_value()) {
        sequence_number = tmp_data_sequence_number.value();
      } else if (!tmp_data_sequence_number.has_value()
                 && (0 == parent_manifest_file.sequence_number
                     || ManifestEntryStatus::ADDED == status)) {
        // 对于 v1 表，parent_manifest_file.sequence_number 就已经是 0 了，所以直接用即可。
        // 如果 status 不是 ADDED 的，那么 data/file sequence number 必须显示指定
        sequence_number = parent_manifest_file.sequence_number;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sequence number", K(ret), K(status));
      }
      break;
    }
    case FILE_SEQUENCE_NUMBER_FIELD_ID: {
      // file_sequence_number
      // in v1 tables, the file sequence number is not persisted and can be safely defaulted to 0
      // in v2 tables, the file sequence number should be inherited iff the entry status is ADDED
      std::optional<int64_t> tmp_file_sequence_number;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_file_sequence_number))) {
        LOG_WARN("failed to get file_sequence_number", K(ret));
      } else if (tmp_file_sequence_number.has_value()) {
        file_sequence_number = tmp_file_sequence_number.value();
      } else if (!tmp_file_sequence_number.has_value()
                 && (0 == parent_manifest_file.sequence_number
                     || ManifestEntryStatus::ADDED == status)) {
        // 对于 v1 表，parent_manifest_file.sequence_number 就已经是 0 了，所以直接用即可。
        // 如果 status 不是 ADDED 的，那么 data/file sequence number 必须显示指定
        file_sequence_number = parent_manifest_file.sequence_number;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid file sequence number", K(ret), K(status));
      }
      break;
    }
    case DATA_FILE_FIELD_ID: {
      // data_file
      if (OB_FAIL(decode_data_file_(manifest_metadata, field_projection, decoder))) {
        LOG_WARN("failed to get data_file", K(ret));
      }
      break;
    }
    default: {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported field id", K(ret), K(field_projection.field_id_));
    }
  }
  return ret;
}

bool ManifestEntry::is_alive() const
{
  return ManifestEntryStatus::EXISTING == status || ManifestEntryStatus::ADDED == status;
}

bool ManifestEntry::is_data_file() const
{
  return DataFileContent::DATA == data_file.content;
}

bool ManifestEntry::is_position_delete_file() const
{
  return DataFileContent::POSITION_DELETES == data_file.content;
}

bool ManifestEntry::is_deletion_vector_file() const
{
  return DataFileContent::POSITION_DELETES == data_file.content
         && DataFileFormat::PUFFIN == data_file.file_format;
}

bool ManifestEntry::is_equality_delete_file() const
{
  return DataFileContent::EQUALITY_DELETES == data_file.content;
}

bool ManifestEntry::is_delete_file() const
{
  return is_position_delete_file() || is_equality_delete_file();
}

int ManifestEntry::decode_data_file_(const ManifestMetadata &manifest_metadata,
                                     const FieldProjection &field_projection,
                                     avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < field_projection.children_.count(); idx++) {
    const FieldProjection &child_field_projection = field_projection.children_[idx];
    if (OB_UNLIKELY(FieldProjection::Kind::Invalid == child_field_projection.kind_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid child field projection", K(ret));
    } else if (FieldProjection::Kind::NOT_EXISTED == child_field_projection.kind_) {
      if (OB_FAIL(AvroUtils::skip_decode(child_field_projection.avro_node_, decoder))) {
        LOG_WARN("failed to skip decode", K(ret));
      }
    } else if (OB_FAIL(
                   data_file.decode_field(manifest_metadata, child_field_projection, decoder))) {
      LOG_WARN("failed to set data_file", K(ret));
    }
  }

  return ret;
}

int ManifestEntry::get_read_expected_schema(ObIAllocator &allocator,
                                            const ManifestMetadata &manifest_metadata,
                                            StructType *&struct_type)
{
  int ret = OB_SUCCESS;
  SchemaField *datafile_schema_field = NULL;
  StructType *datafile_struct_type = NULL;
  if (OB_FAIL(
          DataFile::get_read_expected_schema(allocator, manifest_metadata, datafile_struct_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get datafile schema", K(ret));
  } else if (OB_ISNULL(datafile_schema_field = OB_NEWx(SchemaField,
                                                       &allocator,
                                                       DATA_FILE_FIELD_ID,
                                                       DATA_FILE,
                                                       datafile_struct_type))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_ISNULL(struct_type = OB_NEWx(StructType,
                                             &allocator,
                                             {&STATUS_FIELD,
                                              &SNAPSHOT_ID_FIELD,
                                              &SEQUENCE_NUMBER_FIELD,
                                              &FILE_SEQUENCE_NUMBER_FIELD,
                                              datafile_schema_field}))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  }
  return ret;
}

} // namespace iceberg

} // namespace sql

} // namespace oceanbase

namespace avro
{
using namespace oceanbase::sql::iceberg;

void codec_traits<ManifestEntryDatum>::encode(Encoder &e, const ManifestEntryDatum &v)
{
  throw avro::Exception("unsupported encoded");
}

void codec_traits<ManifestEntryDatum>::decode(Decoder &d, ManifestEntryDatum &v)
{
  int ret = OB_SUCCESS;
  // reset previous value
  v.manifest_entry_ = NULL;

  const ObFixedArray<FieldProjection, ObIAllocator> &field_projections
      = v.schema_projection_.fields_;

  if (OB_ISNULL(v.manifest_entry_ = OB_NEWx(ManifestEntry, &v.allocator_, v.allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_projections.count(); i++) {
    const FieldProjection &field_projection = field_projections[i];
    if (OB_UNLIKELY(FieldProjection::Kind::Invalid == field_projection.kind_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid field projection", K(ret));
    } else if (FieldProjection::Kind::NOT_EXISTED == field_projection.kind_) {
      if (OB_FAIL(AvroUtils::skip_decode(field_projection.avro_node_, d))) {
        LOG_WARN("failed to skip decode", K(ret));
      }
    } else if (OB_FAIL(v.manifest_entry_->decode_field(v.parent_manifest_file_,
                                                       v.manifest_metadata_,
                                                       field_projection,
                                                       d))) {
      LOG_WARN("failed to set manifest entry", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(v.manifest_entry_->apply(v.manifest_metadata_))) {
      LOG_WARN("failed to apply manifest metadata", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to decode manifest entry", K(ret));
    throw avro::Exception("failed to decode manifest entry");
  }
}

} // namespace avro
