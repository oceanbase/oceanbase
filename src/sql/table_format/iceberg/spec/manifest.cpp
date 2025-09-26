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
#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

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
      if (format_version_num < 1 || format_version_num > 2) {
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
    : SpecWithAllocator(allocator),
      partition(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      column_sizes(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      value_counts(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      null_value_counts(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      nan_value_counts(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      lower_bounds(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      upper_bounds(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      split_offsets(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      equality_ids(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_))
{
}

int DataFile::init_from_avro(const ManifestMetadata &manifest_metadata,
                             const avro::GenericRecord &avro_data_file)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    std::optional<int32_t> tmp_data_file_content;
    if (OB_FAIL(
            ObCatalogAvroUtils::get_primitive(avro_data_file, CONTENT, tmp_data_file_content))) {
      LOG_WARN("failed to get content", K(ret));
    } else if (!tmp_data_file_content.has_value()) {
      // all v1 files are data files
      content = DataFileContent::DATA;
    } else if (tmp_data_file_content.value() < static_cast<int32_t>(DataFileContent::DATA)
               || tmp_data_file_content.value() > static_cast<int32_t>(DataFileContent::EQUALITY_DELETES)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid data file content", K(ret));
    } else {
      content = static_cast<DataFileContent>(tmp_data_file_content.value());
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_STRING>(allocator_,
                                                                        avro_data_file,
                                                                        FILE_PATH,
                                                                        file_path))) {
      LOG_WARN("fail to get file_path", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObString tmp_file_format;
    ObArenaAllocator tmp_allocator;
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_STRING>(tmp_allocator,
                                                                        avro_data_file,
                                                                        FILE_FORMAT,
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
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_partitions_(manifest_metadata, avro_data_file))) {
      LOG_WARN("fail to get partition", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_data_file, RECORD_COUNT, record_count))) {
      LOG_WARN("fail to get record_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_data_file,
                                                  FILE_SIZE_IN_BYTES,
                                                  file_size_in_bytes))) {
      LOG_WARN("failed to get file_size_in_bytes", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            ObCatalogAvroUtils::get_primitive_map(avro_data_file, COLUMN_SIZES, column_sizes))) {
      LOG_WARN("fail to get column_sizes", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            ObCatalogAvroUtils::get_primitive_map(avro_data_file, VALUE_COUNTS, value_counts))) {
      LOG_WARN("failed to get value_counts", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive_map(avro_data_file,
                                                      NULL_VALUE_COUNTS,
                                                      null_value_counts))) {
      LOG_WARN("failed to get null_value_counts", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive_map(avro_data_file,
                                                      NAN_VALUE_COUNTS,
                                                      nan_value_counts))) {
      LOG_WARN("failed to get nan_value_counts", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary_map<avro::Type::AVRO_BYTES>(allocator_,
                                                                           avro_data_file,
                                                                           LOWER_BOUNDS,
                                                                           lower_bounds))) {
      LOG_WARN("failed to get lower_bounds", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary_map<avro::Type::AVRO_BYTES>(allocator_,
                                                                           avro_data_file,
                                                                           UPPER_BOUNDS,
                                                                           upper_bounds))) {
      LOG_WARN("failed to get upper_bounds", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_BYTES>(allocator_,
                                                                       avro_data_file,
                                                                       KEY_METADATA,
                                                                       key_metadata))) {
      LOG_WARN("failed to get key_metadata", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive_array(avro_data_file,
                                                        SPLIT_OFFSETS,
                                                        split_offsets))) {
      LOG_WARN("failed to get split_offsets", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            ObCatalogAvroUtils::get_primitive_array(avro_data_file, EQUALITY_IDS, equality_ids))) {
      LOG_WARN("failed to get equality_ids", K(ret));
    } else if (DataFileContent::EQUALITY_DELETES == content && equality_ids.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("eq delete must have equality_deletes field", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_data_file, SORT_ORDER_ID, sort_order_id))) {
      LOG_WARN("failed to get sort_order_id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_STRING>(allocator_,
                                                                        avro_data_file,
                                                                        REFERENCED_DATA_FILE,
                                                                        referenced_data_file))) {
      LOG_WARN("failed to get referenced_data_file", K(ret));
    }
  }

  return ret;
}

int DataFile::get_partitions_(const ManifestMetadata &manifest_metadata,
                              const avro::GenericRecord &avro_data_file)
{
  int ret = OB_SUCCESS;
  const avro::GenericDatum *partition_generic_datum = NULL;
  if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_RECORD>(avro_data_file,
                                                                     PARTITION,
                                                                     partition_generic_datum))) {
    LOG_WARN("failed to get avro partition", K(ret));
  } else if (OB_ISNULL(partition_generic_datum)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is null", K(ret));
  } else if (avro::Type::AVRO_RECORD != partition_generic_datum->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition is illegal type", K(ret), K(partition_generic_datum->type()));
  } else if (OB_FAIL(DataFile::read_partition_values_from_avro(
                 allocator_,
                 manifest_metadata,
                 partition_generic_datum->value<avro::GenericRecord>(),
                 partition))) {
    LOG_WARN("fail to get partition values", K(ret));
  }
  return ret;
}

int DataFile::read_partition_values_from_avro(ObIAllocator &allocator,
                                              const ManifestMetadata &manifest_metadata,
                                              const avro::GenericRecord &avro_record_partition,
                                              ObIArray<ObObj> &partition_values)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; i < manifest_metadata.partition_spec.fields.size(); i++) {
    const PartitionField *partition_field = manifest_metadata.partition_spec.fields.at(i);
    const schema::ObColumnSchemaV2 *column_schema = NULL;
    avro::GenericDatum avro_value;
    ObObj partition_value;
    if (OB_FAIL(manifest_metadata.schema.get_column_schema_by_field_id(partition_field->source_id,
                                                                       column_schema))) {
      LOG_WARN("failed to get column_schema", K(ret), K(partition_field->source_id));
    } else if (OB_FAIL(read_partition_value_from_avro(allocator,
                                                      partition_field,
                                                      column_schema,
                                                      avro_record_partition,
                                                      partition_value))) {
      LOG_WARN("failed to get partition value", K(ret));
    } else if (OB_FAIL(partition_values.push_back(partition_value))) {
      LOG_WARN("failed to push_back partition_value", K(ret), K(partition_value));
    }
  }
  return ret;
}

int DataFile::read_partition_value_from_avro(ObIAllocator &allocator,
                                             const PartitionField *partition_field,
                                             const schema::ObColumnSchemaV2 *column_schema,
                                             const avro::GenericRecord &avro_record_partition,
                                             ObObj &obj)
{
  int ret = OB_SUCCESS;
  const Transform &transform = partition_field->transform;
  const ObString &partition_column_name = partition_field->name;
  const avro::GenericDatum *partition_avro_datum = NULL;
  ObObjType result_type;
  if (OB_FAIL(transform.get_result_type(column_schema->get_data_type(), result_type))) {
    LOG_WARN("failed to get result_type", K(ret));
  } else {
    switch (result_type) {
      case ObTinyIntType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_BOOL>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_bool(partition_avro_datum->value<bool>());
        }
        break;
      }
      case ObInt32Type: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_INT>(avro_record_partition,
                                                                        partition_column_name,
                                                                        partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_int32(partition_avro_datum->value<int32_t>());
        }
        break;
      }
      case ObIntType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_LONG>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_int(partition_avro_datum->value<int64_t>());
        }
        break;
      }
      case ObFloatType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_FLOAT>(avro_record_partition,
                                                                          partition_column_name,
                                                                          partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_float(partition_avro_datum->value<float>());
        }
        break;
      }
      case ObDoubleType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_DOUBLE>(avro_record_partition,
                                                                           partition_column_name,
                                                                           partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_double(partition_avro_datum->value<double>());
        }
        break;
      }
      case ObDecimalIntType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_FIXED>(avro_record_partition,
                                                                          partition_column_name,
                                                                          partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else if (column_schema->get_data_precision() <= 0
                   || column_schema->get_data_scale() <= 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid decimal type", K(ret));
        } else {
          const avro::GenericFixed &fixed_datum = partition_avro_datum->value<avro::GenericFixed>();
          int32_t buffer_size
              = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(column_schema->get_data_precision());
          uint8_t *buf = static_cast<uint8_t *>(allocator.alloc(buffer_size));
          memset(buf, 0, buffer_size);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          } else {
            for (int32_t i = 0; i < fixed_datum.value().size(); i++) {
              const uint8_t &tmp = fixed_datum.value()[i];
              buf[buffer_size - 1 - i] = tmp;
            }
            ObDecimalInt *decint = reinterpret_cast<ObDecimalInt *>(buf);
            obj.set_decimal_int(buffer_size, column_schema->get_data_scale(), decint);
          }
        }
        break;
      }
      case ObDateType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_INT>(avro_record_partition,
                                                                        partition_column_name,
                                                                        partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_date(partition_avro_datum->value<int32_t>());
        }
        break;
      }
      case ObTimeType: {
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_LONG>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_time(partition_avro_datum->value<int64_t>());
        }
        break;
      }
      case ObDateTimeType: {
        // aka iceberg timestamp
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_LONG>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_datetime(partition_avro_datum->value<int64_t>());
        }
        break;
      }
      case ObTimestampType: {
        // aka iceberg timestamptz
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_LONG>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          obj.set_timestamp(partition_avro_datum->value<int64_t>());
        }
        break;
      }
      case ObMediumTextType: {
        if (ObCollationType::CS_TYPE_BINARY == column_schema->get_collation_type()) {
          // iceberg binary type
          ObString deep_copy_str;
          ObString lob_with_header;
          if (OB_FAIL(
                  ObCatalogAvroUtils::get_value<avro::Type::AVRO_BYTES>(avro_record_partition,
                                                                        partition_column_name,
                                                                        partition_avro_datum))) {
            LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
          } else if (NULL == partition_avro_datum
                     || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
            obj.set_null();
          } else {
            std::vector<uint8_t> bytes = partition_avro_datum->value<std::vector<uint8_t>>();
            OZ(ob_write_string(allocator,
                               ObString(bytes.size(), reinterpret_cast<const char *>(bytes.data())),
                               deep_copy_str));
            OZ(storage::ObLobManager::fill_lob_header(allocator, deep_copy_str, lob_with_header));
            OX(obj.set_string(ObObjType::ObMediumTextType, lob_with_header));
            OX(obj.set_has_lob_header());
            OX(obj.set_collation_type(ObCollationType::CS_TYPE_BINARY));
          }
        } else {
          // iceberg string type
          ObString deep_copy_str;
          ObString lob_with_header;
          if (OB_FAIL(
                  ObCatalogAvroUtils::get_value<avro::Type::AVRO_STRING>(avro_record_partition,
                                                                         partition_column_name,
                                                                         partition_avro_datum))) {
            LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
          } else if (NULL == partition_avro_datum
                     || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
            obj.set_null();
          } else {
            std::string str = partition_avro_datum->value<std::string>();
            ObString deep_copy_str;
            OZ(ob_write_string(allocator, ObString(str.size(), str.data()), deep_copy_str));
            OZ(storage::ObLobManager::fill_lob_header(allocator, deep_copy_str, lob_with_header));
            OX(obj.set_string(ObObjType::ObMediumTextType, lob_with_header));
            OX(obj.set_has_lob_header());
            OX(obj.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_BIN));
          }
        }
        break;
      }
      case ObVarcharType: {
        // fixed/uuid
        if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_BYTES>(avro_record_partition,
                                                                          partition_column_name,
                                                                          partition_avro_datum))) {
          LOG_WARN("failed to get partition avro datum", K(ret), K(partition_column_name));
        } else if (NULL == partition_avro_datum
                   || avro::Type::AVRO_NULL == partition_avro_datum->type()) {
          obj.set_null();
        } else {
          std::vector<uint8_t> bytes = partition_avro_datum->value<std::vector<uint8_t>>();
          ObString deep_copy_str;
          OZ(ob_write_string(allocator,
                             ObString(bytes.size(), reinterpret_cast<const char *>(bytes.data())),
                             deep_copy_str));
          OX(obj.set_varbinary(deep_copy_str));
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

ManifestEntry::ManifestEntry(ObIAllocator &allocator)
    : SpecWithAllocator(allocator), data_file(allocator), partition_spec(allocator)
{
}

int ManifestEntry::init_from_avro(const ManifestFile &parent_manifest_file,
                                  const ManifestMetadata &manifest_metadata,
                                  const avro::GenericRecord &avro_manifest_entry)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    int32_t tmp_status = -1;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_entry, STATUS, tmp_status))) {
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
          LOG_WARN("invalid status", K(tmp_status));
      }
    }
  }

  if (OB_SUCC(ret)) {
    std::optional<int64_t> tmp_snapshot_id;
    if (OB_FAIL(
            ObCatalogAvroUtils::get_primitive(avro_manifest_entry, SNAPSHOT_ID, tmp_snapshot_id))) {
      LOG_WARN("failed to get snapshot_id", K(ret));
    } else if (tmp_snapshot_id.has_value()) {
      snapshot_id = tmp_snapshot_id.value();
    } else {
      // Inherited when null.
      snapshot_id = parent_manifest_file.added_snapshot_id;
    }
  }

  if (OB_SUCC(ret)) {
    // in v1 tables, the data sequence number is not persisted and can be safely defaulted to 0
    // in v2 tables, the data sequence number should be inherited iff the entry status is ADDED
    std::optional<int64_t> tmp_data_sequence_number;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_entry,
                                                  SEQUENCE_NUMBER,
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
  }

  if (OB_SUCC(ret)) {
    // in v1 tables, the file sequence number is not persisted and can be safely defaulted to 0
    // in v2 tables, the file sequence number should be inherited iff the entry status is ADDED
    std::optional<int64_t> tmp_file_sequence_number;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_entry,
                                                  FILE_SEQUENCE_NUMBER,
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
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_data_file_(manifest_metadata, avro_manifest_entry))) {
      LOG_WARN("failed to get data_file", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    partition_spec_id = manifest_metadata.partition_spec_id;
    partition_spec.assign(manifest_metadata.partition_spec);
    // reassign partition_spec's spec_id to correct value
    partition_spec.spec_id = partition_spec_id;
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

int ManifestEntry::get_data_file_(const ManifestMetadata &manifest_metadata,
                                  const avro::GenericRecord &avro_manifest_entry)
{
  int ret = OB_SUCCESS;
  const avro::GenericDatum *field_datum = NULL;
  if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_RECORD>(avro_manifest_entry,
                                                                     DATA_FILE,
                                                                     field_datum))) {
    LOG_WARN("failed to get data_file", K(ret));
  } else if (OB_ISNULL(field_datum) ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null data_file", K(ret));
  } else if (avro::Type::AVRO_RECORD != field_datum->type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_file is illegal type", K(ret), K(field_datum->type()));
  } else if (OB_FAIL(data_file.init_from_avro(manifest_metadata,
                                              field_datum->value<avro::GenericRecord>()))) {
    LOG_WARN("failed to init data_file", K(ret));
  }

  return ret;
}

} // namespace iceberg

} // namespace sql

} // namespace oceanbase
