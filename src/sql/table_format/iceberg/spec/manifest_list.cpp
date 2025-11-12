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

#include "sql/table_format/iceberg/spec/manifest_list.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/avro_schema_util.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/schema.h"

#include <avro/DataFile.hh>

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

PartitionFieldSummary::PartitionFieldSummary(ObIAllocator &allocator) : SpecWithAllocator(allocator)
{
}

int PartitionFieldSummary::decode_field(const FieldProjection &field_projection,
                                        avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  switch (field_projection.field_id_) {
    case CONTAINS_NULL_FIELD_ID: {
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, contains_null))) {
        LOG_WARN("fail to get contains_null", K(ret));
      }
      break;
    }
    case CONTAINS_NAN_FIELD_ID: {
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, contains_nan))) {
        LOG_WARN("fail to get contains_nan", K(ret));
      }
      break;
    }
    case LOWER_BOUND_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           lower_bound))) {
        LOG_WARN("failed to get lower_bound", K(ret));
      }
      break;
    }
    case UPPER_BOUND_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           upper_bound))) {
        LOG_WARN("failed to get upper_bound", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported field", K(ret), K(field_projection.field_id_));
    }
  }
  return ret;
}

ManifestFile::ManifestFile(ObIAllocator &allocator)
    : SpecWithAllocator(allocator), partitions(allocator_), cached_manifest_entries_(allocator_)
{
}

int ManifestFile::decode_field(const FieldProjection &field_projection, avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  switch (field_projection.field_id_) {
    case MANIFEST_PATH_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           manifest_path))) {
        LOG_WARN("failed to get manifest_path", K(ret));
      }
      break;
    }
    case MANIFEST_LENGTH_FIELD_ID: {
      if (OB_FAIL(
              AvroUtils::decode_primitive(field_projection.avro_node_, decoder, manifest_length))) {
        LOG_WARN("failed to get manifest_length", K(ret));
      }
      break;
    }
    case PARTITION_SPEC_ID_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              partition_spec_id))) {
        LOG_WARN("failed to get partition_spec_id", K(ret));
      }
      break;
    }
    case CONTENT_FIELD_ID: {
      std::optional<int32_t> tmp_content;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_, decoder, tmp_content))) {
        LOG_WARN("failed to get content", K(ret));
      } else if (!tmp_content.has_value()) {
        // DATA for all v1 manifests
        content = ManifestContent::DATA;
      } else {
        switch (tmp_content.value()) {
          case 0:
            content = ManifestContent::DATA;
            break;
          case 1:
            content = ManifestContent::DELETES;
            break;
          default:
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid content type", K(ret));
        }
      }
      break;
    }
    case SEQUENCE_NUMBER_FIELD_ID: {
      std::optional<int64_t> tmp_sequence_number;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_sequence_number))) {
        LOG_WARN("failed to get sequence_number", K(ret));
      } else if (!tmp_sequence_number.has_value()) {
        // use 0 when reading v1 manifest lists
        sequence_number = 0L;
      } else {
        sequence_number = tmp_sequence_number.value();
      }
      break;
    }
    case MIN_SEQUENCE_NUMBER_FIELD_ID: {
      std::optional<int64_t> tmp_min_sequence_number;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_min_sequence_number))) {
        LOG_WARN("failed to get min_sequence_number", K(ret));
      } else if (!tmp_min_sequence_number.has_value()) {
        // use 0 when reading v1 manifest lists
        min_sequence_number = 0L;
      } else {
        min_sequence_number = tmp_min_sequence_number.value();
      }
      break;
    }
    case ADDED_SNAPSHOT_ID_FIELD_ID: {
      // 虽然 v1/v2/v3 标准里面这个字段是 required 的，但是在 v1 里面存在一种情况，
      // 即字段类型是 AVRO_UNION，而不是直接是 AVRO_INT，当然 AVRO_UNION 其是一定有值的(不会是 AVRO_NULL)
      // 所以我们要按照 AVRO_UNION 类型去 decode 字段，且要检查其必须是含有有效值。
      std::optional<int64_t> tmp_added_snapshot_id;
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              tmp_added_snapshot_id))) {
        LOG_WARN("failed to get added_snapshot_id", K(ret));
      } else if (!tmp_added_snapshot_id.has_value()) {
        LOG_WARN("added_snapshot_id must existed", K(ret));
      } else {
        added_snapshot_id = tmp_added_snapshot_id.value();
      }
      break;
    }
    case ADDED_FILES_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              added_files_count))) {
        LOG_WARN("failed to get added_files_count", K(ret));
      }
      break;
    }
    case EXISTING_FILES_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              existing_files_count))) {
        LOG_WARN("failed to get existing_files_count", K(ret));
      }
      break;
    }
    case DELETED_FILES_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              deleted_files_count))) {
        LOG_WARN("failed to get deleted_files_count", K(ret));
      }
      break;
    }
    case ADDED_ROWS_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              added_rows_count))) {
        LOG_WARN("failed to get added_rows_count", K(ret));
      }
      break;
    }
    case EXISTING_ROWS_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              existing_rows_count))) {
        LOG_WARN("failed to get existing_rows_count", K(ret));
      }
      break;
    }
    case DELETED_ROWS_COUNT_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_primitive(field_projection.avro_node_,
                                              decoder,
                                              deleted_rows_count))) {
        LOG_WARN("failed to get deleted_rows_count", K(ret));
      }
      break;
    }
    case PARTITIONS_FIELD_ID: {
      if (OB_FAIL(decode_partitions_(field_projection, decoder))) {
        LOG_WARN("failed to get partitions", K(ret));
      }
      break;
    }
    case KEY_METADATA_FIELD_ID: {
      if (OB_FAIL(AvroUtils::decode_binary(allocator_,
                                           field_projection.avro_node_,
                                           decoder,
                                           key_metadata))) {
        LOG_WARN("failed to get key_metadata", K(ret));
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unsupported field", K(ret), K(field_projection.field_id_));
  }
  return ret;
}

int ManifestFile::decode_partitions_(const FieldProjection &field_projection,
                                     avro::Decoder &decoder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(FieldProjection::Kind::Projected != field_projection.kind_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid field projection", K(ret));
  } else if (OB_UNLIKELY(1 != field_projection.children_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must has only one element if partition existed", K(ret));
  } else {
    avro::NodePtr target_avro_node = NULL;
    if (avro::Type::AVRO_UNION == field_projection.avro_node_->type()) {
      size_t union_index = decoder.decodeUnionIndex();
      if (OB_UNLIKELY(union_index >= field_projection.avro_node_->leaves())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid union index",
                 K(ret),
                 K(union_index),
                 K(field_projection.avro_node_->leaves()));
      } else if (avro::Type::AVRO_NULL
                 == field_projection.avro_node_->leafAt(union_index)->type()) {
        decoder.decodeNull();
      } else {
        target_avro_node = field_projection.avro_node_->leafAt(union_index);
      }
    } else {
      target_avro_node = field_projection.avro_node_;
    }

    if (NULL != target_avro_node) {
      // 不是 optional，开始解析 array
      const FieldProjection &element_record_projection = *field_projection.children_[0];
      ObArray<PartitionFieldSummary *> tmp_partitions;
      if (OB_UNLIKELY(avro::AVRO_ARRAY != target_avro_node->type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid avro node", K(ret), K(target_avro_node->type()));
      } else if (OB_UNLIKELY(avro::AVRO_RECORD != element_record_projection.avro_node_->type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid avro node", K(ret), K(element_record_projection.avro_node_->type()));
      }

      for (size_t n = decoder.arrayStart(); OB_SUCC(ret) && n != 0; n = decoder.arrayNext()) {
        for (size_t i = 0; OB_SUCC(ret) && i < n; ++i) {
          PartitionFieldSummary *tmp_summary = NULL;
          if (OB_ISNULL(tmp_summary = OB_NEWx(PartitionFieldSummary, &allocator_, allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate failed", K(ret));
          }

          for (int64_t field_idx = 0;
               OB_SUCC(ret) && field_idx < element_record_projection.children_.count();
               field_idx++) {
            const FieldProjection &element_record_field_projection
                = *element_record_projection.children_[field_idx];
            if (OB_FAIL(tmp_summary->decode_field(element_record_field_projection, decoder))) {
              LOG_WARN("failed to decode PartitionFieldSummary field", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            tmp_partitions.push_back(tmp_summary);
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(partitions.assign(tmp_partitions))) {
          LOG_WARN("failed to assign partitions", K(ret));
        }
      }
    }
  }

  return ret;
}

int ManifestFile::get_manifest_entries(const ObString &access_info,
                                       ObIArray<ManifestEntry *> &manifest_entries)
{
  int ret = OB_SUCCESS;
  if (cached_manifest_entries_.empty()) {
    if (OB_FAIL(get_manifest_entries_(access_info, cached_manifest_entries_))) {
      LOG_WARN("failed to get manifest entries", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // 这里不能用 assign，因为 manifest_entries 指所有的 manifest file 的 manifest entries
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cached_manifest_entries_.count(); ++idx) {
      if (OB_FAIL(manifest_entries.push_back(cached_manifest_entries_[idx]))) {
        LOG_WARN("failed to add manifest entry", K(ret));
      }
    }
  }
  return ret;
}

int ManifestFile::get_read_expected_schema(StructType *&struct_type)
{
  int ret = OB_SUCCESS;
  static StructType READ_MANIFEST_FILE_TYPE = StructType({&MANIFEST_PATH_FIELD,
                                                          &MANIFEST_LENGTH_FIELD,
                                                          &PARTITION_SPEC_ID_FIELD,
                                                          &OPTIONAL_CONTENT_FIELD,
                                                          &OPTIONAL_SEQUENCE_NUMBER_FIELD,
                                                          &OPTIONAL_MIN_SEQUENCE_NUMBER_FIELD,
                                                          &ADDED_SNAPSHOT_ID_FIELD,
                                                          &OPTIONAL_ADDED_FILES_COUNT_FIELD,
                                                          &OPTIONAL_EXISTING_FILES_COUNT_FIELD,
                                                          &OPTIONAL_DELETED_FILES_COUNT_FIELD,
                                                          &OPTIONAL_ADDED_ROWS_COUNT_FIELD,
                                                          &OPTIONAL_EXISTING_ROWS_COUNT_FIELD,
                                                          &OPTIONAL_DELETED_ROWS_COUNT_FIELD,
                                                          &PARTITIONS_FIELD,
                                                          &KEY_METADATA_FIELD});
  struct_type = &READ_MANIFEST_FILE_TYPE;
  return ret;
}

int ManifestFile::get_manifest_entries_(const ObString &access_info,
                                        ObIArray<ManifestEntry *> &manifest_entries) const
{
  int ret = OB_SUCCESS;
  ObArray<ManifestEntry *> tmp_manifest_entries;
  ObArenaAllocator tmp_allocator;
  char *buf = NULL;
  int64_t read_size = 0;
  std::unique_ptr<avro::InputStream> input_stream;
  if (OB_FAIL(
          ObIcebergFileIOUtils::read(tmp_allocator, manifest_path, access_info, buf, read_size))) {
    LOG_WARN("fail to read manifest", K(ret));
  } else if (OB_FALSE_IT(input_stream
                         = avro::memoryInputStream(reinterpret_cast<uint8_t *>(buf), read_size))) {
  } else {
    try {
      std::unique_ptr<avro::DataFileReaderBase> avro_reader_base
          = std::make_unique<avro::DataFileReaderBase>(std::move(input_stream));
      const std::map<std::string, std::vector<uint8_t>> &metadata = avro_reader_base->metadata();
      StructType *expected_avro_schema = NULL;
      SchemaProjection schema_projection(tmp_allocator);
      ManifestMetadata *manifest_metadata
          = NULL; // 为所有 manifest entry 共享，所以需要从 allocator_ 里面创建
      if (OB_ISNULL(manifest_metadata = OB_NEWx(ManifestMetadata, &allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate manifest metadata", K(ret));
      } else if (OB_FAIL(manifest_metadata->init_from_metadata(metadata))) {
        LOG_WARN("failed to init manifest metadata", K(ret));
      } else if (OB_FAIL(ManifestEntry::get_read_expected_schema(tmp_allocator,
                                                                 *manifest_metadata,
                                                                 expected_avro_schema))) {
        LOG_WARN("failed to get expected schema", K(ret));
      } else if (OB_FAIL(AvroSchemaProjectionUtils::project(tmp_allocator,
                                                            *expected_avro_schema,
                                                            avro_reader_base->dataSchema().root(),
                                                            schema_projection))) {
        LOG_WARN("failed to project avro schema", K(ret));
      } else {
        avro::DataFileReader<ManifestEntryDatum> avro_reader(std::move(avro_reader_base));
        ManifestEntryDatum manifest_entry_datum(allocator_,
                                                schema_projection,
                                                *this,
                                                *manifest_metadata);
        while (OB_SUCC(ret) && avro_reader.read(manifest_entry_datum)) {
          if (OB_ISNULL(manifest_entry_datum.manifest_entry_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to read manifest entry", K(ret));
          } else if (OB_FAIL(tmp_manifest_entries.push_back(manifest_entry_datum.manifest_entry_))) {
            LOG_WARN("failed to add manifest entry", K(ret));
          }
        }
        avro_reader.close();
      }
    } catch (std::exception &e) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to read manifest", K(ret), K(e.what()));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(manifest_entries.assign(tmp_manifest_entries))) {
        LOG_WARN("failed to assign manifest entries", K(ret));
      }
    }
  }
  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

namespace avro
{
using namespace oceanbase::common;
using namespace oceanbase::sql::iceberg;

void codec_traits<ManifestFileDatum>::encode(Encoder &e, const ManifestFileDatum &v)
{
  throw avro::Exception("unimplemented");
}

void codec_traits<ManifestFileDatum>::decode(Decoder &d, ManifestFileDatum &v)
{
  int ret = OB_SUCCESS;
  v.manifest_file_ = NULL;

  const ObFixedArray<FieldProjection *, ObIAllocator> &field_projections
      = v.schema_projection_.fields_;

  if (OB_ISNULL(v.manifest_file_ = OB_NEWx(ManifestFile, &v.allocator_, v.allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_projections.count(); i++) {
    const FieldProjection *field_projection = field_projections[i];
    if (OB_ISNULL(field_projection) || FieldProjection::Kind::Invalid == field_projection->kind_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid field projection", K(ret));
    } else if (FieldProjection::Kind::NOT_EXISTED == field_projection->kind_) {
      if (OB_FAIL(AvroUtils::skip_decode(field_projection->avro_node_, d))) {
        LOG_WARN("failed to skip decode", K(ret));
      }
    } else if (OB_FAIL(v.manifest_file_->decode_field(*field_projection, d))) {
      LOG_WARN("failed to decode manifest entry field", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to decode manifest file", K(ret));
    throw avro::Exception("failed to decode manifest field");
  }
}

} // namespace avro
