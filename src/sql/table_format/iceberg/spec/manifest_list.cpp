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

#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/GenericDatum.hh>

#include "share/ob_define.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/partition.h"
#include "sql/table_format/iceberg/spec/schema.h"


namespace oceanbase
{
namespace sql
{
namespace iceberg
{

PartitionFieldSummary::PartitionFieldSummary(ObIAllocator &allocator) : SpecWithAllocator(allocator)
{
}

int PartitionFieldSummary::init_from_avro(const avro::GenericRecord &avro_partition_field_summary)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_partition_field_summary,
                                                  CONTAINS_NULL,
                                                  contains_null))) {
      LOG_WARN("fail to get contains_null", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_partition_field_summary,
                                                  CONTAINS_NAN,
                                                  contains_nan))) {
      LOG_WARN("fail to get contains_nan", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_BYTES>(allocator_,
                                                                       avro_partition_field_summary,
                                                                       LOWER_BOUND,
                                                                       lower_bound))) {
      LOG_WARN("failed to get lower_bound", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_BYTES>(allocator_,
                                                                       avro_partition_field_summary,
                                                                       UPPER_BOUND,
                                                                       upper_bound))) {
      LOG_WARN("failed to get upper_bound", K(ret));
    }
  }

  return ret;
}

ManifestFile::ManifestFile(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      partitions(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_)),
      cached_manifest_entries_(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator_))
{
}

int ManifestFile::init_from_avro(const avro::GenericRecord &avro_manifest_file)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::AVRO_STRING>(allocator_,
                                                                  avro_manifest_file,
                                                                  MANIFEST_PATH,
                                                                  manifest_path))) {
      LOG_WARN("failed to get manifest_path", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  MANIFEST_LENGTH,
                                                  manifest_length))) {
      LOG_WARN("failed to get manifest_length", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  PARTITION_SPEC_ID,
                                                  partition_spec_id))) {
      LOG_WARN("failed to get partition_spec_id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    std::optional<int32_t> tmp_content;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file, CONTENT, tmp_content))) {
      LOG_WARN("failed to get content");
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
  }

  if (OB_SUCC(ret)) {
    std::optional<int64_t> tmp_sequence_number;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  SEQUENCE_NUMBER,
                                                  tmp_sequence_number))) {
      LOG_WARN("failed to get sequence_number", K(ret));
    } else if (!tmp_sequence_number.has_value()) {
      // use 0 when reading v1 manifest lists
      sequence_number = 0L;
    } else {
      sequence_number = tmp_sequence_number.value();
    }
  }

  if (OB_SUCC(ret)) {
    std::optional<int64_t> tmp_min_sequence_number;
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  MIN_SEQUENCE_NUMBER,
                                                  tmp_min_sequence_number))) {
      LOG_WARN("failed to get min_sequence_number", K(ret));
    } else if (!tmp_min_sequence_number.has_value()) {
      // use 0 when reading v1 manifest lists
      min_sequence_number = 0L;
    } else {
      min_sequence_number = tmp_min_sequence_number.value();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  ADDED_SNAPSHOT_ID,
                                                  added_snapshot_id))) {
      LOG_WARN("failed to get added_snapshot_id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  ADDED_FILES_COUNT,
                                                  added_files_count))) {
      LOG_WARN("failed to get added_files_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  EXISTING_FILES_COUNT,
                                                  existing_files_count))) {
      LOG_WARN("failed to get existing_files_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  DELETED_FILES_COUNT,
                                                  deleted_files_count))) {
      LOG_WARN("failed to get deleted_files_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  ADDED_ROWS_COUNT,
                                                  added_rows_count))) {
      LOG_WARN("failed to get added_rows_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  EXISTING_ROWS_COUNT,
                                                  existing_rows_count))) {
      LOG_WARN("failed to get existing_rows_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_primitive(avro_manifest_file,
                                                  DELETED_ROWS_COUNT,
                                                  deleted_rows_count))) {
      LOG_WARN("failed to get deleted_rows_count", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_partitions_(avro_manifest_file))) {
      LOG_WARN("failed to get partitions", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogAvroUtils::get_binary<avro::Type::AVRO_BYTES>(allocator_,
                                                                       avro_manifest_file,
                                                                       KEY_METADATA,
                                                                       key_metadata))) {
      LOG_WARN("failed to get key_metadata", K(ret));
    }
  }
  return ret;
}

int ManifestFile::get_partitions_(const avro::GenericRecord &avro_manifest_file)
{
  int ret = OB_SUCCESS;
  const avro::GenericDatum *field_datum = NULL;
  if (OB_FAIL(ObCatalogAvroUtils::get_value<avro::Type::AVRO_ARRAY>(avro_manifest_file,
                                                                    PARTITIONS,
                                                                    field_datum))) {
    LOG_WARN("failed to get partitions", K(ret));
  } else if (NULL == field_datum || avro::Type::AVRO_NULL == field_datum->type()) {
    partitions.reset();
  } else {
    const avro::GenericArray &datum_array = field_datum->value<avro::GenericArray>();
    for (size_t i = 0; OB_SUCC(ret) && i < datum_array.value().size(); i++) {
      PartitionFieldSummary *summary = NULL;
      const avro::GenericDatum &element_datum = datum_array.value()[i];
      if (avro::Type::AVRO_RECORD != element_datum.type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid partition field summary type", K(ret), K(element_datum.type()));
      } else if (OB_FALSE_IT(summary = OB_NEWx(PartitionFieldSummary, &allocator_, allocator_))) {
      } else if (OB_ISNULL(summary)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate failed", K(ret));
      } else if (OB_FAIL(summary->init_from_avro(element_datum.value<avro::GenericRecord>()))) {
        LOG_WARN("failed to init PartitionFieldSummary", K(ret));
      } else if (OB_FAIL(partitions.push_back(summary))) {
        LOG_WARN("failed to add PartitionFieldSummary", K(ret));
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
    if (OB_UNLIKELY(cached_manifest_entries_.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty manifest entry", K(ret));
    } else if (OB_FAIL(manifest_entries.reserve(cached_manifest_entries_.size()))) {
      LOG_WARN("failed to reserve manifest entries", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cached_manifest_entries_.size(); i++) {
        OZ(manifest_entries.push_back(cached_manifest_entries_[i]));
      }
    }
  }
  return ret;
}

int ManifestFile::get_manifest_entries_(const ObString &access_info,
                                       ObIArray<ManifestEntry *> &manifest_entries) const
{
  int ret = OB_SUCCESS;
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
      ManifestMetadata manifest_metadata(allocator_);
      if (OB_FAIL(manifest_metadata.init_from_metadata(metadata))) {
        LOG_WARN("failed to init manifest metadata", K(ret));
      } else {
        avro::DataFileReader<avro::GenericDatum> avro_reader(std::move(avro_reader_base));
        avro::GenericDatum generic_datum(avro_reader.dataSchema());
        while (OB_SUCC(ret) && avro_reader.read(generic_datum)) {
          ManifestEntry *manifest_entry = OB_NEWx(ManifestEntry, &allocator_, allocator_);
          if (avro::Type::AVRO_RECORD != generic_datum.type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid manifest entry", K(ret), K(generic_datum.type()));
          } else if (OB_FAIL(manifest_entry->init_from_avro(
                         *this,
                         manifest_metadata,
                         generic_datum.value<avro::GenericRecord>()))) {
            LOG_WARN("failed to init manifest entry", K(ret));
          } else if (OB_FAIL(manifest_entries.push_back(manifest_entry))) {
            LOG_WARN("failed to add manifest entry", K(ret));
          }
        }
      }
    } catch (std::exception &e) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to read manifest", K(ret), K(e.what()));
    }
  }

  return ret;
}

} // namespace iceberg
} // namespace sql
} // namespace oceanbase