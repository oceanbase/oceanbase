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

#include "sql/table_format/iceberg/spec/statistics.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"

#define USING_LOG_PREFIX SQL

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

BlobMetadata::BlobMetadata(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      fields(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator)),
      properties(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int BlobMetadata::assign(const BlobMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    snapshot_id = other.snapshot_id;
    sequence_number = other.sequence_number;
    if (OB_FAIL(ObIcebergUtils::deep_copy_optional_string(allocator_, other.type, type))) {
      LOG_WARN("failed to deep copy type", K(ret));
    } else if (OB_FAIL(fields.assign(other.fields))) {
      LOG_WARN("failed to assign fields", K(ret));
    } else if (OB_FAIL(ObIcebergUtils::deep_copy_map_string(allocator_,
                                                            other.properties,
                                                            properties))) {
      LOG_WARN("failed to deep copy map", K(ret));
    }
  }
  return ret;
}

int BlobMetadata::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator_, json_object, TYPE, type))) {
      LOG_WARN("failed to get type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SNAPSHOT_ID, snapshot_id))) {
      LOG_WARN("failed to get snapshot-id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SEQUENCE_NUMBER, sequence_number))) {
      LOG_WARN("failed to get sequence-number", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive_array(json_object, FIELDS, fields))) {
      LOG_WARN("failed to get fields", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const ObJsonNode *json_properties = json_object.get_value(PROPERTIES);
    if (NULL != json_properties) {
      if (ObJsonNodeType::J_OBJECT != json_properties->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid json properties", K(ret));
      } else if (OB_FAIL(ObCatalogJsonUtils::convert_json_object_to_map(
                     allocator_,
                     *down_cast<const ObJsonObject *>(json_properties),
                     properties))) {
        LOG_WARN("failed to get properties", K(ret));
      }
    } else {
      properties.reset();
    }
  }
  return ret;
}

StatisticsFile::StatisticsFile(ObIAllocator &allocator)
    : SpecWithAllocator(allocator),
      blob_metadata(OB_MALLOC_SMALL_BLOCK_SIZE, ModulePageAllocator(allocator))
{
}

int StatisticsFile::assign(const StatisticsFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    snapshot_id = other.snapshot_id;
    file_size_in_bytes = other.file_size_in_bytes;
    file_footer_size_in_bytes = other.file_footer_size_in_bytes;
    if (OB_FAIL(ob_write_string(allocator_, other.statistics_path, statistics_path))) {
      LOG_WARN("failed to copy statistics-path", K(ret));
    } else if (OB_FAIL(ObIcebergUtils::deep_copy_optional_string(allocator_,
                                                                 other.key_metadata,
                                                                 key_metadata))) {
      LOG_WARN("failed to copy key-metadata", K(ret));
    } else if (OB_FAIL(ObIcebergUtils::deep_copy_array_object(allocator_,
                                                              other.blob_metadata,
                                                              blob_metadata))) {
      LOG_WARN("failed to copy blob-metadata", K(ret));
    }
  }
  return ret;
}

int StatisticsFile::init_from_json(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object, SNAPSHOT_ID, snapshot_id))) {
      LOG_WARN("failed to get snapshot-id", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_string(allocator_,
                                               json_object,
                                               STATISTICS_PATH,
                                               statistics_path))) {
      LOG_WARN("failed to get statistics-path", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                  FILE_SIZE_IN_BYTES,
                                                  file_size_in_bytes))) {
      LOG_WARN("failed to get file-size-in-bytes", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObCatalogJsonUtils::get_primitive(json_object,
                                                  FILE_FOOTER_SIZE_IN_BYTES,
                                                  file_footer_size_in_bytes))) {
      LOG_WARN("failed to get file-footer-size-in-bytes", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(
            ObCatalogJsonUtils::get_string(allocator_, json_object, KEY_METADATA, key_metadata))) {
      LOG_WARN("failed to get key-metadata", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse_blob_metadata_(json_object))) {
      LOG_WARN("failed to parse blob-metadata", K(ret));
    }
  }
  return ret;
}

int StatisticsFile::parse_blob_metadata_(const ObJsonObject &json_object)
{
  int ret = OB_SUCCESS;
  const ObJsonNode *json_blob_metadata = json_object.get_value(BLOB_METADATA);
  if (OB_ISNULL(json_blob_metadata)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("blob-metadata must existed", K(ret));
  } else if (ObJsonNodeType::J_ARRAY != json_blob_metadata->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid blob-metadata type", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < json_blob_metadata->element_count(); i++) {
      ObIJsonBase *json_element = NULL;
      BlobMetadata *blob = NULL;
      if (OB_FAIL(json_blob_metadata->get_array_element(i, json_element))) {
        LOG_WARN("failed to get blob-metadata", K(ret));
      } else if (ObJsonNodeType::J_OBJECT != json_element->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid json element type", K(ret));
      } else if (OB_ISNULL(blob = OB_NEWx(BlobMetadata, &allocator_, allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FAIL(blob->init_from_json(*down_cast<ObJsonObject *>(json_element)))) {
        LOG_WARN("failed to init blob-metadata", K(ret));
      } else {
        OZ(blob_metadata.push_back(blob));
      }
    }
  }
  return ret;
}

} // namespace iceberg

} // namespace sql
} // namespace oceanbase