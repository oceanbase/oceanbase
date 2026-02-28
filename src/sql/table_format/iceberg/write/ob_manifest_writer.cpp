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

#include "sql/table_format/iceberg/write/ob_manifest_writer.h"

#include <avro/Generic.hh>

#include "share/ob_define.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"


namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int ObIcebergWriter::open_file()
{
  int ret = OB_SUCCESS;
  common::ObStorageType device_type;
  if (OB_FAIL(get_storage_type_from_path_for_external_table(url_, device_type))) {
    LOG_WARN("failed to get storage type from path", K(ret));
  } else {
    ObArenaAllocator tmp_allocator;
    ObString url_cstr;
    ObString access_info_cstr;
    OZ(ob_write_string(tmp_allocator, url_, url_cstr, true));
    OZ(ob_write_string(tmp_allocator, access_info_, access_info_cstr, true));
    ObStorageAccessType access_type = (device_type == OB_STORAGE_HDFS)
                                      ? OB_STORAGE_ACCESS_APPENDER
                                      : OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER;
    storage_info_ = (device_type == OB_STORAGE_HDFS)
                    ? static_cast<ObObjectStorageInfo*>(&hdfs_info_)
                    : static_cast<ObObjectStorageInfo*>(&backup_info_);

    if (OB_FAIL(storage_info_->set(url_cstr.ptr(), access_info_cstr.ptr()))) {
      LOG_WARN("failed to set storage info", K(ret));
    } else if (!storage_info_->is_valid()) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file url_ not exist", K(ret), KPC(storage_info_));
    }

    if (OB_SUCC(ret) && !can_overwrite_) {
      bool is_exist = false;
      ObExternalIoAdapter adapter;
      if (OB_FAIL(adapter.is_exist(url_, storage_info_, is_exist))) {
        // When file object does not exist on hdfs then return OB_HDFS_PATH_NOT_FOUND.
        if (device_type == OB_STORAGE_HDFS && OB_HDFS_PATH_NOT_FOUND == ret) {
          ret = OB_SUCCESS;
          is_exist = false;
        } else {
          LOG_WARN("failed to check file exist", K(ret), K(url_), KPC(storage_info_));
        }
      }
      if (OB_SUCC(ret) && is_exist) {
        ret = OB_FILE_ALREADY_EXIST;
        LOG_WARN("file already exist", K(ret), K(url_), KPC(storage_info_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(storage_appender_.open(storage_info_, url_, access_type))) {
      LOG_WARN("failed to open file", K(ret), K(url_), KPC(storage_info_));
    } else {
      is_file_opened_ = true;
    }
  }
  return ret;
}

int ObIcebergWriter::close()
{
  int ret = OB_SUCCESS;
  OZ(storage_appender_.close());
  return ret;
}

int ObManifestWriter::close()
{
  int ret = OB_SUCCESS;
  try {
    writer_->close();
    file_size_ = storage_appender_.offset_;
    OZ(storage_appender_.close());
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when closing", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERROR_DURING_COMMIT, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when closing", K(ret));
    }
  }
  return ret;
}

int ObManifestListWriter::open_file()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH;
  int64_t pos = 0;
  ObString iceberg_schema;
  if (OB_FAIL(ObIcebergWriter::open_file())) {
    LOG_WARN("failed to open file", K(ret));
  } else if (format_version_ != FormatVersion::V2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support this format version now", K(format_version_));
  } else if (OB_ISNULL(metadata_ = OB_NEWx(V2Metadata, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc metadata", K(ret));
  } else if (OB_FAIL(metadata_->init_manifest_file_node())) {
    LOG_WARN("failed to init manifest file node", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
  } else if (OB_ISNULL(metadata_->get_manifest_file_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest file type is null", K(ret));
  } else if (OB_FALSE_IT(metadata_->get_manifest_file_type()->set_schema_id(current_schema_id_))) {
  } else if (OB_FAIL(metadata_->get_manifest_file_type()->to_json_kv_string(buf, buf_len, pos))) {
    LOG_WARN("failed to convert manifest file struct to json", K(ret));
  } else {
    iceberg_schema.assign_ptr(buf, pos);
    avro::ValidSchema schema(metadata_->get_manifest_file_node());
    std::map<std::string, std::string> metadata;
    metadata["format-version"] = std::to_string(static_cast<int32_t>(format_version_));
    metadata["parent-snapshot-id"] = parent_snapshot_id_.has_value()
                                     ? std::to_string(parent_snapshot_id_.value())
                                     : "null";
    metadata["snapshot-id"] = std::to_string(new_snapshot_id_);
    metadata["sequence-number"] = std::to_string(new_sequence_number_);
    metadata["iceberg.schema"] = std::string(iceberg_schema.ptr(), iceberg_schema.length());
    writer_ = OB_NEWx(DataFileWriter<avro::GenericDatum>,
                      &allocator_,
                      ObAvroOutputStream::create_avro_output_stream(storage_appender_),
                      schema,
                      metadata,
                      16 * 1024,
                      avro::Codec::DEFLATE_CODEC); //todo: hardcode now
    if (OB_ISNULL(writer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc writer", K(ret));
    }
  }
  return ret;
}

int ObManifestListWriter::write_manifest_file(ManifestFile& manifest_file)
{
  int ret = OB_SUCCESS;
  avro::GenericDatum* datum = nullptr;
  try {
    if (OB_UNLIKELY(!is_file_opened_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("file not opened", K(ret));
    } else if (OB_ISNULL(metadata_) || OB_ISNULL(writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("metadata or writer is null", K(ret));
    } else if (OB_FAIL(metadata_->convert_to_avro(manifest_file, datum))) {
      LOG_WARN("failed to convert_to_avro", K(ret));
    } else {
      writer_->write(*datum);
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when writing manifest file", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERROR_DURING_COMMIT, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when writing manifest file", K(ret));
    }
  }
  return ret;
}

int ObManifestFileWriter::open_file()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH;
  int64_t pos = 0;
  ObString iceberg_schema;
  if (OB_FAIL(ObIcebergWriter::open_file())) {
    LOG_WARN("failed to open file", K(ret));
  } else if (format_version_ != FormatVersion::V2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support this format version now", K(format_version_));
  } else if (OB_ISNULL(metadata_ = OB_NEWx(V2Metadata, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc metadata", K(ret));
  } else if (OB_FAIL(metadata_->init_manifest_entry_node())) {
    LOG_WARN("failed to init manifest entry node", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
  } else if (OB_ISNULL(metadata_->get_manifest_entry_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest_entry_type is NULL", K(ret));
  } else if (OB_FALSE_IT(metadata_->get_manifest_entry_type()->set_schema_id(current_schema_id_))) {
  } else if (OB_FAIL(metadata_->get_manifest_entry_type()->to_json_kv_string(buf, buf_len, pos))) {
    LOG_WARN("failed to convert manifest entry struct to json", K(ret));
  } else {
    iceberg_schema.assign_ptr(buf, pos);
    avro::ValidSchema schema(metadata_->get_manifest_entry_node());
    std::map<std::string, std::string> metadata;
    metadata["schema"] = std::string(current_schema_str_.ptr(), current_schema_str_.length());
    metadata["schema-id"] = std::to_string(current_schema_id_);
    metadata["format-version"] = std::to_string(static_cast<int32_t>(format_version_));
    metadata["iceberg.schema"] = std::string(iceberg_schema.ptr(), iceberg_schema.length());
    //todo: hardcode now
    metadata["partition-spec-id"] = std::to_string(0);
    metadata["partition-spec"] = "[]";
    metadata["content"] = "data";
    writer_ = OB_NEWx(DataFileWriter<avro::GenericDatum>,
                      &allocator_,
                      ObAvroOutputStream::create_avro_output_stream(storage_appender_),
                      schema,
                      metadata,
                      16 * 1024,
                      avro::Codec::DEFLATE_CODEC); //todo: hardcode now
    if (OB_ISNULL(writer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc writer", K(ret));
    }
  }
  return ret;
}

int ObManifestFileWriter::write_manifest_entry(ManifestEntry& manifest_entry)
{
  int ret = OB_SUCCESS;
  avro::GenericDatum* datum = nullptr;
  try {
    if (OB_UNLIKELY(!is_file_opened_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("file not opened", K(ret));
    } else if (OB_FAIL(metadata_->convert_to_avro(manifest_entry, datum))) {
      LOG_WARN("failed to convert_to_avro", K(ret));
    } else {
      writer_->write(*datum);
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when writing manifest entry", K(ret), "Info", ex.what());
      LOG_USER_ERROR(OB_ERROR_DURING_COMMIT, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERROR_DURING_COMMIT;
      LOG_WARN("caught exception when writing manifest entry", K(ret));
    }
  }
  return ret;
}

int ObMetadataWriter::write_metadata(const TableMetadata& metadata,
                                     ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH / 2;
  int64_t pos = 0;
  int64_t write_size = 0;
  do {
    buf_len *= 2;
    pos = 0;
    ret = OB_SUCCESS;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_FAIL(metadata.to_json_kv_string(buf, buf_len, pos))) {
      LOG_WARN("failed to convert metadata to json", K(ret));
    }
  } while (OB_SIZE_OVERFLOW == ret);

  if (OB_SUCC(ret) && OB_FAIL(storage_appender_.append(buf, pos, write_size))) {
    LOG_WARN("failed to append metadata", K(ret), K(pos));
  }
  return ret;
}

int ObVersionHintWriter::overwrite_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  std::string buf = std::to_string(version);
  int64_t write_size = 0;
  if (OB_FAIL(storage_appender_.append(buf.c_str(), buf.length(), write_size))) {
    LOG_WARN("failed to append metadata", K(ret));
  }
  return ret;
}


} // namespace iceberg
} // namespace sql
} // namespace oceanbase