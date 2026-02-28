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

#ifndef MANIFEST_WRITER_H
#define MANIFEST_WRITER_H

#define USING_LOG_PREFIX SQL

#include "share/external_table/ob_hdfs_storage_info.h"
#include "sql/engine/basic/ob_select_into_basic.h"
#include "sql/table_format/iceberg/write/avro_data_file_writer.h"
#include "sql/table_format/iceberg/write/metadata_helper.h"


namespace oceanbase
{
namespace sql
{
namespace iceberg
{

class ObIcebergWriter
{
public:
  ObIcebergWriter(const ObString& url,
                  const ObString& access_info,
                  bool can_overwrite = false)
    : url_(url),
      access_info_(access_info),
      can_overwrite_(can_overwrite) {}
  virtual ~ObIcebergWriter() = default;

  virtual int open_file();
  virtual int close();

protected:
  const common::ObString url_;
  const common::ObString access_info_;
  bool can_overwrite_;
  ObStorageAppender storage_appender_;
  share::ObExternalTableStorageInfo backup_info_;
  share::ObHDFSStorageInfo hdfs_info_;
  common::ObObjectStorageInfo* storage_info_ = nullptr;
  bool is_file_opened_ = false;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIcebergWriter);
};


class ObManifestWriter : public ObIcebergWriter
{
public:
  ObManifestWriter(const ObString& url,
                   const ObString& access_info,
                   FormatVersion format_version,
                   int32_t current_schema_id)
    : ObIcebergWriter(url, access_info),
      format_version_(format_version),
      current_schema_id_(current_schema_id)
    {}
  virtual ~ObManifestWriter() override {
    if (metadata_ != nullptr) {
      metadata_->~Metadata();
      metadata_ = nullptr;
    }
    if (writer_ != nullptr) {
      writer_->~DataFileWriter<avro::GenericDatum>();
      writer_ = nullptr;
    }
  }
  virtual int open_file() = 0;
  int64_t file_size() const { return file_size_; }
  virtual int close() override;

protected:
  FormatVersion format_version_;
  int32_t current_schema_id_;
  common::ObArenaAllocator allocator_;
  Metadata* metadata_ = nullptr;
  DataFileWriter<avro::GenericDatum>* writer_ = nullptr;
  int64_t file_size_ = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObManifestWriter);
};


class ObManifestListWriter : public ObManifestWriter
{
public:
  ObManifestListWriter(const ObString& url,
                       const ObString& access_info,
                       FormatVersion format_version,
                       int32_t current_schema_id,
                       std::optional<int64_t> parent_snapshot_id,
                       int64_t new_snapshot_id,
                       int64_t new_sequence_number)
    : ObManifestWriter(url, access_info, format_version, current_schema_id),
      parent_snapshot_id_(parent_snapshot_id),
      new_snapshot_id_(new_snapshot_id),
      new_sequence_number_(new_sequence_number)
  {}
  virtual ~ObManifestListWriter() override = default;
  virtual int open_file() override;
  int write_manifest_file(ManifestFile& manifest_file);

private:
  std::optional<int64_t> parent_snapshot_id_;
  int64_t new_snapshot_id_;
  int64_t new_sequence_number_;
  DISALLOW_COPY_AND_ASSIGN(ObManifestListWriter);
};


class ObManifestFileWriter : public ObManifestWriter
{
public:
  ObManifestFileWriter(const ObString& url,
                       const ObString& access_info,
                       FormatVersion format_version,
                       int32_t current_schema_id,
                       const ObString& current_schema_str)
    : ObManifestWriter(url, access_info, format_version, current_schema_id),
      current_schema_str_(current_schema_str)
  {}
  virtual ~ObManifestFileWriter() override = default;
  virtual int open_file() override;
  int write_manifest_entry(ManifestEntry& manifest_entry);

private:
  common::ObString current_schema_str_;
  DISALLOW_COPY_AND_ASSIGN(ObManifestFileWriter);
};


class ObMetadataWriter : public ObIcebergWriter
{
public:
  ObMetadataWriter(const ObString& url,
                   const ObString& access_info)
    : ObIcebergWriter(url, access_info)
    {}
  virtual ~ObMetadataWriter() override = default;
  int write_metadata(const TableMetadata& metadata,
                     ObIAllocator& allocator);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMetadataWriter);
};


// only for filesystem catalog (S3)
class ObVersionHintWriter : public ObIcebergWriter
{
public:
  ObVersionHintWriter(const ObString& url,
                      const ObString& access_info)
    : ObIcebergWriter(url, access_info, true)
    {}
  virtual ~ObVersionHintWriter() override = default;
  int overwrite_version(const int64_t version);

private:
  DISALLOW_COPY_AND_ASSIGN(ObVersionHintWriter);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // MANIFEST_WRITER_H