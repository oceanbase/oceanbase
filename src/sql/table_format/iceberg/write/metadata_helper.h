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

#ifndef METADATA_HELPER_H
#define METADATA_HELPER_H

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/spec/manifest_list.h"
#include "sql/table_format/iceberg/spec/manifest.h"

namespace oceanbase
{

namespace sql
{

namespace iceberg
{

class Metadata
{
public:
  Metadata(ObIAllocator& allocator) : allocator_(allocator) {}
  virtual ~Metadata()
  {
    if (manifest_entry_type_ != nullptr) {
      manifest_entry_type_->~StructType();
      manifest_entry_type_ = nullptr;
    }
    if (data_file_type_ != nullptr) {
      data_file_type_->~StructType();
      data_file_type_ = nullptr;
    }
  }

  virtual int init_manifest_file_node() = 0;
  virtual int init_manifest_entry_node() = 0;
  virtual int init_manifest_entry_type(const StructType& partition_type) = 0;
  virtual int init_data_file_type(const StructType& partition_type) = 0;
  virtual int convert_to_avro(const ManifestFile& manifest_file,
                              avro::GenericDatum*& manifest_file_datum) = 0;
  virtual int convert_to_avro(const ManifestEntry& manifest_entry,
                              avro::GenericDatum*& manifest_entry_datum) = 0;
  virtual int convert_to_avro(const DataFile& data_file, avro::GenericDatum& data_file_datum) = 0;
  ::avro::NodePtr& get_manifest_file_node() { return manifest_file_node_; }
  ::avro::NodePtr& get_manifest_entry_node() { return manifest_entry_node_; }
  virtual StructType* get_manifest_file_type() = 0;
  StructType* get_manifest_entry_type() { return manifest_entry_type_; }
  StructType* get_data_file_type() { return data_file_type_; }

  static constexpr const char *MANIFEST_FILE = "manifest_file";
  static constexpr const char *MANIFEST_ENTRY = "manifest_entry";

protected:
  ObIAllocator& allocator_;
  ::avro::NodePtr manifest_file_node_;
  ::avro::NodePtr manifest_entry_node_;
  StructType* manifest_entry_type_ = nullptr;
  StructType* data_file_type_ = nullptr;

private:
  DISALLOW_COPY_AND_ASSIGN(Metadata);
};

class V2Metadata : public Metadata
{
public:
  V2Metadata(ObIAllocator& allocator) : Metadata(allocator) {}
  virtual ~V2Metadata() override = default;

  int init_manifest_file_node() override;
  int init_manifest_entry_node() override;
  int init_manifest_entry_type(const StructType& partition_type) override;
  int init_data_file_type(const StructType& partition_type) override;
  int convert_to_avro(const ManifestFile& manifest_file,
                      avro::GenericDatum*& manifest_file_datum) override;
  int convert_to_avro(const ManifestEntry& manifest_entry,
                      avro::GenericDatum*& manifest_entry_datum) override;
  int convert_to_avro(const DataFile& data_file,
                      avro::GenericDatum& data_file_datum) override;
  virtual StructType* get_manifest_file_type() override { return &manifest_file_type_; }

  inline static StructType manifest_file_type_ = StructType({&ManifestFile::MANIFEST_PATH_FIELD,
                                                            &ManifestFile::MANIFEST_LENGTH_FIELD,
                                                            &ManifestFile::PARTITION_SPEC_ID_FIELD,
                                                            &ManifestFile::REQUIRED_CONTENT_FIELD,
                                                            &ManifestFile::REQUIRED_SEQUENCE_NUMBER_FIELD,
                                                            &ManifestFile::REQUIRED_MIN_SEQUENCE_NUMBER_FIELD,
                                                            &ManifestFile::ADDED_SNAPSHOT_ID_FIELD,
                                                            &ManifestFile::REQUIRED_ADDED_FILES_COUNT_FIELD,
                                                            &ManifestFile::REQUIRED_EXISTING_FILES_COUNT_FIELD,
                                                            &ManifestFile::REQUIRED_DELETED_FILES_COUNT_FIELD,
                                                            &ManifestFile::REQUIRED_ADDED_ROWS_COUNT_FIELD,
                                                            &ManifestFile::REQUIRED_EXISTING_ROWS_COUNT_FIELD,
                                                            &ManifestFile::REQUIRED_DELETED_ROWS_COUNT_FIELD,
                                                            &ManifestFile::PARTITIONS_FIELD,
                                                            &ManifestFile::KEY_METADATA_FIELD});

private:
  DISALLOW_COPY_AND_ASSIGN(V2Metadata);
};

} // namespace iceberg
} // namespace sql
} // namespace oceanbase

#endif // METADATA_HELPER_H