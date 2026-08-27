/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_lake_table_fwd.h"
#include "sql/table_format/iceberg/spec/manifest.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObLakeTablePartKey::ObLakeTablePartKey() : manifest_entry_(nullptr), hash_value_(0)
{}

int ObLakeTablePartKey::assign(const ObLakeTablePartKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    manifest_entry_ = other.manifest_entry_;
    hash_value_ = other.hash_value_;
  }
  return ret;
}

void ObLakeTablePartKey::reset()
{
  manifest_entry_ = nullptr;
  hash_value_ = 0;
}

int ObLakeTablePartKey::from_manifest_entry(const iceberg::ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(manifest_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null data file");
  } else {
    manifest_entry_ = manifest_entry;
    hash_value_ = do_hash(manifest_entry->partition_spec_id, 0);
    const ObIArray<ObObj> &part_values = manifest_entry->data_file.partition;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_values.count(); ++i) {
      uint64_t next_hash_value = 0;
      if (OB_FAIL(part_values.at(i).hash(next_hash_value, hash_value_))) {
        LOG_WARN("failed to hash partition value", K(ret), K(i));
      } else {
        hash_value_ = next_hash_value;
      }
    }
  }
  return ret;
}

int ObLakeTablePartKey::hash(uint64_t &hash_val) const
{
  hash_val = hash_value_;
  return OB_SUCCESS;
}

int ObLakeTablePartKey::get_partition_value(const int64_t idx, const ObObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  if (OB_ISNULL(manifest_entry_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("manifest entry is null", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= manifest_entry_->data_file.partition.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition value index is out of range",
             K(ret),
             K(idx),
             K(manifest_entry_->data_file.partition.count()));
  } else {
    value = &manifest_entry_->data_file.partition.at(idx);
  }
  return ret;
}

bool ObLakeTablePartKey::operator== (const ObLakeTablePartKey &other) const
{
  bool is_equal = manifest_entry_ == other.manifest_entry_;
  if (!is_equal && OB_NOT_NULL(manifest_entry_) && OB_NOT_NULL(other.manifest_entry_)
      && manifest_entry_->partition_spec_id == other.manifest_entry_->partition_spec_id
      && manifest_entry_->data_file.partition.count()
             == other.manifest_entry_->data_file.partition.count()) {
    is_equal = true;
    for (int64_t i = 0; is_equal && i < manifest_entry_->data_file.partition.count(); ++i) {
      if (!manifest_entry_->data_file.partition.at(i).is_equal(
              other.manifest_entry_->data_file.partition.at(i))) {
        is_equal = false;
      }
    }
  }
  return is_equal;
}

int ObLakeDeleteFile::assign(const ObLakeDeleteFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    type_ = other.type_;
    file_url_ = other.file_url_;
    file_size_ = other.file_size_;
    modification_time_ = other.modification_time_;
    file_format_ = other.file_format_;
    dv_content_offset_ = other.dv_content_offset_;
    dv_content_size_in_bytes_ = other.dv_content_size_in_bytes_;
    is_file_scoped_ = other.is_file_scoped_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLakeDeleteFile,
                    type_,
                    file_url_,
                    file_size_,
                    modification_time_,
                    file_format_,
                    dv_content_offset_,
                    dv_content_size_in_bytes_,
                    is_file_scoped_);

int ObIOptLakeTableFile::assign(const ObIOptLakeTableFile &other)
{
  type_ = other.type_;
  return OB_SUCCESS;
}

void ObIOptLakeTableFile::reset()
{
  type_ = LakeFileType::INVALID;
}

int ObIOptLakeTableFile::create_opt_lake_table_file_by_type(ObIAllocator &allocator,
                                                            LakeFileType type,
                                                            ObIOptLakeTableFile *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (type == LakeFileType::ICEBERG) {
    file = OB_NEWx(ObOptIcebergFile, &allocator, allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObOptHiveFile, &allocator);
  } else if (type == LakeFileType::EXT_PLUGIN) {
    file = OB_NEWx(ObOptPluginFile, &allocator, allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file type", K(type));
  }
  return ret;
}

int ObOptIcebergFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptIcebergFile &iceberg_file = static_cast<const ObOptIcebergFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else {
      file_url_ = iceberg_file.file_url_;
      file_size_ = iceberg_file.file_size_;
      modification_time_ = iceberg_file.modification_time_;
      file_format_ = iceberg_file.file_format_;
      record_count_ = iceberg_file.record_count_;
      part_id_ = iceberg_file.part_id_;
      partition_spec_id_ = iceberg_file.partition_spec_id_;
      file_desc_idx_ = iceberg_file.file_desc_idx_;
      if (OB_FAIL(delete_files_.assign(iceberg_file.delete_files_))) {
        LOG_WARN("failed to assign delete files");
      }
    }
  }
  return ret;
}

void ObOptIcebergFile::reset()
{
  ObIOptLakeTableFile::reset();
  file_url_.reset();
  file_size_ = 0;
  modification_time_ = 0;
  file_format_ = iceberg::DataFileFormat::INVALID;
  delete_files_.reset();
  record_count_ = 0;
  part_id_ = OB_INVALID_INDEX_INT64;
  partition_spec_id_ = -1;
  file_desc_idx_ = OB_INVALID_INDEX_INT64;
}

int ObOptHiveFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptHiveFile &hive_file = static_cast<const ObOptHiveFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else {
      file_url_ = hive_file.file_url_;
      file_size_ = hive_file.file_size_;
      modification_time_ = hive_file.modification_time_;
      part_id_ = hive_file.part_id_;
    }
  }
  return ret;
}

void ObOptHiveFile::reset()
{
  ObIOptLakeTableFile::reset();
  file_url_.reset();
  file_size_ = 0;
  modification_time_ = 0;
  part_id_ = OB_INVALID_PARTITION_ID;
}

int ObOptPluginFile::assign(const ObIOptLakeTableFile &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    const ObOptPluginFile &plugin_file = static_cast<const ObOptPluginFile&>(other);
    if (OB_FAIL(ObIOptLakeTableFile::assign(other))) {
      LOG_WARN("failed to assign ObIOptLakeTableFile");
    } else if (OB_FAIL(ob_write_string(allocator_, plugin_file.task_json_, task_json_))) {
      LOG_WARN("failed to copy plugin task json", K(ret));
    } else {
      record_count_ = plugin_file.record_count_;
    }
  }
  return ret;
}

void ObOptPluginFile::reset()
{
  ObIOptLakeTableFile::reset();
  task_json_.reset();
  record_count_ = 0;
}

OB_SERIALIZE_MEMBER(ObIExtTblScanTask);

OB_SERIALIZE_MEMBER((ObFileScanTask, ObIExtTblScanTask), file_url_, file_size_, modification_time_);

int ObFileScanTask::create_lake_table_file_by_type(ObIAllocator &allocator,
                                                  LakeFileType type,
                                                  ObFileScanTask *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (type == LakeFileType::ICEBERG) {
    file = OB_NEWx(ObIcebergScanTask, &allocator, allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObHiveScanTask, &allocator);
  } else if (type == LakeFileType::EXT_PLUGIN) {
    file = OB_NEWx(ObPluginScanTask, &allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file type", K(type));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObIcebergScanTask, ObFileScanTask),
                    file_format_,
                    delete_files_,
                    record_count_,
                    part_id_,
                    partition_spec_id_);

int ObIcebergScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                    const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_iceberg_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptIcebergFile &opt_iceberg_file = static_cast<const ObOptIcebergFile&>(opt_table_file);
    if (OB_FAIL(delete_files_.init(opt_iceberg_file.delete_files_.count()))) {
        LOG_WARN("allocate failed");
    } else if (OB_FAIL(ob_write_string(allocator, opt_iceberg_file.file_url_, file_url_))) {
      LOG_WARN("failed to write file url");
    } else {
      file_size_ = opt_iceberg_file.file_size_;
      modification_time_ = opt_iceberg_file.modification_time_;
      file_format_ = opt_iceberg_file.file_format_;
      record_count_ = opt_iceberg_file.record_count_;
      part_id_ = opt_iceberg_file.part_id_;
      partition_spec_id_ = opt_iceberg_file.partition_spec_id_;
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_iceberg_file.delete_files_.count(); i++) {
        const ObLakeDeleteFile *other_delete_file = opt_iceberg_file.delete_files_.at(i);
        if (OB_ISNULL(other_delete_file)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("delete file is null");
        } else {
          ObLakeDeleteFile delete_file;
          OZ(delete_file.assign(*other_delete_file));
          OZ(ob_write_string(allocator, other_delete_file->file_url_, delete_file.file_url_));
          OZ(delete_files_.push_back(delete_file));
        }
      }
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObHiveScanTask, ObFileScanTask),
                    part_id_);

int ObHiveScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                  const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_hive_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptHiveFile &opt_hive_file = static_cast<const ObOptHiveFile&>(opt_table_file);
    if (OB_FAIL(ob_write_string(allocator, opt_hive_file.file_url_, file_url_))) {
      LOG_WARN("failed to write file url");
    } else {
      file_size_ = opt_hive_file.file_size_;
      modification_time_ = opt_hive_file.modification_time_;
      part_id_ = opt_hive_file.part_id_;
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObPluginScanTask, ObFileScanTask),
                    part_id_,
                    task_json_,
                    record_count_);

int ObPluginScanTask::init_with_opt_lake_table_file(ObIAllocator &allocator,
                                                    const ObIOptLakeTableFile &opt_table_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!opt_table_file.is_ext_plugin_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected opt table file type", K(opt_table_file.get_file_type()));
  } else {
    const ObOptPluginFile &opt_plugin_file = static_cast<const ObOptPluginFile &>(opt_table_file);
    if (OB_FAIL(ob_write_string(allocator, opt_plugin_file.task_json_, task_json_))) {
      LOG_WARN("failed to write plugin task json");
    } else {
      record_count_ = opt_plugin_file.record_count_;
    }
  }
  return ret;
}

int ObExtTableScanTask::init_parallel_parse_csv_info(ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(parallel_parse_csv_info_)) {
    parallel_parse_csv_info_ = OB_NEWx(ObCsvParallelInfo, (&allocator));
    if (OB_ISNULL(parallel_parse_csv_info_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ObCsvParallelInfo", K(ret));
    }
  }
  return ret;
}

}
}
