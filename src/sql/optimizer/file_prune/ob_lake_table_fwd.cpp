/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "ob_lake_table_fwd.h"
#include "sql/table_format/iceberg/spec/manifest.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
ObLakeTablePartKey::ObLakeTablePartKey()
: spec_id_(-1),
  part_values_()
{}

int ObLakeTablePartKey::assign(const ObLakeTablePartKey &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    spec_id_ = other.spec_id_;
    ret = part_values_.assign(other.part_values_);
  }
  return ret;
}

void ObLakeTablePartKey::reset()
{
  spec_id_ = -1;
  part_values_.reset();
}

int ObLakeTablePartKey::from_manifest_entry(iceberg::ManifestEntry *manifest_entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(manifest_entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null data file");
  } else if (OB_FAIL(part_values_.assign(manifest_entry->data_file.partition))) {
    LOG_WARN("failed to assign part values");
  } else {
    spec_id_ = manifest_entry->partition_spec_id;
  }
  return ret;
}

int ObLakeTablePartKey::hash(uint64_t &hash_val) const
{
  int ret = OB_SUCCESS;
  hash_val = do_hash(spec_id_, hash_val);
  for (int64_t i = 0; i < part_values_.count(); ++i) {
    hash_val = part_values_.at(i).hash(hash_val);
  }
  return ret;
}

bool ObLakeTablePartKey::operator== (const ObLakeTablePartKey &other) const
{
  bool is_equal = false;
  if (spec_id_ == other.spec_id_ &&
      part_values_.count() == other.part_values_.count()) {
    is_equal = true;
    for (int64_t i = 0; is_equal && i < part_values_.count(); ++i) {
      if (!part_values_.at(i).is_equal(other.part_values_.at(i))) {
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
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObLakeDeleteFile, type_, file_url_, file_size_, modification_time_, file_format_);

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
    file = OB_NEWx(ObOptIcebergFile, &allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObOptHiveFile, &allocator);
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

// type_不需要序列化。由于反序列化时要先知道ObILakeTableFile的具体类型才能创建对应的子类，
// 因此在序列化、反序列化ObILakeTableFile之前一定要先单独序列化type_，这里不需要再重复序列化了。
OB_SERIALIZE_MEMBER(ObILakeTableFile);

int ObILakeTableFile::create_lake_table_file_by_type(ObIAllocator &allocator,
                                                     LakeFileType type,
                                                     ObILakeTableFile *&file)
{
  int ret = OB_SUCCESS;
  file = nullptr;
  if (type == LakeFileType::ICEBERG) {
    file = OB_NEWx(ObIcebergFile, &allocator, allocator);
  } else if (type == LakeFileType::HIVE) {
    file = OB_NEWx(ObHiveFile, &allocator, allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected file type", K(type));
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObIcebergFile, ObILakeTableFile),
                    file_url_,
                    file_size_,
                    modification_time_,
                    file_format_,
                    delete_files_);

int ObIcebergFile::init_with_opt_lake_table_file(ObIAllocator &allocator,
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

OB_SERIALIZE_MEMBER((ObHiveFile, ObILakeTableFile),
                    file_url_,
                    file_size_,
                    modification_time_,
                    part_id_);

int ObHiveFile::init_with_opt_lake_table_file(ObIAllocator &allocator,
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

}
}