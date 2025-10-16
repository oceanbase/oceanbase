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

#ifndef _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FWD_H
#define _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FWD_H

#include "common/object/ob_object.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{
class ManifestEntry;
}

struct ObLakeTablePartKey
{
public:
  ObLakeTablePartKey();
  int assign(const ObLakeTablePartKey &other);
  void reset();
  int from_manifest_entry(iceberg::ManifestEntry *manifest_entry);
  int hash(uint64_t &hash_val) const;
  bool operator== (const ObLakeTablePartKey &other) const;
  TO_STRING_KV(K_(spec_id), K_(part_values));
  int32_t spec_id_;
  ObArray<common::ObObj> part_values_;
};

enum class ObLakeDeleteFileType
{
  INVALID = 0,
  POSITION_DELETE,
  EQUALITY_DELETE,
  DELETION_VECTOR
};

struct ObLakeDeleteFile
{
public:
  OB_UNIS_VERSION(1);
public:
  int assign(const ObLakeDeleteFile &other);
  ObLakeDeleteFileType type_ = ObLakeDeleteFileType::INVALID;
  ObString file_url_;
  int64_t file_size_ = 0;
  int64_t modification_time_ = 0;
  iceberg::DataFileFormat file_format_;
  TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time));
};

enum class LakeFileType{
    INVALID = 0,
    ICEBERG = 1,
    HIVE = 2
  };

/* structs for optimization */

struct ObIOptLakeTableFile
{
public:
  explicit ObIOptLakeTableFile(LakeFileType type)
  : type_(type)
  {}
  virtual int assign(const ObIOptLakeTableFile &other);
  virtual void reset();
  virtual LakeFileType get_file_type() const { return type_; }
  bool is_iceberg_file() const { return LakeFileType::ICEBERG == type_; }
  bool is_hive_file() const { return LakeFileType::HIVE == type_; }
  static int create_opt_lake_table_file_by_type(ObIAllocator &allocator, LakeFileType type, ObIOptLakeTableFile *&file);
  VIRTUAL_TO_STRING_KV(K_(type));
public:
  LakeFileType type_;
};

struct ObOptIcebergFile : public ObIOptLakeTableFile
{
public:
  ObOptIcebergFile()
  : ObIOptLakeTableFile(LakeFileType::ICEBERG),
    file_url_(), file_size_(0), modification_time_(0),
    file_format_(iceberg::DataFileFormat::INVALID), delete_files_(), record_count_(0)
  {}
  virtual int assign(const ObIOptLakeTableFile &other) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time), K_(delete_files), K_(record_count));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  iceberg::DataFileFormat file_format_;
  common::ObSEArray<const ObLakeDeleteFile *, 1, common::ModulePageAllocator, true> delete_files_;
  int64_t record_count_;
};

struct ObOptHiveFile : public ObIOptLakeTableFile
{
public:
  ObOptHiveFile()
  : ObIOptLakeTableFile(LakeFileType::HIVE),
    file_url_(), file_size_(0), modification_time_(0), part_id_(OB_INVALID_PARTITION_ID)
  {}
  virtual int assign(const ObIOptLakeTableFile &other) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time), K_(part_id));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  int64_t part_id_;
};

/* structs for execution */
struct ObIExtTblScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObIExtTblScanTask()
  : file_url_(), part_id_(OB_INVALID_PARTITION_ID), first_lineno_(1), last_lineno_(INT64_MAX)
  {}

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(part_id), K_(first_lineno), K_(last_lineno));

public:
  ObString file_url_; // serialized in ObFileScanTask
  int64_t part_id_; // serialized in ObHiveScanTask
  int64_t first_lineno_;
  int64_t last_lineno_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIExtTblScanTask);
  int assign(const ObIExtTblScanTask &other);
};

struct ObFileScanTask : public ObIExtTblScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObFileScanTask(LakeFileType type)
  : ObIExtTblScanTask(),
    type_(type),
    file_size_(0),
    modification_time_(0),
    file_id_(0),
    content_digest_(),
    record_count_(0)
  {}

  virtual LakeFileType get_file_type() const { return type_; }
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                           const ObIOptLakeTableFile &opt_table_file) = 0;
  static int create_lake_table_file_by_type(ObIAllocator &allocator, LakeFileType type, ObFileScanTask *&file);
  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count));

  LakeFileType type_;
  int64_t file_size_;
  int64_t modification_time_;
  int64_t file_id_;
  common::ObString content_digest_;
  int64_t record_count_; // serialized in ObIcebergScanTask

private:
  DISALLOW_COPY_AND_ASSIGN(ObFileScanTask);
  int assign(const ObFileScanTask &other);
};

struct ObIcebergScanTask : public ObFileScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObIcebergScanTask(ObIAllocator &allocator)
  : ObFileScanTask(LakeFileType::ICEBERG),
    allocator_(allocator),
    file_format_(iceberg::DataFileFormat::INVALID),
    delete_files_(allocator)
  {}

  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                           const ObIOptLakeTableFile &opt_table_file) override;

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(delete_files), K_(file_id), K_(part_id), K_(content_digest),
                      K_(record_count));

  ObIAllocator &allocator_;
  iceberg::DataFileFormat file_format_;
  common::ObFixedArray<ObLakeDeleteFile, ObIAllocator> delete_files_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIcebergScanTask);
  int assign(const ObIcebergScanTask &other);
};

struct ObHiveScanTask : public ObFileScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObHiveScanTask()
  : ObFileScanTask(LakeFileType::HIVE)
  {}

  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                           const ObIOptLakeTableFile &opt_table_file) override;

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count));

private:
  DISALLOW_COPY_AND_ASSIGN(ObHiveScanTask);
  int assign(const ObHiveScanTask &other);
};

struct ObExtTableScanTask : public ObFileScanTask
{
public:
  explicit ObExtTableScanTask()
  : ObFileScanTask(LakeFileType::INVALID)
  {}

  int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                    const ObIOptLakeTableFile &opt_table_file) override
  { return OB_NOT_SUPPORTED; }

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count));

private:
  DISALLOW_COPY_AND_ASSIGN(ObExtTableScanTask);
  int assign(const ObExtTableScanTask &other);
};

struct ObOdpsScanTask : public ObIExtTblScanTask
{
public:
  explicit ObOdpsScanTask()
  : ObIExtTblScanTask(),
    session_id_(), first_split_idx_(0), last_split_idx_(0)
  {}

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(part_id), K_(session_id), K_(first_split_idx),
                      K_(last_split_idx));

  ObString session_id_;
  int64_t first_split_idx_;
  int64_t last_split_idx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObOdpsScanTask);
  int assign(const ObOdpsScanTask &other);
};

}
}
#endif
