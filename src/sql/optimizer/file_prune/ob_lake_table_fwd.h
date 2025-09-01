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
    file_format_(iceberg::DataFileFormat::INVALID), delete_files_()
  {}
  virtual int assign(const ObIOptLakeTableFile &other) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time), K_(delete_files));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  iceberg::DataFileFormat file_format_;
  common::ObSEArray<const ObLakeDeleteFile *, 1, common::ModulePageAllocator, true> delete_files_;
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
struct ObILakeTableFile
{
public:
  OB_UNIS_VERSION_V(1);
public:

  explicit ObILakeTableFile(ObIAllocator &allocator, LakeFileType type)
  : allocator_(allocator),
    type_(type)
  {}
  virtual LakeFileType get_file_type() const { return type_; }
  bool is_iceberg_file() const { return LakeFileType::ICEBERG == type_; }
  bool is_hive_file() const { return LakeFileType::HIVE == type_; }
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator, const ObIOptLakeTableFile &opt_table_file) = 0;
  static int create_lake_table_file_by_type(ObIAllocator &allocator, LakeFileType type, ObILakeTableFile *&file);
  VIRTUAL_TO_STRING_KV(K_(type));

public:
  ObIAllocator &allocator_;
  LakeFileType type_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObILakeTableFile);
  int assign(const ObILakeTableFile &other);
};

struct ObIcebergFile : public ObILakeTableFile
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObIcebergFile(ObIAllocator &allocator)
  : ObILakeTableFile(allocator, LakeFileType::ICEBERG),
    file_url_(), file_size_(0), modification_time_(0),
    file_format_(iceberg::DataFileFormat::INVALID), delete_files_(allocator)
  {}
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator, const ObIOptLakeTableFile &opt_table_file);
  VIRTUAL_TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time), K_(delete_files));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  iceberg::DataFileFormat file_format_;
  common::ObFixedArray<ObLakeDeleteFile, ObIAllocator> delete_files_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIcebergFile);
  int assign(const ObIcebergFile &other);
};

struct ObHiveFile : public ObILakeTableFile
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObHiveFile(ObIAllocator &allocator)
  : ObILakeTableFile(allocator, LakeFileType::HIVE),
    file_url_(), file_size_(0), modification_time_(0), part_id_(OB_INVALID_PARTITION_ID)
  {}
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator, const ObIOptLakeTableFile &opt_table_file);
  VIRTUAL_TO_STRING_KV(K_(type), K_(file_url), K_(file_size), K_(modification_time), K_(part_id));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  int64_t part_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHiveFile);
  int assign(const ObHiveFile &other);
};

}
}
#endif
