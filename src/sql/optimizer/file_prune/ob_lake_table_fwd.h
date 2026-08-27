/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FWD_H
#define _OCEANBASE_SQL_OPTIMIZER_OB_LAKE_TABLE_FWD_H

#include "common/object/ob_object.h"
#include "sql/resolver/ob_sql_array.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{
class ManifestEntry;
}

// 非持有型分区键：直接引用 (spec_id, partition tuple)，避免每次查找都复制分区值。
struct ObLakeTablePartKey
{
public:
  ObLakeTablePartKey();
  int assign(const ObLakeTablePartKey &other);
  void reset();
  int from_manifest_entry(const iceberg::ManifestEntry *manifest_entry);
  int hash(uint64_t &hash_val) const;
  int get_partition_value(const int64_t idx, const common::ObObj *&value) const;
  bool operator== (const ObLakeTablePartKey &other) const;
  TO_STRING_KV(KP_(manifest_entry), K_(hash_value));
  // ManifestEntry 的生命周期覆盖优化阶段的分区 Map，因此这里可以安全保存裸指针。
  const iceberg::ManifestEntry *manifest_entry_;
  uint64_t hash_value_;
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
  int64_t dv_content_offset_ = 0;
  int64_t dv_content_size_in_bytes_ = 0;
  bool is_file_scoped_ = false;
  TO_STRING_KV(K_(type),
               K_(file_url),
               K_(file_size),
               K_(modification_time),
               K_(file_format),
               K_(dv_content_offset),
               K_(dv_content_size_in_bytes),
               K_(is_file_scoped));
};

enum class LakeFileType
{
  INVALID = 0,
  ICEBERG = 1,
  HIVE = 2,
  // A scan task produced by the generic external-table plugin contract
  // (ob_external_table_plugin.h). Payload = the contract task_json string (a
  // single scan-task JSON object, incl. payload_b64). Used by any plugin-backed
  // format — not a format-specific task type. Value 3 reuses the legacy PAIMON
  // enum value so previously serialized plugin scan tasks stay compatible.
  EXT_PLUGIN = 3
};

enum class CsvTaskType{
  INVALID = 0,
  GAMBLING_BOUND = 1,
  FULL_SCAN_BOUND = 2,
  PARSE_DATA = 3
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
  bool is_ext_plugin_file() const { return LakeFileType::EXT_PLUGIN == type_; }
  static int create_opt_lake_table_file_by_type(ObIAllocator &allocator, LakeFileType type, ObIOptLakeTableFile *&file);
  VIRTUAL_TO_STRING_KV(K_(type));
public:
  LakeFileType type_;
};

struct ObOptIcebergFile : public ObIOptLakeTableFile
{
public:
  ObOptIcebergFile(common::ObIAllocator &allocator)
      : ObIOptLakeTableFile(LakeFileType::ICEBERG), file_url_(), file_size_(0),
        modification_time_(0), file_format_(iceberg::DataFileFormat::INVALID),
        delete_files_(allocator), record_count_(0), part_id_(OB_INVALID_INDEX_INT64),
        partition_spec_id_(-1), file_desc_idx_(OB_INVALID_INDEX_INT64)
  {}
  virtual int assign(const ObIOptLakeTableFile &other) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(type),
                       K_(file_url),
                       K_(file_size),
                       K_(modification_time),
                       K_(delete_files),
                       K_(record_count),
                       K_(part_id),
                       K_(partition_spec_id),
                       K_(file_desc_idx));

  ObString file_url_;
  int64_t file_size_;
  int64_t modification_time_;
  iceberg::DataFileFormat file_format_;
  ObSqlArray<const ObLakeDeleteFile *> delete_files_;
  int64_t record_count_;
  int64_t part_id_;
  int32_t partition_spec_id_;
  // 仅用于优化阶段回查 file desc，不会序列化到 ObIcebergScanTask。
  int64_t file_desc_idx_;
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

struct ObOptPluginFile : public ObIOptLakeTableFile
{
public:
  explicit ObOptPluginFile(common::ObIAllocator &allocator)
  : ObIOptLakeTableFile(LakeFileType::EXT_PLUGIN),
    task_json_(),
    record_count_(0),
    allocator_(allocator)
  {}
  virtual int assign(const ObIOptLakeTableFile &other) override;
  virtual void reset() override;
  VIRTUAL_TO_STRING_KV(K_(type), K_(task_json), K_(record_count));

  ObString task_json_;                 // single scan-task JSON text
  int64_t record_count_;
  common::ObIAllocator &allocator_;
};

/* structs for execution */

struct ObCsvParallelInfo
{
public:
  explicit ObCsvParallelInfo()
  : start_pos_(0),
    end_pos_(INT64_MAX),
    chunk_idx_(OB_INVALID_INDEX),
    chunk_cnt_(OB_INVALID_INDEX),
    csv_task_type_(CsvTaskType::INVALID),
    is_gambling_end_with_escaped_(false)
  {}
  ~ObCsvParallelInfo();
  TO_STRING_KV(K_(start_pos), K_(end_pos),
               K_(chunk_idx), K_(chunk_cnt), K_(csv_task_type),
               K_(is_gambling_end_with_escaped));

  int64_t start_pos_;
  int64_t end_pos_;
  int64_t chunk_idx_;
  int64_t chunk_cnt_;
  CsvTaskType csv_task_type_;
  bool is_gambling_end_with_escaped_;  // gambling 阶段结束时的 escape 状态，传递给 full scan
private:
  DISALLOW_COPY_AND_ASSIGN(ObCsvParallelInfo);
  int assign(const ObCsvParallelInfo &other);
};

struct ObIExtTblScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObIExtTblScanTask()
  : file_url_(), part_id_(OB_INVALID_PARTITION_ID), first_lineno_(1), last_lineno_(INT64_MAX)
  {}
  virtual ~ObIExtTblScanTask() {}

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(part_id), K_(first_lineno), K_(last_lineno));

public:
  ObString file_url_; // 在 ObFileScanTask 中序列化
  int64_t part_id_;   // 在具体格式的 scan task 中序列化
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
  virtual ~ObFileScanTask() {}
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
  int64_t record_count_; // 在 ObIcebergScanTask 中序列化
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
      : ObFileScanTask(LakeFileType::ICEBERG), file_format_(iceberg::DataFileFormat::INVALID),
        delete_files_(allocator), partition_spec_id_(-1)
  {
    // Iceberg 查询内稠密分区 ID 从 0 开始，-1 表示尚未生成。
    part_id_ = OB_INVALID_INDEX_INT64;
  }
  virtual ~ObIcebergScanTask() {}
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                           const ObIOptLakeTableFile &opt_table_file) override;

  VIRTUAL_TO_STRING_KV(K_(file_url),
                       K_(type),
                       K_(file_size),
                       K_(modification_time),
                       K_(delete_files),
                       K_(file_id),
                       K_(part_id),
                       K_(content_digest),
                       K_(record_count),
                       K_(partition_spec_id));

  iceberg::DataFileFormat file_format_;
  common::ObFixedArray<ObLakeDeleteFile, ObIAllocator> delete_files_;
  int32_t partition_spec_id_;

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
  : ObFileScanTask(LakeFileType::HIVE),
    parallel_parse_csv_info_(nullptr)
  {}
  virtual ~ObHiveScanTask() {}

  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                           const ObIOptLakeTableFile &opt_table_file) override;

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count),
                      KPC_(parallel_parse_csv_info));
  // for parallel parse csv
  ObCsvParallelInfo *parallel_parse_csv_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHiveScanTask);
  int assign(const ObHiveScanTask &other);
};

/// Execution-side scan task for the generic plugin contract. Carries the
/// contract task_json (the single scan-task JSON text) which the row iterator
/// hands to `reader_create`. Rides in ObVTableScanParam::scan_tasks_ alongside
/// the format-specific task types.
struct ObPluginScanTask : public ObFileScanTask
{
public:
  OB_UNIS_VERSION_V(1);
public:
  explicit ObPluginScanTask()
  : ObFileScanTask(LakeFileType::EXT_PLUGIN),
    task_json_()
  {}
  virtual ~ObPluginScanTask() {}
  virtual int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                            const ObIOptLakeTableFile &opt_table_file) override;

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count),
                      K_(task_json));

  ObString task_json_;  // single scan-task JSON text (carries payload_b64)
private:
  DISALLOW_COPY_AND_ASSIGN(ObPluginScanTask);
  int assign(const ObPluginScanTask &other);
};

struct ObExtTableScanTask : public ObFileScanTask
{
public:
  explicit ObExtTableScanTask()
  : ObFileScanTask(LakeFileType::INVALID),
    parallel_parse_csv_info_(nullptr)
  {}
  virtual ~ObExtTableScanTask() {}

  int init_with_opt_lake_table_file(ObIAllocator &allocator,
                                    const ObIOptLakeTableFile &opt_table_file) override
  { return OB_NOT_SUPPORTED; }

  int init_parallel_parse_csv_info(ObIAllocator &allocator);

  VIRTUAL_TO_STRING_KV(K_(file_url), K_(type), K_(file_size), K_(modification_time),
                      K_(file_id), K_(part_id), K_(content_digest), K_(record_count),
                      KPC_(parallel_parse_csv_info));

  // for parallel parse csv
  ObCsvParallelInfo *parallel_parse_csv_info_;
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
  virtual ~ObOdpsScanTask() {}

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
