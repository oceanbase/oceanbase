// Copyright 2020 OceanBase Inc. All Rights Reserved
// Author:
//     yanfeng <yangyi.yyy@antgroup.com>

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_META_STORE_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_META_STORE_H_

#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_path.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ob_storage_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/backup/ob_partition_backup_struct.h"

namespace oceanbase {
namespace share {

struct ObBackupMeta {
  ObBackupMeta();
  ~ObBackupMeta();

  void reset();
  bool is_valid() const;
  int64_t get_total_partition_count() const
  {
    return partition_meta_list_.count();
  }
  int64_t get_total_macro_block_count() const;
  TO_STRING_KV(K_(partition_meta_list), K_(sstable_meta_list), K_(table_key_list), K_(partition_group_meta));

  common::ObArray<storage::ObPartitionStoreMeta*> partition_meta_list_;
  common::ObArray<blocksstable::ObSSTableBaseMeta> sstable_meta_list_;
  common::ObArray<storage::ObITable::TableKey> table_key_list_;
  storage::ObPartitionGroupMeta partition_group_meta_;
  ObBackupMetaIndex pg_meta_index_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMeta);
};

class ObBackupMetaFileStore final {
  struct MemoryFile {
    MemoryFile()
        : backup_path_(),
          meet_file_end_mark_(false),
          need_parse_common_header_(true),
          end_pos_(0),
          padding_(0),
          reader_(),
          task_id_(0)
    {}
    ~MemoryFile()
    {}
    TO_STRING_KV(K_(backup_path), K_(meet_file_end_mark), K_(need_parse_common_header), K_(end_pos), K_(padding),
        K_(reader), K_(task_id));

    int assign(const MemoryFile& that)
    {
      int ret = OB_SUCCESS;
      const blocksstable::ObBufferReader& that_reader = that.reader_;
      backup_path_ = that.backup_path_;
      meet_file_end_mark_ = that.meet_file_end_mark_;
      need_parse_common_header_ = that.need_parse_common_header_;
      end_pos_ = that.end_pos_;
      padding_ = that.padding_;
      reader_.assign(that_reader.data(), that_reader.capacity(), that_reader.pos());
      task_id_ = that.task_id_;
      return ret;
    }

    ObBackupPath backup_path_;
    bool meet_file_end_mark_;
    bool need_parse_common_header_;  // true first, then set to false
    int64_t end_pos_;
    int64_t padding_;
    blocksstable::ObBufferReader reader_;
    int64_t task_id_;
  };

public:
  ObBackupMetaFileStore();
  ~ObBackupMetaFileStore();
  int init(const share::ObBackupBaseDataPathInfo& path_info);
  int next(ObBackupMeta& backup_meta);

private:
  int init_from_meta_file(const share::ObBackupBaseDataPathInfo& path_info);
  int get_meta_file_list(
      const share::ObBackupBaseDataPathInfo& path_info, common::ObIArray<share::ObBackupPath>& file_name_list);
  int read_meta_file(const common::ObString& file_path, const common::ObString& storage_info, MemoryFile& mem_file);
  int get_next_mem_file(MemoryFile*& mem_file);
  int may_need_parse_common_header(MemoryFile*& mem_file);

private:
  int parse_common_header(blocksstable::ObBufferReader& reader, share::ObBackupCommonHeader& header);
  int parse_sealed_message(MemoryFile*& mem_file, ObBackupMeta& backup_meta, bool& end_of_one_block);
  int parse_pg_meta(blocksstable::ObBufferReader& reader, storage::ObPartitionGroupMeta& pg_meta);
  int parse_partition_meta(blocksstable::ObBufferReader& reader, storage::ObPartitionStoreMeta& partition_meta);
  int parse_sstable_meta(
      blocksstable::ObBufferReader& reader, common::ObIArray<blocksstable::ObSSTableBaseMeta>& sstable_meta_array);
  int parse_table_key_meta(
      blocksstable::ObBufferReader& reader, common::ObIArray<storage::ObITable::TableKey>& table_key_array);
  int get_task_id(const ObString& file_path, int64_t& task_id);
  int parse_pg_meta_info(blocksstable::ObBufferReader& reader, storage::ObBackupPGMetaInfo& pg_meta_info);

private:
  bool is_inited_;
  int64_t pos_;
  MemoryFile cur_mem_file_;
  common::ObArenaAllocator allocator_;
  share::ObBackupBaseDataPathInfo base_path_info_;
  common::ObArray<share::ObBackupPath> meta_file_path_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaFileStore);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
