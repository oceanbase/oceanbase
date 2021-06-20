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

#ifndef OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OSS_READER_H_
#define OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OSS_READER_H_

#include "share/ob_define.h"
#include "lib/restore/ob_storage_path.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "storage/ob_i_partition_base_data_reader.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace storage {
class ObPartitionMetaStorageReader {
public:
  ObPartitionMetaStorageReader();
  virtual ~ObPartitionMetaStorageReader();

  int init(const share::ObRestoreArgs& arg, const ObPartitionKey& pkey);
  int read_partition_meta(ObPGPartitionStoreMeta& partition_store_meta);
  int read_sstable_pair_list(const uint64_t index_tid, common::ObIArray<blocksstable::ObSSTablePair>& pair_list);
  int read_sstable_meta(const uint64_t backup_index, blocksstable::ObSSTableBaseMeta& sstable_meta);
  int64_t get_sstable_count() const
  {
    return sstable_meta_array_.count();
  }
  int64_t get_data_size() const
  {
    return data_size_;
  }
  int read_table_ids(common::ObIArray<uint64_t>& table_id_array);
  int read_table_keys_by_table_id(const uint64_t table_id, ObIArray<ObITable::TableKey>& table_keys_array);
  bool is_inited() const
  {
    return is_inited_;
  }

private:
  // int read_one_file(const common::ObStoragePath &path, const share::ObRestoreArgs &args,
  //    ObIAllocator &allocator, char *&buf, int64_t &read_size);
  int read_all_sstable_meta();
  int read_table_keys();

private:
  bool is_inited_;
  int64_t sstable_index_;
  int64_t data_size_;
  common::ObArray<blocksstable::ObSSTableBaseMeta> sstable_meta_array_;
  common::ObArray<ObITable::TableKey> table_keys_array_;
  const share::ObRestoreArgs* args_;
  common::ObArenaAllocator meta_allocator_;
  int64_t table_count_;
  ObPartitionKey pkey_;

  DISALLOW_COPY_AND_ASSIGN(ObPartitionMetaStorageReader);
};

class ObMacroBlockStorageReader : public ObDynamicThreadTask {
  const static uint64_t DEFAULT_WAIT_TIME = 10 * 1000 * 1000;  // 10s
public:
  ObMacroBlockStorageReader();
  virtual ~ObMacroBlockStorageReader();

  int init(common::ObInOutBandwidthThrottle& bandwidth_throttle, const share::ObRestoreArgs& args,
      const ObPartitionKey& pkey, const uint64_t backup_index_tid, const blocksstable::ObSSTablePair& backup_pair);

  virtual int process(const bool& is_stop) override;
  int get_macro_block_meta(blocksstable::ObMacroBlockMeta*& meta, blocksstable::ObBufferReader& data);
  int64_t get_data_size() const
  {
    return data_size_;
  }
  void set_scheduled()
  {
    is_scheduled_ = true;
  }
  void reset();
  TO_STRING_KV(K_(is_inited), K_(args), K_(data_size), K_(result_code), K_(is_data_ready), K_(backup_pair),
      K_(backup_index_tid), KP_(meta), K_(data), K_(init_ts), K_(finish_ts), "limit time", finish_ts_ - init_ts_);

private:
  int wait_finish();

private:
  bool is_inited_;
  const share::ObRestoreArgs* args_;
  common::ObArenaAllocator allocator_;
  int64_t data_size_;

  int32_t result_code_;
  bool is_data_ready_;
  blocksstable::ObSSTablePair backup_pair_;
  uint64_t backup_index_tid_;
  blocksstable::ObMacroBlockMeta* meta_;
  blocksstable::ObBufferReader data_;
  ObThreadCond cond_;
  common::ObInOutBandwidthThrottle* bandwidth_throttle_;
  int64_t init_ts_;
  int64_t finish_ts_;
  bool is_scheduled_;
  ObPartitionKey pkey_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockStorageReader);
};

class ObPartitionGroupMetaReader {
public:
  ObPartitionGroupMetaReader();
  virtual ~ObPartitionGroupMetaReader()
  {}

  int init(const share::ObRestoreArgs& arg);
  int read_partition_group_meta(ObPartitionGroupMeta& pg_meta);
  int64_t get_data_size() const
  {
    return data_size_;
  }
  bool is_inited() const
  {
    return is_inited_;
  }

private:
  int read_one_file(const common::ObStoragePath& path, const share::ObRestoreArgs& args, ObIAllocator& allocator,
      char*& buf, int64_t& read_size);

private:
  bool is_inited_;
  int64_t data_size_;
  const share::ObRestoreArgs* args_;
  common::ObArenaAllocator meta_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionGroupMetaReader);
};

class ObOssReaderUtil {
public:
  static int read_one_file(const common::ObStoragePath& path, const share::ObRestoreArgs& args, ObIAllocator& allocator,
      char*& buf, int64_t& read_size);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif /* OCEANBASE_STORAGE_OB_PARTITION_BASE_DATA_OSS_READER_H_ */
