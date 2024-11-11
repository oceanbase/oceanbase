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

#pragma once
#include "observer/table_load/backup/v_3_x/ob_table_load_backup_sstable_block_scanner.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/ob_define.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup_v_3_x
{

class ObTableLoadBackupPartScanner : public ObNewRowIterator
{
  static const int64_t MIN_SUBPART_MACRO_BLOCK_COUNT = 16;
  static const int64_t SSTABLE_BLOCK_BUF_SIZE = 2 * 1024 * 1024 + 256 * 1024;
public:
  ObTableLoadBackupPartScanner();
  virtual ~ObTableLoadBackupPartScanner() {}
  int init(
      const ObSchemaInfo &schema_info,
      const share::ObBackupStorageInfo &storage_info,
      const ObString &data_path,
      const ObString &backup_set_id,
      int64_t backup_table_id,
      int64_t subpart_count,
      int64_t subpart_idx);
  void reset() override;
  int get_next_row(ObNewRow *&row) override;
  TO_STRING_KV(
      K_(storage_info),
      K_(data_path),
      K_(backup_set_id),
      K_(macro_block_index),
      K_(cur_idx),
      K_(is_inited));
private:
  int parse_macro_block_index_file(
      const int64_t subpart_count,
      const int64_t subpart_idx);
  int init_macro_block_index(
      blocksstable::ObBufferReader &buffer_reader,
      const int64_t subpart_count,
      const int64_t subpart_idx);
  int locate_subpart_macro_block(
      const int64_t total_macro_block_count,
      const int64_t subpart_count,
      const int64_t subpart_idx,
      int64_t &start_block_idx,
      int64_t &end_block_idx);
  int switch_next_macro_block();
  int read_lob_data(common::ObNewRow *&row);
  int fill_lob_buf(
      const ObBackupLobIndex &lob_index,
      char *&buf,
      const int64_t buf_size,
      int64_t &pos);
  int read_macro_block_data(
      int64_t block_idx,
      char *&data_buf,
      int64_t &read_size);
private:
  enum ObTableType
  {
    MEMTABLE = 0,
    MAJOR_SSTABLE = 1,
    MINOR_SSTABLE = 2, // obsoleted type after 2.2
    TRANS_SSTABLE = 3, // new table type from 3.1
    MULTI_VERSION_MINOR_SSTABLE = 4,
    COMPLEMENT_MINOR_SSTABLE  = 5, // new table type from 3.1
    MULTI_VERSION_SPARSE_MINOR_SSTABLE = 6, // reserved table type
    MINI_MINOR_SSTABLE = 7,
    BUF_MINOR_SSTABLE = 8,
    MAX_TABLE_TYPE
  };
  class ObLogTsRange
  {
    static const int64_t LOG_TS_RANGE_VERSION = 1;
    OB_UNIS_VERSION(LOG_TS_RANGE_VERSION);
  public:
    ObLogTsRange();
    ~ObLogTsRange() = default;
    bool is_valid() const;
    void reset();
    TO_STRING_KV(
        K_(start_log_ts),
        K_(end_log_ts),
        K_(max_log_ts));
  public:
    int64_t start_log_ts_;
    int64_t end_log_ts_;
    int64_t max_log_ts_;
};
  class ObPartitionKey
  {
  public:
    ObPartitionKey();
    ~ObPartitionKey() = default;
    bool is_valid() const;
    void reset();
    NEED_SERIALIZE_AND_DESERIALIZE;
    TO_STRING_KV(
        K_(table_id),
        K_(part_id),
        K_(subpart_id));
  public:
    //After version 2.0, reuse partition key to represent partition group key
    //Use table_id to represent table_group_id, the encoding method of table_id and table_group_id has changed,
    //The highest bit of the 40 bits is 1, and this bit is used to identify PG and Partition
    //The encoding method of partition_group_id is consistent with partition_id
    union
    {
      uint64_t table_id_;     //Indicates the ID of the table
      uint64_t rg_id_;
      uint64_t tablegroup_id_;
    };
    int32_t part_id_; //First level part_id
    union {
      int32_t subpart_id_;  //Secondary part_id
      int32_t part_cnt_;  //Part_cnt is recorded in the first-level partition
      int32_t assit_id_;  //Assistance information, when the highest bit is 1, it means the subpart_id of the secondary partition,
                          //The highest bit is 0, indicating that the key is the first-level partition part_cnt
    };
  };
  class ObTableKey
  {
    static const int64_t TABLE_KEY_VERSION = 1;
    OB_UNIS_VERSION(TABLE_KEY_VERSION);
  public:
    ObTableKey();
    ~ObTableKey() = default;
    void reset();
    bool is_valid() const;
    TO_STRING_KV(
        K_(table_type),
        K_(pkey),
        K_(table_id),
        K_(trans_version_range),
        K_(log_ts_range),
        K_(version));
  public:
    ObTableType table_type_;
    ObPartitionKey pkey_;
    uint64_t table_id_;
    common::ObVersionRange trans_version_range_;
    common::ObVersion version_;// only used for major merge
    ObLogTsRange log_ts_range_;
  };
  class ObTableKeyInfo
  {
    static const int64_t TABLE_KEY_INFO_VERSION = 1;
    OB_UNIS_VERSION(TABLE_KEY_INFO_VERSION);
  public:
    ObTableKeyInfo();
    ~ObTableKeyInfo() = default;
    bool is_valid() const;
    void reset();
    TO_STRING_KV(
        K_(table_key),
        K_(total_macro_block_count));
  public:
    ObTableKey table_key_;
    int64_t total_macro_block_count_;
  };
  class ObTableMacroIndex
  {
    static const int64_t TABLE_MACRO_INDEX_VERSION = 1;
    OB_UNIS_VERSION(TABLE_MACRO_INDEX_VERSION);
  public:
    ObTableMacroIndex();
    ~ObTableMacroIndex() = default;
    void reset();
    bool is_valid() const;
    TO_STRING_KV(
        K_(sstable_macro_index),
        K_(data_version),
        K_(data_seq),
        K_(backup_set_id),
        K_(sub_task_id),
        K_(offset),
        K_(data_length));
  public:
    int64_t sstable_macro_index_;
    int64_t data_version_;
    int64_t data_seq_;
    int64_t backup_set_id_;
    int64_t sub_task_id_;
    int64_t offset_;
    int64_t data_length_; //=ObBackupDataHeader(header_length_+macro_meta_length_ + macro_data_length_)
  };
private:
  ObArenaAllocator allocator_;
  ObSchemaInfo schema_info_;
  share::ObBackupStorageInfo storage_info_;
  ObString data_path_;
  ObString backup_set_id_;
  int64_t backup_table_id_;
  ObArray<ObTableMacroIndex> macro_block_index_;
  common::hash::ObHashMap<int64_t, int64_t> macro_block_idx_map_;
  // 备份文件先存储数据宏块再存储lob宏块，遍历宏块下标到第一个lob宏块下标时，可以提前结束迭代
  int64_t macro_block_min_skip_idx_;
  ObArray<char *> lob_col_buf_;
  ObArray<int64_t> lob_col_buf_size_;
  char *buf_;
  char *lob_buf_;
  int64_t cur_idx_;
  ObTableLoadBackupSSTableBlockScanner sstable_block_scanner_;
  bool is_inited_;
};

} // table_load_backup_v_3_x
} // namespace observer
} // namespace oceanbase
