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

#ifndef OCEANBASE_TOOL_LOG_ENTRY_PARSER
#define OCEANBASE_TOOL_LOG_ENTRY_PARSER

#include "ob_func_utils.h"
#include "ob_log_entry_filter.h"
#include "clog/ob_log_block.h"
#include "clog/ob_raw_entry_iterator.h"
#include "lib/net/ob_addr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "rpc/obrpc/ob_net_client.h"

namespace oceanbase {

namespace share {
class ObKmsClient;
}
namespace clog {
struct ObLogStat {
  ObLogStat()
      : primary_table_id_(OB_INVALID_ID),
        data_block_header_size_(0),
        log_header_size_(0),
        log_size_(0),
        trans_log_size_(0),
        mutator_size_(0),
        padding_size_(0),
        new_row_size_(0),
        old_row_size_(0),
        new_primary_row_size_(0),
        primary_row_count_(0),
        total_row_count_(0),
        total_log_count_(0),
        dist_trans_count_(0),
        sp_trans_count_(0),
        non_compressed_log_cnt_(0),
        compressed_log_cnt_(0),
        compressed_log_size_(0),
        original_log_size_(0),
        compressed_tenant_ids_()
  {}
  ~ObLogStat()
  {}
  int init();
  uint64_t primary_table_id_;
  int64_t data_block_header_size_;
  int64_t log_header_size_;
  int64_t log_size_;
  int64_t trans_log_size_;
  int64_t mutator_size_;
  int64_t padding_size_;
  int64_t new_row_size_;
  int64_t old_row_size_;
  int64_t new_primary_row_size_;
  int64_t primary_row_count_;
  int64_t total_row_count_;
  int64_t total_log_count_;
  int64_t dist_trans_count_;
  int64_t sp_trans_count_;
  // compressed info
  int64_t non_compressed_log_cnt_;  // number of uncompressed entries`
  int64_t compressed_log_cnt_;      // number of compressed entries
  int64_t compressed_log_size_;     // data size of compressed entries
  int64_t original_log_size_;       // data size of compressed entries before compression
  hash::ObHashSet<uint64_t> compressed_tenant_ids_;
  TO_STRING_KV(K_(data_block_header_size), K_(log_header_size), K_(log_size), K_(trans_log_size), K_(mutator_size),
      K_(padding_size), K_(new_row_size), K_(old_row_size), K_(new_primary_row_size), K_(primary_row_count),
      K_(total_row_count), K_(total_log_count), K_(dist_trans_count), K_(sp_trans_count), K_(non_compressed_log_cnt),
      K_(compressed_log_cnt), K_(compressed_log_size), K_(original_log_size), K_(compressed_tenant_ids));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogStat);
};

class ObInfoEntryDumpFunctor {
public:
  ObInfoEntryDumpFunctor(const uint64_t file_id) : file_id_(file_id)
  {}
  virtual ~ObInfoEntryDumpFunctor()
  {}
  bool operator()(const common::ObPartitionKey& partition_key, const uint64_t min_log_id);

private:
  uint64_t file_id_;
};

enum ObLogFileType {
  OB_UNKNOWN_FILE_TYPE,
  OB_CLOG_FILE_TYPE,
  OB_ILOG_FILE_TYPE,
  OB_MAX_FILE_TYPE,
};

class ObLogEntryParserImpl {
public:
  ObLogEntryParserImpl()
      : is_inited_(false),
        dump_hex_(false),
        file_id_(-1),
        cur_offset_(OB_INVALID_OFFSET),
        print_buf_(NULL),
        allocator_(ObModIds::OB_LOG_TOOL)
  {}
  virtual ~ObLogEntryParserImpl()
  {}
  int init(const int64_t file_id, const ObLogEntryFilter& filter, const common::ObString& host, const int32_t port,
      const char* config_file);
  bool is_inited() const
  {
    return is_inited_;
  };
  int dump_clog_entry(ObLogEntry& entry, int64_t pos);

protected:
  // parse mutator data
  int dump_memtable_mutator(const char* buf, int64_t len);
  // parse trans log entry
  int dump_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_sp_trans_abort_log(const char* data, int64_t len);
  int dump_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_prepare_log(const char* data, int64_t len);
  int dump_trans_commit_log(const char* data, int64_t len);
  int dump_trans_abort_log(const char* data, int64_t len);
  int dump_trans_clear_log(const char* data, int64_t len);
  int dump_trans_prepare_commit_log(const char* data, int64_t len);
  int dump_trans_redo_prepare_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_redo_prepare_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_prepare_commit_clear_log(const char* data, int64_t len);
  int dump_trans_redo_prepare_commit_clear_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_mutator_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_mutator_state_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int dump_trans_state_log(const char* data, int64_t len);
  int dump_trans_mutator_abort_log(const char* data, int64_t len);
  int dump_part_split_src_log(const char* data, int64_t len);
  int dump_part_split_dest_log(const char* data, int64_t len);
  int dump_trans_checkpoint_log(const char* data, int64_t len);
  int dump_new_offline_partition_log(const char* data, int64_t len);
  int dump_add_partition_to_pg_log(const char* data, int64_t len);
  int dump_remove_partition_from_pg_log(const char* data, int64_t len);
  int dump_trans_log(const ObStorageLogType log_type, const int64_t trans_inc, const uint64_t real_tenant_id,
      const char* buf, const int64_t buf_len, int64_t& pos);
  int dump_partition_schema_version_change_log(const char* data, int64_t len);

  // parse freeze log
  int dump_freeze_log(const char* buf, const int64_t buf_len, ObStorageLogType& log_type, ObFreezeType& freeze_type,
      ObPartitionKey& pkey, int64_t& frozen_version, ObSavedStorageInfo& info);
  int dump_obj(const common::ObObj& obj, uint64_t column_id);

  int format_dump_clog_entry(ObLogEntry& entry);
  int format_dump_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_trans_redo_prepare_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_trans_redo_prepare_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_trans_redo_prepare_commit_clear_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int format_dump_memtable_mutator(const char* buf, int64_t len);
  int format_dump_obj(const common::ObObj& obj, uint64_t column_id);
  int check_filter(const ObLogEntry& entry, bool& need_print);

private:
  int dump_clog_entry_(ObLogEntry& entry, int64_t pos);

protected:
  static const int64_t MAGIC_NUM_LEN = 2L;
  static const int64_t SKIP_STEP = 4 * 1024L;
  static const int64_t PRINT_BUF_SIZE = 5 * 1024 * 1024;
  bool is_inited_;
  bool dump_hex_;
  uint64_t file_id_;
  int64_t cur_offset_;
  ObTransID cur_trans_id_;
  char* print_buf_;
  ObLogEntryFilter filter_;
  ObArenaAllocator allocator_;
  common::ObAddr host_addr_;
  obrpc::ObNetClient client_;
  obrpc::ObSrvRpcProxy rpc_proxy_;
};

class ObLogEntryParser : public ObLogEntryParserImpl {
public:
  ObLogEntryParser()
      : ObLogEntryParserImpl(), log_file_type_(OB_UNKNOWN_FILE_TYPE), buf_cur_(NULL), buf_end_(NULL), is_ofs_(false)
  {}
  virtual ~ObLogEntryParser()
  {}

  int init(uint64_t file_id, char* buf, int64_t buf_len, const ObLogEntryFilter& filter, const common::ObString& host,
      const int32_t port, const char* config_file, const bool is_ofs);
  int dump_all_entry(bool is_hex);
  int format_dump_entry();
  int stat_log();
  const ObLogStat& get_log_stat() const
  {
    return log_stat_;
  }

protected:
  int get_type_(ObCLogItemType& item_type);
  int dump_block_(const ObLogBlockMetaV2& meta);
  inline void advance_(const int64_t step);
  int advance_to_next_align_();

  int parse_next_entry();
  int format_dump_next_entry();
  int stat_next_entry();

  // for format_dump
  int skip_block_(const ObLogBlockMetaV2& meta);
  int dump_ilog_entry(ObIndexEntry& entry);

  // for stat clog file
  int stat_block_(const ObLogBlockMetaV2& meta);
  int stat_clog_entry(const ObLogEntry& entry, const int64_t pos, const bool is_compressed);
  int stat_sp_trans_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_sp_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_trans_redo_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_trans_redo_prepare_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_trans_redo_prepare_commit_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_trans_redo_prepare_commit_clear_log(const char* data, int64_t len, const uint64_t real_tenant_id);
  int stat_memtable_mutator(const char* buf, int64_t len);

protected:
  static const int64_t MAGIC_NUM_LEN = 2L;
  static const int64_t SKIP_STEP = 4 * 1024L;
  static const int64_t PRINT_BUF_SIZE = 5 * 1024 * 1024;

  ObLogFileType log_file_type_;
  char* buf_cur_;
  char* buf_end_;
  bool is_ofs_;
  ObLogStat log_stat_;
  ObReadBuf compress_rbuf_;

  DISALLOW_COPY_AND_ASSIGN(ObLogEntryParser);
};
}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_RAW_ENTRY_ITERATOR_
