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

#ifndef _SHARE_OB_ALL_VIRTUAL_PG_LOG_ARCHIVE_STAT_H_
#define _SHARE_OB_ALL_VIRTUAL_PG_LOG_ARCHIVE_STAT_H_
#include "share/ob_virtual_table_iterator.h"
#include "lib/hash/ob_link_hashmap.h"  // Iterator
#include "clog/ob_log_define.h"
#include "archive/ob_archive_pg_mgr.h"

namespace oceanbase {
namespace observer {
struct PGLogArchiveStat {
public:
  PGLogArchiveStat()
  {
    reset();
  }
  ~PGLogArchiveStat()
  {
    reset();
  }
  void reset();
  ObPGKey pg_key_;
  int64_t incarnation_;
  int64_t round_;
  int64_t epoch_;
  bool been_deleted_;
  bool is_first_record_finish_;
  bool has_encount_error_;
  clog::file_id_t current_ilog_id_;
  uint64_t max_log_id_;
  uint64_t round_start_log_id_;
  int64_t round_start_log_ts_;
  int64_t round_snapshot_version_;
  uint64_t cur_start_log_id_;
  uint64_t fetcher_max_split_log_id_;
  uint64_t clog_max_split_log_id_;
  int64_t clog_max_split_log_ts_;
  int64_t clog_split_checkpoint_ts_;
  uint64_t max_archived_log_id_;
  int64_t max_archived_log_ts_;
  int64_t max_archived_checkpoint_ts_;
  int64_t archived_clog_epoch_;
  int64_t archived_accum_checksum_;
  uint64_t cur_index_file_id_;
  int64_t index_file_offset_;
  uint64_t cur_data_file_id_;
  int64_t data_file_offset_;
};

class ObAllVirtualPGLogArchiveStat : public common::ObVirtualTableIterator {
  // friend class PGArchiveMap;
  enum PG_LOG_ARCHIVE_STAT_COLUMN {
    SVR_IP = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_ID,
    INCARNATION,
    LOG_ARCHIVE_ROUND,
    EPOCH,
    BEEN_DELETE_FLAG,
    FIRST_RECORD_FINISH_FLAG,
    ENCOUNTER_ERROR_FLAG,
    CUR_ILOG_ID,
    MAX_LOG_ID,
    ROUND_START_LOG_ID,
    ROUND_START_LOG_TS,
    ROUND_SNAPSHOT_VERSION,
    CUR_EPOCH_START_LOG_ID,
    FETCHER_MAX_SPLIT_LOG_ID,
    CLOG_MAX_SPLIT_LOG_ID,
    CLOG_MAX_SPLIT_LOG_TS,
    CLOG_SPLIT_CHECKPOINT_TS,
    MAX_ARCHIVED_LOG_ID,
    MAX_ARCHIVED_LOG_TS,
    MAX_ARCHIVED_CHECKPOINT_TS,
    ARCHIVED_CLOG_EPOCH,
    ARCHIVED_ACCUM_CHECKSUM,
    CUR_INDEX_FILE_ID,
    CUR_INDEX_FILE_OFFSET,
    CUR_DATA_FILE_ID,
    CUR_DATA_FILE_OFFSET,
  };

public:
  ObAllVirtualPGLogArchiveStat();
  virtual ~ObAllVirtualPGLogArchiveStat();

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  void reset();
  int init(storage::ObPartitionService* partition_service, common::ObAddr& addr);

private:
  int get_log_archive_stat_(PGLogArchiveStat& stat);
  int get_iter_();

private:
  bool is_inited_;
  storage::ObPartitionService* ps_;
  common::ObAddr addr_;
  archive::PGArchiveMap::Iterator* iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
};
}  // namespace observer
}  // namespace oceanbase
#endif  //_SHARE_OB_ALL_VIRTUAL_PG_LOG_ARCHIVE_STAT_H_
