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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_DATA_GENERATOR_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_DATA_GENERATOR_H_

#include <cstdint>
#include <functional>
#include "lib/net/ob_addr.h"                        // ObAddr
#include "lib/string/ob_string.h"                   // ObString
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"             // print
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#include "logservice/restoreservice/ob_log_restore_define.h"
#include "share/backup/ob_backup_struct.h"
#include "share/ob_ls_id.h"                         // ObLSID
#include "logservice/palf/log_iterator_storage.h"   // MemoryStorage
#include "logservice/palf/palf_iterator.h"          // MemPalfGroupBufferIterator
#include "logservice/palf/lsn.h"                    // LSN
#include "ob_remote_log_source.h"                   // Parent
#include "ob_log_restore_rpc_define.h"              // ObRemoteFetchLogResponse

namespace oceanbase
{
namespace palf
{
class LogGroupEntry;
}
namespace logservice
{
using oceanbase::palf::LSN;
using oceanbase::palf::LogGroupEntry;
using oceanbase::share::ObLSID;
struct RemoteDataBuffer
{
  char *data_;
  int64_t data_len_;
  LSN start_lsn_;
  LSN cur_lsn_;
  LSN end_lsn_;
  palf::MemoryStorage mem_storage_;
  palf::MemPalfGroupBufferIterator iter_;

  RemoteDataBuffer() { reset(); }
  ~RemoteDataBuffer() { reset(); }
  int set(const LSN &start_lsn, char *data, const int64_t data_len);
  void reset();
  bool is_valid() const;
  bool is_empty() const;
  int next(LogGroupEntry &entry, LSN &lsn, char *&buf, int64_t &buf_size);

private:
  int set_iterator_();

private:
  DISALLOW_COPY_AND_ASSIGN(RemoteDataBuffer);
};

class RemoteDataGenerator
{
public:
  RemoteDataGenerator(const uint64_t tenant_id,
      const ObLSID &id,
      const LSN &start_lsn,
      const LSN &end_lsn,
      const int64_t end_log_ts);
  virtual ~RemoteDataGenerator();

public:
  virtual int next_buffer(RemoteDataBuffer &buffer) = 0;
  int update_max_lsn(const palf::LSN &lsn);
  bool is_valid() const;
  bool is_fetch_to_end() const;
  TO_STRING_KV(K_(tenant_id), K_(id), K_(start_lsn), K_(next_fetch_lsn), K_(end_log_ts),
      K_(end_lsn), K_(to_end), K_(max_consumed_lsn));

protected:
  int process_origin_data_(char *origin_buf, const int64_t origin_buf_size, char *buf, int64_t &buf_size);

protected:
  uint64_t tenant_id_;
  ObLSID id_;
  LSN start_lsn_;
  LSN next_fetch_lsn_;
  int64_t end_log_ts_;
  LSN end_lsn_;
  bool to_end_;
  LSN max_consumed_lsn_;

private:
  DISALLOW_COPY_AND_ASSIGN(RemoteDataGenerator);
};

class ServiceDataGenerator : public RemoteDataGenerator
{
public:
  ServiceDataGenerator(const uint64_t tenant_id,
      const ObLSID &id,
      const LSN &start_lsn,
      const LSN &end_lsn,
      const int64_t end_log_ts,
      const ObAddr &server);
  virtual ~ServiceDataGenerator();

public:
  int next_buffer(RemoteDataBuffer &buffer);
  INHERIT_TO_STRING_KV("RemoteDataGenerator", RemoteDataGenerator, K_(server), K_(result));

private:
  bool is_valid() const;
  int fetch_log_from_net_();

private:
  common::ObAddr server_;
  obrpc::ObRemoteFetchLogResponse result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ServiceDataGenerator);
};

class LocationDataGenerator : public RemoteDataGenerator
{
  // TODO 考虑物理恢复日志流并发拉日志消费，应该使用64M以及整数倍, 暂时不考虑
  static const int64_t MAX_DATA_BUF_LEN = 128 * 1024 * 1024L;   // 128M
public:
  LocationDataGenerator(const uint64_t tenant_id,
      const int64_t pre_log_ts,
      const ObLSID &id,
      const LSN &start_lsn,
      const LSN &end_lsn,
      const int64_t end_log_ts,
      share::ObBackupDest *dest,
      ObLogArchivePieceContext *piece_context);
  ~LocationDataGenerator();
  int next_buffer(RemoteDataBuffer &buffer);
  int update_max_lsn(const palf::LSN &lsn);
  INHERIT_TO_STRING_KV("RemoteDataGenerator", RemoteDataGenerator,
      K_(pre_log_ts), K_(base_lsn), K_(data_len), KPC_(dest), KPC_(piece_context),
      K_(dest_id), K_(round_id), K_(piece_id), K_(max_file_id), K_(max_file_offset));

private:
  bool is_valid() const;
  int fetch_log_from_location_(char *&buf, int64_t &siz);
  int get_precise_file_and_offset_(
      int64_t &file_id,
      int64_t &file_offset,
      palf::LSN &lsn,
      share::ObBackupPath &piece_path);
private:
  int64_t pre_log_ts_;
  palf::LSN base_lsn_;
  int64_t data_len_;
  char data_[MAX_DATA_BUF_LEN];
  share::ObBackupDest *dest_;
  ObLogArchivePieceContext *piece_context_;

  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  // 读取该归档文件信息
  int64_t max_file_id_;
  int64_t max_file_offset_;

private:
  DISALLOW_COPY_AND_ASSIGN(LocationDataGenerator);
};

class RawPathDataGenerator : public RemoteDataGenerator
{
public:
  // TODO 考虑物理恢复日志流并发拉日志消费，应该使用64M以及整数倍, 暂时不考虑
  static const int64_t MAX_DATA_BUF_LEN = 128 * 1024 * 1024L;   // 128M
  RawPathDataGenerator(const uint64_t tenant_id,
      const ObLSID &id,
      const LSN &start_lsn,
      const LSN &end_lsn,
      const DirArray &array,
      const int64_t end_log_ts,
      const int64_t piece_index,
      const int64_t min_file_id,
      const int64_t max_file_id);

  virtual ~RawPathDataGenerator();
  int next_buffer(RemoteDataBuffer &buffer);

  INHERIT_TO_STRING_KV("RemoteDataGenerator", RemoteDataGenerator, K_(array), K_(data_len),
      K_(file_id), K_(base_lsn), K_(index), K_(min_file_id), K_(max_file_id));

private:
  int fetch_log_from_dest_();
  int cal_lsn_to_file_id_(const LSN &lsn);
  int list_dir_files_(const ObString &uri, const share::ObBackupStorageInfo *storage_info,
      int64_t &min_file_id, int64_t &max_file_id);
  int read_file_(const ObString &prefix, const share::ObBackupStorageInfo *storage_info, const int64_t file_id);
  int extract_archive_file_header_();
  int locate_precise_piece_();
  bool piece_index_match_() const;

private:
  DirArray array_;
  int64_t data_len_;
  char data_[MAX_DATA_BUF_LEN];

  int64_t file_id_;
  LSN base_lsn_;

  int64_t index_;
  int64_t min_file_id_;
  int64_t max_file_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(RawPathDataGenerator);
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_ITERATOR_H_ */
