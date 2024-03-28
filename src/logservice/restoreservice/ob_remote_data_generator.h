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
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
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
#include "share/scn.h"                    // SCN
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
class ObLogExternalStorageHandler;
using oceanbase::palf::LSN;
using oceanbase::palf::LogGroupEntry;
using oceanbase::share::ObLSID;
template<class LogEntryType>
struct RemoteDataBuffer
{
  char *data_;
  int64_t data_len_;
  LSN start_lsn_;
  LSN cur_lsn_;
  LSN end_lsn_;
  palf::MemoryStorage mem_storage_;
  palf::PalfIterator<palf::MemoryIteratorStorage, LogEntryType> iter_;

  RemoteDataBuffer() { reset(); }
  ~RemoteDataBuffer() { reset(); }
  int set(const LSN &start_lsn, char *data, const int64_t data_len)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(! start_lsn.is_valid() || NULL == data || data_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      data_ = data;
      data_len_ = data_len;
      start_lsn_ = start_lsn;
      cur_lsn_ = start_lsn;
      end_lsn_ = start_lsn + data_len;
      ret = set_iterator_();
    }
    return ret;
  }
  void reset()
  {
    data_ = NULL;
    data_len_ = 0;
    start_lsn_.reset();
    cur_lsn_.reset();
    end_lsn_.reset();
  }
  bool is_valid() const
  {
    return start_lsn_.is_valid()
      && cur_lsn_.is_valid()
      && end_lsn_.is_valid()
      && end_lsn_ > start_lsn_
      && NULL != data_
      && data_len_ > 0;
  }
  bool is_empty() const
  {
    return cur_lsn_ == end_lsn_;
  }
  int next(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size)
  {
    int ret = OB_SUCCESS;
    if (is_empty()) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(iter_.next())) {
      CLOG_LOG(WARN, "next failed", K(ret));
    } else if (OB_FAIL(iter_.get_entry(buf, entry, lsn))) {
      CLOG_LOG(WARN, "get_entry failed", K(ret));
    } else {
      // 当前返回entry对应buff和长度
      buf_size = entry.get_serialize_size();
      cur_lsn_ = lsn + buf_size;
    }
    return ret;
  }

  TO_STRING_KV(K_(data), K_(data_len), K_(start_lsn), K_(cur_lsn), K_(end_lsn), K_(mem_storage), K_(iter));

private:
  int set_iterator_()
  {
    mem_storage_.destroy();
    iter_.destroy();
    int ret = OB_SUCCESS;
    auto get_file_size = [&]() -> LSN { return end_lsn_;};
    if (OB_FAIL(mem_storage_.init(start_lsn_))) {
      CLOG_LOG(WARN, "MemoryStorage init failed", K(ret), K(start_lsn_));
    } else if (OB_FAIL(mem_storage_.append(data_, data_len_))) {
      CLOG_LOG(WARN, "MemoryStorage append failed", K(ret));
    } else if (OB_FAIL(iter_.init(start_lsn_, get_file_size, &mem_storage_))) {
      CLOG_LOG(WARN, "MemPalfGroupBufferIterator init failed", K(ret));
    } else {
      iter_.set_need_print_error(false /*need_print_error*/);
      CLOG_LOG(INFO, "MemPalfGroupBufferIterator init succ", K(start_lsn_), K(end_lsn_));
    }
    return ret;
  }

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
      const share::SCN &end_scn,
      logservice::ObLogExternalStorageHandler *log_ext_handler);
  virtual ~RemoteDataGenerator();

public:
  virtual int next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size) = 0;
  virtual int update_max_lsn(const palf::LSN &lsn) = 0;
  virtual int advance_step_lsn(const palf::LSN &lsn) = 0;
  bool is_valid() const;
  bool is_fetch_to_end() const;
  TO_STRING_KV(K_(tenant_id), K_(id), K_(start_lsn), K_(next_fetch_lsn), K_(end_scn),
      K_(end_lsn), K_(to_end));

protected:
  int process_origin_data_(char *origin_buf, const int64_t origin_buf_size, char *buf, int64_t &buf_size);
  int update_next_fetch_lsn_(const palf::LSN &lsn);
  int read_file_(const ObString &base,
    const share::ObBackupStorageInfo *storage_info,
    const share::ObLSID &id,
    const int64_t file_id,
    const int64_t offset,
    char *data,
    const int64_t data_len,
    int64_t &data_size);

protected:
  uint64_t tenant_id_;
  ObLSID id_;
  LSN start_lsn_;
  LSN next_fetch_lsn_;
  share::SCN end_scn_;
  LSN end_lsn_;
  bool to_end_;
  logservice::ObLogExternalStorageHandler *log_ext_handler_;

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
      const share::SCN &end_scn,
      const ObAddr &server,
      logservice::ObLogExternalStorageHandler *log_ext_handler);
  virtual ~ServiceDataGenerator();

public:
  int next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size);
  int update_max_lsn(const palf::LSN &lsn) { UNUSED(lsn); return common::OB_SUCCESS; }
  int advance_step_lsn(const palf::LSN &lsn) override { UNUSED(lsn); return common::OB_SUCCESS;}
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
public:
  LocationDataGenerator(const uint64_t tenant_id,
      const share::SCN &pre_scn,
      const ObLSID &id,
      const LSN &start_lsn,
      const LSN &end_lsn,
      const share::SCN &end_scn,
      share::ObBackupDest *dest,
      ObLogArchivePieceContext *piece_context,
      char *buf,
      const int64_t buf_size,
      const int64_t single_read_size,
      logservice::ObLogExternalStorageHandler *log_ext_handler);
  ~LocationDataGenerator();
  int next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size);
  int update_max_lsn(const palf::LSN &lsn);
  int advance_step_lsn(const palf::LSN &lsn) override;
  INHERIT_TO_STRING_KV("RemoteDataGenerator", RemoteDataGenerator,
      K_(pre_scn), K_(base_lsn), K_(data_len), KPC_(dest), KPC_(piece_context),
      K_(cur_file));

private:
  struct FileDesc
  {
    //bool inited_;
    bool is_origin_data_;
    int64_t dest_id_;
    int64_t round_id_;
    int64_t piece_id_;
    int64_t file_id_;
    int64_t base_file_offset_;
    int64_t max_file_offset_;
    int64_t cur_offset_;
    palf::LSN base_lsn_;
    palf::LSN cur_lsn_;

    FileDesc() { reset(); }
    virtual ~FileDesc() { reset(); }
    void reset();
    bool is_valid() const;
    bool match(const int64_t dest_id,
        const int64_t round_id,
        const int64_t piece_id,
        const palf::LSN &lsn) const;
    int set(const bool origin_data,
        const int64_t dest_id,
        const int64_t round_id,
        const int64_t piece_id,
        const int64_t file_id,
        const int64_t base_file_offset,
        const int64_t max_file_offset,
        const palf::LSN &base_lsn);
    int advance(const palf::LSN &cur_lsn);

    TO_STRING_KV(K_(is_origin_data),
        K_(dest_id),
        K_(round_id),
        K_(piece_id),
        K_(file_id),
        K_(base_file_offset),
        K_(max_file_offset),
        K_(cur_offset),
        K_(base_lsn),
        K_(cur_lsn));
  };
private:
  bool is_valid() const;
  int fetch_log_from_location_(char *&buf, int64_t &siz);
  int get_precise_file_and_offset_(int64_t &dest_id,
      int64_t &round_id,
      int64_t &piece_id,
      int64_t &file_id,
      int64_t &file_offset,
      palf::LSN &lsn,
      share::ObBackupPath &piece_path);
  void cal_read_size_(const int64_t dest_id,
      const int64_t round_id,
      const int64_t piece_id,
      const int64_t file_id,
      const int64_t file_offset,
      int64_t &size);

private:
  share::SCN pre_scn_;
  // base_lsn_ is the start_lsn from the archive file, while the next_fetch_lsn_ is the start_lsn to fetch,
  // when the piece context does not match the log to fetch, the two variables are not equal
  palf::LSN base_lsn_;
  int64_t data_len_;
  char *buf_;
  int64_t buf_size_;
  int64_t single_read_size_;
  share::ObBackupDest *dest_;
  ObLogArchivePieceContext *piece_context_;

  FileDesc cur_file_;

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
      ObLogRawPathPieceContext *rawpath_ctx,
      const share::SCN &end_scn,
      logservice::ObLogExternalStorageHandler *log_ext_handler);

  virtual ~RawPathDataGenerator();
  int next_buffer(palf::LSN &lsn, char *&buf, int64_t &buf_size);
  int update_max_lsn(const palf::LSN &lsn);
  int advance_step_lsn(const palf::LSN &lsn) override;

  INHERIT_TO_STRING_KV("RemoteDataGenerator", RemoteDataGenerator, K_(rawpath_ctx), K_(data_len), K_(base_lsn));

private:
  int fetch_log_from_dest_();
  int read_file_(const ObString &prefix, const share::ObBackupStorageInfo *storage_info, const int64_t file_id);
  int extract_archive_file_header_();

private:
  ObLogRawPathPieceContext *rawpath_ctx_;
  int64_t data_len_;
  char data_[MAX_DATA_BUF_LEN];
  LSN base_lsn_;

private:
  DISALLOW_COPY_AND_ASSIGN(RawPathDataGenerator);
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_ITERATOR_H_ */
