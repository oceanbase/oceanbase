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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_ITERATOR_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_ITERATOR_H_

#include "lib/net/ob_addr.h"                   // ObAddr
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"        // print
#include "logservice/palf/log_define.h"        // MAX_LOG_BUFFER_SIZE
#include "logservice/palf/palf_iterator.h"     // MemPalfGroupBufferIterator
#include "share/ob_ls_id.h"                    // ObLSID
#include "ob_remote_log_source.h"              // ObRemoteLogParent
#include "logservice/palf/log_group_entry.h"   // LogGroupEntry
#include "logservice/palf/lsn.h"               // LSN
#include "share/scn.h"               // SCN
#include "logservice/palf/log_iterator_storage.h" // MemoryStorage
#include "ob_remote_data_generator.h"          // RemoteDataBuffer
#include "lib/profile/ob_trace_id.h"
#include "lib/utility/ob_tracepoint.h"                  // EventTable
#include "lib/restore/ob_storage.h"                     // is_io_error
#include "lib/stat/ob_session_stat.h"
#include "share/restore/ob_log_restore_source.h"
#include "share/backup/ob_backup_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/archiveservice/large_buffer_pool.h"
#include "logservice/ob_log_external_storage_handler.h"       // ObLogExternalHandler

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
class RemoteDataGenerator;
typedef const std::function<int(const share::ObLSID &id, ObRemoteSourceGuard &guard)> GetSourceFunc;
typedef const std::function<int(const share::ObLSID &id, ObRemoteLogParent *source)> UpdateSourceFunc;
typedef const std::function<int(share::ObBackupDest &dest)> RefreshStorageInfoFunc;
// Remote Log Iterator, support remote dest or service, used as local iterator
template<class LogEntryType>
class ObRemoteLogIterator
{
  static const int64_t DEFAULT_SINGLE_READ_SIZE = 8 * palf::MAX_LOG_BUFFER_SIZE;
public:
  // @param[in] get_source_func, an function to get the input log restore source
  // @param[in] update_source_func, an function to update location info the the log restore source,
  //            which eliminates the location of a single log if the log restore source is used to init
  //            remote log iterator repeatedly.
  // @param[in] refresh_storage_info_func, when log restore source is oss/cos..., the id/key may be modified,
  //            so if you want remote log iteraotr to adapt automatically, this function should be provided,
  //            otherwise ignore it and use the default func, and which means when then id/key is changed,
  //            iteraotr should be inited again to follow this change
  ObRemoteLogIterator(GetSourceFunc &get_source_func,
      UpdateSourceFunc &update_source_func = [](const share::ObLSID &id, ObRemoteLogParent *source) { return OB_SUCCESS;},
       RefreshStorageInfoFunc &refresh_storage_info_func = [](share::ObBackupDest &dest) { return OB_NOT_SUPPORTED;});
  virtual ~ObRemoteLogIterator();
  // init remote log iterator
  // @param[in] tenant_id, the tenant id of the LS
  // @param[in] id, the id of the LS
  // @param[in] pre_scn, the log scn of the previous one, which is used to locate piece, this is an optional value
  //            if this param can not be provided, set it with OB_INVALID_TIMESTAMP
  // @param[in] start_lsn, the LSN of the first log to iterate
  // @param[in] end_lsn, the LSN of the last log to iterate, which can be set a limited or INT64_MAX
  int init(const uint64_t tenant_id,
      const ObLSID &id,
      const share::SCN &pre_scn,
      const LSN &start_lsn,
      const LSN &end_lsn,
      archive::LargeBufferPool *buffer_pool,
      logservice::ObLogExternalStorageHandler *log_ext_handler,
      const int64_t single_read_size = DEFAULT_SINGLE_READ_SIZE);
  // @brief used as local iterator, get one entry if not to end
  // @param[out] entry LogGroupEntry or LogEntry
  // @param[out] lsn entry start lsn
  // @param[out] buf, the pointer of the serialized entry
  // @param[out] buf_size, the size of the serialized entry
  // @ret_code  OB_SUCCESS next entry success
  //            OB_ITER_END iterate entry to the newest or to end_lsn
  //            other code  failed
  int next(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size);

  void reset();
  // support read buffer in parallel, iterator can read data only
  // @param[out] empty, if data read or not
  int pre_read(bool &empty);

  // @brief call update_source_func explicitly
  void update_source_cb();

  bool is_init() const { return inited_; }
  bool is_empty() const { return data_buffer_.is_empty(); }
  char *get_buffer() { return buf_; }

  TO_STRING_KV(K_(inited), K_(tenant_id), K_(id), K_(start_lsn), K_(cur_lsn), K_(end_lsn), K_(single_read_size), K_(gen));

private:
  int build_data_generator_(const share::SCN &pre_scn,
      ObRemoteLogParent *source,
      const std::function<int(share::ObBackupDest &dest)> &refresh_storage_info_func);
  int build_service_data_generator_(ObRemoteSerivceParent *source);
  int build_dest_data_generator_(const share::SCN &pre_scn, ObRemoteRawPathParent *source);
  int build_location_data_generator_(const share::SCN &pre_scn,
      ObRemoteLocationParent *source,
      const std::function<int(share::ObBackupDest &dest)> &refresh_storage_info_func);
  int next_entry_(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size);
  int prepare_buf_();
  int get_entry_(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size);
  void update_data_gen_max_lsn_();
  void advance_data_gen_lsn_();
  void mark_source_error_(const int ret_code);
  bool need_prepare_buf_(const int ret_code) const;

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSID id_;
  LSN start_lsn_;
  LSN cur_lsn_;            // 迭代最新一条日志所对应终点LSN
  share::SCN cur_scn_;     // 迭代最新一条日志scn
  LSN end_lsn_;
  int64_t single_read_size_;
  ObRemoteSourceGuard source_guard_;
  RemoteDataBuffer<LogEntryType> data_buffer_;
  RemoteDataGenerator *gen_;
  char *buf_;
  int64_t buf_size_;
  archive::LargeBufferPool *buffer_pool_;
  logservice::ObLogExternalStorageHandler *log_ext_handler_;
  GetSourceFunc get_source_func_;
  UpdateSourceFunc update_source_func_;
  RefreshStorageInfoFunc refresh_storage_info_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogIterator);
};

#include "ob_remote_log_iterator.ipp"

typedef ObRemoteLogIterator< palf::LogGroupEntry > ObRemoteLogGroupEntryIterator;
typedef ObRemoteLogIterator< palf::LogEntry > ObRemoteLogpEntryIterator;

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_ITERATOR_H_ */
