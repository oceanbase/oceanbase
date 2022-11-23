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
#include "logservice/palf/palf_iterator.h"     // MemPalfGroupBufferIterator
#include "share/ob_ls_id.h"                    // ObLSID
#include "ob_remote_log_source.h"              // ObRemoteLogParent
#include "logservice/palf/log_group_entry.h"   // LogGroupEntry
#include "logservice/palf/lsn.h"               // LSN
#include "logservice/palf/log_iterator_storage.h"  // MemoryStorage
#include "ob_remote_data_generator.h"          // RemoteDataBuffer

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
class ObRemoteLogIterator
{
public:
  ObRemoteLogIterator(GetSourceFunc &get_source_func,
      UpdateSourceFunc &update_source_func = [](const share::ObLSID &id, ObRemoteLogParent *source) { return OB_SUCCESS;},
       RefreshStorageInfoFunc &refresh_storage_info_func = [](share::ObBackupDest &dest) { return OB_NOT_SUPPORTED;});
  virtual ~ObRemoteLogIterator();
  // init remote log iterator
  // @param[in] tenant_id, the tenant id of the LS
  // @param[in] id, the id of the LS
  // @param[in] pre_log_ts, the log ts of the previous one, which is used to locate piece, this is an optional value
  //            if this param can not be provided, set it with OB_INVALID_TIMESTAMP
  // @param[in] start_lsn, the LSN of the first log to iterate
  // @param[in] end_lsn, the LSN of the last log to iterate, which can be set a limited or INT64_MAX
  // @param[in] get_source_func, an function to get the input log archive source
  // @param[in] update_source_func, an function to update location info the the log archive source,
  //            which eliminates the location of a single log if the log archive source is used to init
  //            remote log iterator repeatedly.
  // @param[in] refresh_storage_info_func, when log archive source is oss/cos..., the id/key may be modified,
  //            so if you want remote log iteraotr to adapt automatically, this function should be provided,
  //            otherwise ignore it and use the default func, and which means when then id/key is changed,
  //            iteraotr should be inited again to follow this change
  int init(const uint64_t tenant_id,
      const ObLSID &id,
      const int64_t pre_log_ts,
      const LSN &start_lsn,
      const LSN &end_lsn);
    // = [](share::ObBackupDest &dest) -> int { return OB_NOT_SUPPORTED; });
  // @param start_lsn 迭代日志起点
  // @param end_lsn 迭代日志终点, 最多取到第一条大于等于该值的日志
  int init(const uint64_t tenant_id, const ObLSID &id, const LSN &start_lsn,
      const LSN &end_lsn, ObRemoteLogParent *source);
  // @brief used as local iterator, get one entry if not to end
  // @param[out] entry 迭代的LogGroupEntry
  // @param[out] lsn LogGroupEntry在日志流起始offset
  int next(LogGroupEntry &entry, LSN &lsn, char *&buf, int64_t &buf_size);
  int get_cur_lsn_ts(LSN &lsn, int64_t &timestamp) const;

  TO_STRING_KV(K_(inited), K_(tenant_id), K_(id), K_(start_lsn), K_(cur_lsn), K_(end_lsn), K_(gen));

private:
  int build_data_generator_(const int64_t pre_log_ts,
      ObRemoteLogParent *source,
      const std::function<int(share::ObBackupDest &dest)> &refresh_storage_info_func);
  int build_service_data_generator_(ObRemoteSerivceParent *source);
  int build_dest_data_generator_(const int64_t pre_log_ts, ObRemoteRawPathParent *source);
  int build_location_data_generator_(const int64_t pre_log_ts,
      ObRemoteLocationParent *source,
      const std::function<int(share::ObBackupDest &dest)> &refresh_storage_info_func);
  int next_entry_(LogGroupEntry &entry, LSN &lsn, char *&buf, int64_t &buf_size);
  int prepare_buf_(RemoteDataBuffer &buffer);
  int get_entry_(LogGroupEntry &entry, LSN &lsn, char *&buf, int64_t &buf_size);
  void update_data_gen_max_lsn_();
  void mark_source_error_(const int ret_code);
  bool is_retry_ret_(const bool ret_code) const;

private:
  bool inited_;
  uint64_t tenant_id_;
  ObLSID id_;
  LSN start_lsn_;
  LSN cur_lsn_;            // 迭代最新一条日志所对应终点LSN
  int64_t cur_log_ts_;     // 迭代最新一条日志log_ts
  LSN end_lsn_;
  ObRemoteSourceGuard source_guard_;
  RemoteDataBuffer data_buffer_;
  RemoteDataGenerator *gen_;
  GetSourceFunc get_source_func_;
  UpdateSourceFunc update_source_func_;
  RefreshStorageInfoFunc refresh_storage_info_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogIterator);
};

} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_FETCH_LOG_ITERATOR_H_ */
