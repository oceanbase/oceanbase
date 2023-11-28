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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_RAW_READER_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_RAW_READER_H_

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_ls_id.h"
#include "logservice/palf/log_define.h"
#include "ob_remote_data_generator.h"
#include "ob_remote_log_source.h"
#include <cstdint>

namespace oceanbase
{

namespace logservice
{
class ObLogArchivePieceContext;
typedef const std::function<int(const share::ObLSID &id, ObRemoteSourceGuard &guard)> GetSourceFunc;
typedef const std::function<int(const share::ObLSID &id, ObRemoteLogParent *source)> UpdateSourceFunc;
typedef const std::function<int(share::ObBackupDest &dest)> RefreshStorageInfoFunc;
class ObRemoteLogRawReader
{
  static const int64_t DEFAULT_SINGLE_READ_SIZE = 8 * palf::MAX_LOG_BUFFER_SIZE;
  class DefaultUpdateSourceFunctor
  {
  public:
    int operator()(const share::ObLSID &id, ObRemoteLogParent *source) { return OB_SUCCESS; }
  };
  class DefaultRefreshStorageInfoFunctor
  {
  public:
    int operator()(share::ObBackupDest &dest) { return OB_NOT_SUPPORTED; }
  };
public:
  // @param[in] get_source_func, an function to get the input log restore source
  // @param[in] update_source_func, an function to update location info the the log restore source,
  //            which eliminates the location of a single log if the log restore source is used to init
  //            remote log iterator repeatedly.
  // @param[in] refresh_storage_info_func, when log restore source is oss/cos..., the id/key may be modified,
  //            so if you want remote log iteraotr to adapt automatically, this function should be provided,
  //            otherwise ignore it and use the default func, and which means when then id/key is changed,
  //            iteraotr should be inited again to follow this change
  ObRemoteLogRawReader(GetSourceFunc &get_source_func,
      UpdateSourceFunc &update_source_func = DefaultUpdateSourceFunctor(),
       RefreshStorageInfoFunc &refresh_storage_info_func = DefaultRefreshStorageInfoFunctor());
  virtual ~ObRemoteLogRawReader();
public:
  // init remote log raw reader, as archive log directory is splitted by scn, the pre_scn is indispensable to help locate directory
  //
  // @param[in] tenant_id, log source tenant id, to check consistency among the tenant and the archive
  // @param[in] id, ls id
  // @param[in] pre_scn, the scn of the previous one or several log the help locate directory
  // @param[in] log_ext_handler, the module to promote read performance
  int init(const uint64_t tenant_id,
      const share::ObLSID &id,
      const share::SCN &pre_scn,
      ObLogExternalStorageHandler *log_ext_handler);

  void destroy();

  // raw read from archive files
  //
  // @param[in] start_lsn, the start lsn to read
  // @param[in] buffer, buffer to hold read data
  // @param[in] nbytes, total size to read
  // @param[out] read_size, real size read
  //
  // @ret_code OB_SUCCESS
  //           OB_INVALID_ARGUMENT
  //           OB_ERR_OUT_OF_LOWER_BOUND, the lsn is out of lower bound
  //           OB_ERR_OUT_OF_UPPER_BOUND, the lsn is out of upper bound
  //           others error enexpected
  int raw_read(const palf::LSN &start_lsn,
      void *buffer,
      const int64_t nbytes,
      int64_t &read_size);

  TO_STRING_KV(K_(inited), K_(tenant_id), K_(id), K_(pre_scn), K_(start_lsn), K_(cur_lsn), K_(max_lsn), K_(log_ext_handler));

private:
  int raw_read_(char *buffer, const int64_t buffer_size, int64_t &total_read_size);

  int read_once_(char *buffer, const int64_t buffer_size, int64_t &total_read_size);

private:
  bool inited_;
  uint64_t tenant_id_;
  share::ObLSID id_;
  share::SCN pre_scn_;
  palf::LSN start_lsn_;
  palf::LSN cur_lsn_;
  palf::LSN max_lsn_;
  ObLogExternalStorageHandler *log_ext_handler_;
  ObRemoteSourceGuard source_guard_;
  GetSourceFunc get_source_func_;
  UpdateSourceFunc update_source_func_;
  RefreshStorageInfoFunc refresh_storage_info_func_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLogRawReader);
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOG_RAW_READER_H_ */
