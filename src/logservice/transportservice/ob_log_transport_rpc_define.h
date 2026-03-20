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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_DEFINE_
#define OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_DEFINE_

#include "lib/net/ob_addr.h"
#include "lib/utility/serialization.h"
#include "lib/thread/ob_thread_lease.h"
#include "share/scn.h"
#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace logservice
{

struct ObLogTransportReq
{
  ObLogTransportReq() : standby_cluster_id_(), standby_tenant_id_(), ls_id_(), start_lsn_(), end_lsn_(), scn_(), log_data_(), log_size_(0), src_() {}

  int64_t standby_cluster_id_;
  uint64_t standby_tenant_id_;
  share::ObLSID ls_id_;
  palf::LSN start_lsn_;
  palf::LSN end_lsn_;
  share::SCN scn_;
  const char *log_data_;
  int64_t log_size_;
  common::ObAddr src_;

  bool is_valid() const
  {
    return standby_cluster_id_ > 0 &&
           standby_tenant_id_ > 0 &&
           ls_id_.is_valid() &&
           start_lsn_.is_valid() &&
           end_lsn_.is_valid() &&
           end_lsn_ > start_lsn_ &&
           log_data_ != nullptr &&
           log_size_ > 0 &&
           scn_.is_valid();
  }

  TO_STRING_KV(K(standby_cluster_id_), K(standby_tenant_id_), K(ls_id_), K(start_lsn_), K(end_lsn_), K(scn_), K(log_size_), K(src_));

  OB_UNIS_VERSION(1);
};

struct ObLogSyncStandbyInfo
{
  ObLogSyncStandbyInfo() : standby_cluster_id_(), standby_tenant_id_(), ls_id_(),
      ret_code_(OB_SUCCESS), refresh_info_ret_code_(OB_SUCCESS),
      standby_committed_end_lsn_(), standby_committed_end_scn_() {}

  int64_t standby_cluster_id_;
  uint64_t standby_tenant_id_;
  share::ObLSID ls_id_;
  int ret_code_;
  int refresh_info_ret_code_;
  palf::LSN standby_committed_end_lsn_;
  share::SCN standby_committed_end_scn_;

  TO_STRING_KV(K(standby_cluster_id_), K(standby_tenant_id_), K(ls_id_),
      K(ret_code_), K(refresh_info_ret_code_),
      K(standby_committed_end_lsn_), K(standby_committed_end_scn_));

  OB_UNIS_VERSION(1);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_TRANSPORT_RPC_DEFINE_