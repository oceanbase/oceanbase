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

#include "ob_log_transport_rpc_define.h"
#include "lib/utility/serialization.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace logservice
{

// ========================== ObLogTransportReq ================================== //

// 注意：log_data_ 是 const char* 指针，指向外部数据，需要序列化实际数据内容
OB_DEF_SERIALIZE(ObLogTransportReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      standby_cluster_id_,
      standby_tenant_id_,
      ls_id_,
      start_lsn_,
      end_lsn_,
      scn_,
      log_size_,
      src_);
  if (OB_SUCC(ret) && log_size_ > 0 && log_data_ != nullptr) {
    if (buf_len - pos < log_size_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, log_data_, log_size_);
      pos += log_size_;
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLogTransportReq)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      standby_cluster_id_,
      standby_tenant_id_,
      ls_id_,
      start_lsn_,
      end_lsn_,
      scn_,
      log_size_,
      src_);
  if (OB_SUCC(ret) && log_size_ > 0) {
    if (data_len - pos < log_size_) {
      ret = OB_DESERIALIZE_ERROR;
      CLOG_LOG(WARN, "deserialize log_data failed, buf not enough", K(ret), K(log_size_), K(data_len), K(pos));
    } else {
      // 注意：log_data_ 是 const char*，在反序列化时指向输入缓冲区
      // RPC 框架保证缓冲区在 RPC 处理期间有效
      log_data_ = buf + pos;
      pos += log_size_;
    }
  } else {
    log_data_ = nullptr;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLogTransportReq)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      standby_cluster_id_,
      standby_tenant_id_,
      ls_id_,
      start_lsn_,
      end_lsn_,
      scn_,
      log_size_,
      src_);
  len += log_size_;
  return len;
}

// ========================== ObLogSyncStandbyInfo ================================== //

OB_SERIALIZE_MEMBER(ObLogSyncStandbyInfo, standby_cluster_id_, standby_tenant_id_, ls_id_,
    ret_code_, refresh_info_ret_code_, standby_committed_end_lsn_, standby_committed_end_scn_);

} // namespace logservice
} // namespace oceanbase
