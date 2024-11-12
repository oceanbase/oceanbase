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

#ifndef OCEANBASE_LOGSERVICE_LOG_GRPC_ADAPTER_
#define OCEANBASE_LOGSERVICE_LOG_GRPC_ADAPTER_

#include "common/storage/ob_io_device.h"
#include "share/ob_errno.h"        // errno
#include "grpc/newlogstorepb.grpc.pb.h"
#include "grpc/ob_grpc_context.h"
namespace oceanbase
{
namespace logservice
{
class ObLogGrpcAdapter {
public:
  ObLogGrpcAdapter();
  ~ObLogGrpcAdapter();
  int init(const ObAddr &addr, const int64_t cluster_id);
  int init(const ObAddr &addr, const int64_t rpc_timeout_us, const int64_t cluster_id);
  void destroy();
  int load_log_store(const newlogstorepb::LoadLogStoreReq &req, newlogstorepb::LoadLogStoreResp &resp);
  int open(const newlogstorepb::OpenReq &req, newlogstorepb::OpenResp &resp);
  int close(const newlogstorepb::CloseReq &req, newlogstorepb::CloseResp &resp);
  int pread(const newlogstorepb::PreadReq &req, newlogstorepb::PreadResp &resp);
  int pwrite(const newlogstorepb::PwriteReq &req, newlogstorepb::PwriteResp &resp);
  int fallocate(const newlogstorepb::FallocateReq &req, newlogstorepb::FallocateResp &resp);
  int ftruncate(const newlogstorepb::FtruncateReq &req, newlogstorepb::FtruncateResp &resp);
  int stat(const newlogstorepb::StatReq &req, newlogstorepb::StatResp &resp);
  //int fstat(const newlogstorepb::FstatatReq &req, newlogstorepb::StatResp &resp);
  int mkdir(const newlogstorepb::MkdirReq &req, newlogstorepb::MkdirResp &resp);
  int rmdir(const newlogstorepb::RmdirReq &req, newlogstorepb::RmdirResp &resp);
  int unlink(const newlogstorepb::UnlinkReq &req, newlogstorepb::UnlinkResp &resp);
  int rename(const newlogstorepb::RenameReq &req, newlogstorepb::RenameResp &resp);
  int scan_dir(const newlogstorepb::ScanDirReq &req, newlogstorepb::ScanDirResp &resp);
  int fsync(const newlogstorepb::FsyncReq &req, newlogstorepb::FsyncResp &resp);
  int get_log_store_info(const newlogstorepb::GetLogStoreInfoReq &req, newlogstorepb::GetLogStoreInfoResp &resp);
  int batch_fallocate(const newlogstorepb::BatchFallocateReq &req, newlogstorepb::BatchFallocateResp &resp);
private:
  bool is_need_retry_(const int ret) {
    return OB_TIMEOUT == ret || OB_RPC_SEND_ERROR == ret;
  }
private:
  static const int64_t retry_interval_us = 10 * 1000L;  // 10ms
private:
  mutable oceanbase::obgrpc::ObGrpcClient<newlogstorepb::NewLogStore> *grpc_client_;
  bool is_inited_;
};
}
}

#endif // OCEANBASE_LOGSERVICE_LOG_GRPC_ADAPTER_
