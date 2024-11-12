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

#include "ob_log_grpc_adapter.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <linux/falloc.h>
#include "share/ob_errno.h"                      // errno
#include "share/ob_define.h"

using namespace oceanbase::common;
using namespace newlogstorepb;

#define CALL_LOGSTORE(grpc_client, func, args...)                                                 \
  ({                                                                                              \
    do {                                                                                          \
      if (OB_SUCC(GRPC_CALL((*grpc_client), func, req, &resp))) {                                 \
        CLOG_LOG(TRACE, "grpc call successfully");                                                \
      } else if (true == is_need_retry_(ret)) {                                                   \
        CLOG_LOG(WARN, "grpc fails, maybe need to retry", K(ret));                                \
        ret = OB_EAGAIN;                                                                          \
        ob_usleep(ObLogGrpcAdapter::retry_interval_us);                                           \
      } else {                                                                                    \
        CLOG_LOG(WARN, "grpc adapter encounter unexpected error", K(ret));                        \
      }                                                                                           \
    } while (OB_EAGAIN == ret);                                                                   \
  })

#define CALL_LOGSTORE_TIMEOUT(grpc_client, timeout, func, args...)                                \
  ({                                                                                              \
    do {                                                                                          \
      if (OB_SUCC(GRPC_CALL_TIMEOUT((*grpc_client), timeout, func, req, &resp))) {                \
        CLOG_LOG(TRACE, "grpc call successfully");                                                \
      } else if (true == is_need_retry_(ret)) {                                                   \
        CLOG_LOG(WARN, "grpc fails, maybe need to retry", K(ret));                                \
        ret = OB_EAGAIN;                                                                          \
        ob_usleep(ObLogGrpcAdapter::retry_interval_us);                                           \
      } else {                                                                                    \
        CLOG_LOG(WARN, "grpc adapter encounter unexpected error", K(ret));                        \
      }                                                                                           \
    } while (OB_EAGAIN == ret);                                                                   \
  })
namespace oceanbase
{
namespace logservice
{
ObLogGrpcAdapter::ObLogGrpcAdapter() : grpc_client_(NULL), is_inited_(false) {}
ObLogGrpcAdapter::~ObLogGrpcAdapter()
{
  destroy();
}

int ObLogGrpcAdapter::init(const ObAddr &addr,
                           const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "The log grpc adapter has been inited, ", K(ret));
  } else if (FALSE_IT(grpc_client_ = OB_NEW(oceanbase::obgrpc::ObGrpcClient<newlogstorepb::NewLogStore>, "GRPC"))) {
  } else if (OB_ISNULL(grpc_client_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "fail to allocate memory", K(addr));
  } else if (OB_FAIL(grpc_client_->init(addr, DEFAULT_GRPC_TIMEOUT_US, cluster_id, OB_SERVER_TENANT_ID))) {
    CLOG_LOG(WARN, "fail to init grpc_client", K(addr));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObLogGrpcAdapter::init(const ObAddr &addr,
                           const int64_t rpc_timeout_us,
                           const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "The log grpc adapter has been inited, ", K(ret));
  } else if (FALSE_IT(grpc_client_ = OB_NEW(oceanbase::obgrpc::ObGrpcClient<newlogstorepb::NewLogStore>, "GRPC"))) {
  } else if (OB_ISNULL(grpc_client_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "fail to allocate memory", K(addr));
  } else if (OB_FAIL(grpc_client_->init(addr, rpc_timeout_us, cluster_id, OB_SERVER_TENANT_ID))) {
    CLOG_LOG(WARN, "fail to init grpc_client", K(addr));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogGrpcAdapter::destroy()
{
  is_inited_ = false;
  OB_DELETE(ObGrpcClient, "GRPC", grpc_client_);
  grpc_client_ = NULL;
}

int ObLogGrpcAdapter::load_log_store(const LoadLogStoreReq &req, LoadLogStoreResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, LoadLogStore, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::open(const OpenReq &req, OpenResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Open, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::close(const CloseReq &req, CloseResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Close, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::pread(const PreadReq &req, PreadResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Pread, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::pwrite(const PwriteReq &req, PwriteResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Pwrite, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::fallocate(const FallocateReq &req, FallocateResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Fallocate, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::ftruncate(const FtruncateReq &req, FtruncateResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Ftruncate, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::stat(const StatReq &req, StatResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Stat, req, &resp);
  }
  return ret;
}

// int ObLogGrpcAdapter::fstat(const FstatatReq &req, StatResp &resp)
// {
//   int ret = OB_SUCCESS;
//   if (IS_NOT_INIT) {
//     ret = OB_NOT_INIT;
//   } else {
//     do {
//       if (OB_FAIL(GRPC_CALL(grpc_client_, Fstatat, req, &resp))) {
//         CLOG_LOG(WARN, "fstat completes with failed ret code", K(ret));
//       } else if (true == is_need_retry_(ret)){
//         ret = OB_EAGAIN;
//         ob_usleep(ObLogGrpcAdapter::retry_interval_us);
//       } else {
//         CLOG_LOG(TRACE, "grpc adapter fstat successfully");
//       }
//     } while (OB_EAGAIN == ret);
//   }
//   return ret;
// }

int ObLogGrpcAdapter::mkdir(const MkdirReq &req, MkdirResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Mkdir, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::rmdir(const RmdirReq &req, RmdirResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Rmdir, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::unlink(const UnlinkReq &req, UnlinkResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Unlink, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::rename(const RenameReq &req, RenameResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Rename, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::scan_dir(const ScanDirReq &req, ScanDirResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Scandir, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::fsync(const FsyncReq &req, FsyncResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, Fsync, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::get_log_store_info(const GetLogStoreInfoReq &req, GetLogStoreInfoResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    CALL_LOGSTORE(grpc_client_, GetLogStoreInfo, req, &resp);
  }
  return ret;
}

int ObLogGrpcAdapter::batch_fallocate(const newlogstorepb::BatchFallocateReq &req,
                                      newlogstorepb::BatchFallocateResp &resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    constexpr int64_t timeout = 24 * 3600 * 1000 * 1000ul;
    CALL_LOGSTORE_TIMEOUT(grpc_client_, timeout, BatchFallocate, req, &resp);
  }
  return ret;
}
}
}
