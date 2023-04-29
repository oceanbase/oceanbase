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

#define USING_LOG_PREFIX SQL
#include "sql/engine/ob_exec_context.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/ob_trans_service.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_file_rpc_processor.h"
#include "share/external_table/ob_external_table_file_task.h"
namespace oceanbase
{
namespace share
{

int ObFlushExternalTableKVCacheP::process()
{
  int ret = OB_SUCCESS;
  ObFlushExternalTableFileCacheReq &req = arg_;
  ObFlushExternalTableFileCacheRes &res = result_;
  if (OB_FAIL(ObExternalTableFileManager::get_instance().flush_cache(req.tenant_id_, req.table_id_))) {
    LOG_WARN("erase kvcache result failed", K(ret));
  }
  res.rcode_.rcode_ = ret;
  return OB_SUCCESS;
}

int ObAsyncLoadExternalTableFileListP::process()
{
  int ret = OB_SUCCESS;
  ObLoadExternalFileListReq &req = arg_;
  ObLoadExternalFileListRes &res = result_;
  ObString &location = req.location_;
  ObSEArray<ObString, 16> file_urls;
  ObString access_info;
  ObArenaAllocator allocator;
  if (OB_FAIL(ObExternalTableFileManager::get_instance().get_external_file_list_on_device(location,
                                                                                          file_urls,
                                                                                          res.file_sizes_,
                                                                                          access_info,
                                                                                          allocator))) {
    LOG_WARN("get external table file on device failed", K(ret));
  }
  for (int64_t i =0 ; OB_SUCC(ret) && i < file_urls.count(); i++) {
    ObString tmp;
    OZ(ob_write_string(res.get_alloc(), file_urls.at(i), tmp));
    OZ(res.file_urls_.push_back(tmp));
  }
  res.rcode_.rcode_ = ret;
  LOG_DEBUG("get external table file", K(ret), K(location), K(file_urls), K(res.file_urls_));
  return ret;
}

void ObRpcAsyncLoadExternalTableFileCallBack::on_timeout()
{
  int ret = OB_TIMEOUT;
  int64_t current_ts = ObTimeUtility::current_time();
  int64_t timeout_ts = get_send_ts() + timeout_;
  if (current_ts < timeout_ts) {
    LOG_DEBUG("rpc return OB_TIMEOUT before actual timeout, change error code to OB_RPC_CONNECT_ERROR", KR(ret),
              K(timeout_ts), K(current_ts));
    ret = OB_RPC_CONNECT_ERROR;
  }
  LOG_WARN("async task timeout", KR(ret));
  result_.rcode_.rcode_ = ret;
  context_->inc_concurrency_limit_with_signal();
}

void ObRpcAsyncLoadExternalTableFileCallBack::on_invalid()
{
  int ret = OB_SUCCESS;
  // a valid packet on protocol level, but can't decode it.
  result_.rcode_.rcode_ = OB_INVALID_ERROR;
  LOG_WARN("async task invalid", K(result_.rcode_.rcode_));
  context_->inc_concurrency_limit_with_signal();
}

int ObRpcAsyncLoadExternalTableFileCallBack::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("async access callback process", K_(result));
  if (OB_FAIL(get_rcode())) {
    result_.rcode_.rcode_ = get_rcode();
    LOG_WARN("async rpc execution failed", K(get_rcode()), K_(result));
  }
  context_->inc_concurrency_limit_with_signal();
  return ret;
}

oceanbase::rpc::frame::ObReqTransport::AsyncCB *ObRpcAsyncLoadExternalTableFileCallBack::clone(
    const oceanbase::rpc::frame::SPAlloc &alloc) const {
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB * const>(this));
}

void ObRpcAsyncFlushExternalTableKVCacheCallBack::on_timeout()
{
  int ret = OB_TIMEOUT;
  int64_t current_ts = ObTimeUtility::current_time();
  int64_t timeout_ts = get_send_ts() + timeout_;
  if (current_ts < timeout_ts) {
    LOG_DEBUG("rpc return OB_TIMEOUT before actual timeout, change error code to OB_RPC_CONNECT_ERROR", KR(ret),
              K(timeout_ts), K(current_ts));
    ret = OB_RPC_CONNECT_ERROR;
  }
  LOG_WARN("async task timeout", KR(ret));
  result_.rcode_.rcode_ = ret;
  context_->inc_concurrency_limit_with_signal();
}


void ObRpcAsyncFlushExternalTableKVCacheCallBack::on_invalid()
{
  int ret = OB_SUCCESS;
  // a valid packet on protocol level, but can't decode it.
  result_.rcode_.rcode_ = OB_INVALID_ERROR;
  LOG_WARN("async task invalid", K(result_.rcode_.rcode_));
  context_->inc_concurrency_limit_with_signal();
}

int ObRpcAsyncFlushExternalTableKVCacheCallBack::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("async access callback process", K_(result));
  if (OB_FAIL(get_rcode())) {
    result_.rcode_.rcode_ = get_rcode();
    // we need to clear op results because they are not decoded from das async rpc due to rpc error.
    LOG_WARN("async rpc execution failed", K(get_rcode()), K_(result));
  }
  context_->inc_concurrency_limit_with_signal();
  return ret;
}

oceanbase::rpc::frame::ObReqTransport::AsyncCB *ObRpcAsyncFlushExternalTableKVCacheCallBack::clone(
    const oceanbase::rpc::frame::SPAlloc &alloc) const {
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB * const>(this));
}


}  // namespace share
}  // namespace oceanbase
