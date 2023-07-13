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

#include "palf_env.h"
#include "lib/ob_errno.h"
#include "lib/file/file_directory_utils.h"
#include "palf_env_impl.h"
#include "palf_handle_impl.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "palf_handle.h"
#include "palf_options.h"
#include "election/interface/election.h"

namespace oceanbase
{
namespace palf
{
PalfEnv::PalfEnv() : palf_env_impl_()
{

}

PalfEnv::~PalfEnv()
{
  stop_();
  wait_();
  destroy_();
}

int PalfEnv::create_palf_env(
    const PalfOptions &options,
    const char *base_dir,
    const common::ObAddr &self,
    rpc::frame::ObReqTransport *transport,
    common::ObILogAllocator *log_alloc_mgr,
    ILogBlockPool *log_block_pool,
    PalfMonitorCb *monitor,
    PalfEnv *&palf_env)
{
  int ret = OB_SUCCESS;
  palf_env = MTL_NEW(PalfEnv, "PalfEnv");
  if (NULL == palf_env) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(FileDirectoryUtils::delete_tmp_file_or_directory_at(base_dir))) {
    CLOG_LOG(WARN, "delete_tmp_file_or_directory_at failed", K(ret), K(base_dir));
  } else if (OB_FAIL(palf_env->palf_env_impl_.init(options, base_dir, self, obrpc::ObRpcNetHandler::CLUSTER_ID,
                                                   MTL_ID(), transport,
                                                   log_alloc_mgr, log_block_pool, monitor))) {
    PALF_LOG(WARN, "PalfEnvImpl init failed", K(ret), K(base_dir));
  } else if (OB_FAIL(palf_env->start_())) {
    PALF_LOG(WARN, "start palf env failed", K(ret), K(base_dir));
  } else {
    PALF_LOG(INFO, "create_palf_handle_impl success", K(base_dir));
  }
  if (NULL != palf_env && OB_FAIL(ret)) {
    MTL_DELETE(PalfEnv, "PalfEnv", palf_env);
    palf_env = NULL;
  }
  return ret;
}

void PalfEnv::destroy_palf_env(PalfEnv *&palf_env)
{
  MTL_DELETE(PalfEnv, "palf_env", palf_env);
  PALF_LOG_RET(WARN, OB_SUCCESS, "destroy_palf_env success", K(palf_env));
}

int PalfEnv::start_()
{
  int ret = OB_SUCCESS;
  ret = palf_env_impl_.start();
  return ret;
}

void PalfEnv::stop_()
{
  palf_env_impl_.stop();
}

void PalfEnv::wait_()
{
  palf_env_impl_.wait();
}

void PalfEnv::destroy_()
{
  palf_env_impl_.destroy();
}

int PalfEnv::create(const int64_t id,
                    const AccessMode &access_mode,
                    const PalfBaseInfo &palf_base_info,
                    PalfHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t  palf_id(id);
  palf::IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_FAIL(palf_env_impl_.create_palf_handle_impl(palf_id, access_mode, palf_base_info, palf_handle_impl))) {
    PALF_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(palf_id));
  } else if (FALSE_IT(handle.palf_handle_impl_ = palf_handle_impl)) {
  } else {
    PALF_LOG(INFO, "create palf handle success", K(id));
  }
  if (OB_FAIL(ret)) {
    handle.palf_handle_impl_ = NULL;
  }
  return ret;
}

int PalfEnv::open(const int64_t id, PalfHandle &handle)
{
  int ret = OB_SUCCESS;
  int64_t  palf_id(id);
  palf::IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_FAIL(palf_env_impl_.get_palf_handle_impl(palf_id, palf_handle_impl))) {
    PALF_LOG(TRACE, "get_palf_handle_impl failed", K(ret), K(palf_id));
  } else if (FALSE_IT(handle.palf_handle_impl_ = palf_handle_impl)) {
  } else {
    PALF_LOG(TRACE, "PalfEnv open success", K(ret), K(id), K(handle));
  }
  if (OB_FAIL(ret)) {
    handle.palf_handle_impl_ = NULL;
  }
  return ret;
}

void PalfEnv::close(PalfHandle &handle)
{
  (void)handle.unregister_file_size_cb();
  (void)handle.unregister_role_change_cb();
  (void)handle.unregister_rebuild_cb();
  palf_env_impl_.revert_palf_handle_impl(handle.palf_handle_impl_);
  handle.palf_handle_impl_ = NULL;
  PALF_LOG(TRACE, "PalfEnv close success", K(handle));
}

int PalfEnv::remove(int64_t id)
{
  int64_t palf_id(id);
  return palf_env_impl_.remove_palf_handle_impl(palf_id);
}

int PalfEnv::get_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  return palf_env_impl_.get_disk_usage(used_size_byte, total_size_byte);
}

int PalfEnv::get_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  return palf_env_impl_.get_stable_disk_usage(used_size_byte, total_size_byte);
}

int PalfEnv::get_options(PalfOptions &options)
{
  return palf_env_impl_.get_options(options);
}

int PalfEnv::update_options(const PalfOptions &options)
{
  return palf_env_impl_.update_options(options);
}

bool PalfEnv::check_disk_space_enough()
{
  return palf_env_impl_.check_disk_space_enough();
}

int PalfEnv::for_each(const ObFunction<int(const PalfHandle &)> &func)
{
  return palf_env_impl_.for_each(func);
}

int PalfEnv::get_io_start_time(int64_t &last_working_time)
{
  return palf_env_impl_.get_io_start_time(last_working_time);
}

// should be removed in version 4.2.0.0
int PalfEnv::update_replayable_point(const SCN &replayable_scn)
{
  return palf_env_impl_.update_replayable_point(replayable_scn);
}

} // end namespace palf
} // end namespace oceanbase
