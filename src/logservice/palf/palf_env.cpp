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

#define USING_LOG_PREFIX PALF
#include "palf_env.h"
#include "palf_handle.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "log_io_adapter.h"
#include "share/ob_local_device.h"                            // ObLocalDevice
#include "share/io/ob_io_manager.h"                           // ObIOManager
#include "logservice/ipalf/ipalf_env.h"
#include "logservice/ipalf/ipalf_define.h"
#include "logservice/ipalf/ipalf_handle.h"
#include "share/config/ob_server_config.h"
#include "palf_env.h"

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

bool PalfEnv::operator==(const ipalf::IPalfEnv &rhs) const
{
  return static_cast<const void*>(this) == static_cast<const void*>(&rhs);
}

int PalfEnv::create_palf_env(ipalf::PalfEnvCreateParams *params, PalfEnv *&palf_env)
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_logservice) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "logservice is enabled, can not create palf env", K(ret), K(GCONF.enable_logservice));
  } else if (OB_ISNULL(params) || OB_NOT_NULL(palf_env)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), KP(params), KP(palf_env));
  } else if (OB_ISNULL(palf_env = MTL_NEW(PalfEnv, "PalfEnv"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(FileDirectoryUtils::delete_tmp_file_or_directory_at(params->base_dir_))) {
    PALF_LOG(WARN, "delete_tmp_file_or_directory_at failed", K(ret), K(params->base_dir_));
  } else if (OB_FAIL(palf_env->palf_env_impl_.init(*params->options_, params->base_dir_, *params->self_, obrpc::ObRpcNetHandler::CLUSTER_ID,
                                                   MTL_ID(), params->transport_, params->batch_rpc_,
                                                   params->log_alloc_mgr_, params->log_block_pool_, params->monitor_,
                                                   params->log_local_device_, params->resource_manager_, params->io_manager_))) {
    PALF_LOG(WARN, "PalfEnvImpl init failed", K(ret), K(params->base_dir_));
  } else {
    PALF_LOG(INFO, "create_palf_handle_impl success", K(params->base_dir_));
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

int PalfEnv::start()
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
                    const ipalf::AccessMode &access_mode,
                    const palf::PalfBaseInfo &palf_base_info,
                    ipalf::IPalfHandle *&handle)
{
  int ret = OB_SUCCESS;
  PalfHandle *palf_handle = NULL;
  int64_t  palf_id(id);
  palf::IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_NOT_NULL(handle)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "handle is already setted", K(ret), KP(handle));
  } else if (!ipalf::is_valid_palf_id(id) || ipalf::AccessMode::INVALID_ACCESS_MODE == access_mode || !palf_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(id), K(access_mode), K(palf_base_info));
  } else if (OB_ISNULL(palf_handle = MTL_NEW(PalfHandle, "PalfHandle"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloca palf handle failed", K(ret));
  } else if (OB_FAIL(palf_env_impl_.create_palf_handle_impl(palf_id, access_mode, palf_base_info, palf_handle_impl))) {
    PALF_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(palf_id));
  } else if (FALSE_IT(palf_handle->palf_handle_impl_ = palf_handle_impl)) {
  } else {
    handle = palf_handle;
    PALF_LOG(INFO, "create palf handle success", K(id));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(palf_handle_impl)) {
    palf_env_impl_.revert_palf_handle_impl(palf_handle_impl);
    palf_handle_impl = NULL;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(palf_handle)) {
    palf_handle->palf_handle_impl_ = NULL;
    MTL_DELETE(PalfHandle, "PalfHandle", palf_handle);
    palf_handle = NULL;
    handle = NULL;
  }
  return ret;
}

int PalfEnv::load(const int64_t id, ipalf::IPalfHandle *&handle)
{
    return open(id, handle);
}

int PalfEnv::open(const int64_t id, ipalf::IPalfHandle *&handle)
{
  int ret = OB_SUCCESS;
  PalfHandle *palf_handle = NULL;
  int64_t  palf_id(id);
  palf::IPalfHandleImpl *palf_handle_impl = NULL;
  if (OB_NOT_NULL(handle)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "handle is already setted", K(ret), KP(handle));
  } else if (!ipalf::is_valid_palf_id(id)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(id));
  } else if (OB_ISNULL(palf_handle = MTL_NEW(PalfHandle, "PalfHandle"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PALF_LOG(WARN, "alloca palf handle failed", K(ret));
  } else if (OB_FAIL(palf_env_impl_.get_palf_handle_impl(palf_id, palf_handle_impl))) {
    PALF_LOG(TRACE, "get_palf_handle_impl failed", K(ret), K(palf_id));
  } else if (FALSE_IT(palf_handle->palf_handle_impl_ = palf_handle_impl)) {
  } else {
    handle = palf_handle;
    PALF_LOG(TRACE, "PalfEnv open success", K(ret), K(id), K(handle));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(palf_handle_impl)) {
    palf_env_impl_.revert_palf_handle_impl(palf_handle_impl);
    palf_handle_impl = NULL;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(palf_handle)) {
    palf_handle->palf_handle_impl_ = NULL;
    MTL_DELETE(PalfHandle, "PalfHandle", palf_handle);
    palf_handle = NULL;
    handle = NULL;
  }
  return ret;
}

int PalfEnv::open(const int64_t id, PalfHandle *&handle)
{
  int ret = OB_SUCCESS;
  ipalf::IPalfHandle *ipalf_handle = static_cast<ipalf::IPalfHandle*>(handle);
  if (OB_FAIL(open(id, ipalf_handle))) {
    PALF_LOG(WARN, "open failed", K(ret), K(id));
  } else {
    handle = static_cast<PalfHandle*>(ipalf_handle);
    PALF_LOG(TRACE, "PalfEnv open success", K(ret), K(id), K(handle));
  }
  return ret;
}

int PalfEnv::close(ipalf::IPalfHandle *&handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handle)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "handle is null", K(ret), KP(handle));
  } else {
    PalfHandle *palf_handle = static_cast<PalfHandle*>(handle);
    (void)palf_handle->unregister_file_size_cb();
    (void)palf_handle->unregister_role_change_cb();
    (void)palf_handle->unregister_rebuild_cb();
    palf_env_impl_.revert_palf_handle_impl(palf_handle->palf_handle_impl_);
    palf_handle->palf_handle_impl_ = NULL;
    MTL_DELETE(PalfHandle, "PalfHandle", palf_handle);
    palf_handle = NULL;
    handle = NULL;
    PALF_LOG(TRACE, "palf handle close success", K(handle));
  }
  return ret;
}

int PalfEnv::close(PalfHandle *&handle)
{
  int ret = OB_SUCCESS;
  ipalf::IPalfHandle *ipalf_handle = static_cast<ipalf::IPalfHandle*>(handle);
  if (OB_FAIL(close(ipalf_handle))) {
    PALF_LOG(WARN, "close failed", K(ret), K(handle));
  } else {
    handle = NULL;
    PALF_LOG(TRACE, "palf handle success", K(handle));
  }
  return ret;
}

int PalfEnv::remove(int64_t id)
{
  int64_t palf_id(id);
  return palf_env_impl_.remove_palf_handle_impl(palf_id);
}

int PalfEnv::update_options(const PalfOptions &options)
{
  return palf_env_impl_.update_options(options);
}

int PalfEnv::get_options(PalfOptions &options)
{
  return palf_env_impl_.get_options(options);
}

int PalfEnv::get_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  return palf_env_impl_.get_disk_usage(used_size_byte, total_size_byte);
}

int PalfEnv::get_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte)
{
  return palf_env_impl_.get_stable_disk_usage(used_size_byte, total_size_byte);
}

bool PalfEnv::check_disk_space_enough()
{
  return palf_env_impl_.check_disk_space_enough();
}

int PalfEnv::get_io_statistic_info(int64_t &last_working_time,
                                   int64_t &pending_write_size,
                                   int64_t &pending_write_count,
                                   int64_t &pending_write_rt,
                                   int64_t &accum_write_size,
                                   int64_t &accum_write_count,
                                   int64_t &accum_write_rt)
{
  return palf_env_impl_.get_io_statistic_info(last_working_time,
      pending_write_size, pending_write_count, pending_write_rt,
      accum_write_size, accum_write_count, accum_write_rt);
}

int PalfEnv::for_each(const ObFunction<int(const ipalf::IPalfHandle &)> &func)
{
  const common::ObFunction<int (const PalfHandle &)> wrapper_func = [func](const palf::PalfHandle &palf_handle) -> int {
    return func(palf_handle);
  };
  return palf_env_impl_.for_each(wrapper_func);
}

int PalfEnv::for_each_derived(const ObFunction<int(const PalfHandle&)> &func)
{
  return palf_env_impl_.for_each(func);
}

// should be removed in version 4.2.0.0
int PalfEnv::update_replayable_point(const SCN &replayable_scn)
{
  return palf_env_impl_.update_replayable_point(replayable_scn);
}

int64_t PalfEnv::get_tenant_id()
{
  return palf_env_impl_.get_tenant_id();
}

int PalfEnv::advance_base_lsn(int64_t id, palf::LSN lsn)
{
  return common::OB_NOT_SUPPORTED;
}

} // end namespace palf
} // end namespace oceanbase
