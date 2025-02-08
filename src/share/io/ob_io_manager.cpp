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

#define USING_LOG_PREFIX COMMON

#include "share/io/ob_io_manager.h"

#include "lib/time/ob_time_utility.h"
#include "lib/ob_running_mode.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "share/errsim_module/ob_errsim_module_interface_imp.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "observer/ob_server.h"
#include "share/ob_io_device_helper.h"
#include "lib/restore/ob_object_device.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

ObIOManager::ObIOManager()
  : is_inited_(false),
    is_working_(false),
    mutex_(ObLatchIds::GLOBAL_IO_CONFIG_LOCK),
    io_config_(),
    allocator_(),
    fault_detector_(io_config_),
    io_scheduler_(io_config_, allocator_)
{
}

ObIOManager::~ObIOManager()
{
  destroy();
}

ObIOManager &ObIOManager::get_instance()
{
  static ObIOManager instance;
  return instance;
}

int ObIOManager::init(const int64_t memory_limit,
                      const int32_t queue_depth,
                      const int32_t schedule_thread_count,
                      const int64_t schedule_media_id)
{
  int ret = OB_SUCCESS;
  int64_t schedule_queue_count = 0 != schedule_thread_count ? schedule_thread_count : (lib::is_mini_mode() ? 2 : 8);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(memory_limit <= 0|| schedule_queue_count <= 0 || schedule_media_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(memory_limit), K(schedule_queue_count), K(schedule_media_id));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE, "IO_MGR", OB_SERVER_TENANT_ID, memory_limit))) {
    LOG_WARN("init io allocator failed", K(ret));
  } else if (OB_FAIL(channel_map_.create(7, "IO_CHANNEL_MAP"))) {
    LOG_WARN("create channel map failed", K(ret));
  } else if (OB_FAIL(io_scheduler_.init(schedule_queue_count, schedule_media_id))) {
    LOG_WARN("init io scheduler failed", K(ret));
  } else if (OB_FAIL(fault_detector_.init())) {
    LOG_WARN("init io fault detector failed", K(ret));
  } else if (OB_ISNULL(server_io_manager_ = OB_NEW(ObTenantIOManager, "IO_MGR"))) {
  } else if (OB_FAIL(server_io_manager_->init(OB_SERVER_TENANT_ID, ObTenantIOConfig::default_instance(), &io_scheduler_))) {
    LOG_WARN("init server tenant io mgr failed", K(ret));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, "IO_MGR");
    SET_USE_500(attr);
    allocator_.set_attr(attr);
    io_config_.set_default_value();
    is_inited_ = true;
    if (OB_FAIL(server_io_manager_->start())) {
      LOG_WARN("init server tenant io mgr start failed", K(ret));
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

struct DestroyChannelMapFn
{
public:
  DestroyChannelMapFn(ObIAllocator &allocator) : allocator_(allocator) {}
  int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObDeviceChannel *> &entry) {
    if (nullptr != entry.second) {
      entry.second->~ObDeviceChannel();
      allocator_.free(entry.second);
    }
    return OB_SUCCESS;
  }
private:
  ObIAllocator &allocator_;
};

void ObIOManager::destroy()
{
  stop();
  fault_detector_.destroy();
  io_scheduler_.destroy();
  DestroyChannelMapFn destry_channel_map_fn(allocator_);
  channel_map_.foreach_refactored(destry_channel_map_fn);
  channel_map_.destroy();
  OB_DELETE(ObTenantIOManager, "IO_MGR", server_io_manager_);
  server_io_manager_ = nullptr;
  allocator_.destroy();
  is_inited_ = false;
  LOG_INFO("io manager is destroyed");
}

int ObIOManager::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IO manager not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(io_scheduler_.start())) {
    LOG_WARN("start io scheduler failed", K(ret));
  } else if (OB_FAIL(fault_detector_.start())) {
    LOG_WARN("start io fault detector failed", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObIOManager::stop()
{
  is_working_ = false;
  if (OB_NOT_NULL(server_io_manager_)) {
    server_io_manager_->stop();
  }
  io_scheduler_.stop();
}

void ObIOManager::wait()
{
  io_scheduler_.wait();
}

bool ObIOManager::is_stopped() const
{
  return !is_working_;
}

int ObIOManager::read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aio_read(info, handle))) {
    LOG_WARN("aio read failed", K(ret), K(info));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    LOG_WARN("io handle wait failed", K(ret), K(info), K(timeout_ms));
  }
  return ret;
}

int ObIOManager::write(const ObIOInfo &info, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObIOHandle handle;
  if (OB_FAIL(aio_write(info, handle))) {
    LOG_WARN("aio write failed", K(ret), K(info));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    LOG_WARN("io handle wait failed", K(ret), K(info), K(timeout_ms));
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(tenant_aio(info, handle))) {
    LOG_WARN("inner aio failed", K(ret), K(info));
  }
  return ret;
}

int ObIOManager::aio_write(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(tenant_aio(info, handle))) {
    LOG_WARN("inner aio failed", K(ret), K(info));
  }
  return ret;
}

int ObIOManager::pread(ObIOInfo &info, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid() || nullptr == info.buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    info.flag_.set_read();
    info.flag_.set_sync();
    ObIOHandle handle;
    if (OB_FAIL(tenant_aio(info, handle))) {
      LOG_WARN("do inner aio failed", K(ret), K(info));
    } else {
      while (OB_SUCC(ret) || OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) { // wait to die
        if (OB_FAIL(handle.wait(MAX_IO_WAIT_TIME_MS))) {
          if (OB_DATA_OUT_OF_RANGE != ret) {
            LOG_WARN("sync read failed", K(ret), K(info));
          }
        } else {
          break;
        }
      }
    }
    if (OB_SUCC(ret) || OB_DATA_OUT_OF_RANGE == ret) {
      read_size = handle.get_data_size();
      MEMCPY(const_cast<char *>(info.buf_), handle.get_buffer(), read_size);
    }
  }
  return ret;
}

int ObIOManager::pwrite(ObIOInfo &info, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid() || nullptr == info.buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    info.flag_.set_write();
    info.flag_.set_sync();
    ObIOHandle handle;
    if (OB_FAIL(tenant_aio(info, handle))) {
      LOG_WARN("do inner aio failed", K(ret), K(info));
    } else {
      while (OB_SUCC(ret) || OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) { // wait to die
        if (OB_FAIL(handle.wait(MAX_IO_WAIT_TIME_MS))) {
          if (OB_DATA_OUT_OF_RANGE != ret) {
            LOG_WARN("sync write failed", K(ret), K(info));
          }
        } else {
          break;
        }
      }
    }
    if (OB_SUCC(ret) || OB_DATA_OUT_OF_RANGE == ret) {
      write_size = handle.get_data_size();
    }
  }
  return ret;
}

int ObIOManager::detect_read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(get_tenant_io_manager(info.tenant_id_, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(info.tenant_id_));
  } else if (OB_FAIL(tenant_holder.get_ptr()->detect_aio(info, handle))) {
    LOG_WARN("tenant io manager do aio failed", K(ret), K(info), KPC(tenant_holder.get_ptr()));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    LOG_WARN("io handle wait failed", K(ret), K(info), K(timeout_ms));
  }
  return ret;
}

int ObIOManager::tenant_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
#ifdef ERRSIM
  const ObErrsimModuleType type = THIS_WORKER.get_module_type();
  if (is_errsim_module(info.tenant_id_, type.type_)) {
    ret = OB_IO_ERROR;
    LOG_ERROR("[ERRSIM MODULE] errsim IO error", K(ret), "tenant_id", info.tenant_id_);
    return ret;
  }
#endif

  if (OB_FAIL(get_tenant_io_manager(info.tenant_id_, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(info.tenant_id_));
  } else if (OB_FAIL(tenant_holder.get_ptr()->inner_aio(info, handle))) {
    LOG_WARN("tenant io manager do aio failed", K(ret), K(info), KPC(tenant_holder.get_ptr()));
  }
  return ret;
}

int ObIOManager::adjust_tenant_clock()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObTenantIOManager *tenant_io_mgr = nullptr;
    ObArray<ObTenantIOClock *> io_clocks;
    ObVector<uint64_t> tenant_ids;
    if (OB_NOT_NULL(GCTX.omt_)) {
      GCTX.omt_->get_tenant_ids(tenant_ids);
    }
    for (int64_t i = 0; i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (OB_FAIL(get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
      } else if (FALSE_IT(tenant_io_mgr = tenant_holder.get_ptr())) {
      } else if (OB_FAIL(io_clocks.push_back(tenant_io_mgr->get_io_clock()))) {
        LOG_WARN("push back io clock failed", K(ret), K(tenant_ids.size()));
      }
    }
    if (!io_clocks.empty()) {
      if (OB_FAIL(ObTenantIOClock::sync_clocks(io_clocks))) {
        LOG_WARN("sync io clocks failed", K(ret), K(io_clocks));
      }
    }
  }
  return ret;
}

int ObIOManager::set_io_config(const ObIOConfig &conf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObIOManager has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!conf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(conf), K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    io_config_ = conf;
    LOG_INFO("Success to config io manager, ", K(conf));
  }
  return ret;
}

const ObIOConfig &ObIOManager::get_io_config() const
{
  return io_config_;
}

ObIOFaultDetector &ObIOManager::get_device_health_detector()
{
  return fault_detector_;
}

int ObIOManager::get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time)
{
  return fault_detector_.get_device_health_status(dhs, device_abnormal_time);
}

int ObIOManager::reset_device_health()
{
  int ret = OB_SUCCESS;
  fault_detector_.reset_device_health();
  return ret;
}

int ObIOManager::add_device_channel(ObIODevice *device_handle,
                                    const int64_t async_channel_count,
                                    const int64_t sync_channel_count,
                                    const int64_t max_io_depth)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(device_handle) || async_channel_count < 0 || sync_channel_count <= 0 || max_io_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle), K(async_channel_count), K(sync_channel_count), K(max_io_depth));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDeviceChannel)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc device channel failed", K(ret));
  } else if (FALSE_IT(device_channel = new (buf) ObDeviceChannel)) {
  } else if (OB_FAIL(device_channel->init(device_handle,
                                          async_channel_count,
                                          sync_channel_count,
                                          max_io_depth,
                                          allocator_))) {
    LOG_WARN("init device_channel failed", K(ret), K(async_channel_count), K(sync_channel_count));
  } else if (OB_FAIL(channel_map_.set_refactored(reinterpret_cast<int64_t>(device_handle), device_channel))) {
    LOG_WARN("set channel map failed", K(ret), KP(device_handle));
  } else {
    LOG_INFO("add io device channel succ", KP(device_handle));
    device_channel = nullptr;
  }
  if (OB_UNLIKELY(nullptr != device_channel)) {
    device_channel->~ObDeviceChannel();
    allocator_.free(device_channel);
  }
  return ret;
}

int ObIOManager::remove_device_channel(ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle));
  } else if (OB_FAIL(channel_map_.erase_refactored(reinterpret_cast<int64_t>(device_handle), &device_channel))) {
    LOG_WARN("remove from channel map failed", K(ret), KP(device_handle));
  } else if (nullptr != device_channel) {
    device_channel->~ObDeviceChannel();
    allocator_.free(device_channel);
  }
  return ret;
}

int ObIOManager::get_device_channel(const ObIODevice *device_handle, ObDeviceChannel *&device_channel)
{
  int ret = OB_SUCCESS;
  device_channel = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle));
  } else if (OB_FAIL(channel_map_.get_refactored(reinterpret_cast<int64_t>(device_handle), device_channel))) {
    LOG_WARN("get device channel failed", K(ret), KP(device_handle));
  }
  return ret;
}

int ObIOManager::refresh_tenant_io_config(const uint64_t tenant_id, const ObTenantIOConfig &tenant_io_config)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
                         tenant_io_config.memory_limit_ <= 0 ||
                         tenant_io_config.callback_thread_count_ <= 0 ||
                         !tenant_io_config.unit_config_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->update_basic_io_config(tenant_io_config))) {
    LOG_WARN("update tenant io config failed", K(ret), K(tenant_id), K(tenant_io_config));
  }
  return ret;
}

// for unittest
int ObIOManager::modify_group_io_config(const uint64_t tenant_id,
                                        const uint64_t index,
                                        const int64_t min_percent,
                                        const int64_t max_percent,
                                        const int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->modify_group_io_config(index, min_percent, max_percent, weight_percent, false, false))) {
    LOG_WARN("update tenant io config failed", K(ret), K(tenant_id), K(min_percent), K(max_percent), K(weight_percent));
  } else if (OB_FAIL(tenant_holder.get_ptr()->refresh_group_io_config())) {
    LOG_WARN("fail to refresh group config", K(ret));
  }
  return ret;
}

int ObIOManager::get_tenant_io_manager(const uint64_t tenant_id, ObRefHolder<ObTenantIOManager> &tenant_holder)
{
  int ret = OB_SUCCESS;
  if (OB_SERVER_TENANT_ID == tenant_id) {
    tenant_holder.hold(server_io_manager_);
  } else if (is_virtual_tenant_id(tenant_id)) {
    // DO NOT SUPPORT
  } else if (MTL_ID() == tenant_id) {
    ObTenantIOManager *tenant_io_mgr = MTL(ObTenantIOManager*);
    tenant_holder.hold(tenant_io_mgr);
  } else {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_SUCC(guard.switch_to(tenant_id, false))) {
      ObTenantIOManager *tenant_io_mgr = MTL(ObTenantIOManager*);
      tenant_holder.hold(tenant_io_mgr);
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_HASH_NOT_EXIST; // for compatibility
  }
  return ret;
}

ObIOScheduler *ObIOManager::get_scheduler()
{
  return &io_scheduler_;
}

/******************             TenantIOManager              **********************/

int ObTenantIOManager::mtl_new(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  io_service = nullptr;
  if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTenantIOManager), ObMemAttr(MTL_ID(), "IO_MGR")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    FLOG_WARN("failed to alloc tenant io mgr", K(ret));
  } else {
    io_service = new (buf) ObTenantIOManager();
  }
  return ret;
}

int ObTenantIOManager::mtl_init(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(io_service)) {
    if (is_virtual_tenant_id(tenant_id)) {
      // do nothing
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else if (OB_FAIL(io_service->init(tenant_id,
                                      ObTenantIOConfig::default_instance(),
                                      &OB_IO_MANAGER.io_scheduler_))) {
    FLOG_WARN("mtl iit tenant io manager failed", K(tenant_id));
  } else {
    FLOG_INFO("mtl init tenant io manager success", K(tenant_id), KPC(io_service));
  }
  return ret;
}

void ObTenantIOManager::mtl_destroy(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(io_service)) {
    io_service->~ObTenantIOManager();
    ob_free(io_service);
    io_service = nullptr;
    FLOG_INFO("mtl destroy tenant io manager success", K(MTL_ID()));
  }
}

ObTenantIOManager::ObTenantIOManager()
  : is_inited_(false),
    is_working_(false),
    ref_cnt_(0),
    tenant_id_(0),
    io_config_(),
    io_clock_(nullptr),
    io_allocator_(),
    io_scheduler_(nullptr),
    callback_mgr_(),
    io_config_lock_(ObLatchIds::TENANT_IO_CONFIG_LOCK)
{

}

ObTenantIOManager::~ObTenantIOManager()
{
  destroy();
}

int ObTenantIOManager::init(const uint64_t tenant_id,
         const ObTenantIOConfig &io_config,
         ObIOScheduler *io_scheduler)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
        || !io_config.is_valid()
        || nullptr == io_scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(io_config), KP(io_scheduler));
  } else if (OB_FAIL(io_allocator_.init(tenant_id, io_config.memory_limit_))) {
    LOG_WARN("init io allocator failed", K(ret), K(tenant_id), K(io_config.memory_limit_));
  } else if (OB_FAIL(io_tracer_.init(tenant_id))) {
    LOG_WARN("init io tracer failed", K(ret));
  } else if (OB_FAIL(alloc_io_clock(io_allocator_, io_clock_))) {
    LOG_WARN("alloc io clock failed", K(ret));
  } else if (OB_FAIL(io_usage_.init(tenant_id, io_config.group_num_))) {
     LOG_WARN("init io usage failed", K(ret), K(io_usage_), K(io_config.group_num_));
  } else if (OB_FAIL(io_backup_usage_.init())) {
     LOG_WARN("init io usage failed", K(ret), K(io_backup_usage_), K(SYS_MODULE_CNT));
  } else if (OB_FAIL(io_clock_->init(io_config, &io_usage_))) {
    LOG_WARN("init io clock failed", K(ret), K(io_config));
  } else if (OB_FAIL(io_scheduler->init_group_queues(tenant_id, io_config.group_num_, &io_allocator_))) {
    LOG_WARN("init io map failed", K(ret), K(tenant_id), K(io_allocator_));
  } else if (OB_FAIL(init_group_index_map(tenant_id, io_config))) {
    LOG_WARN("init group map failed", K(ret));
  } else if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("copy io config failed", K(ret), K(io_config_));
  } else {
    tenant_id_ = tenant_id;
    io_scheduler_ = io_scheduler;
    inc_ref();
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObTenantIOManager::destroy()
{
  ATOMIC_STORE(&is_working_, false);

  const int64_t start_ts = ObTimeUtility::current_time();
  if (is_inited_) {
    while (1 != get_ref_cnt()) {
      if (REACH_TIME_INTERVAL(1000L * 1000L)) { //1s
        LOG_INFO("wait tenant io manager quit", K(MTL_ID()), K(start_ts), K(get_ref_cnt()));
      }
      ob_usleep((useconds_t)10L * 1000L); //10ms
    }
    dec_ref();
  }

  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(io_scheduler_) && OB_FAIL(io_scheduler_->remove_phyqueues(MTL_ID()))) {
    LOG_WARN("remove phy_queues from map failed", K(ret), K(MTL_ID()));
  }

  if (OB_NOT_NULL(io_clock_)) {
    io_clock_->destroy();
    io_allocator_.free(io_clock_);
    io_clock_ = nullptr;
  }
  callback_mgr_.destroy();
  io_tracer_.destroy();
  io_scheduler_ = nullptr;
  tenant_id_ = 0;
  group_id_index_map_.destroy();
  io_allocator_.destroy();
  is_inited_ = false;
}

int ObTenantIOManager::start()
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_QUEUE_DEPTH = 100000;
  int64_t thread_count = io_config_.callback_thread_count_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (is_working()) {
    // do nothing
  } else if (OB_FAIL(callback_mgr_.init(tenant_id_, thread_count, DEFAULT_QUEUE_DEPTH * thread_count))) {
    LOG_WARN("init callback manager failed", K(ret), K(tenant_id_), K(thread_count));
  } else {
    is_working_ = true;
    int tmp_ret = OB_IO_MANAGER.adjust_tenant_clock();
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("adjust tenant clock failed", K(tmp_ret));
    }
  }
  return ret;
}

void ObTenantIOManager::stop()
{
  ATOMIC_STORE(&is_working_, false);
  callback_mgr_.destroy();
}

bool ObTenantIOManager::is_working() const
{
  return ATOMIC_LOAD(&is_working_);
}

int ObTenantIOManager::inner_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObIORequest *req = nullptr;
  const int64_t callback_size = nullptr == info.callback_ ? 0 : info.callback_->size();
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if ((SLOG_IO != info.flag_.get_sys_module_id() && CLOG_IO != info.flag_.get_sys_module_id()) &&
             NULL != detector && detector->is_data_disk_has_fatal_error()) {
    ret = OB_DISK_HUNG;
    // for temporary positioning issue, get lbt of log replay
    LOG_DBA_ERROR(OB_DISK_HUNG, "msg", "data disk has fatal error");
  } else if (OB_FAIL(alloc_io_request(io_allocator_, callback_size, req))) {
    LOG_WARN("alloc io request failed", K(ret), KP(req));
  } else if (FALSE_IT(req->tenant_io_mgr_.hold(this))) {
  } else if (OB_FAIL(handle.set_request(*req))) { // not safe, unless handle is only used after this function.
    LOG_WARN("fail to set master to handle", K(ret), KP(req));
  } else if (OB_FAIL(req->init(info))) {
    LOG_WARN("init request failed", K(ret), K(info), KPC(req));
  } else if (OB_FAIL(io_scheduler_->schedule_request(*req))) {
    LOG_WARN("schedule request failed", K(ret), KPC(req));
  }
  if (OB_FAIL(ret)) {
    handle.reset();
  }
  return ret;
}

int ObTenantIOManager::detect_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObIORequest *req = nullptr;
  ObDeviceChannel *device_channel = nullptr;
  ObTimeGuard time_guard("detect_aio_request", 100000); //100ms

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(info.callback_ != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback should be nullptr", K(ret), K(info.callback_));
  } else if (OB_FAIL(alloc_io_request(io_allocator_, 0 /*callback = null*/, req))) {
    LOG_WARN("alloc io request failed", K(ret), KP(req));
  } else if (FALSE_IT(req->tenant_io_mgr_.hold(this))) {
  } else if (OB_FAIL(handle.set_request(*req))) { // not safe, unless handle is only used after this function.
    LOG_WARN("fail to set master to handle", K(ret), KP(req));
  } else if (OB_FAIL(req->init(info))) {
    LOG_WARN("init request failed", K(ret), K(info), KPC(req));
  } else if (OB_FAIL(req->prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (FALSE_IT(time_guard.click("prepare_detect_req"))) {
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_channel(req->io_info_.fd_.device_handle_, device_channel))) {
    LOG_WARN("get device channel failed", K(ret), K(req));
  } else {
    ObThreadCondGuard guard(req->cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to guard master condition", K(ret));
    } else if (req->is_canceled_) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(device_channel->submit(*req))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("submit io request failed", K(ret), K(*req), KPC(device_channel));
      }
    } else {
      time_guard.click("device_submit_detect");
    }
  }
  if (time_guard.get_diff() > 100000) {// 100ms
    //print req
    LOG_INFO("submit_detect_request cost too much time", K(ret), K(time_guard), K(req));
  }
  return ret;
}

int ObTenantIOManager::enqueue_callback(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_FAIL(callback_mgr_.enqueue_callback(req))) {
    LOG_WARN("push io request into callback queue failed", K(ret), K(req));
  }
  return ret;
}

int ObTenantIOManager::update_basic_io_config(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (io_config == io_config_) {
    // basic config not change, do nothing
  } else {
    MTL_SWITCH(tenant_id_) {
      if (OB_FAIL(io_allocator_.update_memory_limit(io_config.memory_limit_))) {
        LOG_WARN("update memory limit failed", K(ret), K(io_config.memory_limit_));
      } else if (OB_FAIL(callback_mgr_.update_thread_count(io_config.callback_thread_count_))) {
        LOG_WARN("callback manager adjust thread failed", K(ret), K(io_config));
      } else {
        // just update basic config
        DRWLock::WRLockGuard guard(io_config_lock_);
        io_config_.memory_limit_ = io_config.memory_limit_;
        io_config_.callback_thread_count_ = io_config.callback_thread_count_;
        io_config_.unit_config_ = io_config.unit_config_;
        ATOMIC_SET(&io_config_.enable_io_tracer_, io_config.enable_io_tracer_);
        if (!io_config.enable_io_tracer_) {
          io_tracer_.reuse();
        }
        if (OB_FAIL(io_clock_->update_io_clocks(io_config_))) {
          LOG_WARN("refresh io clock failed", K(ret), K(io_config_));
        } else {
          LOG_INFO("update basic io config success", K(tenant_id_), K(io_config_), K(io_config), KPC(io_clock_));
        }
      }
    }
  }
  return ret;
}

int ObTenantIOManager::alloc_io_request(ObIAllocator &allocator, const int64_t callback_size, ObIORequest *&req)
{
  int ret = OB_SUCCESS;
  req = nullptr;
  void *buf = nullptr;
  const int64_t req_size = sizeof(ObIORequest) + callback_size;
  if (OB_UNLIKELY(callback_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(callback_size));
  } else if (OB_ISNULL(buf = allocator.alloc(req_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(req_size), K(callback_size));
  } else {
    req = new (buf) ObIORequest;
    req->callback_buf_size_ = callback_size;
  }
  return ret;
}

int ObTenantIOManager::alloc_io_clock(ObIAllocator &allocator, ObTenantIOClock *&io_clock)
{
  int ret = OB_SUCCESS;
  io_clock = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTenantIOClock)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    io_clock = new (buf) ObTenantIOClock;
  }
  return ret;
}

int ObTenantIOManager::init_group_index_map(const int64_t tenant_id,
                                            const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "GROUP_INDEX_MAP");
  if (OB_FAIL(group_id_index_map_.create(7, attr, attr))) {
    LOG_WARN("create group index map failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < io_config.group_num_; ++i) {
      if(OB_FAIL(group_id_index_map_.set_refactored(io_config.group_ids_.at(i), i, 1 /*overwrite*/))) {
        LOG_WARN("init group_index_map failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTenantIOManager::get_group_index(const int64_t group_id, uint64_t &index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid group id", K(ret), K(group_id));
  } else if (OB_FAIL(group_id_index_map_.get_refactored(group_id, index))) {
    if(OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
    }
  } else if (OB_UNLIKELY(index == INT64_MAX)) {
    //index == INT64_MAX means group has been deleted
    ret = OB_STATE_NOT_MATCH;
  }
  return ret;
}

int ObTenantIOManager::get_group_config(const int64_t group_id, ObTenantIOConfig::GroupConfig &group_config) const
{
  int ret = OB_SUCCESS;
  uint64_t index = INT64_MAX;
  if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid group id", K(ret), K(group_id));
  } else if (OB_FAIL(group_id_index_map_.get_refactored(group_id, index))) {
    if(OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
    }
  } else if (OB_UNLIKELY(index == INT64_MAX)) {
    // other groups
    group_config = io_config_.other_group_config_;
  } else if (OB_UNLIKELY(index < 0 || index >= io_config_.group_configs_.count())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(index));
  } else {
    group_config = io_config_.group_configs_.at(index);
  }
  return ret;
}

int ObTenantIOManager::modify_group_io_config(const uint64_t index,
                                              const int64_t min_percent,
                                              const int64_t max_percent,
                                              const int64_t weight_percent,
                                              const bool deleted,
                                              const bool cleared)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (index < 0 || (index >= io_config_.group_configs_.count() && index != INT64_MAX) ||
             min_percent < 0 || min_percent > 100 || max_percent < 0 || max_percent > 100 ||
             max_percent < min_percent || weight_percent < 0 || weight_percent > 100) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(index), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    if (INT64_MAX == index) {
      // other groups
      io_config_.other_group_config_.min_percent_ = min_percent;
      io_config_.other_group_config_.max_percent_ = max_percent;
      io_config_.other_group_config_.weight_percent_ = weight_percent;
      io_config_.other_group_config_.cleared_ = cleared;
      io_config_.other_group_config_.deleted_ = deleted;
    } else {
      io_config_.group_configs_.at(index).min_percent_ = min_percent;
      io_config_.group_configs_.at(index).max_percent_ = max_percent;
      io_config_.group_configs_.at(index).weight_percent_ = weight_percent;
      io_config_.group_configs_.at(index).cleared_ = cleared;
      io_config_.group_configs_.at(index).deleted_ = deleted;
    }
    io_config_.group_config_change_ = true;
  }
  return ret;
}

int ObTenantIOManager::modify_io_config(const uint64_t group_id,
                                        const int64_t min_percent,
                                        const int64_t max_percent,
                                        const int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else {
    uint64_t index = INT64_MAX;
    DRWLock::WRLockGuard guard(io_config_lock_);
    if (OB_UNLIKELY(!is_valid_resource_group(group_id))) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid group id", K(ret), K(tenant_id_), K(group_id));
    } else if (min_percent < 0 || min_percent > 100 ||
               max_percent < 0 || max_percent > 100 ||
               max_percent < min_percent || weight_percent < 0 || weight_percent > 100) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid group config", K(ret), K(tenant_id_), K(min_percent), K(max_percent), K(weight_percent));
    } else if (0 == group_id) {
      //1. modify OTHER GROUPS
      if (io_config_.other_group_config_.min_percent_ == min_percent &&
          io_config_.other_group_config_.max_percent_ == max_percent &&
          io_config_.other_group_config_.weight_percent_ == weight_percent) {
        //config did not change, do nothing
      } else if (OB_FAIL(modify_group_io_config(INT64_MAX, min_percent, max_percent, weight_percent))) {
        LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(min_percent), K(max_percent), K(weight_percent));
      }
    } else if (OB_FAIL(get_group_index(group_id, index))) {
      if (OB_STATE_NOT_MATCH == ret) {
        //group has been deleted, do nothing
        LOG_INFO("group has been deleted before flush directive", K(group_id), K(index));
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        //2. add new group
        if (OB_FAIL(add_group_io_config(group_id, min_percent, max_percent, weight_percent))) {
          LOG_WARN("add consumer group failed", K(ret), K(tenant_id_), K(group_id));
        } else {
          io_config_.group_config_change_ = true;
        }
      } else {
        LOG_WARN("get group index failed", K(ret), K(tenant_id_), K(group_id));
      }
    } else {
      //3. modify exits groups
      if (index < 0 || (index >= io_config_.group_configs_.count() && index != INT64_MAX)) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid index", K(ret), K(index), K(io_config_.group_configs_.count()));
      } else if (io_config_.group_configs_.at(index).min_percent_ == min_percent &&
                 io_config_.group_configs_.at(index).max_percent_ == max_percent &&
                 io_config_.group_configs_.at(index).weight_percent_ == weight_percent) {
        //config did not change, do nothing
      } else {
        if (io_config_.group_configs_.at(index).cleared_) {
          //并发状态可能先被clear
          io_config_.group_configs_.at(index).cleared_ = false;
        } else if (OB_FAIL(modify_group_io_config(index, min_percent, max_percent, weight_percent))) {
          LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(min_percent), K(max_percent), K(weight_percent));
        }
      }
    }
  }
  return ret;
}

int ObTenantIOManager::add_group_io_config(const int64_t group_id,
                                           const int64_t min_percent,
                                           const int64_t max_percent,
                                           const int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_resource_manager_group(group_id)) || min_percent < 0 || min_percent > 100 ||
             max_percent < 0 || max_percent > 100 || max_percent < min_percent ||
             weight_percent < 0 || weight_percent > 100) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(ret), K(group_id), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    int64_t group_num = ATOMIC_LOAD(&io_config_.group_num_);
    if (OB_FAIL(io_config_.add_single_group_config(tenant_id_, group_id, min_percent, max_percent, weight_percent))) {
      LOG_WARN("init single group failed", K(group_id));
    } else if (OB_FAIL(group_id_index_map_.set_refactored(group_id, group_num, 1))) {// overwrite
      LOG_WARN("set group_id and index into map failed", K(ret), K(group_id), K(group_num));
    } else {
      ATOMIC_INC(&io_config_.group_num_);
      LOG_INFO("add group config success", K(group_id), K(io_config_));
    }
  }
  return ret;
}

int ObTenantIOManager::reset_all_group_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else {
    DRWLock::WRLockGuard guard(io_config_lock_);
    for (int64_t i = 0; i < io_config_.group_num_; ++i) {
      if(io_config_.group_configs_.at(i).deleted_) {
        //do nothing
      } else if (OB_FAIL(modify_group_io_config(i, 0, 100, 0, false, true/*cleared*/))) {
         LOG_WARN("modify group io config failed", K(ret), K(i));
      }
    }
    if(OB_SUCC(ret)) {
      LOG_INFO ("stop all group io control success when delete plan", K(tenant_id_), K(io_config_));
    }
  }
  return ret;
}

int ObTenantIOManager::reset_consumer_group_config(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("cannot reset other group io config", K(ret), K(group_id));
  } else {
    // 对应group资源清零
    uint64_t index = INT_MAX64;
    DRWLock::WRLockGuard guard(io_config_lock_);
    if (OB_FAIL(get_group_index(group_id, index))) {
      if (OB_HASH_NOT_EXIST == ret) {
        //directive not flush yet, do nothing
        ret = OB_SUCCESS;
        LOG_INFO("directive not flush yet", K(group_id));
      } else if (OB_STATE_NOT_MATCH == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("group has been deleted", K(group_id));
      } else {
        LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
      }
    } else if (OB_UNLIKELY(index == INT64_MAX || index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index, maybe try to reset OTHER_GROUPS or deleted_groups", K(ret), K(index), K(group_id));
    } else if (OB_FAIL(modify_group_io_config(index, 0, 100, 0, false, true/*cleared*/))) {
      LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(index));
    } else {
      LOG_INFO ("stop group io control success when delete directive", K(tenant_id_), K(group_id), K(index), K(io_config_));
    }
  }
  return ret;
}

int ObTenantIOManager::delete_consumer_group_config(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("cannot delete other group io config", K(ret), K(group_id));
  } else {
    // 1.map设置非法值
    // 2.config设为unusable，资源清零
    // 3.phyqueue停止接受新请求，但是不会析构
    // 4.clock设置为stop，但是不会析构
    // 5.io_usage暂不处理
    uint64_t index = INT_MAX64;
    DRWLock::WRLockGuard guard(io_config_lock_);
    if (OB_FAIL(get_group_index(group_id, index))) {
      if (OB_HASH_NOT_EXIST == ret) {
        //GROUP 没有在map里，可能是没有指定资源，io未对其进行隔离或还未下刷
        ret = OB_SUCCESS;
        LOG_INFO("io control not active for this group", K(group_id));
        if (OB_FAIL(group_id_index_map_.set_refactored(group_id, INT64_MAX, 1))) { //使用非法值覆盖
          LOG_WARN("stop phy queues failed", K(ret), K(tenant_id_), K(index));
        }
      } else if (OB_STATE_NOT_MATCH == ret) {
        // group delete twice, maybe deleted by delete_directive or delete_plan
        LOG_INFO("group delete twice", K(ret), K(index), K(group_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
      }
    } else if (OB_UNLIKELY(index == INT64_MAX || index < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index, maybe try to delete OTHER_GROUPS or deleted_groups", K(ret), K(index), K(group_id));
    } else {
      if (OB_FAIL(group_id_index_map_.set_refactored(group_id, INT64_MAX, 1))) { //使用非法值覆盖
        LOG_WARN("stop phy queues failed", K(ret), K(tenant_id_), K(index));
      } else if (OB_FAIL(modify_group_io_config(index, 0, 100, 0, true/*deleted*/, false))) {
        LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(index));
      }
    }
    if (OB_SUCC(ret) && index != INT64_MAX) {
      if (OB_FAIL(io_scheduler_->stop_phy_queues(tenant_id_, index))) {
        LOG_WARN("stop phy queues failed", K(ret), K(tenant_id_), K(index));
      } else {
        io_clock_->stop_clock(index);
        LOG_INFO ("stop group io control success when delete group", K(tenant_id_), K(group_id), K(index), K(io_config_));
      }
    }
  }
  return ret;
}

int ObTenantIOManager::refresh_group_io_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_LIKELY(!io_config_.group_config_change_)) {
    // group config not change, do nothing
  } else if (OB_FAIL(io_usage_.refresh_group_num(io_config_.group_num_))) {
    LOG_WARN("refresh io usage array failed", K(ret), K(io_usage_.get_io_usage_num()), K(io_config_.group_num_));
  } else if (OB_FAIL(io_scheduler_->update_group_queues(tenant_id_, io_config_.group_num_))) {
    LOG_WARN("refresh phyqueue num failed", K(ret), K(io_config_.group_num_));
  } else if (OB_FAIL(io_clock_->update_io_clocks(io_config_))) {
    LOG_WARN("refresh io clock failed", K(ret), K(io_config_));
  } else {
    LOG_INFO("refresh group io config success", K(tenant_id_), K(io_config_));
    io_config_.group_config_change_ = false;
  }

  return ret;
}

const ObTenantIOConfig &ObTenantIOManager::get_io_config()
{
  return io_config_;
}

int ObTenantIOManager::trace_request_if_need(const ObIORequest *req, const char* msg, ObIOTracer::TraceType trace_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_LIKELY(!ATOMIC_LOAD(&io_config_.enable_io_tracer_))) {
    // do nothing
  } else if (OB_FAIL(io_tracer_.trace_request(req, msg, trace_type))) {
    LOG_WARN("trace io request failed", K(ret), KP(req), KCSTRING(msg), K(trace_type));
  }
  return ret;
}

int64_t ObTenantIOManager::get_group_num()
{
  DRWLock::RDLockGuard guard(io_config_lock_);
  int64_t group_num = io_config_.group_num_;
  return group_num;
}

uint64_t ObTenantIOManager::get_usage_index(const int64_t group_id)
{
  uint64_t index = INT64_MAX;
  int ret = group_id_index_map_.get_refactored(group_id, index);
  if (OB_FAIL(ret) || index == INT64_MAX) {
    //maybe deleted or reset
    index = 0;
  } else {
    //other group occupies the first place, so return index + 1
    index += 1;
  }
  return index;
}

void ObTenantIOManager::print_io_status()
{
  int ret = OB_SUCCESS;
  if (is_working() && is_inited_) {
    class IOStatusLog
    {
    public:
      IOStatusLog(ObTenantIOManager *io_manager, ObIOUsage::AvgItems *avg_size, ObIOUsage::AvgItems *avg_iops,
          ObIOUsage::AvgItems *avg_rt)
          : io_manager_(io_manager), avg_size_(avg_size), avg_iops_(avg_iops), avg_rt_(avg_rt)
      {}
      ~IOStatusLog()
      {}
      int64_t to_string(char *buf, const int64_t len) const
      {
        int64_t pos = 0;

        for (int64_t i = 1; i < io_manager_->io_usage_.get_io_usage_num() && i < avg_size_->count() &&
                            i < avg_iops_->count() && i < avg_rt_->count();
             ++i) {
          if (!io_manager_->io_config_.group_configs_.at(i - 1).deleted_) {
            if (avg_size_->at(i).at(static_cast<int>(ObIOMode::READ)) > std::numeric_limits<double>::epsilon()) {
              databuff_printf(buf,
                  len,
                  pos,
                  "group_id: %ld, mode:  read, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
                  io_manager_->io_config_.group_ids_.at(i - 1),
                  avg_size_->at(i).at(static_cast<int>(ObIOMode::READ)),
                  avg_iops_->at(i).at(static_cast<int>(ObIOMode::READ)),
                  avg_rt_->at(i).at(static_cast<int>(ObIOMode::READ)));
            }
            if (avg_size_->at(i).at(static_cast<int>(ObIOMode::WRITE)) > std::numeric_limits<double>::epsilon()) {
              databuff_printf(buf,
                  len,
                  pos,
                  "group_id: %ld, mode: write, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
                  io_manager_->io_config_.group_ids_.at(i - 1),
                  avg_size_->at(i).at(static_cast<int>(ObIOMode::WRITE)),
                  avg_iops_->at(i).at(static_cast<int>(ObIOMode::WRITE)),
                  avg_rt_->at(i).at(static_cast<int>(ObIOMode::WRITE)));
            }
          }
        }
        // OTHER_GROUPS
        if (avg_size_->at(0).at(static_cast<int>(ObIOMode::READ)) > std::numeric_limits<double>::epsilon()) {
          databuff_printf(buf,
              len,
              pos,
              "group_id: %ld, group_name: %s, mode:  read, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
              0L,
              "OTHER_GROUPS",
              avg_size_->at(0).at(static_cast<int>(ObIOMode::READ)),
              avg_iops_->at(0).at(static_cast<int>(ObIOMode::READ)),
              avg_rt_->at(0).at(static_cast<int>(ObIOMode::READ)));
        }
        if (avg_size_->at(0).at(static_cast<int>(ObIOMode::WRITE)) > std::numeric_limits<double>::epsilon()) {
          databuff_printf(buf,
              len,
              pos,
              "group_id: %ld, group_name: %s, mode: write, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
              0L,
              "OTHER_GROUPS",
              avg_size_->at(0).at(static_cast<int>(ObIOMode::WRITE)),
              avg_iops_->at(0).at(static_cast<int>(ObIOMode::WRITE)),
              avg_rt_->at(0).at(static_cast<int>(ObIOMode::WRITE)));
        }
        return pos;
      }
      ObTenantIOManager *io_manager_;
      ObIOUsage::AvgItems *avg_size_;
      ObIOUsage::AvgItems *avg_iops_;
      ObIOUsage::AvgItems *avg_rt_;
    };
    ObIOUsage::AvgItems avg_size;
    ObIOUsage::AvgItems avg_iops;
    ObIOUsage::AvgItems avg_rt;
    io_usage_.calculate_io_usage();
    bool need_print_io_status = false;
    if (OB_FAIL(io_usage_.get_io_usage(avg_iops, avg_size, avg_rt))) {
      LOG_ERROR("fail to get io usage", K(ret));
    } else {
      IOStatusLog io_status_log(this, &avg_size, &avg_iops, &avg_rt);
      for (int64_t i = 0; !need_print_io_status && i < io_usage_.get_io_usage_num() && i < avg_size.count(); ++i) {
        if (i == 0 || (i > 0 && !io_config_.group_configs_.at(i - 1).deleted_)) {
          if (avg_size.at(i).at(static_cast<int>(ObIOMode::READ)) > std::numeric_limits<double>::epsilon() ||
              avg_size.at(i).at(static_cast<int>(ObIOMode::WRITE)) > std::numeric_limits<double>::epsilon()) {
            need_print_io_status = true;
          }
        }
      }
      if (need_print_io_status) {
        LOG_INFO("[IO STATUS]", K_(tenant_id), K(io_status_log));
      }
    }

    // MOCK SYS GROUPS
    class IOStatusSysLog
    {
    public:
      IOStatusSysLog(ObTenantIOManager *io_manager, ObSysIOUsage::SysAvgItems *sys_avg_iops,
          ObSysIOUsage::SysAvgItems *sys_avg_size, ObSysIOUsage::SysAvgItems *sys_avg_rt)
          : io_manager_(io_manager), sys_avg_iops_(sys_avg_iops), sys_avg_size_(sys_avg_size), sys_avg_rt_(sys_avg_rt)
      {}
      ~IOStatusSysLog()
      {}
      int64_t to_string(char *buf, const int64_t len) const
      {
        int64_t pos = 0;

        for (int64_t j = 0; j < sys_avg_size_->count(); ++j) {
          if (j >= sys_avg_size_->count() || j >= sys_avg_iops_->count() || j >= sys_avg_rt_->count()) {
            // ignore
          } else {
            ObIOModule module = static_cast<ObIOModule>(SYS_MODULE_START_ID + j);
            if (sys_avg_size_->at(j).at(static_cast<int>(ObIOMode::READ)) > std::numeric_limits<double>::epsilon()) {
              databuff_printf(buf,
                  len,
                  pos,
                  "sys_group_name: %s, mode:  read, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
                  get_io_sys_group_name(module),
                  sys_avg_size_->at(j).at(static_cast<int>(ObIOMode::READ)),
                  sys_avg_iops_->at(j).at(static_cast<int>(ObIOMode::READ)),
                  sys_avg_rt_->at(j).at(static_cast<int>(ObIOMode::READ)));
            }
            if (sys_avg_size_->at(j).at(static_cast<int>(ObIOMode::WRITE)) > std::numeric_limits<double>::epsilon()) {
              databuff_printf(buf,
                  len,
                  pos,
                  "sys_group_name: %s, mode: write, size: %10.2f, iops: %8.2f, rt: %8.2f; ",
                  get_io_sys_group_name(module),
                  sys_avg_size_->at(j).at(static_cast<int>(ObIOMode::WRITE)),
                  sys_avg_iops_->at(j).at(static_cast<int>(ObIOMode::WRITE)),
                  sys_avg_rt_->at(j).at(static_cast<int>(ObIOMode::WRITE)));
            }
          }
        }
        return pos;
      }
      ObTenantIOManager *io_manager_;
      ObSysIOUsage::SysAvgItems *sys_avg_iops_;
      ObSysIOUsage::SysAvgItems *sys_avg_size_;
      ObSysIOUsage::SysAvgItems *sys_avg_rt_;
    };
    ObSysIOUsage::SysAvgItems sys_avg_iops, sys_avg_size, sys_avg_rt;
    io_backup_usage_.calculate_io_usage();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(io_backup_usage_.get_io_usage(sys_avg_iops, sys_avg_size, sys_avg_rt))) {
      LOG_ERROR("fail to get io usage", K(ret));
    } else {
      IOStatusSysLog io_status_sys_log(this, &sys_avg_iops, &sys_avg_size, &sys_avg_rt);
      bool need_print_sys_io_status = false;
      for (int64_t j = 0; !need_print_sys_io_status && j < sys_avg_size.count(); ++j) {
        if (sys_avg_size.at(j).at(static_cast<int>(ObIOMode::READ)) > std::numeric_limits<double>::epsilon() ||
            sys_avg_size.at(j).at(static_cast<int>(ObIOMode::WRITE)) > std::numeric_limits<double>::epsilon()) {
          need_print_sys_io_status = true;
        }
      }
      if (need_print_sys_io_status) {
        LOG_INFO("[IO STATUS SYS]", K_(tenant_id), K(io_status_sys_log));
      }

      if (need_print_io_status || need_print_sys_io_status) {
        ObArray<int64_t> queue_count_array;
        int ret = OB_SUCCESS;
        int64_t callback_queues = callback_mgr_.get_queue_count();
        LOG_INFO("[IO STATUS CONFIG]",
            K_(tenant_id),
            K_(ref_cnt),
            K_(io_config),
            "allocated_memory",
            io_allocator_.get_allocated_size(),
            "pre_allocated_count",
            io_allocator_.get_pre_allocated_count(),
            "callback_queues",
            callback_queues);

        if (ATOMIC_LOAD(&io_config_.enable_io_tracer_)) {
          io_tracer_.print_status();
        }
      }
    }
  }
  // print io function status
  print_io_function_status();
}

int ObTenantIOManager::print_io_function_status()
{
  int ret = OB_SUCCESS;
  if (!is_working() || !is_inited_) {
    LOG_WARN("is not working or not inited", K(is_working()), K(is_inited_));
  } else {
    char io_status[1024] = { 0 };
    int FUNC_NUM = static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM);
    int GROUP_MODE_NUM = static_cast<uint8_t>(ObIOGroupMode::MODECNT);
    ObSEArray<ObIOFuncUsage, static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM)> &func_usages = io_func_infos_.func_usages_;
    double avg_size = 0;
    double avg_iops = 0;
    int64_t avg_bw = 0;
    int64_t avg_prepare_delay = 0;
    int64_t avg_schedule_delay = 0;
    int64_t avg_submit_delay = 0;
    int64_t avg_device_delay = 0;
    int64_t avg_total_delay = 0;
    for (int i = 0; i < FUNC_NUM; ++i) {
      for (int j = 0; j < GROUP_MODE_NUM; ++j) {
        avg_size = 0;
        const char *mode_str = get_io_mode_string(static_cast<ObIOGroupMode>(j));
        if (i >= func_usages.count()) {
          LOG_ERROR("func usages out of range", K(i), K(func_usages.count()));
        } else if (j >= func_usages.at(i).count()) {
          LOG_ERROR("func usages by mode out of range", K(i), K(j), K(func_usages.at(i).count()));
        } else if (OB_FAIL(func_usages.at(i).at(j).calc(
                       avg_size,
                       avg_iops,
                       avg_bw,
                       avg_prepare_delay,
                       avg_schedule_delay,
                       avg_submit_delay,
                       avg_device_delay,
                       avg_total_delay))) {
          LOG_WARN("fail to calc func usage", K(ret), K(i), K(j));
        } else if (avg_size < std::numeric_limits<double>::epsilon()) {
        } else {
          const char *func_name = get_io_function_name(static_cast<share::ObFunctionType>(i)).ptr();
          snprintf(io_status, sizeof(io_status),
                    "function_name:%s, mode:%s, avg_size:%ld, avg_iops:%ld, avg_bw:%ld, [delay/us]: prepare:%ld, schedule:%ld, submit:%ld, device:%ld, total:%ld",
                    func_name,
                    mode_str,
                    static_cast<int64_t>(avg_size + 0.5),
                    static_cast<int64_t>(avg_iops + 0.99),
                    avg_bw,
                    avg_prepare_delay,
                    avg_schedule_delay,
                    avg_submit_delay,
                    avg_device_delay,
                    avg_total_delay);
          LOG_INFO("[IO STATUS FUNCTION]", K_(tenant_id), KCSTRING(io_status));
        }
      }
    }
  }
  return ret;
}

void ObTenantIOManager::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObTenantIOManager::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bug: ref_cnt < 0", K(ret), K(tmp_ref));
    abort();
  }
}

int ObTenantIOManager::get_throttled_time(uint64_t group_id, int64_t &throttled_time)
{
  int ret = OB_SUCCESS;
  int64_t current_throttled_time_us = -1;
  if (OB_ISNULL(GCTX.cgroup_ctrl_) || !GCTX.cgroup_ctrl_->is_valid()) {
    // do nothing
  } else if (OB_FAIL(GCTX.cgroup_ctrl_->get_throttled_time(tenant_id_, current_throttled_time_us, group_id))) {
    LOG_WARN("get throttled time failed", K(ret), K(tenant_id_), K(group_id));
  } else if (current_throttled_time_us > 0) {
    uint64_t idx = 0;
    if (OB_FAIL(get_group_index(group_id, idx))) {
      LOG_WARN("get group index failed", K(ret), K(group_id));
    } else {
      throttled_time = current_throttled_time_us - io_usage_.group_throttled_time_us_.at(idx);
      io_usage_.group_throttled_time_us_.at(idx) = current_throttled_time_us;
    }
  }
  return ret;
}


int ObTenantIOManager::get_io_func_infos(ObIOFuncUsages &io_func_infos) const
{
  int ret = OB_SUCCESS;
  io_func_infos = io_func_infos_;
  return ret;
}
