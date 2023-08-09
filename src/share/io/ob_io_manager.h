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

#ifndef OCEANBASE_LIB_STORAGE_OB_IO_MANAGER_H
#define OCEANBASE_LIB_STORAGE_OB_IO_MANAGER_H

#include "common/storage/ob_io_device.h"
#include "share/io/ob_io_struct.h"

namespace oceanbase
{
namespace common
{

class ObTenantIOManager;

class ObIOManager final
{
public:
  static ObIOManager &get_instance();
  int init(const int64_t memory_limit = DEFAULT_MEMORY_LIMIT,
           const int32_t queue_depth = DEFAULT_QUEUE_DEPTH,
           const int32_t schedule_thread_count = 0,
           const int64_t schedule_media_id = 0);
  void destroy();
  int start();
  void stop();
  bool is_stopped() const;

  int read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms = MAX_IO_WAIT_TIME_MS);

  int write(const ObIOInfo &info, const uint64_t timeout_ms = MAX_IO_WAIT_TIME_MS);

  int aio_read(const ObIOInfo &info, ObIOHandle &handle);

  int aio_write(const ObIOInfo &info, ObIOHandle &handle);

  int pread(ObIOInfo &info, int64_t &read_size);

  int pwrite(ObIOInfo &info, int64_t &write_size);

  int detect_read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms);

  // config related, thread safe
  int set_io_config(const ObIOConfig &conf);
  const ObIOConfig &get_io_config() const;

  // device health management
  ObIOFaultDetector &get_device_health_detector();
  int get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time);
  int reset_device_health();

  // device channel management
  int add_device_channel(ObIODevice *device_handle,
                         const int64_t async_channel_count,
                         const int64_t sync_channel_count,
                         const int64_t max_io_depth);
  int remove_device_channel(ObIODevice *device_handle);
  int get_device_channel(const ObIODevice *device_handle, ObDeviceChannel *&device_channel);

  // tenant management
  int add_tenant_io_manager(const uint64_t tenant_id, const ObTenantIOConfig &tenant_io_config);
  int remove_tenant_io_manager(const uint64_t tenant_id);
  int refresh_tenant_io_config(const uint64_t tenant_id, const ObTenantIOConfig &tenant_io_config);
  int get_tenant_io_manager(const uint64_t tenant_id, ObRefHolder<ObTenantIOManager> &tenant_holder);
  int get_tenant_ids(ObIArray<uint64_t> &tenant_ids);
  ObIOScheduler *get_scheduler();

private:
  friend class ObTenantIOManager;
  static const int64_t DEFAULT_MEMORY_LIMIT = 10L * 1024L * 1024L * 1024L; // 10GB
  static const int32_t DEFAULT_QUEUE_DEPTH = 10000;
  ObIOManager();
  ~ObIOManager();
  int tenant_aio(const ObIOInfo &info, ObIOHandle &handle);
  int adjust_tenant_clock();
  DISABLE_COPY_ASSIGN(ObIOManager);
private:
  bool is_inited_;
  bool is_working_;
  lib::ObMutex mutex_;
  ObIOConfig io_config_;
  ObConcurrentFIFOAllocator allocator_;
  hash::ObHashMap<int64_t /*device_handle*/, ObDeviceChannel *> channel_map_;
  ObIOFaultDetector fault_detector_;
  ObIOScheduler io_scheduler_;
  DRWLock tenant_map_lock_;
  hash::ObHashMap<uint64_t /*tenant_id*/, ObTenantIOManager *> tenant_map_;
};

class ObTenantIOManager final
{
public:
  static int mtl_init(ObTenantIOManager *&io_service);
  static void mtl_destroy(ObTenantIOManager *&io_service);
public:
  ObTenantIOManager();
  ~ObTenantIOManager();
  int init(const uint64_t tenant_id,
           const ObTenantIOConfig &io_config,
           ObIOScheduler *io_scheduler);
  void destroy();
  int start();
  void stop();
  bool is_working() const;
  int inner_aio(const ObIOInfo &info, ObIOHandle &handle);
  int detect_aio(const ObIOInfo &info, ObIOHandle &handle);
  int enqueue_callback(ObIORequest &req);
  ObTenantIOClock *get_io_clock() { return io_clock_; }
  ObIOUsage &get_io_usage() { return io_usage_; }
  ObSysIOUsage &get_backup_io_usage() { return io_backup_usage_; }
  int update_basic_io_config(const ObTenantIOConfig &io_config);
  int alloc_io_request(ObIAllocator &allocator,const int64_t callback_size,  ObIORequest *&req);
  int alloc_io_clock(ObIAllocator &allocator, ObTenantIOClock *&io_clock);
  int init_group_index_map(const int64_t tenant_id, const ObTenantIOConfig &io_config);
  int get_group_index(const int64_t group_id, uint64_t &index);
  int modify_group_io_config(const uint64_t index,
                             const int64_t min_percent,
                             const int64_t max_percent,
                             const int64_t weight_percent,
                             const bool deleted = false,
                             const bool cleared = false);

  //for modify group config
  int modify_io_config(const uint64_t group_id,
                       const int64_t min_percent,
                       const int64_t max_percent,
                       const int64_t weight_percent);
  //for add group
  int add_group_io_config(const int64_t group_id,
                          const int64_t min_percent,
                          const int64_t max_percent,
                          const int64_t weight_percent);
  //for delete plan
  int reset_all_group_config();
  //for delete directive
  int reset_consumer_group_config(const int64_t group_id);
  //for delete group
  int delete_consumer_group_config(const int64_t group_id);
  //随directive refresh而定期刷新(最晚10S一次)
  int refresh_group_io_config();
  const ObTenantIOConfig &get_io_config();
  int trace_request_if_need(const ObIORequest *req, const char* msg, ObIOTracer::TraceType trace_type);
  int64_t get_group_num();
  int64_t get_ref_cnt() { return ATOMIC_LOAD(&ref_cnt_); }
  uint64_t get_usage_index(const int64_t group_id);
  ObIOAllocator *get_tenant_io_allocator() { return &io_allocator_; }
  void print_io_status();
  void inc_ref();
  void dec_ref();
  TO_STRING_KV(K(is_inited_), K(ref_cnt_), K(tenant_id_), K(io_config_), K(io_clock_),
       K(io_allocator_), KPC(io_scheduler_), K(callback_mgr_));
private:
  friend class ObIORequest;
  bool is_inited_;
  bool is_working_;
  int64_t ref_cnt_;
  uint64_t tenant_id_;
  ObTenantIOConfig io_config_;
  ObTenantIOClock *io_clock_;
  ObIOAllocator io_allocator_;
  ObIOScheduler *io_scheduler_;
  ObIOCallbackManager callback_mgr_;
  ObIOUsage io_usage_;
  ObSysIOUsage io_backup_usage_; //for backup mock group
  ObIOTracer io_tracer_;
  DRWLock io_config_lock_; //for map and config
  hash::ObHashMap<uint64_t, uint64_t> group_id_index_map_; //key:group_id, value:index
};

#define OB_IO_MANAGER (oceanbase::common::ObIOManager::get_instance())
}// end namespace common
}// end namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_OB_IO_MANAGER_H
