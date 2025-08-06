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
#include "share/io/io_schedule/ob_io_schedule_v2.h"
#include "share/io/ob_io_struct.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/io/ob_ss_io_request.h"
#endif

namespace oceanbase
{
namespace obrpc
{
struct ObSharedDeviceResource;
struct ObSharedDeviceResourceArray;
enum ResourceType { ops = 0, ips = 1, iops = 2, obw = 3, ibw = 4, iobw = 5, tag = 6, ResourceTypeCnt };
inline const char *get_resource_type_str(const ResourceType type)
{
  const char *str;
  switch (type) {
    case ops:
      str = "ops";
      break;
    case ips:
      str = "ips";
      break;
    case iops:
      str = "iops";
      break;
    case obw:
      str = "obw";
      break;
    case ibw:
      str = "ibw";
      break;
    case iobw:
      str = "iobw";
      break;
    case tag:
      str = "tag";
      break;
    default:
      str = "unknown";
      break;
  }
  return str;
}
}  // namespace obrpc
namespace common
{
int64_t get_norm_iops(const int64_t size, const double iops, const ObIOMode mode);
int64_t get_norm_bw(const int64_t size, const ObIOMode mode);
class ObTenantIOManager;
#ifdef OB_BUILD_SHARED_STORAGE
class ObSSIORequest;
#endif

struct ResourceUsage
{
  ResourceUsage() : type_(obrpc::ResourceType::ResourceTypeCnt), total_(0)
  {}
  ~ResourceUsage()
  {}
  obrpc::ResourceType type_;
  int64_t total_;
};

static ObString other_group_name("OTHER_GROUP");
class ObTrafficControl
{
public:
  struct ObStorageKey
  {
    OB_UNIS_VERSION(1);

  public:
    explicit ObStorageKey() : storage_id_(0), tenant_id_(0), category_(ObStorageInfoType::ALL_ZONE_STORAGE)
    {}
    explicit ObStorageKey(uint64_t storage_id, uint64_t tenant_id, ObStorageInfoType category)
        : storage_id_(storage_id), tenant_id_(tenant_id), category_(category)
    {
      if (ObStorageInfoType::ALL_ZONE_STORAGE == category_) {
        tenant_id_ = OB_INVALID_TENANT_ID;
      } else {
        tenant_id_ = gen_user_tenant_id(tenant_id_);
      }
    }
    int assign(const ObTrafficControl::ObStorageKey &other)
    {
      storage_id_ = other.storage_id_;
      tenant_id_ = other.tenant_id_;
      category_ = other.category_;
      return OB_SUCCESS;
    }
    uint64_t hash() const
    {
      return (storage_id_ << 48) ^ (tenant_id_ << 32) ^ ((uint64_t)category_ << 16);
    }
    int hash(uint64_t &res) const
    {
      res = hash();
      return OB_SUCCESS;
    }
    bool operator==(const ObStorageKey &that) const
    {
      return storage_id_ == that.storage_id_ && tenant_id_ == that.tenant_id_ && category_ == that.category_;
    }
    bool operator!=(const ObStorageKey &that) const
    {
      return !(*this == that);
    }
    uint64_t get_storage_id() const
    {
      return storage_id_;
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }
    ObStorageInfoType get_category() const
    {
      return category_;
    }
    TO_STRING_KV(K(storage_id_), K_(tenant_id), K_(category));

  private:
    uint64_t storage_id_;
    uint64_t tenant_id_;  // tenant_id of storage
    ObStorageInfoType category_;
  };

private:
  struct IORecord
  {
    IORecord() : last_ts_(ObTimeUtility::fast_current_time()), total_size_(0), last_record_(0), size_(0)
    {}
    int64_t calc();
    void inc(int64_t size)
    {
      IGNORE_RETURN ATOMIC_FAA(&size_, size);
    }
    int64_t clear()
    {
      return ATOMIC_SET(&total_size_, 0);
    }

  private:
    int64_t last_ts_;
    int64_t total_size_;
    int64_t last_record_;
    int64_t size_;  // CACHE_ALIGNED?
  };

public:
  struct ObIORecordKey
  {
    explicit ObIORecordKey() : tenant_id_(0), id_()
    {}
    explicit ObIORecordKey(const ObStorageKey &id, uint64_t tenant_id) : tenant_id_(tenant_id), id_(id)
    {}
    int hash(uint64_t &res) const
    {
      id_.hash(res);
      res ^= tenant_id_;
      return OB_SUCCESS;
    }
    bool operator==(const ObIORecordKey &that) const
    {
      return tenant_id_ == that.tenant_id_ && id_ == that.id_;
    }
    TO_STRING_KV(K(tenant_id_), K(id_));
    uint64_t tenant_id_; // tenant_id of req
    ObStorageKey id_;
  };

public:
  struct ObSharedDeviceIORecord
  {
    int calc_usage(ObIORequest &req);
    void reset_total_size(ResourceUsage usages[]);
    IORecord ibw_;
    IORecord obw_;
    IORecord ips_;
    IORecord ops_;
    IORecord tagps_;
  };

public:
  struct ObSharedDeviceControl
  {
    ObSharedDeviceControl();
    int calc_clock(const int64_t current_ts, ObIORequest &req, int64_t &deadline_ts);
    void set_limit(const obrpc::ObSharedDeviceResource &limit);
    ObAtomIOClock *get_clock(obrpc::ResourceType type)
    {
      ObAtomIOClock *ret = nullptr;
      switch (type) {
        case obrpc::ResourceType::ops:
          ret = &ops_clock_;
          break;
        case obrpc::ResourceType::ips:
          ret = &ips_clock_;
          break;
        case obrpc::ResourceType::iops:
          ret = &iops_clock_;
          break;
        case obrpc::ResourceType::obw:
          ret = &obw_clock_;
          break;
        case obrpc::ResourceType::ibw:
          ret = &ibw_clock_;
          break;
        case obrpc::ResourceType::iobw:
          ret = &iobw_clock_;
          break;
        case obrpc::ResourceType::tag:
          ret = &tagps_clock_;
          break;
        default:
          break;
      }
      return ret;
    }
    // bw means bandwidth
    ObAtomIOClock ibw_clock_;
    ObAtomIOClock obw_clock_;
    ObAtomIOClock iobw_clock_;
    ObAtomIOClock ips_clock_;
    ObAtomIOClock ops_clock_;
    ObAtomIOClock iops_clock_;
    ObAtomIOClock tagps_clock_;
    TO_STRING_KV(
        K(ibw_clock_), K(obw_clock_), K(iobw_clock_), K(ips_clock_), K(ops_clock_), K(iops_clock_), K(tagps_clock_));
  };
  struct ObSharedDeviceControlV2
  {
    struct ObSDGroupList
    {
      ObSDGroupList();
      ~ObSDGroupList();
      int clear();
      int assign(const ObSDGroupList &other);
      int add_group(const ObIOSSGrpKey &grp_key, int qid, int* limit_ids, int l_size);
      int is_group_key_exist(const ObIOSSGrpKey &grp_key);
      common::ObSEArray<ObIOSSGrpKey, 7> grp_list_;
      TO_STRING_KV(K(grp_list_));
    };
    ObSharedDeviceControlV2();
    ObSharedDeviceControlV2(const ObStorageKey &key);
    ~ObSharedDeviceControlV2();
    int init();
    void destroy();
    int set_storage_key(const ObTrafficControl::ObStorageKey &key);
    int add_shared_device_limits();
    int fill_qsched_req_storage_key(ObIORequest& req);
    int add_group(const ObIOSSGrpKey &grp_key, const int qid);
    int is_group_key_exist(const ObIOSSGrpKey &grp_key);
    int64_t get_limit(const obrpc::ResourceType type) const;
    int update_limit(const obrpc::ObSharedDeviceResource &limit);
    int64_t to_string(char* buf, const int64_t buf_len) const;
    ObStorageKey storage_key_;
    // limit and limit_ids: ops = 0, ips = 1, iops = 2, obw = 3, ibw = 4, iobw = 5, tag = 6
    int64_t limits_[static_cast<int>(obrpc::ResourceType::ResourceTypeCnt)];
    int limit_ids_[static_cast<int>(obrpc::ResourceType::ResourceTypeCnt)];
    ObSDGroupList group_list_;
  };

public:
  ObTrafficControl();
  int calc_clock(const int64_t current_ts, ObIORequest &req, int64_t &deadline_ts);
  int calc_usage(ObIORequest &req);
  void print_server_status();
  void print_bucket_status_V1();
  void print_bucket_status_V2();
  int set_limit(const obrpc::ObSharedDeviceResourceArray &limit);
  int set_limit_v2(const obrpc::ObSharedDeviceResourceArray &limit);
  int get_storage_count() const { return shared_device_map_.size(); };
  int register_bucket(ObIORequest &req, const int qid);
  int add_shared_device_limits(const ObStorageKey &key, const int qid);
  template<class _cb>
  int foreach_limit(_cb &cb) const { return shared_device_map_.foreach_refactored(cb); }
  template <class _cb>
  int foreach_limit_v2(_cb &cb) const { return shared_device_map_v2_.foreach_refactored(cb); }
  template<class _cb>
  int foreach_record(_cb &cb) const { return io_record_map_.foreach_refactored(cb); }
  int64_t get_net_ibw() { return net_ibw_.calc(); }
  int64_t get_net_obw() { return net_obw_.calc(); }
  int64_t get_device_bandwidth() const { return device_bandwidth_; }
  void set_device_bandwidth(int64_t bw) { device_bandwidth_ = ibw_clock_.iops_ = obw_clock_.iops_ = bw; }
  int gc_tenant_infos();
private:
  void inner_calc_();
  static int transform_ret(int ret);
private:
  // for device limitation
  hash::ObHashMap<ObStorageKey, ObSharedDeviceControl> shared_device_map_;
  hash::ObHashMap<ObStorageKey, ObSharedDeviceControlV2*> shared_device_map_v2_;
  // for diagnose
  hash::ObHashMap<ObIORecordKey, ObSharedDeviceIORecord> io_record_map_;
  // maybe different key between limitation and diagnose later
  // so there are two maps.
  IORecord shared_storage_ibw_;
  IORecord shared_storage_obw_;
  IORecord net_ibw_;
  IORecord net_obw_;
  IORecord failed_shared_storage_ibw_;
  IORecord failed_shared_storage_obw_;
  ObAtomIOClock ibw_clock_;
  ObAtomIOClock obw_clock_;
  int64_t device_bandwidth_;
  DRWLock rw_lock_;
};

class ObIOManager final
{
public:
  static ObIOManager &get_instance();
  int init(const int64_t memory_limit = DEFAULT_MEMORY_LIMIT, const int32_t queue_depth = DEFAULT_QUEUE_DEPTH,
      const int32_t schedule_thread_count = 0);
  void destroy();
  int start();
  void stop();
  void wait();
  bool is_stopped() const;

  int read(const ObIOInfo &info, ObIOHandle &handle);

  int write(const ObIOInfo &info);

  int aio_read(const ObIOInfo &info, ObIOHandle &handle);

  int aio_write(const ObIOInfo &info, ObIOHandle &handle);

  int pread(ObIOInfo &info, int64_t &read_size);

  int pwrite(ObIOInfo &info, int64_t &write_size);

  int detect_read(const ObIOInfo &info, ObIOHandle &handle);

  // config related, thread safe
  int set_io_config(const ObIOConfig &conf);
  const ObIOConfig &get_io_config() const;

  // device health management
  ObIOFaultDetector &get_device_health_detector();
  int get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time);
  int reset_device_health();

  // device channel management
  int add_device_channel(ObIODevice *device_handle, const int64_t async_channel_count, const int64_t sync_channel_count,
      const int64_t max_io_depth);
  int remove_device_channel(ObIODevice *device_handle);
  int get_device_channel(const ObIORequest &req, ObDeviceChannel *&device_channel);

  // tenant management
  int refresh_tenant_io_unit_config(const uint64_t tenant_id, const ObTenantIOConfig::UnitConfig &tenant_io_unit_config);
  // tenant management
  int refresh_tenant_io_param_config(const uint64_t tenant_id, const ObTenantIOConfig::ParamConfig &tenant_io_param_config);
  int get_tenant_io_manager(const uint64_t tenant_id, ObRefHolder<ObTenantIOManager> &tenant_holder) const;
  OB_INLINE bool is_inited()
  {
    return is_inited_;
  }
  int modify_group_io_config(const uint64_t tenant_id, const uint64_t index, const int64_t min_percent,
      const int64_t max_percent, const int64_t weight_percent);
  ObIOScheduler *get_scheduler()
  {
    return &io_scheduler_;
  }
  ObTrafficControl &get_tc()
  {
    return tc_;
  }
  void print_sender_status();
  void print_tenant_status();
  void print_channel_status();
  void print_status();
  int64_t get_object_storage_io_timeout_ms(const uint64_t tenant_id) const;

private:
  friend class ObTenantIOManager;
  static const int64_t DEFAULT_MEMORY_LIMIT = 10L * 1024L * 1024L * 1024L;  // 10GB
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
  ObTrafficControl tc_;
  ObTenantIOManager *server_io_manager_;
};

class ObTenantIOManager final
{
public:
  static int mtl_new(ObTenantIOManager *&io_service);
  static int mtl_init(ObTenantIOManager *&io_service);
  static void mtl_destroy(ObTenantIOManager *&io_service);

public:
  ObTenantIOManager();
  ~ObTenantIOManager();
  int init(const uint64_t tenant_id, const ObTenantIOConfig &io_config, ObIOScheduler *io_scheduler);
  int init_io_config();
  void destroy();
  int start();
  void stop();
  bool is_working() const;
  int alloc_and_init_result(const ObIOInfo &info, ObIOResult *&io_result);
  int alloc_req_and_result(const ObIOInfo &info, ObIOHandle &handle, ObIORequest *&io_request, RequestHolder &req_holder);
  int inner_aio(const ObIOInfo &info, ObIOHandle &handle);
  int detect_aio(const ObIOInfo &info, ObIOHandle &handle);
  int enqueue_callback(ObIORequest &req);
  int retry_io(ObIORequest &req);
  ObTenantIOClock *get_io_clock()
  {
    return &io_clock_;
  }
  ObIOUsage &get_io_usage()
  {
    return io_usage_;
  }
  ObIOCallbackManager &get_callback_mgr()
  {
    return callback_mgr_;
  };
  ObIOUsage &get_sys_io_usage()
  {
    return io_sys_usage_;
  }
  int update_basic_io_unit_config(const ObTenantIOConfig::UnitConfig &io_unit_config);
  int update_basic_io_param_config(const ObTenantIOConfig::ParamConfig &io_param_config);
  int try_alloc_req_until_timeout(const int64_t timeout_ts, ObIORequest *&req);
  int try_alloc_result_until_timeout(const int64_t timeout_ts, ObIOResult *&result);
  int alloc_io_request(ObIORequest *&req);
  int alloc_io_result(ObIOResult *&result);
  int alloc_io_clock(ObIAllocator &allocator, ObTenantIOClock *&io_clock);
  int init_group_index_map(const int64_t tenant_id, const ObTenantIOConfig &io_config);
  int get_group_index(const ObIOGroupKey &key, uint64_t &index);
  int get_group_config(const ObIOGroupKey &key, ObTenantIOConfig::GroupConfig &index) const;
  int calc_io_memory(const uint64_t tenant_id, const int64_t memory);
  int init_memory_pool(const uint64_t tenant_id, const int64_t memory);
  int update_memory_pool(const int64_t memory);
  int modify_group_io_config(const uint64_t index,
                             const int64_t min_percent,
                             const int64_t max_percent,
                             const int64_t weight_percent,
                             const bool deleted = false,
                             const bool cleared = false);

  //for modify group config
  int modify_io_config(const uint64_t group_id,
                       const char *group_name,
                       const int64_t min_percent,
                       const int64_t max_percent,
                       const int64_t weight_percent,
                       const int64_t max_net_bandwidth_percent,
                       const int64_t net_bandwidth_weight_percent);
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
  ObIOAllocator *get_tenant_io_allocator() { return &io_allocator_; }
  int print_io_status();
  int print_io_function_status();
  void inc_ref();
  void dec_ref();
  int get_throttled_time(uint64_t group_id, int64_t &throttled_time);
  int64_t get_local_iops_util() const {return local_iops_util_;};
  const ObIOFuncUsages& get_io_func_infos();
  OB_INLINE int64_t get_object_storage_io_timeout_ms() const { return io_config_.param_config_.object_storage_io_timeout_ms_; }

  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(ref_cnt_), K(io_memory_limit_), K(request_count_), K(result_count_),
       K(io_config_), K(io_clock_), K(io_allocator_), KPC(io_scheduler_), K(callback_mgr_), K(io_memory_limit_),
       K(request_count_), K(result_count_), K(local_iops_util_));
private:
  friend class ObIORequest;
  friend class ObIOResult;
  bool is_inited_;
  bool is_working_;
  int64_t ref_cnt_;
  int64_t io_memory_limit_;
  int64_t request_count_;
  int64_t result_count_;
  uint64_t tenant_id_;
  ObTenantIOConfig io_config_;
  ObTenantIOClock io_clock_;
  ObIOAllocator io_allocator_;
  ObIOScheduler *io_scheduler_;
  ObIOCallbackManager callback_mgr_;
  ObIOUsage io_usage_;            // user group usage
  ObIOUsage io_sys_usage_;        // sys group usage
  ObIOMemStats io_mem_stats_;     // Group Level: IO memory monitor
  ObIOFuncUsages io_func_infos_;  // Tenant Level: IO function group usage monitor
  ObIOTracer io_tracer_;
  DRWLock io_config_lock_;                                      // for map and config
  hash::ObHashMap<ObIOGroupKey, uint64_t> group_id_index_map_;  // key:group_id, value:index
  ObTenantIOSchedulerV2 qsched_;
  int64_t local_iops_util_;
};

#define OB_IO_MANAGER (oceanbase::common::ObIOManager::get_instance())
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_LIB_STORAGE_OB_IO_MANAGER_H
