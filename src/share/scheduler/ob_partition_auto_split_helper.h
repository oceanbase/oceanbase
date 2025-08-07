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

#ifndef OCEABASE_STORAGE_PARTITION_AUTO_SPLIT_HELPER_
#define OCEABASE_STORAGE_PARTITION_AUTO_SPLIT_HELPER_

#include "logservice/ob_log_base_type.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"

namespace oceanbase
{
namespace obrpc
{
struct ObAlterTableArg;
}
namespace share
{
namespace schema
{
struct AlterTableSchema;
class ObSimpleDatabaseSchema;
}

class ObSplitTask
{
public:
  ObSplitTask()
  : tenant_id_(OB_INVALID_TENANT_ID),
    tablet_id_(common::ObTabletID::INVALID_TABLET_ID)
  {}

  ObSplitTask(const uint64_t tenant_id, const ObTabletID &tablet_id)
  : tenant_id_(tenant_id),
    tablet_id_(tablet_id)
  {}
  virtual ~ObSplitTask() = default;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash(); return OB_SUCCESS;
  }
  bool operator==(const ObSplitTask &other) const;
  bool operator!=(const ObSplitTask &other) const;
  virtual bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && tablet_id_.is_valid();
  }
  virtual void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    tablet_id_.reset();
  }
  virtual int assign(const ObSplitTask&other);
  int get_tenant_id() const { return tenant_id_; }

  VIRTUAL_TO_STRING_KV(K(tenant_id_), K(tablet_id_));

public:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
};

struct ObAutoSplitTask final : public ObSplitTask
{
public:
  ObAutoSplitTask()
    :  ls_id_(), auto_split_tablet_size_(OB_INVALID_SIZE), used_disk_space_(OB_INVALID_SIZE), retry_times_(OB_INVALID_COUNT)
    {}
  ObAutoSplitTask(const uint64_t tenant_id, const ObLSID &ls_id, const ObTabletID &tablet_id, const int64_t auto_split_tablet_size, const int64_t used_disk_space, const int64_t retry_times)
    : ObSplitTask(tenant_id, tablet_id), ls_id_(ls_id), auto_split_tablet_size_(auto_split_tablet_size),
      used_disk_space_(used_disk_space), retry_times_(retry_times)
    {}
  virtual ~ObAutoSplitTask() = default;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(auto_split_tablet_size), K_(used_disk_space), K_(retry_times));
  virtual bool is_valid() const override
  {
    return ls_id_.is_valid() && tablet_id_.is_valid() && retry_times_ >= 0
        && auto_split_tablet_size_ > 0 && used_disk_space_ > 0 && used_disk_space_ > auto_split_tablet_size_;
  }
  virtual void reset() override
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    auto_split_tablet_size_ = OB_INVALID_SIZE;
    used_disk_space_ = OB_INVALID_SIZE;
    retry_times_ = OB_INVALID_COUNT;
    ls_id_.reset();
    tablet_id_.reset();
  }
  virtual int assign(const ObAutoSplitTask &other);
  void increment_retry_times() { ++retry_times_; }
public:
  ObLSID ls_id_;
  int64_t auto_split_tablet_size_;
  int64_t used_disk_space_;
  int64_t retry_times_;
};

enum TabletSplitTaskTatus
{
  INVALID_STATUS,
  WAITING_SPLIT_DATA_COMPLEMENT,
  WAITING_PRE_WARM,
};

struct ObTabletSplitTask final : public ObSplitTask
{
public:
  ObTabletSplitTask()
    : ObSplitTask(), next_schedule_time_(OB_INVALID_TIMESTAMP), task_status_(INVALID_STATUS)
    {}
  ObTabletSplitTask(const uint64_t tenant_id, const ObTabletID &tablet_id, const int64_t next_schedule_time, const TabletSplitTaskTatus &task_status)
    : ObSplitTask(tenant_id, tablet_id), next_schedule_time_(next_schedule_time), task_status_(task_status)
    {}

  virtual ~ObTabletSplitTask() {}
  virtual void reset() override
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    tablet_id_ = OB_INVALID_ID;
    next_schedule_time_ = OB_INVALID_TIMESTAMP;
    task_status_ = INVALID_STATUS;
  }

 virtual bool is_valid() const override
 {
   return OB_INVALID_TENANT_ID != tenant_id_ && tablet_id_.is_valid()
      && next_schedule_time_ > 0 && TabletSplitTaskTatus::INVALID_STATUS != task_status_;
 }
 virtual int assign(const ObTabletSplitTask &other);
 TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(next_schedule_time), K_(task_status));

public:
  int64_t next_schedule_time_;
  TabletSplitTaskTatus task_status_;
};


class ObSplitTaskCache
{
public:
  ObSplitTaskCache()
    : inited_(false), total_tasks_(0), tenant_id_(OB_INVALID_TENANT_ID),
      host_tenant_id_(OB_INVALID_TENANT_ID), lock_()
    {}
  virtual ~ObSplitTaskCache() {}
  virtual int pop_tasks(const int64_t num_tasks_to_pop, ObIArray<ObSplitTask*> &task_array, ObIAllocator &allocator) = 0;
  virtual int push_tasks(const ObIArray<const ObSplitTask*> &task_array) = 0;
  inline uint64_t get_tenant_id() { return tenant_id_; };
  inline int64_t get_tasks_num() { return ATOMIC_LOAD(&total_tasks_); }

  VIRTUAL_TO_STRING_KV(K_(inited), K_(total_tasks), K_(tenant_id), K_(host_tenant_id));
public:
  bool inited_;
  int64_t total_tasks_;
  uint64_t tenant_id_;
  uint64_t host_tenant_id_;
  ObSpinLock lock_;
};

class ObAutoSplitTaskCache final : public ObSplitTaskCache
{
  struct ObAutoSplitTaskWrapper
  {
  public:
    ObAutoSplitTaskWrapper ()
     : priority_(0), pos_at_min_heap_(-1), pos_at_max_heap_(-1), task_()
     {}
    ~ObAutoSplitTaskWrapper () {}
    TO_STRING_KV(K_(priority), K_(pos_at_min_heap), K_(pos_at_max_heap), K_(task));
  public:
    double priority_;
    int64_t pos_at_min_heap_;
    int64_t pos_at_max_heap_;
    ObAutoSplitTask task_;
  };
  struct MaxHeapComp
  {
  public:
    int get_error_code() { return OB_SUCCESS; }
    bool operator()(const ObAutoSplitTaskWrapper *lhs, const ObAutoSplitTaskWrapper *rhs) { return lhs->priority_ < rhs->priority_ ? true : false; }
  };
  struct MinHeapComp
  {
  public:
    int get_error_code() { return OB_SUCCESS; }
    bool operator()(const ObAutoSplitTaskWrapper *lhs, const ObAutoSplitTaskWrapper *rhs) { return lhs->priority_ > rhs->priority_ ? true : false; }
  };
public:
  ObAutoSplitTaskCache ();
  virtual ~ObAutoSplitTaskCache() override { destroy(); }
  int init(const int64_t bucket_num, const uint64_t tenant_id, const uint64_t host_tenant_id);
  void destroy();
  virtual int pop_tasks(const int64_t num_tasks_to_pop, ObIArray<ObSplitTask*> &task_array, ObIAllocator &allocator) override;
  virtual int push_tasks(const ObIArray<const ObSplitTask*> &task_array) override;
  static int mtl_init(ObAutoSplitTaskCache *&task_cache);
  TO_STRING_KV(K_(inited), K_(tenant_id), K_(total_tasks), K_(host_tenant_id), K_(max_heap), K_(min_heap), K_(tasks_set));
public:
  const static int64_t CACHE_MAX_CAPACITY = 100;
private:
  int atomic_push_task(const ObAutoSplitTask &task);
  int atomic_pop_task(ObAutoSplitTask &task);
  int atomic_remove_task();
  int remove_tasks(const int64_t num_tasks_to_rem);

private:
  MaxHeapComp max_comp_;
  MinHeapComp min_comp_;
  ObMalloc cache_malloc_;
  ObRemovableHeap<ObAutoSplitTaskWrapper *, MaxHeapComp, &ObAutoSplitTaskWrapper::pos_at_max_heap_> max_heap_;
  ObRemovableHeap<ObAutoSplitTaskWrapper *, MinHeapComp, &ObAutoSplitTaskWrapper::pos_at_min_heap_> min_heap_;
  common::hash::ObHashSet<ObSplitTask> tasks_set_;
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObTabletSplitTaskCache final : public ObSplitTaskCache {
  struct MinHeapComp
  {
  public:
    int get_error_code() { return OB_SUCCESS; }
    bool operator()(const ObTabletSplitTask *lhs, const ObTabletSplitTask *rhs) { return lhs->next_schedule_time_ > rhs->next_schedule_time_ ? true : false; }
  };
public:
  ObTabletSplitTaskCache ()
  : ObSplitTaskCache(), tasks_set_(), comp(), cache_malloc_(), schedule_time_min_heap_(comp, &cache_malloc_)
  {}
  virtual ~ObTabletSplitTaskCache() override { destroy(); }
  int init(const int64_t capacity, const uint64_t tenant_id, const uint64_t host_tenant_id);
  void destroy() {};
  virtual int pop_tasks(const int64_t num_tasks_to_pop, ObIArray<ObSplitTask*> &task_array, ObIAllocator &allocator) override;
  virtual int push_tasks(const ObIArray<const ObSplitTask*> &task_array) override;
  static int mtl_init(ObTabletSplitTaskCache *&task_cache);

public:
  const static int64_t BUCKET_NUM = 50;
public:
  common::hash::ObHashSet<ObTabletSplitTask> tasks_set_;
  MinHeapComp comp;
  ObMalloc cache_malloc_;
  ObBinaryHeap<ObTabletSplitTask *, MinHeapComp, 50> schedule_time_min_heap_;
};
#endif

enum ObSplitCacheType
{
  INVALID_SPLIT_CACHE_TYPE,
  AUTO_SPLIT_CACHE_TYPE,
#ifdef OB_BUILD_SHARED_STORAGE
  TABLET_SPLIT_CACHE_TYPE,
#endif
};

class ObSplitTaskPollingMgr
{
  struct GcTenantCacheOperator
  {
  public:
    GcTenantCacheOperator(common::hash::ObHashSet<uint64_t> &existed_tenants_set)
      : existed_tenants_set_(existed_tenants_set)
      {}
    int operator () (oceanbase::common::hash::HashMapPair<uint64_t, ObSplitTaskCache*> &entry);
  public:
    common::hash::ObHashSet<uint64_t> &existed_tenants_set_;
    ObSEArray<oceanbase::common::hash::HashMapPair<uint64_t, ObSplitTaskCache*>, 1> needed_gc_tenant_caches_;
  };
public:
  ObSplitTaskPollingMgr(const bool is_root_server, const ObSplitCacheType cache_type)
    : is_root_server_(is_root_server), cache_type_(cache_type), inited_(false), total_tasks_(0)
    {}
  ~ObSplitTaskPollingMgr() { reset(); }
  int init();
  void reset();
  int pop_tasks(const int64_t num_tasks_to_pop,
                const bool need_to_prior_pop,
                ObIArray<ObSEArray<ObSplitTask*, 10>> &task_array,
                ObIAllocator &allocator);
  int push_tasks(const ObIArray<const ObSplitTask*> &task_array);
  inline bool is_busy() { return ATOMIC_LOAD(&total_tasks_) >= ObSplitTaskPollingMgr::BUSY_THRESHOLD; }
  inline bool is_empty() { return ATOMIC_LOAD(&total_tasks_) == 0; };
  inline int64_t get_total_tasks_num() { return ATOMIC_LOAD(&total_tasks_); };
  int gc_deleted_tenant_caches();
  int get_or_create_task_cache(uint64_t tenant_id, ObSplitTaskCache *&task_cache);

private:
  inline int64_t get_total_tenants() { return map_tenant_to_cache_.size(); }
  int pop_tasks_from_tenant_cache(const int64_t num_tasks_to_pop,
                                  ObIArray<ObSplitTask*> &task_array,
                                  ObSplitTaskCache *tenant_cache,
                                  ObIAllocator &allocator);
  int create_auto_split_task_cache(const uint64_t tenant_id, const uint64_t host_tenant_id, ObAutoSplitTaskCache *&tenant_cache);
  int register_task_cache(const uint64_t tenant_id, ObSplitTaskCache * const tenant_cache);
  int get_task_cache(const int tenant_id, ObSplitTaskCache *&tenant_cache);
  int push_tasks_(const ObIArray<const ObSplitTask*> &task_array, uint64_t tenant_id, ObSplitTaskCache *task_cache);

private:
  const static int64_t INITIAL_TENANT_COUNT = 10;
  const static int64_t BUSY_THRESHOLD = 500;
  const bool is_root_server_;
  const ObSplitCacheType cache_type_;
  bool inited_;

  int64_t total_tasks_;
  //key:tenant_id, val: idx at tenants_cache_
  common::hash::ObHashMap<uint64_t, ObSplitTaskCache*> map_tenant_to_cache_;
  ObMalloc polling_mgr_malloc_;
  ObSpinLock lock_;
};

class ObAutoSplitArgBuilder final
{
public:
  ObAutoSplitArgBuilder() {}
  ~ObAutoSplitArgBuilder() {}
  inline static int32_t get_max_split_partition_num() { return MAX_SPLIT_PARTITION_NUM; }
  int build_arg(const uint64_t tenant_id,
                const share::ObLSID ls_id,
                const ObTabletID tablet_id,
                const int64_t auto_split_tablet_size,
                const int64_t used_disk_space,
                obrpc::ObAlterTableArg &arg);
  static int print_identifier(ObIAllocator &allocator,
                              const bool is_oracle_mode,
                              const ObString &name,
                              ObString &ident);
  static int convert_rowkey_to_sql_literal(const ObRowkey &rowkey,
                                           const bool is_oracle_mode,
                                           const ObTimeZoneInfo *tz_info,
                                           ObIAllocator &allocator,
                                           ObString &rowkey_str);

  static int acquire_table_id_of_tablets(const uint64_t tenant_id,
                                         const ObIArray<ObTabletID> &tablet_ids,
                                         ObIArray<uint64_t> &table_ids);
private:
  int acquire_schema_info_of_tablet_(const uint64_t tenant_id,
                                     const ObTabletID tablet_id,
                                     const share::schema::ObTableSchema *&table_schema,
                                     const share::schema::ObSimpleDatabaseSchema *&db_schema,
                                     share::schema::ObSchemaGetterGuard &guard,
                                     obrpc::ObAlterTableArg &arg);
  int build_arg_(const uint64_t tenant_id,
                 const ObString &db_name,
                 const share::schema::ObTableSchema &table_schema,
                 const ObTabletID split_source_tablet_id,
                 const ObArray<ObNewRange> &ranges,
                 obrpc::ObAlterTableArg &arg);
  int build_ddl_stmt_str_(const share::schema::ObTableSchema &orig_table_schema,
                          const share::schema::AlterTableSchema &alter_table_schema,
                          const ObTabletID &src_tablet_id,
                          const ObTimeZoneInfo *tz_info,
                          ObIAllocator &allocator,
                          ObString &ddl_stmt_str);
  int build_alter_table_schema_(const uint64_t tenant_id,
                                const ObString &db_name,
                                const share::schema::ObTableSchema &table_schema,
                                const ObTabletID split_source_tablet_id,
                                const ObArray<ObNewRange> &ranges,
                                const ObTimeZoneInfo *tz_info,
                                share::schema::AlterTableSchema &alter_table_schema);
  int build_partition_(const uint64_t tenant_id, const uint64_t table_id,
                       const ObTabletID split_source_tablet_id,
                       const ObRowkey &high_bound_val,
                       const ObTimeZoneInfo *tz_info,
                       share::schema::ObPartition &new_part);
  int check_and_cast_high_bound(const ObRowkey &origin_high_bound_val,
                                const ObTimeZoneInfo *tz_info,
                                ObRowkey &cast_hight_bound_val,
                                bool &need_cast,
                                ObIAllocator &allocator);
  int check_need_to_cast(const ObObj &obj, bool &need_to_cast);
  int check_null_value(const ObRowkey &high_bound_val);
private:
  static const int32_t MAX_SPLIT_PARTITION_NUM = 2;
};

class ObAutoSpTaskSchedEntry final
{
public:
  ObAutoSpTaskSchedEntry()
    : tenant_id_(OB_INVALID_TENANT_ID), table_id_(OB_INVALID_ID), next_valid_schedule_time_(OB_INVALID_TIMESTAMP), task_()
    {}
  ~ObAutoSpTaskSchedEntry() {}
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_TIMESTAMP != next_valid_schedule_time_ && OB_INVALID_ID != table_id_ && task_.is_valid();
  }
  bool operator ==(const ObAutoSpTaskSchedEntry &other) const
  {
    return tenant_id_ == other.tenant_id_ && table_id_ == other.table_id_;
  }
  int assign(const ObAutoSpTaskSchedEntry&other);
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    table_id_ = OB_INVALID_ID;
    next_valid_schedule_time_ = OB_INVALID_TIMESTAMP;
    task_.reset();
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(next_valid_schedule_time), K_(task));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t next_valid_schedule_time_;
  ObAutoSplitTask task_;
};

class ObRsAutoSplitScheduler final
{
public:
  const static int64_t MAX_SPLIT_TASKS_ONE_ROUND = 5;
public:
  static ObRsAutoSplitScheduler &get_instance();
  inline bool is_busy() { return polling_mgr_.is_busy(); }
  int push_tasks(const ObArray<ObAutoSplitTask> &task_array);
  int init();
  void reset()
  {
    inited_ = false;
    polling_mgr_.reset();
  }
  int pop_tasks(const int64_t num_tasks_can_pop, const bool throttle_by_table, ObArray<ObAutoSplitTask> &task_array);
  static int check_ls_migrating(const uint64_t tenant_id, const ObTabletID &tablet_id, bool &is_migrating);
  static bool can_retry(const ObAutoSplitTask &task, const int ret);
  int gc_deleted_tenant_caches();
  void reset_direct_cache() { task_direct_cache_.reset(); }
private:
  ObRsAutoSplitScheduler ()
    : polling_mgr_(true/*is_root_server*/, ObSplitCacheType::AUTO_SPLIT_CACHE_TYPE)
    {}
  ~ObRsAutoSplitScheduler () {}
  int pop_from_direct_cache(const int64_t num_tasks_can_pop, ObIArray<ObAutoSplitTask> &task_array);
  int push_to_direct_cache(ObArray<ObArray<ObAutoSplitTask>> &tenant_task_arrays);

private:
  bool inited_;
  const static int64_t MAX_TIMES_TASK_RETRY = 5;
  const static int64_t MAX_SPLIT_TASK_DIRECT_CACHE_SIZE = 10;
  const static int64_t SINGLE_TABLE_SCHEDULE_TIME_INTERVAL = 5L * 1000L * 1000L; //5s
  // to help avoid schedule too much times of split on a single table in a short time interval
  ObSEArray<ObAutoSpTaskSchedEntry, MAX_SPLIT_TASK_DIRECT_CACHE_SIZE> task_direct_cache_;
  ObSplitTaskPollingMgr polling_mgr_;
};

class ObServerAutoSplitScheduler final
{
public:
  const static int64_t OB_SERVER_DELAYED_TIME = (10 * 1000L * 1000L); //10s
public:
  static ObServerAutoSplitScheduler &get_instance();
  int push_task(const storage::ObTabletHandle &teblet_handle, storage::ObLS &ls);
  int pop_tasks(ObIArray<ObSEArray<ObSplitTask*, 10>> &task_array, ObIAllocator &allocator);
  int init();
  void reset()
  {
    polling_manager_.reset();
    inited_ = false;
  }
  static int cal_real_auto_split_size(const double base_ratio, const double cur_ratio, const int64_t auto_split_size, int64_t &real_auto_split_size);
  static int check_tablet_creation_limit(const int64_t inc_tablet_cnt, const double safe_ratio, const int64_t auto_split_size, int64_t &real_auto_split_size);
private:
  ObServerAutoSplitScheduler ()
    : next_valid_time_(0), polling_manager_(false/*is_root_server*/, ObSplitCacheType::AUTO_SPLIT_CACHE_TYPE)
    {}
  ~ObServerAutoSplitScheduler () {}
  int batch_send_split_request(const ObIArray<ObSEArray<ObSplitTask*, 10>> &tenant_task_arrays);
  int check_and_fetch_tablet_split_info(const storage::ObTabletHandle &teblet_handle, storage::ObLS &ls, bool &can_split, ObAutoSplitTask &task);
  int check_sstable_limit(const storage::ObTablet &tablet, bool &exceed_limit);
private:
  bool inited_;
  const static int64_t MAX_SPLIT_RPC_IN_BATCH = 20;
  const static int64_t TABLET_CNT_PER_GB = 20000;
  const static int64_t SOURCE_TABLET_SSTABLE_LIMIT = 30;
  int64_t next_valid_time_;
  ObSplitTaskPollingMgr polling_manager_;
};

#ifdef OB_BUILD_SHARED_STORAGE
class ObLSTabletSplitScheduler final
{
public:
  static ObLSTabletSplitScheduler &get_instance();
  static int is_split_src_tablet(ObTabletHandle &tablet_handle, bool &is_split_tablet);
  int init ();
  bool is_empty() { return polling_manager_.is_empty(); };
  int push_task(const ObIArray<ObTabletSplitTask> &task_array);
  int try_schedule_available_tasks();
public:
  const static int64_t SPLIT_TASK_CHECK_TIME_INTERVAL = (10 * 1000L * 1000L); //10s

private:
  ObLSTabletSplitScheduler ()
    : inited_(false), polling_manager_(false/*is_root_server_*/, ObSplitCacheType::TABLET_SPLIT_CACHE_TYPE)
    {}
  ~ObLSTabletSplitScheduler () {}
  int process_rpc_results(const ObIArray<obrpc::ObDDLBuildSingleReplicaRequestArg> &args,
                          const ObIArray<ObAddr> &addrs,
                          const ObIArray<const obrpc::ObDDLBuildSingleReplicaRequestResult *> &result_array,
                          const ObIArray<int> &ret_array,
                          const ObIArray<TabletSplitTaskTatus> &split_task_status);
  int construct_args_from_mds_data(const ObLSID &ls_id,
                                   const ObTabletSplitTask &split_task,
                                   const ObIArray<ObAddr> &split_replica_addrs,
                                   ObTabletSplitInfoMdsUserData &split_info_data,
                                   ObIArray<obrpc::ObDDLBuildSingleReplicaRequestArg> &args,
                                   ObIArray<ObAddr> &addrs,
                                   ObIArray<TabletSplitTaskTatus> &split_task_status,
                                   ObIAllocator &allocator);
  int schedule_data_split_dag(ObIArray<ObTabletSplitTask> &split_data_tasks);
  int get_split_addr(const ObTabletSplitTask &split_task, ObLSID &ls_id, ObIArray<ObAddr> &split_replica_addrs);

private:
  bool inited_;
  ObSplitTaskPollingMgr polling_manager_;
};
#endif

class ObSplitSampler
{
public:
  typedef share::schema::ObPartitionSchemaIter::Info PartitionMeta;

public:
  ObSplitSampler() {}
  ~ObSplitSampler() {}
  int query_ranges(const uint64_t tenant_id,
                   const ObString &db_name,
                   const share::schema::ObTableSchema &table_schema,
                   const ObTabletID tablet_id,
                   const int64_t range_num, const int64_t used_disk_space,
                   common::ObArenaAllocator &range_allocator,
                   ObArray<ObNewRange> &ranges);
  int query_ranges(const uint64_t tenant_id,
                   const ObString &db_name,
                   const share::schema::ObTableSchema &data_table_schema,
                   const ObIArray<ObString> &column_names,
                   const ObIArray<ObNewRange> &column_ranges,
                   const int64_t range_num, const int64_t used_disk_space,
                   common::ObArenaAllocator &range_allocator,
                   ObArray<ObNewRange> &ranges);

private:
  int query_ranges_(const uint64_t tenant_id, const ObString &db_name, const ObString &table_name, const PartitionMeta &part_meta,
                    const ObIArray<ObString> &column_names,
                    const ObIArray<ObNewRange> &column_ranges,
                    const int64_t range_num, const int64_t used_disk_space,
                    const bool query_index,
                    const bool is_oracle_mode,
                    const bool is_query_table_hidden,
                    common::ObRowkey &low_bound_val,
                    common::ObRowkey &high_bound_val,
                    common::ObArenaAllocator &range_allocator,
                    ObArray<ObNewRange> &ranges);
  int build_sample_sql_(const ObString &db_name, const ObString &table_name, const ObString* part_name,
                        const ObIArray<ObString> &column_names,
                        const ObIArray<ObNewRange> &column_ranges,
                        const int range_num, const double sample_pct,
                        const bool is_oracle_mode,
                        ObSqlString &sql);
  int add_sample_condition_sqls_(const ObIArray<ObString> &columns,
                                 const ObIArray<ObNewRange> &column_ranges,
                                 ObSqlString &sql);
  int add_sample_condition_sql_(const ObString &column,
                                const ObNewRange &range,
                                ObSqlString &sql);
  int acquire_partition_meta_(const share::schema::ObTableSchema &table_schema, const ObTabletID &tablet_id, PartitionMeta &meta);
  int acquire_partition_key_name_(const share::schema::ObTableSchema &table_schema, ObIArray<ObString> &column_names);
  int gen_column_alias_(const ObIArray<ObString> &columns,
                        const bool is_oracle_mode,
                        ObSqlString &col_alias_str,
                        ObSqlString &col_name_alias_str);
  int fill_query_range_bounder(const PartitionMeta& part_meta,
                               const ObIArray<ObNewRange> &column_ranges,
                               const int64_t presetting_part_column_cnt,
                               common::ObRowkey &low_bound_val,
                               common::ObRowkey &high_bound_val,
                               common::ObArenaAllocator &allocator);
};
}

}

#endif
