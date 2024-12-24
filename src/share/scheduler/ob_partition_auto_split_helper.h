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

class ObAutoSplitTaskKey final
{
public:
  ObAutoSplitTaskKey();
  ObAutoSplitTaskKey(const uint64_t tenant_id, const ObTabletID &tablet_id);
  ~ObAutoSplitTaskKey() = default;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash(); return OB_SUCCESS;
  }
  bool operator==(const ObAutoSplitTaskKey &other) const;
  bool operator!=(const ObAutoSplitTaskKey &other) const;
  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_ && tablet_id_.is_valid();
  }
  int assign(const ObAutoSplitTaskKey&other);
  TO_STRING_KV(K(tenant_id_), K(tablet_id_));

public:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
};

struct ObAutoSplitTask final
{
public:
  ObAutoSplitTask()
    : tenant_id_(OB_INVALID_ID), ls_id_(), tablet_id_(), auto_split_tablet_size_(OB_INVALID_SIZE), used_disk_space_(OB_INVALID_SIZE), retry_times_(OB_INVALID_COUNT)
    {}
  ObAutoSplitTask(const uint64_t tenant_id, const ObLSID &ls_id, const ObTableID &tablet_id, const int64_t auto_split_tablet_size, const int64_t used_disk_space, const int64_t retry_times)
    : tenant_id_(tenant_id), ls_id_(ls_id), tablet_id_(tablet_id),
      auto_split_tablet_size_(auto_split_tablet_size), used_disk_space_(used_disk_space), retry_times_(retry_times)
    {}
  ~ObAutoSplitTask() = default;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(auto_split_tablet_size), K_(used_disk_space), K_(retry_times));
  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid() && retry_times_ >= 0
        && auto_split_tablet_size_ > 0 && used_disk_space_ > 0 && used_disk_space_ > auto_split_tablet_size_;
  }
  void reset()
  {
    tenant_id_ = OB_INVALID_ID;
    auto_split_tablet_size_ = OB_INVALID_SIZE;
    used_disk_space_ = OB_INVALID_SIZE;
    retry_times_ = OB_INVALID_COUNT;
    ls_id_.reset();
    tablet_id_.reset();
  }
  int assign(const ObAutoSplitTask &other);
  void increment_retry_times() { ++retry_times_; }
public:
  uint64_t tenant_id_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t auto_split_tablet_size_;
  int64_t used_disk_space_;
  int64_t retry_times_;
};

class ObAutoSplitTaskCache final
{
  struct ObAutoSplitTaskWrapper
  {
  public:
    ObAutoSplitTaskWrapper () {}
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
  ~ObAutoSplitTaskCache() { destroy(); }
  int init(const int64_t capacity, const uint64_t tenant_id, const uint64_t host_tenant_id);
  void destroy();
  int pop_tasks(const int64_t num_tasks_to_pop, ObArray<ObAutoSplitTask> &task_array);
  int push_tasks(const ObArray<ObAutoSplitTask> &task_array);
  uint64_t get_tenant_id() const { return tenant_id_; };
  inline int64_t get_tasks_num() { return ATOMIC_LOAD(&total_tasks_); }
  inline uint64_t get_tenant_id() { return tenant_id_; }
  static int mtl_init(ObAutoSplitTaskCache *&task_cache);
  TO_STRING_KV(K_(max_heap), K_(min_heap), K_(tasks_set));
public:
  const static int64_t CACHE_MAX_CAPACITY = 100;
private:
  int atomic_push_task(const ObAutoSplitTask &task);
  int atomic_pop_task(ObAutoSplitTask &task);
  int atomic_remove_task();
  int remove_tasks(const int64_t num_tasks_to_rem);

private:
  bool inited_;
  int64_t total_tasks_;
  uint64_t tenant_id_;
  uint64_t host_tenant_id_;
  ObSpinLock lock_;
  MaxHeapComp max_comp_;
  MinHeapComp min_comp_;
  ObMalloc cache_malloc_;
  ObRemovableHeap<ObAutoSplitTaskWrapper *, MaxHeapComp, &ObAutoSplitTaskWrapper::pos_at_max_heap_> max_heap_;
  ObRemovableHeap<ObAutoSplitTaskWrapper *, MinHeapComp, &ObAutoSplitTaskWrapper::pos_at_min_heap_> min_heap_;
  common::hash::ObHashSet<ObAutoSplitTaskKey> tasks_set_;
};

class ObAutoSplitTaskPollingMgr
{
public:
  ObAutoSplitTaskPollingMgr(const bool is_root_server)
    : is_root_server_(is_root_server), inited_(false), total_tasks_(0)
    {}
  ~ObAutoSplitTaskPollingMgr() { reset(); }
  int init();
  void reset();
  int pop_tasks(const int64_t num_tasks_to_pop, ObArray<ObArray<ObAutoSplitTask>> &task_array);
  int push_tasks(const ObArray<ObAutoSplitTask> &task_array);
  inline bool is_busy() { return ATOMIC_LOAD(&total_tasks_) >= ObAutoSplitTaskPollingMgr::BUSY_THRESHOLD; }
  inline bool empty() { return ATOMIC_LOAD(&total_tasks_) == 0; };

private:
  inline int64_t get_total_tenants() { return map_tenant_to_cache_.size(); }
  int pop_tasks_from_tenant_cache(const int64_t num_tasks_to_pop,
                                  ObArray<ObAutoSplitTask> &task_array,
                                  ObAutoSplitTaskCache *tenant_cache);
  int create_tenant_cache(const uint64_t tenant_id, const uint64_t host_tenant_id, ObAutoSplitTaskCache *&tenant_cache);
  int register_tenant_cache(const uint64_t tenant_id, ObAutoSplitTaskCache * const tenant_cache);
  int get_tenant_cache(const int tenant_id, ObAutoSplitTaskCache *&tenant_cache);

private:
  const static int64_t INITIAL_TENANT_COUNT = 10;
  const static int64_t BUSY_THRESHOLD = 500;
  const bool is_root_server_;
  bool inited_;
  int64_t total_tasks_;
  //key:tenant_id, val: idx at tenants_cache_
  common::hash::ObHashMap<uint64_t, ObAutoSplitTaskCache*> map_tenant_to_cache_;
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
private:
  int acquire_schema_info_of_tablet_(const uint64_t tenant_id,
                                     const ObTabletID tablet_id,
                                     const share::schema::ObTableSchema *&table_schema,
                                     const share::schema::ObSimpleDatabaseSchema *&db_schema,
                                     obrpc::ObAlterTableArg &arg);
  int acquire_table_id_of_tablet_(const uint64_t tenant_id,
                                  const ObTabletID tablet_id,
                                  uint64_t &table_id);
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
                                share::schema::AlterTableSchema &alter_table_schema);
  int build_partition_(const uint64_t tenant_id, const uint64_t table_id,
                       const ObTabletID split_source_tablet_id,
                       const ObRowkey &high_bound_val,
                       share::schema::ObPartition &new_part);
private:
  static const int32_t MAX_SPLIT_PARTITION_NUM = 2;
};


class ObRsAutoSplitScheduler final
{
public:
  static ObRsAutoSplitScheduler &get_instance();
  inline bool is_busy() { return polling_mgr_.is_busy(); }
  int push_tasks(const ObArray<ObAutoSplitTask> &task_array);
  int pop_tasks(ObArray<ObAutoSplitTask> &task_array);
  bool can_retry(const ObAutoSplitTask &task, const int ret);
  int init() { return polling_mgr_.init(); }
  void reset() { polling_mgr_.reset(); }
  static int check_ls_migrating(const uint64_t tenant_id, const ObTabletID &tablet_id, bool &is_migrating);
private:
  ObRsAutoSplitScheduler ()
    : polling_mgr_(true/*is_root_server*/)
    {}
  ~ObRsAutoSplitScheduler () {}
private:
  const static int64_t MAX_SPLIT_TASKS_ONE_ROUND = 5;
  const static int64_t MAX_TIMES_TASK_RETRY = 5;
  ObAutoSplitTaskPollingMgr polling_mgr_;
};

class ObServerAutoSplitScheduler final
{
public:
  const static int64_t OB_SERVER_DELAYED_TIME = (10 * 1000L * 1000L); //10s
public:
  static ObServerAutoSplitScheduler &get_instance();
  int push_task(const storage::ObTabletHandle &teblet_handle, storage::ObLS &ls);
  int init() { return polling_manager_.init(); }
  void reset() { polling_manager_.reset(); }
  static int cal_real_auto_split_size(const double base_ratio, const double cur_ratio, const int64_t auto_split_size, int64_t &real_auto_split_size);
  static int check_tablet_creation_limit(const int64_t inc_tablet_cnt, const double safe_ratio, const int64_t auto_split_size, int64_t &real_auto_split_size);
private:
  ObServerAutoSplitScheduler ()
    : next_valid_time_(0), polling_manager_(false/*is_root_server*/)
    {}
  ~ObServerAutoSplitScheduler () {}
  int batch_send_split_request(const ObArray<ObArray<ObAutoSplitTask>> &task_array);
  int check_and_fetch_tablet_split_info(const storage::ObTabletHandle &teblet_handle, storage::ObLS &ls, bool &can_split, ObAutoSplitTask &task);
  int check_sstable_limit(const storage::ObTablet &tablet, bool &exceed_limit);
private:
  const static int64_t MAX_SPLIT_RPC_IN_BATCH = 20;
  const static int64_t TABLET_CNT_PER_GB = 20000;
  const static int64_t SOURCE_TABLET_SSTABLE_LIMIT = 30;
  int64_t next_valid_time_;
  ObAutoSplitTaskPollingMgr polling_manager_;
};

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
                    common::ObRowkey &low_bound_val,
                    common::ObRowkey &high_bound_val,
                    common::ObArenaAllocator &range_allocator,
                    ObArray<ObNewRange> &ranges);
  int build_sample_sql_(const ObString &db_name, const ObString &table_name, const ObString* part_name,
                        const ObIArray<ObString> &column_names,
                        const ObIArray<ObNewRange> &column_ranges,
                        const int range_num, const double sample_pct,
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
