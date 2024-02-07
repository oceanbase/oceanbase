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
#ifndef OCEANBASE_SHARE_OB_INDEX_USAGE_INFO_MGR_H_
#define OCEANBASE_SHARE_OB_INDEX_USAGE_INFO_MGR_H_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/function/ob_function.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_list.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/task/ob_timer.h"
#include "lib/time/ob_time_utility.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace share
{

class ObIndexUsageInfoMgr;

enum ObIndexUsageOpMode
{
  UPDATE = 0, // for update haspmap
  RESET   // for reset hashmap
};

struct ObIndexUsageKey final
{
public:
  ObIndexUsageKey(const uint64_t index_table_id) : index_table_id_(index_table_id) {}
  ObIndexUsageKey() : index_table_id_(OB_INVALID_ID) {}
  ~ObIndexUsageKey() {}

  uint64_t hash() const { return index_table_id_; }
  inline int hash(uint64_t &hash_val) const {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObIndexUsageKey &other) const {
    return index_table_id_ == other.index_table_id_;
  }
  int assign(const ObIndexUsageKey &key) {
    index_table_id_ = key.index_table_id_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(index_table_id));

public:
  uint64_t index_table_id_;
};

/* strcut stores increment stasitic data */
struct ObIndexUsageInfo final
{
public:
  ObIndexUsageInfo()
      : total_access_count_(0),
        total_exec_count_(0),
        total_rows_returned_(0),
        bucket_0_access_count_(0),
        bucket_1_access_count_(0),
        bucket_2_10_access_count_(0),
        bucket_2_10_rows_returned_(0),
        bucket_11_100_access_count_(0),
        bucket_11_100_rows_returned_(0),
        bucket_101_1000_access_count_(0),
        bucket_101_1000_rows_returned_(0),
        bucket_1000_plus_access_count_(0),
        bucket_1000_plus_rows_returned_(0),
        last_used_time_(ObTimeUtility::current_time()) {}
  ~ObIndexUsageInfo() {}

  void reset() {
    total_access_count_ = 0;
    total_exec_count_ = 0;
    total_rows_returned_ = 0;
    bucket_0_access_count_ = 0;
    bucket_1_access_count_ = 0;
    bucket_2_10_access_count_ = 0;
    bucket_2_10_rows_returned_ = 0;
    bucket_11_100_access_count_ = 0;
    bucket_11_100_rows_returned_ = 0;
    bucket_101_1000_access_count_ = 0;
    bucket_101_1000_rows_returned_ = 0;
    bucket_1000_plus_access_count_ = 0;
    bucket_1000_plus_rows_returned_ = 0;
    last_used_time_ = ObTimeUtility::current_time();
  }

  bool has_data () {
    return (total_access_count_ > 0 ||
            total_exec_count_ > 0 ||
            total_rows_returned_ > 0 ||
            bucket_0_access_count_ > 0 ||
            bucket_1_access_count_ > 0 ||
            bucket_2_10_access_count_ > 0 ||
            bucket_2_10_rows_returned_ > 0 ||
            bucket_11_100_access_count_ > 0 ||
            bucket_11_100_rows_returned_ > 0 ||
            bucket_101_1000_access_count_ > 0 ||
            bucket_101_1000_rows_returned_ > 0 ||
            bucket_1000_plus_access_count_ > 0 ||
            bucket_1000_plus_rows_returned_ > 0);
  }

  int assign (const ObIndexUsageInfo &info) {
    total_access_count_ = info.total_access_count_;
    total_exec_count_ = info.total_exec_count_;
    total_rows_returned_ = info.total_rows_returned_;
    bucket_0_access_count_ = info.bucket_0_access_count_;
    bucket_1_access_count_ = info.bucket_1_access_count_;
    bucket_2_10_access_count_ = info.bucket_2_10_access_count_;
    bucket_2_10_rows_returned_ = info.bucket_2_10_rows_returned_;
    bucket_11_100_access_count_ = info.bucket_11_100_access_count_;
    bucket_11_100_rows_returned_ = info.bucket_11_100_rows_returned_;
    bucket_101_1000_access_count_ = info.bucket_101_1000_access_count_;
    bucket_101_1000_rows_returned_ = info.bucket_101_1000_rows_returned_;
    bucket_1000_plus_access_count_ = info.bucket_1000_plus_access_count_;
    bucket_1000_plus_rows_returned_ = info.bucket_1000_plus_rows_returned_;
    last_used_time_ = info.last_used_time_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(total_access_count), K_(total_exec_count), K_(total_rows_returned),
               K_(bucket_0_access_count), K_(bucket_1_access_count),
               K_(bucket_2_10_access_count), K_(bucket_2_10_rows_returned),
               K_(bucket_11_100_access_count), K_(bucket_11_100_rows_returned),
               K_(bucket_101_1000_access_count), K_(bucket_101_1000_rows_returned),
               K_(bucket_1000_plus_access_count), K_(bucket_1000_plus_rows_returned),
               K_(last_used_time));

public:
  volatile int64_t total_access_count_;
  volatile int64_t total_exec_count_;
  volatile int64_t total_rows_returned_;
  volatile int64_t bucket_0_access_count_;
  volatile int64_t bucket_1_access_count_;
  volatile int64_t bucket_2_10_access_count_;
  volatile int64_t bucket_2_10_rows_returned_;
  volatile int64_t bucket_11_100_access_count_;
  volatile int64_t bucket_11_100_rows_returned_;
  volatile int64_t bucket_101_1000_access_count_;
  volatile int64_t bucket_101_1000_rows_returned_;
  volatile int64_t bucket_1000_plus_access_count_;
  volatile int64_t bucket_1000_plus_rows_returned_;
  int64_t last_used_time_;
};

typedef common::hash::ObHashMap<ObIndexUsageKey, ObIndexUsageInfo, common::hash::ReadWriteDefendMode> ObIndexUsageHashMap;
typedef common::hash::ObHashMap<ObIndexUsageKey, uint64_t, common::hash::ReadWriteDefendMode> IndexUsageDeletedMap;
typedef common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> ObIndexUsagePair;
typedef common::ObList<ObIndexUsagePair, common::ObIAllocator> ObIndexUsagePairList;

class ObIndexUsageReportTask : public common::ObTimerTask
{
  static const int64_t MAX_DUMP_ITEM_COUNT = 6000;
  static const int64_t DUMP_BATCH_SIZE = 100;
  static const int64_t MAX_CHECK_NOT_EXIST_CNT = 8;  // 2h
  static const int64_t MAX_DELETE_HASHMAP_SIZE = 3000;
public:
  ObIndexUsageReportTask();
  virtual ~ObIndexUsageReportTask() {};

  int init(ObIndexUsageInfoMgr *mgr);
  void destroy();
  void set_sql_proxy(common::ObMySQLProxy *sql_proxy) { sql_proxy_ = sql_proxy; }
  void set_mgr(ObIndexUsageInfoMgr *mgr) { mgr_ = mgr; }
  void set_is_inited(const bool is_inited) { is_inited_ = is_inited; }
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  ObIndexUsageInfoMgr *get_mgr() { return mgr_; }
  bool get_is_inited() { return is_inited_; }

private:
  virtual void runTimerTask();
  int storage_index_usage(const ObIndexUsagePairList &info_list);
  int del_index_usage(const ObIndexUsageKey &key);
  int check_and_delete(const ObIArray<ObIndexUsageKey> &candidate_deleted_item, ObIndexUsageHashMap *hashmap);
  int dump();

private:
  struct GetIndexUsageItemsFn final
  {
  public:
    GetIndexUsageItemsFn(IndexUsageDeletedMap &deleted_map, const uint64_t tenant_id, common::ObIAllocator &allocator) :
      deleted_map_(deleted_map),
      schema_guard_(nullptr),
      tenant_id_(tenant_id),
      dump_items_(allocator),
      remove_items_(),
      total_dump_count_(0)
    {}
  public:
    ~GetIndexUsageItemsFn() = default;
    int operator() (common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &entry);
    void set_schema_guard(schema::ObSchemaGetterGuard *schema_guard) { schema_guard_ = schema_guard; }
  public:
    IndexUsageDeletedMap &deleted_map_;
    schema::ObSchemaGetterGuard *schema_guard_;
    uint64_t tenant_id_;
    ObIndexUsagePairList dump_items_;
    ObArray<ObIndexUsageKey> remove_items_;
    uint64_t total_dump_count_;
  };
private:
  bool is_inited_;
  ObIndexUsageInfoMgr *mgr_;
  common::ObMySQLProxy *sql_proxy_; // 写入内部表需要 sql proxy
  IndexUsageDeletedMap deleted_map_;
};

class ObIndexUsageRefreshConfTask : public common::ObTimerTask
{
  //friend ObIndexUsageInfoMgr;
public:
  ObIndexUsageRefreshConfTask();
  virtual ~ObIndexUsageRefreshConfTask() {};
  int init(ObIndexUsageInfoMgr *mgr);
  void destroy();
  void set_mgr(ObIndexUsageInfoMgr *mgr) { mgr_ = mgr; }
  void set_is_inited(const bool is_inited) { is_inited_ = is_inited; }
  ObIndexUsageInfoMgr *get_mgr() { return mgr_; }
  bool get_is_inited() { return is_inited_; }

private:
  virtual void runTimerTask();

private:
  bool is_inited_;
  ObIndexUsageInfoMgr *mgr_;
};

// callback for update or reset map value
class ObIndexUsageOp final
{
public:
  explicit ObIndexUsageOp(ObIndexUsageOpMode mode, const uint64_t time = 0) :
    op_mode_(mode), old_info_(), current_time_(time) {}
  virtual ~ObIndexUsageOp() {}
  void operator() (common::hash::HashMapPair<ObIndexUsageKey, ObIndexUsageInfo> &data);
  const ObIndexUsageInfo &retrive_info() { return old_info_; }

private:
  ObIndexUsageOpMode op_mode_;
  ObIndexUsageInfo old_info_;
  uint64_t current_time_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexUsageOp);
};

#define INDEX_USAGE_INFO_MGR (MTL(ObIndexUsageInfoMgr*))

class ObIndexUsageInfoMgr final
{
  static const int64_t SAMPLE_RATIO = 10; // 采样模式下的采样比例 10%
  static const int64_t DEFAULT_MAX_HASH_BUCKET_CNT = 3000;
  static const int64_t INDEX_USAGE_REFRESH_CONF_INTERVAL = 2 * 1000 * 1000L; // 2s
  static const int64_t ONE_HASHMAP_MEMORY = 4 << 20; // 4M
  static const int64_t DEFAULT_CPU_QUATO_CONCURRENCY = 2;
#ifdef ERRSIM
  static const int64_t INDEX_USAGE_REPORT_INTERVAL = 2 * 1000L * 1000L; // 2s
#else
  static const int64_t INDEX_USAGE_REPORT_INTERVAL = 15 * 60 * 1000L * 1000L; // 15min
#endif

public:
  static int mtl_init(ObIndexUsageInfoMgr *&index_usage_mgr);
  ObIndexUsageInfoMgr();
  ~ObIndexUsageInfoMgr();

public:
  int start(); // start timer task
  int init(const uint64_t tenant_id);
  void stop();
  void wait();
  void destroy();
  void update(const uint64_t tenant_id, const uint64_t index_table_id);

  void refresh_config();
  void set_is_enabled(const bool is_enable) { is_enabled_ = is_enable; }
  void set_is_sample_mode(const bool mode) { is_sample_mode_ = mode; }
  void set_max_entries(const uint64_t entries) { max_entries_ = entries; }
  void set_current_time(const uint64_t time) { current_time_ = time; }
  void set_min_tenant_data_version(const uint64_t version) { min_tenant_data_version_ = version; }

  bool get_is_enabled() { return is_enabled_; }
  bool get_is_sample_mode() { return is_sample_mode_; }

  uint64_t get_max_entries() { return max_entries_; }
  uint64_t get_current_time() { return current_time_; }
  uint64_t get_min_tenant_data_version() { return min_tenant_data_version_; }
  uint64_t get_tenant_id() { return tenant_id_; }
  uint64_t calc_hashmap_count(const uint64_t tenant_id);
  uint64_t get_hashmap_count() { return hashmap_count_; }

  ObIndexUsageHashMap *get_index_usage_map() { return index_usage_map_; }
  common::ObIAllocator &get_allocator() { return allocator_; }

private:
  bool sample_filterd(const uint64_t random_num);
  void destroy_hash_map();
  int create_hash_map(const uint64_t tenant_id);

private:
  bool is_inited_;
  bool is_enabled_;
  bool is_sample_mode_;
  uint64_t max_entries_;
  uint64_t current_time_;
  uint64_t min_tenant_data_version_;
  uint64_t tenant_id_;
  uint64_t hashmap_count_;
  ObIndexUsageHashMap *index_usage_map_;
  ObIndexUsageReportTask report_task_;
  ObIndexUsageRefreshConfTask refresh_conf_task_;
  common::ObFIFOAllocator allocator_;
};

} // namespace share
} // namespace oceanbase
#endif
