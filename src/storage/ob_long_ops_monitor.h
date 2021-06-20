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

#ifndef OCEANBASE_STORAGE_OB_CREATE_INDEX_MONITOR_H_
#define OCEANBASE_STORAGE_OB_CREATE_INDEX_MONITOR_H_

#include "lib/container/ob_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/task/ob_timer.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ob_resource_map.h"

namespace oceanbase {
namespace storage {

struct ObILongOpsTaskStat {
  enum class TaskState {
    INIT = 0,
    RUNNING,
    SUCCESS,
    FAIL,
  };
  enum class TaskType {
    INVALID = 0,
    SCAN,
    SORT,
  };
  ObILongOpsTaskStat() : task_id_(0), cpu_cost_(0), io_cost_(0), state_(TaskState::INIT), type_(TaskType::INVALID)
  {}
  virtual ~ObILongOpsTaskStat() = default;
  virtual int estimate_cost() = 0;
  double get_cost()
  {
    return cpu_cost_ + io_cost_;
  }
  bool is_valid() const
  {
    return task_id_ >= 0;
  }
  virtual int assign(const ObILongOpsTaskStat& task_stat);
  TO_STRING_KV(K_(task_id), K_(cpu_cost), K_(io_cost), K_(state), K_(type));
  int64_t task_id_;
  double cpu_cost_;
  double io_cost_;
  TaskState state_;
  TaskType type_;
};

struct ObCreateIndexScanTaskStat : public ObILongOpsTaskStat {
  ObCreateIndexScanTaskStat() : macro_count_(0)
  {}
  virtual ~ObCreateIndexScanTaskStat() = default;
  virtual int estimate_cost() override
  {
    io_cost_ = static_cast<double>(macro_count_);
    return common::OB_SUCCESS;
  }
  virtual int assign(const ObILongOpsTaskStat& task_stat) override;
  INHERIT_TO_STRING_KV("ObILongOpsTaskStat", ObILongOpsTaskStat, K(macro_count_));
  int64_t macro_count_;
};

struct ObCreateIndexSortTaskStat : public ObILongOpsTaskStat {
  ObCreateIndexSortTaskStat() : macro_count_(0), run_count_(0)
  {}
  virtual ~ObCreateIndexSortTaskStat()
  {}
  virtual int estimate_cost() override
  {
    io_cost_ = static_cast<double>(run_count_) * static_cast<double>(macro_count_);
    return common::OB_SUCCESS;
  }
  virtual int assign(const ObILongOpsTaskStat& task_stat) override;
  INHERIT_TO_STRING_KV("ObILongOpsTaskStat", ObILongOpsTaskStat, K(macro_count_), K_(run_count));
  int64_t macro_count_;
  int64_t run_count_;
};

struct ObILongOpsKey {
  ObILongOpsKey();
  virtual ~ObILongOpsKey() = default;
  virtual int64_t hash() const;
  virtual bool is_valid() const;
  bool operator==(const ObILongOpsKey& other) const;
  virtual int to_key_string()
  {
    return common::OB_NOT_SUPPORTED;
  }
  TO_STRING_KV(K_(tenant_id), K_(sid), K_(name), K_(target));
  uint64_t tenant_id_;
  uint64_t sid_;
  char name_[common::MAX_LONG_OPS_NAME_LENGTH];
  char target_[common::MAX_LONG_OPS_TARGET_LENGTH];
};

struct ObCreateIndexKey : public ObILongOpsKey {
  ObCreateIndexKey() : index_table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObCreateIndexKey()
  {}
  virtual int to_key_string() override;
  bool is_valid() const
  {
    return common::OB_INVALID_ID != index_table_id_ && common::OB_INVALID_ID != partition_id_;
  }
  INHERIT_TO_STRING_KV("ObILongOpsKey", ObILongOpsKey, K_(index_table_id), K_(partition_id));
  int64_t index_table_id_;
  int64_t partition_id_;
};

struct ObCommonOpsStatValue {
  ObCommonOpsStatValue();
  virtual ~ObCommonOpsStatValue() = default;
  ObCommonOpsStatValue& operator=(const ObCommonOpsStatValue& other);
  void reset();
  TO_STRING_KV(
      K_(start_time), K_(finish_time), K_(elapsed_time), K_(remaining_time), K_(percentage), K_(last_update_time));
  int64_t start_time_;
  int64_t finish_time_;
  int64_t elapsed_time_;
  int64_t remaining_time_;
  int64_t percentage_;
  int64_t last_update_time_;
  int64_t is_updated_;
  char message_[common::MAX_LONG_OPS_MESSAGE_LENGTH];
  common::SpinRWLock lock_;
};

struct ObILongOpsStat {
  enum class Type {
    CREATE_INDEX = 0,
  };
  ObILongOpsStat() : common_value_()
  {}
  virtual ~ObILongOpsStat()
  {}
  virtual Type get_type() const = 0;
  virtual bool is_valid() const = 0;
  virtual int estimate_cost() = 0;
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual int deep_copy(char* buf, const int64_t buf_len, ObILongOpsStat*& value) const = 0;
  virtual int update_task_stat(const ObILongOpsTaskStat& task_stat) = 0;
  virtual const ObILongOpsKey& get_key() const = 0;
  virtual int check_can_purge(bool& can_purge) = 0;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;
  ObCommonOpsStatValue common_value_;
};

struct ObCreateIndexPartitionStat : public ObILongOpsStat {
  ObCreateIndexPartitionStat();
  virtual ~ObCreateIndexPartitionStat();
  virtual int update_task_stat(const ObILongOpsTaskStat& task_stat) override;
  void reset();
  virtual int estimate_cost() override;
  int copy_task(const ObILongOpsTaskStat* src, ObILongOpsTaskStat*& dest);
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(*this);
  }
  virtual int deep_copy(char* buf, const int64_t buf_len, ObILongOpsStat*& value) const override;
  Type get_type() const override
  {
    return ObILongOpsStat::Type::CREATE_INDEX;
  }
  virtual bool is_valid() const override
  {
    return key_.is_valid();
  }
  virtual int check_can_purge(bool& can_purge) override;
  const ObILongOpsKey& get_key() const override
  {
    return key_;
  }
  TO_STRING_KV(K_(key), K_(task_stats), K_(common_value));
  static const int64_t MAX_LIFE_TIME = 7 * 3600 * 1000;  // 7 days
  ObCreateIndexKey key_;
  common::ObArray<ObILongOpsTaskStat*> task_stats_;
  common::ObArenaAllocator allocator_;
};

class ObILongOpsStatHandle : public ObResourceHandle<ObILongOpsStat> {
  friend class ObLongOpsMonitor;

public:
  ObILongOpsStatHandle();
  virtual ~ObILongOpsStatHandle();
  void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObILongOpsStatHandle);
};

using MAP = storage::ObResourceMap<ObILongOpsKey, ObILongOpsStat>;
using PAIR = common::hash::HashMapPair<ObILongOpsKey, ObILongOpsStat*>;

class ObLongOpsMonitor {
public:
  static ObLongOpsMonitor& get_instance();
  int init();
  int add_long_ops_stat(const ObILongOpsKey& key, ObILongOpsStat& stat);
  int get_long_ops_stat(const ObILongOpsKey& key, ObILongOpsStatHandle& handle);
  int remove_long_ops_stat(const ObILongOpsKey& key);
  int update_task_stat(const ObILongOpsKey& key, const ObILongOpsTaskStat& new_task_stat);
  template <typename Callback>
  int foreach (Callback& callback);
  int dec_handle_ref(ObILongOpsStatHandle& handle);

private:
  ObLongOpsMonitor();
  virtual ~ObLongOpsMonitor();

private:
  static const int64_t DEFAULT_BUCKET_NUM = 1543L;
  static const int64_t TOTAL_LIMIT = 10 * 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 8 * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;
  bool is_inited_;
  MAP map_;
};

class ObLongOpsMonitorIterator {
public:
  class ObKeySnapshotCallback {
  public:
    explicit ObKeySnapshotCallback(common::ObIArray<ObILongOpsKey>& key_snapshot);
    virtual ~ObKeySnapshotCallback() = default;
    int operator()(PAIR& pair);

  private:
    common::ObIArray<ObILongOpsKey>& key_snapshot_;
  };
  ObLongOpsMonitorIterator();
  virtual ~ObLongOpsMonitorIterator();
  int init();
  int get_next_stat(ObILongOpsStatHandle& part_stat);

private:
  int make_key_snapshot();

private:
  bool is_inited_;
  common::ObArray<ObILongOpsKey> key_snapshot_;
  int64_t key_cursor_;
};

class ObPurgeCompletedMonitorInfoTask : public common::ObTimerTask {
public:
  ObPurgeCompletedMonitorInfoTask();
  virtual ~ObPurgeCompletedMonitorInfoTask(){};
  int init(share::schema::ObMultiVersionSchemaService* schema_service, int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  static const int64_t MAX_BATCH_COUNT = 10000;
  static const int64_t SCHEDULE_INTERVAL = 3600 * 1000;  // 1h
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
};

}  // end namespace storage
}  // end namespace oceanbase

#define LONG_OPS_MONITOR_INSTANCE (::oceanbase::storage::ObLongOpsMonitor::get_instance())

#endif  // OCEANBASE_STORAGE_OB_CREATE_INDEX_MONITOR_H_
