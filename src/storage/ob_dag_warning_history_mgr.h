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

#ifndef SRC_STORAGE_OB_DAG_WARNING_HISTORY_MGR_H_
#define SRC_STORAGE_OB_DAG_WARNING_HISTORY_MGR_H_

#include "common/ob_simple_iterator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_string.h"
#include "lib/ob_errno.h"
#include "lib/hash/ob_hashmap.h"
#include "share/scheduler/ob_dag_scheduler.h"

namespace oceanbase {
namespace storage {

template <typename T>
class ObNodeArray {
public:
  ObNodeArray();
  ~ObNodeArray();
  void destory();
  void clear();
  int init(const char* label, const int64_t max_cnt);
  T* at(const int64_t idx);
  int get_node(int64_t& idx);
  int free_node(const int64_t idx);
  bool is_empty(const int64_t idx)
  {
    return free_node_bitset_.has_member(idx);
  }

  bool is_inited_;
  int64_t max_cnt_;
  common::ObArenaAllocator allocator_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE> free_node_bitset_;
  T* array_;
};

template <typename Key, typename Value>
class ObInfoManager {
public:
  ObInfoManager();
  virtual ~ObInfoManager();

  int init(const int64_t bucket_num, const char* label, const int64_t max_cnt);
  void destory();
  void clear();
  int get(Key key, Value*& item);
  int del(Key key);
  int alloc_and_add(Key key, Value*& item);
  int size()
  {
    return map_.size();
  }

private:
  typedef common::hash::ObHashMap<Key, int64_t> InfoMap;  // Value of map: index in node_array_

protected:
  common::SpinRWLock lock_;
  int64_t max_cnt_;
  ObNodeArray<Value> node_array_;

private:
  bool is_inited_;
  InfoMap map_;
};

enum ObDagStatus {
  ODS_WARNING = 0,
  ODS_RETRYED,
  ODS_MAX,
};

static common::ObString ObDagStatusStr[ODS_MAX] = {"WARNING", "RETRYED"};

static common::ObString ObDagModuleStr[share::ObIDag::DAG_TYPE_MAX] = {"EMPTY",
    "COMPACTION",
    "COMPACTION",
    "INDEX",
    "SPLIT",
    "OTHER",
    "MIGRATE",
    "COMPACTION",
    "MIGRATE",
    "INDEX",
    "COMPACTION",
    "TRANS_TABLE_MERGE",
    "FAST_RECOVERY",
    "FAST_RECOVERY",
    "BACKUP",
    "OTHER",
    "OTHER",
    "OTHER"};

struct ObDagWarningInfo {
public:
  ObDagWarningInfo();
  ~ObDagWarningInfo();
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(dag_type), K_(dag_ret), K_(dag_status), K_(gmt_create), K_(gmt_modified),
      K_(warning_info));
  ObDagWarningInfo& operator=(const ObDagWarningInfo& other);

private:
  bool operator==(const ObDagWarningInfo& other) const;  // for unittest
public:
  int64_t tenant_id_;
  share::ObDagId task_id_;
  share::ObIDag::ObIDagType dag_type_;
  int64_t dag_ret_;
  ObDagStatus dag_status_;
  int64_t gmt_create_;
  int64_t gmt_modified_;
  char warning_info_[common::OB_DAG_WARNING_INFO_LENGTH];
};

inline void ObDagWarningInfo::reset()
{
  tenant_id_ = 0;
  task_id_.reset();
  dag_type_ = share::ObIDag::ObIDagType::DAG_TYPE_MAX;
  dag_ret_ = OB_SUCCESS;
  dag_status_ = ODS_MAX;
  gmt_create_ = 0;
  gmt_modified_ = 0;
  MEMSET(warning_info_, '\0', common::OB_DAG_WARNING_INFO_LENGTH);
}

/*
 * ObDagWarningHistoryManager
 * */

class ObDagWarningHistoryManager : public ObInfoManager<int64_t, ObDagWarningInfo> {
public:
  ObDagWarningHistoryManager()
  {}
  ~ObDagWarningHistoryManager()
  {}

  static ObDagWarningHistoryManager& get_instance()
  {
    static ObDagWarningHistoryManager instance_;
    return instance_;
  }

  int init()
  {
    return ObInfoManager::init(BUCKET_NUM, "ObDagWarningHistory", DAG_WARNING_INFO_MAX_CNT);
  }

  int add_dag_warning_info(share::ObIDag* dag);
  int get_info(const int64_t pos, ObDagWarningInfo& info);

private:
  friend class ObDagWarningInfoIterator;

private:
  static const int64_t BUCKET_NUM = 98317l;
  static const int64_t DAG_WARNING_INFO_MAX_CNT = 100 * 1000;  // 10w
};

/*
 * ObDagWarningInfoIterator
 * */

class ObDagWarningInfoIterator {
public:
  ObDagWarningInfoIterator() : cur_idx_(0), is_opened_(false)
  {}
  virtual ~ObDagWarningInfoIterator()
  {
    reset();
  }
  int open();
  int get_next_info(ObDagWarningInfo& info);
  void reset()
  {
    cur_idx_ = 0;
    is_opened_ = false;
  }

private:
  int64_t cur_idx_;
  bool is_opened_;
};

/*
 * ObNodeArray Func
 * */

template <typename T>
ObNodeArray<T>::ObNodeArray() : is_inited_(false), max_cnt_(0), allocator_(), free_node_bitset_(), array_(NULL)
{}

template <typename T>
ObNodeArray<T>::~ObNodeArray()
{}

template <typename T>
void ObNodeArray<T>::destory()
{
  max_cnt_ = 0;
  free_node_bitset_.clear_all();
  allocator_.free(array_);
  array_ = NULL;
  allocator_.reset();
  is_inited_ = false;
}

template <typename T>
void ObNodeArray<T>::clear()
{
  free_node_bitset_.set_all();
}

template <typename T>
int ObNodeArray<T>::init(const char* label, const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(free_node_bitset_.prepare_allocate(max_cnt))) {
    STORAGE_LOG(WARN, "failed to init bitset", K(ret), K(max_cnt));
  } else {
    void* buf = NULL;
    allocator_.set_label(label);
    if (NULL == (buf = allocator_.alloc(sizeof(T) * max_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc info node", K(ret));
    } else {
      array_ = new (buf) T[max_cnt];
      free_node_bitset_.set_all();
      max_cnt_ = max_cnt;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
T* ObNodeArray<T>::at(const int64_t idx)
{
  T* ret_ptr = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(WARN, "ObNodeArray has not been inited");
  } else if (idx >= 0 && idx < max_cnt_) {
    ret_ptr = &array_[idx];
  }
  return ret_ptr;
}

template <typename T>
int ObNodeArray<T>::get_node(int64_t& idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObNodeArray has not been inited", K(ret));
  } else if (free_node_bitset_.is_empty()) {
    ret = OB_SIZE_OVERFLOW;
    if (REACH_TIME_INTERVAL(30 * 1000 * 1000)) {
      STORAGE_LOG(WARN, "info node is reach uplimits", K(ret), K(max_cnt_));
    }
  } else if (OB_FAIL(free_node_bitset_.find_first(idx)) || idx < 0) {
    STORAGE_LOG(WARN, "failed to find free pos", K(ret), K(idx), K(max_cnt_));
  } else if (OB_FAIL(free_node_bitset_.del_member(idx))) {
    STORAGE_LOG(WARN, "failed to del bitset member", K(ret), K(idx));
  }
  return ret;
}

template <typename T>
int ObNodeArray<T>::free_node(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObNodeArray has not been inited", K(ret));
  } else if (idx < 0 || idx >= max_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid index", K(ret), K(idx), K(max_cnt_));
  } else if (OB_FAIL(free_node_bitset_.add_member(idx))) {
    STORAGE_LOG(WARN, "failed to add bitset member", K(ret), K(idx));
  } else {
    array_[idx].reset();
  }
  return ret;
}

/*
 * ObInfoManager Func
 * */
template <typename Key, typename Value>
ObInfoManager<Key, Value>::ObInfoManager() : lock_(), max_cnt_(0), node_array_(), is_inited_(false), map_()
{}

template <typename Key, typename Value>
ObInfoManager<Key, Value>::~ObInfoManager()
{
  destory();
}

template <typename Key, typename Value>
int ObInfoManager<Key, Value>::init(const int64_t bucket_num, const char* label, const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.create(bucket_num, label))) {
    STORAGE_LOG(WARN, "failed to create map", K(ret), K(bucket_num), K(label));
  } else if (OB_FAIL(node_array_.init(label, max_cnt))) {
    STORAGE_LOG(WARN, "failed to init node array", K(ret), K(max_cnt), K(label));
  } else {
    max_cnt_ = max_cnt;
    is_inited_ = true;
  }
  return ret;
}

template <typename Key, typename Value>
void ObInfoManager<Key, Value>::destory()
{
  common::SpinWLockGuard guard(lock_);
  max_cnt_ = 0;
  map_.destroy();
  node_array_.destory();
  is_inited_ = false;
}

template <typename Key, typename Value>
void ObInfoManager<Key, Value>::clear()
{
  common::SpinWLockGuard guard(lock_);
  map_.clear();
  node_array_.clear();
}

template <typename Key, typename Value>
int ObInfoManager<Key, Value>::get(Key key, Value*& ptr)
{
  int ret = OB_SUCCESS;
  int64_t node_index;
  ptr = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObInfoManager has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, node_index))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret), K(key));
    }
  } else if (NULL == (ptr = node_array_.at(node_index))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get from map", K(ret), K(node_index), K(ptr));
  }
  return ret;
}

template <typename Key, typename Value>
int ObInfoManager<Key, Value>::del(Key key)
{
  int ret = OB_SUCCESS;
  int64_t node_index = -1;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObInfoManager has not been inited", K(ret));
  } else if (OB_FAIL(map_.get_refactored(key, node_index))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get from map", K(ret));
    }
  } else if (OB_FAIL(map_.erase_refactored(key))) {
    STORAGE_LOG(WARN, "fail to erase from map", K(ret));
  } else if (OB_FAIL(node_array_.free_node(node_index))) {
    STORAGE_LOG(WARN, "fail to free node", K(ret), K(node_index));
  }
  return ret;
}

template <typename Key, typename Value>
int ObInfoManager<Key, Value>::alloc_and_add(Key key, Value*& ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObInfoManager has not been inited", K(ret));
  } else {
    int64_t node_idx = -1;
    ptr = NULL;
    if (OB_FAIL(node_array_.get_node(node_idx))) {
      if (OB_SIZE_OVERFLOW != ret) {
        STORAGE_LOG(WARN, "fail to allocate memory", K(ret), K(sizeof(Value)));
      }
    } else if (NULL == (ptr = node_array_.at(node_idx))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to get node ptr from array", K(ret), K(node_idx));
    } else if (OB_FAIL(map_.set_refactored(key, node_idx))) {
      STORAGE_LOG(WARN, "fail to set to map", K(ret));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_DAG_WARNING_HISTORY_MGR_H_ */
