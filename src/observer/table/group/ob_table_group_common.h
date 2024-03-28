/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_GROUP_COMMON_H_
#define OCEANBASE_OBSERVER_OB_TABLE_GROUP_COMMON_H_

#include "observer/table/ob_table_trans_utils.h"
#include "observer/table/ob_table_schema_cache.h"
#include "observer/table/ob_table_session_pool.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/table/ob_table.h"
#include "share/table/ob_table_rpc_struct.h"
#include "rpc/ob_request.h"
#include "lib/list/ob_list.h"

namespace oceanbase
{

namespace table
{
typedef common::ObFixedArray<ObTableOperation, common::ObIAllocator> OpFixedArray;
typedef common::ObFixedArray<ObTableOperationResult, common::ObIAllocator> ResultFixedArray;

struct ObTableGroupCommitSingleOp : public common::ObDLinkBase<ObTableGroupCommitSingleOp>
{
public:
  ObTableGroupCommitSingleOp()
      : req_(nullptr),
        timeout_ts_(0)
  {}
  virtual ~ObTableGroupCommitSingleOp() {}
  TO_STRING_KV(K_(entity),
               K_(op),
               KPC_(req),
               K_(timeout_ts));
public:
  OB_INLINE bool is_valid() const { return OB_NOT_NULL(req_) && timeout_ts_ != 0; }
  void reset()
  {
    entity_.reset();
    op_.reset();
    req_ = nullptr;
    timeout_ts_ = 0;
  }
  void reuse()
  {
   reset();
  }
public:
  ObTableEntity entity_; // value is shaddow copy from rpc packet
  ObTableOperation op_; // single operation
  rpc::ObRequest *req_; // rpc request
  int64_t timeout_ts_;
};

// @note thread-safe
template <typename T>
class ObTableGroupFactory final
{
public:
  ObTableGroupFactory(common::ObIAllocator &alloc)
      : alloc_(alloc)
  {}
  virtual ~ObTableGroupFactory() { free_all(); }
  TO_STRING_KV(K(used_list_.get_size()),
               K(free_list_.get_size()));
public:
  T *alloc();
  void free(T *obj);
  void free_and_reuse();
  int64_t get_free_count() const { return free_list_.get_size(); }
  int64_t get_used_count() const { return used_list_.get_size(); }
  int64_t get_used_mem() const { return alloc_.used(); }
  int64_t get_total_mem() const { return alloc_.total(); }
  void free_all();
private:
  common::ObIAllocator &alloc_;
  common::ObSpinLock lock_;
  common::ObDList<T> used_list_;
  common::ObDList<T> free_list_;
};

template <typename T>
T *ObTableGroupFactory<T>::alloc()
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);

  T *obj = free_list_.remove_first();
  if (NULL == obj) {
    void *ptr = alloc_.alloc(sizeof(T));
    if (NULL == ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "fail to alloc memory", K(ret), K(sizeof(T)));
    } else {
      obj = new(ptr) T();
      used_list_.add_last(obj);
    }
  } else {
    used_list_.add_last(obj);
  }

  return obj;
}

template <typename T>
void ObTableGroupFactory<T>::free(T *obj)
{
  if (NULL != obj) {
    ObLockGuard<ObSpinLock> guard(lock_);
    obj->reuse();
    used_list_.remove(obj);
    free_list_.add_last(obj);
  }
}

template <typename T>
void ObTableGroupFactory<T>::free_and_reuse()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  while (!used_list_.is_empty()) {
    this->free(used_list_.get_first());
  }
}

template <typename T>
void ObTableGroupFactory<T>::free_all()
{
  ObLockGuard<ObSpinLock> guard(lock_);
  T *obj = NULL;
  while (NULL != (obj = used_list_.remove_first())) {
    obj->~T();
    alloc_.free(obj);
  }
  while (NULL != (obj = free_list_.remove_first())) {
    obj->~T();
    alloc_.free(obj);
  }
}


struct ObTableGroupCommitKey final
{
public:
  explicit ObTableGroupCommitKey(share::ObLSID ls_id,
                                 common::ObTabletID tablet_id,
                                 common::ObTableID table_id,
                                 int64_t schema_version,
                                 ObTableOperationType::Type op_type);
  virtual ~ObTableGroupCommitKey() {}
  TO_STRING_KV(K_(ls_id),
               K_(tablet_id),
               K_(table_id),
               K_(schema_version),
               K_(op_type),
               K_(hash));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTableID table_id_;
  int64_t schema_version_;
  ObTableOperationType::Type op_type_;
  uint64_t hash_;
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupCommitKey);
};

struct ObTableGroupCommitOps : public common::ObDLinkBase<ObTableGroupCommitOps>
{
public:
  static const int64_t DEFAULT_OP_SIZE = 200;
  ObTableGroupCommitOps()
  {
    reset();
  }
  virtual ~ObTableGroupCommitOps() = default;
  TO_STRING_KV(K_(is_inited),
               K_(credential),
               K_(ls_id),
               K_(table_id),
               K_(tablet_id),
               K_(entity_type),
               K_(timeout),
               K_(timeout_ts),
               K_(ops));
public:
  void reset()
  {
    is_inited_ = false;
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    table_id_ = common::OB_INVALID_ID;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reset();
    ops_.set_attr(ObMemAttr(MTL_ID(), "KvTbGroup"));
  }
  void reuse()
  {
    ls_id_ = share::ObLSID::INVALID_LS_ID;
    table_id_ = common::OB_INVALID_ID;
    tablet_id_ = common::ObTabletID::INVALID_TABLET_ID;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reuse();
  }
  int init(ObTableApiCredential credential,
           const share::ObLSID &ls_id,
           uint64_t table_id,
           common::ObTabletID tablet_id,
           ObTableEntityType entity_type,
           int64_t timeout_ts);
  int add_op(ObTableGroupCommitSingleOp *op);
  OB_INLINE bool need_execute(int64_t execute_size)
  {
    return ((execute_size != 0) && ops_.count() >= execute_size) || is_timeout();
  }

  OB_INLINE bool is_timeout() const
  {
    return ObTimeUtility::fast_current_time() - timeout_ts_ >= 0;
  }
  OB_INLINE bool is_get() const
  {
    return !ops_.empty() && ops_.at(0)->op_.type() == ObTableOperationType::Type::GET;
  }
  int get_ops(common::ObIArray<ObTableOperation> &ops);
public:
  bool is_inited_;
  ObTableApiCredential credential_; // for session info
  share::ObLSID ls_id_;
  uint64_t table_id_;
  common::ObTabletID tablet_id_;
  ObTableEntityType entity_type_;
  int64_t timeout_; // use in execute failed or timeout group
  int64_t timeout_ts_;
  common::ObSEArray<ObTableGroupCommitSingleOp *, DEFAULT_OP_SIZE> ops_;
};

typedef ObTableOperationResult ObTableGroupTriggerResult;
class ObTableGroupTriggerRequest final
{
public:
  ObTableGroupTriggerRequest()
      : is_inited_(false)
  {}
  virtual ~ObTableGroupTriggerRequest() = default;
  TO_STRING_KV(K_(is_inited),
               K_(op_request),
               K_(request_entity));
public:
  int init(const ObTableApiCredential &credential);
  OB_INLINE bool is_inited() const { return is_inited_; }
public:
  bool is_inited_;
  ObTableOperationRequest op_request_;
  ObTableEntity request_entity_;
  char credential_buf_[ObTableApiCredential::CREDENTIAL_BUF_SIZE];
};

class ObTableFailedGroups final
{
public:
  static const int64_t DEFAULT_FAILED_GROUP_SIZE = 200;
  ObTableFailedGroups(common::ObIAllocator &allocator)
      : failed_ops_(allocator)
  {}
  virtual ~ObTableFailedGroups() {}
  TO_STRING_KV(K_(failed_ops));
public:
  OB_INLINE bool empty() const { return failed_ops_.empty(); }
  OB_INLINE int64_t count() const { return failed_ops_.size(); }
  OB_INLINE int add(ObTableGroupCommitOps *group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    return failed_ops_.push_back(group);
  }
  OB_INLINE ObTableGroupCommitOps* get()
  {
    int ret = OB_SUCCESS;
    ObTableGroupCommitOps *group = nullptr;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (OB_FAIL(failed_ops_.pop_front(group))) {
      COMMON_LOG(WARN, "fail to pop front group", K(ret));
    }
    return group;
  }
  OB_INLINE const common::ObList<ObTableGroupCommitOps*, common::ObIAllocator>& get_failed_groups() const { return failed_ops_; }
  int construct_trigger_requests(common::ObIAllocator &request_allocator,
                                 common::ObIArray<ObTableGroupTriggerRequest*> &requests);
private:
  common::ObSpinLock lock_;
  common::ObList<ObTableGroupCommitOps*, common::ObIAllocator> failed_ops_;
};

class ObTableGroupUtils final
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 10 * 1000 * 1000L; // 10s
  static int init_sess_guard(ObTableGroupCommitOps &group,
                             ObTableApiSessGuard &sess_guard);
  static int init_schema_cache_guard(const ObTableGroupCommitOps &group,
                                     share::schema::ObSchemaGetterGuard &schema_guard,
                                     ObKvSchemaCacheGuard &cache_guard,
                                     const share::schema::ObSimpleTableSchemaV2 *&simple_schema);
  static int trigger(const ObTableGroupTriggerRequest &request);
};

template <typename T>
class ObTableMovingAverage final
{
public:
  ObTableMovingAverage(int64_t window_size)
      : malloc_(ObMemAttr(MTL_ID(), "TbMvAvg")),
        values_(malloc_),
        window_size_(window_size),
        total_(0)
  {}
  virtual ~ObTableMovingAverage() = default;
  TO_STRING_KV(K_(values),
               K_(window_size),
               K_(total));
public:
  OB_INLINE int add(T value)
  {
    int ret = OB_SUCCESS;
    values_.push_back(value);
    total_ += value;

    if (values_.size() > window_size_) {
      T front = 0;
      if (OB_FAIL(values_.pop_front(front))) {
        COMMON_LOG(WARN, "fail to pop front", K(ret));
      } else {
        total_ -= front;
      }
    }
    return ret;
  }
  OB_INLINE double get_average() const
  {
    double avg = 0;
    if (!values_.empty()) {
      avg = total_ / values_.size();
    }
    return avg;
  }
private:
  ObMalloc malloc_;
  common::ObList<T, common::ObIAllocator> values_;
  int64_t window_size_;
  double total_;
};

class ObTableGroupOpsCounter
{
public:
  ObTableGroupOpsCounter()
      : get_ops_(0),
        put_ops_(0)
  {}
  ~ObTableGroupOpsCounter() = default;
  TO_STRING_KV(K_(get_ops),
               K_(put_ops));
public:
  OB_INLINE void inc(ObTableOperationType::Type type)
  {
    if (type == ObTableOperationType::Type::PUT) {
      ATOMIC_INC(&put_ops_);
    } else if (type == ObTableOperationType::Type::GET) {
      ATOMIC_INC(&get_ops_);
    }
  }
  OB_INLINE int64_t get_ops() const { return ATOMIC_LOAD(&get_ops_) + ATOMIC_LOAD(&put_ops_); }
  OB_INLINE int64_t get_get_op_ops() const { return ATOMIC_LOAD(&get_ops_); }
  OB_INLINE int64_t get_put_op_ops() const { return ATOMIC_LOAD(&put_ops_); }
  OB_INLINE void reset_ops()
  {
    ATOMIC_STORE(&get_ops_, 0);
    ATOMIC_STORE(&put_ops_, 0);
  }
private:
  int64_t get_ops_;
  int64_t put_ops_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_COMMON_H_ */
