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

class ObTableGroupCommitOps;
typedef common::ObFixedArray<ObTableOperation, common::ObIAllocator> OpFixedArray;
typedef common::ObFixedArray<ObTableOperationResult, common::ObIAllocator> ResultFixedArray;

struct ObTableGroupCommitSingleOp : public common::ObDLinkBase<ObTableGroupCommitSingleOp>
{
public:
  ObTableGroupCommitSingleOp()
      : req_(nullptr),
        timeout_ts_(0),
        timeout_(0),
        tablet_id_(ObTabletID::INVALID_TABLET_ID),
        is_insup_use_put_(false)
  {}
  virtual ~ObTableGroupCommitSingleOp() {}
  TO_STRING_KV(K_(entity),
               K_(op),
               KPC_(req),
               K_(timeout_ts),
               K_(timeout),
               K_(tablet_id),
               K_(is_insup_use_put));
public:
  OB_INLINE bool is_valid() const { return OB_NOT_NULL(req_) && timeout_ts_ != 0; }
  OB_INLINE bool is_get() const { return op_.type() == ObTableOperationType::Type::GET; }
  OB_INLINE bool is_insup_use_put() const { return is_insup_use_put_; }
  void reset()
  {
    entity_.reset();
    op_.reset();
    req_ = nullptr;
    timeout_ts_ = 0;
    timeout_ = 0;
    tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    is_insup_use_put_ = false;
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
  int64_t timeout_;
  ObTabletID tablet_id_;
  bool is_insup_use_put_;
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
  ObTableGroupCommitKey(share::ObLSID ls_id,
                      ObTableID table_id,
                      int64_t schema_version,
                      ObTableOperationType::Type op_type)
    : is_inited_(false),
      ls_id_(ls_id),
      table_id_(table_id),
      schema_version_(schema_version),
      op_type_(op_type),
      is_insup_use_put_(false),
      is_fail_group_key_(false)
  {}

  ObTableGroupCommitKey()
    : is_inited_(false),
      ls_id_(),
      table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      op_type_(ObTableOperationType::Type::INVALID),
      is_insup_use_put_(false),
      is_fail_group_key_(false)
  {}
  virtual ~ObTableGroupCommitKey() {}
  int init();
  TO_STRING_KV(K_(is_inited),
               K_(ls_id),
               K_(table_id),
               K_(schema_version),
               K_(op_type),
               K_(hash),
               K_(is_insup_use_put),
               K_(is_fail_group_key));
public:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTableID table_id_;
  int64_t schema_version_;
  ObTableOperationType::Type op_type_;
  bool is_insup_use_put_;  // just for marked the source op_type
  bool is_fail_group_key_;
  uint64_t hash_;
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupCommitKey);
};

struct ObTableGroupMeta final
{
public:
  ObTableGroupMeta()
  {
    reset();
  }
  virtual ~ObTableGroupMeta() = default;
  TO_STRING_KV(K_(is_inited),
               K_(is_get),
               K_(is_same_type),
               K_(credential),
               K_(ls_id),
               K_(table_id),
               K_(entity_type));
public:
  int init(bool is_get,
           bool is_same_type,
           const ObTableApiCredential &credential,
           const share::ObLSID &ls_id,
           uint64_t table_id,
           ObTableEntityType entity_type);
  int init(const ObTableGroupMeta &other);
  void reset()
  {
    is_inited_ = false;
    is_get_ = false;
    is_same_type_ = true;
    ls_id_.reset();
    table_id_ = common::OB_INVALID_ID;
    entity_type_ = ObTableEntityType::ET_DYNAMIC;
  }
public:
  bool is_inited_;
  bool is_get_;
  bool is_same_type_;
  ObTableApiCredential credential_;
  share::ObLSID ls_id_;
  uint64_t table_id_;
  ObTableEntityType entity_type_;
};

struct ObTableGroupCommitOps : public common::ObDLinkBase<ObTableGroupCommitOps>
{
public:
  static const int64_t DEFAULT_OP_SIZE = 200;
  ObTableGroupCommitOps()
  {
    ops_.set_attr(ObMemAttr(MTL_ID(), "KvTbGroup"));
    reset();
  }
  virtual ~ObTableGroupCommitOps() = default;
  TO_STRING_KV(K_(is_inited),
               K_(meta),
               K_(timeout),
               K_(timeout_ts),
               K_(ops),
               K_(tablet_ids));
public:
  void reset()
  {
    is_inited_ = false;
    meta_.reset();
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reset();
    tablet_ids_.reset();
  }
  void reuse()
  {
    is_inited_ = false;
    meta_.reset();
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reuse();
    tablet_ids_.reuse();
  }
  int init(const ObTableGroupMeta &meta, ObIArray<ObTableGroupCommitSingleOp *> &ops);
  int init(const ObTableGroupMeta &meta,
           ObTableGroupCommitSingleOp *op,
           int64_t timeout,
           int64_t timeout_ts);
  OB_INLINE bool is_get() const { return meta_.is_get_; }
  OB_INLINE bool is_same_type() const { return meta_.is_same_type_; }
  int get_ops(common::ObIArray<ObTableOperation> &ops);
public:
  bool is_inited_;
  ObTableGroupMeta meta_;
  int64_t timeout_;
  int64_t timeout_ts_;
  common::ObSEArray<ObTableGroupCommitSingleOp *, DEFAULT_OP_SIZE> ops_;
  common::ObSEArray<ObTabletID, DEFAULT_OP_SIZE> tablet_ids_;
};

struct ObTableLsGroup final
{
public:
  int64_t MAX_SLOT_SIZE = 30000;
  int8_t EXECUTABLE_BATCH_SIZE_FACTOR = 2;
public:
  ObTableLsGroup()
      : is_inited_(false),
        last_active_ts_(0),
        allocator_("TbLsGroup", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        queue_()
  {}
  virtual ~ObTableLsGroup()
  {
    queue_.destroy();
    allocator_.reset();
  }
  TO_STRING_KV(K_(is_inited), K_(last_active_ts), K_(meta));
public:
  int init(const ObTableApiCredential &credential,
           const share::ObLSID &ls_id,
           uint64_t table_id,
           ObTableEntityType entity_type,
           ObTableOperationType::Type op_type);
  int get_executable_batch(int64_t batch_size, ObIArray<ObTableGroupCommitSingleOp*> &batch_ops, bool check_queue_size = true);
  int add_op_to_queue(ObTableGroupCommitSingleOp *op, bool &add_op_success);
  bool has_executable_ops() { return queue_.get_curr_total() > 0;}
  int64_t get_queue_size() { return queue_.get_curr_total(); }
public:
  bool is_inited_;
  int64_t last_active_ts_;
  ObArenaAllocator allocator_;
  ObTableGroupMeta meta_;
  ObFixedQueue<ObTableGroupCommitSingleOp> queue_;
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

class ObTableExpiredGroups final
{
public:
  static const int64_t DEFAULT_EXPIRED_LS_GROUP_SIZE = 100;
   ObTableExpiredGroups()
      : is_inited_(false),
        allocator_(MTL_ID()),
        expired_groups_(allocator_),
        clean_groups_(allocator_)
  {}
  virtual ~ObTableExpiredGroups() {}
  TO_STRING_KV(K_(expired_groups), K_(clean_groups));
public:
  OB_INLINE int init()
  {
    int ret = OB_SUCCESS;
    const ObMemAttr attr(MTL_ID(), "TbGrpExpAlloc");
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "init twice", K(ret));
    } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      COMMON_LOG(WARN, "fail to init allocator", K(ret));
    } else {
      is_inited_ = true;
    }
    return ret;
  }

  OB_INLINE bool is_groups_empty() const { return expired_groups_.empty() && clean_groups_.empty(); }

  OB_INLINE int add_expired_group(ObTableLsGroup *ls_group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(expired_groups_.push_back(ls_group))) {
      COMMON_LOG(WARN, "fail to push back ls group", K(ret));
    }
    return ret;
  }

  OB_INLINE int add_clean_group(ObTableLsGroup *ls_group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(clean_groups_.push_back(ls_group))) {
      COMMON_LOG(WARN, "fail to push back ls group", K(ret));
    }
    return ret;
  }

  OB_INLINE int pop_expired_group(ObTableLsGroup *&ls_group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(expired_groups_.pop_front(ls_group))) {
      COMMON_LOG(WARN, "fail to pop front group", K(ret));
    }
    return ret;
  }

  OB_INLINE int pop_clean_group(ObTableLsGroup *&ls_group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(clean_groups_.pop_front(ls_group))) {
      COMMON_LOG(WARN, "fail to pop front group", K(ret));
    }
    return ret;
  }

  OB_INLINE int64_t get_clean_group_counts() const { return clean_groups_.size(); }
  OB_INLINE const common::ObList<ObTableLsGroup *, common::ObIAllocator>& get_expired_groups() const { return expired_groups_; }
  OB_INLINE const common::ObList<ObTableLsGroup *, common::ObIAllocator>& get_clean_groups() const { return clean_groups_; }
  int construct_trigger_requests(common::ObIAllocator &request_allocator,
                                common::ObIArray<ObTableGroupTriggerRequest*> &requests);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  common::ObFIFOAllocator allocator_;
  common::ObList<ObTableLsGroup *, common::ObIAllocator> expired_groups_; // store the expired ls_group moved from group map
  common::ObList<ObTableLsGroup *, common::ObIAllocator> clean_groups_; // store the ls_group ready to clean, moved from expired_groups_
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
  OB_INLINE static bool is_hybird_op(ObTableOperationType::Type op_type)
  {
    return op_type == ObTableOperationType::Type::UPDATE ||
      op_type == ObTableOperationType::Type::INSERT_OR_UPDATE ||
      op_type == ObTableOperationType::Type::INCREMENT ||
      op_type == ObTableOperationType::Type::APPEND;
  }
  OB_INLINE static bool is_write_op(ObTableOperationType::Type type) {
    return type == ObTableOperationType::Type::PUT ||
           type == ObTableOperationType::Type::INSERT ||
           type == ObTableOperationType::Type::INSERT_OR_UPDATE ||
           type == ObTableOperationType::Type::UPDATE ||
           type == ObTableOperationType::Type::DEL ||
           type == ObTableOperationType::Type::REPLACE ||
           type == ObTableOperationType::Type::INCREMENT ||
           type == ObTableOperationType::Type::APPEND;
  }
  OB_INLINE static bool is_read_op(ObTableOperationType::Type type) {
    return type == ObTableOperationType::Type::GET;
  }
  // only judge by config: kv_group_commit_batch_size
  static bool is_group_commit_config_enable();
  // judge by config: kv_group_commit_batch_size && kv_group_commit_rw_mode
  static bool is_group_commit_config_enable(ObTableOperationType::Type op_type);
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
      : read_ops_(0),
        write_ops_(0)
  {}
  ~ObTableGroupOpsCounter() = default;
  TO_STRING_KV(K_(read_ops),
               K_(write_ops));
public:
  OB_INLINE void inc(ObTableOperationType::Type type)
  {
    if (ObTableGroupUtils::is_write_op(type)) {
      ATOMIC_INC(&write_ops_);
    } else if (ObTableGroupUtils::is_read_op(type)) {
      ATOMIC_INC(&read_ops_);
    }
  }
  OB_INLINE int64_t get_ops() const { return ATOMIC_LOAD(&read_ops_) + ATOMIC_LOAD(&write_ops_); }
  OB_INLINE int64_t get_read_ops() const { return ATOMIC_LOAD(&read_ops_); }
  OB_INLINE int64_t get_write_ops() const { return ATOMIC_LOAD(&write_ops_); }
  OB_INLINE void reset_ops()
  {
    ATOMIC_STORE(&read_ops_, 0);
    ATOMIC_STORE(&write_ops_, 0);
  }
private:
  int64_t read_ops_;
  int64_t write_ops_;
};

struct ObTableLsGroupInfo final
{
public:
  enum GroupType: uint8_t {
    GET = 0,
    PUT,
    DEL,
    REPLACE,
    INSERT,
    HYBIRD,
    FAIL,
    INVALID
  };
public:
  ObTableLsGroupInfo()
    : client_addr_(),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      group_type_(GroupType::INVALID),
      ls_id_(),
      schema_version_(OB_INVALID_VERSION),
      queue_size_(-1),
      batch_size_(-1),
      gmt_created_(0),
      gmt_modified_(0)
  {}
  ~ObTableLsGroupInfo() {}
  int init(ObTableGroupCommitKey &commit_key);
  bool is_read_group() { return group_type_ == GroupType::GET; }
  bool is_fail_group() { return group_type_ == GroupType::FAIL; }
  bool is_normal_group() { return group_type_ != GroupType::INVALID && group_type_ != GroupType::FAIL; }
  const char* get_group_type_str();
  TO_STRING_KV(K_(client_addr),
              K_(tenant_id),
              K_(table_id),
              K_(schema_version),
              K_(table_id),
              K_(ls_id),
              K_(queue_size),
              K_(batch_size),
              K_(gmt_created),
              K_(gmt_modified));
public:
  common::ObAddr client_addr_;
  int64_t tenant_id_;
  common::ObTableID table_id_;
  GroupType group_type_;
  share::ObLSID ls_id_;
  int64_t schema_version_;
  int64_t queue_size_;
  int64_t batch_size_;
  int64_t gmt_created_;
  int64_t gmt_modified_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_COMMON_H_ */
