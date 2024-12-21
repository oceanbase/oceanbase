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
#include "ob_i_table_struct.h"
namespace oceanbase
{

namespace table
{

struct ObTableGroupMeta : public ObITableGroupMeta
{
public:
  ObTableGroupMeta()
    : ObITableGroupMeta(),
      op_type_(ObTableOperationType::Type::INVALID),
      entity_type_(ObTableEntityType::ET_DYNAMIC),
      credential_(),
      table_id_(OB_INVALID_ID)
  {}
  ObTableGroupMeta(ObTableGroupType op_type)
    : ObITableGroupMeta(op_type),
      op_type_(ObTableOperationType::Type::INVALID),
      entity_type_(ObTableEntityType::ET_DYNAMIC),
      credential_(),
      table_id_(OB_INVALID_ID)
  {}
  VIRTUAL_TO_STRING_KV(K_(op_type), K_(entity_type), K_(credential), K_(table_id));
  virtual int deep_copy(common::ObIAllocator &allocator, const ObITableGroupMeta &other);
  virtual void reset();
public:
  ObTableOperationType::Type op_type_;
  ObTableEntityType entity_type_;
  ObTableApiCredential credential_;
  common::ObTableID table_id_;
};

struct ObTableGroup : public common::ObDLinkBase<ObTableGroup>
{
public:
  static const int64_t DEFAULT_OP_SIZE = 200;
  ObTableGroup()
  {
    ops_.set_attr(ObMemAttr(MTL_ID(), "KvTbGroup"));
    reset();
  }
  virtual ~ObTableGroup() = default;
  TO_STRING_KV(K_(is_inited),
               K_(group_meta),
               K_(timeout),
               K_(timeout_ts),
               K_(ops));
public:
  void reset()
  {
    is_inited_ = false;
    group_meta_.reset();
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reset();
  }
  void reuse()
  {
    is_inited_ = false;
    group_meta_.reset();
    timeout_ = 0;
    timeout_ts_ = INT64_MAX;
    ops_.reuse();
  }
  int init(const ObTableGroupMeta &meta, ObIArray<ObITableOp *> &ops);
  int init(const ObTableGroupMeta &meta, ObITableOp *op);
  int init(const ObTableGroupCtx &ctx, ObIArray<ObITableOp *> &ops);
  int add_op(ObITableOp *op);
  OB_INLINE ObIArray<ObITableOp *>& get_ops() { return ops_; }
public:
  bool is_inited_;
  ObTableGroupMeta group_meta_;
  int64_t timeout_;
  int64_t timeout_ts_;
  common::ObSEArray<ObITableOp *, DEFAULT_OP_SIZE> ops_;
};

struct ObTableGroupValue: public ObITableGroupValue
{
public:
  int64_t MAX_SLOT_SIZE = 10000;
  int8_t EXECUTABLE_BATCH_SIZE_FACTOR = 2;
public:
  ObTableGroupValue()
    : ObITableGroupValue(),
      is_inited_(false),
      allocator_("TbLsGroup", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      group_meta_(),
      queue_()
  {}

  ObTableGroupValue(ObTableGroupType op_type)
    : ObITableGroupValue(op_type),
      is_inited_(false),
      allocator_("TbLsGroup", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      group_meta_(),
      queue_()
  {}
  virtual ~ObTableGroupValue()
  {
    queue_.destroy();
    allocator_.reset();
  }
  TO_STRING_KV(K_(is_inited));
public:
  virtual int init(const ObTableGroupCtx &ctx);
  virtual int get_executable_group(int64_t batch_size, ObIArray<ObITableOp *> &ops, bool check_queue_size = true);
  virtual int add_op_to_group(ObITableOp *op);
  virtual bool has_executable_batch() { return queue_.get_curr_total() > 0;}
  virtual int64_t get_group_size() { return queue_.get_curr_total(); }
public:
  bool is_inited_;
  ObArenaAllocator allocator_;
  ObTableGroupMeta group_meta_;
  ObFixedQueue<ObITableOp> queue_;
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
      : is_inited_(false),
        failed_ops_(allocator),
        group_info_()
  {}
  virtual ~ObTableFailedGroups() {}
  TO_STRING_KV(K_(failed_ops), K_(group_info));
public:
  OB_INLINE bool empty() const { return failed_ops_.empty(); }
  OB_INLINE int64_t count() const { return failed_ops_.size(); }
  OB_INLINE const common::ObList<ObTableGroup*, common::ObIAllocator>& get_failed_groups() const { return failed_ops_; }
  OB_INLINE ObTableGroupInfo& get_group_info() { return group_info_; }
  int init();
  int add(ObTableGroup *group);
  ObTableGroup* get();
  int construct_trigger_requests(common::ObIAllocator &request_allocator,
                                 common::ObIArray<ObTableGroupTriggerRequest*> &requests);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  common::ObList<ObTableGroup*, common::ObIAllocator> failed_ops_;
  ObTableGroupInfo group_info_;
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

  OB_INLINE int add_expired_group(ObITableGroupValue *group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(expired_groups_.push_back(group))) {
      COMMON_LOG(WARN, "fail to push back ls group", K(ret));
    }
    return ret;
  }

  OB_INLINE int add_clean_group(ObITableGroupValue *group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(clean_groups_.push_back(group))) {
      COMMON_LOG(WARN, "fail to push back ls group", K(ret));
    }
    return ret;
  }

  OB_INLINE int pop_expired_group(ObITableGroupValue *&group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else {
      ret = expired_groups_.pop_front(group);
    }
    return ret;
  }

  OB_INLINE int pop_clean_group(ObITableGroupValue *&group)
  {
    int ret = OB_SUCCESS;
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "ObTableExpiredGroups not inited", K(ret));
    } else if (OB_FAIL(clean_groups_.pop_front(group))) {
      COMMON_LOG(WARN, "fail to pop front group", K(ret));
    }
    return ret;
  }

  OB_INLINE int64_t get_clean_group_counts() const { return clean_groups_.size(); }
  OB_INLINE const common::ObList<ObITableGroupValue *, common::ObIAllocator>& get_expired_groups() const { return expired_groups_; }
  OB_INLINE const common::ObList<ObITableGroupValue *, common::ObIAllocator>& get_clean_groups() const { return clean_groups_; }
  int construct_trigger_requests(common::ObIAllocator &request_allocator,
                                common::ObIArray<ObTableGroupTriggerRequest*> &requests);
private:
  bool is_inited_;
  common::ObSpinLock lock_;
  common::ObFIFOAllocator allocator_;
  common::ObList<ObITableGroupValue *, common::ObIAllocator> expired_groups_; // store the expired ls_group moved from group map
  common::ObList<ObITableGroupValue *, common::ObIAllocator> clean_groups_; // store the ls_group ready to clean, moved from expired_groups_
};

class ObTableGroupUtils final
{
public:
  static const int64_t DEFAULT_TIMEOUT_US = 10 * 1000 * 1000L; // 10s
  static int init_sess_guard(ObTableGroup &group,
                             ObTableApiSessGuard &sess_guard);
  static int init_schema_cache_guard(const ObTableGroup &group,
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
  static bool is_group_commit_enable(ObTableOperationType::Type op_type);
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
        write_ops_(0),
        other_ops_(0)
  {}
  ~ObTableGroupOpsCounter() = default;
  TO_STRING_KV(K_(read_ops),
               K_(write_ops),
               K_(other_ops));
public:
  OB_INLINE void inc(ObTableOperationType::Type type)
  {
    if (ObTableGroupUtils::is_write_op(type)) {
      ATOMIC_INC(&write_ops_);
    } else if (ObTableGroupUtils::is_read_op(type)) {
      ATOMIC_INC(&read_ops_);
    } else {
      ATOMIC_INC(&other_ops_);
    }
  }
  OB_INLINE int64_t get_ops() const { return ATOMIC_LOAD(&read_ops_) + ATOMIC_LOAD(&write_ops_) + ATOMIC_LOAD(&other_ops_); }
  OB_INLINE int64_t get_read_ops() const { return ATOMIC_LOAD(&read_ops_); }
  OB_INLINE int64_t get_write_ops() const { return ATOMIC_LOAD(&write_ops_); }
  OB_INLINE int64_t get_other_ops() const { return ATOMIC_LOAD(&other_ops_); }
  OB_INLINE void reset_ops()
  {
    ATOMIC_STORE(&read_ops_, 0);
    ATOMIC_STORE(&write_ops_, 0);
    ATOMIC_STORE(&other_ops_, 0);
  }
private:
  int64_t read_ops_;
  int64_t write_ops_;
  int64_t other_ops_; // Todo: inc by enum ObTableOp
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_GROUP_COMMON_H_ */
