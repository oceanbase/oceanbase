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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_TENANT_GROUP_H_
#define OCEANBASE_OBSERVER_OB_TABLE_TENANT_GROUP_H_

#include "ob_table_group_execute.h"

namespace oceanbase
{

namespace table
{

#define TABLEAPI_GROUP_COMMIT_MGR (MTL(ObTableGroupCommitMgr*))
class ObTableGroupCommitMgr final
{
public:
  typedef hash::ObHashMap<uint64_t, ObTableGroupCommitOps*> ObTableGroupCommitMap; // key is hash of ObTableGroupCommitKey
  static const int64_t DEFAULT_GROUP_SIZE = 32;
  static const int64_t DEFAULT_QUEUE_TIME_WINDOW_SIZE = 100;
  ObTableGroupCommitMgr()
      : is_inited_(false),
        is_group_commit_disable_(true),
        allocator_("TbGroupComMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        group_allocator_("TbGroupAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        op_allocator_("TbOpAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        failed_groups_allocator_("TbFgroupAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        failed_groups_(failed_groups_allocator_),
        queue_time_(0),
        queue_times_(DEFAULT_QUEUE_TIME_WINDOW_SIZE),
        statis_and_trigger_task_(*this),
        group_size_and_ops_task_(*this),
        group_factory_(group_allocator_),
        op_factory_(op_allocator_),
        put_op_group_size_(0),
        get_op_group_size_(0),
        last_queue_time_(0),
        last_put_ops_(0),
        last_get_ops_(0)
  {}
  virtual ~ObTableGroupCommitMgr() {}
  TO_STRING_KV(K_(is_inited),
               K_(failed_groups),
               K_(queue_time),
               K_(group_factory),
               K_(op_factory),
               K_(put_op_group_size),
               K_(get_op_group_size),
               K_(last_queue_time),
               K_(last_put_ops),
               K_(last_get_ops));
public:
  static int mtl_init(ObTableGroupCommitMgr *&mgr) { return mgr->init(); }
  int start();
  void stop();
  void wait();
  void destroy();
  int init();
  int start_timer();
  OB_INLINE void set_group_commit_disable(bool disable) { ATOMIC_STORE(&is_group_commit_disable_, disable); }
  OB_INLINE bool is_group_commit_disable() const { return ATOMIC_LOAD(&is_group_commit_disable_); }
  OB_INLINE ObTableGroupOpsCounter& get_ops_counter() { return ops_counter_; }
  OB_INLINE ObTableGroupCommitMap& get_groups() { return groups_; }
  OB_INLINE void set_queue_time(int64_t time) { ATOMIC_STORE(&queue_time_, time); }
  OB_INLINE void set_last_queue_time(int64_t time) { last_queue_time_ = time; }
  OB_INLINE int64_t get_last_queue_time() const { return last_queue_time_; }
  OB_INLINE void set_last_put_ops(int64_t ops) { last_put_ops_ = ops; }
  OB_INLINE int64_t get_last_put_ops() const { return last_put_ops_; }
  OB_INLINE void set_last_get_ops(int64_t ops) { last_get_ops_ = ops; }
  OB_INLINE int64_t get_last_get_ops() const { return last_get_ops_; }
  OB_INLINE int64_t get_queue_time() const { return ATOMIC_LOAD(&queue_time_); }
  OB_INLINE ObTableMovingAverage<int64_t>& get_queue_times() { return queue_times_; }
  OB_INLINE ObTableGroupCommitSingleOp* alloc_op() { return op_factory_.alloc(); }
  OB_INLINE void free_op(ObTableGroupCommitSingleOp *op) { op_factory_.free(op); }
  OB_INLINE ObTableGroupCommitOps* alloc_group() { return group_factory_.alloc(); }
  OB_INLINE void free_group(ObTableGroupCommitOps *group) { group_factory_.free(group); }
  OB_INLINE bool has_failed_groups() const { return !failed_groups_.empty(); }
  OB_INLINE ObTableFailedGroups& get_failed_groups() { return failed_groups_; }
  OB_INLINE ObTableGroupFactory<ObTableGroupCommitOps>& get_group_factory() { return group_factory_; }
  OB_INLINE ObTableGroupFactory<ObTableGroupCommitSingleOp>& get_op_factory() { return op_factory_; }
  OB_INLINE int64_t get_put_op_group_size() const { return put_op_group_size_.value(); }
  OB_INLINE void set_put_op_group_size(int64_t size) { put_op_group_size_.set(size); }
  OB_INLINE int64_t get_get_op_group_size() const { return get_op_group_size_.value(); }
  OB_INLINE void set_get_op_group_size(int64_t size) { get_op_group_size_.set(size); }
  OB_INLINE int64_t get_group_size(bool is_get) const
  {
    if (is_get) {
      return get_op_group_size_.value();
    } else {
      return put_op_group_size_.value();
    }
  }
  int create_and_add_group(const ObTableGroupCtx &ctx);
public:
	class ObTableGroupStatisAndTriggerTask : public common::ObTimerTask
  {
  public:
    static const int64_t TASK_SCHEDULE_INTERVAL = 10 * 1000 ; // 10ms
    ObTableGroupStatisAndTriggerTask(ObTableGroupCommitMgr &mgr)
        : group_mgr_(mgr)
    {}
    virtual void runTimerTask(void) override;
  private:
    int run_trigger_task();
    int trigger_other_group();
    int trigger_failed_group();
    void run_statistics_task();
  private:
    ObTableGroupCommitMgr &group_mgr_;
  };
  class ObTableGroupSizeAndOpsTask : public common::ObTimerTask
  {
  public:
    static const int64_t TASK_SCHEDULE_INTERVAL = 1000 * 1000; // 1s
    static const int64_t DEFAULT_GROUP_CHANGE_SIZE = 2;
    static const int64_t DEFAULT_MAX_GROUP_SIZE = 50;
    static const int64_t DEFAULT_MIN_OPS = 2000;
    static const int64_t DEFAULT_MIN_GROUP_SIZE = 1;
    static const int64_t DEFAULT_ENABLE_GROUP_COMMIT_QUEUE_TIME = 1 * 1000; // 1ms
    static constexpr double DEFAULT_FLUCTUATION = 0.1;
    ObTableGroupSizeAndOpsTask(ObTableGroupCommitMgr &mgr)
        : group_mgr_(mgr)
    {}
    virtual void runTimerTask(void) override;
  private:
    int64_t get_new_group_size(int64_t cur, double persent);
    ObTableGroupCommitMgr &group_mgr_;
  };
private:
  bool is_inited_;
  bool is_group_commit_disable_;
  common::ObArenaAllocator allocator_;
  common::ObArenaAllocator group_allocator_;
  common::ObArenaAllocator op_allocator_;
  common::ObArenaAllocator failed_groups_allocator_;
  common::ObTimer timer_;
  ObTableGroupCommitMap groups_;
  ObTableFailedGroups failed_groups_;
  ObTableGroupOpsCounter ops_counter_;
  int64_t queue_time_;
  ObTableMovingAverage<int64_t> queue_times_;
  ObTableGroupStatisAndTriggerTask statis_and_trigger_task_;
  ObTableGroupSizeAndOpsTask group_size_and_ops_task_;
  ObTableGroupFactory<ObTableGroupCommitOps> group_factory_;
  ObTableGroupFactory<ObTableGroupCommitSingleOp> op_factory_;
  ObTableAtomicValue<int64_t> put_op_group_size_;
  ObTableAtomicValue<int64_t> get_op_group_size_;
  int64_t last_queue_time_;
  int64_t last_put_ops_;
  int64_t last_get_ops_;
};

class ObTableGroupFeeder final
{
public:
  typedef common::hash::HashMapPair<uint64_t, ObTableGroupCommitOps*> MapKV;
  ObTableGroupFeeder(ObTableGroupCommitMgr &mgr, ObTableGroupCommitSingleOp *op)
      : need_execute_(false),
        group_mgr_(mgr),
        op_(op),
        group_(nullptr)
  {}
  virtual ~ObTableGroupFeeder() {}
public:
  int operator()(MapKV &entry);
  OB_INLINE ObTableGroupCommitOps *get_group() { return group_; }
  OB_INLINE bool need_execute() const { return need_execute_; }
private:
  bool need_execute_;
  ObTableGroupCommitMgr &group_mgr_;
  ObTableGroupCommitSingleOp *op_;
  ObTableGroupCommitOps *group_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupFeeder);
};

class ObTableExecuteGroupsGetter final
{
public:
  ObTableExecuteGroupsGetter() {}
  virtual ~ObTableExecuteGroupsGetter() = default;
  int operator()(common::hash::HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv);
public:
  ObSEArray<uint64_t, 8> can_execute_keys_;
};

class ObTableTriggerGroupsGetter final
{
public:
  ObTableTriggerGroupsGetter()
      : allocator_("TbGroupTri", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {
    trigger_requests_.set_attr(ObMemAttr(MTL_ID(), "GpTriggerGtr"));
  }
  virtual ~ObTableTriggerGroupsGetter() = default;
  int operator()(common::hash::HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv);
public:
  common::ObArenaAllocator allocator_;
  ObSEArray<ObTableGroupTriggerRequest*, 8> trigger_requests_;
};

class ObTableTimeoutGroupCounter final
{
public:
  ObTableTimeoutGroupCounter()
      : count_(0)
  {}
  virtual ~ObTableTimeoutGroupCounter() = default;
  int operator()(common::hash::HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv);
public:
  int64_t count_;
};

class ObTableGroupForeacher final
{
public:
  ObTableGroupForeacher()
  {
    groups_.set_attr(ObMemAttr(MTL_ID(), "GpForeacher"));
  }
  virtual ~ObTableGroupForeacher() = default;
  int operator()(common::hash::HashMapPair<uint64_t, ObTableGroupCommitOps*> &kv);
public:
  ObSEArray<ObTableGroupCommitOps*, 64> groups_;
};

} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_TENANT_GROUP_H_ */
