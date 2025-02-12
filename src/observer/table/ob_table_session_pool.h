/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_SESSION_POOL_H_
#define OCEANBASE_OBSERVER_OB_TABLE_SESSION_POOL_H_
#include "lib/hash/ob_hashmap.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/table/ob_table.h" // for ObTableApiCredential
#include "ob_table_mode_control.h"

namespace oceanbase
{
namespace table
{
class ObTableApiSessPool;
class ObTableApiSessNode;
class ObTableApiSessGuard;
class ObTableApiSessNodeVal;
class ObTableApiSessNodeAtomicOp;

struct ObTableRelatedSysVars
{
public:
  struct StaticSysVars
  {
  public:
    StaticSysVars()
        : tenant_kv_mode_(ObKvModeType::ALL)
    {}
    virtual ~StaticSysVars() {}
  public:
    OB_INLINE ObKvModeType get_kv_mode() const
    {
      return tenant_kv_mode_;
    }
    OB_INLINE void set_kv_mode(ObKvModeType val)
    {
      tenant_kv_mode_ = val;
    }
  private:
    ObKvModeType tenant_kv_mode_;
  };

  struct DynamicSysVars
  {
  public:
    DynamicSysVars()
        : binlog_row_image_(-1),
          kv_group_commit_batch_size_(10),
          group_rw_mode_(0),
          query_record_size_limit_(-1),
          enable_query_response_time_stats_(true)
    {}
    virtual ~DynamicSysVars() {}
  public:
    OB_INLINE int64_t get_binlog_row_image() const
    {
      return ATOMIC_LOAD(&binlog_row_image_);
    }
    OB_INLINE void set_binlog_row_image(int64_t val)
    {
      ATOMIC_STORE(&binlog_row_image_, val);
    }
    OB_INLINE int64_t get_kv_group_commit_batch_size() const
    {
      return ATOMIC_LOAD(&kv_group_commit_batch_size_);
    }
    OB_INLINE void set_kv_group_commit_batch_size(int64_t val)
    {
      ATOMIC_STORE(&kv_group_commit_batch_size_, val);
    }
    OB_INLINE ObTableGroupRwMode get_group_rw_mode() const
    {
      return static_cast<ObTableGroupRwMode>(ATOMIC_LOAD(&group_rw_mode_));
    }
    OB_INLINE void set_group_rw_mode(ObTableGroupRwMode val)
    {
      ATOMIC_STORE(&group_rw_mode_, static_cast<int64_t>(val));
    }
    OB_INLINE int64_t get_query_record_size_limit() const
    {
      return ATOMIC_LOAD(&query_record_size_limit_);
    }
    OB_INLINE void set_query_record_size_limit(int64_t val)
    {
      ATOMIC_STORE(&query_record_size_limit_, val);
    }
    OB_INLINE bool is_enable_query_response_time_stats() const
    {
      return ATOMIC_LOAD(&enable_query_response_time_stats_);
    }
    OB_INLINE void set_enable_query_response_time_stats(bool val)
    {
      ATOMIC_STORE(&enable_query_response_time_stats_, val);
    }
  private:
    int64_t binlog_row_image_;
    int64_t kv_group_commit_batch_size_;
    int64_t group_rw_mode_;
    int64_t query_record_size_limit_;
    bool enable_query_response_time_stats_;
  };
public:
  ObTableRelatedSysVars()
      : is_inited_(false)
  {}
  virtual ~ObTableRelatedSysVars() {}
  int update_sys_vars(bool only_update_dynamic_vars);
  int init();
public:
  bool is_inited_;
  ObSpinLock lock_; // Avoid repeated initialization
  StaticSysVars static_vars_;
  DynamicSysVars dynamic_vars_;
};

class ObTableApiSessPoolMgr final
{
public:
  ObTableApiSessPoolMgr()
      : is_inited_(false),
        allocator_("TbSessPoolMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        pool_(nullptr)
  {}
  virtual ~ObTableApiSessPoolMgr() { destroy(); }
  TO_STRING_KV(K_(is_inited),
               KPC_(pool));
public:
  class ObTableApiSessEliminationTask : public common::ObTimerTask
  {
  public:
    ObTableApiSessEliminationTask()
        : is_inited_(false),
          sess_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(sess_pool_mgr));
    void runTimerTask(void);
  private:
    // 回收已经淘汰的session
    int run_recycle_retired_sess_task();
    // 淘汰长期未被使用的session
    int run_retire_sess_task();
  public:
    bool is_inited_;
    ObTableApiSessPoolMgr *sess_pool_mgr_;
  };
  class ObTableApiSessSysVarUpdateTask : public common::ObTimerTask
  {
  public:
    ObTableApiSessSysVarUpdateTask()
        : is_inited_(false),
          sess_pool_mgr_(nullptr)
    {
    }
    TO_STRING_KV(K_(is_inited), KPC_(sess_pool_mgr));
    void runTimerTask(void);
  private:
    int run_update_sys_var_task();
  public:
    bool is_inited_;
    ObTableApiSessPoolMgr *sess_pool_mgr_;
  };
public:
  static int mtl_init(ObTableApiSessPoolMgr *&mgr);
  int start();
  void stop();
  void wait();
  void destroy();
  int init();
  int get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard);
  int update_sess(ObTableApiCredential &credential);
  int update_sys_vars(bool only_update_dynamic_vars)
  {
    return sys_vars_.update_sys_vars(only_update_dynamic_vars);
  }
public:
  OB_INLINE int64_t get_binlog_row_image() const
  {
    return sys_vars_.dynamic_vars_.get_binlog_row_image();
  }
  OB_INLINE ObKvModeType get_kv_mode() const
  {
    return sys_vars_.static_vars_.get_kv_mode();
  }
  OB_INLINE int64_t get_kv_group_commit_batch_size() const
  {
    return sys_vars_.dynamic_vars_.get_kv_group_commit_batch_size();
  }
  OB_INLINE ObTableGroupRwMode get_group_rw_mode() const
  {
    return sys_vars_.dynamic_vars_.get_group_rw_mode();
  }
  OB_INLINE int64_t get_query_record_size_limit() const
  {
    return sys_vars_.dynamic_vars_.get_query_record_size_limit();
  }
  OB_INLINE bool is_enable_query_response_time_stats() const
  {
    return sys_vars_.dynamic_vars_.is_enable_query_response_time_stats();
  }
private:
  int create_session_pool_safe();
  int create_session_pool_unsafe();
private:
  static const int64_t ELIMINATE_SESSION_DELAY = 5 * 1000 * 1000; // 5s
  static const int64_t SYS_VAR_REFRESH_DELAY = 5 * 1000 * 1000; // 5s
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObTableApiSessPool *pool_;
  ObTableApiSessEliminationTask elimination_task_;
  ObTableApiSessSysVarUpdateTask sys_var_update_task_;
  ObTableRelatedSysVars sys_vars_;
  ObSpinLock lock_; // for double check pool creating
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessPoolMgr);
};

#define TABLEAPI_SESS_POOL_MGR (MTL(ObTableApiSessPoolMgr*))

class ObTableApiSessPool final
{
public:
  // key is ObTableApiCredential.hash_val_
  typedef common::hash::ObHashMap<uint64_t, ObTableApiSessNode*> CacheKeyNodeMap;
  static const int64_t SESS_POOL_DEFAULT_BUCKET_NUM = 10; // default user number
  static const int64_t SESS_RETIRE_TIME = 300 * 1000 * 1000; // marked as retired more than 300 seconds are not accessed
  static const int64_t BACKCROUND_TASK_DELETE_SESS_NUM = 2000; // number of eliminated session nodes in background task per time
  static const int64_t SESS_UPDATE_TIME_INTERVAL = 5 * 1000 * 1000; // the update interval cannot exceed 5 seconds
public:
  explicit ObTableApiSessPool()
      : allocator_(MTL_ID()),
        is_inited_(false),
        last_update_ts_(0)
  {}
  ~ObTableApiSessPool() { destroy(); };
  TO_STRING_KV(K_(is_inited),
               K_(retired_nodes));
  int init(int64_t hash_bucket = SESS_POOL_DEFAULT_BUCKET_NUM);
  void destroy();
  int get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard);
  int update_sess(ObTableApiCredential &credential);
  int retire_session_node();
  int evict_retired_sess();
  int create_node_safe(ObTableApiCredential &credential, ObTableApiSessNode *&node);
  int move_node_to_retired_list(ObTableApiSessNode *node);
private:
  int replace_sess_node_safe(ObTableApiCredential &credential);
  int create_and_add_node_safe(ObTableApiCredential &credential);
  int get_sess_node(uint64_t key, ObTableApiSessNode *&node);
private:
  common::ObFIFOAllocator allocator_;
  bool is_inited_;
  CacheKeyNodeMap key_node_map_;
  // 已经淘汰的node，等待被后台删除
  // 前台login时、后台淘汰时都会操作retired_nodes_，因此需要加锁
  common::ObDList<ObTableApiSessNode> retired_nodes_;
  ObSpinLock retired_nodes_lock_; // for lock retired_nodes_
  int64_t last_update_ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessPool);
};

class ObTableApiSessNodeVal : public common::ObDLinkBase<ObTableApiSessNodeVal>
{
friend class ObTableApiSessNode;
friend class ObTableApiSessGuard;
public:
  explicit ObTableApiSessNodeVal(ObTableApiSessNode *owner, uint64_t tenant_id)
      : is_inited_(false),
        tenant_id_(tenant_id),
        sess_info_(tenant_id_), // sess_info_ use 500 tenant default, so we must set tenant_id
        owner_node_(owner)
  {}
  TO_STRING_KV(K_(is_inited),
               K_(sess_info));
public:
  void destroy();
  sql::ObSQLSessionInfo& get_sess_info() { return sess_info_; }
  int init_sess_info();
  void reset_tx_desc() { // 防止异步提交场景在 session 析构的时候 rollback 事务
    sql::ObSQLSessionInfo::LockGuard guard(sess_info_.get_thread_data_lock());
    sess_info_.get_tx_desc() = nullptr;
  }
  int push_back_to_queue();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  sql::ObSQLSessionInfo sess_info_;
  ObTableApiSessNode *owner_node_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessNodeVal);
};

class ObTableApiSessNode : public common::ObDLinkBase<ObTableApiSessNode>
{
friend class ObTableApiSessPool;
friend class ObTableApiSessNodeVal;
public:
  explicit ObTableApiSessNode(ObTableApiCredential &credential)
      : is_inited_(false),
        mem_ctx_(nullptr),
        queue_allocator_("TbSessQue", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        last_active_ts_(0),
        credential_(credential),
        sess_ref_cnt_(0)
  {
  }
  ~ObTableApiSessNode() { destroy(); }
  TO_STRING_KV(K_(is_inited),
               K_(last_active_ts),
               K_(credential),
               K_(tenant_name),
               K_(db_name),
               K_(user_name));
public:
  int init();
  void destroy();
  bool is_empty() const { return ATOMIC_LOAD(&sess_ref_cnt_) == 0 && sess_queue_.get_total() == 0; }
  int get_sess_node_val(ObTableApiSessGuard &guard);
  OB_INLINE int push_back_sess_to_queue(ObTableApiSessNodeVal *val)
  {
    int ret = sess_queue_.push(val);
    if (OB_SUCC(ret)) {
      ATOMIC_DEC(&sess_ref_cnt_);
    }
    return ret;
  }
  OB_INLINE const ObTableApiCredential& get_credential() const { return credential_; }
  OB_INLINE int64_t get_last_active_ts() const { return last_active_ts_; }
  int remove_unused_sess();
  OB_INLINE const ObString& get_tenant_name() const { return tenant_name_; }
  OB_INLINE const ObString& get_database_name() const { return db_name_; }
  OB_INLINE const ObString& get_user_name() const { return user_name_; }
  OB_INLINE void free_sess_val(ObTableApiSessNodeVal *val)
  {
    if (OB_NOT_NULL(mem_ctx_) && OB_NOT_NULL(val)) {
      mem_ctx_->free(val);
    }
  }
  OB_INLINE void dec_ref_cnt() { ATOMIC_DEC(&sess_ref_cnt_); }
private:
  int extend_and_get_sess_val(ObTableApiSessGuard &guard);
private:
  bool is_inited_;
  lib::MemoryContext mem_ctx_;
  ObArenaAllocator queue_allocator_;
  int64_t last_active_ts_;
  ObTableApiCredential credential_;
  ObString tenant_name_;
  ObString db_name_;
  ObString user_name_;
  ObFixedQueue<ObTableApiSessNodeVal> sess_queue_;
  int64_t sess_ref_cnt_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessNode);
};

class ObTableApiSessGuard final
{
friend class ObTableApiSessPool;
friend class ObTableApiSessNode;
public:
  ObTableApiSessGuard()
      : sess_node_val_(nullptr)
  {}
  // 析构需要做的两件事：
  // 1. reset事务描述符，避免session析构时，回滚事务
  // 2. 将session归还到队列，归还失败直接释放（destroy()会将owner_node_设置为null,需要提前记录owner_node）
  ~ObTableApiSessGuard()
  {
    if (OB_NOT_NULL(sess_node_val_)) {
      int ret = OB_SUCCESS;
      sess_node_val_->get_sess_info().get_trans_result().reset();
      sess_node_val_->reset_tx_desc();
      if (OB_FAIL(sess_node_val_->push_back_to_queue())) {
        COMMON_LOG(WARN, "fail to push back session to queue", K(ret));
        ObTableApiSessNode *owner_node = sess_node_val_->owner_node_;
        sess_node_val_->destroy();
        if (OB_NOT_NULL(owner_node)) {
          owner_node->free_sess_val(sess_node_val_);
          owner_node->dec_ref_cnt();
        }
      }
      sess_node_val_ = nullptr;
    }
  }
  TO_STRING_KV(KPC_(sess_node_val));
public:
  ObTableApiSessNodeVal* get_sess_node_val() const { return sess_node_val_; }
  sql::ObSQLSessionInfo& get_sess_info() { return sess_node_val_->get_sess_info(); }
  const sql::ObSQLSessionInfo& get_sess_info() const { return sess_node_val_->get_sess_info(); }
  int get_credential(const ObTableApiCredential *&credential) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(sess_node_val_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_ISNULL(sess_node_val_->owner_node_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      credential = &sess_node_val_->owner_node_->get_credential();
    }
    return ret;
  }
  OB_INLINE const ObString get_tenant_name() const
  {
    ObString tenant_name = ObString::make_empty_string();
    if (OB_NOT_NULL(sess_node_val_) && OB_NOT_NULL(sess_node_val_->owner_node_)) {
      tenant_name = sess_node_val_->owner_node_->get_tenant_name();
    }
    return tenant_name;
  }
  OB_INLINE const ObString get_database_name() const
  {
    ObString database_name = ObString::make_empty_string();
    if (OB_NOT_NULL(sess_node_val_) && OB_NOT_NULL(sess_node_val_->owner_node_)) {
      database_name = sess_node_val_->owner_node_->get_database_name();
    }
    return database_name;
  }
  OB_INLINE const ObString get_user_name() const
  {
    ObString user_name = ObString::make_empty_string();
    if (OB_NOT_NULL(sess_node_val_) && OB_NOT_NULL(sess_node_val_->owner_node_)) {
      user_name = sess_node_val_->owner_node_->get_user_name();
    }
    return user_name;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessGuard);
  ObTableApiSessNodeVal *sess_node_val_;
};

class ObTableApiSessNodeAtomicOp
{
protected:
  typedef common::hash::HashMapPair<uint64_t, ObTableApiSessNode*> MapKV;
public:
  ObTableApiSessNodeAtomicOp()
      : sess_node_(nullptr)
  {}
  virtual ~ObTableApiSessNodeAtomicOp() {}
  virtual int get_value(ObTableApiSessNode *&node);
  void operator()(MapKV &entry)
  {
    if (nullptr != entry.second) {
      sess_node_ = entry.second;
    }
  }
protected:
  ObTableApiSessNode *sess_node_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessNodeAtomicOp);
};

class ObTableApiSessNodeReplaceOp
{
protected:
  typedef common::hash::HashMapPair<uint64_t, ObTableApiSessNode*> MapKV;
public:
  ObTableApiSessNodeReplaceOp(ObTableApiSessPool &pool, ObTableApiCredential &credential)
      : pool_(pool),
        credential_(credential)
  {}
  virtual ~ObTableApiSessNodeReplaceOp() {}
  int operator()(MapKV &entry);
private:
  ObTableApiSessPool &pool_;
  ObTableApiCredential &credential_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessNodeReplaceOp);
};

class ObTableApiSessForeachOp
{
public:
  struct ObTableApiSessKV
  {
    ObTableApiSessKV() : node_(nullptr) {}
    ObTableApiSessKV(uint64_t key, ObTableApiSessNode *node)
      : key_(key),
        node_(node) {}
    TO_STRING_KV(K(key_), KP(node_));
    uint64_t key_;
    ObTableApiSessNode *node_;
  };
public:
  typedef common::hash::HashMapPair<uint64_t, ObTableApiSessNode*> MapKV;
  typedef common::ObSEArray<ObTableApiSessKV , 16> SessKvArray;
  ObTableApiSessForeachOp()
  {}
  int operator()(MapKV &entry);
  const SessKvArray &get_key_value_array() const { return key_value_array_; }
  void reset() { key_value_array_.reset(); }
private:
  SessKvArray key_value_array_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessForeachOp);
};

class ObTableApiSessUtil final
{
public:
  static int init_sess_info(uint64_t tenant_id,
                            const common::ObString &tenant_name,
                            share::schema::ObSchemaGetterGuard &schema_guard,
                            sql::ObSQLSessionInfo &sess_info);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiSessUtil);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_SESSION_POOL_H_ */
