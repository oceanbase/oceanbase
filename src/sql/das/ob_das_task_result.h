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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_TASK_RESULT_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_TASK_RESULT_H_
#include "share/ob_define.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
namespace oceanbase
{
namespace sql
{
struct ObDASTCBInterruptInfo;
class ObDASTaskResultMgr;
class ObDASMemProfileInfo;
struct ObDASTCBMemProfileKey;

typedef common::ObIntWarp DASTCBInfo;
typedef common::LinkHashNode<DASTCBInfo> DASTCBHashNode;
typedef common::LinkHashValue<DASTCBInfo> DASTCBInfoValue;

struct ObDASTCBInterruptInfo
{
public:
  ObDASTCBInterruptInfo() : check_node_sequence_id_(0),
                            detectable_id_(),
                            interrupt_id_(),
                            self_addr_() {};

  void operator=(const ObDASTCBInterruptInfo &t)
  {
    check_node_sequence_id_ = t.check_node_sequence_id_;
    detectable_id_ = t.detectable_id_;
    interrupt_id_ = t.interrupt_id_;
    self_addr_ = t.self_addr_;
  }

public:
  TO_STRING_KV(K_(check_node_sequence_id),
               K_(detectable_id),
               K_(interrupt_id),
               K_(self_addr));

  // the unique id of callback for unregister in detect manager
  uint64_t check_node_sequence_id_;
  // the key of callback in detect manager
  // key: detectable_id_ -> value: callbacks[check_node_sequence_id_]
  ObDetectableId detectable_id_;
  // interrupted tid at callback
  ObInterruptibleTaskID interrupt_id_;
  common::ObAddr self_addr_;
};

class ObDASTCB : public DASTCBInfoValue
{
public:
  ObDASTCB();
  ~ObDASTCB() {}

  int init(int64_t task_id, const ObDASScanRtDef *scan_rtdef,
           const ObDASScanCtDef *scan_ctdef, const ExprFixedArray *output_exprs,
           ObDASTCBMemProfileKey &mem_profile_key, ObDASMemProfileInfo *mem_profile_info,
           const ObDASTCBInterruptInfo &interrupt_info);
  void destory(ObDASMemProfileInfo *mem_profile_info);

public:
  TO_STRING_KV(K_(is_inited),
               K_(task_id),
               K_(table_id),
               K_(expire_ts),
               K_(packet_cnt),
               K_(is_reading),
               K_(is_exiting),
               K_(io_read_bytes),
               K_(ssstore_read_bytes),
               K_(ssstore_read_row_cnt),
               K_(memstore_read_row_cnt),
               K_(mem_profile_key),
               K_(interrupt_info));

  int register_reading();
  int unregister_reading();
  int register_exiting(bool &is_already_exiting);

public:
  bool is_inited_;
  int64_t task_id_;
  int64_t table_id_;
  int64_t expire_ts_; //结果过期时间戳
  int packet_cnt_; //已经读出的packet大小，只有当存储中间结果时有效
  bool is_reading_; //正在读取中间结果，TCB处于ping状态，不能退出
  bool is_exiting_; //is_exiting_ = true表示该TCB正在退出，持有的结果已经失效，不能再访问
  bool is_vectorized_;
  bool enable_rich_format_;
  int64_t read_rows_;
  int64_t max_batch_size_;
  const ObChunkDatumStore::StoredRow *stored_row_;
  const ObChunkDatumStore::StoredRow **stored_row_arr_;
  ObChunkDatumStore *datum_store_;
  ObChunkDatumStore::Iterator result_iter_;
  const ObCompactRow *vec_stored_row_;
  const ObCompactRow **vec_stored_row_arr_;
  ObTempRowStore *vec_row_store_;
  ObTempRowStore::Iterator vec_result_iter_;

  int64_t io_read_bytes_;
  int64_t ssstore_read_bytes_;
  int64_t ssstore_read_row_cnt_;
  int64_t memstore_read_row_cnt_;
  ObDASTCBMemProfileKey mem_profile_key_;

  // for detect & interrupt
  ObDASTCBInterruptInfo interrupt_info_;
private:
  common::ObSpinLock tcb_lock_; //用于控制资源资源释放的时序，保证并发访问的安全
};

class ObDASTCBAlloc
{
public:
  ObDASTCB* alloc_value() { return NULL; }
  void free_value(ObDASTCB *p)
  {
    if (p != nullptr) {
      op_free(p);
      p = nullptr;
    }
  }
  DASTCBHashNode* alloc_node(ObDASTCB *p)
  {
    UNUSED(p);
    return op_alloc(DASTCBHashNode);
  }
  void free_node(DASTCBHashNode *node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

class ObDASMemProfileInfo : public ObSqlMemoryCallback
{
public:
  ObDASMemProfileInfo(const uint64_t tenant_id)
    : ref_count_(0), row_count_(0),
      allocator_(tenant_id),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_),
      mutex_(common::ObLatchIds::SQL_MEMORY_MGR_MUTEX_LOCK)
  { }
  ~ObDASMemProfileInfo() {}

  void alloc(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.alloc(size);
  }

  void free(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.free(size);
  }

  void dumped(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.dumped(size);
  }

  void set_number_pass(int32_t num_pass)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.set_number_pass(num_pass);
  }

  void update_row_count(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    row_count_ += size;
  }

  TO_STRING_KV(K(ref_count_), K(row_count_), K(profile_));

public:
  static const int64_t CACHE_SIZE = 16 * 1024 * 1024;

  int64_t ref_count_;
  int64_t row_count_;

  common::ObFIFOAllocator allocator_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  lib::ObMutex mutex_;
};

class ObDASTaskResultGC
{
  friend class ObDASTaskResultMgr;
public:
  ObDASTaskResultGC() : cur_time_(0), task_result_mgr_(NULL) {}
  ~ObDASTaskResultGC() = default;
  void set_current_time() { cur_time_ = oceanbase::common::ObTimeUtility::current_time(); }
  bool operator() (const DASTCBInfo &tcb_info, ObDASTCB *tcb);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTaskResultGC);
private:
  int64_t cur_time_;
  ObDASTaskResultMgr *task_result_mgr_;
};

class ObDASTaskResultGCRunner : public common::ObTimerTask
{
  friend class ObDASTaskResultMgr;
public:
  ObDASTaskResultGCRunner() {}
  ~ObDASTaskResultGCRunner() = default;
  static ObDASTaskResultGCRunner& get_instance();
  static int schedule_timer_task();
  void runTimerTask();
public:
  const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L; // 10s
private:
  DISALLOW_COPY_AND_ASSIGN(ObDASTaskResultGCRunner);
};

class ObDASTaskResultCheckUpdateMem
{
public:
  explicit ObDASTaskResultCheckUpdateMem(int64_t cur_row_count) : cur_row_count_(cur_row_count) {}
  ~ObDASTaskResultCheckUpdateMem() = default;
  bool operator()(int64_t max_row_count) {
    return cur_row_count_ > max_row_count;
  }
private:
  int64_t cur_row_count_;
};

class ObDASTaskResultCheckDump
{
public:
  explicit ObDASTaskResultCheckDump(int64_t cur_mem_size) : cur_mem_size_(cur_mem_size) {}
  ~ObDASTaskResultCheckDump() = default;
  bool operator()(int64_t max_memory_size) {
    return cur_mem_size_ > max_memory_size;
  }
private:
  int64_t cur_mem_size_;
};

class ObAtomicGetMemProfileCall
{
public:
  explicit ObAtomicGetMemProfileCall() : ret_(OB_SUCCESS), mem_profile_info_(nullptr) {}
  ~ObAtomicGetMemProfileCall() = default;
  void operator() (common::hash::HashMapPair<ObDASTCBMemProfileKey, ObDASMemProfileInfo *> &entry);
public:
  int ret_;
  ObDASMemProfileInfo *mem_profile_info_;
};

class ObDASMemProfileErase
{
public:
  ObDASMemProfileErase() = default;
  ~ObDASMemProfileErase() = default;
  bool operator() (common::hash::HashMapPair<ObDASTCBMemProfileKey, ObDASMemProfileInfo *> &entry);
};

class ObDASTaskResultMgr
{
public:
  ObDASTaskResultMgr();
  ~ObDASTaskResultMgr();
  int init();
  void destory();
  int save_task_result(int64_t task_id,
                       const ObDASTCBInterruptInfo &interrupt_info,
                       const ExprFixedArray *output_exprs,
                       ObEvalCtx *eval_ctx,
                       common::ObNewRowIterator &result,
                       int64_t read_rows,
                       const ObDASScanCtDef *scan_ctdef,
                       ObDASScanRtDef *scan_rtdef,
                       ObDASScanOp &scan_op);
  int erase_task_result(int64_t task_id, bool need_unreg_dm);
  //从中间结果管理器中获取一个block大小的结果，默认为2M大小
  int iterator_task_result(ObDASDataFetchRes &res,
                           int64_t &io_read_bytes,
                           int64_t &ssstore_read_bytes,
                           int64_t &ssstore_read_row_cnt,
                           int64_t &memstore_read_row_cnt);
  int remove_expired_results();

private:
  int save_task_result_by_normal(int64_t &read_rows,
                                const ExprFixedArray *output_exprs,
                                ObEvalCtx *eval_ctx,
                                ObDASTCB *tcb,
                                common::ObNewRowIterator &result,
                                const ObDASScanRtDef *scan_rtdef,
                                ObDASMemProfileInfo *mem_profile_info);
  int save_task_result_by_vector(int64_t &read_rows,
                                const ExprFixedArray *output_exprs,
                                ObEvalCtx *eval_ctx,
                                ObDASTCB *tcb,
                                common::ObNewRowIterator &result,
                                const ObDASScanRtDef *scan_rtdef,
                                ObDASMemProfileInfo *mem_profile_info);
  int fetch_result_by_normal(ObDASTCB *tcb, ObDASDataFetchRes &res, bool &has_more);
  int fetch_result_by_vector(ObDASTCB *tcb, ObDASDataFetchRes &res, bool &has_more);

private:
  int check_mem_profile_key(ObDASTCBMemProfileKey &key);
  bool need_dump(ObDASMemProfileInfo *info) {
    return info->sql_mem_processor_.get_data_size() > info->sql_mem_processor_.get_mem_bound();
  }
  int process_dump(ObDASTCB *tcb, ObDASMemProfileInfo *info);
  int get_mem_profile(ObDASTCBMemProfileKey &key, ObDASMemProfileInfo *&info, ObExecContext *exec_ctx, const ObDASScanRtDef *scan_rtdef);
  int init_mem_profile(ObDASTCBMemProfileKey &key, ObDASMemProfileInfo *&info, ObExecContext *exec_ctx, const ObDASScanRtDef *scan_rtdef);
  int destroy_mem_profile(const ObDASTCBMemProfileKey& key);
  void inc_mem_profile_ref_count(ObDASMemProfileInfo *info);
  int dec_mem_profile_ref_count(const ObDASTCBMemProfileKey &key,
                                ObDASMemProfileInfo *&info);
  void free_mem_profile(ObDASMemProfileInfo *&info);

private:
  int check_interrupt();

private:
  static const int64_t DAS_TCB_MAP_SHRINK_THRESHOLD = 128;
  static const int64_t DAS_TCB_CHECK_INTERRUPT_INTERVAL = 32; // check interrupt per 32 loop
  typedef common::hash::ObHashMap<ObDASTCBMemProfileKey,
                                  ObDASMemProfileInfo*> MemProfileMap;
  typedef common::ObLinkHashMap<DASTCBInfo,
                                ObDASTCB,
                                ObDASTCBAlloc,
                                common::RefHandle,
                                DAS_TCB_MAP_SHRINK_THRESHOLD> DASTCBMap;

private:
  lib::ObMutex mem_map_mutex_;
  MemProfileMap mem_profile_map_;
  DASTCBMap tcb_map_;
  ObDASTaskResultGC gc_;
};
} // namespace sql
} // namespace oceanbase
#endif // OBDEV_SRC_SQL_DAS_OB_DAS_TASK_RESULT_H_
