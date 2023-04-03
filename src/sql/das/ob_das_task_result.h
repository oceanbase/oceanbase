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
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
namespace oceanbase
{
namespace sql
{
class ObDASTaskResultMgr;
typedef common::ObIntWarp DASTCBInfo;
typedef common::LinkHashNode<DASTCBInfo> DASTCBHashNode;
typedef common::LinkHashValue<DASTCBInfo> DASTCBInfoValue;

class ObDASTCB : public DASTCBInfoValue
{
public:
  ObDASTCB();
  ~ObDASTCB() {}

public:
  TO_STRING_KV(K_(task_id),
               K_(expire_ts),
               K_(packet_cnt),
               K_(is_reading),
               K_(is_exiting));

  int register_reading();
  int unregister_reading();
  int register_exiting(bool &is_already_exiting);

public:
  int64_t task_id_;
  int64_t expire_ts_; //结果过期时间戳
  int packet_cnt_; //已经读出的packet大小，只有当存储中间结果时有效
  bool is_reading_; //正在读取中间结果，TCB处于ping状态，不能退出
  bool is_exiting_; //is_exiting_ = true表示该TCB正在退出，持有的结果已经失效，不能再访问
  bool is_vectorized_;
  int64_t read_rows_;
  int64_t max_batch_size_;
  const ObChunkDatumStore::StoredRow *stored_row_;
  const ObChunkDatumStore::StoredRow **stored_row_arr_;
  ObChunkDatumStore datum_store_;
  ObChunkDatumStore::Iterator result_iter_;
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

static const int64_t DAS_TCB_MAP_SHRINK_THRESHOLD = 128;
typedef common::ObLinkHashMap<DASTCBInfo,
                              ObDASTCB,
                              ObDASTCBAlloc,
                              common::RefHandle,
                              DAS_TCB_MAP_SHRINK_THRESHOLD> DASTCBMap;

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

class ObDASTaskResultMgr
{
public:
  ObDASTaskResultMgr();
  ~ObDASTaskResultMgr();
  int init();
  int save_task_result(int64_t task_id,
                       const ExprFixedArray *output_exprs,
                       ObEvalCtx *eval_ctx,
                       common::ObNewRowIterator &result,
                       int64_t read_rows,
                       const ObDASScanCtDef *scan_ctdef,
                       const ObDASScanRtDef * scan_rtdef,
                       ObDASScanOp &scan_op);
  int erase_task_result(int64_t task_id);
  //从中间结果管理器中获取一个block大小的结果，默认为2M大小
  int iterator_task_result(int64_t task_id,
                           ObChunkDatumStore &datum_store,
                           bool &has_more);
  int remove_expired_results();
private:
  DASTCBMap tcb_map_;
  ObDASTaskResultGC gc_;
};
} // namespace sql
} // namespace oceanbase
#endif // OBDEV_SRC_SQL_DAS_OB_DAS_TASK_RESULT_H_