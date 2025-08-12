/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#pragma once

#include "share/detect/ob_detectable_id.h"
#include "share/detect/ob_detect_callback.h"
#include "share/detect/ob_detect_rpc_proxy.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashset.h"
#include "lib/list/ob_dlist.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase {
namespace common {

class ObDetectableIdGen
{
public:
  static ObDetectableIdGen &instance();
  uint64_t generate_callback_sequence_id() { return ATOMIC_AAF(&callback_sequence_id_, 1); }
  int generate_detectable_id(ObDetectableId &detectable_id, uint64_t tenant_id);
private:
  uint64_t generate_detect_sequence_id() { return ATOMIC_AAF(&detect_sequence_id_, 1); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectableIdGen);
  ObDetectableIdGen();
  volatile uint64_t detect_sequence_id_; // for DetectableId
  volatile uint64_t callback_sequence_id_; // mark specific callback in linked list(value of CHECK_MAP)
};

// Detect Manager multi Double link list
// ObDMMultiDlist is used to record all the detect callback's detectable id in a fifo manner,
// the node of list is inline in detect callback.
// when add or remove a list node, the lock is applied to guarantee the atomicity.
class ObDMMultiDlist
{
private:
  static const int64_t MAX_LOCK_LENGTH = 100;
  // the reason why we need to derive is that ObFixedArray<T> needs T has member function to_string
  class ObLockWrapper : public common::ObSpinLock
  {
  public:
    inline int64_t to_string(char *buf, const int64_t len) const { return 0; }
  } CACHE_ALIGNED;

public:
  ObDMMultiDlist()
    : bucket_num_(0), allocator_(), locks_(), buckets_() {}
  ~ObDMMultiDlist() {}
  int init(int64_t tenant_id, int64_t bucket_num);
  void destroy();
  // remove a node in list, the atomicity is guaranteed by lock
  void remove_list_node(const ObDetectableId &detectable_id, ObDetectableIdDNode *node);
  // insert a node at the tail of list, the atomicity is guaranteed by lock
  void add_list_node_tail(const ObDetectableId &detectable_id, ObDetectableIdDNode *node);
  // pop all activate node from list, the atomicity is guaranteed by lock
  void pop_active_node(hash::ObHashSet<ObDetectableId, hash::NoPthreadDefendMode> &still_need_check_id);
private:
  int64_t bucket_num_;
  ObArenaAllocator allocator_;
  ObFixedArray<ObLockWrapper *, common::ObIAllocator> locks_;
  ObFixedArray<ObDList<ObDetectableIdDNode> *, common::ObIAllocator> buckets_;
};

static const int64_t DEFAULT_REQUEST_MAP_BUCKETS_COUNT = 100; //100
typedef hash::ObHashMap<common::ObAddr, obrpc::ObTaskStateDetectReq *,
                hash::SpinReadWriteDefendMode,
                hash::hash_func<common::ObAddr>,
                hash::equal_to<common::ObAddr>> REQUEST_MAP;

// For implementation workarounds: ObDList is not allowed to copy assign.
class ObDMCallbackDList final : public ObDList<ObIDetectCallback>
{
public:
  int assign(const ObDMCallbackDList &src);
};

// design doc:
class ObDetectManager
{
private:
  static const int64_t MAX_TENANT_MEM_LIMIT = 17179869184; //16G
  // for CHECK_MAP
  static const int64_t DEFAULT_CHECK_MAP_BUCKETS_COUNT = 1000000; //100w
  static const int64_t MIDDLE_SET_BUCKETS_COUNT = 100000; //10w
  // for ObDMMultiDlist bucket
  static const int64_t DM_MDLIST_BUCKETS_COUNT = 128;
  static const int64_t DM_MDLIST_BUCKETS_COUNT_FOR_META_TENANT = 4;

  // dm only detects check items that have been present for at least "ACTIVATE_DELAY_TIME" seconds
  static const uint64_t ACTIVATE_DELAY_TIME = 5 * 1000L * 1000L;
public:
  static int mtl_new(ObDetectManager *&dm);
  static int mtl_init(ObDetectManager *&dm);
  static void mtl_destroy(ObDetectManager *&dm);
public:
  /* The CHECK_MAP's construct:
      key_id     bucket node       value
                    ___
      123  ->      |___|->ObDList<ObIDetectCallback>
                   |___|->ObDList<ObIDetectCallback>

                    ___
      124  ->      |___|->ObDList<ObIDetectCallback>
                   |___|->ObDList<ObIDetectCallback>

      means the value of the CHECK_MAP is a linked list.
      for same ObDetectableId, we may insert more than one callback into the linked list.
      when the linked list is empty, we use ObDetectCallbackSetCall to set the first callback
      otherwise, we use ObDetectCallbackAddCall to insert a callback into the linked list
  */
  typedef hash::ObHashMap<ObDetectableId, ObDMCallbackDList,
                  hash::SpinReadWriteDefendMode, hash::hash_func<ObDetectableId>,
                  hash::equal_to<ObDetectableId>> CHECK_MAP;
  /* tool classes */
  // Atomic insert the first detect callback in link list
  class ObDetectCallbackSetCall
  {
  public:
    ObDetectCallbackSetCall(ObDetectManager *dm, ObIDetectCallback *callback)
        : dm_(dm), callback_(callback) {};
    int operator()(const hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);
  private:
    ObDetectManager *dm_;
    ObIDetectCallback *callback_;
  };

  // Atomic insertion a callback into the linked list
  class ObDetectCallbackAddCall
  {
  public:
    void operator()(hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);

    ObDetectCallbackAddCall(ObIDetectCallback *callback, ObDetectManager *dm) :
        callback_(callback), dm_(dm), is_empty_(false) {};

    inline bool is_empty() const { return is_empty_; }
  private:
    ObIDetectCallback *callback_;
    ObDetectManager *dm_;
    bool is_empty_;
  };

  // Atomic removal callback, lock the bucket to avoid reading and inserting operations during removal
  class ObDetectCallbackRemoveCall
  {
  public:
    bool operator()(hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);
    ObDetectCallbackRemoveCall(ObDetectManager *dm, const uint64_t &sequence_id)
      : dm_(dm), sequence_id_(sequence_id) {};
  private:
    ObDetectManager *dm_;
    uint64_t sequence_id_;
  };

  // Atomic execute
  class ObDetectCallbackExecuteCall
  {
  public:
    bool operator()(hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);
    ObDetectCallbackExecuteCall(ObDetectManager *dm, const ObDetectableId &detectable_id,
        const common::ObAddr &addr) : dm_(dm), detectable_id_(detectable_id), from_svr_addr_(addr) {}
  private:
    friend class ObDetectManager;
    ObDetectManager *dm_;
    ObDetectableId detectable_id_;
    common::ObAddr from_svr_addr_;
  };

  // Atomic check task is finish
  class ObCheckStateFinishCall
  {
  public:
    void operator()(hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);
    explicit ObCheckStateFinishCall(const common::ObAddr &addr) : addr_(addr), finished_(false) {}
    inline bool is_finished() const { return finished_; }
  private:
    const common::ObAddr &addr_;
    bool finished_;
  };

  // Atomic get request
  class ObDetectReqGetCall
  {
  public:
    ObDetectReqGetCall(REQUEST_MAP &req_map, lib::MemoryContext &req_map_context) : req_map_(req_map), req_map_context_(req_map_context) {};
    void operator()(hash::HashMapPair<ObDetectableId, ObDMCallbackDList> &entry);
  private:
    REQUEST_MAP &req_map_;
    lib::MemoryContext &req_map_context_;
  };

public:
  ObDetectManager(uint64_t tenant_id) : tenant_id_(tenant_id), mem_context_(nullptr), is_inited_(false) {}
  int init(const ObAddr &self, double mem_factor);
  void destroy();

  int register_detectable_id(const ObDetectableId &detectable_id);
  int unregister_detectable_id(const ObDetectableId &detectable_id);

  // @param sequence_id, return the unique id of callback for unregister
  // @param need_ref, if is true, means that work thread want to use callback.
  // will add ref_count for callback so that dm can't free it until ref_count changes to 0
  // typical scene: qc detects sqc, qc wants to moidify the state of sqc in callback while callback
  // be freed by dm, add ref_count is necessary
  // @param args, used in constructor of T
  template <typename T, typename... U>
  int register_check_item(const ObDetectableId &detectable_id, T *&cb,
                            uint64_t &sequence_id, bool need_ref, U& ... args)
  {
    int ret = OB_SUCCESS;
    T *ptr = nullptr;
    void *buf = nullptr;
    ObIAllocator &allocator = get_mem_context()->get_malloc_allocator();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LIB_LOG(ERROR, "[DM] detect manager not inited", K(ret));
    } else if (OB_UNLIKELY(nullptr != cb)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "[DM] cb must allocated from detect manager", K(cb));
    } else if (detectable_id.is_invalid()) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "[DM] invaild detectable_id", K(common::lbt()));
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "[DM] failed to new cb ", K(ptr));
    } else {
      ptr = new(buf) T(detectable_id.tenant_id_, args...);
      if (!ptr->alloc_succ()) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "[DM] failed to new cb ", K(ptr));
        delete_callback(ptr);
      } else {
        ObCurTraceId::TraceId *cur_thread_id = ObCurTraceId::get_trace_id();
        sequence_id = ObDetectableIdGen::instance().generate_callback_sequence_id();
        ptr->set_trace_id(*cur_thread_id);
        ptr->set_sequence_id(sequence_id);
        ptr->d_node_.detectable_id_ = detectable_id;
        ptr->d_node_.activate_tm_ = ObTimeUtility::current_time() + ACTIVATE_DELAY_TIME;
        // if need_ref is true, which means that work thread may use cb, so add ref_count in case
        // of deleting cb during ObDetectCallbackExecuteCall
        // typical scene: qc may change sqc's task state in callback upon receiving the report
        // message from sqc, but dm may free callback already
        if (need_ref) {
          ptr->inc_ref_count();
        }
        LIB_LOG(DEBUG, "[DM] dm new cb ", K(ptr));
        if (OB_FAIL(do_register_check_item(detectable_id, ptr))) {
          LIB_LOG(WARN, "[DM] failed to register_check_item", K(ptr));
        } else {
          cb = ptr;
        }
      }
    }
    return ret;
  }

  // @param detectable_id, key of CHECK_MAP
  // @param sequence_id, mark which callback to be removed in the link list
  int unregister_check_item(const ObDetectableId &detectable_id, const uint64_t &sequence_id);

  bool is_task_alive(const ObDetectableId &detectable_id)
  {
    return OB_HASH_EXIST == detectable_ids_.exist_refactored(detectable_id);
  }

  bool has_check_item_with_detectable_id(const ObDetectableId &detectable_id)
  {
    return nullptr != all_check_items_.get(detectable_id);
  }

  lib::MemoryContext &get_mem_context() { return mem_context_; }

private:
  int do_register_check_item(const ObDetectableId &detectable_id, ObIDetectCallback *callback);

  void delete_callback(ObIDetectCallback *callback);
  int gather_requests(REQUEST_MAP &req_map, lib::MemoryContext &req_map_context);
  void do_detect_local(const ObDetectableId &detectable_id);
  void do_handle_one_result(const ObDetectableId &detectable_id, const obrpc::ObDetectRpcStatus &rpc_status);

private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectManager);
  friend class ObDetectManagerThread;

  hash::ObHashSet<ObDetectableId, hash::SpinReadWriteDefendMode> detectable_ids_;
  ObDMMultiDlist dm_multi_list_;
  CHECK_MAP all_check_items_;
  // still_need_check_id_ only operated by dm's detect thread, there is no data race.
  hash::ObHashSet<ObDetectableId, hash::NoPthreadDefendMode> still_need_check_id_;

  ObAddr self_;
  uint64_t tenant_id_;
  lib::MemoryContext mem_context_;
  bool is_inited_;
};

class ObDetectManagerThread : public lib::TGRunnable
{
private:
  static const int64_t DETECT_MSG_TIMEOUT = 1 * 1000 * 1000; // 1 second
  static const uint64_t COND_WAIT_TIME_USEC = 500; // 500 us
  static const uint64_t DETECT_COST_TIME_THRESHOLD = 2 * 1000L * 1000L; // for log only
  static const uint64_t DETECT_INTERVAL = 10 * 1000L * 1000L; // detect loop interval, default 10 seconds
public:
public:
  static ObDetectManagerThread &instance();
  int init(const ObAddr &self, rpc::frame::ObReqTransport *transport);
  void run1() override;
private:
  ObDetectManagerThread() : self_(), is_inited_(false) {}
  int detect();
  void send_requests(REQUEST_MAP &req_map, int64_t &send_cnt, lib::MemoryContext &mem_context);
  void detect_local(const obrpc::ObTaskStateDetectReq *req);
  void detect_remote(const common::ObAddr &dst, const obrpc::ObTaskStateDetectReq *req, int64_t &send_cnt, lib::MemoryContext &mem_context);
  void handle_rpc_results(int64_t &send_cnt, lib::MemoryContext &mem_context);
  void handle_one_result(const obrpc::ObDetectRpcStatus &rpc_status);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectManagerThread);
  REQUEST_MAP req_map_;
  obrpc::ObDetectRpcProxy rpc_proxy_;
  ObArray<obrpc::ObDetectRpcStatus *> rpc_statuses_;

  ObAddr self_;
  bool is_inited_;
  ObThreadCond cond_;
};

} // end namespace common
} // end namespace oceanbase
