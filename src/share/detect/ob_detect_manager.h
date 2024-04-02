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

struct ObDetectCallbackNode
{
  ObDetectCallbackNode(ObIDetectCallback *cb, uint64_t sequence_id) :
      next_(nullptr), prev_(nullptr), cb_(cb), sequence_id_(sequence_id), executed_(false) {}
  ObDetectCallbackNode() : next_(nullptr), prev_(nullptr), cb_(nullptr) {}
  ~ObDetectCallbackNode() = default;
  void set_executed() { executed_ = true; }
  bool is_executed() { return executed_; }
  ObDetectCallbackNode *next_;
  ObDetectCallbackNode *prev_;
  ObIDetectCallback *cb_;
  uint64_t sequence_id_;
  bool executed_;
};

class ObDetectableIdGen
{
public:
  static ObDetectableIdGen &instance();
  uint64_t get_detect_sequence_id() { return ATOMIC_AAF(&detect_sequence_id_, 1); }
  uint64_t get_callback_node_sequence_id() { return ATOMIC_AAF(&callback_node_sequence_id_, 1); }
  int generate_detectable_id(ObDetectableId &detectable_id, uint64_t tenant_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDetectableIdGen);
  ObDetectableIdGen();
  volatile uint64_t detect_sequence_id_; // for DetectableId
  volatile uint64_t callback_node_sequence_id_; // for mark specific node in linked list(value of CHECK_MAP)
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

// design doc:
class ObDetectManager
{
private:
  static const int64_t MAX_TENANT_MEM_LIMIT = 17179869184; //16G
  // for CHECK_MAP
  static const int64_t DEFAULT_CHECK_MAP_BUCKETS_COUNT = 1000000; //100w
  static const int64_t MINI_MODE_CHECK_MAP_BUCKETS_COUNT = 10000; //1w
  // for detectable_ids_
  static const int64_t DEFAULT_SET_BUCKETS_COUNT = 1000000; //100w
  static const int64_t MINI_MODE_SET_BUCKETS_COUNT = 10000; //1w
  // for still_need_check_id_
  static const int64_t MIDDLE_SET_BUCKETS_COUNT = 100000; //10w
  // for ObDMMultiDlist bucket
  static const int64_t DM_MDLIST_BUCKETS_COUNT = 128;
  static const int64_t DM_MDLIST_BUCKETS_COUNT_FOR_META_TENANT = 4;

  static const uint64_t ACTIVATE_DELAY_TIME = 5 * 1000L * 1000L; // dm only detects checkitems that have been present for at least "ACTIVATE_DELAY_TIME" seconds
public:
  static int mtl_new(ObDetectManager *&dm);
  static int mtl_init(ObDetectManager *&dm);
  static void mtl_destroy(ObDetectManager *&dm);
public:
  /* tool classes */
  /* The CHECK_MAP's construct:
      key_id     bucket node       value
                    ___
      123  ->      |___|->ObDetectCallbackNode*->ObDetectCallbackNode*->...->nullptr
                   |___|->ObDetectCallbackNode*->ObDetectCallbackNode*->...->nullptr

                    ___
      124  ->      |___|->ObDetectCallbackNode*->ObDetectCallbackNode*->...->nullptr
                   |___|->ObDetectCallbackNode*->ObDetectCallbackNode*->...->nullptr

      means the value of the CHECK_MAP is a linked list.
      for same ObDetectableId, we may insert more than one nodes into the linked list.
      when the linked list is empty, we use ObObDetectCallbackNodeSetCall to set the first node
      otherwise, we use ObDetectCallbackNodeAddCall to insert a node into the linked list
  */
  typedef hash::ObHashMap<ObDetectableId, ObDetectCallbackNode *,
                  hash::SpinReadWriteDefendMode, hash::hash_func<ObDetectableId>,
                  hash::equal_to<ObDetectableId>> CHECK_MAP;

  // Atomic insert the first detect callback node in linklist
  class ObObDetectCallbackNodeSetCall
  {
    public:
      explicit ObObDetectCallbackNodeSetCall(ObDetectManager *dm) : dm_(dm) {};
      int operator()(const hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);
    private:
      ObDetectManager *dm_;
  };

  // Atomic insertion a callback node into the linklist
  class ObDetectCallbackNodeAddCall
  {
  public:
    void operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);

    ObDetectCallbackNodeAddCall(ObDetectCallbackNode *cb_node, ObDetectManager *dm) :
        cb_node_(cb_node), dm_(dm), is_empty_(false) {};

    inline bool is_empty()
    {
      return is_empty_;
    }
  private:
    ObDetectCallbackNode *cb_node_;
    ObDetectManager *dm_;
    bool is_empty_;
  };

  // Atomic removal callback, lock the bucket to avoid reading and inserting operations during removal
  class ObDetectCallbackNodeRemoveCall
  {
  public:
    bool operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);
    ObDetectCallbackNodeRemoveCall(ObDetectManager *dm, const uint64_t &node_sequence_id)
      : found_node_(false), dm_(dm), node_sequence_id_(node_sequence_id) {};
    bool is_found_node() { return found_node_; };
  private:
    bool found_node_;
    ObDetectManager *dm_;
    uint64_t node_sequence_id_;
  };

  // Atomic execute
  class ObDetectCallbackNodeExecuteCall
  {
  public:
    bool operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);
    ObDetectCallbackNodeExecuteCall(ObDetectManager *dm, const ObDetectableId &detectable_id,
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
    void operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);
    explicit ObCheckStateFinishCall(const common::ObAddr &addr) : addr_(addr), finished_(false) {}
    bool is_finished() { return finished_; }
  private:
    const common::ObAddr &addr_;
    bool finished_;
  };

  // Atomic get request
  class ObDetectReqGetCall
  {
  public:
    ObDetectReqGetCall(REQUEST_MAP &req_map, lib::MemoryContext &req_map_context) : req_map_(req_map), req_map_context_(req_map_context) {};
    void operator()(hash::HashMapPair<ObDetectableId, ObDetectCallbackNode *> &entry);
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

  // @param node_sequence_id, return the unique id of node for unregister
  // @param need_ref, if is true, means that work thread want to use callback.
  // will add ref_count for callbak so that dm can't free it until ref_count changes to 0
  // typical scene: qc detects sqc, qc wants to moidfy the state of sqc in callback while callback be freed by dm, add ref_count is necessary
  // @param args, used in constructor of T
  template <typename T, typename... U>
  int register_check_item(const ObDetectableId &detectable_id, T *&cb,
                            uint64_t &node_sequence_id, bool need_ref, U& ... args)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LIB_LOG(ERROR, "[DM] detect manager not inited", K(ret));
    } else if (OB_UNLIKELY(nullptr != cb)) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "[DM] cb must allocated from detect manager", K(cb));
    } else if (detectable_id.is_invalid()) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "[DM] invaild detectable_id", K(common::lbt()));
    } else {
      T* ptr = nullptr;
      ObDetectCallbackNode *cb_node = nullptr;
      ObIAllocator &allocator = get_mem_context()->get_malloc_allocator();
      // Simultaneously allocate memory for both ObDetectCallbackNode and ObDetectCallback
      // to reduce the number of allocations.
      void *buf = allocator.alloc(sizeof(ObDetectCallbackNode) + sizeof(T));
      if (OB_NOT_NULL(buf)) {
        ptr = new((char *)(buf) + sizeof(ObDetectCallbackNode)) T(detectable_id.tenant_id_, args...);
        if (!ptr->alloc_succ()) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          ptr->destroy();
          allocator.free(buf);
          LIB_LOG(WARN, "[DM] failed to new cb ", K(ptr));
        } else {
          cb_node = new(buf) ObDetectCallbackNode(ptr, ObDetectableIdGen::instance().get_callback_node_sequence_id());
          ObCurTraceId::TraceId *cur_thread_id = ObCurTraceId::get_trace_id();
          ptr->set_trace_id(*cur_thread_id);
          ptr->d_node_.detectable_id_ = detectable_id;
          ptr->d_node_.activate_tm_ = ObTimeUtility::current_time() + ACTIVATE_DELAY_TIME;
          LIB_LOG(DEBUG, "[DM] dm new cb ", K(ptr));
          if (OB_FAIL(do_register_check_item(detectable_id, cb_node, node_sequence_id, need_ref))) {
            LIB_LOG(WARN, "[DM] failed to register_check_item", K(ptr));
          } else {
            cb = ptr;
          }
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(WARN, "[DM] failed to new cb ", K(ptr));
      }
    }
    return ret;
  }

  // @param detectable_id, key of CHECK_MAP
  // @param node_sequence_id, mark which node to be removed in the linklist(value of CHECK_MAP)
  int unregister_check_item(const ObDetectableId &detectable_id, const uint64_t &node_sequence_id);

  bool is_task_alive(const ObDetectableId &detectable_id)
  {
    return OB_HASH_EXIST == detectable_ids_.exist_refactored(detectable_id);
  }

  lib::MemoryContext &get_mem_context() { return mem_context_; }

private:
  int do_register_check_item(const ObDetectableId &detectable_id, ObDetectCallbackNode *cb_node,
                              uint64_t &node_sequence_id, bool need_ref = false);

  void delete_cb_node(ObDetectCallbackNode *&cb_node);
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
