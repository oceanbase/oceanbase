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

#ifndef OCEANBASE_COMMON_GLOBAL_INTERRUPT_CALL_H_
#define OCEANBASE_COMMON_GLOBAL_INTERRUPT_CALL_H_

#include "share/interrupt/ob_interrupt_rpc_proxy.h"
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/coro/co_var.h"

using oceanbase::obrpc::ObInterruptMessage;
using oceanbase::obrpc::ObInterruptRpcProxy;

namespace oceanbase {

namespace common {

// InterruptibleTaskID supports 128 bits
// Supports arbitrary POD assignment, does not distinguish between types, and compares with 128-bit binary to confirm whether they are equal (complement 0 for insufficient bits)
// Does not distinguish the differences brought about by different data types
// That is to say, int = 1 and uint64_t = 1 and trace_id = (1,0) are regarded as the same task_id
struct ObInterruptibleTaskID
{
  OB_UNIS_VERSION(1);
public:
  ObInterruptibleTaskID() : first_(0), last_(0) {}

  // uint64_t goes directly to this implicit conversion, avoiding copying
  ObInterruptibleTaskID(uint64_t first, uint64_t last = 0) : first_(first), last_(last) {}

  // Support template implicit conversion, easy to use
  template<class T>
  ObInterruptibleTaskID(const T &t) : first_(0), last_(0)
  {
    // sizeof T will be changed to a value when compiling, here is mainly to avoid errors in the development process
    assert(sizeof(T) <= 16);
    memcpy(&first_, &t, sizeof(T));
  }

  void operator=(const ObInterruptibleTaskID &t)
  {
    first_ = t.first_;
    last_ = t.last_;
  }

  bool operator==(const ObInterruptibleTaskID &t) const
  {
    return first_ == t.first_ && last_ == t.last_;
  }

  bool operator!=(const ObInterruptibleTaskID &t) const
  {
    return !(*this == t);
  }

  inline int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, len, pos, "task(%ld,%ld)", first_, last_);
    return pos;
  }

  // For the time being, only the hash value is provided through the upper quantile
  uint64_t hash() const {
    return first_;
  }
  int hash(uint64_t &hash_val) const {
    hash_val = hash();
    return OB_SUCCESS;
  }

  // The 128-bit split into upper and lower int64 is to facilitate the construction of RPC messages and provide effective to_string messages
  uint64_t first_;
  uint64_t last_;
};


/// Interrupt signal checker
class ObInterruptChecker
{
  friend class ObInterruptCheckerUpdateCall;

  friend class ObInterruptCheckerAddCall;

  friend class ObInterruptCheckerRemoveCall;

  friend class ObGlobalInterruptManager;

public:
  /// Only the interface is provided externally:
  /// is_interrupted(): Determine whether to receive interrupt information
  /// is_set_and_interrupted(): Determine whether the interrupt information is received, if the interrupt is not initialized, it will return false
  /// get_interrupt_code(): Get interrupt error code
  /// clear_status() resets the state of the inspector to the initial state
  /// register_checker(tid) to register yourself on the interrupt manager
  ObInterruptChecker() { ObInterruptChecker(false, static_cast<uint64_t>(GETTID())); };
  ObInterruptChecker(bool mod, uint64_t rid) : interrupted_(false), array_pos_(0),
                                               ref_count_(0), mod_(mod), rid_(rid)
  {
    MEMSET(interrupt_code_array_, 0, T_ARRAY_SIZE);
  };

  // Automatically log yourself out during destruction
  ~ObInterruptChecker()
  {
  }

  int register_checker(const ObInterruptibleTaskID &tid);

  void unregister_checker(const ObInterruptibleTaskID &tid);

  bool is_interrupted();

  ObInterruptCode &get_interrupt_code();

  void clear_status();
  void clear_interrupt_status();
  void interrupt(ObInterruptCode &interrupt_code);

private:
  /*
   * T_ARRAY_SIZE代表一个执行单元在执行过程中所能收到的最多的中断个数.
   * 这些中断信息存放在固定大小为T_ARRAY_SIZE的数组中.
   */
  static const uint64_t T_ARRAY_SIZE = 16;
  DISALLOW_COPY_AND_ASSIGN(ObInterruptChecker);
private:
  bool interrupted_;
  ObInterruptCode interrupt_code_array_[T_ARRAY_SIZE];
  int64_t array_pos_;
  /*
  * ref_count_为checker的引用计数, 当引用计数为1时, unregister_interrupt会将interrupted_置false.
  */
  int64_t ref_count_;

  // following info for debug
  // mod_ = true  | work in coro
  // mod_ = false | work in thread
  // rid_ = coro_id in coro mod / thread_id in thread mod
  bool mod_;
  uint64_t rid_;
};

struct ObInterruptCheckerNode
{
  ObInterruptCheckerNode(ObInterruptChecker *checker) :
      checker_(checker), next_(nullptr), prev_(nullptr) {}
  ObInterruptCheckerNode() : checker_(nullptr), next_(nullptr), prev_(nullptr) {}
  ~ObInterruptCheckerNode() = default;
  ObInterruptChecker *checker_;
  ObInterruptCheckerNode *next_;
  ObInterruptCheckerNode *prev_;
};

/// Atomic insertion callback, used when taskid is registered by multiple interrupt checkers
class ObInterruptCheckerAddCall
{
public:
  void operator()(hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry);

  ObInterruptCheckerAddCall(ObInterruptCheckerNode *checker_node) :
      checker_node_(checker_node), is_empty_(false) {};

  inline bool is_empty()
  {
    return is_empty_;
  }
private:
  ObInterruptCheckerNode *checker_node_;
  bool is_empty_;
};

/// Atomic removal callback, lock the bucket to avoid reading and inserting operations during removal
class ObInterruptCheckerRemoveCall
{
public:
  void operator()(hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry);

  ObInterruptCheckerRemoveCall(ObInterruptCheckerNode *checker_node) :
      checker_node_(checker_node), is_empty_(false) {};

  inline bool is_empty()
  {
    return is_empty_;
  }
private:
  ObInterruptCheckerNode *checker_node_;
  bool is_empty_;
};

/// Atomic write callback of map
/// Modify the semaphore state in the checker by callback when acquiring the bucket read lock
class ObInterruptCheckerUpdateCall
{
public:
  void operator()(hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry);

  ObInterruptCheckerUpdateCall(ObInterruptCode &code) : code_(code) {};

private:
  ObInterruptCode code_;
};

// Get checker_node_ through map
class ObInterruptGetCheckerNodeCall
{
public:
  void operator()(hash::HashMapPair<ObInterruptibleTaskID, ObInterruptCheckerNode *> &entry);

  ObInterruptGetCheckerNodeCall(ObInterruptChecker *checker) : checker_(checker),
      checker_node_(NULL), checker_exist_(false) {}
  inline bool is_checker_exist() { return checker_exist_; }
  ObInterruptCheckerNode *get_checker_node() { return checker_node_; }

private:
  ObInterruptChecker *checker_;
  ObInterruptCheckerNode *checker_node_;
  bool checker_exist_;
};


/// Remote interrupt manager
/// Exist in singleton mode to provide
/// Interface for remote interrupt signal transmission
class ObGlobalInterruptManager
{
private:
  /// Use 100W as the bucket initialization parameter of Map
  static const int64_t DEFAULT_HASH_MAP_BUCKETS_COUNT = 250000; //25w
  static const int64_t MINI_MODE_HASH_MAP_BUCKETS_COUNT = 10000; //1w
  static const int64_t DEFAULT_NODE_NUM = 20000; //2w, SimpleAllocer holds each block with memory less than 2M
public:
  /// Hashmap with SpinLock, because the coroutine interrupt signal lock time is short, and there is almost no resource conflict, so SpinLock is used
  typedef hash::ObHashMap<ObInterruptibleTaskID, ObInterruptCheckerNode *,
          hash::SpinReadWriteDefendMode, hash::hash_func<ObInterruptibleTaskID>,
          hash::equal_to<ObInterruptibleTaskID>,
          hash::SimpleAllocer<typename hash::HashMapTypes<ObInterruptibleTaskID,
          ObInterruptCheckerNode *>::AllocType, DEFAULT_NODE_NUM, hash::MultiWriteDefendMode>> MAP;
public:
  static ObGlobalInterruptManager *getInstance();

  /// The initialization method is used to obtain the host and rpc transmitter of the current machine, and initialize the map
  int init(const ObAddr &host, ObInterruptRpcProxy *rpc_proxy);

  /// Record the checker pointer in the map with tid as the key
  int register_checker(ObInterruptChecker *checker, const ObInterruptibleTaskID &tid);

  int unregister_checker(ObInterruptChecker *checker, const ObInterruptibleTaskID &tid);

  /// Interface for remote call
  /// If the dst is consistent with the local address, the interrupt message will not be sent through rpc
  int interrupt(const ObAddr &dst, const ObInterruptibleTaskID &tid, ObInterruptCode &code);

  /// Local coroutine notification interface
  /// You can directly modify the state of the local coroutine checker or as the handle method of the RPC receiver
  int interrupt(const ObInterruptibleTaskID &tid, ObInterruptCode &code);

  /// Used to release map resources, etc.
  void destroy();

  // Create a checker node as the value of the hash map.
  int create_checker_node(ObInterruptChecker *checker, ObInterruptCheckerNode *&checker_node);

  MAP &get_map() { return map_; }
private:
  ObGlobalInterruptManager() : rpc_proxy_(nullptr), map_(), is_inited_(false) {};
  ObGlobalInterruptManager(const ObGlobalInterruptManager &) {};

private:
  static ObGlobalInterruptManager *instance_;

private:
  ObAddr local_;
  ObInterruptRpcProxy *rpc_proxy_;
  MAP map_;
  bool is_inited_;
};

/// The method of obtaining the interrupter, the execution on the coroutine will return a coroutine interrupter, and the execution on the thread will return a thread-private interrupter
OB_INLINE ObInterruptChecker *get_checker()
{
  RLOCAL_INLINE(ObInterruptChecker, th_checker_);
  return &th_checker_;
}

/// Global method, set an interrupter for the current execution unit
OB_INLINE int SET_INTERRUPTABLE(const ObInterruptibleTaskID &tid)
{
  if (OB_ISNULL(get_checker())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "interrupt checker may not be set correctly");
    return OB_ERR_UNEXPECTED;
  }
  return get_checker()->register_checker(tid);
}

/// Global method, check whether the current actuator is in the interrupted state
OB_INLINE bool IS_INTERRUPTED()
{
  if (OB_ISNULL(get_checker())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "interrupt checker may not be set correctly");
    return false;
  }
  return get_checker()->is_interrupted();
}

/// Global method to get the interrupt value of the current executor
OB_INLINE ObInterruptCode &GET_INTERRUPT_CODE()
{
  static ObInterruptCode err_code(OB_ERR_UNEXPECTED);
  if (OB_ISNULL(get_checker())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "interrupt checker may not be set correctly");
    return err_code;
  }
  return get_checker()->get_interrupt_code();
}

/// Global method, unset the current interrupt checker
OB_INLINE void UNSET_INTERRUPTABLE(const ObInterruptibleTaskID &tid)
{
  if (OB_ISNULL(get_checker())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "interrupt checker may not be set correctly");
  } else {
    get_checker()->unregister_checker(tid);
  }
}

OB_INLINE void CLEAR_INTERRUPTABLE()
{
  if (OB_ISNULL(get_checker())) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "interrupt checker may not be set correctly");
  } else {
    get_checker()->clear_interrupt_status();
  }
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_OBSERVER_OB_REMOTE_INTERRUPT_CALL_H_
