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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_MGR_
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_DETECTOR_MGR_
#include "lib/utility/ob_macro_utils.h"
#include "ob_deadlock_detector_common_define.h"
#include "ob_deadlock_parameters.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "storage/tx/ob_time_wheel.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_deadlock_arg_checker.h"
#include "ob_deadlock_message.h"
#include "lib/function/ob_function.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "ob_lcl_scheme/ob_lcl_batch_sender_thread.h"
#include <type_traits>

namespace oceanbase
{
namespace unittest
{
class TestObDeadLockDetector;
}
namespace obrpc
{
class ObDetectorRpcProxy;
class ObDeadLockDetectorRpc;
}// namespace obrpc
namespace rpc
{
namespace frame
{
class ObReqTransport;
}// namespace frame
}// namespace rpc
namespace share
{
namespace detector
{
class ObDeadLockDetectorRpc;
class ObLCLBatchSenderThread;
// ObDeadLockDetectorMgr is the unique global manager(within observer),
// who manages all operations of all detector instances(within observer also)
// Operations including register, unregister, block, and activate,
// specified by variable types of user key, no need of building a user adapter layer
//
// Note:
// Here are all main operation interfaces:
// 1, register resource operation: register_key()
// 2, ungister resource operation: unregister_key()
// 3, build directed dependency relationship between two detector: block()
// 4, remove directed dependency relationship between two detector: activate()
// all interfaces above thread-safe guaranteed
class ObDeadLockDetectorMgr
{
  friend class DetectorRefGuard;
  friend class ActivateFn;
  friend class ObLCLNode;
  friend class ObLCLBatchSenderThread;
  friend class unittest::TestObDeadLockDetector;
  friend class ObLCLBatchSenderThread;
public:
  static bool is_deadlock_enabled() { return ObServerConfig::get_instance()._lcl_op_interval != 0; }
public:
  ObDeadLockDetectorMgr();
  // all inner object centralized initialization interface,
  // should be callde from partition_service's init()
  int init();
  static int mtl_init(ObDeadLockDetectorMgr *&p_deadlock_detector_mgr);
  int start();
  void stop();
  void wait();
  // all inner object centralized destruction interface,
  // should be called from partition_service's destroy()
  void destroy();
  // simple summary detector number info, for unittest
  int64_t get_detector_create_count() { return ATOMIC_LOAD(&inner_alloc_handle_.inner_factory_.create_count_); }
  int64_t get_detector_release_count() { return ATOMIC_LOAD(&inner_alloc_handle_.inner_factory_.release_count_); }
  // register resource operation
  template<typename KeyType>
  int register_key(const KeyType &key,
                   const DetectCallBack &detect_callback,
                   const CollectCallBack &on_collect_operation,
                   const ObDetectorPriority &priority = ObDetectorPriority(0),
                   const uint64_t start_delay = 0,
                   const uint32_t count_down_allow_detect = 0,
                   const bool auto_activate_when_detected = true);
  template<typename KeyType1, typename KeyType2>
  int add_parent(const KeyType1 &key, const KeyType2 &parent_key);
  template<typename KeyType1, typename KeyType2>
  int add_parent(const KeyType1 &key,
                 const common::ObAddr &dest_addr,
                 const KeyType2 &parent_key);
  template<typename KeyType>
  int set_timeout(const KeyType &key, const int64_t timeout);
  template<typename KeyType>
  int check_detector_exist(const KeyType &key, bool &exist);
  // ungister resource operation
  template<typename KeyType>
  int unregister_key(const KeyType &key);
  int unregister_key_(const UserBinaryKey &key);
  // build directed dependency relationship between two detector
  template<typename T1, typename T2, typename std::enable_if<!std::is_base_of<common::ObIArray<ObDependencyResource>, T2>::value, bool>::type = true>
  int block(const T1 &src_key, const T2 &dest_key);
  template<typename T1>
  int block(const T1 &src_key, const common::ObIArray<ObDependencyResource> &new_list);
  template<typename T1, typename T2, typename std::enable_if<!std::is_base_of<common::ObIArray<ObDependencyResource>, T2>::value, bool>::type = true>
  int block(const T1 &src_key, const common::ObAddr &dest_addr, const T2 &dest_key);
  // func is a callback method to get DependencyResource dynamically
  // func input args: ObDependencyResource& - the resource will blocked on
  //                  bool& - remove this callback from block list if true, setted false by default
  // func return value: int - err code, this callback's result will not used if ret is not SUCCESS
  template<typename T>
  int block(const T &src_key, const BlockCallBack &func);
  // replace block list by new one
  template<typename T>
  int replace_block_list(const T &src_key,
                                const common::ObIArray<ObDependencyResource> &new_list);
  template<typename T>
  int get_block_list(const T &src_key, common::ObIArray<ObDependencyResource> &cur_list);
  template<typename T>
  int dec_count_down_allow_detect(const T &src_key);
  // remove directed dependency relationship between two detector
  template<typename T1, typename T2>
  int activate(const T1 &src_key, const T2 &dest_key);
  template<typename T1, typename T2>
  int activate(const T1 &src_key, const common::ObAddr &dest_addr, const T2 &dest_key);
  template<typename T1>
  int activate_all(const T1 &src_key);
  // exposed to detector RPC, shouldn't called from others
  int process_lcl_message(const ObLCLMessage &lcl_msg);
  int process_collect_info_message(const ObDeadLockCollectInfoMessage &collect_info_msg);
  int process_notify_parent_message(const ObDeadLockNotifyParentMessage &collect_info_msg);

private:
  // exposed to deadlock detector, shouldn't called from others
  common::ObTimeWheel& get_time_wheel() { return time_wheel_; }
  ObDeadLockDetectorRpc& get_rpc() { return (*rpc_); }
  int check_and_report_cycle_(const ObDeadLockCollectInfoMessage &collect_info_msg);
  uint64_t calculate_cycle_hash_(const ObDeadLockCollectInfoMessage &collect_info_msg);
  int check_and_record_cycle_hash_(const uint64_t hash);

  // define for ObLinkHashMap
  class InnerAllocHandle
  {
  public:
    // factory for creating detecor instance
    class InnerFactory
    {
    public:
      InnerFactory() : logic_id_(0) {}
      int create(const UserBinaryKey &key,
                 const DetectCallBack &on_detect_operation,
                 const CollectCallBack &on_collect_operation,
                 const ObDetectorPriority &priority,
                 const uint64_t start_delay,
                 const uint32_t count_down_allow_detect,
                 const bool auto_activate_when_detected,
                 ObIDeadLockDetector *&p_detector);
      void release(ObIDeadLockDetector *p_detector);
      static uint64_t create_count_;
      static uint64_t release_count_;
    private:
      uint64_t logic_id_;
    } inner_factory_;
    ObIDeadLockDetector *alloc_value();
    void free_value(ObIDeadLockDetector *p);
    common::LinkHashNode<UserBinaryKey> *alloc_node(ObIDeadLockDetector *p);
    void free_node(common::LinkHashNode<UserBinaryKey> *node);
  } inner_alloc_handle_;

  // A simple guard object for protecting reference count of detector instance
  // should only used on stack
  class DetectorRefGuard
  {
  public:
    DetectorRefGuard() : p_detector_(nullptr) {}
    ~DetectorRefGuard();
    int set_detector(ObIDeadLockDetector* p_detector);
    ObIDeadLockDetector* const& get_detector() const { return p_detector_; }
  private:
    ObIDeadLockDetector* p_detector_;
  };

  class ActivateFn
  {
  public:
    bool operator()(const UserBinaryKey &key, ObIDeadLockDetector *p_detector);
  };

  template <typename KeyType>
  int try_create_inner_detector_(const KeyType &key);
  int get_detector_(const UserBinaryKey &user_key, DetectorRefGuard &detector_guard);

  bool is_inited_;// marked ObDeadLockDetectorMgr hash been inited or not
  int64_t stop_ts_;// the timestamp of stop() called
  // global single instance timer for detector period tansmit operation
  common::ObTimeWheel time_wheel_;
  obrpc::ObDetectorRpcProxy *proxy_;// used by rpc_
  ObDeadLockDetectorRpc *rpc_;// message sender/receiver
  ObLCLBatchSenderThread sender_thread_;// inner thread to cache, batch message and send them
  common::ObLinkHashMap<UserBinaryKey,
                        ObIDeadLockDetector,
                        InnerAllocHandle,
                        common::RefHandle> detector_map_;//UserBinaryKey to detector instance
};
// register a user specified key
// register action means user specified key is associated with a new created detector instance
//
// all related out-of-function state are from detector_map_/InnerFactory,
// their interfaces provide thread-safe semantics, thus this interface is thread-safe guaranteed
//
// @param [in] key user specified key
// @param [in] on_detect_operation call-back operation clalled while deadlock detected
// @param [in] report_info user input info, for reporting while deadlock detected
// @param [in] priority priority of created detector instance,
//             lower priority means higher killed probability.
// @param [in] start_delay the associated detector start work after start_delay(ms) time
// @param [in] auto_activate_when_detected indicate whether auto-remove related dependency
//             relationship while deadlock detected or not. setted true by default,
//             otherwise on_detect_operation may be called more than one time,
//             which usually unexpected
// @return error code
template<typename KeyType>
int ObDeadLockDetectorMgr::register_key(const KeyType &key,
                                        const DetectCallBack &on_detect_operation,
                                        const CollectCallBack &on_collect_operation,
                                        const ObDetectorPriority &priority,
                                        const uint64_t start_delay,
                                        const uint32_t count_down_allow_detect,
                                        const bool auto_activate_when_detected)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(key, on_detect_operation, on_collect_operation, priority, start_delay);
  #define PRINT_WRAPPER KR(ret), K(key), K(on_detect_operation), K(on_collect_operation),\
                        K(priority), K(start_delay), K(auto_activate_when_detected)
  int ret = common::OB_SUCCESS;
  ObIDeadLockDetector *p_detector = nullptr;
  UserBinaryKey binary_key;

  if (OB_FAIL(binary_key.set_user_key(key))) {
    DETECT_LOG(WARN, "user key seialized to binary code failed", PRINT_WRAPPER);
  } else if (common::OB_SUCCESS == (ret = detector_map_.get(binary_key, p_detector))) {
    ret = common::OB_ENTRY_EXIST;
    detector_map_.revert(p_detector);
    // DETECT_LOG(INFO, "key already exist", PRINT_WRAPPER);
  } else if (common::OB_ENTRY_NOT_EXIST == ret) {// create obj and insert to map
    if (OB_FAIL(inner_alloc_handle_.inner_factory_.create(binary_key,
                                                          on_detect_operation,
                                                          on_collect_operation,
                                                          priority,
                                                          start_delay,
                                                          count_down_allow_detect,
                                                          auto_activate_when_detected,
                                                          p_detector))) {
      DETECT_LOG(WARN, "create new detector instance failed", PRINT_WRAPPER, KP(p_detector));
    } else if (OB_FAIL(detector_map_.insert_and_get(binary_key, p_detector))) {
      DETECT_LOG(WARN, "detector_map_ insert key and value failed", PRINT_WRAPPER, KP(p_detector));
      inner_alloc_handle_.inner_factory_.release(p_detector);
    } else if (OB_FAIL(p_detector->register_timer_task())) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        ret = common::OB_EAGAIN;// telling user there is a concurrent problem, need retry
      }
      DETECT_LOG(WARN, "start timer task failed", PRINT_WRAPPER, KP(p_detector));
      (void)detector_map_.del(binary_key);
      detector_map_.revert(p_detector);
    } else {
      detector_map_.revert(p_detector);
      DETECT_LOG(TRACE, "register key success", PRINT_WRAPPER);
    }
  } else {
    DETECT_LOG(ERROR, "get key error, couldn't handle", PRINT_WRAPPER);
  }

  return ret;
  #undef PRINT_WRAPPER
}
template<typename KeyType>
int ObDeadLockDetectorMgr::check_detector_exist(const KeyType &key, bool &exist)
{
  CHECK_INIT();
  CHECK_ARGS(key);
  #define PRINT_WRAPPER KR(ret), K(key)
  int ret = common::OB_SUCCESS;
  UserBinaryKey user_key;
  DetectorRefGuard ref_guard;
  if (OB_FAIL(user_key.set_user_key(key))) {
    DETECT_LOG(WARN, "user key serialization failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(user_key, ref_guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      exist = false;
      ret = OB_SUCCESS;
    }
  } else {
    exist = true;
  }
  return ret;
  #undef PRINT_WRAPPER
}
// unregister a user specified key
// unregister action means:
// 1. the detector instance associated with user specified key will be released
// 2. user specified key will not associated with a local unqiue ID anymore
//
// all related out-of-function state are from id_map_/detector_map_/InnerFactory
// their interfaces provide thread-safe semantics, thus this interface is thread-safe guaranteed
//
// @param [in] key user specified key
// @return error code
template<typename KeyType>
int ObDeadLockDetectorMgr::unregister_key(const KeyType &key)
{
  CHECK_INIT();
  CHECK_ARGS(key);
  #define PRINT_WRAPPER KR(ret), K(key)
  int ret = common::OB_SUCCESS;
  UserBinaryKey user_key;

  if (OB_FAIL(user_key.set_user_key(key))) {
    DETECT_LOG(WARN, "user key serialization failed", PRINT_WRAPPER);
  } else {
    ret = unregister_key_(user_key);
  }
  return ret;
  #undef PRINT_WRAPPER
}

// call for building directed dependency relationship between two detector(both in local)
// thread-safe guaranteed
//
// @param [in] src_key the source key resource specified by user
// @param [in] dest_key the destination key resource specified by user
// @return error code
template<typename T1, typename T2, typename std::enable_if<!std::is_base_of<common::ObIArray<ObDependencyResource>, T2>::value, bool>::type>
int ObDeadLockDetectorMgr::block(const T1 &src_key, const T2 &dest_key)
{
  return block(src_key, GCTX.self_addr(), dest_key);
}

// call for building directed dependency relationship between two detector(local to remote)
// thread-safe guaranteed
//
// @param [in] src_key the source key resource specified by user
// @param [in] appened_list the destination network address specified by user
// @return error code
template<typename T1>
int ObDeadLockDetectorMgr::block(const T1 &src_key,
                                 const common::ObIArray<ObDependencyResource> &appened_list)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(src_key);
  #define PRINT_WRAPPER KR(ret), K(src_key), K(appened_list)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;
  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else {
    for (int64_t idx = 0; idx < appened_list.count() && OB_SUCC(ret); ++idx) {
      if (OB_FAIL(ref_guard.get_detector()->block(appened_list.at(idx)))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          DETECT_LOG(WARN, "detector block op failed", PRINT_WRAPPER);
        }
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

// call for building directed dependency relationship between two detector(local to remote)
// thread-safe guaranteed
//
// @param [in] src_key the source key resource specified by user
// @param [in] dest_addr the destination network address specified by user
// @param [in] dest_key the destination key resource specified by user
// @return error code
template<typename T1, typename T2, typename std::enable_if<!std::is_base_of<common::ObIArray<ObDependencyResource>, T2>::value, bool>::type>
int ObDeadLockDetectorMgr::block(const T1 &src_key,
                                 const common::ObAddr &dest_addr,
                                 const T2 &dest_key)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(src_key, dest_addr, dest_key);
  #define PRINT_WRAPPER KR(ret), K(src_key), K(dest_addr), K(dest_key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;
  UserBinaryKey dest_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(dest_user_key.set_user_key(dest_key))) {
    DETECT_LOG(WARN, "dest_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else {
    ObDependencyResource resource(dest_addr, dest_user_key);
    if (OB_FAIL(ref_guard.get_detector()->block(resource))) {
      DETECT_LOG(WARN, "detector block op failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG(INFO, "detector block op success", PRINT_WRAPPER);
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

template<typename T>
int ObDeadLockDetectorMgr::block(const T &src_key,
                                 const BlockCallBack &func)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(func, src_key);
  #define PRINT_WRAPPER KR(ret), K(src_key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else if (OB_FAIL(ref_guard.get_detector()->block(func))) {
    DETECT_LOG(WARN, "block on callback failed", PRINT_WRAPPER);
  } else {}

  return ret;
  #undef PRINT_WRAPPER
}

// replace block list by new one
//
// @param [in] src_key detector key
// @param [in] new_list specified new block list
// @return error code
template<typename T>
int ObDeadLockDetectorMgr::replace_block_list(const T &src_key,
                                              const common::ObIArray<ObDependencyResource>
                                                    &new_list)
{
  CHECK_INIT();
  CHECK_ENABLED();
  #define PRINT_WRAPPER KR(ret), K(src_key), K(new_list)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else if (OB_FAIL(ref_guard.get_detector()->replace_block_list(new_list))) {
    // DETECT_LOG(WARN, "replace block list failed", PRINT_WRAPPER);
  } else {
    // DETECT_LOG(INFO, "replace block list success", PRINT_WRAPPER);
  }

  return ret;
  #undef PRINT_WRAPPER
}
template<typename T>
int ObDeadLockDetectorMgr::get_block_list(const T &src_key,
                                          common::ObIArray<ObDependencyResource> &cur_list)
{
  CHECK_INIT();
  CHECK_ENABLED();
  #define PRINT_WRAPPER KR(ret), K(src_key), K(cur_list)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else if (OB_FAIL(ref_guard.get_detector()->get_block_list(cur_list))) {
    DETECT_LOG(WARN, "get block list failed", PRINT_WRAPPER);
  } else {
    // DETECT_LOG(INFO, "replace block list success", PRINT_WRAPPER);
  }

  return ret;
  #undef PRINT_WRAPPER
}

template<typename T>
int ObDeadLockDetectorMgr::dec_count_down_allow_detect(const T &src_key)
{
  CHECK_INIT();
  CHECK_ENABLED();
  #define PRINT_WRAPPER KR(ret)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else {
    ref_guard.get_detector()->dec_count_down_allow_detect();
  }

  return ret;
  #undef PRINT_WRAPPER
}
// call for removing directed dependency relationship between two detector(both in local)
// thread-safe guaranteed
//
// @param [in] src_key the source resource key specified by user
// @param [in] dest_key the destination resource key specified by user
// @return error code
template<typename T1, typename T2>
int ObDeadLockDetectorMgr::activate(const T1 &src_key, const T2 &dest_key)
{
  return activate(src_key, GCTX.self_addr(), dest_key);
}
// call for removing directed dependency relationship between two detector(local to remote)
// thread-safe guaranteed
//
// @param [in] src_key the source resource key specified by user
// @param [in] dest_addr the destination network address specified by user
// @param [in] dest_key the destination resource key specified by user
// @return error code
template<typename T1, typename T2>
int ObDeadLockDetectorMgr::activate(const T1 &src_key,
                                    const common::ObAddr &dest_addr,
                                    const T2 &dest_key)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(src_key, dest_addr, dest_key);
  #define PRINT_WRAPPER KR(ret), K(src_key), K(dest_addr), K(dest_key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;
  UserBinaryKey dest_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(dest_user_key.set_user_key(dest_key))) {
    DETECT_LOG(WARN, "dest_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else {
    ObDependencyResource resource(dest_addr, dest_user_key);
    if (OB_FAIL(ref_guard.get_detector()->activate(resource))) {
      DETECT_LOG(WARN, "detector activate op failed", PRINT_WRAPPER, K(resource));
    } else {
      DETECT_LOG(INFO, "detector activate op success", PRINT_WRAPPER, K(resource));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

template<typename T1>
int ObDeadLockDetectorMgr::activate_all(const T1 &src_key)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(src_key);
  #define PRINT_WRAPPER KR(ret), K(src_key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;
  UserBinaryKey dest_user_key;

  if (OB_FAIL(src_user_key.set_user_key(src_key))) {
    DETECT_LOG(WARN, "src_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(src_user_key, ref_guard))) {
    DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else if (OB_FAIL(ref_guard.get_detector()->activate_all())) {
    DETECT_LOG(WARN, "activate all failed", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template<typename KeyType1, typename KeyType2>
int ObDeadLockDetectorMgr::add_parent(const KeyType1 &key,
                                      const KeyType2 &parent_key)
{
  return add_parent(key, GCTX.self_addr(), parent_key);
}

template<typename KeyType1, typename KeyType2>
int ObDeadLockDetectorMgr::add_parent(const KeyType1 &key,
                                      const common::ObAddr &dest_addr,
                                      const KeyType2 &parent_key)
{
  CHECK_INIT();
  CHECK_ENABLED();
  CHECK_ARGS(key, dest_addr, parent_key);
  #define PRINT_WRAPPER KR(ret), K(src_user_key), K(dest_addr), K(dest_user_key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey src_user_key;
  UserBinaryKey dest_user_key;

  if (OB_FAIL(src_user_key.set_user_key(key))) {
    DETECT_LOG(WARN, "src_user_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(dest_user_key.set_user_key(parent_key))) {
    DETECT_LOG(WARN, "dest_user_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_SUCC(get_detector_(src_user_key, ref_guard))) {
    ObDependencyResource resource(dest_addr, dest_user_key);
    if (OB_FAIL(ref_guard.get_detector()->add_parent(resource))) {
      DETECT_LOG(WARN, "detector add parent failed", PRINT_WRAPPER, K(resource));
    } else {
      DETECT_LOG(INFO, "detector add parent success", PRINT_WRAPPER, K(resource));
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

template<typename KeyType>
int ObDeadLockDetectorMgr::set_timeout(const KeyType &key, const int64_t timeout)
{
  CHECK_INIT();
  CHECK_ARGS(key, timeout);
  #define PRINT_WRAPPER KR(ret), K(user_key), K(timeout)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  UserBinaryKey user_key;

  if (OB_FAIL(user_key.set_user_key(key))) {
    DETECT_LOG(WARN, "dest_user_key serialzation failed", PRINT_WRAPPER);
  } else if (OB_FAIL(get_detector_(user_key, ref_guard))) {
    DETECT_LOG(WARN, "con not get obj", PRINT_WRAPPER);
  } else {
    if (timeout > 1_hour && REACH_TIME_INTERVAL(1_s)) {
      DETECT_LOG(INFO, "timeout value more than 1 hour", PRINT_WRAPPER);
    }
    ref_guard.get_detector()->set_timeout(timeout);
  }

  return ret;
  #undef PRINT_WRAPPER
}

}// namespace detector
}// namespace share
}// namespace oceanbase
#endif
