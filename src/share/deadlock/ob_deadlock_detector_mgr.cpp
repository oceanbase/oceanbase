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

#include "ob_deadlock_detector_mgr.h"
#include "ob_deadlock_detector_rpc.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_batch_sender_thread.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_node.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_parameters.h"
#include "ob_deadlock_arg_checker.h"
#include "ob_deadlock_inner_table_service.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;
uint64_t ObDeadLockDetectorMgr::InnerAllocHandle::InnerFactory::create_count_ = 0;
uint64_t ObDeadLockDetectorMgr::InnerAllocHandle::InnerFactory::release_count_ = 0;
const char * MEMORY_LABEL = "DeadLock";

// definition and initializaion of class static member

ObDeadLockDetectorMgr::ObDeadLockDetectorMgr()
: is_inited_(false),
stop_ts_(0),
proxy_(nullptr),
rpc_(nullptr),
sender_thread_(this) {}

/* * * * * * definition of ObDeadLockDetectorMgr::InnerAllocHandle * * * * */

ObIDeadLockDetector* ObDeadLockDetectorMgr::InnerAllocHandle::alloc_value()
{
  // do not allow alloc val in hashmap
  return nullptr;
}

void ObDeadLockDetectorMgr::InnerAllocHandle::free_value(ObIDeadLockDetector *p)
{
  inner_factory_.release(p);
}

LinkHashNode<UserBinaryKey>* ObDeadLockDetectorMgr::
  InnerAllocHandle::alloc_node(ObIDeadLockDetector *p)
{
  UNUSED(p);
  return OB_NEW(LinkHashNode<UserBinaryKey>, MEMORY_LABEL);
}

void ObDeadLockDetectorMgr::InnerAllocHandle::free_node(LinkHashNode<UserBinaryKey> *node)
{
  if (node != nullptr) {
    //ob_free(node);
    OB_DELETE(LinkHashNode<UserBinaryKey>, MEMORY_LABEL, node);
  }
}

/* * * * * * define for ObDeadLockDetectorMgr::InnerFactory * * * * */

// Create a new detector instance
int ObDeadLockDetectorMgr::InnerAllocHandle::InnerFactory::create(const UserBinaryKey &key,
                                                                  const DetectCallBack &on_detect_operation,
                                                                  const CollectCallBack &on_collect_operation,
                                                                  const ObDetectorPriority &priority,
                                                                  const uint64_t start_delay,
                                                                  const uint32_t count_down_allow_detect,
                                                                  const bool auto_activate_when_detected,
                                                                  ObIDeadLockDetector *&p_detector)
{
  int ret = OB_SUCCESS;

  ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
  SET_USE_500(attr);
  int64_t alived_count = ATOMIC_LOAD(&create_count_) - ATOMIC_LOAD(&release_count_);
  if (alived_count > 50 * 1000) {// limit in 5w active nodes
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "too many detector", K(alived_count), KR(ret));
  } else if (nullptr ==
     (p_detector =
     (ObIDeadLockDetector *)ob_malloc(sizeof(ObLCLNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "DetectorFactory alloc new detector failed", KR(ret));
  } else {
    p_detector = new (p_detector) ObLCLNode(key,
                                            ATOMIC_AAF(&logic_id_, 1),
                                            on_detect_operation,
                                            on_collect_operation,
                                            priority,
                                            start_delay,
                                            count_down_allow_detect,
                                            auto_activate_when_detected);
    if (false == static_cast<ObLCLNode*>(p_detector)->
                 is_successfully_constructed()) {
      ret = OB_INIT_FAIL;
      DETECT_LOG(WARN, "construct ObLCLNode obj failed", KR(ret));
    }
    ATOMIC_INC(&create_count_);
  }

  return ret;
}

// destroy a created detector instance, free its memory
void ObDeadLockDetectorMgr::InnerAllocHandle::InnerFactory::release(ObIDeadLockDetector *p_detector)
{
  if (nullptr == p_detector) {
    DETECT_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "p_detector is nullptr", KP(p_detector));
  } else {
    p_detector->~ObIDeadLockDetector();
    ob_free(p_detector);
    ATOMIC_INC(&release_count_);
  }
}

/* * * * * * definition of ObDeadLockDetectorMgr::DetectorRefGuard * * * * */

// guard should only used on stack, auto-revert pointer when guard destructed
ObDeadLockDetectorMgr::DetectorRefGuard::~DetectorRefGuard()
{
  ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
  if (OB_ISNULL(p_deadlock_detector_mgr)) {
    DETECT_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr), K(MTL_ID()));
  } else {
    p_deadlock_detector_mgr->detector_map_.revert(p_detector_);
  }
}

int ObDeadLockDetectorMgr::DetectorRefGuard::set_detector(ObIDeadLockDetector* p_detector)
{
  CHECK_ARGS(p_detector);
  p_detector_ = p_detector;
  return OB_SUCCESS;
}

/* * * * * * define for ObDeadLockDetectorMgr * * * * */

int ObDeadLockDetectorMgr::mtl_init(ObDeadLockDetectorMgr *&p_deadlock_detector_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(p_deadlock_detector_mgr->init())) {
    DETECT_LOG(ERROR, "init failure detector failed", KR(ret), K(MTL_ID()));
  }
  return ret;
}

int ObDeadLockDetectorMgr::init()
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;

  if (nullptr != proxy_ || nullptr != rpc_) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "proxy_ or rpc_ is not null", PRINT_WRAPPER);
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
    SET_USE_500(attr);
    if (nullptr ==
       (proxy_ =
       (obrpc::ObDetectorRpcProxy *)ob_malloc(sizeof(obrpc::ObDetectorRpcProxy), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc proxy_ memory failed", KR(ret));
    } else if (nullptr == (rpc_ = (ObDeadLockDetectorRpc *)ob_malloc(sizeof(ObDeadLockDetectorRpc),
                                                                     attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc rpc_ memory failed", KR(ret));
    } else {
      proxy_ = new (proxy_) obrpc::ObDetectorRpcProxy();
      rpc_ = new (rpc_) ObDeadLockDetectorRpc();
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(time_wheel_.init(TIME_WHEEL_PRECISION_US,
                                 TIMER_THREAD_COUNT,
                                 DETECTOR_TIMER_NAME))) {
      DETECT_LOG(WARN, "time_wheel_ init failed", PRINT_WRAPPER);
    } else if (OB_FAIL(proxy_->init(GCTX.net_frame_->get_req_transport(), GCTX.self_addr()))) {
      DETECT_LOG(WARN, "req_transport init failed", PRINT_WRAPPER);
    } else if (OB_FAIL(rpc_->init(proxy_, GCTX.self_addr()))) {
      DETECT_LOG(WARN, "rpc_ init faile", PRINT_WRAPPER);
    } else if (OB_FAIL(detector_map_.init(attr))) {
      DETECT_LOG(WARN, "detector_map_ init failed", PRINT_WRAPPER);
    } else if (OB_FAIL(sender_thread_.init())) {
      DETECT_LOG(WARN, "ObLCLBatchSenderThread init failed", PRINT_WRAPPER);
    } else {
      is_inited_ = true;
      DETECT_LOG(INFO, "ObDeadLockDetectorMgr init success", PRINT_WRAPPER);
    }
    DETECT_LOG(INFO, "ObDeadLockDetectorMgr init called", PRINT_WRAPPER, K(lbt()));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != proxy_) {
      proxy_->destroy();
      ob_free(proxy_);
      proxy_ = nullptr;
    }
    if (nullptr != rpc_) {
      rpc_->destroy();
      ob_free(rpc_);
      rpc_ = nullptr;
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(time_wheel_.start())) {
    DETECT_LOG(WARN, "time wheel start failed");
  } else if (OB_FAIL(sender_thread_.start())) {
    DETECT_LOG(WARN, "ObLCLBatchSenderThread start failed");
  }
  return ret;
}

bool ObDeadLockDetectorMgr::ActivateFn::operator()(const UserBinaryKey &key,
                                                   ObIDeadLockDetector *p_detector)
{
  UNUSED(key);
  p_detector->unregister_timer_task();
  return true;
}

void ObDeadLockDetectorMgr::stop()
{
  int ret = OB_SUCCESS;
  ActivateFn fn;
  detector_map_.for_each(fn);
  ob_usleep(PHASE_TIME * 2);
  sender_thread_.stop();
  if (OB_FAIL(time_wheel_.stop())) {
    DETECT_LOG(WARN, "ObDeadLockDetectorMgr stop time wheel failed", KR(ret));
  }
}

void ObDeadLockDetectorMgr::wait()
{
  int ret = OB_SUCCESS;
  sender_thread_.wait();
  if (OB_FAIL(time_wheel_.wait())) {
    DETECT_LOG(WARN, "ObDeadLockDetectorMgr wait time wheel failed", KR(ret));
  }
}

// ObDeadLockDetectorMgr destroy process, all related role should be destroyed within this
void ObDeadLockDetectorMgr::destroy()
{
  int ret = OB_SUCCESS;

  if (false == is_inited_) {
    DETECT_LOG(WARN, "ObDeadLockDetectorMgr not init or has been destroyed");
  } else {
    sender_thread_.destroy();
    detector_map_.destroy();
    if (nullptr != proxy_) {
      proxy_->destroy();
      ob_free(proxy_);
      proxy_ = nullptr;
    }
    if (nullptr != rpc_) {
      rpc_->destroy();
      ob_free(rpc_);
      rpc_ = nullptr;
    }
    time_wheel_.destroy();
    is_inited_ = false;
    DETECT_LOG(INFO, "ObDeadLockDetectorMgr destroy success");
  }
  DETECT_LOG(INFO, "ObDeadLockDetectorMgr destroy called", K(lbt()));

  return;
}

int ObDeadLockDetectorMgr::get_detector_(const UserBinaryKey &user_key,
                                         DetectorRefGuard &detector_guard)
{
  CHECK_INIT();
  CHECK_ARGS(user_key);
  int ret = OB_SUCCESS;
  ObIDeadLockDetector *p_detector = nullptr;

  if (OB_FAIL(detector_map_.get(user_key, p_detector))) {
    // DETECT_LOG(WARN, "detector_map_ get detector failed", KR(ret), K(user_key), KP(p_detector));
  } else {
    detector_guard.set_detector(p_detector);
  }

  return ret;
}

int ObDeadLockDetectorMgr::unregister_key_(const UserBinaryKey &key)
{
  #define PRINT_WRAPPER KR(ret), K(key)
  int ret = common::OB_SUCCESS;
  DetectorRefGuard ref_guard;
  if (OB_FAIL(get_detector_(key, ref_guard))) {
    // DETECT_LOG(WARN, "get_detector failed", PRINT_WRAPPER);
  } else {
    ref_guard.get_detector()->unregister_timer_task();
    if (OB_FAIL(detector_map_.del(key))) {
      DETECT_LOG(WARN, "detector_map_ erase node failed", PRINT_WRAPPER);
    } else {
      DETECT_LOG(TRACE, "unregister key success", PRINT_WRAPPER);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::process_lcl_message(const ObLCLMessage &lcl_msg)
{
  CHECK_INIT();
  CHECK_ARGS(lcl_msg);
  #define PRINT_WRAPPER KR(ret), K(lcl_msg)
  int ret = OB_SUCCESS;
  DetectorRefGuard ref_guard;

  if (OB_FAIL(get_detector_(lcl_msg.get_user_key(), ref_guard))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      DETECT_LOG(WARN, "fail to get detector", PRINT_WRAPPER);
    }
  } else if (OB_FAIL(ref_guard.get_detector()->process_lcl_message(lcl_msg))) {
    ObIDeadLockDetector *detector = ref_guard.get_detector();
    DETECT_LOG(WARN, "fail to process message", PRINT_WRAPPER, KP(detector));
  } else {}

  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::process_collect_info_message(
                           const ObDeadLockCollectInfoMessage &collect_info_msg)
{
  CHECK_INIT();
  CHECK_ARGS(collect_info_msg);
  #define PRINT_WRAPPER KR(ret), K(collect_info_msg)
  int ret = OB_SUCCESS;
  DetectorRefGuard ref_guard;

  (void)check_and_report_cycle_(collect_info_msg);
  if (OB_FAIL(get_detector_(collect_info_msg.get_dest_key(), ref_guard))) {
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      // the local resource has been unregistered
      DETECT_LOG(INFO, "dest_resource not in map", PRINT_WRAPPER);
    }
  } else if (OB_FAIL(ref_guard.get_detector()->process_collect_info_message(collect_info_msg))) {
    ObIDeadLockDetector *detector = ref_guard.get_detector();
    DETECT_LOG(WARN, "fail to process message", PRINT_WRAPPER, KP(detector));
  } else {
    // do nothing
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::process_notify_parent_message(
                           const ObDeadLockNotifyParentMessage &notify_msg)
{
  CHECK_INIT();
  CHECK_ARGS(notify_msg);
  #define PRINT_WRAPPER KR(ret), KP(p_detector), K(notify_msg)
  int ret = OB_SUCCESS;
  ObIDeadLockDetector *p_detector = nullptr;

  const UserBinaryKey &binary_key = notify_msg.get_parent_key();
  const UserBinaryKey &downstream_key = notify_msg.get_src_key();
  if (common::OB_SUCCESS == (ret = detector_map_.get(binary_key, p_detector))) {
    ret = common::OB_ENTRY_EXIST;
    detector_map_.revert(p_detector);
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, MEMORY_LABEL);
    SET_USE_500(attr);
    ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
    if (OB_ISNULL(p_deadlock_detector_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KP(p_deadlock_detector_mgr), K(MTL_ID()));
    } else if (OB_FAIL(p_deadlock_detector_mgr->inner_alloc_handle_.inner_factory_.create(binary_key,
                                                            [](const common::ObIArray<ObDetectorInnerReportInfo> &,
                                                               const int64_t) -> int { DETECT_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "should not kill inner node");
                                                                                       return common::OB_ERR_UNEXPECTED; },
                                                            [binary_key,attr](ObDetectorUserReportInfo& report_info) -> int {
                                                              ObSharedGuard<char> ptr;
                                                              ptr.assign((char*)"detector", [](char*){});
                                                              report_info.set_module_name(ptr);
                                                              char *buffer = (char*)ob_malloc(sizeof(char) * 128, attr);
                                                              if (OB_NOT_NULL(buffer)) {
                                                                binary_key.to_string(buffer, 128);
                                                                ptr.assign(buffer, [](char* p){ ob_free(p); });
                                                              } else {
                                                                ptr.assign((char*)"inner visitor", [](char*){});
                                                              }
                                                              report_info.set_visitor(ptr);
                                                              ptr.assign((char*)"waiting for child execution", [](char*){});
                                                              report_info.set_resource(ptr);
                                                              return common::OB_SUCCESS;
                                                            },
                                                            ObDetectorPriority(PRIORITY_RANGE::EXTREMELY_HIGH, 0),
                                                            0,
                                                            0,
                                                            true,
                                                            p_detector))) {
      DETECT_LOG(WARN, "create new detector instance failed", PRINT_WRAPPER);
    } else if (OB_FAIL(detector_map_.insert_and_get(binary_key, p_detector))) {
      DETECT_LOG(WARN, "detector_map_ insert key and value failed", PRINT_WRAPPER);
      p_deadlock_detector_mgr->inner_alloc_handle_.inner_factory_.release(p_detector);
    } else if (FALSE_IT(p_detector->set_timeout(10 * 1000 * 1000))) {
    } else if (OB_FAIL(p_detector->register_timer_task())) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        ret = common::OB_EAGAIN;// telling user there is a concurrent problem, need retry
      }
      DETECT_LOG(WARN, "start timer task failed", PRINT_WRAPPER);
      (void)detector_map_.del(binary_key);
      detector_map_.revert(p_detector);
    } else {
      ObDependencyResource resource(notify_msg.get_src_addr(), notify_msg.get_src_key());
      if (OB_FAIL(p_detector->block(resource))) {
        DETECT_LOG(WARN, "block child failed", PRINT_WRAPPER);
        p_detector->unregister_timer_task();
        (void)detector_map_.del(binary_key);
      } else {
        DETECT_LOG(INFO, "register parent key success", PRINT_WRAPPER);
      }
      detector_map_.revert(p_detector);
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::check_and_report_cycle_(
                           const ObDeadLockCollectInfoMessage &collect_info_msg)
{
  int ret = OB_SUCCESS;
  if (collect_info_msg.get_collected_info().empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObDetectorInnerReportInfo &organizer = collect_info_msg.get_collected_info().at(0);
    if (organizer.get_addr() == GCTX.self_addr() &&
        organizer.get_user_key() == collect_info_msg.get_dest_key()) {
      uint64_t cycle_hash = calculate_cycle_hash_(collect_info_msg);
      if (OB_FAIL(check_and_record_cycle_hash_(cycle_hash))) {
        DETECT_LOG(INFO, "this cycle may has been reported",
                         KR(ret), K(collect_info_msg), K(cycle_hash));
      } else {
        if (OB_FAIL(ObDeadLockInnerTableService::
                    insert_all(collect_info_msg.get_collected_info()))) {
          DETECT_LOG(INFO, "report inner table success", KR(ret), K(collect_info_msg));
        } else {
          DETECT_LOG(INFO, "report inner table success", K(collect_info_msg));
        }
      }
    }
  }
  return ret;
}

uint64_t ObDeadLockDetectorMgr::calculate_cycle_hash_(
                                const ObDeadLockCollectInfoMessage &collect_info_msg)
{
  uint64_t hash = 0;
  const ObSArray<ObDetectorInnerReportInfo> &collected_info = collect_info_msg.get_collected_info();
  for (int64_t idx = 0; idx < collected_info.count(); ++idx) {
    const ObAddr &addr = collected_info.at(idx).get_addr();
    const uint64_t id = collected_info.at(idx).get_detector_id();
    hash = murmurhash(&addr, sizeof(addr), hash);
    hash = murmurhash(&id, sizeof(id), hash);
  }
  return hash;
}

template<typename T, int POW_OF_2 = 7>
class LimitRecordBuffer
{
  static_assert(POW_OF_2<=20,
                "slots defined more than 2^20=1048576, be sure you want so many slots");
public:
  LimitRecordBuffer() : begin_(0), end_(0) {}
  int check_and_push(const T &element) {
    int ret = OB_SUCCESS;
    ObSpinLockGuard guard(lock_);
    uint64_t idx = begin_;
    for (; idx < end_ && OB_SUCC(ret); ++idx) {
      if (buffer_[real_idx_(idx)] == element) {
        ret = OB_ENTRY_EXIST;
      }
    }
    if (idx == end_) {// not exist
      buffer_[real_idx_(end_++)] = element;
      if (end_ - begin_ > NUM_OF_SLOTS) {
        begin_ = end_ - NUM_OF_SLOTS;
      }
    }
    return ret;
  }
private:
  static constexpr const uint64_t NUM_OF_SLOTS = 1L << POW_OF_2;
  static constexpr const uint64_t MASK = NUM_OF_SLOTS - 1;
  inline uint64_t real_idx_(const uint64_t logic_idx) {
    return (logic_idx & MASK);
  }
  uint64_t begin_;
  uint64_t end_;
  ObSpinLock lock_;
  T buffer_[NUM_OF_SLOTS];
};

int ObDeadLockDetectorMgr::check_and_record_cycle_hash_(const uint64_t hash)
{
  static LimitRecordBuffer<uint64_t> reported_cycle_record;
  return reported_cycle_record.check_and_push(hash);
}

}// detector
}// share
}// oceanbase
