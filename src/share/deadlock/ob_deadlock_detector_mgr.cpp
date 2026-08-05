/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_deadlock_detector_mgr.h"
#include "ob_deadlock_detector_rpc.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_batch_sender_thread.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_node.h"
#include "ob_deadlock_inner_table_service.h"
#include "observer/ob_server.h"
#include "lib/string/ob_occam_regex.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_table_access_helper.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

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
  LinkHashNode<UserBinaryKey> *ret = NULL;
  void *buf = mtl_malloc(sizeof(LinkHashNode<UserBinaryKey>), "DetectorMap");
  if (OB_NOT_NULL(buf)) {
    ret = new (buf) LinkHashNode<UserBinaryKey>();
  }
  return ret;
}

void ObDeadLockDetectorMgr::InnerAllocHandle::free_node(LinkHashNode<UserBinaryKey> *node)
{
  if (node != nullptr) {
    node->~LinkHashNode();
    mtl_free(node);
  }
}

/* * * * * * define for ObDeadLockDetectorMgr::InnerFactory * * * * */

// Create a new detector instance
int ObDeadLockDetectorMgr::InnerAllocHandle::InnerFactory::create(const UserBinaryKey &key,
                                                                  const DetectCallBack &on_detect_operation,
                                                                  const CollectCallBack &on_collect_operation,
                                                                  const FillVirtualInfoCallBack &fill_virtual_info_callbeck,
                                                                  const int64_t waiter_create_time,
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
     (ObIDeadLockDetector *)mtl_malloc(sizeof(ObLCLNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "DetectorFactory alloc new detector failed", KR(ret));
  } else {
    p_detector = new (p_detector) ObLCLNode(key,
                                            ATOMIC_AAF(&logic_id_, 1),
                                            on_detect_operation,
                                            on_collect_operation,
                                            fill_virtual_info_callbeck,
                                            waiter_create_time,
                                            priority,
                                            start_delay,
                                            count_down_allow_detect,
                                            auto_activate_when_detected);
    if (false == static_cast<ObLCLNode*>(p_detector)->
                 is_successfully_constructed()) {
      ret = OB_INIT_FAIL;
      DETECT_LOG(WARN, "construct ObLCLNode obj failed", KR(ret));
      mtl_free(p_detector);
    } else {
      ATOMIC_INC(&create_count_);
    }
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
    mtl_free(p_detector);
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
    if (OB_FAIL(ret)) {
    } else if (nullptr ==
       (proxy_ =
       (obrpc::ObDetectorRpcProxy *)mtl_malloc(sizeof(obrpc::ObDetectorRpcProxy), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc proxy_ memory failed", KR(ret));
    } else if (nullptr == (rpc_ = (ObDeadLockDetectorRpc *)mtl_malloc(sizeof(ObDeadLockDetectorRpc),
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
      mtl_free(proxy_);
      proxy_ = nullptr;
    }
    if (nullptr != rpc_) {
      rpc_->destroy();
      mtl_free(rpc_);
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
      mtl_free(proxy_);
      proxy_ = nullptr;
    }
    if (nullptr != rpc_) {
      rpc_->destroy();
      mtl_free(rpc_);
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

struct InnerNodeDetectCallback {
  int operator()(const common::ObIArray<ObDetectorInnerReportInfo> &, const int64_t) {
    DETECT_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "should not kill inner node");
    return common::OB_ERR_UNEXPECTED;
  }
};
struct InnerNodeCollectCallback {
  InnerNodeCollectCallback(const UserBinaryKey &binary_key)
  : binary_key_(binary_key) {}
  int operator()(const ObDependencyHolder &, ObDetectorUserReportInfo& report_info) {
    ObSharedGuard<char> ptr;
    ptr.assign((char*)"detector", DoNothingDeleter());
    report_info.set_module_name(ptr);
    char *buffer = (char*)share::mtl_malloc(sizeof(char) * 128, "DETECT_INNER");
    if (OB_NOT_NULL(buffer)) {
      binary_key_.to_string(buffer, 128);
      ptr.assign(buffer, MtlDeleter());
    } else {
      ptr.assign((char*)"inner visitor", DoNothingDeleter());
    }
    report_info.set_visitor(ptr);
    ptr.assign((char*)"waiting for child execution", DoNothingDeleter());
    report_info.set_resource(ptr);
    return OB_SUCCESS;
  }
private:
  UserBinaryKey binary_key_;
};
struct InnerNodeFillVirtualInfoCallback {
  InnerNodeFillVirtualInfoCallback() : action_() {}
  int assign(const InnerNodeFillVirtualInfoCallback &rhs) {
    return action_.assign(rhs.action_);
  }
  int operator()(const bool need_fill_conflict_action_flag,
                 char *buffer,/*to_string buffer*/
                 const int64_t buffer_len/*to_string buffer length*/,
                 int64_t &pos,/*to_string current position*/
                 DetectorNodeInfoForVirtualTable &info/*virtual info to fill*/) {
    int ret = OB_SUCCESS;
    int64_t to_string_len = action_.to_string(buffer + pos, buffer_len - pos);
    info.action_.assign(buffer + pos, to_string_len);
    pos += to_string_len;
    return ret;
  }
  ObStringHolder action_;
};
int ObDeadLockDetectorMgr::process_notify_parent_message(const ObDeadLockNotifyParentMessage &notify_msg)
{
  CHECK_INIT();
  CHECK_ARGS(notify_msg);
  #define PRINT_WRAPPER KR(ret), KP(p_detector), K(notify_msg)
  int ret = OB_SUCCESS;
  ObIDeadLockDetector *p_detector = nullptr;
  const UserBinaryKey &binary_key = notify_msg.get_parent_key();
  const UserBinaryKey &downstream_key = notify_msg.get_src_key();
  if (common::OB_SUCCESS == (ret = detector_map_.get(binary_key, p_detector))) {
    bool renew_lease_success = false;
    if (OB_FAIL(p_detector->check_and_renew_lease(notify_msg, renew_lease_success))) {
      DETECT_LOG(WARN, "failed to check and renew lease", KR(ret), KPC(p_detector), K(MTL_ID()));
    }
    detector_map_.revert(p_detector);
    if (OB_FAIL(ret) || !renew_lease_success) {
      unregister_key_(binary_key);
      p_detector = nullptr;
    }
  }
  if (OB_ISNULL(p_detector)) {
    ObDeadLockDetectorMgr *p_deadlock_detector_mgr = MTL(ObDeadLockDetectorMgr *);
    InnerNodeFillVirtualInfoCallback fill_virtual_info_cb;
    if (OB_FAIL(fill_virtual_info_cb.action_.assign(notify_msg.get_action()))) {
      DETECT_LOG(WARN, "failed to assign action", KR(ret), K(notify_msg));
    } else if (OB_ISNULL(p_deadlock_detector_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(ERROR, "can not get ObDeadLockDetectorMgr", KR(ret), KP(p_deadlock_detector_mgr), K(MTL_ID()));
    } else if (OB_FAIL(p_deadlock_detector_mgr
                       ->inner_alloc_handle_.inner_factory_
                       .create(binary_key,
                               InnerNodeDetectCallback(),
                               InnerNodeCollectCallback(binary_key),
                               fill_virtual_info_cb,
                               0,
                               ObDetectorPriority(PRIORITY_RANGE::EXTREMELY_HIGH, 0),
                               0,
                               0,
                               true,
                               p_detector))) {
      DETECT_LOG(WARN, "create new detector instance failed", PRINT_WRAPPER);
    } else if (OB_FAIL(detector_map_.insert_and_get(binary_key, p_detector))) {
      DETECT_LOG(WARN, "detector_map_ insert key and value failed", PRINT_WRAPPER);
      p_deadlock_detector_mgr->inner_alloc_handle_.inner_factory_.release(p_detector);
    } else if (FALSE_IT(p_detector->set_timeout(INNER_NODE_LEASE))) {
    } else if (OB_FAIL(p_detector->register_timer_task())) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        ret = common::OB_EAGAIN;// telling user there is a concurrent problem, need retry
      }
      DETECT_LOG(WARN, "start timer task failed", PRINT_WRAPPER);
      (void)detector_map_.del(binary_key);
      detector_map_.revert(p_detector);
    } else {
      ObDependencyHolder resource(notify_msg.get_src_addr(), notify_msg.get_src_key());
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
        const int64_t INSERT_ALL_INFO_TIME = collect_info_msg.get_collected_info().count() * 10_ms;
        const int64_t MIN_INSERT_ALL_INFO_TIME = 1_s;
        const int64_t remain_timeout_ts = THIS_WORKER.get_timeout_remain();
        const int64_t timeout_ts = THIS_WORKER.get_timeout_ts();
        const int64_t estimate_insert_time = INSERT_ALL_INFO_TIME < MIN_INSERT_ALL_INFO_TIME
                                          ? MIN_INSERT_ALL_INFO_TIME : INSERT_ALL_INFO_TIME;
        if (remain_timeout_ts > estimate_insert_time) {
          THIS_WORKER.set_timeout_ts(timeout_ts - estimate_insert_time);
          get_trans_history_sql_from_audit_(collect_info_msg);
          THIS_WORKER.set_timeout_ts(timeout_ts);
        }
        if (OB_FAIL(ObDeadLockInnerTableService::
                    insert_all(collect_info_msg.get_collected_info()))) {
          DETECT_LOG(WARN, "report inner table failed", KR(ret), K(collect_info_msg));
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
    const ObDetectorInnerReportInfo &info = collected_info.at(idx);
    const uint64_t tenant_id = info.get_tenant_id();
    const ObAddr &addr = info.get_addr();
    const uint64_t id = info.get_detector_id();
    hash = murmurhash(&tenant_id, sizeof(tenant_id), hash);
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
  LimitRecordBuffer() : begin_(0), end_(0), lock_(common::ObLatchIds::LIMIT_RECORD_BUFFER_SPIN_LOCK) {}
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

static void collect_cycle_audit_exec_addrs_(
  const ObSArray<ObDetectorInnerReportInfo> &collected_info_array,
  const int64_t waiter_idx,
  ObIArray<ObAddr> &addrs)
{
  addrs.reset();
  if (waiter_idx >= 0 && waiter_idx < collected_info_array.count()) {
    const ObAddr &waiter_addr = collected_info_array.at(waiter_idx).get_addr();
    if (waiter_addr.is_valid()) {
      (void)addrs.push_back(waiter_addr);
    }
  }
  for (int64_t i = 0; i < collected_info_array.count(); ++i) {
    if (i == waiter_idx) {
      continue;
    }
    const ObAddr &addr = collected_info_array.at(i).get_addr();
    if (!addr.is_valid()) {
      continue;
    }
    bool exists = false;
    for (int64_t j = 0; j < addrs.count(); ++j) {
      if (addrs.at(j) == addr) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      (void)addrs.push_back(addr);
    }
  }
}

static int get_wait_sql_from_report_info_(
  const ObDetectorUserReportInfo &user_report_info,
  ObString &wait_sql)
{
  int ret = OB_ENTRY_NOT_EXIST;
  const ObIArray<ObString> &names = user_report_info.get_extra_columns_names();
  const ObIArray<ObString> &values = user_report_info.get_extra_columns_values();
  wait_sql.reset();
  for (int64_t i = 0; i < names.count() && i < values.count(); ++i) {
    if (names.at(i) == ObString("wait_sql")) {
      wait_sql = values.at(i);
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

static bool is_same_wait_sql_body_(const ObString &audit_query_sql, const ObString &wait_sql_with_trace)
{
  if (audit_query_sql.empty() || OB_ISNULL(audit_query_sql.ptr())
      || wait_sql_with_trace.empty() || OB_ISNULL(wait_sql_with_trace.ptr())) {
    return false;
  }
  const char *lhs_ptr = audit_query_sql.ptr();
  const int64_t lhs_len = audit_query_sql.length();
  const char *rhs_ptr = wait_sql_with_trace.ptr();
  const char *rhs_end = wait_sql_with_trace.ptr() + wait_sql_with_trace.length();
  const char *colon = static_cast<const char *>(memchr(rhs_ptr, ':', wait_sql_with_trace.length()));
  if (OB_NOT_NULL(colon) && colon + 1 < rhs_end) {
    rhs_ptr = colon + 1;
  }
  const int64_t rhs_len = rhs_end - rhs_ptr;
  return lhs_len == rhs_len && lhs_len > 0 && 0 == MEMCMP(lhs_ptr, rhs_ptr, lhs_len);
}

static int pick_hold_sql_from_merged_history_(
  const ObIArray<ObTuple<ObStringHolder, ObStringHolder, int64_t>> &sql_history,
  const transaction::ObTxSEQ &hold_seq,
  const ObString &wait_sql_to_exclude,
  ObStringHolder &holding_sql_request_time,
  ObStringHolder &holding_sql)
{
  int ret = OB_ENTRY_NOT_EXIST;
  const int64_t seq_upper_bound = hold_seq.is_valid() ? hold_seq.get_seq() : INT64_MAX;
  int64_t best_seq = -1;
  int64_t best_idx = -1;
  for (int64_t i = 0; i < sql_history.count(); ++i) {
    const int64_t seq = sql_history.at(i).template element<2>();
    const ObString &sql = sql_history.at(i).template element<1>().get_ob_string();
    if (sql.empty() || seq > seq_upper_bound) {
      continue;
    }
    if (!wait_sql_to_exclude.empty()
        && (sql == wait_sql_to_exclude || is_same_wait_sql_body_(sql, wait_sql_to_exclude))) {
      continue;
    }
    if (seq > best_seq) {
      best_seq = seq;
      best_idx = i;
    }
  }
  if (best_idx >= 0) {
    if (OB_FAIL(holding_sql_request_time.assign(sql_history.at(best_idx).template element<0>()))) {
    } else if (OB_FAIL(holding_sql.assign(sql_history.at(best_idx).template element<1>()))) {
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

static bool is_associated_zero_visitor_(const ObString &visitor)
{
  if (visitor.empty() || OB_ISNULL(visitor.ptr())) {
    return false;
  }
  static const ObString marker("(associated:0)");
  return OB_NOT_NULL(memmem(visitor.ptr(), visitor.length(), marker.ptr(), marker.length()));
}

static bool parse_trans_detector_visitor_(
  const ObDetectorInnerReportInfo &info,
  ObStringHolder &sess_id,
  ObStringHolder &trans_id)
{
  bool is_trans_detector = false;
  const ObString &visitor = info.get_user_report_info().get_resource_visitor();
  ObSEArray<ObStringHolder, 3> match_result;
  if (OB_SUCCESS == ObOccamRegex::regex_match(visitor, "\\{session_id:([0-9]+).*txid:([0-9]+)\\}", match_result)
      && match_result.count() == 3
      && OB_SUCCESS == sess_id.assign(match_result[1])
      && OB_SUCCESS == trans_id.assign(match_result[2])) {
    is_trans_detector = true;
  }
  return is_trans_detector;
}

static int get_last_trans_blocked_sql_seq(const ObSArray<ObDetectorInnerReportInfo> &collected_info_array,
                                   const int64_t current_idx,
                                   transaction::ObTxSEQ &this_tx_hold_lock_seq) {
  int ret = OB_ENTRY_NOT_EXIST;
  const int64_t count = collected_info_array.count();
  if (count <= 1 || current_idx < 0 || current_idx >= count) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "not expected collected info size", K(collected_info_array), K(current_idx));
  } else {
    int64_t prev_idx = (current_idx + count - 1) % count;
    for (int64_t i = 0; i < count; ++i) {
      if (prev_idx == current_idx) {
        break;
      }
      if (collected_info_array.at(prev_idx).get_user_report_info().get_module_name() == ObString("transaction")) {
        this_tx_hold_lock_seq = collected_info_array.at(prev_idx).get_user_report_info().get_blocked_seq();
        if (this_tx_hold_lock_seq.is_valid()) {
          ret = OB_SUCCESS;
          break;
        }
      }
      prev_idx = (prev_idx + count - 1) % count;
    }
  }
  return ret;
}

static int get_hold_seq_from_canonical_local_trans_(
  const ObSArray<ObDetectorInnerReportInfo> &collected_info_array,
  const int64_t current_idx,
  const ObStringHolder &trans_id,
  transaction::ObTxSEQ &holding_seq)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (current_idx < 0 || current_idx >= collected_info_array.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString &current_visitor = collected_info_array.at(current_idx).get_user_report_info().get_resource_visitor();
    if (!is_associated_zero_visitor_(current_visitor)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      for (int64_t idx = 0; idx < collected_info_array.count() && OB_SUCCESS != ret; ++idx) {
        if (idx == current_idx) {
          continue;
        }
        const ObDetectorInnerReportInfo &candidate_info = collected_info_array.at(idx);
        const ObString &candidate_visitor = candidate_info.get_user_report_info().get_resource_visitor();
        if (is_associated_zero_visitor_(candidate_visitor)) {
          continue;
        }
        ObStringHolder candidate_sess_id;
        ObStringHolder candidate_trans_id;
        if (!parse_trans_detector_visitor_(candidate_info, candidate_sess_id, candidate_trans_id)) {
          continue;
        } else if (candidate_trans_id.get_ob_string() != trans_id.get_ob_string()) {
          continue;
        } else if (OB_FAIL(get_last_trans_blocked_sql_seq(collected_info_array, idx, holding_seq))) {
          DETECT_LOG(INFO, "failed to fetch hold seq from canonical local trans node",
                     KR(ret), K(idx), K(current_idx), K(candidate_info));
        }
      }
    }
  }
  return ret;
}

void ObDeadLockDetectorMgr::get_trans_history_sql_from_audit_(const ObDeadLockCollectInfoMessage &collect_info_msg)
{
  int ret = OB_SUCCESS;
  const ObSArray<ObDetectorInnerReportInfo> &collected_info_array = collect_info_msg.get_collected_info();
  if (collected_info_array.empty() || collected_info_array.count() == 1) {
    DETECT_LOG(WARN, "not expected collected info size", K(collected_info_array), K(collect_info_msg));
  } else {
    for (int64_t idx = 0; idx < collected_info_array.count(); ++idx) {
      const ObDetectorInnerReportInfo &collected_info = collected_info_array[idx];
      ObDetectorUserReportInfo &user_report_info = const_cast<ObDetectorUserReportInfo &>(collected_info.get_user_report_info());
      ObStringHolder sess_id;
      ObStringHolder trans_id;
      transaction::ObTxSEQ holding_seq;
      if (is_trans_detector_(collected_info, sess_id, trans_id)) {
        int tmp_ret = get_last_trans_blocked_sql_seq(collected_info_array, idx, holding_seq);
        if (OB_SUCCESS != tmp_ret) {
          int tmp_ret2 = get_hold_seq_from_canonical_local_trans_(collected_info_array, idx, trans_id, holding_seq);
          if (OB_SUCCESS != tmp_ret2) {
            DETECT_LOG(WARN, "no prev trans blocked_seq for hold_sql", KR(tmp_ret), KR(tmp_ret2), K(idx), K(collected_info));
            continue;
          } else {
            DETECT_LOG(INFO, "resolved hold seq from canonical local trans node", K(idx), K(collected_info), K(holding_seq));
          }
        }
        if (!holding_seq.is_valid()) {
          DETECT_LOG(WARN, "resolved hold seq is invalid", K(idx), K(collected_info), K(holding_seq));
          continue;
        }
        ObString wait_sql_to_exclude;
        (void)get_wait_sql_from_report_info_(user_report_info, wait_sql_to_exclude);
        ObSEArray<ObAddr, 8> audit_exec_addrs;
        collect_cycle_audit_exec_addrs_(collected_info_array, idx, audit_exec_addrs);
        ObStringHolder holding_sql;
        ObStringHolder hold_sql_request_time;
        ObSharedGuard<char> holding_sql_guard;
        ObSharedGuard<char> hold_sql_request_time_guard;
        if (OB_SUCCESS != (tmp_ret = get_holding_sql(trans_id, holding_seq, hold_sql_request_time, holding_sql,
                                                     collected_info.get_tenant_id(), wait_sql_to_exclude, &audit_exec_addrs))) {
          DETECT_LOG(WARN, "fail to get holding sql", KR(tmp_ret), K(collected_info), K(collect_info_msg), K(holding_seq), K(idx));
        } else if (OB_SUCCESS != (tmp_ret = convert_string_holder_to_shared_guard_(holding_sql, holding_sql_guard))) {
          DETECT_LOG(WARN, "failed to convert string holder to shared guard", KR(tmp_ret), K(holding_sql));
        } else if (OB_SUCCESS != (tmp_ret = convert_string_holder_to_shared_guard_(hold_sql_request_time, hold_sql_request_time_guard))) {
          DETECT_LOG(WARN, "failed to convert string holder to shared guard", KR(tmp_ret), K(hold_sql_request_time));
        } else if (OB_SUCCESS != (tmp_ret = user_report_info.append_column("hold_sql_request_time", hold_sql_request_time_guard))) {
          DETECT_LOG(WARN, "fail to appened request time", KR(tmp_ret), K(collected_info), K(collect_info_msg));
        } else if (OB_SUCCESS != (tmp_ret = user_report_info.append_column("hold_sql", holding_sql_guard))) {
          DETECT_LOG(WARN, "fail to appened hold sql", KR(tmp_ret), K(collected_info), K(collect_info_msg));
        } else {
          DETECT_LOG(INFO, "get trans sql history done", KR(tmp_ret), K(collected_info), K(holding_sql));
        }
      }
    }
  }
}

bool ObDeadLockDetectorMgr::is_trans_detector_(const ObDetectorInnerReportInfo &info, ObStringHolder &sess_id, ObStringHolder &trans_id)
{
  int ret = OB_SUCCESS;
  bool is_trans_detector = false;
  const ObString &visitor = info.get_user_report_info().get_resource_visitor();
  ObSEArray<ObStringHolder, 3> match_result;
  if (OB_FAIL(ObOccamRegex::regex_match(visitor, "\\{session_id:([0-9]+).*txid:([0-9]+)\\}", match_result))) {
    DETECT_LOG(WARN, "fail to match regex", KR(ret), K(info), K(visitor), K(match_result));
  } else if (match_result.count() != 3) {
    DETECT_LOG(INFO, "maybe not trans detector", KR(ret), K(info), K(visitor), K(match_result));
  } else if (OB_FAIL(sess_id.assign(match_result[1]))) {
    DETECT_LOG(WARN, "fail to assign sess_id", KR(ret), K(info), K(visitor), K(match_result));
  } else if (OB_FAIL(trans_id.assign(match_result[2]))) {
    DETECT_LOG(WARN, "fail to assign trans_id", KR(ret), K(info), K(visitor), K(match_result));
  } else {
    is_trans_detector = true;
  }
  return is_trans_detector;
}

int ObDeadLockDetectorMgr::get_sql_history_(const uint64_t query_tenant_id,
                                            const ObStringHolder &trans_id,
                                            ObIArray<ObTuple<ObStringHolder, ObStringHolder, int64_t>> &sql_hisory,
                                            const ObAddr *exec_sql_addr)
{
  int ret = OB_SUCCESS;
  constexpr int64_t BUFFER_SIZE = 512;
  ObCStringHelper helper;
  char condition_buffer[BUFFER_SIZE] = {0};
  char svr_ip[MAX_IP_ADDR_LENGTH] = {0};
  const uint64_t tenant_id = (OB_INVALID_ID == query_tenant_id) ? MTL_ID() : query_tenant_id;
  if (OB_NOT_NULL(exec_sql_addr) && exec_sql_addr->is_valid() && exec_sql_addr->get_port() > 0
      && exec_sql_addr->ip_to_string(svr_ip, MAX_IP_ADDR_LENGTH)) {
    if (OB_FAIL(databuff_printf(condition_buffer,
                               BUFFER_SIZE,
                               "where tenant_id = %lu and transaction_id = %s "
                               "and svr_ip = '%s' and svr_port = %d "
                               "and is_executor_rpc = 0 and is_inner_sql = 0 order by request_time limit 128 ",
                               tenant_id, helper.convert(trans_id), svr_ip, exec_sql_addr->get_port()))) {
      DETECT_LOG(WARN, "fail to construct where condition", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
    }
  } else if (OB_FAIL(databuff_printf(condition_buffer,
                                     BUFFER_SIZE,
                                     "where tenant_id = %lu and transaction_id = %s "
                                     "and is_executor_rpc = 0 and is_inner_sql = 0 order by request_time limit 128 ",
                                     tenant_id, helper.convert(trans_id)))) {
    DETECT_LOG(WARN, "fail to construct where condition", KR(ret), K(tenant_id), K(trans_id));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(exec_sql_addr) && exec_sql_addr->is_valid()
             && *exec_sql_addr != GCTX.self_addr()) {
    ObSqlString sql;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      DETECT_LOG(WARN, "GCTX.sql_proxy_ is null", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
    } else if (OB_FAIL(sql.append_fmt("SELECT CAST(USEC_TO_TIME(request_time) AS CHAR(32)), query_sql, seq_num "
                                      "FROM %s %s",
                                      share::OB_ALL_VIRTUAL_SQL_AUDIT_TNAME,
                                      condition_buffer))) {
      DETECT_LOG(WARN, "fail to construct sql", KR(ret), K(tenant_id), K(trans_id), K(condition_buffer));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr(), exec_sql_addr))) {
          DETECT_LOG(WARN, "fail to read sql audit remotely", KR(ret), K(tenant_id), K(trans_id), K(sql), KPC(exec_sql_addr));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          DETECT_LOG(WARN, "fail to get sql result", KR(ret), K(tenant_id), K(trans_id), K(sql), KPC(exec_sql_addr));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                DETECT_LOG(WARN, "fail to iterate sql audit result", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              }
            } else {
              ObTuple<ObStringHolder, ObStringHolder, int64_t> row;
              ObString request_time;
              ObString query_sql;
              int64_t seq_num = 0;
              if (OB_FAIL(result->get_varchar(static_cast<int64_t>(0), request_time))) {
                DETECT_LOG(WARN, "fail to get request_time", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              } else if (OB_FAIL(row.template element<0>().assign(request_time))) {
                DETECT_LOG(WARN, "fail to assign request_time", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              } else if (OB_FAIL(result->get_varchar(static_cast<int64_t>(1), query_sql))) {
                DETECT_LOG(WARN, "fail to get query_sql", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              } else if (OB_FAIL(row.template element<1>().assign(query_sql))) {
                DETECT_LOG(WARN, "fail to assign query_sql", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              } else if (OB_FAIL(result->get_int(static_cast<int64_t>(2), seq_num))) {
                DETECT_LOG(WARN, "fail to get seq_num", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
              } else {
                row.template element<2>() = seq_num;
                if (OB_FAIL(sql_hisory.push_back(row))) {
                  DETECT_LOG(WARN, "fail to push sql history row", KR(ret), K(tenant_id), K(trans_id), KPC(exec_sql_addr));
                }
              }
            }
          }
        }
      }
    }
  } else if (OB_FAIL(ObTableAccessHelper::read_multi_row(OB_SYS_TENANT_ID,
                                                         {"CAST(USEC_TO_TIME(request_time) AS CHAR(32))", "query_sql", "seq_num"},
                                                         share::OB_ALL_VIRTUAL_SQL_AUDIT_TNAME,
                                                         condition_buffer,
                                                         sql_hisory))) {
    DETECT_LOG(WARN, "fail to read multi row", KR(ret), K(tenant_id), K(trans_id), K(condition_buffer));
  }
  return ret;
}

int ObDeadLockDetectorMgr::get_holding_sql(const ObStringHolder &trans_id,
                                           const transaction::ObTxSEQ &hold_seq,
                                           ObStringHolder &holding_sql_request_time,
                                           ObStringHolder &holding_sql,
                                           const uint64_t query_tenant_id,
                                           const ObString &wait_sql_to_exclude,
                                           const ObIArray<ObAddr> *audit_exec_addrs)
{
  #define PRINT_WRAPPER KR(ret), K(trans_id), K(hold_seq), K(query_tenant_id), K(wait_sql_to_exclude), KPC(audit_exec_addrs), K(holding_sql_request_time), K(holding_sql)
  int ret = OB_SUCCESS;
  char *sql_translate_buffer = nullptr;
  constexpr int64_t BUFFER_SIZE = 1_MB;
  const uint64_t tenant_id = (OB_INVALID_ID == query_tenant_id) ? MTL_ID() : query_tenant_id;
  ObArray<ObTuple<ObStringHolder, ObStringHolder, int64_t>> merged_sql_history;
  if (OB_NOT_NULL(audit_exec_addrs) && audit_exec_addrs->count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < audit_exec_addrs->count(); ++i) {
      const ObAddr &audit_addr = audit_exec_addrs->at(i);
      ObArray<ObTuple<ObStringHolder, ObStringHolder, int64_t>> part_sql_history;
      const ObAddr *audit_addr_ptr = audit_addr.is_valid() ? &audit_addr : nullptr;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(get_sql_history_(tenant_id, trans_id, part_sql_history, audit_addr_ptr))) {
        DETECT_LOG(WARN, "get sql history from one audit addr failed", KR(tmp_ret), K(trans_id), K(audit_addr));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < part_sql_history.count(); ++j) {
          if (OB_FAIL(merged_sql_history.push_back(part_sql_history.at(j)))) {
            DETECT_LOG(WARN, "merge sql audit history failed", KR(ret), K(trans_id), K(audit_addr));
            break;
          }
        }
      }
    }
  } else {
    // Fallback: cluster-wide audit scan (tenant_id + transaction_id), same as base when no svr filter.
    if (OB_FAIL(get_sql_history_(tenant_id, trans_id, merged_sql_history, nullptr))) {
      DETECT_LOG(WARN, "fail to get sql history", KR(ret), K(tenant_id), K(trans_id));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    if (merged_sql_history.count() <= 0) {
      ret = OB_ENTRY_NOT_EXIST;
      DETECT_LOG(WARN, "no sql audit history for hold_sql", PRINT_WRAPPER);
    } else if (OB_FAIL(pick_hold_sql_from_merged_history_(merged_sql_history, hold_seq, wait_sql_to_exclude,
                                                          holding_sql_request_time, holding_sql))) {
      DETECT_LOG(WARN, "fail to pick hold sql from merged audit", PRINT_WRAPPER);
    } else if (OB_ISNULL(sql_translate_buffer = (char *)mtl_malloc(BUFFER_SIZE, "DETECT.sql"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "fail to alloc heap memory", PRINT_WRAPPER);
    } else {
      transaction::ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(
          holding_sql.get_ob_string().ptr(),
          holding_sql.get_ob_string().length(),
          sql_translate_buffer,
          BUFFER_SIZE);
      if (OB_FAIL(holding_sql.assign(ObString(sql_translate_buffer)))) {
        DETECT_LOG(WARN, "failed to translate sql", PRINT_WRAPPER);
      } else {
        DETECT_LOG(INFO, "get holding sql from merged audit", PRINT_WRAPPER, K(holding_sql),
                   "merged_row_cnt", merged_sql_history.count());
      }
    }
  }
  // release dynamic buffer
  if (OB_NOT_NULL(sql_translate_buffer)) {
    mtl_free(sql_translate_buffer);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObDeadLockDetectorMgr::convert_string_holder_to_shared_guard_(const ObStringHolder &holder,
                                                                  ObSharedGuard<char> &shared_guard)
{
  int ret = OB_SUCCESS;
  int64_t str_len = holder.get_ob_string().length();// not including '\0'
  char *dynamic_buffer = (char *)mtl_malloc(str_len + 1/*add '\0'*/, "DeadLockSql");
  if (OB_ISNULL(dynamic_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    DETECT_LOG(WARN, "failed to alloc memory", K(str_len), K(holder));
  } else {
    memcpy(dynamic_buffer, holder.get_ob_string().ptr(), str_len);
    dynamic_buffer[str_len] = '\0';
    if (OB_FAIL(shared_guard.assign(dynamic_buffer, MtlDeleter()))) {
      DETECT_LOG(WARN, "failed to construct shared guard", K(str_len), K(holder));
      mtl_free(dynamic_buffer);
    } else {
      DETECT_LOG(TRACE, "success to convert string holder to shared guard", K(str_len), K(holder));
    }
  }
  return ret;
}

}// detector
}// share
}// oceanbase
