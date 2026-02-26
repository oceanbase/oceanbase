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

#ifndef OCEANBASE_SENSITIVE_TEST_MOCK_OBJECT_STORAGE_
#define OCEANBASE_SENSITIVE_TEST_MOCK_OBJECT_STORAGE_

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#include <unordered_map>
#include <thread>
#define private public
#define protected public
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "lib/random/ob_random.h"                       // ObRandom
#include "lib/queue/ob_link_queue.h"                    // ObLinkQueue
#include "lib/lock/ob_scond.h"                          // SimpleCond
#include "lib/lock/ob_latch.h"
#include "storage/incremental/atomic_protocol/ob_atomic_file_mgr.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{

namespace storage
{
using namespace blocksstable;
using std::unordered_map;

#define OB_MOCK_OBJ_SRORAGE (storage::ObMockObjectManager::get_instance())

enum class ObMockSSReqType : uint8_t
{
  INVAL_REQ = 0,
  WRITE_REQ = 1,
  READ_REQ = 2,
  MAX_REQ
};

typedef int64_t ObInjectErrKey;

// struct ObInjectErrKey
// {
// public:
//   ObInjectErrKey() : inject_point_(0) {};
//   ~ObInjectErrKey() {}
//   ObInjectErrKey( ObMockSSReqType inject_req_type,
//                  int64_t inject_point)
//     : inject_req_type_(inject_req_type),
//       inject_point_(inject_point) {}
//   int operator==(const ObInjectErrKey &rhs) const
//   {
//     return inject_req_type_ == rhs.inject_req_type_
//         && inject_point_ == rhs.inject_point_;
//   }
//   ObMockSSReqType inject_req_type_;
//   int64_t inject_point_;
//   VIRTUAL_TO_STRING_KV(K_(inject_req_type),
//                        K_(inject_point));
// };

// struct ObInjectErrKeyHash {
//     std::size_t operator()(const ObInjectErrKey& key) const {
//       // 使用内置类型的哈希函数来计算哈希值
//       // size_t hashX = std::hash<ObStorageObjectType>()(key.obj_type_);
//       size_t hashY = std::hash<ObMockSSReqType>()(key.inject_req_type_);
//       size_t hashZ = std::hash<int64_t>()(key.inject_point_);
//       // 将 x, y 和 z 的哈希值合并起来
//       return hashY ^ (hashZ << 1); // 使用不同的移位量以减少碰撞
//     }
// };

struct ObInjectedErr
{
public:
  ObInjectedErr() : v_(0), need_wakeup_err_(nullptr) { is_succ_ = true;}
  ~ObInjectedErr() {}
  union {
    uint64_t v_;
    struct {
      // the execution of the request occurs out of order
      bool is_shuffled_ : 1;
      // the request execution has returned timeout
      bool is_timeout_ : 1;
      // whether the request executed successfully
      bool is_succ_ : 1; // default true
      // sswriter change before processing this request
      bool is_sswriter_change_ : 1;
      // need wakeup waiting request before doing this request
      bool is_need_wake_waiting_ : 1;
    };
  };
  // need wait until this flag set to true
  bool is_need_waiting_ = false;
  void set_need_wait()
  {
    ATOMIC_SET(&is_need_waiting_, true);
  }
  void reset_need_wait()
  {
    ATOMIC_SET(&is_need_waiting_, false);
  }
  bool get_need_wait()
  {
    return  ATOMIC_LOAD(&is_need_waiting_);
  }
  // int64_t inject_point_;
  common::ObLatch lock_;
  ObInjectedErr *need_wakeup_err_;
  VIRTUAL_TO_STRING_KV(K(is_shuffled_),
                       K(is_timeout_),
                       K(is_succ_),
                       K(is_sswriter_change_),
                       K(is_need_wake_waiting_),
                       K(is_need_waiting_),
                      //  KP(need_wakeup_err_),
                       K(is_shuffled_));
};

struct ObMockSSReq : public ObLink
{
  ObMockSSReq(const ObMockSSReqType type, ObStorageObjectHandle *object_handle)
    : type_(type),
      object_handle_(object_handle),
      injected_err_(nullptr),
      has_done_(false) {
        init_ts_ = ObTimeUtility::current_time();
      }
  ObMockSSReqType type_;
  ObStorageObjectHandle *object_handle_;
  ObInjectedErr *injected_err_;
  bool has_done_;
  int ret_;
  int64_t init_ts_;
  VIRTUAL_TO_STRING_KV(K_(type),
                      //  KPC_(object_handle),
                      //  KP_(injected_err),
                       K_(has_done),
                       K_(ret));
};

struct ObMockSSWriteReq : public ObMockSSReq
{
public:
  ObMockSSWriteReq(const ObStorageObjectOpt *opt,
                   const ObStorageObjectWriteInfo *write_info,
                   ObStorageObjectHandle *object_handle)
    : ObMockSSReq(ObMockSSReqType::WRITE_REQ, object_handle),
      opt_(opt),
      write_info_(write_info) {
      }
  const ObStorageObjectOpt *opt_;
  const ObStorageObjectWriteInfo *write_info_;
  // INHERIT_TO_STRING_KV("ObMockSSReq", ObMockSSReq);
};

struct ObMockSSReadReq : public ObMockSSReq
{
public:
  ObMockSSReadReq(const ObStorageObjectReadInfo *read_info,
                  ObStorageObjectHandle *object_handle)
    : ObMockSSReq(ObMockSSReqType::READ_REQ, object_handle),
      read_info_(read_info) {}
  const ObStorageObjectReadInfo *read_info_;
  // INHERIT_TO_STRING_KV("ObMockSSReq", ObMockSSReq, KPC_(read_info));
};

class ObMockObjectManager
{
public:
  static unordered_map<ObInjectErrKey, ObInjectedErr*> inject_err_map;
  ObMockObjectManager()
    : is_sleeping_(false),
      is_hanging_up_(false),
      process_point_(0),
      process_point_for_write_(true), tenant_(nullptr),
      stop_(false),
      alloc_cnt_(0),
      free_cnt_(0),
      read_cnt_(0),
      write_cnt_(0) {
        start();
        auto req_consumer = [&] (ObMockObjectManager *mock_obj_mgr) {
          while (mock_obj_mgr->tenant_ == nullptr && !mock_obj_mgr->has_set_stop()) {
            ob_usleep(100);
          }
          ObTenantEnv::set_tenant(mock_obj_mgr->tenant_);
          LOG_INFO("req consumer run!", KPC(mock_obj_mgr), KP(MTL(ObTenantFileManager *)));
          while (!mock_obj_mgr->has_set_stop() || req_queue_.size() > 0) {
            ObLink *e = NULL;
            mock_obj_mgr->req_queue_.pop(e);
            if (e) {
              process_ss_req_(static_cast<ObMockSSReq*>(e));
            } else if (ATOMIC_BCAS(&mock_obj_mgr->is_sleeping_, false, true)) {
              LOG_INFO("to sleeping", KPC(mock_obj_mgr));
              auto key = mock_obj_mgr->sleep_cond_.get_key();
              mock_obj_mgr->sleep_cond_.wait(key, 2000);
              LOG_INFO("wakeup", KPC(mock_obj_mgr));
            }
          }
          ObTenantEnv::set_tenant(NULL);
          LOG_INFO("req consumer stop!", KPC(mock_obj_mgr),
            KP(MTL(ObTenantFileManager *)), K(mock_obj_mgr->alloc_cnt_), K(mock_obj_mgr->free_cnt_));
        };
        std::thread th(req_consumer, this);
        ths_.push_back(std::move(th));
    }
  ~ObMockObjectManager() {
    stop();
    LOG_INFO("wait threads end");
    for (auto &th : ths_) {
      th.join();
    }
  }
  static int mtl_init(ObMockObjectManager* &obj_mgr);
  int init();
  int start();
  void stop();
  bool has_set_stop() const { return ATOMIC_LOAD(&stop_); }
  void destroy();
  void run1();
  static ObMockObjectManager& get_instance()
  {
    static ObMockObjectManager mgr;
    return mgr;
  }
public:
  void inject_err_to_object_type(ObInjectErrKey err_key,
                                 ObInjectedErr *err);
  // // shuffle request in the queue
  // int shuffle_requests();
  // // make the request timeout, is_succ means whether it executes successfully
  // int inject_request_timeout(bool is_succ);
public:
  // push request to msg_queue
  int read_object(
      const ObStorageObjectReadInfo &read_info,
      ObStorageObjectHandle &object_handle);
  int write_object(
      const ObStorageObjectOpt &opt,
      ObStorageObjectWriteInfo &write_info,
      ObStorageObjectHandle &object_handle);

  VIRTUAL_TO_STRING_KV(K_(is_sleeping),
                       K_(is_hanging_up));
private:
  // consume ss req
  void process_ss_req_(ObMockSSReq *req);
  int push_to_queue_(ObMockSSReq *req);
  // handle req and push to queue
  int inject_err_(ObMockSSReq *req);
  // handle req and push to queue
  int handle_and_push_req_(ObMockSSReq *req, bool &need_wait);
  void inc_alloc_cnt() { ATOMIC_INC(&alloc_cnt_); }
  void inc_free_cnt() { ATOMIC_INC(&free_cnt_); }
  DISALLOW_COPY_AND_ASSIGN(ObMockObjectManager);

private:
  static ObLinkQueue req_queue_;
  common::SimpleCond sleep_cond_;
  common::SimpleCond hang_cond_;
  bool is_sleeping_;
  bool is_hanging_up_;
  common::ObLatch map_lock_;
  int64_t process_point_;
  bool process_point_for_write_;
  std::vector<std::thread> ths_;
  ObTenantBase* tenant_;
  bool stop_;
  int64_t alloc_cnt_;
  int64_t free_cnt_;
  int64_t read_cnt_;
  int64_t write_cnt_;
};
} // storage

void mock_switch_sswriter();
}  // namespace oceanbase
#endif