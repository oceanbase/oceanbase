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

#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "observer/ob_srv_network_frame.h"
#include "lib/net/ob_addr.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_parameters.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_node.h"
#include "share/deadlock/ob_lcl_scheme/ob_lcl_message.h"
#include "lib/function/ob_function.h"
#include "common/ob_clock_generator.h"
#include "share/deadlock/test/test_key.h"
#include <string>
#include <iostream>
#include <thread>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <numeric>
#include "storage/tx/ob_trans_define.h"
#include "share/deadlock/ob_deadlock_detector_rpc.h"
#include "share/deadlock/mock_deadlock_rpc.h"
#include "storage/memtable/ob_memtable_context.h"

using namespace oceanbase::obrpc;
using namespace std;

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace share::detector;
using namespace share;
using namespace std;
static ObDetectorUserReportInfo user_report_info;

// 用户自定义的operation操作
class TestOperation {
public:
  TestOperation(uint64_t hash) : hash_(hash) {
    ATOMIC_AAF(&alive_count, 1);
    ObSharedGuard<char> ptr;
    ptr.assign((char*)"test", [](char*){});
    user_report_info.set_module_name(ptr);
    ptr.assign((char*)"a", [](char*){});
    user_report_info.set_visitor(ptr);
    ptr.assign((char*)"b", [](char*){});
    user_report_info.set_resource(ptr);
  }
  TestOperation(const TestOperation &rhs) : hash_(rhs.hash_), key_(rhs.key_) {
    ATOMIC_AAF(&alive_count, 1);
    ObSharedGuard<char> ptr;
    ptr.assign((char*)"test", [](char*){});
    user_report_info.set_module_name(ptr);
    ptr.assign((char*)"a", [](char*){});
    user_report_info.set_visitor(ptr);
    ptr.assign((char*)"b", [](char*){});
    user_report_info.set_resource(ptr);
  }
  TestOperation(const ObDeadLockTestIntKey &key) :
  key_(key.get_value()) {
    ATOMIC_AAF(&alive_count, 1);
    ObSharedGuard<char> ptr;
    ptr.assign((char*)"test", [](char*){});
    user_report_info.set_module_name(ptr);
    ptr.assign((char*)"a", [](char*){});
    user_report_info.set_visitor(ptr);
    ptr.assign((char*)"a", [](char*){});
    user_report_info.set_resource(ptr);
  }
  ~TestOperation() { ATOMIC_AAF(&alive_count, -1); }
  TO_STRING_KV(K(key_));
  uint64_t hash_;
public:
  int operator()(const common::ObIArray<ObDetectorInnerReportInfo> &collected_info, const int64_t self_index) {
    UNUSED(collected_info);
    int64_t interval = chrono::duration_cast<chrono::milliseconds>(chrono::high_resolution_clock::now() - loop_occurd_time).count();
    DETECT_LOG(INFO, "detect delay time", K(interval));
    v_record.push_back(interval);
    ObDetectorPriority lowest_priority(PRIORITY_RANGE::EXTREMELY_HIGH, UINT64_MAX);
    int64_t lowest_priority_idx = -1;
    for (int64_t i = 0; i < collected_info.count(); ++i) {
      if (collected_info.at(i).get_priority() < lowest_priority) {
        lowest_priority_idx = i;
        lowest_priority = collected_info.at(i).get_priority();
      }
    }
    v_record_is_lowest_priority.push_back(self_index == lowest_priority_idx);
    v_killed_node.push_back(key_.get_value());
    if (key_.get_value() != -1) {
      MTL(ObDeadLockDetectorMgr*)->unregister_key(key_);
    }
    std::cout << "my hash is:" << hash_ << std::endl;
    return OB_SUCCESS;
  }
  ObDeadLockTestIntKey key_;
  static vector<int64_t> v_record;
  static vector<bool> v_record_is_lowest_priority;
  static int64_t alive_count;
  static vector<int> v_killed_node;
  static decltype(chrono::high_resolution_clock::now()) loop_occurd_time;
};

// 真实使用过程中注册和搭建依赖关系的时间点是随机的，通过随机休眠模拟使用场景
auto collect_callback = [](ObDetectorUserReportInfo& arg){ return arg.assign(user_report_info); };

#define JUDGE_RECORDER(v1,v2,v3,v4,v5,v6,v7,v8)\
ASSERT_EQ(recorder.function_default_construct_time, v1);\
ASSERT_EQ(recorder.function_copy_construct_time, v2);\
ASSERT_EQ(recorder.function_move_construct_time, v3);\
ASSERT_EQ(recorder.function_general_construct_time, v4);\
ASSERT_EQ(recorder.function_copy_assign_time, v5);\
ASSERT_EQ(recorder.function_move_assign_time, v6);\
ASSERT_EQ(recorder.function_general_assign_time, v7);\
ASSERT_EQ(recorder.derived_construct_time, v8);\
recorder.reset();

function::DebugRecorder &recorder = function::DebugRecorder::get_instance();

ObDeadLockDetectorMgr *mgr;

class TestObDeadLockDetector : public ::testing::Test {
public:
  TestObDeadLockDetector() :
    case_num_(0), ctx(1) {}
  ~TestObDeadLockDetector() {}
  virtual void SetUp() {
    share::ObTenantEnv::get_tenant_local()->id_ = 1;
    auto &gctx = GCTX;
    gctx.net_frame_ = new observer::ObSrvNetworkFrame(gctx);
    gctx.net_frame_->rpc_transport_ = reinterpret_cast<rpc::frame::ObReqTransport*>(0x123456);
    gctx.self_addr_seq_.server_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 1234);
    mgr = new ObDeadLockDetectorMgr();
    DETECT_LOG(INFO, "print mgr", KP(mgr), KP(&mgr->inner_alloc_handle_.inner_factory_.release_count_));
    ctx.set(mgr);
    ObTenantEnv::set_tenant(&ctx);
    mgr->init();
    static int case_num = 0;
    ++case_num;
    cout << "\n<<<<<<<<<<<<<<<<<<<<" << "start case" << case_num << ">>>>>>>>>>>>>>>>>>>>" << endl;
    // do not use rpc, will core dump
    // ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->init());
    oceancase::unittest::MockDeadLockRpc *rpc = (oceancase::unittest::MockDeadLockRpc *)ob_malloc(sizeof(oceancase::unittest::MockDeadLockRpc),
                                                                                                  ObNewModIds::TEST);
    rpc = new (rpc) oceancase::unittest::MockDeadLockRpc();
    MTL(ObDeadLockDetectorMgr*)->rpc_ = rpc;

    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->start());
    ObServerConfig::get_instance()._lcl_op_interval = 10000;
    std::this_thread::sleep_for(chrono::seconds(3));
    TestOperation::v_killed_node.clear();
  }
  virtual void TearDown() {
    MTL(ObDeadLockDetectorMgr*)->stop();
    MTL(ObDeadLockDetectorMgr*)->wait();
    MTL(ObDeadLockDetectorMgr*)->proxy_ = nullptr;
    MTL(ObDeadLockDetectorMgr*)->rpc_ = nullptr;
    MTL(ObDeadLockDetectorMgr*)->destroy();
    DETECT_LOG(INFO, "print mgr", KP(mgr), KP(&mgr->inner_alloc_handle_.inner_factory_.release_count_));
    int64_t release_count_from_mtl = MTL(ObDeadLockDetectorMgr*)->get_detector_release_count();
    int64_t release_count_from_obj = mgr->get_detector_release_count();
    DETECT_LOG(INFO, "print count", K(release_count_from_mtl), K(release_count_from_obj));
    ASSERT_EQ(MTL(ObDeadLockDetectorMgr*)->get_detector_create_count(), MTL(ObDeadLockDetectorMgr*)->get_detector_release_count());
    ASSERT_EQ(0, TestOperation::alive_count);// 预期用户创建的operation对象销毁
    delete MTL(ObDeadLockDetectorMgr*);
  }
  template <typename T>
  ObIDeadLockDetector *get_detector_ptr(const T &key) {
    ObDeadLockDetectorMgr::DetectorRefGuard guard;
    UserBinaryKey binary_key;
    binary_key.set_user_key(key);
    MTL(ObDeadLockDetectorMgr*)->get_detector_(binary_key, guard);
    return guard.get_detector();
  }
private:
  int case_num_ = 0;
  share::ObTenantBase ctx;
};

vector<int64_t> TestOperation::v_record;
vector<bool> TestOperation::v_record_is_lowest_priority;
int64_t TestOperation::alive_count = 0;
vector<int> TestOperation::v_killed_node;
decltype(chrono::high_resolution_clock::now()) TestOperation::loop_occurd_time = chrono::high_resolution_clock::now();

TEST_F(TestObDeadLockDetector, test_ObDetectorUserReportInfo) {
  char a[5] = {'1', '2', '3', '4', '\0'};
  {
    ObDetectorUserReportInfo info;
    int ret = OB_SUCCESS;
    char *buffer = nullptr;
    if (OB_UNLIKELY(nullptr == (buffer = (char*)ob_malloc(128, "deadlockCB")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      transaction::ObTransID self_trans_id;
      ObSharedGuard<char> temp_uniqe_guard;
      int step = 0;
      (void) self_trans_id.to_string(buffer, 128);
      char *node_key_buffer = a;
      if (++step && OB_FAIL(temp_uniqe_guard.assign((char*)"transaction", [](char*){}))) {
      } else if (++step && OB_FAIL(info.set_module_name(temp_uniqe_guard))) {
      } else if (++step && OB_FAIL(temp_uniqe_guard.assign(buffer, [](char* buffer){ ob_free(buffer); DETECT_LOG(INFO, "visitor str destory", K(lbt()));}))) {
      } else if (++step && OB_FAIL(info.set_visitor(temp_uniqe_guard))) {
      } else if (++step && OB_FAIL(temp_uniqe_guard.assign(node_key_buffer, [](char*ptr){ ptr[0] = '2'; DETECT_LOG(INFO, "resource str destory", K(lbt())); }))) {
      } else if (++step && OB_FAIL(info.set_resource(temp_uniqe_guard))) {
      } else if (++step && OB_FAIL(info.set_extra_info("current sql", "1"))) {
      } else {}
      if (OB_FAIL(ret)) {
        DETECT_LOG(WARN, "get string failed in deadlock", KR(ret), K(step));
      }
    }
    ASSERT_EQ(info.get_extra_columns_names().size(), 1);
    ASSERT_EQ(info.get_extra_columns_values().size(), 1);
    DETECT_LOG(INFO, "print info", K(info));
  }
  ASSERT_EQ(a[0], '2');
}

// 正常的注册key和注销key的流程
// 测试重点：
// 1，用户registrer_key后，对应的detector节点会创建，可以拿到对应的resource_id。
// 2，用户ungister_key之后，对应的detector节点预期会被销毁。
// 3，detector节点销毁之后，预期用户创建的operation对象也会被销毁。
TEST_F(TestObDeadLockDetector, register_ungister_key) {
  // 由外层来构造自己的operation对象
  TestOperation* op = new TestOperation(7710038271457258879UL);
  ASSERT_EQ(1, TestOperation::alive_count);// 用户创建了一个operation对象
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(1), *op, collect_callback, 1));// 注册key
  //JUDGE_RECORDER()
  auto call_back = get_detector_ptr(ObDeadLockTestIntKey(1));
  ASSERT_EQ(1, MTL(ObDeadLockDetectorMgr*)->get_detector_create_count());// 预期同步创建出了一个detector对象
  ObIDeadLockDetector *ptr = get_detector_ptr(ObDeadLockTestIntKey(1));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->unregister_key(ObDeadLockTestIntKey(1)));// 注销key
  delete op;
}

// 异常注册和注销key时的处理
// 测试重点：
// 1，
// 2，同一类型的重复的key的注册应当失败。
// 3，一个key在注册并且注销后，应当可以重新注册
// 4，一个key在未注册的情况下，注销操作应当失败
// 5，一个已经注册的key在已经注销的情况下，二次注销应当失败
// 6, 在ObDeadLockDetectorMgr销毁的时候，还存在detector没有销毁，预期Mgr销毁后，所有创建的detector都销毁
TEST_F(TestObDeadLockDetector, register_ungister_in_wrong_way) {
  // do not use rpc, will core dump
  TestOperation *op = new TestOperation(7710038271457258879UL);
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(12), *op, collect_callback, 1));
  //auto &call_back = ((ObLCLNode*)(get_detector_ptr(ObDeadLockTestIntKey(12))))->get_detect_callback_();
  //TestOperation &op_cp = static_cast<ObFunction<int(const common::ObIArray<ObDetectorInnerReportInfo> &, const int64_t)>::Derived<TestOperation>*>(call_back.get_func_ptr())->func_;
  DETECT_LOG(INFO, "1");
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(12), *op, collect_callback, 1));// 2，同一类型的重复的key的注册应当失败
  DETECT_LOG(INFO, "2");
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->unregister_key(ObDeadLockTestIntKey(12)));
  DETECT_LOG(INFO, "3");
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(12), *op, collect_callback, 1));// 3，一个key在注册并且注销后，应当可以重新注册
  DETECT_LOG(INFO, "4");
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->unregister_key(ObDeadLockTestIntKey(13)));// 4，一个key在未注册的情况下，注销操作应当失败
  DETECT_LOG(INFO, "5");
  cout << MTL(ObDeadLockDetectorMgr*)->get_detector_create_count() << " " << MTL(ObDeadLockDetectorMgr*)->get_detector_release_count();
  std::this_thread::sleep_for(chrono::milliseconds(100));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->unregister_key(ObDeadLockTestIntKey(12)));
  DETECT_LOG(INFO, "6");
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->unregister_key(ObDeadLockTestIntKey(12)));// 5，一个已经注册的key在已经注销的情况下，二次注销应当失败
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(12), *op, collect_callback, 1));// 6, 在ObDeadLockDetectorMgr销毁的时候，还存在detector没有销毁，预期Mgr销毁后，所有创建的detector都销毁
  std::this_thread::sleep_for(chrono::microseconds(PERIOD * 2));
  delete op;
}

// block和activate的流程
// 测试重点：
// 1，block某个已经存在的key可以成功
// 2，block一个已经block过的key返回失败
// 3，可以block多个不同的已经存在的key
// 4，block某个不存在的key返回失败
// 5，activate某个block过的key可以成功
// 6，重复activate一个key返回失败
// 7，activate一个从未注册过的key返回失败
// 8，activate一个不在block列表里但是已经注册过的key返回失败
TEST_F(TestObDeadLockDetector, block_and_activate) {
  TestOperation *op = new TestOperation(7710038271457258879UL);
  // 注册第一个key
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(1), *op, collect_callback, 1));
  // 注册第二个key
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(2), *op, collect_callback, 1));
  // 注册第三个key
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestDoubleKey(3.0), *op, collect_callback, 1));
  // 注册第四个key
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestDoubleKey(4.0), *op, collect_callback, 1));
  // 为两个key搭建依赖关系
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(2)));// 1，block某个已经存在的key可以成功
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(2)));// 2，block一个已经block过的key返回失败
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestDoubleKey(3.0)));// 3，可以block多个不同的已经存在的key
  // ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestDoubleKey(5.0)));// 4，block某个不存在的key返回失败
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->activate(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(2)));// 5，activate某个block过的key可以成功
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->activate(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(2)));// 6，重复activate一个key返回失败
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->activate(ObDeadLockTestIntKey(1), ObDeadLockTestDoubleKey(5.0)));// 7，activate一个从未注册过的key返回失败
  ASSERT_NE(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->activate(ObDeadLockTestIntKey(1), ObDeadLockTestDoubleKey(4.0)));// 8，activate一个不在block列表里但是已经注册过的key返回失败
  delete op;
}

// 测试死锁的探测功能
// 测试重点：
// 1，未形成死锁时不会探测到死锁
// 2，形成死锁后在一定时间内可以探测到死锁，探测时间不超过(节点数+1)*Transmit间隔
TEST_F(TestObDeadLockDetector, dead_lock) {
  int loop_times = 5;
  std::srand(std::time(nullptr));
  for (int i = 0; i < loop_times; ++i) {// 进行多次测试，看探测时延的最大、最小、平均值
    int index = i * 5;
    std::this_thread::sleep_for(chrono::milliseconds(std::rand() % 300));
    // 注册第一个key
    // TestOperation *op1 = new TestOperation(ObDeadLockTestIntKey(index + 1));
    // ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(index + 1), *op1, collect_callback, 1, std::rand() % 100));
    // 注册第二个key
    TestOperation *op2 = new TestOperation(ObDeadLockTestIntKey(index + 2));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(index + 2), *op2, collect_callback, 1, std::rand() % 100));
    // 注册第三个key
    TestOperation *op3 = new TestOperation(ObDeadLockTestIntKey(index + 3));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(index + 3), *op3, collect_callback, 1, std::rand() % 100));
    // 注册第四个key
    TestOperation *op4 = new TestOperation(ObDeadLockTestIntKey(index + 4));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(index + 4), *op4, collect_callback, 1, std::rand() % 100));
    // 注册第五个key
    TestOperation *op5 = new TestOperation(ObDeadLockTestIntKey(index + 5));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(index + 5), *op5, collect_callback, 1, std::rand() % 100));
    // 为两个key搭建依赖关系
    // ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 1), ObDeadLockTestIntKey(index + 2)));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->add_parent(ObDeadLockTestIntKey(index + 2), GCTX.self_addr(), ObDeadLockTestIntKey(index + 1)));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 2), ObDeadLockTestIntKey(index + 3)));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 3), ObDeadLockTestIntKey(index + 4)));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 4), ObDeadLockTestIntKey(index + 5)));
    ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 5), ObDeadLockTestIntKey(index + 1)));
    TestOperation::loop_occurd_time = chrono::high_resolution_clock::now();
    std::this_thread::sleep_for(chrono::microseconds(PERIOD * 2));

    //delete op1;
    delete op2;
    delete op3;
    delete op4;
    delete op5;
    ASSERT_EQ(i + 1, TestOperation::v_killed_node.size());// 一个环中只能有一个节点探测到死锁
  }
  ASSERT_EQ(loop_times, TestOperation::v_killed_node.size());// 一个环中只能有一个节点探测到死锁
  ASSERT_EQ(loop_times, TestOperation::v_record_is_lowest_priority.size());
  for (int i = 0; i < TestOperation::v_record_is_lowest_priority.size(); ++i)
    ASSERT_EQ(true, TestOperation::v_record_is_lowest_priority[i]);
  cout << "max delay:" << *max_element(TestOperation::v_record.begin(), TestOperation::v_record.end()) << "ms" << endl;
  cout << "min delay:" << *min_element(TestOperation::v_record.begin(), TestOperation::v_record.end()) << "ms" << endl;
  cout << "average delay:" << accumulate(TestOperation::v_record.begin(), TestOperation::v_record.end(), 0) / TestOperation::v_record.size() << "ms" << endl;
}

class DeadLockBlockCallBack {
public:
  DeadLockBlockCallBack(uint64_t hash) : hash_(hash) {
    TRANS_LOG(INFO, "hash value when created", K(hash), K(hash_));
  }
  int operator()(ObDependencyResource &resource, bool &need_remove) {
    UNUSED(resource);
    UNUSED(need_remove);
    std::cout<<"hash_:"<<hash_<<std::endl;
    int ret = OB_SUCCESS;
    return ret;
  }
private:
  uint64_t hash_;
};

TEST_F(TestObDeadLockDetector, block_call_back) {
  DeadLockBlockCallBack deadlock_block_call_back(7710038271457258879UL);
  ObFunction<int(ObDependencyResource &,bool &)> func = deadlock_block_call_back;
  ObDependencyResource re;
  bool n;
  func(re, n);
}

TEST_F(TestObDeadLockDetector, test_timeout) {
  TestOperation *op2 = new TestOperation(ObDeadLockTestIntKey(1));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(1), *op2, collect_callback, 1, std::rand() % 100));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(2), *op2, collect_callback, 1, std::rand() % 100));
  MTL(ObDeadLockDetectorMgr*)->set_timeout(ObDeadLockTestIntKey(1), 1000);
  std::this_thread::sleep_for(chrono::seconds(1));
  delete op2;
  ASSERT_EQ(MTL(ObDeadLockDetectorMgr*)->get_detector_create_count(), MTL(ObDeadLockDetectorMgr*)->get_detector_release_count() + 1);
}

// 1 -> 5 -> 3 -> 4 -> 1
// 1 -> 2 - > 3 -> 4 -> 1
// 2 有全局最小优先级
TEST_F(TestObDeadLockDetector, small_cycle_in_big_cycle_bad_case) {
  int loop_times = 5;
  // 注册第一个key
  TestOperation *op1 = new TestOperation(ObDeadLockTestIntKey(1));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(1), *op1, collect_callback, 9));
  // 注册第二个key
  TestOperation *op2 = new TestOperation(ObDeadLockTestIntKey(2));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(2), *op2, collect_callback, 0));// should kill this node
  // 注册第三个key
  TestOperation *op3 = new TestOperation(ObDeadLockTestIntKey(3));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(3), *op3, collect_callback, 9));
  // 注册第四个key
  TestOperation *op4 = new TestOperation(ObDeadLockTestIntKey(4));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(4), *op4, collect_callback, 9));
  // 注册第五个key
  TestOperation *op5 = new TestOperation(ObDeadLockTestIntKey(5));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->register_key(ObDeadLockTestIntKey(5), *op5, collect_callback, 1));
  // 为两个key搭建依赖关系
  // ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(index + 1), ObDeadLockTestIntKey(index + 2)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(5)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(5), ObDeadLockTestIntKey(3)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(3), ObDeadLockTestIntKey(4)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(4), ObDeadLockTestIntKey(1)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(1), ObDeadLockTestIntKey(2)));
  ASSERT_EQ(OB_SUCCESS, MTL(ObDeadLockDetectorMgr*)->block(ObDeadLockTestIntKey(2), ObDeadLockTestIntKey(3)));
  TestOperation::loop_occurd_time = chrono::high_resolution_clock::now();
  std::this_thread::sleep_for(chrono::microseconds(PERIOD * 4));
  delete op1;
  delete op2;
  delete op3;
  delete op4;
  delete op5;
  for (auto v : TestOperation::v_killed_node) {
    DETECT_LOG(INFO, "kill node", K(v));
  }
  ASSERT_EQ(2, TestOperation::v_killed_node.size());
  ASSERT_EQ(true, TestOperation::v_killed_node[0] == 2 || TestOperation::v_killed_node[0] == 5);
  ASSERT_EQ(true, TestOperation::v_killed_node[1] == 2 || TestOperation::v_killed_node[1] == 5);
}

// TEST_F(TestObDeadLockDetector, test_lock_conflict_print) {
//   memtable::RetryInfo retry_info;
//   while ((ObClockGenerator::getClock() % 1000000) < 100_ms);
//   DETECT_LOG(INFO, "DEBUG1", K(ObClockGenerator::getClock()));
//   ASSERT_EQ(retry_info.need_print(), true);
//   retry_info.on_conflict();
//   ASSERT_EQ(retry_info.need_print(), false);
//   this_thread::sleep_for(std::chrono::milliseconds(100));
//   DETECT_LOG(INFO, "DEBUG2", K(ObClockGenerator::getClock()));
//   ASSERT_EQ(retry_info.need_print(), false);
//   this_thread::sleep_for(std::chrono::milliseconds(1000));
//   DETECT_LOG(INFO, "DEBUG", K(retry_info));
//   ASSERT_EQ(retry_info.need_print(), true);
//   for (int i = 0; i < 9; ++i) {
//     retry_info.on_conflict();
//   }
//   ASSERT_EQ(retry_info.need_print(), true);
// }

// TEST_F(TestObDeadLockDetector, print_timestamp) {
//   int64_t ts_ = 1623827288705600;
//   DETECT_LOG(INFO, "test ts", KTIME(ts_));
//   DETECT_LOG(INFO, "test ts", KTIME_(ts));
//   DETECT_LOG(INFO, "test ts", KTIMERANGE(ts_, DAY, MSECOND));
//   DETECT_LOG(INFO, "test ts", KTIMERANGE_(ts, DAY, MSECOND));
//   ObLCLMessage msg;
//   UserBinaryKey key;
//   key.set_user_key(ObDeadLockTestIntKey(1));
//   msg.set_args(GCTX.self_addr(),
//                key,
//                GCTX.self_addr(),
//                key,
//                2,
//                ObLCLLabel(1, ObDetectorPriority(1)),
//                ObClockGenerator::getRealClock());
//   char buffer[4000];
//   msg.to_string(buffer, 4000);
//   std::cout << "test ts" << buffer << std::endl;
//   DETECT_LOG(INFO, "test ts", K(msg));
// }

}// namespace unittest
}// namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_deadlock.log");
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_deadlock.log", false);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
