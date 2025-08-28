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
#include "ob_admin_object_storage_driver_quality.h"
#include "deps/oblib/src/lib/utility/ob_tracepoint.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{
//=========================== OSDQScene ================================
OSDQScene::OSDQScene()
    : is_inited_(false), 
      run_time_s_(0),
      metric_(nullptr),
      base_uri_(nullptr),
      storage_info_(),
      task_handler_(),
      file_set_()
{
}

OSDQScene::~OSDQScene()
{
}

int OSDQScene::loop_send_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "scene not init", KR(ret), K(is_inited_));
  } else {
    const int64_t start_time_us = ObTimeUtility::current_time();
    int64_t current_time_us = start_time_us; 
    while (current_time_us - start_time_us < run_time_s_ * 1000000 
      && (ret == OB_SUCCESS || ret == OB_EAGAIN)) {
      OSDQTask *task = nullptr;
      if (OB_FAIL(task_handler_.gen_task(task))) {
        OB_LOG(WARN, "failed generate task", KR(ret));
        if (ret == OB_FILE_NOT_EXIST) {
          ret = OB_EAGAIN;
        }
      } else {
        do {
          ret = task_handler_.push_task(task);
          if (ret != OB_SUCCESS && ret != OB_EAGAIN) {
            OB_LOG(ERROR, "deliver task failed", KR(ret), K(task->op_type_), K(task->uri_));
          } else {
            ::usleep(10000);
          }
          current_time_us = ObTimeUtility::current_time();
        } while (OB_EAGAIN == ret && current_time_us - start_time_us < run_time_s_ * 1000000);
      }

      if (OB_FAIL(ret)) {
        free_task(task);
      }
      current_time_us = ObTimeUtility::current_time();
    }

    if (ret == OB_EAGAIN) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int OSDQScene::set_thread_cnt(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(thread_cnt <= 0 || thread_cnt > 128)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret));
  } else if (OB_FAIL(task_handler_.set_thread_cnt(thread_cnt))) {
    OB_LOG(WARN, "failed set task handler's thread_cnt", KR(ret), K(thread_cnt));
  }
  return ret;
}
//=========================== OSDQTask ========================================
OSDQTask::OSDQTask()
  : op_type_(OSDQOpType::MAX_OPERATE_TYPE),
    object_id_(-1),
    uri_(nullptr),
    buf_(nullptr),
    buf_len_(-1),
    start_time_stamp_us_(0),
    parallel_(false),
    parallel_op_type_(OSDQOpType::MAX_OPERATE_TYPE)
{}

OSDQTask::~OSDQTask()
{
  if (uri_ != nullptr) {
    free(uri_);
    uri_ = nullptr;
  }
  if (buf_ != nullptr) {
    free(buf_);
    buf_ = nullptr;
  }
}

bool OSDQTask::is_valid() const 
{
  bool valid = true;
  if (OB_UNLIKELY(op_type_ == MAX_OPERATE_TYPE) || OB_ISNULL(uri_)) {
    valid = false;
  } else if (op_type_ == WRITE_SINGLE_FILE || op_type_ == APPEND_WRITE || op_type_ == MULTIPART_WRITE) {
    if (OB_UNLIKELY(object_id_ <= 0) 
        || OB_UNLIKELY(buf_len_ > 0 && buf_ == nullptr)
        || OB_UNLIKELY(buf_len_ < 0)) {
      valid = false;
    } else if (op_type_ == APPEND_WRITE && OB_UNLIKELY(buf_len_ < 4)) {
      valid = false;
    }
  } else if (op_type_ == READ_SINGLE_FILE) {
    if (OB_UNLIKELY(object_id_ <= 0)) {
      valid = false;
    }
  } else if (op_type_ == DEL_FILE) {
    if (OB_UNLIKELY(object_id_ <= 0)) {
      valid = false;
    }
  }
  return valid;
}

void free_task(OSDQTask *&task)
{
  if (OB_NOT_NULL(task)) {
    task->~OSDQTask();
    free(task);
  }
  task = nullptr;
}

//=========================== OSDQHybridTestScene ================================

OSDQHybridTestScene::OSDQHybridTestScene()
  : OSDQScene()
{
}

int OSDQHybridTestScene::init(const OSDQParameters *param, OSDQMetric *metric)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "hybrid test scene init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param_valid(param))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(param), K(storage_info_));
  } else if (OB_FAIL(task_handler_.init(param->base_path_, metric, &file_set_, &storage_info_))) {
    OB_LOG(WARN, "failed init task handler", KR(ret), KPC(param), K(storage_info_));
  } else {
    task_handler_.set_operator_weight(WRITE_SINGLE_FILE, 1);
    task_handler_.set_operator_weight(MULTIPART_WRITE, 1);
    task_handler_.set_operator_weight(APPEND_WRITE, 1);
    task_handler_.set_operator_weight(READ_SINGLE_FILE, 3);
    task_handler_.set_operator_weight(DEL_FILE, 1);
    run_time_s_ = param->run_time_s_;
    metric_ = metric;
    base_uri_ = param->base_path_;
    is_inited_ = true;
  }
  return ret;
}

bool OSDQHybridTestScene::param_valid(const OSDQParameters *param) {
  bool is_valid = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    is_valid = false;
  } else if (OB_FAIL(storage_info_.set(param->base_path_, param->storage_info_str_))) {
    is_valid = false;
    OB_LOG(WARN, "failed to set storage info", KR(ret), KPC(param));
  } else if (OB_UNLIKELY(!storage_info_.is_valid() || param->run_time_s_ <= 0)) {
    is_valid = false;
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(storage_info_), KPC(param));
  } 
  return is_valid;
}

int OSDQHybridTestScene::execute()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else if (OB_FAIL(task_handler_.start())) {
    OB_LOG(WARN, "failed start task handler", KR(ret));
  } else if (OB_FAIL(loop_send_task())) {
    OB_LOG(WARN, "failed exec loop_send_task", KR(ret)); 
  }
  task_handler_.destroy();

  return ret;
}

//==================== OSDQResourceLimitedScene ====================
OSDQResourceLimitedScene::OSDQResourceLimitedScene() 
  : OSDQScene(),
    resource_limited_type_(MAX_RESOURCE_LIMITED_TYPE),
    limit_run_time_s_(0),
    limit_memory_mb_(0),
    limit_cpu_(0)
{}

int OSDQResourceLimitedScene::init(const OSDQParameters *param, OSDQMetric *metric)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "resource limited scene init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param_valid(param))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(param), K(metric));
  } else if (OB_FAIL(task_handler_.init(param->base_path_, metric, &file_set_, &storage_info_))) {
    OB_LOG(WARN, "failed init task handler", KR(ret));
  } else if (OB_FAIL(task_handler_.set_thread_cnt(TASK_HANDLER_DEFAULT_THREAD_CNT))) {
    OB_LOG(WARN, "failed set task handler thread cnt", KR(ret));
  } else {
    task_handler_.set_operator_weight(OSDQOpType::DEL_FILE, 0);
    task_handler_.set_prob_of_parallel(0);
    task_handler_.set_prob_of_writing_old_data(0);
    base_uri_ = param->base_path_;
    run_time_s_ = param->run_time_s_;
    metric_ = metric;
    resource_limited_type_ = static_cast<ResourceLimitedType>(param->resource_limited_type_);
    limit_run_time_s_ = param->limit_run_time_s_;
    limit_memory_mb_ = param->limit_memory_mb_;
    limit_cpu_ = param->limit_cpu_;
    is_inited_ = true;
  }
  return ret;
}

bool OSDQResourceLimitedScene::param_valid(const OSDQParameters *param) {
  bool is_valid = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    is_valid = false;
  } else if (OB_FAIL(storage_info_.set(param->base_path_, param->storage_info_str_))) {
    is_valid = false;
    OB_LOG(WARN, "failed to set storage info", KR(ret), KPC(param));
  } else if (OB_UNLIKELY(!storage_info_.is_valid() || param->run_time_s_ <= 0 
                         || param->resource_limited_type_ < 0 
                         || param->resource_limited_type_ >= MAX_RESOURCE_LIMITED_TYPE
                         || param->limit_run_time_s_ <= 0)) {
    is_valid = false;
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(storage_info_), KPC(param));
  } else if (param->resource_limited_type_ == MEMORY_LIMITED_TYPE) {
    // setting this to a smaller value has been tested to cause the program to break
    if (OB_UNLIKELY(param->limit_memory_mb_ < 64)) {
      is_valid = false;
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret), K(param->limit_memory_mb_));
    }
  } else if (param->resource_limited_type_ == CPU_LIMITED_TYPE) {
    if (OB_UNLIKELY(param->limit_cpu_ > 1 || param->limit_cpu_ < 0
                    || param->limit_run_time_s_ < param->run_time_s_ + 5)) {
      is_valid = false;
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid argument", KR(ret), K(param->limit_cpu_));
      OSDQLogEntry::print_log("INVALID ARGUMENT", "the resource limit run time should be at least 5s larger than run time when limited type is cpu limit", RED_COLOR_PREFIX); 
    }
  }
  return is_valid;
}

int OSDQResourceLimitedScene::execute()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else if (OB_FAIL(task_handler_.start())) {
    OB_LOG(ERROR, "failed start task hanlder", KR(ret));
  } else {
    switch (resource_limited_type_) {
      case NETWORK_PACKET_LOSS_LIMITED_TYPE:
        ret = test_network_packet_loss_limit_();
        break;
      case NETWORK_BANDWIDTH_LIMITED_TYPE:
        ret = test_network_bandwidth_limit_();
        break;
      case MEMORY_LIMITED_TYPE:
        ret = test_memory_limit_();
        break;
      case CPU_LIMITED_TYPE:
        ret = test_cpu_limit_();
        break;
      case MAX_RESOURCE_LIMITED_TYPE:
        ret = OB_ERR_UNEXPECTED;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        break;
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed exec resource limited test", KR(ret), K(resource_limited_type_));
    }
    task_handler_.destroy();
  } 
  return ret;
}

void OSDQResourceLimitedScene::inner_disrupt_network_(
    const int64_t sleep_time_s, 
    const char *class_handle,
    std::mutex &mtx,
    std::condition_variable &cv,
    bool &ready)
{
  int ret = OB_SUCCESS;
  /*
   *                  1:        root qdisc
   *                /   \
   *              1:1   1:2     child class  
   *               |     |
   *              10:   20:       qdisc
   */
  system("tc qdisc add dev eth0 root handle 1: htb default 30");
  system("tc class add dev eth0 parent 1: classid 1:1 htb rate 10000mbit");
  system("tc class add dev eth0 parent 1: classid 1:2 htb rate 10mbit");
  system("tc filter add dev eth0 parent 1: protocol all prio 1 handle 1: cgroup");
  system("tc qdisc add dev eth0 parent 1:1 handle 10: netem loss 100%");
  system("tc qdisc add dev eth0 parent 1:2 handle 20: sfq perturb 10");
  system("cgcreate -g net_cls:/ob_admin_osdq_cgroup");
  std::string bind_str = std::string("echo ") + std::string(class_handle) + 
                         " | tee /sys/fs/cgroup/net_cls/ob_admin_osdq_cgroup/net_cls.classid > /dev/null";
  system(bind_str.c_str());
  const int pid = getpid();
  std::string cmd = std::string("cgclassify -g net_cls:ob_admin_osdq_cgroup ") + std::to_string(pid);

  assert(system(cmd.c_str()) == 0);
  {
    std::unique_lock<std::mutex> lock(mtx);
    ready = true;
  }
  cv.notify_all();
  OSDQLogEntry log;
  if (OB_FAIL(log.init("DISRUPT NETWORK"))) {
    OB_LOG(WARN, "failed init log", KR(ret));
  }
  OSDQLogEntry::print_log("DISRUPT_NETWORK", "THE NETWORK RESTRICTED ENVIRONMENT IS READY");
  OSDQLogEntry::print_log("DISRUPT_NETWORK", 
                          std::string("bind qdisc ") + std::string(class_handle) + std::string(" success"));

  std::this_thread::sleep_for(std::chrono::seconds(sleep_time_s));

  system("echo | tee /sys/fs/cgroup/net_cls/ob_admin_osdq_cgroup/net_cls.classid");
  OSDQLogEntry::print_log("DISRUPT_NETWORK", "NETWORK DISRUPT IS FINISHED");
  system("tc qdisc del dev eth0 root");
}

int OSDQResourceLimitedScene::test_network_packet_loss_limit_()
{
  int ret = OB_SUCCESS;
  std::mutex mtx;
  std::condition_variable cv;
  bool ready = false;
  OSDQLogEntry::print_log("NETWORK TEST1", "Packet loss 100% Test started");
  std::thread t(inner_disrupt_network_, limit_run_time_s_, "0x00010001",
                std::ref(mtx), std::ref(cv), std::ref(ready));
  std::unique_lock<std::mutex> lock(mtx);
  while (!ready) {
    cv.wait(lock);
  }
  ret = loop_send_task(); 
  t.join();
  task_handler_.stop();
  
  if (OB_SUCC(ret)) {
    for (auto req_ret : task_handler_.req_results_) {
      if (req_ret != OB_SUCCESS 
          && req_ret != OB_OBJECT_STORAGE_IO_ERROR
          && req_ret != OB_IO_TIMEOUT 
          && req_ret != OB_TIMEOUT) {
        ret = req_ret;
        break;
      }
    }
  }

  return ret;  
}

int OSDQResourceLimitedScene::test_network_bandwidth_limit_()
{
  int ret = OB_SUCCESS;
  std::mutex mtx;
  std::condition_variable cv;
  bool ready = false;
  OSDQLogEntry::print_log("NETWORK TEST2", "BandWidth Limit Test Started");
  std::thread t(inner_disrupt_network_, limit_run_time_s_, "0x00010002",
                std::ref(mtx), std::ref(cv), std::ref(ready));
  std::unique_lock<std::mutex> lock(mtx);
  while (!ready) {
    cv.wait(lock);
  }

  ret = loop_send_task();
  t.join();
  task_handler_.stop();
  if (OB_SUCC(ret)) {
    for (auto req_ret : task_handler_.req_results_) {
      if (req_ret != OB_SUCCESS && req_ret != OB_IO_TIMEOUT) {
        ret = req_ret;
        break;
      }
    }
  }

  return ret;  
}

void OSDQResourceLimitedScene::inner_limit_memory_(
    const int64_t sleep_time_s, 
    const int64_t limit_memory_size_mb,
    std::mutex &mtx,
    std::condition_variable &cv,
    bool &ready)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  const int64_t limit_memory_size = limit_memory_size_mb * 1024 * 1024; 
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, tenant_holder))) {
    OB_LOG(WARN, "failed get tenant io manager", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->update_memory_pool(limit_memory_size))) {
    OB_LOG(WARN, "faield to update memory limit", KR(ret));
    OSDQLogEntry::print_log("LIMIT MEMORY", "failed to update memory limit", RED_COLOR_PREFIX);
  } else if (OB_FAIL(ObMallocAllocator::get_instance()->set_tenant_limit(OB_SERVER_TENANT_ID, limit_memory_size))) {
    OB_LOG(ERROR, "failed set tenant memory limit", KR(ret), K(limit_memory_size));
  } else {
    lib::set_memory_limit(limit_memory_size);
    // When the tenant is out of memory, it will raise(49), and ob_admin doesn't do the
    // initialisation that observer does, so it will just quit, so in order to make sure
    // that ob_admin runs correctly, need to ignore 49.
    signal(49, SIG_IGN);
    {
      std::unique_lock<std::mutex> lock(mtx);
      ready = true;
    }
    
    cv.notify_all();
    OSDQLogEntry log;
    if (OB_FAIL(log.init("LIMIT MEMORY"))) {
      OB_LOG(ERROR, "failed init log", KR(ret));
    } else {
      log.log_entry_kv("RESTRICTED TIME", std::to_string(sleep_time_s) + "s");
      log.log_entry_kv("TOTAL MEMORY LIMIT", std::to_string(limit_memory_size));
      log.log_entry_kv("TOTAL TENANT MEMORY LIMIT", std::to_string(limit_memory_size));
      log.log_entry_kv("TOTAL TENANT IO MANAGER MEMORY LIMIT", std::to_string(limit_memory_size));
      log.log_entry("THE MEMORY RESTRICTED ENVIRONMENT IS READY");
      log.print();
    }
    ret = OB_SUCCESS;
    std::this_thread::sleep_for(std::chrono::seconds(sleep_time_s));
    lib::set_memory_limit(MEMORY_LIMITED_SIZE);
    lib::set_tenant_memory_limit(500, MEMORY_LIMITED_SIZE);
    if (OB_NOT_NULL(tenant_holder.get_ptr())) {
      if (FAILEDx(tenant_holder.get_ptr()->update_memory_pool(MEMORY_LIMITED_SIZE))) {
        OB_LOG(WARN, "faield to restore memory", KR(ret));
        OSDQLogEntry::print_log("LIMIT MEMORY", "failed to restore memory", RED_COLOR_PREFIX);
      }
    }
  } 

  if (OB_SUCC(ret)) {
    OSDQLogEntry::print_log("LIMIT MEMORY", "THE MEMORY RESTRICETED ENVIRONMENT IS REMOVED", GREEN_COLOR_PREFIX);
  }
}

int OSDQResourceLimitedScene::test_memory_limit_()
{
  int ret = OB_SUCCESS;
  OSDQLogEntry::print_log("MEMORY LIMITD TEST", "MEMORY LIMIT TEST START");
  std::mutex mtx;
  std::condition_variable cv;
  bool ready = false;
  std::thread t(inner_limit_memory_, limit_run_time_s_, limit_memory_mb_, 
                std::ref(mtx), std::ref(cv), std::ref(ready));
  std::unique_lock<std::mutex> lock(mtx);
  while (!ready) {
    cv.wait(lock);
  }
  ret = loop_send_task();
  t.join();
  task_handler_.stop();

  if (OB_SUCC(ret)) {
    for (auto req_ret : task_handler_.req_results_) {
      if (req_ret != OB_SUCCESS 
          && req_ret != OB_HASH_NOT_EXIST
          && req_ret != OB_ALLOCATE_MEMORY_FAILED) {
        ret = req_ret;
        break;
      }
    }
  }

  return ret;  
}

void OSDQResourceLimitedScene::inner_limit_cpu_(
    const int64_t sleep_time_s, 
    const double cpu_rate,
    std::mutex &mtx,
    std::condition_variable &cv,
    bool &ready)
{
  int ret = OB_SUCCESS;
  system("cgcreate -g cpu:/ob_admin_osdq_cgroup");
  const int64_t quota = static_cast<int64_t>(100000 * cpu_rate);
  std::string cmd = std::string("echo ") + std::to_string(quota) + std::string(" > /sys/fs/cgroup/cpu/ob_admin_osdq_cgroup/cpu.cfs_quota_us");
  system(cmd.c_str());

  system("cat /sys/fs/cgroup/cpu/cpuset.mems > /sys/fs/cgroup/cpu/ob_admin_osdq_cgroup/cpuset.mems");
  system("cat /sys/fs/cgroup/cpu/cpuset.cpus > /sys/fs/cgroup/cpu/ob_admin_osdq_cgroup/cpuset.cpus");

  const int pid = getpid();
  cmd = std::string("cgclassify -g cpu:ob_admin_osdq_cgroup ") + std::to_string(pid);
  assert(system(cmd.c_str()) == 0);
  OSDQLogEntry log;
  if (OB_FAIL(log.init("LIMIT CPU"))) {
    OB_LOG(ERROR, "failed init log", KR(ret));
  } else {
    log.log_entry_kv("RESTRICTED TIME", std::to_string(sleep_time_s) + "s");
    log.log_entry_kv("CPU RATE", std::to_string(cpu_rate) + "%");
    log.log_entry("THE MEMORY RESTRICTED ENVIRONMENT IS READY");
    log.log_entry("LIMIT CPU SUCCESS");
    log.print();
  }
  {
    std::unique_lock<std::mutex> lock(mtx);
    ready = true;
  }
  cv.notify_all();
  std::this_thread::sleep_for(std::chrono::seconds(sleep_time_s));
  OSDQLogEntry::print_log("LIMIT CPU", "LIMIT CPU FINISH");
  system("echo 100000 > /sys/fs/cgroup/cpu/ob_admin_osdq_cgroup/cpu.cfs_quota_us");
}

int OSDQResourceLimitedScene::test_cpu_limit_()
{
  int ret = OB_SUCCESS;
  OSDQLogEntry::print_log("CPU LIMIT TEST", "CPU LIMIT TEST START");
  std::mutex mtx;
  std::condition_variable cv;
  bool ready = false;
  std::thread t(inner_limit_cpu_, limit_run_time_s_, limit_cpu_,
                std::ref(mtx), std::ref(cv), 
                std::ref(ready));
  std::unique_lock<std::mutex> lock(mtx);
  while (!ready) {
    cv.wait(lock);
  }
  ret = loop_send_task(); 
  t.join();
  task_handler_.stop();
  
  if (OB_SUCC(ret)) {
    for (auto req_ret : task_handler_.req_results_) {
      if (req_ret != OB_SUCCESS) {
        ret = req_ret;
        break;
      }
    }
  }

  if (metric_->get_real_cpu_usage() >= FINAL_CPU_USAGE_LIMIT) {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

//===================== OSDQErrSimScene ====================

OSDQErrSimScene::OSDQErrSimScene()
{}

int OSDQErrSimScene::init(const OSDQParameters *param, OSDQMetric *metric)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "ErrSim scene init twice", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param_valid(param))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KPC(param));
  } else if (OB_FAIL(task_handler_.init(param->base_path_, metric, &file_set_, &storage_info_))) {
    OB_LOG(WARN, "failed init task handler", KR(ret));
  } else {
    task_handler_.set_operator_weight(WRITE_SINGLE_FILE, 1);
    task_handler_.set_operator_weight(MULTIPART_WRITE, 1);
    task_handler_.set_operator_weight(APPEND_WRITE, 1);
    task_handler_.set_operator_weight(READ_SINGLE_FILE, 3);
    task_handler_.set_operator_weight(DEL_FILE, 1);
    task_handler_.set_prob_of_parallel(0);
    run_time_s_ = param->run_time_s_;
    metric_ = metric;
    base_uri_ = param->base_path_;
    is_inited_ = true;
  }
  return ret;
}

bool OSDQErrSimScene::param_valid(const OSDQParameters *param)
{
  bool is_valid = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    is_valid = false;
  } else if (OB_FAIL(storage_info_.set(param->base_path_, param->storage_info_str_))) {
    is_valid = false;
    OB_LOG(WARN, "failed to set storage info", KR(ret), KPC(param));
  } else if (OB_UNLIKELY(!storage_info_.is_valid() || param->run_time_s_ <= 0)) {
    is_valid = false;
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(storage_info_), KPC(param));
  }
  return is_valid;
}

int OSDQErrSimScene::execute()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else if (OB_FAIL(task_handler_.start())) {
    OB_LOG(WARN, "failed start task handler", KR(ret));
  } else {
    EventItem item;
    item.trigger_freq_ = 1;
    item.error_code_ = OB_OBJECT_STORAGE_CHECKSUM_ERROR;
    /* if (OB_FAIL(EventTable::instance().set_event(EventTable::EN_OBJECT_STORAGE_CHECKSUM_ERROR.item_.no_, item))) { */
    if (OB_FAIL(EventTable::instance().set_event("EN_OBJECT_STORAGE_CHECKSUM_ERROR", item))) {
      OB_LOG(ERROR, "failed set event", KR(ret));
    } else {
      ret = loop_send_task();

      task_handler_.stop();
      if (OB_SUCC(ret)) {
        for (auto req_ret : task_handler_.req_results_) {
          if (req_ret != OB_OBJECT_STORAGE_CHECKSUM_ERROR) {
            ret = req_ret;
            break;
          }
        }
      }
    }
  }

  task_handler_.destroy();
  return ret;
}

} // namespace tools
} // namespace oceanbase
