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

#ifndef OB_ADMIN_IO_DRIVER_QUALITY_H_
#define OB_ADMIN_IO_DRIVER_QUALITY_H_ 

#include <stdio.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <thread>
#include <future>
#include <functional>
#include <atomic>
#include <condition_variable>
#include "../ob_admin_executor.h"
#include "deps/oblib/src/lib/task/ob_timer.h"
#include "deps/oblib/src/lib/ob_define.h"                   // OB_MAX_URI_LENGTH
#include "share/backup/ob_backup_struct.h"                  // OB_MAX_BACKUP_STORAGE_INFO_LENGTH
#include "share/ob_thread_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "src/share/io/ob_io_manager.h"
#include "src/share/ob_device_manager.h"
#include "deps/oblib/src/lib/lock/ob_rwlock.h"
#include "deps/oblib/src/lib/allocator/ob_malloc.h"
#include "deps/oblib/src/lib/allocator/ob_vslice_alloc.h"
#include "deps/oblib/src/lib/alloc/ob_malloc_sample_struct.h"
#include "deps/oblib/src/lib/alloc/ob_tenant_ctx_allocator.h"
#include "deps/oblib/src/lib/alloc/memory_dump.h"
#include "deps/oblib/src/lib/alloc/malloc_hook.h"

namespace oceanbase
{
namespace tools
{
// color prefix in shell
const std::string NONE_COLOR_PREFIX = "\033[m";
const std::string GREEN_COLOR_PREFIX = "\033[0;32;32m";
const std::string RED_COLOR_PREFIX = "\033[0;32;31m";
const std::string LIGHT_BLUE_PREFIX = "\033[1;34m";
const std::string DARY_GRAY_PREFIX = "\033[1;30m";

// global constant
constexpr int64_t TIME_STR_LENGTH = 128;
constexpr int64_t FILE_PATH_LENGTH = common::OB_MAX_URI_LENGTH;
constexpr int64_t MAX_CONTENT_LENGTH = 128L * 1024 * 1024;
constexpr int64_t MAX_OBJECT_NAME_LENGTH = 50;
constexpr int64_t MAX_OBJECT_NAME_SUFFIX_LENGTH = 16;
constexpr int64_t MIN_OBJECT_SIZE = 0;
constexpr int64_t SMALL_OBJECT_SIZE_LIMIT = 128 * 1024;          // 128 KB
constexpr int64_t NORMAL_OBJECT_SIZE_LIMIT = 2L * 1024 * 1024;   // 2MB 
constexpr int64_t LARGE_OBJECT_SIZE_LIMIT = 128L * 1024 * 1024;  // 128MB
constexpr int64_t MEMORY_LIMITED_SIZE = 16L * 1024 * 1024 * 1024;
constexpr int64_t FINAL_OBJECT_STORAGE_MEMORY_LIMIT = 50 * 1024 * 1024;   // 50MB

constexpr double EPS = 1e-6;
// The LARGE_OBJECT_SIZE_RATE = 1.0 - SMALL_OBJECT_SIZE_RATE - NORMAL_OBJECT_SIZE_RATE;
constexpr double SMALL_OBJECT_SIZE_RATE = 0.2;
constexpr double NORMAL_OBJECT_SIZE_RATE = 0.75;
static_assert(SMALL_OBJECT_SIZE_RATE + NORMAL_OBJECT_SIZE_RATE < 1, "The sum of SMALL_OBJECT_SIZE_RATE and NORMAL_OBJECT_SIZE_LIMIT should be less than 1");

// Default Scene Parameter 
constexpr int64_t DEFAULT_RUN_TIME_S                = 20;           // the default running time of the scene
constexpr int64_t DEFAULT_INTERVAL_S                = 1;            // the default interval between each metric display 
constexpr int64_t DEFAULT_THREAD_CNT                = 16;           // the dafault thread cnt for task hanlder
constexpr int64_t DEFAULT_LIMIT_RUN_TIME_S          = 10;           // the default duration of the restrictions in the limited scene
constexpr int64_t DEFAULT_LIMIT_MEMORY_MB           = 64;           // the default memory limited in the limited scene
constexpr double  DEFAULT_LIMIT_CPU                 = 0.2;          // the default cpu limited in the limited scene

constexpr int64_t DEFAULT_QUEUE_SIZE                = 32;          // the default task handler queue size
constexpr int64_t DEFAULT_PROB_OF_WRITING_OLD_DATA  = 20;
constexpr int64_t DEFAULT_PROB_OF_PARALLEL          = 10;

enum OSDQOpType
{
  WRITE_SINGLE_FILE,
  MULTIPART_WRITE, 
  APPEND_WRITE,
  READ_SINGLE_FILE,
  DEL_FILE,
  MAX_OPERATE_TYPE            // invalid type
};


// in order to show in metric summary
static const char *osdq_op_type_names[] = {
  "WS",
  "MW",
  "AW",
  "RS",
  "DE"
};

static_assert((sizeof(osdq_op_type_names) / sizeof(char *)) == static_cast<int>(MAX_OPERATE_TYPE),
    "the length of osdq_op_type_names should be equal to MAX_OPERATE_TYPE");

double cal_time_diff(const timeval &start, const timeval &end);

/**
 *  @brief Generate the contents of the file based on the object_id
 *
 *  @param buf [out] The pointer to the contents of the file and must be requested in advance with a length of buf_len
 *  @param buf_len [in] The length of the buf
 *  @param object_id [in] The Object's ID
 */
int generate_content_by_object_id(char *buf, const int64_t buf_len, const int64_t object_id);

/**
 *  @brief Verify that content is correct by object_id
 *  
 *  @param buf [in] The pointer to the content
 *  @param buf_len [in] The length of the buf
 *  @param object_id [in] The Object's ID
 */
int check_content_by_object_id(const char *buf, const int64_t buf_len, const int64_t object_id);

/**
 *  @brief Get a random length for performing a write operation or a read operation
 *
 *  @return content_length Indicates the content length generated 
 */
int64_t get_random_content_length(const OSDQOpType op_type);

/**
 *  @brief Generate object_name from object_id.
 *         For example, if object_id is k, then the generated line will be like object_k_{suffic}, where suffic is a random string of length MAX_OBJECT_NAME_SUFFIX_LENGTH containing numbers and upper and lower case letters.
 *  @param object_id [in] The Object's ID 
 *  @param object_name [out] The Object's name
 *  @param object_name_len [in] indicates the maximum length of object_name
 */
int construct_object_name(const int64_t object_id, char *object_name, const int64_t object_name_len);

int construct_file_path(
    const char *base_uri, 
    const char *object_name, 
    char *file_path, 
    const int64_t file_path_length); 

ObVSliceAlloc &get_vslice_alloc_instance();

extern int64_t allocator_cnt;

template <typename T>
class STLMemAllocator 
{
public:
  ObVSliceAlloc &allocator_;
public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using void_pointer = void*;
  using const_void_pointer = const void*;
  using size_type = size_t;
  using difference_type = ptrdiff_t;

  template <typename U>
  struct rebind
  {
    using other = STLMemAllocator<U>;
  };
  STLMemAllocator() : allocator_(get_vslice_alloc_instance())
  {}
  template <typename U>
  STLMemAllocator(const STLMemAllocator<U> &other) : allocator_(other.allocator_)
  {}
  pointer allocate(size_type n, const_void_pointer hint = 0) 
  {
    int ret = OB_SUCCESS;
    void *ptr = nullptr;
    do {
      ptr = allocator_.alloc(n * sizeof(T));
      if (OB_ISNULL(ptr)) {
        ::usleep(10000); //10ms 
        if (TC_REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          OB_LOG(WARN, "failed to allocate memory", KR(ret), K(n));
        }
      }
    } while (OB_ISNULL(ptr));
    allocator_cnt++;
    return static_cast<pointer>(ptr);
  }
  void deallocate(pointer p, size_type n) noexcept { allocator_.free(p); }
  size_type max_size() const noexcept { return allocator_.limit(); }
  template <typename U, typename... Args>
  void construct(U *p, Args &&...args) 
  {
    new(p) U(std::forward<Args>(args)...);
  }
  template <typename U>
  void destroy(U* p) { p->~U(); }
};

class OSDQLogEntry
{
public:
  OSDQLogEntry();
  ~OSDQLogEntry() {}
  int init(const std::string &title, const std::string &color = NONE_COLOR_PREFIX);
  static void print_log(const std::string &title, const std::string &content, const std::string &color = GREEN_COLOR_PREFIX);
  void log_entry_kv(const std::string &key, const std::string &value, const std::string &color = NONE_COLOR_PREFIX);
  void log_entry(const std::string &content, const std::string &color = NONE_COLOR_PREFIX);
  void print();
  void reset();

private:
  static std::string get_time_prefix_();

private:
  bool is_inited_;
  std::string prefix_;
  std::string head_holder_;
  std::string content_;
};

/**
 *  @class OSDQTimeMap
 *  @brief Save all the latency for a certain operation
 */
class OSDQTimeMap
{
public:
  OSDQTimeMap();
  ~OSDQTimeMap() {}
  /**
   *  @brief Add the time consumed by a IO operaton to the time_map
   *  @param cost_time_us [in] the time consumed by a IO operaton 
   */
  int log_entry(const int64_t cost_time_us);
  /**
   *  @brief print the information of the time_map
   *  @param map_name_str [in] title of the information
   */
  int summary(const char *map_name_str, OSDQLogEntry &log) const;

  /**
   *  @brief Just estimating the dynamic memory requested by time_map_ 
   */ 
  TO_STRING_KV(K_(total_entry));
private:
  int64_t total_entry_;
  std::map<int64_t, int64_t, std::less<int64_t>, 
    STLMemAllocator<std::pair<const int64_t, int64_t>>> time_map_;
};

/**
 * @class OSDQMetric
 * @brief record the runtime metric, like requests statistics, memory info, cpu info, etc.
 *        Since the size of read and write operation can be random, 
 *        statistics will be categorised by request size
 *        The following table shows the relationship between file size and attributes:
 *
 *            File Size                 | Attribute 
 *            ------------------------------------------
 *            file_size <= 128KB        | small object 
 *            128 KB < file_size <= 2MB | normal object
 *            2MB < file_size <= 128MB  | large object
 */ 
class OSDQMetric
{
  
public:
  enum ObjectSizeType
  {
    SMALL_OBJECT = 0,
    NORMAL_OBJECT_SIZE = 1,
    LARGE_OBJECT_SIZE = 2,
    MAX_OJBECT_SIZE_TYPE 
  };

  struct ReqStatisticalsInfo
  {
    ReqStatisticalsInfo();
    int64_t total_operation_num_;             // total req numbers
    int64_t total_queued_num_;
    double total_throughput_mb_;
    double average_qps_;
    double average_bw_mb_;
    double real_qps_;
    double real_bw_mb_;
  };

  struct CpuInfo
  {
    CpuInfo();
    double cpu_usage_for_100MB_bw_;
    double total_cpu_usage_;
    double total_user_time_;
    double total_system_time_;
    double real_cpu_usage_;
  };

  struct MemInfo 
  {
    MemInfo();
    double start_vm_size_kb_;
    double start_vm_rss_kb_;
    double object_storage_hold_kb_;
    double object_storage_used_kb_;
    double total_hold_kb_;
    double total_used_kb_;
    double vm_peak_kb_;                         // maximum virtual memory usage
    double vm_size_kb_;                         // current virtual memory usage
    double vm_hwm_kb_;                          // physical maxiumu memory usage
    double vm_rss_kb_;                          // current physical memory usage
    double ob_vslice_alloc_used_memory_kb_;
    int64_t ob_vslice_alloc_allocator_cnt_;
  };

public:
  OSDQMetric();
  ~OSDQMetric();
  int init();
  /**
   *  @brief add the latency of the IO operation to the corresponding latency_map
   *  @param op_start_time_us [in] the start time of the IO operation
   *  @param op_type [in] type of operation, e.g. read or write
   *  @param object_size [in] the operation's size
   */
  int add_latency_metric(
      const int64_t op_start_time_us, 
      const OSDQOpType op_type, 
      const int64_t object_size); 
  int add_queued_entry();
  int sub_queued_entry();
  int summary(const bool is_final = false);

public:
  static int get_memory_usage(MemInfo &mem_info);
  double get_real_cpu_usage() const { return last_cpu_info_.real_cpu_usage_; }

private:
  int get_req_statistical_info_(OSDQLogEntry &log);
  int get_req_latency_map_(OSDQLogEntry &log);
  int get_cpu_info_(OSDQLogEntry &log);
  int get_memory_info_(OSDQLogEntry &log, const bool is_final = false);

  int print_csv_title_();
  int print_csv_dump_();

private:
  static constexpr int64_t TITLE_LEN = 64;
  static constexpr int64_t PRECISION = 2;
  lib::ObMutex mutex_;
  bool is_inited_;
  char metric_csv_path_[OB_MAX_FILE_NAME_LENGTH];

  int64_t summary_cnt_;
  int64_t start_real_time_us_;
  int64_t last_real_time_us_;
  int64_t total_operation_num_;
  int64_t total_queued_num_;
  double total_throughput_mb_;
  ReqStatisticalsInfo last_req_statistical_info_;
  struct rusage start_usage_;
  struct rusage last_usage_;
  CpuInfo last_cpu_info_;
  MemInfo start_mem_info_;
  MemInfo last_mem_info_;
  OSDQTimeMap latency_maps_[OSDQOpType::MAX_OPERATE_TYPE][ObjectSizeType::MAX_OJBECT_SIZE_TYPE];
};

/**
 *  @class OSDQMonitor
 *  @brief Monitoring threads, used to print monitoring data such as qps, latency, etc. at regular intervals.
 */
class OSDQMonitor : public common::ObTimerTask 
{
public:
  OSDQMonitor();
  virtual ~OSDQMonitor();
  int init(const int64_t interval_s, OSDQMetric *metric);
  int start();
  void destroy();
  virtual void runTimerTask() override;

private:
  bool is_inited_;
  bool is_started_;
  OSDQMetric *metric_;
  int64_t interval_us_;
  int tg_id_;
};

struct OSDQParameters 
{
  OSDQParameters();
  ~OSDQParameters() {}
  TO_STRING_KV(K(base_path_), K(storage_info_str_), K(scene_type_), 
      K(run_time_s_), K(interval_s_), K(thread_cnt_), K(resource_limited_type_),
      K(limit_run_time_s_), K(limit_memory_mb_), K(limit_cpu_));
  // common param
  char base_path_[common::OB_MAX_URI_LENGTH];
  char storage_info_str_[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  int64_t scene_type_;
  int64_t run_time_s_;
  int64_t interval_s_;
  int64_t thread_cnt_;

  //ResourceLimitedScene param
  int64_t resource_limited_type_;
  int64_t limit_run_time_s_;
  int64_t limit_memory_mb_;
  double limit_cpu_;
};

class OSDQScene;
/**
 *  @class ObAdminObjectStorageDriverQualityExecutor 
 *  @brief The execution entry point of the programme, used for parameter parsing, to start the execution of the scenario.
 */
class ObAdminObjectStorageDriverQualityExecutor : public ObAdminExecutor
{
  enum SceneType 
  {
    HYBRID_TEST_SCENE,
    RESOURCE_LIMITED_SCENE,
    ERRSIM_SCENE,
    MAX_SCENE
  };
public:
  ObAdminObjectStorageDriverQualityExecutor();
  virtual ~ObAdminObjectStorageDriverQualityExecutor() {};
  virtual int execute(int argc, char *argv[]) override;

private:
  int parse_cmd_(int argc, char *argv[]);
  int set_environment_();
  int run_all_tests_();
  int print_usage_();

private:
  OSDQScene *create_scene_();
  void free_scene_(OSDQScene *&scene);

private:
  OSDQParameters params_; 
  OSDQMetric metric_;
  OSDQMonitor monitor_;
private:
  static constexpr char *HELP_FMT = const_cast<char*>("\t%-30s%-12s\n");
  static constexpr int64_t N_WAY = 32;
  static constexpr int64_t DEFAULT_BLOCK_SIZE = 2L * 1024 * 1024;
  DISALLOW_COPY_AND_ASSIGN(ObAdminObjectStorageDriverQualityExecutor);
};

class OSDQIDGenerator
{
public:
  OSDQIDGenerator() : current_id_(0) {}
  int64_t get_next_id() { return current_id_.fetch_add(1) + 1; }

  static OSDQIDGenerator &get_instance() 
  {
     static OSDQIDGenerator instance;
     return instance;
  }

private:
  std::atomic<int64_t> current_id_;
};

/**
 *  @class OSDQFileSet
 *  @brief This class maintains a set of files that have been written, along with the file path.
 */
class OSDQFileSet 
{
  template <typename T>
  struct equal_to {
    bool operator()(const T &lhs, const T &rhs) const {
      return lhs == rhs;
    }
  };
  typedef std::unordered_map<int64_t, char *, std::hash<int64_t>, equal_to<int64_t>, STLMemAllocator<std::pair<const int64_t, char *>>> FilePathMap;
  typedef std::set<int64_t, std::less<const int64_t>, STLMemAllocator<int64_t>> FileSet;
public:
  OSDQFileSet();
  ~OSDQFileSet();
  int add_file(const int64_t object_id, const char *file_path);
  /**
   *  @brief randomly select a file from the file set that have been written to
   *         then delete it from the file set
   *  @param object_id [out] the selected file's object_id 
   *  @param file_path [out] the selected file's path 
   */
  int fetch_and_delete_file(int64_t &object_id, char *&file_path);
  size_t size() const;
private:
  lib::ObMutex mutex_;
  FileSet file_set_;
  FilePathMap file_path_map_;
};

struct OSDQTask
{
  OSDQTask();
  ~OSDQTask();
  bool is_valid() const;
  TO_STRING_KV(K_(op_type), K_(object_id), K_(uri), K_(buf_len), K(start_time_stamp_us_), K(parallel_), K(parallel_op_type_));
  OSDQOpType op_type_;
  int64_t object_id_;
  char *uri_;
  char *buf_;
  int64_t buf_len_;
  int64_t start_time_stamp_us_;

  bool parallel_;      // indicates this task is concurrent
  OSDQOpType parallel_op_type_;
};

void free_task(OSDQTask *&task);

class OSDQResourceLimitedScene;
class OSDQErrSimScene;
/**
 *  @class OSDQTaskHandler
 *  @brief Responsible for task generation and processing
 */
class OSDQTaskHandler : public lib::TGTaskHandler
{
public:
  OSDQTaskHandler();
  virtual ~OSDQTaskHandler();
  int init(
      const char *base_uri,
      OSDQMetric *metric, 
      OSDQFileSet *file_set,
      const share::ObBackupStorageInfo *storage_info);
  void destroy();
  int start();
  void stop();
  int wait();
  int set_thread_cnt(const int64_t thread_cnt);
  virtual void handle(void *task) override;
  virtual void handle_drop(void *task) override;
  int push_task(OSDQTask *task);

public:
  int set_operator_weight(const OSDQOpType op_type, const int64_t op_type_weight);
  int set_prob_of_writing_old_data(const int64_t prob);
  int set_prob_of_parallel(const int64_t prob);
  int gen_task(OSDQTask *&task);
  int gen_write_task(OSDQTask *task, const OSDQOpType op_type);
  int handle_write_task(const OSDQTask *task);
  int gen_read_single_task(OSDQTask *task); 
  int handle_read_single_task(OSDQTask *task);
  int gen_del_task(OSDQTask *task);
  int handle_del_task(OSDQTask *task);

public:
  friend class OSDQResourceLimitedScene;
  friend class OSDQErrSimScene;

private:
  std::packaged_task<int()> get_packaged_task_(OSDQOpType op_type, const OSDQTask *task);
  int handle_read_single_task_helper_(OSDQTask *task);
  int handle_write_single_task_helper_(const OSDQTask *task);
  int handle_multipart_write_task_helper_(const OSDQTask *task);
  int handle_append_write_task_helper_(const OSDQTask *task);
  bool check_parallel_write_result_(
      const OSDQOpType op_type1, 
      const int ret1, 
      const OSDQOpType op_type2, 
      const int ret2);

  void push_req_result_(const int ret);

private:
  int64_t op_type_weights_[OSDQOpType::MAX_OPERATE_TYPE];
  int64_t op_type_random_boundaries_[OSDQOpType::MAX_OPERATE_TYPE];
  int64_t prob_of_writing_old_data_;    // the range is [0, 100), 0 indicates impossiblly write old file
  int64_t prob_of_parallel_;            // the probabilty of parallelly execution, generally considering only write or read operations
  
private:
  bool is_inited_;
  bool is_stopped_;
  int tg_id_; // thread group id
  int thread_cnt_;
  const char *base_uri_;
  OSDQMetric *metric_;
  OSDQFileSet *file_set_;
  const share::ObBackupStorageInfo *storage_info_;
  ObBackupIoAdapter adapter_;
  lib::ObMutex mutex_; 
  std::vector<int, STLMemAllocator<int>> req_results_;
};


/**
 *  @class OSDQScene
 *  @brief base class for scenarios
 */
class OSDQScene
{
public:
  OSDQScene();
  virtual ~OSDQScene();
  virtual int init(const OSDQParameters *param, OSDQMetric *metric) = 0;
  virtual bool param_valid(const OSDQParameters *param) = 0;
  virtual int execute() = 0;
  int loop_send_task();
  int set_thread_cnt(const int64_t thread_cnt);

protected:
  bool is_inited_;
  int64_t run_time_s_;
  OSDQMetric *metric_;
  const char *base_uri_;
  share::ObBackupStorageInfo storage_info_;
  OSDQTaskHandler task_handler_;
  OSDQFileSet file_set_;
};

/**
 *  @class OSDQHybridTestScene
 *  @brief Hybrid scene
 */
class OSDQHybridTestScene : public OSDQScene 
{
public:
  OSDQHybridTestScene();
  virtual ~OSDQHybridTestScene() {} 
  virtual int init(const OSDQParameters *param, OSDQMetric *metric) override;
  virtual bool param_valid(const OSDQParameters *param) override;
  virtual int execute() override;
};

/**
 *  @class OSDQResourceLimitedScene
 *  @brief resource limited scene, contain:
 *          - 0 network packet loss resource limited   
 *          - 1 network bandwidth limited 
 *          - 2 memory limited 
 *          - 3 cpu limited 
 */
class OSDQResourceLimitedScene : public OSDQScene
{
  enum ResourceLimitedType 
  {
    NETWORK_PACKET_LOSS_LIMITED_TYPE,
    NETWORK_BANDWIDTH_LIMITED_TYPE,
    MEMORY_LIMITED_TYPE,
    CPU_LIMITED_TYPE,
    MAX_RESOURCE_LIMITED_TYPE
  };
public:
  OSDQResourceLimitedScene();
  virtual ~OSDQResourceLimitedScene() {}
  virtual int init(const OSDQParameters *param, OSDQMetric *metric) override;
  virtual bool param_valid(const OSDQParameters *param) override;
  virtual int execute() override;

private:
  int test_network_packet_loss_limit_();
  int test_network_bandwidth_limit_();
  int test_memory_limit_();
  int test_cpu_limit_();
  static void inner_disrupt_network_(
      const int64_t sleep_time_s, 
      const char *class_handle,
      std::mutex &mtx,
      std::condition_variable &cv,
      bool &ready);
  static void inner_limit_memory_(
      const int64_t sleep_time_s, 
      const int64_t limit_memory_size_mb,
      std::mutex &mtx,
      std::condition_variable &cv,
      bool &ready);
  static void inner_limit_cpu_(
      const int64_t sleep_time_s, 
      const double cpu_rate,
      std::mutex &mtx,
      std::condition_variable &cv,
      bool &ready);

private:
  static constexpr int64_t TASK_HANDLER_DEFAULT_THREAD_CNT = 16;
  static constexpr double FINAL_CPU_USAGE_LIMIT = 5;
  ResourceLimitedType resource_limited_type_;
  int64_t limit_run_time_s_;
  int64_t limit_memory_mb_;
  double limit_cpu_;
};

class OSDQErrSimScene : public OSDQScene
{
public:
  OSDQErrSimScene();
  virtual ~OSDQErrSimScene() {}
  virtual int init(const OSDQParameters *param, OSDQMetric *metric) override;
  virtual bool param_valid(const OSDQParameters *param) override;
  virtual int execute() override;

};

} // namespace tools
} // namespace oceanbase

#endif
