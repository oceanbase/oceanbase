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

#ifndef OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_RPC_H_
#define OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_RPC_H_

#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "lib/container/ob_bit_set.h"
#include "lib/lock/ob_thread_cond.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace observer
{
struct ObGlobalContext;
}

namespace sql {
class ObLoadbuffer;
class ObLoadResult;
class ObLoadFileBuffer;
class ObDesExecContext;
struct ObShuffleTask;
struct ObShuffleResult;
struct ObInsertTask;
struct ObInsertResult;
class ObDataFragMgr;
struct ObDataFrag;
struct ObShuffleTaskHandle;
class ObInsertValueGenerator;
class ObPartIdCalculator;
class ObPartDataFragMgr;
}

namespace obrpc
{
class ObLoadDataRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObLoadDataRpcProxy);
  RPC_AP(@PR5 ap_load_data_execute, obrpc::OB_LOAD_DATA_EXECUTE, (sql::ObLoadbuffer), sql::ObLoadResult);
  RPC_AP(@PR5 ap_load_data_shuffle, obrpc::OB_LOAD_DATA_SHUFFLE, (sql::ObShuffleTask), sql::ObShuffleResult);
  RPC_AP(@PR5 ap_load_data_insert, obrpc::OB_LOAD_DATA_INSERT, (sql::ObInsertTask), sql::ObInsertResult);
};
}

namespace sql {

static const int64_t DEFAULT_BUFFERRED_ROW_COUNT = 100; //must < 2^15
static const int64_t DEFAULT_PARALLEL_THREAD_COUNT = 4;
static const int64_t EXPECTED_INSERT_COLUMN_NUM = 64;
static const int64_t RPC_BATCH_INSERT_TIMEOUT_US = 10 * 1000 * 1000; //10s


enum class ObLoadTaskResultFlag {
  HAS_FAILED_ROW = 0,
  ALL_ROWS_FAILED,
  NEED_WAIT_MINOR_FREEZE,
  TIMEOUT,
  RPC_CALLBACK_PROCESS_ERROR,
  RPC_REMOTE_PROCESS_ERROR,
  INVALID_MAX_FLAG
};

static_assert(static_cast<int64_t>(ObLoadTaskResultFlag::INVALID_MAX_FLAG) < 64,
              "ObLoadTaskResultFlag max value should less than 64");

enum class ObTaskResFlag {
  RPC_TIMEOUT = 0,
  NEED_WAIT_MINOR_FREEZE,
  HAS_FAILED_ROW,
  ALL_ROW_FAILED,
  //SHUTDOWN_WITH_INTERNAL_ERROR,

  MAX_VALUE
};


typedef ObBitSet<32> ErrRowBitset;


// 单线程调用 on_next_task_id 多线程调用 on_task_finished 时，是无锁的
class ObParallelTaskController
{
public:
  ObParallelTaskController() :
    max_parallelism_(0), task_cnt_(0), processing_cnt_(0) {}
  ~ObParallelTaskController() {}
  int init(int64_t max_parallelism);
  int on_next_task();
  int on_task_finished();
  int64_t get_next_task_id() { return task_cnt_++; }
  void wait_all_task_finish(const char* task_name = NULL, int64_t until_ts = INT64_MAX);
  int64_t get_processing_task_cnt() { return ATOMIC_LOAD(&processing_cnt_); }
  int64_t get_total_task_cnt() { return task_cnt_; }
  int64_t get_max_parallelism() { return max_parallelism_; }
private:
  static const int64_t MAX_TIME_WAIT_MS = 2 * RPC_BATCH_INSERT_TIMEOUT_US / 1000;
  int64_t max_parallelism_;
  int64_t task_cnt_;

  //multi-thread values:
  volatile int64_t processing_cnt_;
  common::ObThreadCond vacant_cond_; //wait on (processing_cnt_ > MaxConcurrentTaskNum)
};

struct ObInsertResult
{
  ObInsertResult() : exec_ret_(0), err_line_no_(0) {}
  void reset() {
    flags_.reset();
    exec_ret_ = 0;
    err_line_no_ = 0;
    err_msg_.reset();
    allocator_.reset();
  }
  int assign(const ObInsertResult &other);
  ObEnumBitSet<ObTaskResFlag> flags_;
  int exec_ret_;
  int64_t err_line_no_;
  common::ObString err_msg_;
  //ErrRowBitset failed_row_offset_;
  //common::Ob2DArray<int> row_errors_;
  //common::Ob2DArray<common::ObString> row_error_msgs_;
  ObArenaAllocator allocator_;
  TO_STRING_KV(K(exec_ret_), K(flags_), K(err_line_no_), K(err_msg_));
  OB_UNIS_VERSION(1);
};

struct ObInsertTask
{
  static constexpr int64_t RETRY_LIMIT = 3;
  static constexpr int64_t COMMON_SIZE = 10;

  ObInsertTask() { reset(); }

  void reuse()
  {
    task_id_ = common::OB_INVALID_ID;
    row_count_ = 0;
    insert_value_data_.reuse();
    source_frag_.reuse();
    result_.reset();
    result_recv_ts_ = 0;
    process_us_ = 0;
    retry_times_ = 0;
    part_mgr = NULL;
    data_size_ = 0;
  }

  void reset() {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    column_count_ = 0;
    reuse();
    token_server_idx_ = OB_INVALID_INDEX;
    insert_stmt_head_.reset();
    insert_value_data_.reset();
    source_frag_.reset();
    timezone_.reset();
    sql_mode_ = 0;
  }

  bool is_empty_task()
  {
    return task_id_ == OB_INVALID_ID;
  }

  TO_STRING_KV(K(tenant_id_),
               K(task_id_),
               K(row_count_),
               K(column_count_),
               K(insert_value_data_.count()));

  //serialized data:
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t row_count_;
  int64_t column_count_;
  common::ObString insert_stmt_head_; //insert into xxx (xxx)

  //insert_values_data_ 总共包括了row_count_行数据，这些数据存储在几个buff中，buff数据用ObString存储
  //每个buff是若干行数据的序列化块，每行数据的序列化格式：
  //1. length of 2           int64_t
  //2. values for one row    ObSEArray<ObString>  using string as buf
  // + for serialize
  common::ObSEArray<common::ObString, COMMON_SIZE> insert_value_data_;

  //no serialized data
  common::ObSEArray<void *, COMMON_SIZE> source_frag_;//一一对应insert_value_data_，表达数据来源
  ObPartDataFragMgr *part_mgr;
  ObInsertResult result_;
  int64_t result_recv_ts_;
  int64_t process_us_;
  int64_t retry_times_;
  int64_t token_server_idx_;
  int64_t data_size_;
  ObTimeZoneInfoWrap timezone_;
  int64_t sql_mode_;

  OB_UNIS_VERSION(1);
};

template<class T>
class ObRpcPointerArg {
public:
  ObRpcPointerArg() : ptr_value_(0) {}
  int set_arg(T *ptr) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ptr)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ptr_ = ptr;
    }
    return ret;
  }
  int get_arg(T *&ptr) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ptr = ptr_)) {
      ret = OB_ERR_NULL_VALUE;
    }
    return ret;
  }

private:
  union {
    uint64_t ptr_value_;
    T *ptr_;
  };
  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER_TEMP(template<class T>, ObRpcPointerArg<T>, ptr_value_);

struct ObShuffleTask {
  ObShuffleTask() : task_id_(OB_INVALID_INDEX_INT64) {}
  int64_t task_id_;
  ObRpcPointerArg<ObShuffleTaskHandle> shuffle_task_handle_;
  ObLoadDataGID gid_;
  TO_STRING_KV(K(task_id_), K(gid_));
  OB_UNIS_VERSION(1);
};

struct ObShuffleResult {
  ObShuffleResult() : task_id_(OB_INVALID_INDEX_INT64), flags_(), exec_ret_(0), row_cnt_(0), process_us_(0) {}
  void reset() {
    task_id_ = OB_INVALID_INDEX_INT64;
    flags_.reset();
    exec_ret_ = 0;
    row_cnt_ = 0;
    process_us_ = 0;
  }
  int64_t task_id_;
  ObEnumBitSet<ObTaskResFlag> flags_;
  int exec_ret_;
  int64_t row_cnt_;
  int64_t process_us_;
  OB_UNIS_VERSION(1);
};

template <typename T>
class ObConcurrentFixedCircularArray
{
public:
  ObConcurrentFixedCircularArray() : array_size_(0), data_(NULL), head_pos_(0), tail_pos_(0),
    lock_(common::ObLatchIds::LOAD_DATA_RPC_CB_LOCK) {}
  ~ObConcurrentFixedCircularArray() {
    if (data_ != NULL) {
      ob_free_align((void *)(data_));
    }
  }
  int init(int64_t array_size)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(data_ = static_cast<T *>(ob_malloc_align(CACHE_ALIGN_SIZE, array_size * sizeof(T),
                                                           "LoadData")))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "alloc memory failed", K(ret));
    } else {
      array_size_ = array_size;
    }
    return ret;
  }
  OB_INLINE int push_back(const T &obj) {
    common::ObSpinLockGuard guard(lock_);
    int ret = common::OB_SUCCESS;
    //push optimistically
    int64_t pos = ATOMIC_FAA(&head_pos_, 1);
    //validate
    if (OB_UNLIKELY(pos - ATOMIC_LOAD(&tail_pos_) >= array_size_)) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      ATOMIC_STORE(&data_[pos % array_size_], obj);
    }
    return ret;
  }
  OB_INLINE int pop(T &output)
  {
    common::ObSpinLockGuard guard(lock_);
    int ret = common::OB_SUCCESS;
    //pop optimistically
    int64_t pos = ATOMIC_FAA(&tail_pos_, 1);
    //validate
    if (OB_UNLIKELY(pos >= ATOMIC_LOAD(&head_pos_))) {
      ret = common::OB_ARRAY_OUT_OF_RANGE;
    } else {
      //output = ATOMIC_LOAD(&data_[pos % array_size_]);
      output = ATOMIC_SET(&data_[pos % array_size_], NULL);
    }
    return ret;
  }
  OB_INLINE int64_t count() {
    return ATOMIC_LOAD(&head_pos_) - ATOMIC_LOAD(&tail_pos_);
  }
private:
  // data members
  int64_t array_size_;
  volatile T * data_;
  volatile int64_t head_pos_;
  volatile int64_t tail_pos_;
  common::ObSpinLock lock_;
};

typedef ObConcurrentFixedCircularArray<ObLoadbuffer*> CompleteTaskArray;

//load data task buffer
class ObLoadbuffer
{
public:
  const static int64_t LOAD_BUFFER_MAX_ROW_COUNT = DEFAULT_BUFFERRED_ROW_COUNT;
  ObLoadbuffer (): tenant_id_(common::OB_INVALID_ID),
                   table_id_(common::OB_INVALID_ID),
                   insert_column_num_(0),
                   stored_row_cnt_(0),
                   stored_pos_(0),
                   task_id_(common::OB_INVALID_ID),
                   tablet_id_(),
                   task_status_(0),
                   insert_mode_(false),
                   returned_timestamp_(0),
                   field_data_allocator_("LoadData"),
                   array_allocator_("LoadData"),
                   insert_column_names_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                   expr_bitset_(array_allocator_),
                   insert_values_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                   file_line_number_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                   failed_inserted_row_idx_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_),
                   error_codes_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, array_allocator_)

  {}
  ~ObLoadbuffer() {}
  int init_array();
  int deep_copy_str(const common::ObString &src, common::ObString &dest);
  int store_row(const common::ObIArray<common::ObString> &row_strs,
                int64_t cur_line_number);
  OB_INLINE bool is_full() { return LOAD_BUFFER_MAX_ROW_COUNT == stored_row_cnt_; }
  OB_INLINE bool is_empty() { return 0 == stored_row_cnt_ && 0 == stored_pos_; }
  OB_INLINE void reuse() {
    task_status_ = 0;
    stored_row_cnt_ = 0;
    stored_pos_ = 0;
    task_id_ = common::OB_INVALID_ID;
    returned_timestamp_ = 0;
    ip_addr_.reset();
    field_data_allocator_.reuse();
    failed_inserted_row_idx_.reuse();
    error_codes_.reuse();
  }

  OB_INLINE void set_addr(common::ObAddr addr) { ip_addr_ = addr; }
  OB_INLINE common::ObAddr get_addr() { return ip_addr_; }
  OB_INLINE void set_returned_timestamp(int64_t timestamp) { returned_timestamp_ = timestamp; }
  OB_INLINE int64_t get_returned_timestamp() { return returned_timestamp_; }
  OB_INLINE void set_task_status(int64_t status) { task_status_ = status; }
  OB_INLINE int64_t &get_task_status() { return task_status_; }
  OB_INLINE void set_task_id(int64_t task_id) { task_id_ = task_id; }
  OB_INLINE int64_t get_task_id() { return task_id_; }
  OB_INLINE void set_tablet_id(ObTabletID tablet_id) { tablet_id_ = tablet_id; }
  OB_INLINE ObTabletID get_tablet_id() { return tablet_id_; }
  OB_INLINE void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  OB_INLINE uint64_t get_table_id() { return table_id_; }
  OB_INLINE void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE uint64_t get_tenant_id(){ return tenant_id_; }
  OB_INLINE void set_column_num(int64_t insert_column_num) { insert_column_num_ = insert_column_num; }
  OB_INLINE int64_t get_column_num(){ return insert_column_num_; }
  OB_INLINE int64_t get_stored_pos() { return stored_pos_; }
  OB_INLINE int64_t get_stored_row_count() { return stored_row_cnt_; }
  OB_INLINE void set_table_name(common::ObString& table_name) { table_name_ = table_name; }
  OB_INLINE common::ObString& get_table_name() { return table_name_; }
  OB_INLINE ObExprValueBitSet& get_expr_bitset() { return expr_bitset_; }
  OB_INLINE void set_load_mode(ObLoadDupActionType insert_mode)
  {
    insert_mode_ = static_cast<int64_t>(insert_mode);
  }
  OB_INLINE ObLoadDupActionType get_load_mode()
  {
    return static_cast<ObLoadDupActionType>(insert_mode_);
  }
  int set_allocator_tenant();
  int prepare_insert_info(const common::ObIArray<common::ObString> &column_names,
                          ObExprValueBitSet &expr_value_bitset);
  common::ObIArray<common::ObString>& get_insert_values() { return insert_values_; }
  common::ObIArray<common::ObString>& get_insert_keys() { return insert_column_names_; }
  common::ObIArray<int64_t>& get_file_line_number() { return file_line_number_; }
  common::ObIArray<int16_t>& get_failed_row_idx() { return failed_inserted_row_idx_; }
  common::ObIArray<int>& get_error_code_array() { return error_codes_; }
  TO_STRING_KV(K_(tenant_id),
               K_(table_id),
               K_(insert_column_num),
               K_(stored_row_cnt),
               K_(stored_pos),
               K_(task_id),
               K_(tablet_id),
               K_(task_status),
               K_(insert_mode),
               K_(returned_timestamp));
  OB_UNIS_VERSION(1);
private:
  //send params
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObString table_name_;
  int64_t insert_column_num_;
  int64_t stored_row_cnt_;
  int64_t stored_pos_;
  int64_t task_id_;
  ObTabletID tablet_id_;
  int64_t task_status_;
  int64_t insert_mode_;
  int64_t returned_timestamp_;
  common::ObAddr ip_addr_;
  //allocator
  common::ObArenaAllocator field_data_allocator_;
  common::ModulePageAllocator array_allocator_;
  //data arrays
  common::ObSEArray<common::ObString, EXPECTED_INSERT_COLUMN_NUM> insert_column_names_;
  ObExprValueBitSet expr_bitset_;  //true if the value is expr
  common::ObSEArray<common::ObString, LOAD_BUFFER_MAX_ROW_COUNT> insert_values_;
  //only used in host server
  common::ObSEArray<int64_t, LOAD_BUFFER_MAX_ROW_COUNT> file_line_number_;  //belongs to [0, file_total_line_number)
  common::ObSEArray<int16_t, LOAD_BUFFER_MAX_ROW_COUNT> failed_inserted_row_idx_;  //idx belongs to [0, LOAD_BUFFER_MAX_ROW_COUNT)
  common::ObSEArray<int, LOAD_BUFFER_MAX_ROW_COUNT> error_codes_;
};

//load data task result
class ObLoadResult
{
public:
  ObLoadResult(): task_id_(-1),
                  tablet_id_(),
                  affected_rows_(0),
                  failed_rows_(0),
                  task_flags_(false) {}
  TO_STRING_KV(K_(task_id), K_(tablet_id), K_(affected_rows), K_(failed_rows), K_(task_flags));
  int64_t task_id_;
  ObTabletID tablet_id_;
  int64_t affected_rows_;
  int64_t failed_rows_;
  int64_t task_flags_;
  common::ObSEArray<int16_t, DEFAULT_BUFFERRED_ROW_COUNT> row_number_;
  common::ObSEArray<int, DEFAULT_BUFFERRED_ROW_COUNT> row_err_code_;
  OB_UNIS_VERSION(1);
};

class ObRpcLoadDataTaskExecuteP : public oceanbase::obrpc::ObRpcProcessor<
    obrpc::ObLoadDataRpcProxy::ObRpc<obrpc::OB_LOAD_DATA_EXECUTE> >
{
public:
  explicit ObRpcLoadDataTaskExecuteP(const observer::ObGlobalContext &gctx) : gctx_(gctx), escape_data_buffer_() {}
  virtual ~ObRpcLoadDataTaskExecuteP() {}
protected:
  int process();
private:
  const observer::ObGlobalContext &gctx_;
  common::ObDataBuffer escape_data_buffer_;
  char str_buf_[common::OB_MAX_DEFAULT_VALUE_LENGTH];  //TODO wjh: change this
};

class ObRpcLoadDataTaskCallBack
      : public obrpc::ObLoadDataRpcProxy::AsyncCB<obrpc::OB_LOAD_DATA_EXECUTE>
{
public:
  ObRpcLoadDataTaskCallBack(ObParallelTaskController &task_controller,
                            CompleteTaskArray &complete_task_list,
                            Request *request)
    : task_controller_(task_controller),
      complete_task_list_(complete_task_list),
      request_buffer_ptr_(request) {}
  virtual void on_timeout();
  void set_args(const Request &arg);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    ObRpcLoadDataTaskCallBack *newcb = NULL;
    if (NULL != buf) {
      newcb = new(buf) ObRpcLoadDataTaskCallBack(task_controller_,
                                                 complete_task_list_,
                                                 request_buffer_ptr_);
    }
    return newcb;
  }
  int process();
private:
  ObParallelTaskController &task_controller_;
  CompleteTaskArray &complete_task_list_;
  Request *request_buffer_ptr_;
};

//load data V2

class ObRpcLoadDataShuffleTaskExecuteP : public oceanbase::obrpc::ObRpcProcessor<
    obrpc::ObLoadDataRpcProxy::ObRpc<obrpc::OB_LOAD_DATA_SHUFFLE> >
{
public:
  explicit ObRpcLoadDataShuffleTaskExecuteP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObRpcLoadDataShuffleTaskExecuteP() {}
protected:
  int process();
private:
  const observer::ObGlobalContext &gctx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
};

class ObRpcLoadDataShuffleTaskCallBack
      : public obrpc::ObLoadDataRpcProxy::AsyncCB<obrpc::OB_LOAD_DATA_SHUFFLE>
{
public:
  ObRpcLoadDataShuffleTaskCallBack(
      ObParallelTaskController &task_controller,
      ObConcurrentFixedCircularArray<ObShuffleTaskHandle *> &complete_task_list,
      ObShuffleTaskHandle *handle
      )
    : task_controller_(task_controller),
      complete_task_list_(complete_task_list),
      handle_(handle) {}
  virtual void on_timeout();
  void set_args(const Request &arg);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    ObRpcLoadDataShuffleTaskCallBack *newcb = NULL;
    if (NULL != buf) {
      newcb = new(buf) ObRpcLoadDataShuffleTaskCallBack(task_controller_,
                                                        complete_task_list_,
                                                        handle_);
    }
    return newcb;
  }
  int process();
  int release_resouce();
private:
  ObParallelTaskController &task_controller_;
  ObConcurrentFixedCircularArray<ObShuffleTaskHandle *> &complete_task_list_;
  ObShuffleTaskHandle *handle_;
};

class ObRpcLoadDataInsertTaskExecuteP : public oceanbase::obrpc::ObRpcProcessor<
    obrpc::ObLoadDataRpcProxy::ObRpc<obrpc::OB_LOAD_DATA_INSERT> >
{
public:
  explicit ObRpcLoadDataInsertTaskExecuteP(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObRpcLoadDataInsertTaskExecuteP() {}
protected:
  int process();
private:
  const observer::ObGlobalContext &gctx_;
};

class ObRpcLoadDataInsertTaskCallBack
      : public obrpc::ObLoadDataRpcProxy::AsyncCB<obrpc::OB_LOAD_DATA_INSERT>
{
public:
  ObRpcLoadDataInsertTaskCallBack(
      ObParallelTaskController &task_controller,
      ObConcurrentFixedCircularArray<ObInsertTask *> &complete_task_list,
      ObInsertTask *insert_task)
    : task_controller_(task_controller),
      complete_task_list_(complete_task_list),
      insert_task_(insert_task) {}
  virtual void on_timeout();
  void set_args(const Request &arg);
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    ObRpcLoadDataInsertTaskCallBack *newcb = NULL;
    if (NULL != buf) {
      newcb = new(buf) ObRpcLoadDataInsertTaskCallBack(task_controller_,
                                                       complete_task_list_,
                                                       insert_task_);
    }
    return newcb;
  }
  int process();
  int release_resouce();
private:
  ObParallelTaskController &task_controller_;
  ObConcurrentFixedCircularArray<ObInsertTask *> &complete_task_list_;
  ObInsertTask *insert_task_;
};

}
}

#endif // OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_RPC_H__
