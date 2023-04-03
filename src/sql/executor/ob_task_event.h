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

#ifndef OCEANBASE_SQL_EXECUTOR_OB_TASK_EVENT_
#define OCEANBASE_SQL_EXECUTOR_OB_TASK_EVENT_

#include "sql/executor/ob_task_location.h"
#include "share/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "sql/executor/ob_slice_id.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_scanner.h"
#include "sql/ob_sql_trans_util.h"
#include "share/rpc/ob_batch_proxy.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace sql
{
struct ObEvalCtx;
// 由于调用了rpc的异步回调接口，该接口不调用参数的析构函数，
// 因此下面的类都要做成不依赖调用析构函数来释放内存

class ObTaskSmallResult
{
  OB_UNIS_VERSION(1);
public:
  const static int64_t MAX_DATA_BUF_LEN = 4 * 1024L; // 4k

  ObTaskSmallResult();
  virtual ~ObTaskSmallResult();

  void reset();
  bool equal(const ObTaskSmallResult &other) const;
  int assign(const ObTaskSmallResult &other);
  inline void set_has_data(bool has_data) { has_data_ = has_data; }
  inline bool has_data() const { return has_data_; }
  inline bool has_empty_data() const { return has_data_ && 0 == data_len_; }
  inline int64_t get_data_len() const { return data_len_; }
  inline void set_data_len(int64_t data_len) { data_len_ = data_len; }
  inline const char *get_data_buf() const { return data_buf_; }
  inline char *get_data_buf_for_update() { return data_buf_; }
  void set_found_rows(const int64_t count) { found_rows_ = count; }
  int64_t get_found_rows() const { return found_rows_; }
  void set_affected_rows(const int64_t count) { affected_rows_ = count; }
  int64_t get_affected_rows() const { return affected_rows_; }
  void set_last_insert_id(const int64_t id) { last_insert_id_ = id; }
  int64_t get_last_insert_id() const { return last_insert_id_; }
  void set_duplicated_rows(int64_t duplicated_rows) { duplicated_rows_ = duplicated_rows; }
  int64_t get_duplicated_rows() const { return duplicated_rows_; }
  void set_matched_rows(int64_t matched_rows) { matched_rows_ = matched_rows; }
  int64_t get_matched_rows() const { return matched_rows_; }
  TO_STRING_KV(K_(has_data),
               K_(data_len),
               K_(affected_rows),
               K_(found_rows),
               K_(last_insert_id),
               K_(matched_rows),
               K_(duplicated_rows));
private:
  bool has_data_;
  int64_t data_len_;
  char data_buf_[MAX_DATA_BUF_LEN];
  int64_t affected_rows_;
  int64_t found_rows_;
  int64_t last_insert_id_;
  int64_t matched_rows_;
  int64_t duplicated_rows_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskSmallResult);
};

enum ObShuffleType
{
  ST_NONE = 0,
  ST_HASH,
  ST_RANGE,
  ST_KEY,
  ST_LIST,
};

class ObSliceEvent final
{
  OB_UNIS_VERSION(1);
public:
  ObSliceEvent()
    : ob_slice_id_(),
      small_result_()
  {}

  void reset();
  bool equal(const ObSliceEvent &other) const;
  // used for normal copy, such as array.puah_back().
  int assign(const ObSliceEvent &other);
  // used for deep copy.
  int assign(common::ObIAllocator &allocator, const ObSliceEvent &other);

  void set_ob_slice_id(const ObSliceID &ob_slice_id) { ob_slice_id_ = ob_slice_id; }
  const ObSliceID &get_ob_slice_id() const { return ob_slice_id_; }
  const ObTaskSmallResult &get_small_result() const { return small_result_; }
  ObTaskSmallResult &get_small_result_for_update() { return small_result_; }
  bool has_small_result() const { return small_result_.has_data(); }
  const char *get_small_result_buf() const { return small_result_.get_data_buf(); }
  int64_t get_small_result_len() const { return small_result_.get_data_len(); }
  bool has_data() const { return small_result_.has_data(); }
  bool has_empty_data() const { return small_result_.has_empty_data(); }
  int64_t get_found_rows() const { return small_result_.get_found_rows(); }
  int64_t get_affected_rows() const { return small_result_.get_affected_rows(); }
  int64_t get_matched_rows() const { return small_result_.get_matched_rows(); }
  int64_t get_duplicated_rows() const { return small_result_.get_duplicated_rows(); }

  TO_STRING_KV(K_(ob_slice_id),
               K_(small_result));

private:
  ObSliceID ob_slice_id_;
  ObTaskSmallResult small_result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSliceEvent);
};

class ObTaskEvent
{
  OB_UNIS_VERSION(1);
public:
  ObTaskEvent();
  virtual ~ObTaskEvent();

  bool equal(const ObTaskEvent &other) const;
  int assign(common::ObIAllocator &allocator, const ObTaskEvent &other);
  int init(const ObTaskLocation &task_loc, int64_t err_code);
  virtual void reset();

  inline const ObTaskLocation &get_task_location() const {return task_loc_;}
  inline int64_t get_err_code() const {return err_code_;}
  inline bool is_valid() const {return inited_ && task_loc_.is_valid();}
  inline void set_task_recv_done(int64_t ts)    { ts_task_recv_done_    = ts; }
  inline void set_result_send_begin(int64_t ts) { ts_result_send_begin_ = ts; }
  inline int64_t get_task_recv_done() const    { return ts_task_recv_done_; }
  inline int64_t get_result_send_begin() const { return ts_result_send_begin_; }
  TO_STRING_KV("task_loc", task_loc_,
               "err_code", err_code_,
               "inited", inited_);
protected:
  ObTaskLocation task_loc_;
  int64_t err_code_;
  bool inited_;
  int64_t ts_task_recv_done_;
  int64_t ts_result_send_begin_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskEvent);
};

class ObMiniTaskResult
{
  OB_UNIS_VERSION(1);
public:
  ObMiniTaskResult(bool use_compact_row = true)
      : task_result_(common::ObModIds::OB_NEW_SCANNER,
                     nullptr,
                     common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
                     common::OB_SERVER_TENANT_ID,
                     use_compact_row),
      extend_result_(common::ObModIds::OB_NEW_SCANNER,
                       nullptr,
                       common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
                       common::OB_SERVER_TENANT_ID,
                       use_compact_row)
  {
  }
  ObMiniTaskResult(common::ObIAllocator &allocator, bool use_compact_row = true)
      : task_result_(allocator,
                     common::ObModIds::OB_NEW_SCANNER,
                     common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
                     common::OB_SERVER_TENANT_ID,
                     use_compact_row),
        extend_result_(allocator,
                       common::ObModIds::OB_NEW_SCANNER,
                       common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
                       common::OB_SERVER_TENANT_ID,
                       use_compact_row)
  {
  }
  ~ObMiniTaskResult()
  {
  }
  void set_tenant_id(uint64_t tenant_id) {
    task_result_.set_tenant_id(tenant_id);
    extend_result_.set_tenant_id(tenant_id);
  }
  int init()
  {
    int ret = common::OB_SUCCESS;
    ret = task_result_.init();
    if (OB_SUCC(ret)) {
      ret = extend_result_.init();
    }
    return ret;
  }
  inline void reset()
  {
    task_result_.reset();
    extend_result_.reset();
  }
  inline void reuse()
  {
    task_result_.reuse();
    extend_result_.reuse();
  }
  //only merge row store from mini task result
  int append_mini_task_result(const ObMiniTaskResult &mini_task_result);
  int assign(const ObMiniTaskResult &other);
  inline common::ObScanner &get_task_result() { return task_result_; }
  inline const common::ObScanner &get_task_result() const { return task_result_; }
  inline common::ObScanner &get_extend_result() { return extend_result_; }
  inline const common::ObScanner &get_extend_result() const { return extend_result_; }
  TO_STRING_KV(K_(task_result), K_(extend_result));
private:
  common::ObScanner task_result_;
  //对于mini task,允许在执行主计划的时候，附带执行一个扩展计划，
  //这个主要是用于当冲突检查的时候，获取unique index的冲突行主键的时候，
  //可以将主键和local unique index的冲突rowkey的其它列信息也一并带回来，优化RPC次数
  common::ObScanner extend_result_;
};

class ObMiniTaskEvent
{
public:
  ObMiniTaskEvent(const common::ObAddr &task_addr, int64_t task_id, int32_t ret_code)
      : task_addr_(task_addr),
        task_id_(task_id),
        ret_code_(ret_code),
        task_result_(false)
  {
  }
  ~ObMiniTaskEvent() {}
  //用来释放task event占用的资源
  inline void close_event() { reset(); }
  void reset() { task_result_.reset(); }
  int init() { return task_result_.init(); }
  int set_mini_task_result(const ObMiniTaskResult &mini_task_result) { return task_result_.assign(mini_task_result); }
  inline const ObMiniTaskResult &get_task_result() const { return task_result_; }
  inline int32_t get_errcode() const { return ret_code_; }
  inline int64_t get_task_id() const { return task_id_; }
  TO_STRING_KV(K_(task_addr), K_(task_id), K_(ret_code));
private:
  const ObAddr task_addr_; // debug purpose when minitask async call fail
  int64_t task_id_;
  int32_t ret_code_;
  ObMiniTaskResult task_result_;
};

class ObMiniTaskRetryInfo
{
public:
  ObMiniTaskRetryInfo() : need_retry_(true), failed_task_lists_(), retry_ret_(common::OB_SUCCESS),
  retry_times_(0), retry_execution_(false), retry_by_single_range_(false), not_master_tasks_() {}
  virtual ~ObMiniTaskRetryInfo() = default;
  void process_minitask_event(const ObMiniTaskEvent &event, int task_ret, int extend_ret);
  inline bool need_retry() const { return need_retry_ && !failed_task_lists_.empty(); };
  void never_retry() { need_retry_ = false; }
  void add_retry_times() { ++retry_times_; }
  inline int64_t get_retry_times() { return retry_times_; }
  const common::ObIArray<std::pair<int64_t, int64_t> > &get_task_list() const {
    return failed_task_lists_;
  }
  common::ObIArray<int64_t> &get_not_master_task_id() {
    return not_master_tasks_;
  }
  inline void reuse_task_list() {
    failed_task_lists_.reuse();
  }
  inline void reuse() {
    reuse_task_list();
    need_retry_ = true;
    retry_ret_ = OB_SUCCESS;
    not_master_tasks_.reuse();
  }
  int add_not_master_task_id(int64_t task_id) {
    return not_master_tasks_.push_back(task_id);
  }
  inline void do_retry_execution() { retry_execution_ = true; }
  inline bool is_retry_execution() { return retry_execution_; }
  inline void set_retry_by_single_range() { retry_by_single_range_ = true; }
  inline bool retry_by_single_range() const { return retry_by_single_range_; }
  TO_STRING_KV(K_(need_retry),
               K_(failed_task_lists),
               K_(retry_ret),
               K_(retry_times),
               K_(retry_execution),
               K_(retry_by_single_range));
private:
  bool need_retry_;
  // pair<task id : got rows count>
  common::ObSEArray<std::pair<int64_t, int64_t>, 16> failed_task_lists_;
  int retry_ret_;
  int64_t retry_times_;
  bool retry_execution_;
  bool retry_by_single_range_;
  common::ObSEArray<int64_t, 2> not_master_tasks_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMiniTaskRetryInfo);
};

class ObRemoteResult : public obrpc::ObIFill
{
  OB_UNIS_VERSION(1);
public:
  ObRemoteResult() :
    task_id_(),
    result_(),
    has_more_(false)
  { }
  int init() { return result_.init(); }
  ObTaskID &get_task_id() { return task_id_; }
  const ObTaskID &get_task_id() const { return task_id_; }
  common::ObScanner &get_scanner() { return result_; }
  void set_has_more(bool has_more) { has_more_ = has_more; }
  bool has_more() const { return has_more_; }
  virtual int fill_buffer(char* buf, int64_t size, int64_t &filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const { return get_serialize_size(); }
  virtual int64_t get_estimate_size() const { return result_.get_data_size(); }
  TO_STRING_KV(K_(task_id), K_(result), K_(has_more));
private:
  ObTaskID task_id_;
  common::ObScanner result_;
  bool has_more_;
};
}
}
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_EVENT_ */
