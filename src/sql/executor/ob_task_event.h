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
#include "common/ob_partition_key.h"
#include "common/object/ob_object.h"
#include "sql/executor/ob_slice_id.h"
#include "sql/executor/ob_interm_result_item.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_scanner.h"
#include "sql/ob_sql_trans_util.h"
#include "share/rpc/ob_batch_proxy.h"

namespace oceanbase {
namespace sql {
class ObEvalCtx;
// these classes may used as parameter of asynchronous callback of rpc, which will not
// call destructor, so all these classes must free memory not in destructor.

class ObTaskSmallResult {
  OB_UNIS_VERSION(1);

public:
  const static int64_t MAX_DATA_BUF_LEN = 8 * 1024L;

  ObTaskSmallResult();
  virtual ~ObTaskSmallResult();

  void reset();
  bool equal(const ObTaskSmallResult& other) const;
  int assign(const ObTaskSmallResult& other);
  int assign_from_ir_item(const ObIIntermResultItem& ir_item);
  inline void set_has_data(bool has_data)
  {
    has_data_ = has_data;
  }
  inline bool has_data() const
  {
    return has_data_;
  }
  inline bool has_empty_data() const
  {
    return has_data_ && 0 == data_len_;
  }
  inline int64_t get_data_len() const
  {
    return data_len_;
  }
  inline void set_data_len(int64_t data_len)
  {
    data_len_ = data_len;
  }
  inline const char* get_data_buf() const
  {
    return data_buf_;
  }
  inline char* get_data_buf_for_update()
  {
    return data_buf_;
  }
  void set_found_rows(const int64_t count)
  {
    found_rows_ = count;
  }
  int64_t get_found_rows() const
  {
    return found_rows_;
  }
  void set_affected_rows(const int64_t count)
  {
    affected_rows_ = count;
  }
  int64_t get_affected_rows() const
  {
    return affected_rows_;
  }
  void set_last_insert_id(const int64_t id)
  {
    last_insert_id_ = id;
  }
  int64_t get_last_insert_id() const
  {
    return last_insert_id_;
  }
  void set_duplicated_rows(int64_t duplicated_rows)
  {
    duplicated_rows_ = duplicated_rows;
  }
  int64_t get_duplicated_rows() const
  {
    return duplicated_rows_;
  }
  void set_matched_rows(int64_t matched_rows)
  {
    matched_rows_ = matched_rows;
  }
  int64_t get_matched_rows() const
  {
    return matched_rows_;
  }
  TO_STRING_KV(K_(has_data), K_(data_len), K_(affected_rows), K_(found_rows), K_(last_insert_id), K_(matched_rows),
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

enum ObShuffleType {
  ST_NONE = 0,
  ST_HASH,
  ST_RANGE,
  ST_KEY,
  ST_LIST,
};

class ObShuffleKey final {
  OB_UNIS_VERSION(1);

public:
  ObShuffleKey() : type_(ST_NONE)
  {}
  void reset()
  {
    type_ = ST_NONE;
    values_[0].reset();
    values_[1].reset();
  }
  int assign(common::ObIAllocator& allocator, const ObShuffleKey& other);
  bool equal(const ObShuffleKey& other) const;
  bool match(const ObShuffleKey& other) const;
  int compare(const ObShuffleKey& other, int& cmp) const;
  void set_shuffle_type(ObShuffleType shuffle_type)
  {
    type_ = shuffle_type;
  }
  ObShuffleType get_shuffle_type() const
  {
    return type_;
  }
  common::ObObj& get_value0()
  {
    return values_[0];
  }
  common::ObObj& get_value1()
  {
    return values_[1];
  }
  int set_shuffle_type(const share::schema::ObTableSchema& table_schema);
  int set_sub_shuffle_type(const share::schema::ObTableSchema& table_schema);
  DECLARE_TO_STRING;

private:
  ObShuffleType type_;
  common::ObObj values_[2];
  static const int64_t HASH_IDX;
  static const int64_t COUNT_IDX;
  static const int64_t LOWER_IDX;
  static const int64_t UPPER_IDX;
};

class ObShuffleKeys final {
  OB_UNIS_VERSION(1);

public:
  ObShuffleKeys() : part_key_(), subpart_key_()
  {}
  void reset();
  int assign(common::ObIAllocator& allocator, const ObShuffleKeys& other);
  int compare(const ObShuffleKeys& other, bool cmp_part, bool cmp_subpart, int& cmp) const;
  TO_STRING_KV(K_(part_key), K_(subpart_key));

public:
  ObShuffleKey part_key_;
  ObShuffleKey subpart_key_;
};

class ObSliceEvent final {
  OB_UNIS_VERSION(1);

public:
  ObSliceEvent() : ob_slice_id_(), shuffle_keys_(), shuffle_partition_key_(), small_result_()
  {}

  void reset();
  bool equal(const ObSliceEvent& other) const;
  // used for normal copy, such as array.puah_back().
  int assign(const ObSliceEvent& other);
  // used for deep copy.
  int assign(common::ObIAllocator& allocator, const ObSliceEvent& other);

  void set_ob_slice_id(const ObSliceID& ob_slice_id)
  {
    ob_slice_id_ = ob_slice_id;
  }
  const ObSliceID& get_ob_slice_id() const
  {
    return ob_slice_id_;
  }
  const ObShuffleKeys& get_shuffle_keys() const
  {
    return shuffle_keys_;
  }
  ObShuffleKey& get_part_shuffle_key()
  {
    return shuffle_keys_.part_key_;
  }
  ObShuffleKey& get_subpart_shuffle_key()
  {
    return shuffle_keys_.subpart_key_;
  }
  const common::ObPartitionKey& get_shuffle_part_key() const
  {
    return shuffle_partition_key_;
  }
  common::ObPartitionKey& get_shuffle_part_key()
  {
    return shuffle_partition_key_;
  }
  const ObTaskSmallResult& get_small_result() const
  {
    return small_result_;
  }
  ObTaskSmallResult& get_small_result_for_update()
  {
    return small_result_;
  }
  bool has_small_result() const
  {
    return small_result_.has_data();
  }
  const char* get_small_result_buf() const
  {
    return small_result_.get_data_buf();
  }
  int64_t get_small_result_len() const
  {
    return small_result_.get_data_len();
  }
  bool has_data() const
  {
    return small_result_.has_data();
  }
  bool has_empty_data() const
  {
    return small_result_.has_empty_data();
  }
  int64_t get_found_rows() const
  {
    return small_result_.get_found_rows();
  }
  int64_t get_affected_rows() const
  {
    return small_result_.get_affected_rows();
  }
  int64_t get_matched_rows() const
  {
    return small_result_.get_matched_rows();
  }
  int64_t get_duplicated_rows() const
  {
    return small_result_.get_duplicated_rows();
  }

  TO_STRING_KV(K_(ob_slice_id), K_(shuffle_keys), K_(shuffle_partition_key), K_(small_result));

private:
  ObSliceID ob_slice_id_;
  ObShuffleKeys shuffle_keys_;
  common::ObPartitionKey shuffle_partition_key_;
  ObTaskSmallResult small_result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSliceEvent);
};

class ObTaskEvent {
  OB_UNIS_VERSION(1);

public:
  ObTaskEvent();
  virtual ~ObTaskEvent();

  bool equal(const ObTaskEvent& other) const;
  int assign(common::ObIAllocator& allocator, const ObTaskEvent& other);
  int init(const ObTaskLocation& task_loc, int64_t err_code);
  virtual void reset();

  inline const ObTaskLocation& get_task_location() const
  {
    return task_loc_;
  }
  inline int64_t get_err_code() const
  {
    return err_code_;
  }
  // used for partition retry.
  inline void rewrite_err_code(int64_t err)
  {
    err_code_ = err;
  }
  inline bool is_valid() const
  {
    return inited_ && task_loc_.is_valid();
  }
  inline const common::ObIArray<ObSliceEvent>& get_slice_events() const
  {
    return slice_events_;
  }
  int add_slice_event(const ObSliceEvent& slice_event);
  inline void set_task_recv_done(int64_t ts)
  {
    ts_task_recv_done_ = ts;
  }
  inline void set_result_send_begin(int64_t ts)
  {
    ts_result_send_begin_ = ts;
  }
  inline int64_t get_task_recv_done() const
  {
    return ts_task_recv_done_;
  }
  inline int64_t get_result_send_begin() const
  {
    return ts_result_send_begin_;
  }
  TO_STRING_KV("task_loc", task_loc_, "err_code", err_code_, "inited", inited_, K_(slice_events));

protected:
  ObTaskLocation task_loc_;
  int64_t err_code_;
  bool inited_;
  common::ObSEArray<ObSliceEvent, 1> slice_events_;
  // trace.
  int64_t ts_task_recv_done_;
  int64_t ts_result_send_begin_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskEvent);
};

class ObTaskCompleteEvent : public ObTaskEvent {
  OB_UNIS_VERSION(1);

public:
  ObTaskCompleteEvent() : ObTaskEvent(), extend_info_(), trans_result_(){};
  virtual ~ObTaskCompleteEvent(){};

  virtual void reset();
  bool equal(const ObTaskCompleteEvent& other) const;
  int assign(common::ObIAllocator& allocator, const ObTaskCompleteEvent& other);

  const common::ObString& get_extend_info() const
  {
    return extend_info_;
  }
  void set_extend_info(common::ObString& info)
  {
    extend_info_ = info;
  }
  int merge_trans_result(TransResult& trans_result)
  {
    return trans_result_.merge_result(trans_result);
  }
  // when batched_multi_statement is on, we will combine multiple update statements in
  // multi query into a multi upadte plan, so the result information such as affected
  // row of each statement should be recorded here separately.
  int merge_implicit_cursors(const common::ObIArray<ObImplicitCursorInfo>& implicit_cursors);
  const ObIArray<ObImplicitCursorInfo>& get_implicit_cursors() const
  {
    return implicit_cursors_;
  }
  inline const TransResult& get_trans_result() const
  {
    return trans_result_;
  }

protected:
  common::ObString extend_info_;
  TransResult trans_result_;
  common::ObSEArray<ObImplicitCursorInfo, 500> implicit_cursors_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTaskCompleteEvent);
};

class ObTaskResultBuf {
  OB_UNIS_VERSION_V(1);

public:
  ObTaskResultBuf()
  {}
  virtual ~ObTaskResultBuf()
  {}
  void reset()
  {
    task_location_.reset();
    slice_events_buf_.reset();
  }
  void set_task_location(const ObTaskLocation& task_location)
  {
    task_location_ = task_location;
  }
  const ObTaskLocation& get_task_location() const
  {
    return task_location_;
  }
  ObTaskLocation& get_task_location()
  {
    return task_location_;
  }
  int add_slice_event(const ObSliceEvent& slice_event);
  const common::ObIArray<ObSliceEvent>& get_slice_events() const
  {
    return slice_events_buf_;
  }
  TO_STRING_KV(K_(task_location), K_(slice_events_buf));

protected:
  ObTaskLocation task_location_;
  common::ObSEArray<ObSliceEvent, 1> slice_events_buf_;
};

class ObMiniTaskResult {
  OB_UNIS_VERSION(1);

public:
  ObMiniTaskResult(bool use_compact_row = true)
      : task_result_(common::ObModIds::OB_NEW_SCANNER, nullptr, common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
            common::OB_SERVER_TENANT_ID, use_compact_row),
        extend_result_(common::ObModIds::OB_NEW_SCANNER, nullptr, common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
            common::OB_SERVER_TENANT_ID, use_compact_row)
  {}
  ObMiniTaskResult(common::ObIAllocator& allocator, bool use_compact_row = true)
      : task_result_(allocator, common::ObModIds::OB_NEW_SCANNER, common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
            common::OB_SERVER_TENANT_ID, use_compact_row),
        extend_result_(allocator, common::ObModIds::OB_NEW_SCANNER, common::ObScanner::DEFAULT_MAX_SERIALIZE_SIZE,
            common::OB_SERVER_TENANT_ID, use_compact_row)
  {}
  ~ObMiniTaskResult()
  {}
  void set_tenant_id(uint64_t tenant_id)
  {
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
  // only merge row store from mini task result
  int append_mini_task_result(const ObMiniTaskResult& mini_task_result, bool is_static_engine = false);
  int assign(const ObMiniTaskResult& other);
  inline common::ObScanner& get_task_result()
  {
    return task_result_;
  }
  inline const common::ObScanner& get_task_result() const
  {
    return task_result_;
  }
  inline common::ObScanner& get_extend_result()
  {
    return extend_result_;
  }
  inline const common::ObScanner& get_extend_result() const
  {
    return extend_result_;
  }
  TO_STRING_KV(K_(task_result), K_(extend_result));

private:
  common::ObScanner task_result_;
  // mini task can taek a sub plan to get the rowkey of conflict rows caused by unique index,
  // extend_result_ will store these rowkeys as result, to reduce rpc times.
  common::ObScanner extend_result_;
};

class ObMiniTaskEvent {
public:
  ObMiniTaskEvent(const common::ObAddr& task_addr, int64_t task_id, int32_t ret_code)
      : task_addr_(task_addr), task_id_(task_id), ret_code_(ret_code), task_result_(false)
  {}
  ~ObMiniTaskEvent()
  {}
  // release resources.
  inline void close_event()
  {
    reset();
  }
  void reset()
  {
    task_result_.reset();
  }
  int init()
  {
    return task_result_.init();
  }
  int set_mini_task_result(const ObMiniTaskResult& mini_task_result)
  {
    return task_result_.assign(mini_task_result);
  }
  inline const ObMiniTaskResult& get_task_result() const
  {
    return task_result_;
  }
  inline int32_t get_errcode() const
  {
    return ret_code_;
  }
  inline int64_t get_task_id() const
  {
    return task_id_;
  }
  TO_STRING_KV(K_(task_addr), K_(task_id), K_(ret_code));

private:
  const ObAddr task_addr_;  // debug purpose when minitask async call fail
  int64_t task_id_;
  int32_t ret_code_;
  ObMiniTaskResult task_result_;
};

class ObMiniTaskRetryInfo {
public:
  ObMiniTaskRetryInfo()
      : need_retry_(true),
        failed_task_lists_(),
        retry_ret_(common::OB_SUCCESS),
        retry_times_(0),
        retry_execution_(false),
        retry_by_single_range_(false),
        not_master_tasks_()
  {}
  virtual ~ObMiniTaskRetryInfo() = default;
  void process_minitask_event(const ObMiniTaskEvent& event, int task_ret, int extend_ret);
  inline bool need_retry() const
  {
    return need_retry_ && !failed_task_lists_.empty();
  };
  void never_retry()
  {
    need_retry_ = false;
  }
  void add_retry_times()
  {
    ++retry_times_;
  }
  inline int64_t get_retry_times()
  {
    return retry_times_;
  }
  const common::ObIArray<std::pair<int64_t, int64_t> >& get_task_list() const
  {
    return failed_task_lists_;
  }
  common::ObIArray<int64_t>& get_not_master_task_id()
  {
    return not_master_tasks_;
  }
  inline void reuse_task_list()
  {
    failed_task_lists_.reuse();
  }
  inline void reuse()
  {
    reuse_task_list();
    need_retry_ = true;
    retry_ret_ = OB_SUCCESS;
    not_master_tasks_.reuse();
  }
  int add_not_master_task_id(int64_t task_id)
  {
    return not_master_tasks_.push_back(task_id);
  }
  inline void do_retry_execution()
  {
    retry_execution_ = true;
  }
  inline bool is_retry_execution()
  {
    return retry_execution_;
  }
  inline void set_retry_by_single_range()
  {
    retry_by_single_range_ = true;
  }
  inline bool retry_by_single_range() const
  {
    return retry_by_single_range_;
  }
  TO_STRING_KV(K_(need_retry), K_(failed_task_lists), K_(retry_ret), K_(retry_times), K_(retry_execution),
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

class ObRemoteResult : public obrpc::ObIFill {
  OB_UNIS_VERSION(1);

public:
  ObRemoteResult() : task_id_(), result_(), has_more_(false)
  {}
  int init()
  {
    return result_.init();
  }
  ObTaskID& get_task_id()
  {
    return task_id_;
  }
  const ObTaskID& get_task_id() const
  {
    return task_id_;
  }
  common::ObScanner& get_scanner()
  {
    return result_;
  }
  void set_has_more(bool has_more)
  {
    has_more_ = has_more;
  }
  bool has_more() const
  {
    return has_more_;
  }
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  virtual int64_t get_estimate_size() const
  {
    return result_.get_data_size();
  }
  TO_STRING_KV(K_(task_id), K_(result), K_(has_more));

private:
  ObTaskID task_id_;
  common::ObScanner result_;
  bool has_more_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_OB_TASK_EVENT_ */
