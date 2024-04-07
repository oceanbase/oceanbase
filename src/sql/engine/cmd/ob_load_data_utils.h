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

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashmap.h"
#include "common/object/ob_object.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/engine/ob_exec_context.h"

#ifndef OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_UTILS_H_
#define OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_UTILS_H_
namespace oceanbase
{
namespace sql {

enum class ObLoadTaskResultFlag;
enum class ObLoadDupActionType;
class ObSQLSessionInfo;

typedef common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE_FOR_BASE_COLUMN> ObExprValueBitSet;

/* A state machine to handle backslash from a char stream */
class ObLoadEscapeSM {
public:
  static const int64_t ESCAPE_CHAR_MYSQL = static_cast<int64_t>('\\');
  static const int64_t ESCAPE_CHAR_ORACLE = static_cast<int64_t>('\'');
  ObLoadEscapeSM()
    : is_escaped_flag_(false), escape_char_(INT64_MAX), escaped_char_count(0) {}
  OB_INLINE void shift_by_input(char c)
  {
    /* there are 4 situations:
     * 1. STATE : c == \\ && is_escaped_flag_ == True     Action: is_escaped_flag_ = False
     * 2. STATE : c == \\ && is_escaped_flag_ == False    Action: is_escaped_flag_ = True
     * 3. STATE : c != \\ && is_escaped_flag_ == True     Action: is_escaped_flag_ = False
     * 4. STATE : c != \\ && is_escaped_flag_ == False    Action: is_escaped_flag_ = False
     * only state 1-3 need to change is_escaped_flag_, but usual case is state 4
     */
    if (OB_LIKELY(static_cast<int64_t>(c) != escape_char_ && !is_escaped_flag_)) {
      //situation 4, do nothing
    } else {
      //situation 1-3
      is_escaped_flag_ = !is_escaped_flag_;
      if (is_escaped_flag_) {
        escaped_char_count++;
      }
    }
  }
  OB_INLINE bool is_escaping() { return is_escaped_flag_; }
  void set_escape_char(int64_t escape_char) { escape_char_ = escape_char; }
  void reset() { is_escaped_flag_ = false; escaped_char_count = 0; }
  int64_t get_escaped_char_count() { return escaped_char_count; }
private:
  bool is_escaped_flag_;
  int64_t escape_char_;
  int64_t escaped_char_count;
};

class ObLoadDataUtils {
public:

  static const char *NULL_STRING;
  static const char NULL_VALUE_FLAG;

  static inline void remove_last_slash(common::ObString &value)
  {
    const char *data = value.ptr();
    int32_t data_len = value.length();
    if (OB_LIKELY(data_len) > 0 && OB_UNLIKELY(data[data_len - 1] == '\\')) {
      bool is_escaped = false;
      for (int32_t i = data_len - 2; i >= 0 && data[i] == '\\'; --i) {
        is_escaped = !is_escaped;
      }
      if (!is_escaped) {
        value.assign_ptr(data, data_len - 1);
      }
    }
  }

  static inline int str_write_buf(const common::ObString &str, char *&buf, int64_t &buf_len) {
    int ret = common::OB_SUCCESS;
    int64_t data_len = str.length();
    if (OB_UNLIKELY(buf_len <= data_len)) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(buf, str.ptr(), data_len);
      buf += data_len;
      buf_len -= data_len;
    }
    return ret;
  }

  static inline int char_write_buf(char c, char *&buf, int64_t &buf_len) {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(buf_len <= 1)) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      buf[0] = c;
      buf++;
      buf_len--;
    }
    return ret;
  }

  static inline int escape_str_write_buf(const common::ObHexEscapeSqlStr &str,
                                         char *&buf, int64_t &buf_len) {
    int ret = common::OB_SUCCESS;
    int64_t data_len = str.to_string(buf, buf_len);
    if (OB_UNLIKELY(buf_len <= data_len)) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      buf += data_len;
      buf_len -= data_len;
    }
    return ret;
  }

  static inline bool is_null_field(const common::ObString &field_str) {
    int ret_bool = false;
    if (field_str.length() == 1 && *field_str.ptr() == NULL_VALUE_FLAG) {
      ret_bool = true;
    }
    return ret_bool;
  }

  static inline bool is_zero_field(const common::ObString &field_str) {
    int ret_bool = false;
    if (field_str.length() == 2
        && field_str.ptr()[0] == '\xff'
        && field_str.ptr()[1] == '\xff') {
      ret_bool = true;
    }
    return ret_bool;
  }

  static common::ObString escape_quotation(const common::ObString &value, common::ObDataBuffer &data_buf);
  static int init_empty_string_array(common::ObIArray<common::ObString> &new_array, int64_t array_size);

  static int build_insert_sql_string_head(ObLoadDupActionType insert_mode,
                                          const common::ObString &table_name,
                                          const common::ObIArray<common::ObString> &insert_keys,
                                          common::ObSqlString &insertsql_keys,
                                          bool need_gather_opt_stat = false);

  static int check_need_opt_stat_gather(ObExecContext &ctx,
                                        ObLoadDataStmt &load_stmt,
                                        bool &need_opt_stat_gather);

  static int append_values_in_remote_process(int64_t table_column_count,
                                             int64_t append_values_count,
                                             const ObExprValueBitSet &expr_bitset,
                                             const common::ObIArray<common::ObString> &insert_values,
                                             common::ObSqlString &insertsql,
                                             common::ObDataBuffer &data_buffer,
                                             int64_t skipped_row_count = 0);
  static int append_values_for_one_row(const int64_t table_column_count,
                                       const ObExprValueBitSet &expr_value_bitset,
                                       const common::ObIArray<common::ObString> &insert_values,
                                       common::ObSqlString &insertsql,
                                       common::ObDataBuffer &data_buffer,
                                       const int64_t skipped_row_count = 0);
  static int append_value(const common::ObString &cur_column_str,
                          common::ObSqlString &sqlstr_values,
                          bool is_expr_value);
  static int append_values_in_local_process(const int64_t key_columns,
                                            const int64_t values_count,
                                            const common::ObIArray<common::ObString> &insert_values,
                                            const ObExprValueBitSet &expr_value_bitset,
                                            common::ObSqlString &insertsql,
                                            common::ObDataBuffer &data_buffer);

  static inline bool has_flag(int64_t &task_status, int64_t flag) { return 0 != (task_status & (1<<flag)); }
  static inline void set_flag(int64_t &task_status, int64_t flag) { task_status |= (1<<flag); }

  static int check_session_status(ObSQLSessionInfo &session, int64_t reserved_us = 0);
};


class ObLoadTaskStatus {
public:
  ObLoadTaskStatus(): task_status_(0) {}
  enum class ResFlag
  {
    HAS_FAILED_ROW = 0,
    ALL_ROWS_FAILED,
    NEED_WAIT_MINOR_FREEZE,
    TIMEOUT,
    RPC_CALLBACK_PROCESS_ERROR,
    RPC_REMOTE_PROCESS_ERROR,
    INVALID_MAX_FLAG
   };
  static_assert(static_cast<int64_t>(ResFlag::INVALID_MAX_FLAG) < 64,
                "ObLoadTaskResultFlag max value should less than bit size of int64_t");
  OB_INLINE void set_flag(ResFlag flag) { task_status_ |= (1 << static_cast<int64_t>(flag)); }
  OB_INLINE bool has_flag(ResFlag flag) { return 0 != (task_status_ & (1 << static_cast<int64_t>(flag))); }
  TO_STRING_KV(K_(task_status));
  OB_UNIS_VERSION(1);
private:
  int64_t task_status_;
};


class ObLoadDataTimer
{
public:
  ObLoadDataTimer(): total_time_us_(0), temp_start_time_us_(-1) {}
  OB_INLINE void start_stat()
  {
    UNUSED(temp_start_time_us_);
#ifdef TIME_STAT_ON
    temp_start_time_us_ = ObTimeUtility::current_time();
#endif
  }
  OB_INLINE void end_stat() {
#ifdef TIME_STAT_ON
    if (temp_start_time_us_ != -1) {
      total_time_us_ += (ObTimeUtility::current_time() - temp_start_time_us_);
      temp_start_time_us_ = -1;
    }
#endif
  }
  int64_t get_wait_secs() const {
    return total_time_us_/1000000;
  }
  TO_STRING_KV("secs", get_wait_secs());
private:
  int64_t total_time_us_;
  int64_t temp_start_time_us_;
};

/*
 * ObKMPStateMachine is a str matcher
 * efficiently implemented using KMP algorithm
 * to detect a given str from a char stream
 */
class ObKMPStateMachine
{
public:
  ObKMPStateMachine(): is_inited_(false), str_(NULL), str_len_(0), matched_pos_(0), next_(NULL) {}
  int init(common::ObIAllocator &allocator, const common::ObString &str);
  /*
   * accept one char at a time, and update the state of the detector
   * return true if succ matching target str ending with the current char
   */
  OB_INLINE bool accept_char(const char c)
  {
    bool ret_bool = false;
    if (OB_UNLIKELY(!is_inited_)) {
      SQL_ENG_LOG_RET(ERROR, common::OB_NOT_INIT, "ObKmpSeparatorDetector not inited.");
    } else {
      while (matched_pos_ > 0 && c != str_[matched_pos_]) {
        matched_pos_ = next_[matched_pos_];
      }
      if (c == str_[matched_pos_]) {
        matched_pos_++;
      }
      if (matched_pos_ == str_len_) {
        matched_pos_ = 0;
        ret_bool = true;
      }
    }
    return ret_bool;
  }
  OB_INLINE int32_t get_pattern_length() { return str_len_; }
  bool scan_buf(char *&cur_pos, const char* buf_end);
  void reuse() { matched_pos_ = 0; }
private:
  static const int KEY_WORD_MAX_LENGTH = 2 * 1024;
  bool is_inited_;
  char* str_;         //string pattern for matching
  int32_t str_len_;
  int32_t matched_pos_;   //can opt to pointer
  int32_t* next_;         //next array of KMP algorithm
};

struct ObLoadDataGID
{
  static volatile int64_t GlobalLoadDataID;
  static void generate_new_id(ObLoadDataGID &gid)
  {
    gid.id = ATOMIC_AAF(&GlobalLoadDataID, 1);
  }
  ObLoadDataGID() : id(-1) {}
  void reset() { id = -1; }
  bool is_valid() const { return id  > 0; }
  uint64_t hash() const { return common::murmurhash(&id, sizeof(id), 0); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator==(const ObLoadDataGID &other) const { return id == other.id; }
  void operator=(const ObLoadDataGID &other) { id = other.id; }
  int64_t id;
  TO_STRING_KV(K(id));
  OB_UNIS_VERSION(1);
};


struct ObLoadDataStat
{
  ObLoadDataStat() : allocator_(ObModIds::OB_SQL_LOAD_DATA),
                     ref_cnt_(0),
                     tenant_id_(0),
                     job_id_(0),
                     job_type_("normal"),
                     table_name_(),
                     file_path_(),
                     table_column_(0),
                     file_column_(0),
                     batch_size_(0),
                     parallel_(1),
                     load_mode_(0),
                     start_time_(0),
                     estimated_remaining_time_(0),
                     total_bytes_(0),
                     read_bytes_(0),
                     parsed_bytes_(0),
                     parsed_rows_(0),
                     total_shuffle_task_(0),
                     total_insert_task_(0),
                     shuffle_rt_sum_(0),
                     insert_rt_sum_(0),
                     total_wait_secs_(0),
                     max_allowed_error_rows_(0),
                     detected_error_rows_(0),
                     coordinator_(),
                     store_(),
                     message_() {}
  int64_t aquire() {
    return ATOMIC_AAF(&ref_cnt_, 1);
  }
  int64_t release() {
    return ATOMIC_AAF(&ref_cnt_, -1);
  }
  int64_t get_ref_cnt() { return ATOMIC_LOAD(&ref_cnt_); }

  common::ObArenaAllocator allocator_;
  volatile int64_t ref_cnt_;
  int64_t tenant_id_;
  int64_t job_id_;
  common::ObString job_type_; // normal / direct
  common::ObString table_name_;
  common::ObString file_path_;
  int64_t table_column_;
  int64_t file_column_;
  int64_t batch_size_;
  int64_t parallel_;
  int64_t load_mode_;
  int64_t start_time_;
  int64_t estimated_remaining_time_;
  int64_t total_bytes_;
  volatile int64_t read_bytes_;  //bytes read to memory
  volatile int64_t parsed_bytes_;
  volatile int64_t parsed_rows_;
  int64_t total_shuffle_task_;
  int64_t total_insert_task_;
  int64_t shuffle_rt_sum_;
  int64_t insert_rt_sum_;
  int64_t total_wait_secs_;
  int64_t max_allowed_error_rows_;
  int64_t detected_error_rows_;
  struct coordinator {
    coordinator()
      : received_rows_(0),
        last_commit_segment_id_(0),
        status_("none"),
        trans_status_("none")
    {}
    volatile int64_t received_rows_; // received from client
    int64_t last_commit_segment_id_;
    common::ObString status_; // none / inited / loading / frozen / merging / commit / error / abort
    common::ObString trans_status_; // none / inited / running / frozen / commit / error / abort
    TO_STRING_KV(K(received_rows_), K(last_commit_segment_id_), K(status_), K(trans_status_));
  } coordinator_;
  struct store {
    store()
      : processed_rows_(0),
        last_commit_segment_id_(0),
        status_("none"),
        trans_status_("none"),
        compact_stage_load_rows_(0),
        compact_stage_dump_rows_(0),
        compact_stage_product_tmp_files_(0),
        compact_stage_consume_tmp_files_(0),
        compact_stage_merge_write_rows_(0),
        merge_stage_write_rows_(0)
    {}
    volatile int64_t processed_rows_;
    int64_t last_commit_segment_id_;
    common::ObString status_;
    common::ObString trans_status_;
    int64_t compact_stage_load_rows_ CACHE_ALIGNED;
    int64_t compact_stage_dump_rows_ CACHE_ALIGNED;
    int64_t compact_stage_product_tmp_files_ CACHE_ALIGNED;
    int64_t compact_stage_consume_tmp_files_ CACHE_ALIGNED;
    int64_t compact_stage_merge_write_rows_ CACHE_ALIGNED;
    int64_t merge_stage_write_rows_ CACHE_ALIGNED;
    TO_STRING_KV(K(processed_rows_), K(last_commit_segment_id_), K(status_), K(trans_status_),
                 K(compact_stage_load_rows_), K(compact_stage_dump_rows_),
                 K(compact_stage_product_tmp_files_), K(compact_stage_consume_tmp_files_),
                 K(compact_stage_merge_write_rows_), K(merge_stage_write_rows_));
  } store_;
  char message_[common::MAX_LOAD_DATA_MESSAGE_LENGTH];

  TO_STRING_KV(K(tenant_id_), K(job_id_), K(job_type_),
      K(table_name_), K(file_path_), K(table_column_), K(file_column_),
      K(batch_size_), K(parallel_), K(load_mode_),
      K(start_time_), K(estimated_remaining_time_),
      K(total_bytes_), K(read_bytes_), K(parsed_bytes_),
      K(parsed_rows_), K(total_shuffle_task_), K(total_insert_task_),
      K(shuffle_rt_sum_), K(insert_rt_sum_), K(total_wait_secs_),
      K(max_allowed_error_rows_), K(detected_error_rows_),
      K(coordinator_), K(store_), K(message_));
};

class ObGetAllJobStatusOp
{
public:
  ObGetAllJobStatusOp();
  ~ObGetAllJobStatusOp();

public:
  void reset();
  int operator()(common::hash::HashMapPair<ObLoadDataGID, ObLoadDataStat*> &entry);
  int get_next_job_status(ObLoadDataStat *&job_status);

private:
  common::ObSEArray<ObLoadDataStat *, 10> job_status_array_;
  int32_t current_job_index_;
};

class ObLoadDataStatGuard
{
public:
  ObLoadDataStatGuard() : stat_(nullptr) {}
  ObLoadDataStatGuard(const ObLoadDataStatGuard &rhs) : stat_(nullptr)
  {
    aquire(rhs.stat_);
  }
  ~ObLoadDataStatGuard()
  {
    release();
  }

  void aquire(ObLoadDataStat *stat)
  {
    release();
    stat_ = stat;
    if (nullptr != stat_) {
      stat_->aquire();
    }
  }

  void release()
  {
    if (nullptr != stat_) {
      stat_->release();
      stat_ = nullptr;
    }
  }

  ObLoadDataStat *get() const { return stat_; }

  // ObLoadDataStat *operator->() { return stat_; }
  // const ObLoadDataStat *operator->() const { return stat_; }

  ObLoadDataStatGuard &operator=(const ObLoadDataStatGuard &rhs)
  {
    aquire(rhs.stat_);
    return *this;
  }

  TO_STRING_KV(KPC_(stat));

private:
  ObLoadDataStat *stat_;
};

class ObGlobalLoadDataStatMap
{
public:
  static ObGlobalLoadDataStatMap *getInstance();
  ObGlobalLoadDataStatMap() : is_inited_(false) {}
  int init();
  int register_job(const ObLoadDataGID &id, ObLoadDataStat *job_status);
  int unregister_job(const ObLoadDataGID &id, ObLoadDataStat *&job_status);
  int get_job_status(const ObLoadDataGID &id, ObLoadDataStat *&job_status);
  int get_all_job_status(ObGetAllJobStatusOp &job_status_op);
  int get_job_stat_guard(const ObLoadDataGID &id, ObLoadDataStatGuard &guard);
private:
  typedef common::hash::ObHashMap<ObLoadDataGID, ObLoadDataStat*,
          common::hash::SpinReadWriteDefendMode> HASH_MAP;
  static const int64_t bucket_num = 1000;
  static ObGlobalLoadDataStatMap *instance_;
  HASH_MAP map_;
  bool is_inited_;
};

}
}


#endif // OCEANBASE_SQL_ENGINE_CMD_LOAD_DATA_UTILS_H_
