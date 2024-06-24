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
 *
 * OBCDC Utilities
 */

#ifndef OCEANBASE_LIBOBCDC_UTILS_H__
#define OCEANBASE_LIBOBCDC_UTILS_H__

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/BR.h>                        // RecordType
#include <drcmsg/MsgWrapper.h>                // IStrArray
#include <drcmsg/MD.h>                        // ITableMeta
#include <drcmsg/DRCMessageFactory.h>
#endif

#include <sys/prctl.h>
#include "lib/allocator/ob_allocator.h"       // ObIAllocator
#include "lib/allocator/ob_malloc.h"          // ob_malloc
#include "lib/allocator/ob_mod_define.h"      // ObModIds
#include "lib/container/ob_iarray.h"          // ObIArray
#include "lib/container/ob_array.h"           // ObArray
#include "common/object/ob_object.h"          // ObObj
#include "share/schema/ob_column_schema.h"    // ObColumnSchemaV2
#include "storage/blocksstable/ob_datum_row.h"// ObRowDml
#include "share/schema/ob_schema_service.h"   // ObSchemaService
#include "share/inner_table/ob_inner_table_schema.h"   // OB_ALL_SEQUENCE_VALUE_TID
#include "ob_cdc_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace common
{
class ObString;
}

namespace libobcdc
{
/*
 * Memory size.
 */
static const int64_t _K_ = (1L << 10);
static const int64_t _M_ = (1L << 20);
static const int64_t _G_ = (1L << 30);
static const int64_t _T_ = (1L << 40);

static const char *COLUMN_VALUE_IS_EMPTY = "";
static const char *COLUMN_VALUE_IS_NULL = NULL;

/*
 * Time utils.
 * Microsecond Timestamp Generator.
 * Time Constants.
 * Stop Watch.
 * Time Marker.
*/
inline void usec_sleep(const int64_t u) { ob_usleep(u); }

typedef common::ObSEArray<uint64_t, 16> ObLogIdArray;
typedef common::ObSEArray<palf::LSN, 16> ObLogLSNArray;
#define TS_TO_STR(tstamp) HumanTstampConverter(tstamp).str()
#define NTS_TO_STR(tstamp) HumanTstampConverter(tstamp/NS_CONVERSION).str()
#define TVAL_TO_STR(tval) HumanTimevalConverter(tval).str()

const int64_t NS_CONVERSION = 1000L;
const int64_t _MSEC_ = 1000L;
const int64_t _SEC_ = 1000L * _MSEC_;
const int64_t _MIN_ = 60L * _SEC_;
const int64_t _HOUR_ = 60L * _MIN_;
const int64_t _DAY_ = 24L * _HOUR_;
const int64_t _YEAR_ = 365L * _DAY_;

int print_human_tstamp(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tstamp);

int print_human_timeval(char *buf, const int64_t buf_len, int64_t &pos,
    const int64_t usec_tval);

class HumanTstampConverter
{
public:
  explicit HumanTstampConverter(const int64_t usec_tstamp)
  {
    buf_[0] = '\0';
    int64_t pos = 0;
    (void)print_human_tstamp(buf_, BufLen, pos, usec_tstamp);
  }
  virtual ~HumanTstampConverter()
  {
    buf_[0] = '\0';
  }
  const char* str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

class HumanTimevalConverter
{
public:
  explicit HumanTimevalConverter(const int64_t usec_tval)
  {
    buf_[0] = '\0';
    int64_t pos = 0;
    (void)print_human_timeval(buf_, BufLen, pos, usec_tval);
  }
  virtual ~HumanTimevalConverter()
  {
    buf_[0] = '\0';
  }
  const char *str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

inline int64_t get_timestamp() { return ::oceanbase::common::ObTimeUtility::current_time(); }

class HumanDataSizeConverter
{
  static const int64_t BufSize = 128;
public:
  explicit HumanDataSizeConverter(const int64_t bytes) : bytes_(bytes) {}
  ~HumanDataSizeConverter() {}
  const char* to_data_size_cstr()
  {
    double val = 0;
    int64_t pos = 0;
    const char *unit = "";
    if (bytes_ < _K_) {
      val = (double)bytes_;
      unit = "B";
    }
    else if (bytes_ < _M_) {
      val = (double)bytes_ / (double)_K_;
      unit = "KB";
    }
    else if (bytes_ < _G_) {
      val = (double)bytes_ / (double)_M_;
      unit = "MB";
    }
    else {
      val = (double)bytes_ / (double)_G_;
      unit = "GB";
    }

    (void)common::databuff_printf(buf_, BufSize, pos, "%.2f%s", val, unit);

    return buf_;
  }
  const char *str()
  {
    return to_data_size_cstr();
  }
private:
  int64_t bytes_;
  char buf_[BufSize];
};

// Converting data sizes to strings
#define SIZE_TO_STR(size) HumanDataSizeConverter(size).str()

class TstampToDelay
{
public:
  explicit TstampToDelay(const int64_t tstamp)
  {
    if (common::OB_INVALID_TIMESTAMP == tstamp) {
      (void)snprintf(buf_, sizeof(buf_), "[INVALID]");
    } else {
      int64_t cur_time = get_timestamp();
      int64_t delay_us = (cur_time - tstamp) % _SEC_;
      int64_t delay_sec = (cur_time - tstamp) / _SEC_;

      buf_[0] = '\0';

      (void)snprintf(buf_, sizeof(buf_), "[%ld.%.06ld sec]", delay_sec, delay_us);
    }
  }
  ~TstampToDelay() { buf_[0] = '\0'; }
  const char *str() const
  {
    return buf_;
  }
private:
  const static int64_t BufLen = 64;
  char buf_[BufLen];
};

// Converting timestamps to DELAY strings.[1000.0001 sec]
#define TS_TO_DELAY(tstamp) TstampToDelay(tstamp).str()
#define NTS_TO_DELAY(tstamp) TstampToDelay(tstamp/NS_CONVERSION).str()

class StopWatch
{
public:
  StopWatch() : start_(0), elapsed_(0) { }
  virtual ~StopWatch() { }
public:
  void start() { start_ = get_timestamp(); }
  void pause() { elapsed_ += (get_timestamp() - start_); }
  void reset() { start_ = 0; elapsed_ = 0; }
  double elapsed_sec() const { return static_cast<double>(
                                      elapsed_msec()) / 1000.0; }
  int64_t elapsed_msec() const { return elapsed_usec() / 1000; }
  int64_t elapsed_usec() const { return elapsed_; }

private:
  int64_t start_;
  int64_t elapsed_;
};

int get_local_ip(common::ObString &local_ip);

RecordType get_record_type(const blocksstable::ObDmlRowFlag &dml_flag);
const char *print_dml_flag(const blocksstable::ObDmlRowFlag &dml_flag);
const char *print_record_type(int type);
const char *print_src_category(int src_category);
const char *print_record_src_type(int type);
const char *print_table_status(int status);
// Print compatible mode
const char *print_compat_mode(const lib::Worker::CompatMode &compat_mode);
const char *get_ctype_string(int ctype);
bool is_lob_type(const int ctype);
bool is_string_type(const int ctype);
bool is_json_type(const int ctype);
bool is_geometry_type(const int ctype);
bool is_xml_type(const int ctype);
bool is_roaringbitmap_type(const int ctype);
int64_t get_non_hidden_column_count(const oceanbase::share::schema::ObTableSchema &table_schema);

double get_delay_sec(const int64_t tstamp);

bool is_ddl_table(const uint64_t table_id);
int64_t get_ddl_table_id();

// Is  MySQL Client Error Code
bool is_mysql_client_errno(int err);
// Is MySQL Server Error Code
bool is_mysql_server_errno(int err);

// Is ERROR CODE of OB SQL
bool is_ob_sql_errno(int err);

// Is ERROR CODE of OB TRANS
bool is_ob_trans_errno(int err);

// Is ERROR CODE of OB ELECTION
bool is_ob_election_errno(int err);

// Encapsulated temporary memory allocator
void *ob_cdc_malloc(
    const int64_t nbyte,
    const lib::ObLabel &lable = ObModIds::OB_LOG_TEMP_MEMORY,
    const uint64_t tenant_id = OB_SERVER_TENANT_ID);
void ob_cdc_free(void *ptr);

class ObLogBufAllocator : public common::ObIAllocator
{
public:
  ObLogBufAllocator(char *buf, const int64_t buf_size, int64_t &used_buf_len) :
      buf_(buf),
      buf_size_(buf_size),
      used_buf_len_(used_buf_len)
  {
  };
  void *alloc(const int64_t size)
  {
    char *ret = NULL;
    if (NULL != buf_ && (used_buf_len_ + size) <= buf_size_) {
      ret = buf_ + used_buf_len_;
      used_buf_len_ += size;
    }
    return ret;
  };
private:
  char *const buf_;
  const int64_t buf_size_;
  int64_t &used_buf_len_;
};

void column_cast(common::ObObj &obj, const share::schema::ObColumnSchemaV2 &column_schema);
class ColumnSchemaInfo;
void column_cast(common::ObObj &obj, const ColumnSchemaInfo &column_schema_info);

inline void set_cdc_thread_name(const char* name, const int64_t thread_idx = -1)
{
  if (OB_NOT_NULL(name)) {
    char* tname = ob_get_tname();
    if (thread_idx < 0) {
      snprintf(tname, OB_THREAD_NAME_BUF_LEN, "%s", name);
    } else {
      snprintf(tname, OB_THREAD_NAME_BUF_LEN, "%s_%ld", name, thread_idx);
    }
    prctl(PR_SET_NAME, tname);
  }
}

/*
 * Runnable.
 * Call create() to run a thread, join() to wait till it dies.
 * Write code running in thread in routine(). Its error code is returned
 * from join().
 */
class Runnable
{
  typedef Runnable MyType;
public:
  Runnable() : thread_(), joinable_(false) { }
  virtual ~Runnable() { }
  int create();
  int join();
  bool is_joinable() const { return joinable_; }
protected:
  virtual int routine() = 0;
private:
  static void* pthread_routine(void* arg);
private:
  pthread_t thread_;
  bool joinable_;
private:
  DISALLOW_COPY_AND_ASSIGN(Runnable);
};

// filter inner table

class BackupTableHelper
{
private:
  // Add table by TID increment
  static constexpr uint64_t inner_table_ids[] = {
    share::OB_ALL_SEQUENCE_VALUE_TID  // 215
  };

private:
  BackupTableHelper() { }
  virtual ~BackupTableHelper() { }
public:
  static bool is_sys_table_exist_on_backup_mode(const bool is_sys_table, const uint64_t table_id);
  static int get_table_ids_on_backup_mode(common::ObIArray<uint64_t> &table_ids);
};

// key-value collection
// key1${sp1}val1${sp2}key2${sp1}val2${sp2}key3${sp1}val3
//
// currently memory of key/value managed by user!
// input kv_str will be modified by serialize/deserialize of kv pair!
class ObLogKVCollection
{
public:
  // key-value pair
  class KVPair
  {
    public:
      KVPair() { reset(); }
      virtual ~KVPair() { reset(); }

      int init(const char* delimiter);
      void reset()
      {
        key_ = NULL;
        value_ = NULL;
        delimiter_ = NULL;
        inited_ = false;
      }
      int set_key_and_value(const char* key, const char* value);
      const char* get_key() const { return key_; }
      const char* get_value() const { return value_; }
      int length() const;
      bool is_valid() const;
      // set_key_and_value before use this
      // output to a k-v string, linked by provider splitor
      //
      // @param [out]   buf       buf store serialized kv_str(key${delimiter}value)
      // @param [in]    buf_len   total buf size
      // @param [out]   pos       current modified buf position
      int serialize(char* buf, int64_t buf_len, int64_t &pos);
      // deserialize string to KV Pair, with split
      int deserialize(char* buf);
      TO_STRING_KV(K_(inited), K_(delimiter), KP_(key), K_(key), KP_(value), K_(value));

    private:
      bool inited_;
      const char* key_;
      const char* value_;
      const char* delimiter_;
  };
public:
  ObLogKVCollection() {reset();}
  virtual ~ObLogKVCollection() { reset(); }

public:
  int init(const char* kv_delimiter, const char* pair_delimiter);
  void reset()
  {
    kv_pairs_.reset();
    kv_delimiter_ = NULL;
    pair_delimiter_ = NULL;
    inited_ = false;
  }
  // @param [input] kv_pair
  int append_kv_pair(KVPair &kv_pair);
  bool is_valid() const;
  // return number of kv pairs
  int64_t size() const { return kv_pairs_.size(); }
  // return length of kv_str(prediction)
  int length() const;
  // serialize this collection to a kv-string
  int serialize(char* kv_str_output, const int64_t kv_str_len, int64_t &pos);
  // deserialize string to KV Pair, with split
  int deserialize(char* buf);
  int contains_key(const char* key, bool &contain);
  int get_value_of_key(const char *key, const char *&value);
  TO_STRING_KV(K_(inited), K_(kv_delimiter), K_(pair_delimiter), K_(kv_pairs));

private:
  common::ObArray<KVPair> kv_pairs_;
  bool inited_;
  const char* kv_delimiter_;
  const char* pair_delimiter_;
};

/// split string by separator
///
/// @param [in]   str            str to split
/// @param [in]   delimiter      delimiter/separator
/// @param [int]  expect_res_cnt expected res count
/// @param [out]  res            split result array
/// @param [out]  res_cnt        count of split result
///
/// @retval OB_SUCCESS          split success
/// @retval other_error_code    Fail
int split(
    char *str,
    const char *delimiter,
    const int64_t expect_res_cnt,
    const char **res,
    int64_t &res_cnt);

int split_int64(const common::ObString &str, const char delimiter, common::ObIArray<int64_t> &ret_array);

const char *calc_md5_cstr(const char *buf, const int64_t length);
void calc_crc_checksum(uint64_t &crc_value, const char *buf, const int64_t length);

template <class T, class CompareFunc>
int top_k(const common::ObArray<T> &in_array,
    const int64_t k_num,
    common::ObArray<T> &out_array,
    CompareFunc &compare_func)
{
  int ret = common::OB_SUCCESS;
  int64_t array_cnt = in_array.count();
  int64_t cnt = std::min(k_num, array_cnt);

  for (int64_t idx = 0; common::OB_SUCCESS == ret && idx < cnt; ++idx) {
    if (OB_FAIL(out_array.push_back(in_array.at(idx)))) {
      OBLOG_LOG(ERROR, "push back into slow array fail", KR(ret), K(idx));
    } else {
      // do nothing
    }
  }

  if (common::OB_SUCCESS == ret && array_cnt > 0) {
    if (array_cnt <= k_num) {
      // Construct a big top heap, with the top of the heap being the maximum value in the current out_array
      std::make_heap(out_array.begin(), out_array.end(), compare_func);
    } else {
      // Construct a big top heap, with the top of the heap being the maximum value in the current out_array
      std::make_heap(out_array.begin(), out_array.end(), compare_func);

      for (int64_t idx = k_num; common::OB_SUCCESS == ret && idx < array_cnt; ++idx) {
        // If the current element is smaller than the heap top element, replace the heap top element and re-std::make_heap
        if (compare_func(in_array.at(idx), out_array.at(0))) {
          out_array[0] = in_array.at(idx);
          std::make_heap(out_array.begin(), out_array.end(), compare_func);
        } else {
          // do nothing
        }
      } // for
    }

    if (common::OB_SUCCESS == ret) {
      std::sort_heap(out_array.begin(), out_array.end(), compare_func);
    }
  }

  return ret;
}

int deep_copy_str(const ObString &src,
    ObString &dest,
    common::ObIAllocator &allocator);

int get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode,
    volatile bool &stop_flag);

int get_tenant_compat_mode(const uint64_t tenant_id,
    lib::Worker::CompatMode &compat_mode,
    const int64_t timeout);

char *lbt_oblog();

bool is_backup_mode();

struct BRColElem
{
  BRColElem(const char *col_value, size_t col_value_len) : col_value_(col_value), col_value_len_(col_value_len) {}
  BRColElem() { reset(); }
  ~BRColElem() { reset(); }

  void reset()
  {
    col_value_ = NULL;
    col_value_len_ = 0;
  }

  TO_STRING_KV(K_(col_value), K_(col_value_len));

  const char *col_value_;
  size_t col_value_len_;
};

class ObLogTimeMonitor
{
public:
  ObLogTimeMonitor(const char* log_msg_prefix, bool enable = true);
  ~ObLogTimeMonitor();
  // return cost to last mark time
  int64_t mark_and_get_cost(const char *log_msg_suffix, bool need_print = false);
private:
  bool enable_;
  const char* log_msg_prefix_;
  int64_t start_time_usec_;
  int64_t last_mark_time_usec_;
};

int get_br_value(IBinlogRecord *br,
    ObArray<BRColElem> &new_values);
int get_mem_br_value(IBinlogRecord *br,
    std::string &key_str,
    ObArray<BRColElem> &new_values);

int print_unserilized_br_value(IBinlogRecord *br,
    const char* key_c_str,
    const char* trans_id_c_str);
int print_serilized_br_value(std::string &key,
    const std::string &drc_message_factory_binlog_record_type,
    const char *br_string,
    const size_t br_string_len);

int c_str_to_int(const char* str, int64_t &num);

// for sys_table, table_id == tablet_id
bool is_ddl_tablet(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);

bool is_all_ddl_operation_lob_aux_tablet(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id);

// CDCLSNComparator used to sort LSN array(Ascending order)
struct CDCLSNComparator
{
  bool operator() (const palf::LSN &a, const palf::LSN &b)
  {
    return a < b;
  }
};
// sort and unique lsn arr.
// NOT THREAD_SAFE
int sort_and_unique_lsn_arr(ObLogLSNArray &lsn_arr);

// sort arr and remove duplicate item in arr
// 1. Item in array should impl copy-assign
// 2. comparator should compare Item in array, and should obey rule of std::sort
template<class ARRAY, class Comparator>
int sort_and_unique_array(ARRAY &arr, Comparator &comparator)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> duplicated_item_idx_arr;

  if (arr.count() > 1) {
    // sort lsn_arr
    lib::ob_sort(arr.begin(), arr.end(), comparator);
    auto prev = arr.at(0);
    // get duplicate misslog lsn idx
    for(int64_t idx = 1; OB_SUCC(ret) && idx < arr.count(); idx++) {
      auto &cur = arr.at(idx);
      if (prev == cur) {
        if (OB_FAIL(duplicated_item_idx_arr.push_back(idx))) {
          OBLOG_LOG(WARN, "push_back_duplicate_item_arr fail", KR(ret), K(cur), K(prev), K(idx));
        }
      }
      if (OB_SUCC(ret)) {
        prev = cur;
      }
    }

    // remove duplicate misslog lsn
    for(int64_t idx = duplicated_item_idx_arr.count() - 1; OB_SUCC(ret) && idx >= 0; idx--) {
      int64_t duplicate_item_idx = duplicated_item_idx_arr[idx];
      if (OB_UNLIKELY(0 > duplicate_item_idx || duplicate_item_idx > arr.count())) {
        ret = OB_INVALID_ARGUMENT;
        OBLOG_LOG(WARN, "invalid duplicate_cur_lsn_idx", KR(ret), K(arr), K(duplicated_item_idx_arr), K(idx), K(duplicate_item_idx));
      } else if (OB_FAIL(arr.remove(duplicate_item_idx))) {
        OBLOG_LOG(WARN, "remove_duplicate_item failed", KR(ret), K(arr), K(duplicate_item_idx));
      } else {
      }
    }
    OBLOG_LOG(DEBUG, "sort_and_unique_array", KR(ret), K(duplicated_item_idx_arr), K(arr));
  }

  return ret;
}

typedef int32_t offset_t;

// write specified buf to specified file.
int write_to_file(const char *file_path, const char *buf, const int64_t buf_len);
// read content from specified file to buffer
// NOTE: buf should be allocated by invoker.
// @retval OB_SIZE_OVERFLOW buf_len is not enough.
// @retval OB_IO_ERROR      open or read file error.
// @retval OB_INVALID_CONFIG file_path is not valid or buf is NULL of buf_len <= 0
// @retval OB_EMPTY_RESULT  nothing exist in file.
int read_from_file(const char *file_path, char *buf, const int64_t buf_len);

#define RETRY_FUNC_ON_ERROR_WITH_USLEEP_MS(err_no, sleep_ms, stop_flag, var, func, args...) \
  do {\
    if (OB_SUCC(ret)) \
    { \
      int64_t _retry_func_on_error_last_print_time = common::ObClockGenerator::getClock();\
      int64_t _retry_func_on_error_cur_print_time = 0;\
      const int64_t _PRINT_RETRY_FUNC_INTERVAL = 10 * _SEC_;\
      ret = (err_no); \
      while ((err_no) == ret && ! (stop_flag)) \
      { \
        ret = ::oceanbase::common::OB_SUCCESS; \
        ret = (var).func(args); \
        if (err_no == ret) { \
          ob_usleep(sleep_ms); \
        }\
        _retry_func_on_error_cur_print_time = common::ObClockGenerator::getClock();\
        if (_retry_func_on_error_cur_print_time - _retry_func_on_error_last_print_time >= _PRINT_RETRY_FUNC_INTERVAL) {\
          _OBLOG_LOG(INFO, "It has been %ld us since last print, last_print_time=%ld, func_name=%s", \
              _PRINT_RETRY_FUNC_INTERVAL, _retry_func_on_error_last_print_time, #func);\
          _retry_func_on_error_last_print_time = _retry_func_on_error_cur_print_time;\
        }\
      } \
      if ((stop_flag)) \
      { \
        ret = OB_IN_STOP_STATE; \
      } \
    } \
  } while (0)

// convert to compat mode
int convert_to_compat_mode(const common::ObCompatibilityMode &compatible_mode,
    lib::Worker::CompatMode &compat_mode);

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_UTILS_H__ */
