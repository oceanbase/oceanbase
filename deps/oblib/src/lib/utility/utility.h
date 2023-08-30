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

#ifndef OCEANBASE_COMMON_UTILITY_H_
#define OCEANBASE_COMMON_UTILITY_H_

#include <arpa/inet.h>

#include "easy_define.h"
#include "io/easy_io_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/utility/ob_backtrace.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/container/ob_iarray.h"
#include "common/ob_clock_generator.h"

#define FALSE_IT(stmt) ({ (stmt); false; })
#define OB_FALSE_IT(stmt) ({ (stmt); false; })

#define CPUID_STD_SSE4_2 0x00100000

#define ob_htonll(i) \
  ( \
    (((uint64_t)i & 0x00000000000000ff) << 56) | \
    (((uint64_t)i & 0x000000000000ff00) << 40) | \
    (((uint64_t)i & 0x0000000000ff0000) << 24) | \
    (((uint64_t)i & 0x00000000ff000000) << 8) | \
    (((uint64_t)i & 0x000000ff00000000) >> 8) | \
    (((uint64_t)i & 0x0000ff0000000000) >> 24) | \
    (((uint64_t)i & 0x00ff000000000000) >> 40) | \
    (((uint64_t)i & 0xff00000000000000) >> 56)   \
  )

#define DEFAULT_TIME_FORMAT "%Y-%m-%d %H:%M:%S"

#define ob_bswap_16(v) \
  (uint16_t)( \
    (((uint16_t)v & 0x00ff) << 8) | \
    (((uint16_t)v & 0xff00) >> 8) \
  )

namespace oceanbase
{
namespace common
{

class ObScanner;
class ObRowkey;
class ObNewRange;
class ObSqlString;
struct ObObj;
class ObObjParam;
class ObAddr;

void hex_dump(const void *data, const int32_t size,
              const bool char_type = true, const int32_t log_level = OB_LOG_LEVEL_DEBUG);
int32_t parse_string_to_int_array(const char *line,
                                  const char del, int32_t *array, int32_t &size);
/**
 * parse string like int:32 to ObObj
 */
bool is2n(int64_t input);
constexpr int64_t next_pow2(const int64_t x)
{
  return x > 1LL ? (1ULL << (8 * sizeof(int64_t) - __builtin_clzll(x - 1))) : 1LL;
}

bool all_zero(const char *buffer, const int64_t size);
bool all_zero_small(const char *buffer, const int64_t size);
char *str_trim(char *str);
char *ltrim(char *str);
char *rtrim(char *str);
const char *inet_ntoa_s(char *buffer, size_t n, const uint64_t ipport);
const char *inet_ntoa_s(char *buffer, size_t n, const uint32_t ip);

const char *time2str(const int64_t time_s, const char *format = DEFAULT_TIME_FORMAT);
int escape_range_string(char *buffer, const int64_t length, int64_t &pos, const ObString &in);
int escape_enter_symbol(char *buffer, const int64_t length, int64_t &pos, const char *src);

int sql_append_hex_escape_str(const ObString &str, ObSqlString &sql);
inline int sql_append_hex_escape_str(const char *str, const int64_t len, ObSqlString &sql)
{
  return sql_append_hex_escape_str(ObString(0, static_cast<int32_t>(len), str), sql);
}

int convert_comment_str(char *comment_str);

int mem_chunk_serialize(char *buf, int64_t len, int64_t &pos, const char *data, int64_t data_len);
int mem_chunk_deserialize(const char *buf, int64_t len, int64_t &pos, char *data, int64_t data_len,
                          int64_t &real_len);

inline int64_t min(const int64_t x, const int64_t y)
{
  return x > y ? y : x;
}

inline int64_t min(const int32_t x, const int64_t y)
{
  return x > y ? y : x;
}

inline int64_t min(const int64_t x, const int32_t y)
{
  return x > y ? y : x;
}

inline int32_t min(const int32_t x, const int32_t y)
{
  return x > y ? y : x;
}

inline uint64_t min(const uint64_t x, const uint64_t y)
{
  return x > y ? y : x;
}

inline uint32_t min(const uint32_t x, const uint32_t y)
{
  return x > y ? y : x;
}

template <class T>
void min(T, T) = delete;

inline int64_t max(const int64_t x, const int64_t y)
{
  return x < y ? y : x;
}
inline int64_t max(const int64_t x, const int32_t y)
{
  return x < y ? y : x;
}
inline int64_t max(const int32_t x, const int64_t y)
{
  return x < y ? y : x;
}
inline uint64_t max(const uint64_t x, const uint64_t y)
{
  return x < y ? y : x;
}
inline int32_t max(const int32_t x, const int32_t y)
{
  return x < y ? y : x;
}
inline uint32_t max(const uint32_t x, const uint32_t y)
{
  return x < y ? y : x;
}
inline uint8_t max(const uint8_t x, const uint8_t y)
{
  return x < y ? y : x;
}
inline int16_t max(const int16_t x, const int16_t y)
{
  return x < y ? y : x;
}

inline double max(const double x, const double y)
{
  return x < y ? y : x;
}

template <class T>
void max(T, T) = delete;

template<oceanbase::common::ObWaitEventIds::ObWaitEventIdEnum event_id = oceanbase::common::ObWaitEventIds::DEFAULT_SLEEP>
inline void ob_usleep(const useconds_t v)
{
  oceanbase::common::ObSleepEventGuard wait_guard(event_id, 0, (int64_t)v);
  ::usleep(v);
}

int get_double_expand_size(int64_t &new_size, const int64_t limit_size);
/**
 * allocate new memory that twice larger to store %oldp
 * @param oldp: old memory content.
 * @param old_size: old memory size.
 * @param limit_size: expand memory cannot beyond this limit.
 * @param new_size: expanded memory size.
 * @param allocator: memory allocator.
 */
template <typename T, typename Allocator>
int double_expand_storage(T *&oldp, const int64_t old_size,
                          const int64_t limit_size, int64_t &new_size, Allocator &allocator)
{
  int ret = OB_SUCCESS;
  new_size = old_size;
  void *newp = NULL;
  if (OB_SUCCESS != (ret = get_double_expand_size(new_size, limit_size))) {
  } else if (NULL == (newp = allocator.alloc(new_size * sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int64_t i = 0; i < old_size; ++i) {
      reinterpret_cast<T *>(newp)[i] = oldp[i];
    }
    if (NULL != oldp) { allocator.free(reinterpret_cast<char *>(oldp)); }
    oldp = reinterpret_cast<T *>(newp);
  }
  return ret;
}

template <typename T>
int double_expand_storage(T *&oldp, const int64_t old_size,
                          const int64_t limit_size, int64_t &new_size, const lib::ObLabel &label)
{
  ObMalloc allocator;
  allocator.set_label(label);
  return double_expand_storage(oldp, old_size, limit_size, new_size, allocator);
}

extern bool str_isprint(const char *str, const int64_t length);
extern int replace_str(char *src_str, const int64_t src_str_buf_size,
                       const char *match_str, const char *replace_str);

bool ez2ob_addr(ObAddr &addr, easy_addr_t& ez);

inline const char *get_peer_ip(char *buffer, size_t n, easy_request_t *req)
{
  static char mess[8] = "unknown";
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    return easy_inet_addr_to_str(&req->ms->c->addr, buffer, (int)n);
  } else {
    return mess;
  }
}

inline const char *get_peer_ip(char *buffer, size_t n, easy_connection_t *c)
{
  static char mess[8] = "unknown";
  if (OB_LIKELY(NULL != c)) {
    return easy_inet_addr_to_str(&c->addr, buffer, (int)n);
  } else {
    return mess;
  }
}

inline int get_fd(const easy_request_t *req)
{
  int fd = -1;
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    fd = req->ms->c->fd;
  }
  return fd;
}

inline void init_easy_buf(easy_buf_t *buf, char *data, easy_request_t *req, uint64_t size)
{
  if (NULL != buf && NULL != data) {
    buf->pos = data;
    buf->last = data;
    buf->end = data + size;
    buf->cleanup = NULL;
    if (NULL != req && NULL != req->ms) {
      buf->args = req->ms->pool;
    }
    buf->flags = 0;
    easy_list_init(&buf->node);
  }
}

inline easy_addr_t get_easy_addr(easy_request_t *req)
{
  static easy_addr_t empty = {0, 0, {0}, 0};
  if (OB_LIKELY(NULL != req
                && NULL != req->ms
                && NULL != req->ms->c)) {
    return req->ms->c->addr;
  } else {
    return empty;
  }
}

inline int extract_int(const ObString &str, int n, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;

  if (!str.ptr() || str.length() <= 0 || pos < 0 || pos >= str.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *cur_ptr = str.ptr() + pos;
    const char *end_ptr = str.ptr() + str.length();
    int scanned = 0;
    int64_t result = 0;
    int64_t cur_value = 0;

    //skip non-numeric character
    while (cur_ptr < end_ptr && (*cur_ptr > '9' || *cur_ptr < '0')) {
      cur_ptr++;
    }
    if (cur_ptr >= end_ptr) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      n = n > 0 ? n : str.length();
      while (cur_ptr < end_ptr && scanned < n && *cur_ptr <= '9' && *cur_ptr >= '0') {
        cur_value = *cur_ptr - '0';
        result = result * 10L + cur_value;
        scanned++;
        cur_ptr++;
      }
      if (scanned <= 0) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        pos = cur_ptr - str.ptr();
        value = result;
      }
    }
  }
  return ret;
}

inline int extract_int_reverse(const ObString &str, int n, int64_t &pos, int64_t &value)
{
  int ret = OB_SUCCESS;

  if (!str.ptr() || str.length() <= 0 || pos < 0 || pos >= str.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *cur_ptr = str.ptr() + pos;
    const char *end_ptr = str.ptr();
    int scanned = 0;
    int64_t result = 0;
    int64_t multi_unit = 1;
    int64_t cur_value = 0;

    //skip non-numeric character
    while (cur_ptr >= end_ptr && (*cur_ptr > '9' || *cur_ptr < '0')) {
      cur_ptr--;
    }
    if (cur_ptr < end_ptr) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      n = n > 0 ? n : str.length();
      while (cur_ptr >= end_ptr && scanned < n && *cur_ptr <= '9' && *cur_ptr >= '0') {
        cur_value = *cur_ptr - '0';
        result += cur_value * multi_unit;
        multi_unit *= 10L;
        scanned++;
        cur_ptr--;
      }
      if (scanned <= 0) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        value = result;
        pos = cur_ptr - str.ptr();
      }
    }
  }
  return ret;
}

inline int split_on(ObString &src, const char sep, ObIArray<ObString> &result)
{
  int ret = OB_SUCCESS;
  ObString str = src.split_on(sep);
  while (OB_SUCC(ret) && !str.empty()) {
    if (OB_FAIL(result.push_back(str))) {
      LIB_LOG(WARN, "push back error", K(ret));
    } else {
      str = src.split_on(sep);
    }
  }
  if (OB_SUCC(ret) && !src.empty()) {
    ret = result.push_back(src);
  }
  return ret;
}

//TODO why need this function.Remeber there's one deep copy function
template <typename Allocator>
int deep_copy_ob_string(Allocator &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  char *ptr = NULL;
  if (NULL == src.ptr() || 0 >= src.length()) {
    dst.assign_ptr(NULL, 0);
  } else if (NULL == (ptr = reinterpret_cast<char *>(allocator.alloc(src.length())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(ptr, src.ptr(), src.length());
    dst.assign(ptr, src.length());
  }
  return ret;
}

int deep_copy_obj(ObIAllocator &allocator, const ObObj &src, ObObj &dst);
int deep_copy_objparam(ObIAllocator &allocator, const ObObjParam &src, ObObjParam &dst);

struct SeqLockGuard
{
  explicit SeqLockGuard(volatile uint64_t &seq): seq_(seq)
  {
    uint64_t tmp_seq = 0;
    do {
      tmp_seq = seq_;
    } while ((tmp_seq & 1) || !__sync_bool_compare_and_swap(&seq_, tmp_seq, tmp_seq + 1));
  }
  ~SeqLockGuard()
  {
    __sync_synchronize();
    seq_++;
    __sync_synchronize();
  }
  volatile uint64_t &seq_;
};
struct OnceGuard
{
  OnceGuard(volatile uint64_t &seq): seq_(seq), locked_(false)
  {
  }
  ~OnceGuard()
  {
    if (locked_) {
      __sync_synchronize();
      seq_++;
    }
  }
  bool try_lock()
  {
    uint64_t cur_seq = 0;
    locked_ = (0 == ((cur_seq = seq_) & 1)) &&
              __sync_bool_compare_and_swap(&seq_, cur_seq, cur_seq + 1);
    return locked_;
  }
  volatile uint64_t &seq_;
  bool locked_;
};

struct CountReporter
{
  CountReporter(const char *id, int64_t report_mod)
      : id_(id), seq_lock_(0), report_mod_(report_mod),
        count_(0), start_ts_(0),
        last_report_count_(0), last_report_time_(0),
        total_cost_time_(0), last_cost_time_(0)
  {
  }
  ~CountReporter()
  {
    if (last_report_count_ > 0) {
      _OB_LOG(INFO, "%s=%ld", id_, count_);
    }
  }
  bool has_reported() { return last_report_count_ > 0; }
  void inc(const int64_t submit_time)
  {
    int64_t count = __sync_add_and_fetch(&count_, 1);
    int64_t total_cost_time = __sync_add_and_fetch(&total_cost_time_, (::oceanbase::common::ObTimeUtility::fast_current_time() - submit_time));
    if (0 == (count % report_mod_)) {
      SeqLockGuard lock_guard(seq_lock_);
      int64_t cur_ts = ::oceanbase::common::ObTimeUtility::fast_current_time();
      _OB_LOG_RET(ERROR, OB_ERROR, "%s=%ld:%ld:%ld\n", id_, count,
                1000000 * (count - last_report_count_) / (cur_ts - last_report_time_),
                (total_cost_time - last_cost_time_)/report_mod_);
      last_report_count_ = count;
      last_report_time_ = cur_ts;
      last_cost_time_ = total_cost_time;
    }
  }
  const char *id_;
  uint64_t seq_lock_ CACHE_ALIGNED;
  int64_t report_mod_;
  int64_t count_ CACHE_ALIGNED;
  int64_t start_ts_;
  int64_t last_report_count_;
  int64_t last_report_time_;
  int64_t total_cost_time_;
  int64_t last_cost_time_;
};

inline int64_t get_cpu_num()
{
  static int64_t cpu_num = sysconf(_SC_NPROCESSORS_ONLN);
  return cpu_num;
}

inline int64_t get_cpu_id()
{
  return sched_getcpu();
}

// ethernet speed: byte / second.
int get_ethernet_speed(const char *devname, int64_t &speed);
int get_ethernet_speed(const ObString &devname, int64_t &speed);

inline int64_t get_phy_mem_size()
{
  static int64_t page_size = sysconf(_SC_PAGE_SIZE);
  static int64_t phys_pages = sysconf(_SC_PHYS_PAGES);
  return page_size * phys_pages;
}

int64_t get_level1_dcache_size();

int64_t get_level1_icache_size();

int64_t get_level2_cache_size();

int64_t get_level3_cache_size();

inline bool is_cpu_support_sse42()
{
 #if defined (__x86_64__)
  uint32_t data;
  asm("cpuid"
      : "=c"(data)
      : "a"(1)
      :);
  return 0 != (data & CPUID_STD_SSE4_2);
 #elif defined(__aarch64__)
  return 0;
 #else
  #error arch unsupported
 #endif
}

///@brief Whether s1 is equal to s2, ignoring case and regarding space between words as one blank.
///If equal, return true. Otherwise, return false.
///
///For example:
///s1:"  show  collation " s2:"show collation", return true
///s1:"sh ow collation" s2:"show collation", return false
///@param [in] s1 input of string1
///@param [in] s1_len length of string1
///@param [in] s2 input of string2
///@param [in] s2_len length of string2
///@return true s1 is equal to s2,ignoring case and space
///@return false s1 is not equal to s2, or input arguments are wrong
bool is_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len);

///@brief Whether s1 is equal to s2 in no more than N characters, ignoring case and regarding space behind  a word as one blank.
///If equal, return true. Otherwise, return false.
///
///For example:
///s1:" set names hello" s2:"set names *", cmp_len:strlen(s2)-1  return true
///WARN:To deal SQL"set names *", s1:"set names" s2:"set names *", cmp_len:strlen(s2)-1  return false
///@param [in] s1 input of string1
///@param [in] s1_len length of string1
///@param [in] s2 input of string2
///@param [in] s2_len length of string2
///@param [in] cmp_len length to be compared
///@return true s1 is equal to s2 in no more than N characters,ignoring case and space
///@return false s1 is not equal to s2, or input arguments are wrong
bool is_n_case_space_equal(const char *s1, int64_t s1_len, const char *s2, int64_t s2_len,
                           int64_t cmp_len);

///@brief Whether str is equal to wild_str with wild charset.
///  wild_many '%', wild_one '_', wild_prefix '\'
///@param [in] str string to be compared
///@param [in] wild_str string to be compared with wild charset
///@param [in] str_is_pattern whether str is pattern.
//             When grant privileges to a database_name, this should be true;
int wild_compare(const char *str, const char *wild_str, const bool str_is_pattern);

///@brief Same functionality as 'wild_compare' with input of 'const char *'.
int wild_compare(const ObString &str, const ObString &wild_str, const bool str_is_pattern);

///@brief Get the sort value.
///  The string wich is more specific has larger number. Each string has 8 bits. The number 128
///represents string without wild char. Others represent the position of the fist wild char.
///wild char
///param [in] count count of arguments
///param [in] ... strings of needed to compute sort value
uint64_t get_sort(uint count, ...);

///@brief Get the sort value.
///param [in] str string needed to compute sort value
uint64_t get_sort(const ObString &str);

bool prefix_match(const char *prefix, const char *str);
int str_cmp(const void *v1, const void *v2);
////////////////////////////////////////////////////////////////////////////////////////////////////

inline void bind_self_to_core(uint64_t id)
{
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(id, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}
inline void bind_core()
{
  static uint64_t idx = 0;
  bind_self_to_core(ATOMIC_FAA(&idx, 1));
}

/*
 * Load file to string. We alloc one more char for C string terminate '\0',
 * so it is safe to use %str.ptr() as C string.
 */
template <typename Allocator>
int load_file_to_string(const char *path, Allocator &allocator, ObString &str)
{
  int ret = OB_SUCCESS;
  struct stat st;
  char *buf = NULL;
  int fd = -1;
  int64_t size = 0;

  if (NULL == path || strlen(path) == 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if ((fd = ::open(path, O_RDONLY)) < 0) {
    _OB_LOG(WARN, "open file %s failed, errno %d", path, errno);
    ret = OB_ERROR;
  } else if (0 != ::fstat(fd, &st)) {
    _OB_LOG(WARN, "fstat %s failed, errno %d", path, errno);
    ret = OB_ERROR;
  } else if (NULL == (buf = allocator.alloc(st.st_size + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if ((size = static_cast<int64_t>(::read(fd, buf, st.st_size))) < 0) {
    _OB_LOG(WARN, "read %s failed, errno %d", path, errno);
    ret = OB_ERROR;
  } else {
    buf[size] = '\0';
    str.assign(buf, static_cast<int>(size));
  }
  if (fd >= 0) {
    int tmp_ret = close(fd);
    if (tmp_ret < 0) {
      _OB_LOG(WARN, "close %s failed, errno %d", path, errno);
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

/**
 * copy C string safely
 *
 * @param dest destination buffer
 * @param dest_buflen destination buffer size
 * @param src source string
 * @param src_len source string length
 *
 * @return error code
 */
inline int ob_cstrcopy(char *dest, int64_t dest_buflen, const char* src, int64_t src_len)
{
  int ret = OB_SUCCESS;
  if (dest_buflen <= src_len) {
    COMMON_LOG(WARN, "buffer not enough", K(dest_buflen), K(src_len));
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(dest, src, src_len);
    dest[src_len] = '\0';
  }
  return ret;
}

inline int ob_cstrcopy(char *dest, int64_t dest_buflen, const ObString &src_str)
{
  return ob_cstrcopy(dest, dest_buflen, src_str.ptr(), src_str.length());
}

const char* get_default_if();

int start_daemon(const char *pidfile);

int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, va_list ap);
int ob_alloc_printf(ObString &result, ObIAllocator &alloc, const char* fmt, ...) __attribute__((format(printf, 3, 4)));

//Simple to ltoa. Need ensure dst buffer has at least 22 bytes
static const int64_t OB_LTOA10_CHAR_LEN = 22;
char *ltoa10(int64_t val,char *dst,const bool is_signed);

int long_to_str10(int64_t val,char *dst, const int64_t buf_len, const bool is_signed, int64_t &length);

template <typename T>
bool has_exist_in_array(const ObIArray<T> &array, const T &var, int64_t *idx = NULL)
{
  bool ret = false;
  int64_t num = array.count();
  for (int64_t i = 0; i < num; i++) {
    if (var == array.at(i)) {
      ret = true;
      if (idx != NULL) {
        *idx = i;
      }
      break;
    }
  }
  return ret;
}

template <typename T>
bool has_exist_in_array(const T *array, const int64_t num, const T &var)
{
  bool ret = false;
  for (int64_t i = 0; i < num; i++) {
    if (var == array[i]) {
      ret = true;
      break;
    }
  }
  return ret;
}

template <typename ContainerT, typename ElementT>
bool element_exist(const ContainerT &container, const ElementT &var)
{
  bool bret = false;
  FOREACH(var_iter, container) {
    if (*var_iter == var) {
      bret = true;
      break;
    }
  }
  return bret;
}

template <typename T>
int add_var_to_array_no_dup(ObIArray<T> &array, const T &var, int64_t *idx = NULL)
{
  int ret = OB_SUCCESS;
  if (has_exist_in_array(array, var, idx)) {
    //do nothing
  } else if (OB_FAIL(array.push_back(var))) {
    LIB_LOG(WARN, "Add var to array error", K(ret));
  } else if (idx != NULL) {
    *idx = array.count() - 1;
  }
  return ret;
}

template <typename T>
int append_array_no_dup(ObIArray<T> &dst, const ObIArray<T> &src)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < src.count(); ++idx) {
    const T &var = src.at(idx);
    if (has_exist_in_array(dst, var)) {
      //do nothing
    } else if (OB_FAIL(dst.push_back(var))) {
      LIB_LOG(WARN, "Add var to array error", K(ret));
    } else { } //do nothing
  }
  return ret;
}

//size:array capacity, num: current count
template <typename T>
int add_var_to_array_no_dup(T *array, const int64_t size, int64_t &num, const T &var)
{
  int ret = OB_SUCCESS;
  if (num > size) {
    ret = OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "Num >= size", K(ret));
  } else {
    if (has_exist_in_array(array, num, var)) {
      //do nothing
    } else if (num >= size) {
      ret = OB_SIZE_OVERFLOW;
      LIB_LOG(WARN, "Size is not enough", K(ret));
    } else {
      array[num++] = var;
    }
  }
  return ret;
}

template <typename T, int64_t N = 1>
class ObPtrGuard
{
public:
  explicit ObPtrGuard(ObIAllocator &allocator) : ptr_(NULL), allocator_(allocator) {}
  ~ObPtrGuard()
  {
    if (NULL != ptr_) {
      for (int64_t i = 0; i < N; i++) {
        ptr_[i].~T();
      }
      allocator_.free(ptr_);
      ptr_ = NULL;
    }
  }

  int init()
  {
    int ret = OB_SUCCESS;
    if (NULL != ptr_) {
      ret = OB_INIT_TWICE;
      LIB_LOG(WARN, "already inited", K(ret));
    } else {
      T *mem = static_cast<T *>(allocator_.alloc(sizeof(T) * N));
      if (NULL == mem) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "alloc memory failed", K(ret), "size", sizeof(T) * N);
      } else {
        for (int64_t i = 0; i < N; i++) {
          new (&mem[i]) T();
        }
        ptr_ = mem;
      }
    }
    return ret;
  }

  T *ptr() { return ptr_; }

private:
  T *ptr_;
  ObIAllocator &allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPtrGuard);
};

class ObTimeGuard
{
public:
  explicit ObTimeGuard(const char *owner = "unknown", const int64_t warn_threshold = INT64_MAX)
  {
    need_record_log_ = oceanbase::lib::is_trace_log_enabled();
    if (need_record_log_) {
      start_ts_ = common::ObTimeUtility::fast_current_time();
      last_ts_ = start_ts_;
      click_count_ = 0;
      warn_threshold_ = warn_threshold;
      owner_ = owner;
      memset(click_, 0, sizeof(click_));
      memset(click_str_, 0, sizeof(click_str_));
    }
  }
  void click(const char *mod = NULL)
  {
    if (need_record_log_) {
      const int64_t cur_ts = common::ObTimeUtility::fast_current_time();
      if (OB_LIKELY(click_count_ < MAX_CLICK_COUNT)) {
        click_str_[click_count_] = mod;
        click_[click_count_++] = (int32_t)(cur_ts - last_ts_);
        last_ts_ = cur_ts;
      }
    }
  }
  ~ObTimeGuard()
  {
    if (need_record_log_) {
      if (OB_UNLIKELY(get_diff() >= warn_threshold_)) {
        LIB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "destruct", K(*this));
      }
    }
  }
  int64_t get_diff() const
  {
    return need_record_log_ ? common::ObTimeUtility::fast_current_time() - start_ts_ : 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  DECLARE_TO_YSON_KV;
private:
  static const int64_t MAX_CLICK_COUNT = 16;
private:
  int64_t start_ts_;
  int64_t last_ts_;
  int64_t click_count_;
  int64_t warn_threshold_;
  const char *owner_;
  int32_t click_[MAX_CLICK_COUNT];
  bool need_record_log_;
  const char *click_str_[MAX_CLICK_COUNT];
};

class ObSimpleTimeGuard
{
public:
  explicit ObSimpleTimeGuard(const int64_t warn_threshold = INT64_MAX)
  {
    need_record_log_ = oceanbase::lib::is_trace_log_enabled();
    if (need_record_log_) {
      start_ts_ = common::ObTimeUtility::fast_current_time();
      warn_threshold_ = warn_threshold;
    }
  }
  void click()
  {
    if (need_record_log_) {
      if (OB_UNLIKELY(get_diff() >= warn_threshold_)) {
        LIB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "click", K(*this), KCSTRING(lbt()));
      }
    }
  }
  ~ObSimpleTimeGuard()
  {
    if (need_record_log_) {
      if (OB_UNLIKELY(get_diff() >= warn_threshold_)) {
        LIB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "destruct", K(*this), KCSTRING(lbt()));
      }
    }
  }
  int64_t get_diff() const
  {
    return need_record_log_ ? common::ObTimeUtility::fast_current_time() - start_ts_ : 0;
  }
  TO_STRING_KV(K_(start_ts), K_(warn_threshold));
private:
  int64_t start_ts_;
  int64_t warn_threshold_;
  bool need_record_log_;
};

class ObTimeInterval
{
public:
  explicit ObTimeInterval(const int64_t interval, const bool first_reach = true)
      : last_ts_(0), interval_(interval)
  {
    if (!first_reach) {
      last_ts_ = common::ObTimeUtility::fast_current_time();
    }
  }
  ~ObTimeInterval() {}
  void reset()
  {
    last_ts_ = 0;
  }
  bool reach() const
  {
    bool bool_ret = false;
    const int64_t now = common::ObTimeUtility::fast_current_time();
    if (now - last_ts_ > interval_) {
      bool_ret = true;
      last_ts_ = now;
    }
    return bool_ret;
  }
private:
  mutable int64_t last_ts_;
  const int64_t interval_;
};

class ObBandwidthThrottle
{
public:
  ObBandwidthThrottle();
  ~ObBandwidthThrottle();
  int init(const int64_t rate, const char *comment = "unknown");
  int set_rate(const int64_t rate);
  int get_rate(int64_t &rate);
  int limit_and_sleep(const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time, int64_t &sleep_us);
  void destroy();
private:
  int cal_limit(const int64_t bytes, int64_t &avaliable_timestamp);
  static int do_sleep(const int64_t next_avaliable_ts, const int64_t last_active_time, const int64_t max_idle_time, int64_t &sleep_us);
private:
  common::ObSpinLock lock_;
  int64_t rate_; // bandwidth limit bytes/s.
  int64_t next_avaliable_timestamp_;
  int64_t unlimit_bytes_;
  int64_t total_bytes_;
  int64_t total_sleep_ms_;
  int64_t last_printed_bytes_;
  int64_t last_printed_sleep_ms_;
  int64_t last_printed_ts_;
  char comment_[OB_MAX_TASK_COMMENT_LENGTH];
  bool inited_;
};

class ObInOutBandwidthThrottle
{
public:
  ObInOutBandwidthThrottle();
  ~ObInOutBandwidthThrottle();

  int init(const int64_t rate);
  int set_rate(const int64_t rate);
  int get_rate(int64_t &rate);
  int limit_in_and_sleep(const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time);
  int limit_out_and_sleep(const int64_t bytes, const int64_t last_active_time, const int64_t max_idle_time, int64_t *need_sleep_us = nullptr);
  void destroy();

private:
  ObBandwidthThrottle in_throttle_;
  ObBandwidthThrottle out_throttle_;
};

template<bool, typename T> struct __has_assign__;

template <typename T>
struct __has_assign__<true, T>
{
  typedef int (T::*Sign)(const T &);
  typedef char yes[1];
  typedef char no[2];
  template <typename U, U>
  struct type_check;
  template <typename _1> static yes &chk(type_check<Sign, &_1::assign> *);
  template <typename> static no &chk(...);
  static bool const value = sizeof(chk<T>(0)) == sizeof(yes);
};

template <typename T>
struct __has_assign__<false, T>
{
  static bool const value = false;
};

template <typename T>
inline int get_copy_assign_ret_wrap(T &dest, FalseType)
{
  UNUSED(dest);
  return OB_SUCCESS;
}

template <typename T>
inline int get_copy_assign_ret_wrap(T &dest, TrueType)
{
  return dest.get_copy_assign_ret();
}

template <typename T>
inline int copy_assign_wrap(T &dest, const T &src, FalseType)
{
  dest = src;
  return get_copy_assign_ret_wrap(dest, BoolType<HAS_MEMBER(T, get_copy_assign_ret)>());
}

template <typename T>
inline int copy_assign_wrap(T &dest, const T &src, TrueType)
{
  return dest.assign(src);
}

template <typename T>
inline void set_member_allocator_wrap(T &dest, common::ObIAllocator *alloc, FalseType)
{
  UNUSED(dest);
  UNUSED(alloc);
}

template <typename T>
inline void set_member_allocator_wrap(T &dest, common::ObIAllocator *alloc, TrueType)
{
  (void)dest.set_allocator(alloc);
}

template <typename T>
inline void set_member_allocator(T &dest, common::ObIAllocator *alloc)
{
  set_member_allocator_wrap(dest, alloc, BoolType<HAS_MEMBER(T, set_allocator)>());
}

template <typename T>
inline int construct_assign_wrap(T &dest, const T &src, TrueType)
{
  new(&dest) T();
  return dest.assign(src);
}

template <typename T>
inline int construct_assign_wrap(T &dest, const T &src, FalseType)
{
  new(&dest) T(src);
  return get_copy_assign_ret_wrap(dest, BoolType<HAS_MEMBER(T, get_copy_assign_ret)>());
}

// This function is used for copy assignment
// -If T has a member function int assign(const T &), call dest.assign(src),
//   And take the return value of the assign function as the return value;
// -If T is class && without member function int assign(const T &), then:
//    -If T has a member function get_copy_assign_ret(), call dest=src,
//      And get the return value through dest.get_copy_assign_ret() function;
//    -If T has no member function get_copy_assign_ret(), call dest=src,
//      And return OB_SUCCESS.
template <typename T>
inline int copy_assign(T &dest, const T &src)
{
  return copy_assign_wrap(dest, src, BoolType<__has_assign__<__is_class(T), T>::value>());
}

// This function is used for copy construction
// -If T has a member function int assign(const T &) and no get_copy_assign_ret(),
//   Call new(&dest) T() and dest.assign(src), and use the return value of the assign function as the return value;
// -Otherwise, call new(&dest) T(src), and at the same time:
//    -If T has a member function get_copy_assign_ret(), then
//      Obtain the return value through the dest.get_copy_assign_ret() function;
//    -If T has no member function get_copy_assign_ret(), it returns OB_SUCCESS.
template <typename T>
inline int construct_assign(T &dest, const T &src)
{
  return construct_assign_wrap(dest, src,
      BoolType<__has_assign__<__is_class(T), T>::value && !HAS_MEMBER(T, get_copy_assign_ret)>());
}

template <typename T>
inline int objects_copy_wrap(T *dest, const T *src, int64_t count, TrueType)
{
  int ret = OB_SUCCESS;
  if (NULL == dest || NULL == src || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(dest), K(src), K(count), K(ret));
  } else {
    MEMCPY(dest, src, sizeof(T) * count);
  }
  return ret;
}

template <typename T>
inline int objects_copy_wrap(T *dest, const T *src, int64_t count, FalseType)
{
  int ret = OB_SUCCESS;
  if (NULL == dest || NULL == src || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(dest), K(src), K(count), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = copy_assign(dest[i], src[i]);
    }
  }
  return ret;
}

template <typename T>
inline int objects_copy(T *dest, const T *src, int64_t count)
{
  return objects_copy_wrap(dest, src, count, BoolType<std::is_trivially_copyable<T>::value>());
}

class ObMiniStat
{
public:
  class ObStatItem
  {
  public:
    ObStatItem(const char *item, const int64_t stat_interval)
      : item_(item), stat_interval_(stat_interval), last_ts_(0), stat_count_(0), accum_count_(0), lock_tag_(false) {
        MEMSET(extra_info_, '\0', MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH);
      }
    ~ObStatItem() {}
  public:
    void set_extra_info(const char *extra_info)
    {
      MEMCPY(extra_info_, extra_info, MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH);
    }
    void stat(const int64_t count = 0)
    {
      const int64_t cur_ts = ::oceanbase::common::ObTimeUtility::fast_current_time();
      const int64_t cur_stat_count = ATOMIC_AAF(&stat_count_, 1);
      const int64_t cur_accum_count = ATOMIC_AAF(&accum_count_, count);
      if (ATOMIC_LOAD(&last_ts_) + stat_interval_ < cur_ts) {
        if (ATOMIC_BCAS(&lock_tag_, false, true)) {
          LIB_LOG(INFO, NULL == item_ ? "" : item_, K(cur_stat_count), K_(stat_interval), "avg (count/cost)",
              cur_accum_count / cur_stat_count, K(this), K_(extra_info));
          (void)ATOMIC_SET(&last_ts_, cur_ts);
          (void)ATOMIC_SET(&stat_count_, 0);
          (void)ATOMIC_SET(&accum_count_, 0);
          ATOMIC_BCAS(&lock_tag_, true, false);
        }
      }
    }

    void stat(const int64_t count, const int64_t total_time_cost)
    {
      const int64_t cur_ts = ::oceanbase::common::ObTimeUtility::fast_current_time();
      const int64_t cur_stat_count = ATOMIC_AAF(&stat_count_, count);
      const int64_t cur_accum_time = ATOMIC_AAF(&accum_count_, total_time_cost);
      if (ATOMIC_LOAD(&last_ts_) + stat_interval_ < cur_ts) {
        if (ATOMIC_BCAS(&lock_tag_, false, true)) {
          LIB_LOG(INFO, NULL == item_ ? "" : item_, K(cur_stat_count), K_(stat_interval), "avg (count/cost)",
              cur_accum_time / cur_stat_count, K(this), K_(extra_info));
          (void)ATOMIC_SET(&last_ts_, cur_ts);
          (void)ATOMIC_SET(&stat_count_, 0);
          (void)ATOMIC_SET(&accum_count_, 0);
          ATOMIC_BCAS(&lock_tag_, true, false);
        }
      }
    }
  private:
    const char *const item_;
    const int64_t stat_interval_;
    char extra_info_[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH];
    int64_t last_ts_;
    int64_t stat_count_;
    int64_t accum_count_;
    bool lock_tag_;
  };
public:
  static void stat(ObStatItem &item)
  {
    item.stat();
  }
};

class ObIntWarp
{
public:
  ObIntWarp() : v_(0) {}
  explicit ObIntWarp(const uint64_t v) : v_(v) {}
  ~ObIntWarp() { reset(); }
  void reset() { v_ = 0; }
  uint64_t hash() const { return v_; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  uint64_t get_value() const { return v_; }
  int compare(const ObIntWarp &other) const
  {
    int ret = 0;
    if (v_ == other.v_) {
      ret = 0;
    } else if (v_ > other.v_) {
      ret = 1;
    } else {
      ret = -1;
    }
    return ret;
  }
  bool operator==(const ObIntWarp &other) const
  {
    return 0 == compare(other);
  }
  bool operator!=(const ObIntWarp &other) const
  {
    return !operator==(other);
  }
  TO_STRING_KV(K_(v));
private:
  uint64_t v_;
};

class ObTsWindows
{
public:
  ObTsWindows() : start_(0), base_(0), end_(0) {}
  ~ObTsWindows() {}
public:
  void set(const int64_t start, const int64_t base, const int64_t end)
  {
    start_ = start;
    base_ = base;
    end_ = end;
  }
  void reset()
  {
    start_ = 0;
    base_ = 0;
    end_ = 0;
  }
  bool contain(const int64_t ts) const
  {
    return ts >= start_ && ts <= end_;
  }
  int64_t get_start() const { return start_; }
  int64_t get_left_size() const { return base_ - start_; }
  int64_t get_end() const { return end_; }
public:
  TO_STRING_KV(K_(start), K_(base), K_(end));
private:
  int64_t start_;
  int64_t base_;
  int64_t end_;
};

void get_addr_by_proxy_sessid(const uint64_t session_id, ObAddr &addr);

const char *replica_type_to_str(const ObReplicaType &type);

int ob_atoll(const char *str, int64_t &res);
int ob_strtoll(const char *str, char *&endptr, int64_t &res);
int ob_strtoull(const char *str, char *&endptr, uint64_t &res);

/* 功能：根据localtime计算公式实现快速计算的方法, 替代系统函数localtime_r.
   参数：
     in: const time_t *unix_sec, 当前的时间戳(单位秒), 输入值
     out: struct tm *result, 当前时间戳对应的可读时间localtime, 输出值
   返回值：
     无
*/
struct tm *ob_localtime(const time_t *unix_sec, struct tm *result);

/* 功能：根据日志时间戳特点实现最快速计算localtime. 实现逻辑: 首先检查这次输入时间戳是否和上一次相同(秒单位相同, 绝大部分连续日志秒单位都是相同的), 如果相同则直接使用上一次计算的localtime即可. 如果不相同, 就使用快速时间戳计算方法ob_localtime计算localtime.
   参数：
     in/out: time_t &cached_unix_sec, 缓存的上一次时间戳(秒单位)
     in/out: struct tm &cached_localtime, 缓存的上一次时间戳对应的可读时间localtime
     in: const time_t &input_unix_sec, 当前的时间戳(单位秒), 输入值
     out: struct tm *output_localtime, 当前时间戳对应的可读时间localtime, 输出值
   返回值：
     无
*/
void ob_fast_localtime(time_t &cached_unix_sec, struct tm &cached_localtime,
                       const time_t &input_unix_sec, struct tm *output_localtime);

template <typename T>
void call_dtor(T *&ptr)
{
  if (NULL != ptr) {
    ptr->~T();
    ptr = NULL;
  }
}

// Function: Check whether the directory is empty, in particular, if the directory does not exist, is_empty returns true
// parameter:
//      in: dir_name, directory name
//      out: is_empty, whether the directory is empty
// return value:
//      OB_INVALID_ARGUMENT dir_name is NULL
//      OB_IO_ERROR Error executing system call
//      OB_SUCCESS successfully executed
int is_dir_empty(const char *dirname, bool &is_empty);
} // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_UTILITY_H_
