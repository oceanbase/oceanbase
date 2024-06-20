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

#ifdef NDEBUG
#define __ENABLE_TRACEPOINT__ 0
#else
#define __ENABLE_TRACEPOINT__ 1
#endif

#ifndef OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#define OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/list/ob_dlist.h"
#include "lib/coro/co_var.h"
#include "lib/time/ob_tsc_timestamp.h"
#include "common/ob_clock_generator.h"
#include "lib/utility/ob_macro_utils.h"

#define TP_COMMA(x) ,
#define TP_EMPTY(x)
#define TP_PAIR_ARGS() 0, 0
#define TP_THIRD_rescan(x1, x2, x3, ...) x3
#define TP_THIRD(x1, x2, x3) TP_THIRD_rescan(x1, x2, x3, 0)
#define TP_COND(test, true_expr, false_expr) TP_THIRD(TP_PAIR_ARGS test, false_expr, true_expr)

#define TP_MAPCALL(f, cur) TP_COND(cur, TP_COMMA, TP_EMPTY)(cur) TP_COND(cur, f, TP_EMPTY)(cur)
#define TP_MAP8(f, a1, a2, a3, a4, a5, a6, a7, a8, ...)                              \
  TP_COND(a1, f, TP_EMPTY)(a1) TP_MAPCALL(f, a2) TP_MAPCALL(f, a3) TP_MAPCALL(f, a4) \
  TP_MAPCALL(f, a5) TP_MAPCALL(f, a6) TP_MAPCALL(f, a7) TP_MAPCALL(f, a8)
#define TP_MAP(f, ...) TP_MAP8(f, ##__VA_ARGS__, (), (), (), (), (), (), (), ())

#define TP_COMPILER_BARRIER() asm volatile("" ::: "memory")
#define TP_AL(ptr) ({ TP_COMPILER_BARRIER(); (OB_ISNULL(ptr)) ? 0 : *ptr; })
#define TP_AS(x, v) (      \
  {                        \
    TP_COMPILER_BARRIER(); \
    if (OB_ISNULL(x)) {    \
    } else {               \
      *(x) = v;            \
    }                      \
    __sync_synchronize();  \
  })

#define TP_BCAS(x, ov, nv) __sync_bool_compare_and_swap((x), (ov), (nv))
#define TP_RELAX() PAUSE()
#define TP_CALL_FUNC(ptr, ...)({                                             \
      int (*func)(TP_MAP(typeof, ##__VA_ARGS__)) = (typeof(func))TP_AL(ptr); \
      (NULL != func) ? func(__VA_ARGS__): 0; })

#define TRACEPOINT_CALL(name, ...) ({                                      \
      static void** func_ptr = ::oceanbase::common::tracepoint_get(name);  \
      (NULL != func_ptr) ? TP_CALL_FUNC(func_ptr, ##__VA_ARGS__): 0; })

#if __ENABLE_TRACEPOINT__
#define OB_I(key, ...)  \
  TRACEPOINT_CALL(::oceanbase::common::refine_tp_key(__FILE__, __FUNCTION__, #key), ##__VA_ARGS__)?:
#else
#define OB_I(...)
#endif


bool &get_tp_switch();

#define TP_SWITCH_GUARD(v) ::oceanbase::lib::ObSwitchGuard<get_tp_switch> osg_##__COUNTER__##_(v)


namespace oceanbase {
namespace lib {
using GetSwitchFunc = bool& ();

template<GetSwitchFunc fn>
class ObSwitchGuard
{
public:
  ObSwitchGuard(bool newval)
  {
    oldval_ = fn();
    fn() = newval;
  }
  ~ObSwitchGuard()
  {
    fn() = oldval_;
  }
private:
  bool oldval_;
};
}
}


#define EVENT_CALL(name_event, ...) ({ name_event.item_.call(SELECT(1, ##__VA_ARGS__)); })

#define EVENT_CODE(name_event, ...) ({ name_event.item_.get_event_code(); })

#define ERRSIM_POINT_DEF(name, ...)                       \
  static oceanbase::common::NamedEventItem name(          \
  #name, SELECT(1, ##__VA_ARGS__, ""),                    \
  oceanbase::common::EventTable::global_item_list());

// latest doc:

// doc:

// to check if a certain tracepoint is set
// example: if (E(50) OB_SUCCESS) {...}
// you can also specify condition:
// if (E(50, session_id) OB_SUCCESS) { ... }
// which means:
//   check whether event 50 of session_id was raised
#define OB_E(name_event, ...)  \
  EVENT_CALL(name_event, ##__VA_ARGS__)?:

// to set a particular tracepoint
// example: TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, 4016, 1, 1)
// specify condition: TP_SET_EVENT(EventTable::EN_IO_GETEVENTS, 4016, 1, 1, 3302201)
// which means:
//   when session id is 3302201, trigger event 50 with error -4016
#define TP_SET_EVENT(name, error_in, occur, trigger_freq, ...)                 \
  {                                                                            \
    EventItem item;                                                            \
    item.error_code_ = error_in;                                               \
    item.occur_ = occur;                                                       \
    item.trigger_freq_ = trigger_freq;                                         \
    item.cond_ = SELECT(1, ##__VA_ARGS__, 0);                                  \
    name.item_.set_event(item);                                                \
  }

#define TP_SET(file_name, func_name, key, trace_func)                           \
  *::oceanbase::common::tracepoint_get(refine_tp_key(file_name, func_name, key)) = (void*)(trace_func)
#define TP_SET_ERROR(file_name, func_name, key, err)                            \
  TP_SET(file_name, func_name, key, (int (*)())&tp_const_error<(err)>)

namespace oceanbase
{
namespace common
{
inline const char* tp_basename(const char* path)
{
  const char* ret = OB_ISNULL(path) ? NULL : strrchr(path, '/');
  return (NULL == ret) ? path: ++ret;
}

inline const char* refine_tp_key(const char* s1, const char* s2, const char* s3)
{
  constexpr int64_t BUFFER_SIZE = 256;
  typedef struct {
    char buffer_[BUFFER_SIZE];
  } BUFFER;
  RLOCAL(BUFFER, co_buffer);
  char *buffer = (&co_buffer)->buffer_;
  const char *cret = nullptr;
  if (OB_ISNULL(s1) || OB_ISNULL(s2) || OB_ISNULL(s3)) {
  } else {
    s1 = tp_basename(s1);
    snprintf(buffer, BUFFER_SIZE, "%s:%s:%s", s1, s2, s3);
    cret = buffer;
  }
  return cret;
}

template<const int err>
int tp_const_error()
{
  return err;
}

class TPSymbolTable
{
public:
  TPSymbolTable() {}
  ~TPSymbolTable() {}
  void** get(const char* name)
  {
    return (NULL != name) ? do_get(name): NULL;
  }

private:
  static uint64_t BKDRHash(const char *str);
  enum { SYMBOL_SIZE_LIMIT = 128, SYMBOL_COUNT_LIMIT = 64 * 1024 };

  struct SymbolEntry
  {
    enum { FREE = 0, SETTING = 1, OK = 2 };
    SymbolEntry(): lock_(FREE), value_(NULL)
    { name_[0] = '\0'; }
    ~SymbolEntry() {}

    bool find(const char* name);

    int lock_;
    void* value_;
    char name_[SYMBOL_SIZE_LIMIT];
  };

  void** do_get(const char* name);

  SymbolEntry symbol_table_[SYMBOL_COUNT_LIMIT];
};

inline void** tracepoint_get(const char* name)
{
  static TPSymbolTable symbol_table;
  return symbol_table.get(name);
}

struct EventItem
{
  int64_t no_;
  const char *name_;
  const char *describe_;
  int64_t occur_;            // number of occurrences
  int64_t trigger_freq_;         // trigger frequency
  int64_t error_code_;        // error code to return
  int64_t cond_;

  EventItem()
    : no_(-1),
      name_(nullptr),
      describe_(nullptr),
      occur_(0),
      trigger_freq_(0),
      error_code_(0),
      cond_(0) {}

  EventItem(int64_t no, const char *name, const char *describe)
    : no_(no),
      name_(name),
      describe_(describe),
      occur_(0),
      trigger_freq_(0),
      error_code_(0),
      cond_(0) {}

  void set_event(const EventItem& other)
  {
    occur_ = other.occur_;
    trigger_freq_ = other.trigger_freq_;
    error_code_ = other.error_code_;
    cond_ = other.cond_;
  }

  int call(const int64_t v) { return cond_ == v ? call() : 0; }
  int call() { return static_cast<int>(get_event_code()); }
  int64_t get_event_code()
  {
    int64_t event_code = 0;
    int64_t trigger_freq = trigger_freq_;
    if (occur_ > 0) {
      do {
        int64_t occur = occur_;
        if (occur > 0) {
          if (ATOMIC_VCAS(&occur_, occur, occur - 1)) {
            event_code = error_code_;
            break;
          }
        } else {
          event_code = 0;
          break;
        }
      } while (true);
    } else if (OB_LIKELY(trigger_freq == 0)) {
      event_code = 0;
    } else if (get_tp_switch()) { // true means skip errsim
      event_code = 0;
    } else if (trigger_freq == 1) {
      event_code = error_code_;
#ifdef NDEBUG
      if (REACH_TIME_INTERVAL(1 * 1000 * 1000))
#endif
      {
        int ret = static_cast<int>(event_code);
        COMMON_LOG(WARN, "[ERRSIM] sim error", K(event_code));
      }
    } else {
      if (rand() % trigger_freq == 0) {
        event_code = error_code_;
        int ret = static_cast<int>(event_code);
        COMMON_LOG(WARN, "[ERRSIM] sim error", K(ret), K_(error_code), K(trigger_freq), KCSTRING(lbt()));
      } else {
        event_code = 0;
      }
    }
    return event_code;
  }

};

struct NamedEventItem : public ObDLinkBase<NamedEventItem>
{
  template<std::size_t TP_NAME_LEN, std::size_t TP_DESCRIBE_LEN>
  NamedEventItem(const char (&tp_name)[TP_NAME_LEN],
                 const char (&tp_describe)[TP_DESCRIBE_LEN],
                 ObDList<NamedEventItem> &l): item_(-1, tp_name, tp_describe)
  {
    STATIC_ASSERT(TP_NAME_LEN - 1 <= OB_MAX_TRACEPOINT_NAME_LEN,
        "tp_name length longer than OB_MAX_TRACEPOINT_NAME_LEN(128) is not allowed!");
    STATIC_ASSERT(TP_DESCRIBE_LEN - 1 <= OB_MAX_TRACEPOINT_DESCRIBE_LEN,
        "tp_describe length longer than OB_MAX_TRACEPOINT_DESCRIBE_LEN(4096) is not allowed!");
    l.add_last(this);
  }

  template<std::size_t TP_NAME_LEN, std::size_t TP_DESCRIBE_LEN>
  NamedEventItem(const int64_t tp_no,
                 const char (&tp_name)[TP_NAME_LEN],
                 const char (&tp_describe)[TP_DESCRIBE_LEN],
                 ObDList<NamedEventItem> &l) : item_(tp_no, tp_name, tp_describe)
  {
    STATIC_ASSERT(TP_NAME_LEN - 1 <= OB_MAX_TRACEPOINT_NAME_LEN,
        "tp_name length longer than OB_MAX_TRACEPOINT_NAME_LEN(128) is not allowed!");
    STATIC_ASSERT(TP_DESCRIBE_LEN - 1 <= OB_MAX_TRACEPOINT_DESCRIBE_LEN,
        "tp_describe length longer than OB_MAX_TRACEPOINT_DESCRIBE_LEN(4096) is not allowed!");
    l.add_last(this);
  }
  operator int(void) { return item_.call(); }
  EventItem item_;
};

class EventTable
{
  public:
    EventTable() {}
    virtual ~EventTable() {}

    /* set an event value */
    static inline int set_event(int64_t no, const EventItem &item)
    {
      int ret = OB_SUCCESS;
      if (no < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        bool is_find = false;
        DLIST_FOREACH_NORET(i, global_item_list()) {
          if (i->item_.no_ == no) {
            i->item_.set_event(item);
            is_find = true;
            break;
          }
        }
        if (!is_find) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      return ret;
    }

    static inline int set_event(const char *name, const EventItem &item)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(name)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        bool is_find = false;
        DLIST_FOREACH_NORET(i, global_item_list()) {
          if (OB_NOT_NULL(i->item_.name_) && strcasecmp(i->item_.name_, name) == 0) {
            i->item_.set_event(item);
            is_find = true;
            break;
          }
        }
        if (!is_find) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      return ret;
    }

    static ObDList<NamedEventItem> &global_item_list()
    {
      static ObDList<NamedEventItem> g_list;
      return g_list;
    }

    static EventTable &instance()
    {
      static EventTable et;
      return et;
    }

    #define GLOBAL_ERRSIM_POINT_DEF(no, name, describe)       \
    static oceanbase::common::NamedEventItem name
    #include "lib/utility/ob_tracepoint_def.h"
    #undef GLOBAL_ERRSIM_POINT_DEF
};

}
}

#endif //OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_

#if __TEST_TRACEPOINT__
#include <stdio.h>

int fake_syscall()
{
  return 0;
}

int test_tracepoint(int x)
{
  int err = 0;
  printf("=== start call: %d ===\n", x);
  if (0 != (err = OB_I(a, x) fake_syscall())) {
    printf("fail at step 1: err=%d\n", err);
  } else if (0 != (err = OB_I(b) fake_syscall())) {
    printf("fail at step 2: err=%d\n", err);
  } else {
    printf("succ\n");
  }
  return err;
}

int tp_handler(int x)
{
  printf("tp_handler: x=%d\n", x);
  return 0;
}

int run_tests()
{
  test_tracepoint(0);
  TP_SET("tracepoint.h", "test_tracepoint", "a",  &tp_handler);
  TP_SET_ERROR("tracepoint.h", "test_tracepoint", "b",  -1);
  test_tracepoint(1);
  TP_SET_ERROR("tracepoint.h", "test_tracepoint", "b",  -2);
  test_tracepoint(2);
  TP_SET("tracepoint.h", "test_tracepoint", "b",  NULL);
  test_tracepoint(3);
}
#endif
