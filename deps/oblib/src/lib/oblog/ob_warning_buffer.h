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

#ifndef OCEANBASE_LIB_OBLOG_OB_WARNING_BUFFER_
#define OCEANBASE_LIB_OBLOG_OB_WARNING_BUFFER_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <new>

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace common
{
// one global error message buffer and multi global warning buffers
class ObWarningBuffer
{
public:
  struct WarningItem final
  {
    OB_UNIS_VERSION(1);
  public:
    WarningItem() : timestamp_(0),
                    log_level_(ObLogger::USER_WARN),
                    line_no_(0),
                    code_(OB_MAX_ERROR_CODE),
                    column_no_(0)
    { msg_[0] = '\0'; sql_state_[0] = '\0'; }

    static const uint32_t STR_LEN = 512;
    char msg_[STR_LEN];
    int64_t timestamp_;
    int log_level_;
    int line_no_;
    int code_;
    int column_no_;
    char sql_state_[6];

    inline void reset()
    { 
      msg_[0] = '\0'; 
      sql_state_[0] = '\0';
      code_ = OB_MAX_ERROR_CODE; 
      timestamp_ = 0;
      line_no_ = 0; 
      column_no_ = 0; 
      log_level_ = ObLogger::USER_WARN;
    }
    inline void set(const char *str) {snprintf(msg_, STR_LEN, "%s", str);}
    inline void set_code(int code) { code_ = code; }
    inline void set_log_level(ObLogger::UserMsgLevel level) { log_level_ = level; }
    inline const char *get() const {return static_cast<const char *>(msg_);}
    inline int get_code() const { return code_; }
    inline void set_line_no(int line_no) { line_no_ = line_no; }
    inline int get_line_no() const { return line_no_; }
    inline void set_column_no(int col_no) { column_no_ = col_no; }
    inline int get_column_no() const { return column_no_; }
    inline void set_sql_state(const char *str) {snprintf(sql_state_, 6, "%s", str);}
    inline const char *get_sql_state() const {return static_cast<const char *>(sql_state_);}
    WarningItem &operator= (const WarningItem &other);

    TO_STRING_KV(KCSTRING_(msg), K_(code));
  };
private:
  static constexpr int64_t ARRAY_BLOCK_SIZE = sizeof(WarningItem) * 1;
public:
  ObWarningBuffer() : item_(ARRAY_BLOCK_SIZE),
                      append_idx_(0),
                      total_warning_count_(0),
                      error_ret_(OB_SUCCESS)
  {
    item_.set_label(ObModIds::OB_SQL_SESSION_WARNING_BUFFER);
  }
  ~ObWarningBuffer() {reset();}

  inline void reset(void);
  inline void reset_warning(void);

  static inline void set_warn_log_on(const bool is_log_on) {is_log_on_ = is_log_on;}
  static inline bool is_warn_log_on() {return is_log_on_;}

  inline uint32_t get_total_warning_count(void) const {return total_warning_count_;}
  inline uint32_t get_buffer_size(void) const {return MAX_BUFFER_SIZE;}
  inline uint32_t get_readable_warning_count(void) const;
  inline uint32_t get_max_warn_len(void) const {return WarningItem::STR_LEN;}

  /*
   * write WARNING into BUFFER
   * if buffer is full, cover the oldest warning
   * if len(str) > STR_LEN, cut it.
   */
  inline void append_warning(const char *str, int errcode, const char *sql_state = nullptr);
  inline void append_note(const char *str, int errcode);

  inline void reset_err() {err_.reset();}
  inline void set_error(const char *str, int error_code = OB_MAX_ERROR_CODE) {err_.set(str); err_.set_code(error_code);}
  inline void set_error_code(int error_code) {err_.set_code(error_code);}
  inline const char *get_err_msg() const {return err_.get();}
  inline int get_err_code() const {return err_.get_code();}
  inline void set_error_line_column(const int line, const int column) 
  { err_.set_line_no(line); err_.set_column_no(column); }
  inline int get_error_line() const { return err_.get_line_no(); }
  inline int get_error_column() const { return err_.get_column_no(); }
  inline void set_sql_state(const char *str) { err_.set_sql_state(str);}
  inline const char *get_sql_state() const {return err_.get_sql_state();}

  ObWarningBuffer &operator= (const ObWarningBuffer &other);

  /*
   * get WarningItem
   * idx range [0, get_readable_warning_count)
   * return NULL if idx is out of range
   */
  inline const ObWarningBuffer::WarningItem *get_warning_item(const uint32_t idx) const;
  TO_STRING_KV(K_(err), K_(item), K_(total_warning_count), K_(is_log_on));
private:
  inline void append_msg(ObLogger::UserMsgLevel msg_level, const char *str, int code, const char *sql_state = nullptr);
private:
  // const define
  static const uint32_t MAX_BUFFER_SIZE = 64;
  common::ObArray<WarningItem, common::ModulePageAllocator, false> item_;
  WarningItem err_;
  uint32_t append_idx_;
  uint32_t total_warning_count_;
  // it willl alloc memory when appending, so use error_ret_ to keep status
  int error_ret_;
  static bool is_log_on_;
};

inline void ObWarningBuffer::reset_warning()
{
  append_idx_ = 0;
  total_warning_count_ = 0;
}

inline void ObWarningBuffer::reset()
{
  append_idx_ = 0;
  total_warning_count_ = 0;
  err_.reset();
  error_ret_ = OB_SUCCESS;
}

inline uint32_t ObWarningBuffer::get_readable_warning_count() const
{
  return (total_warning_count_ < get_buffer_size()) ? total_warning_count_ : get_buffer_size();
}

inline const ObWarningBuffer::WarningItem *ObWarningBuffer::get_warning_item(const uint32_t idx) const
{
  const ObWarningBuffer::WarningItem *item = NULL;
  if (idx < get_readable_warning_count()) {
    uint32_t loc = idx;
    if (total_warning_count_ > MAX_BUFFER_SIZE) {
      loc = (append_idx_ + idx) % MAX_BUFFER_SIZE;
    }
    item = &item_[loc];
  }
  return item;
}

inline void ObWarningBuffer::append_msg(ObLogger::UserMsgLevel msg_level, const char *str,
                                        int code, const char *sql_state)
{
  // warning_buffer will be reset before requests handled
  if (OB_SUCCESS == error_ret_) {
    std::string eg(str);
    int ret = OB_SUCCESS;
    ObWarningBuffer::WarningItem *item = nullptr;
    if (append_idx_ >= item_.count()) {
      item = item_.alloc_place_holder();
      if (OB_ISNULL(item)) {
        ret = OB_INIT_FAIL;
      }
    } else {
      item = &item_[append_idx_];
    }
    if (OB_FAIL(ret)) {
      error_ret_ = ret;
    } else {
      item->set(str);
      item->set_code(code);
      item->set_log_level(msg_level);
      if (sql_state != nullptr) {
        item->set_sql_state(sql_state);
      }
      append_idx_ = (append_idx_ + 1) % MAX_BUFFER_SIZE;
      total_warning_count_++;
    }
  }
}

inline void ObWarningBuffer::append_warning(const char *str, int code, const char *sql_state)
{
  append_msg(ObLogger::USER_WARN, str, code, sql_state);
}

inline void ObWarningBuffer::append_note(const char *str, int code)
{
  append_msg(ObLogger::USER_NOTE, str, code);
}

////////////////////////////////////////////////////////////////
/// global functions below return thread_local warning buffer
////////////////////////////////////////////////////////////////
ObWarningBuffer *&ob_get_tsi_warning_buffer();
const ObString ob_get_tsi_err_msg(int code);
/*
 * processing function of rpc set thread_local warning buffer of session through
 * set_tsi_warning_buffer function generally, but warning buffer in session could not be used in
 * remote task handler, because result_code was used after process function, so use the default
 * in this situation.
 */ 
void ob_setup_tsi_warning_buffer(ObWarningBuffer *buffer);
void ob_setup_default_tsi_warning_buffer();
// clear warnging buffer of current thread
void ob_reset_tsi_warning_buffer();

////////////////////////////////////////////////////////////////
// private function
RLOCAL_EXTERN(ObWarningBuffer *, g_warning_buffer);
inline ObWarningBuffer *&ob_get_tsi_warning_buffer()
{
  return g_warning_buffer;
}
inline const ObString ob_get_tsi_err_msg(int code)
{
  ObString ret;
  ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
  if (OB_LIKELY(NULL != wb) && code == wb->get_err_code()) {
    ret = ObString::make_string(wb->get_err_msg());
  }
  return ret;
}

inline void ob_setup_tsi_warning_buffer(ObWarningBuffer *buffer)
{
  ob_get_tsi_warning_buffer() = buffer;
}

inline void ob_setup_default_tsi_warning_buffer()
{
  auto *default_wb = GET_TSI(ObWarningBuffer);
  ob_setup_tsi_warning_buffer(default_wb);
}

inline void ob_reset_tsi_warning_buffer()
{
  ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
  if (OB_LIKELY(NULL != wb)) {
    wb->reset();
  }
}

}  // end of namespace common
}
#endif //OB_WARNING_BUFFER_H_
