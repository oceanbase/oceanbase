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

#ifndef SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_
#define SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_

#include "sql/engine/ob_operator.h"
#include "lib/file/ob_file.h"
#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSelectIntoOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObSelectIntoOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObOpInput(ctx, spec),
    task_id_(common::OB_INVALID_ID),
    sqc_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObSelectIntoOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override
  {
    UNUSED(task_info);
    return common::OB_SUCCESS;
  }
  virtual void reset() override {}
  virtual void set_task_id(int64_t task_id) { task_id_ = task_id; }
  virtual void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  int64_t get_task_id() const { return task_id_; }
  int64_t get_sqc_id() const { return sqc_id_; }

  int64_t task_id_;
  int64_t sqc_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSelectIntoOpInput);
};

class ObSelectIntoSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSelectIntoSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      into_type_(T_INTO_OUTFILE),
      user_vars_(alloc),
      outfile_name_(),
      field_str_(),
      line_str_(),
      closed_cht_(),
      is_optional_(false),
      select_exprs_(alloc),
      is_single_(true),
      max_file_size_(DEFAULT_MAX_FILE_SIZE),
      escaped_cht_(),
      parallel_(1),
      file_partition_expr_(NULL),
      buffer_size_(DEFAULT_BUFFER_SIZE)
  {
    cs_type_ = ObCharset::get_system_collation();
  }

  ObItemType into_type_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> user_vars_;
  common::ObObj outfile_name_;
  common::ObObj field_str_; // FARM COMPAT WHITELIST FOR filed_str_: renamed
  common::ObObj line_str_;
  // 431以下版本select into无法并行执行, 不会序列化算子, 修改closed_cht_类型不会导致升级兼容性问题
  common::ObObj closed_cht_; // FARM COMPAT WHITELIST FOR closed_cht_: change type
  bool is_optional_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> select_exprs_;
  bool is_single_;
  int64_t max_file_size_;
  common::ObObj escaped_cht_;
  common::ObCollationType cs_type_;
  int64_t parallel_;
  sql::ObExpr* file_partition_expr_;
  int64_t buffer_size_;
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256LL * 1024 * 1024;
  static const int64_t DEFAULT_BUFFER_SIZE = 1LL * 1024 * 1024;
};

class ObSelectIntoOp : public ObOperator
{
public:
  enum class IntoFileLocation {
    SERVER_DISK,
    REMOTE_OSS,
  };
  ObSelectIntoOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      top_limit_cnt_(INT64_MAX),
      file_appender_(),
      is_first_(true),
      device_handle_(NULL),
      file_location_(IntoFileLocation::SERVER_DISK),
      write_offset_(0),
      write_bytes_(0),
      split_file_id_(0),
      char_enclose_(0),
      char_escape_(0),
      has_enclose_(false),
      has_escape_(false),
      has_lob_(false),
      has_json_(false),
      is_file_opened_(false),
      print_params_(),
      escape_printer_()
  {
  }

  // cs_type of ObString in ObEscapeInfo should be dst_cs_type
  struct ObEscapePrinter
  {
    ObEscapePrinter():
      need_enclose_(false), do_encode_(false), do_escape_(false), print_hex_(false),
      ignore_convert_failed_(false) {}
    int operator() (const ObString &src_str, const ob_wc_t &unicode_value) {
      int ret = OB_SUCCESS;
      ObString dst_str = src_str;
      int result_len = 0;
      char tmp_buf[ObCharset::MAX_MB_LEN];
      if (do_encode_) {
        ret = ObCharset::wc_mb(coll_type_, unicode_value, tmp_buf, ObCharset::MAX_MB_LEN, result_len);
        if (OB_SUCC(ret)) {
          dst_str = ObString(result_len, tmp_buf);
        } else if (ret == OB_ERR_INCORRECT_STRING_VALUE && ignore_convert_failed_) {
          dst_str = convert_replacer_;
          ret = OB_SUCCESS;
        }
      }
      if (OB_FAIL(ret) || !do_escape_ || print_hex_) {
      } else if (dst_str.compare(zero_) == 0
                 || dst_str.compare(enclose_) == 0
                 || dst_str.compare(escape_) == 0
                 || (!need_enclose_ && (dst_str.compare(field_terminator_) == 0
                                        || dst_str.compare(line_terminator_) == 0))) {
        ret = databuff_memcpy(buf_, buf_len_, pos_, escape_.length(), escape_.ptr());
      }
      if (OB_FAIL(ret)) {
      } else if (print_hex_) {
        ret = hex_print(dst_str.ptr(), dst_str.length(), buf_, buf_len_, pos_);
      } else if (do_escape_ && dst_str.compare(zero_) == 0) {
        char zero = '0';
        ret = databuff_memcpy(buf_, buf_len_, pos_, 1, &zero);
      } else {
        ret = databuff_memcpy(buf_, buf_len_, pos_, dst_str.length(), dst_str.ptr());
      }
      return ret;
    }
    ObString enclose_;
    ObString escape_;
    ObString zero_;
    ObString field_terminator_;
    ObString line_terminator_;
    ObString convert_replacer_;
    ObCollationType coll_type_;
    bool need_enclose_;
    bool do_encode_;
    bool do_escape_;
    bool print_hex_;
    bool ignore_convert_failed_;
    char *buf_;
    int64_t buf_len_;
    int64_t pos_;
  };
  class ObIOBufferWriter
  {
  public:
    ObIOBufferWriter():
      buf_(NULL), curr_pos_(0), last_line_pos_(0), buf_len_(0), curr_line_len_(0) {}
    void init(char *buf, int64_t buf_len) {
      buf_ = buf;
      buf_len_ = buf_len;
    }
    void init_json_buf(char *buf, int64_t buf_len) {
      json_buf_ = buf;
      json_buf_len_ = buf_len;
    }
    template<typename flush_func>
    int flush(flush_func flush_data) {
      int ret = common::OB_SUCCESS;
      if (last_line_pos_ > 0) {
        if (OB_FAIL(flush_data(buf_, last_line_pos_))) {
        } else {
          MEMCPY(buf_, buf_ + last_line_pos_, curr_pos_ - last_line_pos_);
          curr_pos_ = curr_pos_ - last_line_pos_;
          last_line_pos_ = 0;
        }
      }
      return ret;
    }
    template<typename flush_func>
    int flush_all_for_lob(flush_func flush_data) {
      int ret = common::OB_SUCCESS;
      if (curr_pos_ > 0) {
        if (OB_FAIL(flush_data(buf_, curr_pos_))) {
        } else {
          curr_line_len_ += (curr_pos_ - last_line_pos_);
          curr_pos_ = 0;
          last_line_pos_ = 0;
        }
      }
      return ret;
    }
    char *get_buf() { return buf_; }
    int64_t get_buf_len() { return buf_len_; }
    char *get_json_buf() { return json_buf_; }
    int64_t get_json_buf_len() { return json_buf_len_; }
    int64_t get_curr_pos() { return curr_pos_; }
    int64_t get_last_line_pos() { return last_line_pos_; }
    int64_t get_curr_line_len() {return curr_line_len_; }
    void set_curr_pos(int64_t curr_pos) { curr_pos_ = curr_pos; }
    void update_last_line_pos() { last_line_pos_ = curr_pos_; }
    void reset_curr_line_len() {curr_line_len_ = 0; }
  private:
    char *buf_;
    int64_t curr_pos_;
    int64_t last_line_pos_;
    int64_t buf_len_;
    int64_t curr_line_len_;
    char *json_buf_;  //json需要多一个buffer用来放转义前的string
    int64_t json_buf_len_;
  };

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  void reset()
  {
    is_first_ = true;
    file_appender_.close();
    device_handle_ = NULL;
    file_location_ = IntoFileLocation::SERVER_DISK;
    write_offset_ = 0;
    write_bytes_ = 0;
    split_file_id_ = 0;
    data_writer_.init(NULL, 0);
    is_file_opened_ = false;
  }

private:
  int get_row_str(const int64_t buf_len, bool is_first_row, char *buf, int64_t &pos);
  int into_dumpfile();
  int into_outfile();
  int into_outfile_batch(const ObBatchRows &brs);
  int extract_fisrt_wchar_from_varhcar(const ObObj &obj, int32_t &wchar);
  int print_wchar_to_buf(char *buf,
                         const int64_t buf_len,
                         int64_t &pos,
                         int32_t wchar,
                         ObString &str,
                         ObCollationType coll_type);
  int print_field(const ObObj &obj);
  int print_lob_field(const ObObj &obj, const ObExpr &expr, const ObDatum &datum);
  void get_buf(char* &buf, int64_t &buf_len, int64_t &pos, bool is_json = false);
  int flush_buf(int64_t &pos);
  int resize_buf(char* &buf, int64_t &buf_len, int64_t &pos, bool is_json = false);
  int write_obj_to_file(const ObObj &obj, bool need_escape = false);
  int print_normal_obj_without_escape(const ObObj &obj,
                                      char* &buf,
                                      int64_t &buf_len,
                                      int64_t &pos,
                                      bool is_json = false);
  int write_single_char_to_file(const char *wchar);
  int write_lob_to_file(const ObObj &obj, const ObExpr &expr, const ObDatum &datum);
  int try_split_file();
  int into_varlist();
  int open_file();
  int calc_next_file_path();
  int calc_first_file_path(ObString &path);
  int split_file();
  void close_file();
  std::function<int(const char *, int64_t)> get_flush_function();
  int prepare_escape_printer();
  int check_has_lob_or_json();

private:
  int64_t top_limit_cnt_;
  ObFileAppender file_appender_;
  bool is_first_;
  ObObj field_str_;
  ObObj line_str_;
  ObObj file_name_;
  ObString url_;
  share::ObBackupStorageInfo access_info_;
  ObIODevice* device_handle_;
  ObIOFd fd_;
  IntoFileLocation file_location_;
  int64_t write_offset_;
  int64_t write_bytes_;
  int64_t split_file_id_;
  ObIOBufferWriter data_writer_;
  char char_enclose_;
  char char_escape_;
  bool has_enclose_;
  bool has_escape_;
  bool has_lob_;
  bool has_json_;
  bool is_file_opened_;
  common::ObObjPrintParams print_params_;
  ObEscapePrinter escape_printer_;
};



}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_ */
