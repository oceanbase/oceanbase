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
#include "share/backup/ob_backup_io_adapter.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#ifdef OB_BUILD_CPP_ODPS
#include <odps/odps_tunnel.h>
#include <odps/odps_api.h>
#endif

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
      buffer_size_(DEFAULT_BUFFER_SIZE),
      is_overwrite_(false),
      external_properties_(alloc),
      external_partition_(alloc)
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
  bool is_overwrite_;
  ObExternalFileFormat::StringData external_properties_;
  ObExternalFileFormat::StringData external_partition_;
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256LL * 1024 * 1024;
  static const int64_t DEFAULT_BUFFER_SIZE = 1LL * 1024 * 1024;
};

struct ObStorageAppender
{
	ObStorageAppender();
  virtual ~ObStorageAppender();
  void reset();

	int open(const share::ObBackupStorageInfo *storage_info,
      const common::ObString &uri, const common::ObStorageAccessType &access_type);
	int append(const char *buf, const int64_t size, int64_t &write_size);
	int close();

	bool is_opened_;
	int64_t offset_;
	ObIOFd fd_;
	ObIODevice *device_handle_;
  ObStorageAccessType access_type_;
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
      is_first_(true),
      basic_url_(),
      file_location_(IntoFileLocation::SERVER_DISK),
      write_offset_(0),
      data_writer_(NULL),
      char_enclose_(0),
      char_escape_(0),
      has_enclose_(false),
      has_escape_(false),
      has_lob_(false),
      has_json_(false),
      print_params_(),
      escape_printer_(),
      do_partition_(false),
      json_buf_(NULL),
      json_buf_len_(0),
      shared_buf_(NULL),
      shared_buf_len_(0),
      use_shared_buf_(false),
      partition_map_(),
      curr_partition_num_(0),
      external_properties_(),
      format_type_(ObExternalFileFormat::FormatType::CSV_FORMAT),
#ifdef OB_BUILD_CPP_ODPS
      upload_(NULL),
      record_writer_(NULL),
#endif
      block_id_(0),
      need_commit_(true)
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
      buf_(NULL),
      buf_len_(0),
      curr_pos_(0),
      last_line_pos_(0),
      curr_line_len_(0),
      write_bytes_(0),
      is_file_opened_(false),
      file_appender_(),
      storage_appender_(),
      fd_(),
      split_file_id_(0),
      url_()
    {}
    ~ObIOBufferWriter() {
      file_appender_.~ObFileAppender();
      storage_appender_.reset();
    }
    void init(char *buf, int64_t buf_len) {
      buf_ = buf;
      buf_len_ = buf_len;
    }
    template<typename flush_func>
    int flush(flush_func flush_data) {
      int ret = common::OB_SUCCESS;
      if (last_line_pos_ > 0 && OB_NOT_NULL(buf_)) {
        if (OB_FAIL(flush_data(buf_, last_line_pos_, this))) {
        } else {
          MEMCPY(buf_, buf_ + last_line_pos_, curr_pos_ - last_line_pos_);
          curr_pos_ = curr_pos_ - last_line_pos_;
          last_line_pos_ = 0;
        }
      }
      return ret;
    }
    char *get_buf() { return buf_; }
    int64_t get_buf_len() { return buf_len_; }
    int64_t get_curr_pos() { return curr_pos_; }
    int64_t get_last_line_pos() { return last_line_pos_; }
    int64_t get_curr_line_len() {return curr_line_len_; }
    int64_t get_write_bytes() { return write_bytes_; }
    void set_curr_pos(int64_t curr_pos) { curr_pos_ = curr_pos; }
    void update_last_line_pos() { last_line_pos_ = curr_pos_; }
    void reset_curr_line_len() { curr_line_len_ = 0; }
    void increase_curr_line_len() { curr_line_len_ += (curr_pos_ - last_line_pos_); }
    void set_write_bytes(int64_t write_bytes) { write_bytes_ = write_bytes; }
  private:
    char *buf_;
    int64_t buf_len_;
    int64_t curr_pos_;
    int64_t last_line_pos_;
    int64_t curr_line_len_;
    int64_t write_bytes_;
  public:
    bool is_file_opened_;
    ObFileAppender file_appender_;
    ObStorageAppender storage_appender_;
    ObIOFd fd_;
    int64_t split_file_id_;
    ObString url_;
  };

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  int init_csv_env();
#ifdef OB_BUILD_CPP_ODPS
  int init_odps_tunnel();
  int into_odps();
  int into_odps_batch(const ObBatchRows &brs);
  int odps_commit_upload();
  int set_odps_column_value_mysql(apsara::odps::sdk::ODPSTableRecord &table_record,
                                  const ObDatum &datum,
                                  const ObDatumMeta &datum_meta,
                                  const ObObjMeta &obj_meta,
                                  uint32_t col_idx);
  int set_odps_column_value_oracle(apsara::odps::sdk::ODPSTableRecord &table_record,
                                   const ObDatum &datum,
                                   const ObDatumMeta &datum_meta,
                                   const ObObjMeta &obj_meta,
                                   uint32_t col_idx);
#endif
  int decimal_or_number_to_int64(const ObDatum &datum, const ObDatumMeta &datum_meta, int64_t &res);
  int decimal_to_string(const ObDatum &datum,
                        const ObDatumMeta &datum_meta,
                        std::string &res,
                        ObIAllocator &allocator);
  int get_row_str(const int64_t buf_len, bool is_first_row, char *buf, int64_t &pos);
  int into_dumpfile(ObIOBufferWriter *data_writer);
  int into_outfile(ObIOBufferWriter *data_writer);
  int into_outfile_batch(const ObBatchRows &brs, ObIOBufferWriter *data_writer);
  int extract_fisrt_wchar_from_varhcar(const ObObj &obj, int32_t &wchar);
  int print_wchar_to_buf(char *buf,
                         const int64_t buf_len,
                         int64_t &pos,
                         int32_t wchar,
                         ObString &str,
                         ObCollationType coll_type);
  int print_field(const ObObj &obj, ObIOBufferWriter &data_writer);
  int print_lob_field(const ObObj &obj,
                      const ObExpr &expr,
                      const ObDatum &datum,
                      ObIOBufferWriter &data_writer);
  int get_buf(char* &buf, int64_t &buf_len, int64_t &pos, ObIOBufferWriter &data_writer);
  int flush_buf(ObIOBufferWriter &data_writer);
  int use_shared_buf(ObIOBufferWriter &data_writer, char* &buf, int64_t &buf_len, int64_t &pos);
  template<typename flush_func>
  int flush_shared_buf(ObIOBufferWriter &data_writer, flush_func flush_data, bool continue_use_shared_buf = false) {
    int ret = common::OB_SUCCESS;
    if (data_writer.get_curr_pos() > 0 && use_shared_buf_) {
      if (OB_FAIL(flush_data(shared_buf_, data_writer.get_curr_pos(), &data_writer))) {
      } else {
        if (has_lob_) {
          data_writer.increase_curr_line_len();
        }
        data_writer.set_curr_pos(0);
        data_writer.update_last_line_pos();
        use_shared_buf_ = continue_use_shared_buf;
      }
    }
    return ret;
  }
  int resize_buf(char* &buf,
                 int64_t &buf_len,
                 int64_t &pos,
                 int64_t curr_pos,
                 bool is_json = false);
  int resize_or_flush_shared_buf(ObIOBufferWriter &data_writer,
                                 char* &buf,
                                 int64_t &buf_len,
                                 int64_t &pos);
  int check_buf_sufficient(ObIOBufferWriter &data_writer,
                           char* &buf,
                           int64_t &buf_len,
                           int64_t &pos,
                           int64_t str_len);
  int write_obj_to_file(const ObObj &obj, ObIOBufferWriter &data_writer, bool need_escape = false);
  int print_str_or_json_with_escape(const ObObj &obj, ObIOBufferWriter &data_writer);
  int print_normal_obj_without_escape(const ObObj &obj, ObIOBufferWriter &data_writer);
  int print_json_to_json_buf(const ObObj &obj,
                             char* &buf,
                             int64_t &buf_len,
                             int64_t &pos,
                             ObIOBufferWriter &data_writer);
  int write_single_char_to_file(const char *wchar, ObIOBufferWriter &data_writer);
  int write_lob_to_file(const ObObj &obj,
                        const ObExpr &expr,
                        const ObDatum &datum,
                        ObIOBufferWriter &data_writer);
  int into_varlist();
  int open_file(ObIOBufferWriter &data_writer);
  int calc_next_file_path(ObIOBufferWriter &data_writer);
  int calc_first_file_path(ObString &path);
  int calc_file_path_with_partition(ObString partition, ObIOBufferWriter &data_writer);
  int try_split_file(ObIOBufferWriter &data_writer);
  int split_file(ObIOBufferWriter &data_writer);
  void close_file(ObIOBufferWriter &data_writer);
  std::function<int(const char *, int64_t, ObIOBufferWriter *)> get_flush_function();
  int prepare_escape_printer();
  int check_has_lob_or_json();
  int create_shared_buffer_for_data_writer();
  int create_the_only_data_writer(ObIOBufferWriter *&data_writer);
  int check_secure_file_path(ObString file_name);
  int get_data_writer_for_partition(ObDatum *partition_datum, ObIOBufferWriter *&data_writer);
  char *get_json_buf() { return json_buf_; }
  int64_t get_json_buf_len() { return json_buf_len_; }
  char *get_shared_buf() { return shared_buf_; }
  int64_t get_shared_buf_len() { return shared_buf_len_; }

private:
  int64_t top_limit_cnt_;
  bool is_first_;
  ObObj field_str_;
  ObObj line_str_;
  ObObj file_name_;
  ObString basic_url_; // url without partition expr
  share::ObBackupStorageInfo access_info_;
  IntoFileLocation file_location_;
  int64_t write_offset_;
  ObIOBufferWriter* data_writer_;
  char char_enclose_;
  char char_escape_;
  bool has_enclose_;
  bool has_escape_;
  bool has_lob_;
  bool has_json_;
  common::ObObjPrintParams print_params_;
  ObEscapePrinter escape_printer_;
  bool do_partition_;
  char *json_buf_;  //json需要多一个buffer用来放转义前的string
  int64_t json_buf_len_;
  char *shared_buf_;
  int64_t shared_buf_len_;
  bool use_shared_buf_;
  typedef common::hash::ObHashMap<common::ObString, ObIOBufferWriter*, hash::NoPthreadDefendMode> ObPartitionWriterMap;
  ObPartitionWriterMap partition_map_;
  int curr_partition_num_;
  ObExternalFileFormat external_properties_;
  ObExternalFileFormat::FormatType format_type_;
#ifdef OB_BUILD_CPP_ODPS
  apsara::odps::sdk::IUploadPtr upload_;
  apsara::odps::sdk::IRecordWriterPtr record_writer_;
#endif
  uint32_t block_id_;
  bool need_commit_;
  static const int64_t SHARED_BUFFER_SIZE = 2LL * 1024 * 1024;
  static const int64_t MAX_OSS_FILE_SIZE = 5LL * 1024 * 1024 * 1024;
  static const int32_t ODPS_DATE_MIN_VAL = -719162; // '0001-1-1'
};

}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_ */
