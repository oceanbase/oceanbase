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

class ObSelectIntoSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObSelectIntoSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      into_type_(T_INTO_OUTFILE),
      user_vars_(alloc),
      outfile_name_(),
      filed_str_(),
      line_str_(),
      closed_cht_(0),
      is_optional_(false),
      select_exprs_(alloc)
  {
  }

  ObItemType into_type_;
  common::ObFixedArray<common::ObString, common::ObIAllocator> user_vars_;
  common::ObObj outfile_name_;
  common::ObObj filed_str_;
  common::ObObj line_str_;
  char closed_cht_;
  bool is_optional_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> select_exprs_;
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
      split_file_id_(0)
  {
  }

  class ObIOBufferWriter
  {
  public:
    ObIOBufferWriter():
      buf_(NULL), pos_(NULL), buf_end_(NULL) {}
    void init(char *buf, int64_t buf_len) {
      buf_ = buf;
      pos_ = buf;
      buf_end_ = buf + buf_len;
    }
    template<typename flush_func>
    int flush(flush_func flush_data) {
      int ret = common::OB_SUCCESS;
      if (pos_ != buf_) {
        if (OB_FAIL(flush_data(buf_, pos_ - buf_))) {
        } else {
          pos_ = buf_;
        }
      }
      return ret;
    }
    template<typename flush_func>
    inline int write(const char *data, int64_t data_len, flush_func flush_data) {
      int ret = common::OB_SUCCESS;
      int64_t remain = buf_end_ - pos_;
      if (data_len > remain) {
        if (OB_FAIL(flush(flush_data))) {
        } else {
          remain = buf_end_ - pos_;
        }
      }
      if (OB_SUCC(ret)) {
        if (data_len > remain) {
          if (OB_FAIL(flush_data(data, data_len))) {
          }
        } else {
          MEMCPY(pos_, data, data_len);
          pos_ += data_len;
        }
      }
      return ret;
    }
  private:
    char *buf_;
    char *pos_;
    char *buf_end_;
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
  }

private:
  int get_row_str(const int64_t buf_len, bool is_first_row, char *buf, int64_t &pos);
  int into_dumpfile();
  int into_outfile();
  int into_outfile_batch(const ObBatchRows &brs);
  int into_varlist();
  int open_file();
  int write_file(const char *data, int64_t data_len);
  int split_file();
  void close_file();
  std::function<int(const char *, int64_t)> get_flush_function();

private:
  int64_t top_limit_cnt_;
  ObFileAppender file_appender_;
  bool is_first_;
  ObObj filed_str_;
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
};

}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_OP_H_ */
