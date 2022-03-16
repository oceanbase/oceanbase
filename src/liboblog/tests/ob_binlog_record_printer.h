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

#ifndef OCEANBASE_LIBOBLOG_TESTS_BINLOG_RECORD_PRINTER_H__
#define OCEANBASE_LIBOBLOG_TESTS_BINLOG_RECORD_PRINTER_H__

#include <LogRecord.h>        // ILogRecord
#include <MetaInfo.h>         // ITableMeta
#include <StrArray.h>         // StrArray

#include "share/ob_define.h"  // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_config.h"    // TCONF
#include "stdlib.h"           //atio

using namespace oceanbase::logmessage;

namespace oceanbase
{
namespace liboblog
{
class ObLogBR;

class IObBinlogRecordPrinter
{
public:
  virtual ~IObBinlogRecordPrinter() {}

public:
  virtual int print_binlog_record(ILogRecord *br) = 0;
};

class ObBinlogRecordPrinter : public IObBinlogRecordPrinter
{
  static const int64_t MAX_DATA_FILE_SIZE = 256 * 1024 * 1024;
  // The length of the major version of the freeze version, used to output the major version to the binlog record,
  // major version is int32_t(2147483647), so max_length is configed to MAJOR_VERSION_LENGTH to 10;
  static const int64_t MAJOR_VERSION_LENGTH = 10;

public:
  ObBinlogRecordPrinter();
  virtual ~ObBinlogRecordPrinter();

public:
  virtual int print_binlog_record(ILogRecord *br);

public:
  int init(const char *data_file,
      const char *heartbeat_file,
      const bool enable_print_console,
      const bool only_print_hex,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_verify_mode,
      const bool enable_print_detail);
  void destroy();

private:
  int open_file_(const char *data_file, int &fd);
  int rotate_data_file();

private:
  static int64_t get_precise_timestamp_(ILogRecord &br);
  static void console_print_statements(ILogRecord *br, ObLogBR *oblog_br);
  static void console_print_heartbeat(ILogRecord *br, ObLogBR *oblog_br);
  static void console_print_commit(ILogRecord *br, ObLogBR *oblog_br);
  static void console_print_begin(ILogRecord *br, ObLogBR *oblog_br);
  static void console_print(ILogRecord *br, ObLogBR *oblog_br);
  static int output_data_file(ILogRecord *br,
      const int record_type,
      ObLogBR *oblog_br,
      const int fd,
      const bool only_print_hex,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_verify_mode,
      const bool enable_print_detail,
      bool &need_rotate_file);
  static int output_data_file_column_data(const bool is_serilized,
      ILogRecord *br,
      ITableMeta *table_meta,
      const int64_t index,
      char *ptr,
      const int64_t size,
      const uint64_t ri,
      const bool only_print_hex,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_print_detail,
      int64_t &pos);
  static int print_hex(const char *str, int64_t len, char *buf, int64_t size, int64_t &pos);
  static int write_data_file(const int fd,
      char *buf,
      const int64_t size,
      const int64_t pos,
      const bool is_line_end,
      bool &need_rotate_file);
  static bool need_print_hex(int ctype);
  static int output_heartbeat_file(const int fd, const int64_t heartbeat_timestamp);
  static int verify_begin_trans_id_(ObLogBR &oblog_br,
      const char *begin_trans_id);
  static int parse_major_version_(const BinLogBuf *filter_rv, int32_t &major_version);

private:
  bool        inited_;
  const char  *data_file_;
  int         data_file_fd_;
  int         heartbeat_file_fd_;
  bool        only_print_hex_;
  bool        enable_print_hex_;
  bool        enable_print_console_;
  bool        enable_print_lob_md5_;
  bool        enable_verify_mode_;
  bool        enable_print_detail_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBinlogRecordPrinter);
};

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBLOG_TESTS_BINLOG_RECORD_PRINTER_H__ */
