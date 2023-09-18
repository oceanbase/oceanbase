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
 * Binlog Record Printer
 */

#ifndef OCEANBASE_LIBOBCDC_TESTS_BINLOG_RECORD_PRINTER_H__
#define OCEANBASE_LIBOBCDC_TESTS_BINLOG_RECORD_PRINTER_H__

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/BR.h>               // IBinlogRecord
#include <drcmsg/MD.h>               // ITableMeta
#include <drcmsg/MsgWrapper.h>       // IStrArray
#include <drcmsg/binlogBuf.h>        // binlogBuf
#endif

#include "share/ob_define.h"  // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_config.h"    // TCONF
#include "stdlib.h"           //atio

namespace oceanbase
{
namespace libobcdc
{
class ObLogBR;

class IObBinlogRecordPrinter
{
public:
  virtual ~IObBinlogRecordPrinter() {}

public:
  virtual bool need_print_binlog_record() = 0;
  virtual int print_binlog_record(IBinlogRecord *br) = 0;
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
  virtual bool need_print_binlog_record()
  { return enable_print_console_ || heartbeat_file_fd_ > 0 || data_file_fd_ > 0; };
  virtual int print_binlog_record(IBinlogRecord *br);

public:
  int init(const char *data_file,
      const char *heartbeat_file,
      const bool enable_print_console,
      const bool only_print_hex,
      const bool only_print_dml_tx_checksum,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_verify_mode,
      const bool enable_print_detail,
      const bool enable_print_special_detail);
  void destroy();
  static int64_t get_precise_timestamp(IBinlogRecord &br);

private:
  int open_file_(const char *data_file, int &fd);
  int rotate_data_file_();
  void do_br_statistic_(IBinlogRecord &br);
  void output_statistics_();

private:
  static void console_print_statements(IBinlogRecord *br, ObLogBR *oblog_br);
  static void console_print_heartbeat(IBinlogRecord *br, ObLogBR *oblog_br);
  static void console_print_commit(IBinlogRecord *br, ObLogBR *oblog_br);
  static void console_print_begin(IBinlogRecord *br, ObLogBR *oblog_br);
  static void console_print(IBinlogRecord *br, ObLogBR *oblog_br);
  static int output_data_file(IBinlogRecord *br,
      const int record_type,
      ObLogBR *oblog_br,
      const int fd,
      const bool only_print_hex,
      const bool only_print_dml_tx_checksum,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_verify_mode,
      const bool enable_print_detail,
      const bool enable_print_special_detail,
      int64_t &tx_br_count,
      uint64_t &dml_data_crc,
      bool &need_rotate_file);
  static int output_data_file_column_data(IBinlogRecord *br,
      ITableMeta *table_meta,
      const int64_t index,
      char *ptr,
      const int64_t size,
      const uint64_t ri,
      const bool only_print_hex,
      const bool enable_print_hex,
      const bool enable_print_lob_md5,
      const bool enable_print_detail,
      const bool enable_print_special_detail,
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
  static int parse_major_version_(const binlogBuf *filter_rv, int32_t &major_version);

private:
  bool        inited_;
  const char  *data_file_;
  int         data_file_fd_;
  int         heartbeat_file_fd_;
  bool        only_print_hex_;
  bool        only_print_dml_tx_checksum_; // only print column checksum for dml trans.
  bool        enable_print_hex_;
  bool        enable_print_console_;
  bool        enable_print_lob_md5_;
  bool        enable_verify_mode_;
  bool        enable_print_detail_;
  bool        enable_print_special_detail_;
  int64_t     dml_tx_br_count_; // br_count of dml tx.
  int64_t     total_tx_count_;  // tx count, statistics by commit br.
  int64_t     total_br_count_;  // DDL/DML br count, ignore heartbeat and begin/commit.
  uint64_t    dml_data_crc_;    // DML column data crc.

private:

private:
  DISALLOW_COPY_AND_ASSIGN(ObBinlogRecordPrinter);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_TESTS_BINLOG_RECORD_PRINTER_H__ */
