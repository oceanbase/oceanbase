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

#define USING_LOG_PREFIX OBLOG_TAILF

#define DATA_DELEMITER  ";"
#define STMT_DELEMITER  "$"

#define LOG_STD(str, ...) \
  do { \
    fprintf(stderr, str, ##__VA_ARGS__); \
  } while (0)

#define ROW_PRINTF(ptr, size, pos, ri, fmt, ...) \
  do {\
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(databuff_printf((ptr), (size), (pos), "[R%lu] " fmt "%s", ri, ##__VA_ARGS__, DATA_DELEMITER))) {\
        LOG_ERROR("databuff_printf fail", KP(ptr), K(size), K(pos), K(ri), K(ret)); \
      } \
    } \
  } while (0)

#define DATABUFF_PRINTF(ptr, size, pos, fmt, ...) \
  do {\
    if (OB_SUCC(ret)) { \
      if (OB_FAIL(databuff_printf((ptr), (size), (pos), fmt, ##__VA_ARGS__))) {\
        LOG_ERROR("databuff_printf fail", KP(ptr), K(size), K(pos), K(ret)); \
      } \
    } \
  } while (0)

#define COL_PRINT_VALUE(val, len) (int)(NULL == val ? sizeof("NULL") : len), (NULL == val ? "NULL" : val), len

#include "ob_binlog_record_printer.h"

#include "rpc/obmysql/ob_mysql_global.h"  // MYSQL_TYPE_*
#include "lib/file/file_directory_utils.h"
#include "lib/time/ob_time_utility.h"     // ObTimeUtility

#include "ob_log_utils.h"                 // calc_md5_cstr
#include "ob_log_binlog_record.h"         // ObLogBR
#include "ob_log_part_trans_task.h"       // PartTransTask

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace libobcdc
{
ObBinlogRecordPrinter::ObBinlogRecordPrinter() : inited_(false),
                                                 data_file_(NULL),
                                                 data_file_fd_(-1),
                                                 heartbeat_file_fd_(-1),
                                                 only_print_hex_(false),
                                                 only_print_dml_tx_checksum_(false),
                                                 enable_print_hex_(false),
                                                 enable_print_console_(false),
                                                 enable_print_lob_md5_(false),
                                                 enable_verify_mode_(false),
                                                 enable_print_detail_(false),
                                                 enable_print_special_detail_(false),
                                                 dml_tx_br_count_(0),
                                                 total_tx_count_(0),
                                                 total_br_count_(0),
                                                 dml_data_crc_(0)
{
}

ObBinlogRecordPrinter::~ObBinlogRecordPrinter()
{
  destroy();
}

int ObBinlogRecordPrinter::init(const char *data_file,
    const char *heartbeat_file,
    const bool enable_print_console,
    const bool only_print_hex,
    const bool only_print_dml_tx_checksum,
    const bool enable_print_hex,
    const bool enable_print_lob_md5,
    const bool enable_verify_mode,
    const bool enable_print_detail,
    const bool enable_print_special_detail)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (NULL != data_file && OB_FAIL(open_file_(data_file, data_file_fd_))) {
    LOG_ERROR("open data file fail", K(ret), K(data_file));
  } else if (NULL != heartbeat_file && OB_FAIL(open_file_(heartbeat_file, heartbeat_file_fd_))) {
    LOG_ERROR("open heartbeat file fail", K(ret), K(heartbeat_file));
  } else {
    data_file_ = data_file;
    only_print_hex_ = only_print_hex;
    only_print_dml_tx_checksum_ = only_print_dml_tx_checksum;
    enable_print_hex_ = enable_print_hex;
    enable_print_console_ = enable_print_console;
    enable_print_lob_md5_ = enable_print_lob_md5;
    enable_verify_mode_ = enable_verify_mode;
    enable_print_detail_ = enable_print_detail;
    enable_print_special_detail_ = enable_print_special_detail;
    inited_ = true;
  }

  return ret;
}

void ObBinlogRecordPrinter::destroy()
{
  if (data_file_fd_ >= 0) {
    close(data_file_fd_);
    data_file_fd_ = -1;
  }

  if (heartbeat_file_fd_ >= 0) {
    close(heartbeat_file_fd_);
    heartbeat_file_fd_ = -1;
  }

  only_print_hex_ = false;
  only_print_dml_tx_checksum_ = false;
  enable_print_hex_ = false;
  enable_print_console_ = false;
  enable_print_lob_md5_ = false;
  enable_verify_mode_ = false;
  enable_print_detail_ = false;
  enable_print_special_detail_ = false;
  dml_tx_br_count_ = 0;
  total_tx_count_ = 0;
  total_br_count_ = 0;
  dml_data_crc_ = 0;
  data_file_ = NULL;
  inited_ = false;
}

int ObBinlogRecordPrinter::open_file_(const char *file_name, int &fd)
{
  OB_ASSERT(NULL != file_name);

  int ret = OB_SUCCESS;
  char *p = strrchr(const_cast<char*>(file_name), '/');
  if (NULL != p) {
    char dir_buffer[OB_MAX_FILE_NAME_LENGTH];
    snprintf(dir_buffer, OB_MAX_FILE_NAME_LENGTH, "%.*s", (int)(p - file_name), file_name);
    common::FileDirectoryUtils::create_full_path(dir_buffer);
  }

  if (OB_SUCC(ret)) {
    fd = open(file_name, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    if (0 > fd) {
      LOG_ERROR("open data file fail", K(file_name), K(errno), KERRMSG);
      ret = OB_IO_ERROR;
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::print_binlog_record(IBinlogRecord *br)
{
  int ret = OB_SUCCESS;
  ObLogBR *oblog_br = NULL;

  if (! inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(br)) {
    LOG_ERROR("invalid arguments", K(br));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(oblog_br = reinterpret_cast<ObLogBR *>(br->getUserData()))) {
    LOG_ERROR("get user data fail", K(br), K(oblog_br));
    ret = OB_INVALID_ARGUMENT;
  } else {
    do_br_statistic_(*br);

    if (enable_print_console_) {
      console_print(br, oblog_br);
    }
    int record_type = br->recordType();

    // Heartbeat timestamp taken directly from the br
    if (HEARTBEAT == br->recordType() && heartbeat_file_fd_ >= 0) {
      if (OB_FAIL(output_heartbeat_file(heartbeat_file_fd_, get_precise_timestamp(*br)))) {
        LOG_ERROR("output_heartbeat_file fail", K(ret), K(heartbeat_file_fd_), K(oblog_br));
      }
    } else if (data_file_fd_ >= 0) {
      bool need_rotate_file = false;

      if (OB_FAIL(output_data_file(br, record_type, oblog_br, data_file_fd_, only_print_hex_,
          only_print_dml_tx_checksum_, enable_print_hex_,
          enable_print_lob_md5_, enable_verify_mode_, enable_print_detail_, enable_print_special_detail_,
          dml_tx_br_count_, dml_data_crc_, need_rotate_file))) {
        LOG_ERROR("output_data_file fail", K_(data_file_fd), K_(data_file), K(ret));
      } else if (need_rotate_file && OB_FAIL(rotate_data_file_())) {
        LOG_ERROR("rotate_data_file fail", K(ret));
      }
    }
  }

  return ret;
}

int64_t ObBinlogRecordPrinter::get_precise_timestamp(IBinlogRecord &br)
{
  int64_t timestamp_sec = br.getTimestamp();
  uint32_t timestamp_usec = br.getRecordUsec();
  int64_t precise_timestamp = timestamp_sec * 1000000 + timestamp_usec;

  return precise_timestamp;
}

void ObBinlogRecordPrinter::console_print(IBinlogRecord *br, ObLogBR *oblog_br)
{
  if (NULL != br && NULL != oblog_br) {
    if (EBEGIN == br->recordType()) {
      console_print_begin(br, oblog_br);
    } else if (ECOMMIT == br->recordType()) {
      console_print_commit(br, oblog_br);
    } else if (HEARTBEAT == br->recordType()) {
      console_print_heartbeat(br, oblog_br);
    } else {
      console_print_statements(br, oblog_br);
    }
  }
}

int ObBinlogRecordPrinter::output_heartbeat_file(const int fd, const int64_t heartbeat_timestamp)
{
  OB_ASSERT(fd >= 0);
  int ret = OB_SUCCESS;

  const static int64_t BUFFER_SIZE = 64;
  char buffer[BUFFER_SIZE];
  int64_t pos = 0;

  DATABUFF_PRINTF(buffer, sizeof(buffer), pos, "%ld\n", heartbeat_timestamp);

  // Empty the file
  (void)ftruncate(fd, 0);

  int64_t left_len = pos;
  const char *ptr = buffer;
  while (OB_SUCCESS == ret && left_len > 0) {
    int64_t write_len = write(fd, ptr, left_len);
    if (write_len < 0) {
      LOG_ERROR("write heartbeat file fail", K(errno), KERRMSG, K(fd), K(left_len));
      ret = OB_ERR_UNEXPECTED;
    } else {
      left_len -= write_len;
      ptr += write_len;
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::output_data_file(IBinlogRecord *br,
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
    bool &need_rotate_file)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(br) || OB_ISNULL(oblog_br) || OB_UNLIKELY(fd < 0)) {
    LOG_ERROR("invalid argument", K(br), K(record_type), "record_type", print_record_type(record_type),
        K(oblog_br), K(fd));
    ret = OB_INVALID_ARGUMENT;
  } else {
    static const int64_t DATA_BUFFER_SIZE = 128 * 1024 * 1024;
    char *data_buffer = (char *)new char[DATA_BUFFER_SIZE];

    // Record Index: index of statements within a transaction
    static uint64_t ri = 0;
    static const int64_t TRANS_ID_BUF_LENGTH = 1024;
    static char begin_trans_id[TRANS_ID_BUF_LENGTH];

    int64_t pos = 0;
    char *ptr = data_buffer;
    int64_t size = DATA_BUFFER_SIZE;
    // get filterRuleValue
    unsigned int filter_rv_count = 0;
    BinlogRecordImpl *filter_rv_impl = static_cast<BinlogRecordImpl *>(br);
    const binlogBuf *filter_rv = filter_rv_impl->filterValues((unsigned int &) filter_rv_count);
    common::ObString trace_id;
    common::ObString unique_id;
    const char *trace_info_ptr = br->obTraceInfo();
    common::ObString trace_info(trace_info_ptr);

    if (filter_rv != NULL && filter_rv_count > 2) {
      unique_id.assign_ptr(filter_rv[1].buf, filter_rv[1].buf_used_size);
      trace_id.assign_ptr(filter_rv[2].buf, filter_rv[2].buf_used_size);
    }

    if (EBEGIN == record_type) {
      ri = 0;
      ROW_PRINTF(ptr, size, pos, ri, "BEGIN");
      ROW_PRINTF(ptr, size, pos, ri, "org_cluster_id:%u", br->getThreadId());

      memset(begin_trans_id, '\0', TRANS_ID_BUF_LENGTH * sizeof(char));
      MEMCPY(begin_trans_id, unique_id.ptr(), unique_id.length());
      begin_trans_id[unique_id.length()] = '\0';
      // The forth slot is major_version
      int32_t major_version;
      if (OB_FAIL(parse_major_version_(filter_rv, major_version))) {
        LOG_ERROR("failed to parse major version", KR(ret), K(oblog_br), K(filter_rv), K(major_version));
      } else if (major_version > 0) {
        ROW_PRINTF(ptr, size, pos, ri, "major_version:%d", major_version);
      } else {
        // do nothing
      }
    } else if (ECOMMIT == record_type) {
      if (only_print_dml_tx_checksum) {
        ri++;
        // TODO
        // Support to print CRC value, at present PDML transaction data change order is not stable
        dml_data_crc = 0;
        ROW_PRINTF(ptr, size, pos, ri, "TX_BR_COUNT:%ld, TX_DATA_CRC:%lu", tx_br_count, dml_data_crc);
        tx_br_count = 0;
        dml_data_crc = 0;
      }
      ri++;
      ROW_PRINTF(ptr, size, pos, ri, "COMMIT");
    } else if (EDDL == record_type) {
      ri = 0;
      ITableMeta *table_meta = NULL;
      if (0 != br->getTableMeta(table_meta)) {
        LOG_ERROR("table_meta is NULL");
        ret = OB_ERR_UNEXPECTED;
      }
      int64_t column_count = table_meta ? table_meta->getColCount() : -1;

      // TODO
      // 2.0.0 DDL binlog record with a new column ddl_schema_version
      // To ensure 1.0 compatibility, column_count is first output as 1
      if (2 == column_count) {
        column_count = 1;
      }

      ROW_PRINTF(ptr, size, pos, ri, "record_type:%s", print_record_type(record_type));
      ROW_PRINTF(ptr, size, pos, ri, "db_name:%s", br->dbname());
      ROW_PRINTF(ptr, size, pos, ri, "table_name:%s", br->tbname());
      ROW_PRINTF(ptr, size, pos, ri, "column_count:%ld", column_count);
      // The DDL is in memory, not persistent, and is accessed via the following interface
      int64_t new_cols_count = 0;
      binlogBuf *new_cols = br->newCols((unsigned int &)new_cols_count);

      for (int64_t index = 0; index < new_cols_count; index++) {
        IColMeta *col_meta = table_meta ? table_meta->getCol((int)index) : NULL;
        const char *cname = col_meta ? col_meta->getName() : "NULL";
        // DDL binlog only output ddl_stmt
        if (0 == index) {
          ROW_PRINTF(ptr, size, pos, ri, "column_name:%s", cname);
          ROW_PRINTF(ptr, size, pos, ri, "ddl_stmt_str: %.*s", (int)new_cols[index].buf_used_size, new_cols[index].buf);
          ROW_PRINTF(ptr, size, pos, ri, "ddl_stmt_len: %ld", new_cols[index].buf_used_size);
        } else {
          LOG_INFO("DDL binlog record", K(index), "column_name", cname,
              "ddl_schema_version", new_cols[index].buf,
              "len", new_cols[index].buf_used_size);
        }
      }

      if (enable_verify_mode) {
        if (unique_id.length() > 0) {
          ROW_PRINTF(ptr, size, pos, ri, "unique_id:[%.*s](%d)", unique_id.length(), unique_id.ptr(), unique_id.length());
        }
      }
    } else if ((EINSERT == record_type || EUPDATE == record_type || EDELETE == record_type) && ! only_print_dml_tx_checksum) {
      ri++;
      ITableMeta *table_meta = br->getTableMeta();
      int64_t column_count = table_meta ? table_meta->getColCount() : -1;
      const char *pks = table_meta ? (table_meta->getPKs()) : "NULL";
      const char *uks = table_meta ? (table_meta->getUKs()) : "NULL";
      const char *has_pk = table_meta ? (table_meta->hasPK() ? "true" : "false") : "NULL";
      const char *has_uk = table_meta ? (table_meta->hasUK() ? "true" : "false") : "NULL";
      const char *pk_info = table_meta ? table_meta->getPkinfo() : "NULL";
      const char *uk_info = table_meta ? table_meta->getUkinfo() : "NULL";

      ROW_PRINTF(ptr, size, pos, ri, "record_type:%s", print_record_type(record_type));
      ROW_PRINTF(ptr, size, pos, ri, "database_name:%s", br->dbname());
      ROW_PRINTF(ptr, size, pos, ri, "table_name:%s", br->tbname());
      ROW_PRINTF(ptr, size, pos, ri, "log_event:%s", br->firstInLogevent() ? "true" : "false");
      ROW_PRINTF(ptr, size, pos, ri, "column_count:%ld", column_count);
      ROW_PRINTF(ptr, size, pos, ri, "source_category:%s", print_src_category(br->getSrcCategory()));
      ROW_PRINTF(ptr, size, pos, ri, "source_type:%s", print_record_src_type(br->getSrcType()));
      ROW_PRINTF(ptr, size, pos, ri, "has_pk:%s", has_pk);
      ROW_PRINTF(ptr, size, pos, ri, "pk_info:%s", pk_info);
      ROW_PRINTF(ptr, size, pos, ri, "pks:%s", pks);
      ROW_PRINTF(ptr, size, pos, ri, "has_uk:%s", has_uk);
      ROW_PRINTF(ptr, size, pos, ri, "uk_info:%s", uk_info);
      ROW_PRINTF(ptr, size, pos, ri, "uks:%s", uks);

      // If trace_id is not empty, then print (trace_id is deprecated in 4.x)
      if (trace_id.length() > 0) {
        ROW_PRINTF(ptr, size, pos, ri, "trace_id:[%.*s](%d)", trace_id.length(), trace_id.ptr(), trace_id.length());
      }

      // if trace_info is not empty and enable_print_detail, then print
      if (enable_print_special_detail && 0 < trace_info.length()) {
        ROW_PRINTF(ptr, size, pos, ri, "trace_info:[%.*s](%d)", trace_info.length(), trace_info.ptr(), trace_info.length());
      }

      for (int64_t index = 0; OB_SUCC(ret) && index < column_count; index++) {
        ret = output_data_file_column_data(br, table_meta, index, ptr, size, ri, only_print_hex, enable_print_hex,
            enable_print_lob_md5, enable_print_detail, enable_print_special_detail, pos);
      }

      DATABUFF_PRINTF(ptr, size, pos, "%s", STMT_DELEMITER);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(verify_begin_trans_id_(*oblog_br, begin_trans_id))) {
          LOG_ERROR("verify_begin_trans_id_ fail", KR(ret), K(oblog_br), K(begin_trans_id));
        }
      }
    }

    if (OB_SUCCESS == ret && 0 < pos) {
      bool is_line_end = false;

      if (EDDL == record_type || ECOMMIT == record_type) {
        is_line_end = true;
      }

      if (OB_FAIL(write_data_file(fd, ptr, size, pos, is_line_end, need_rotate_file))) {
        LOG_ERROR("write_data_file fail", K(ret), K(fd), K(size), K(pos), KP(ptr));
      }
    }

    if (NULL != data_buffer) {
      delete []data_buffer;
      data_buffer = NULL;
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::parse_major_version_(const binlogBuf *filter_rv, int32_t &major_version) {
  int ret = OB_SUCCESS;
  major_version = -1; // default -1, invalid value
  // Get major version, major version is only output if version 1.x is configured and the corresponding configuration item is configured
  bool need_major_version = false;
  if (need_major_version) {
    const binlogBuf *major_version_buf = filter_rv + 3;
    major_version = (int32_t) atoi(major_version_buf->buf);
  }
  return ret;
}

int ObBinlogRecordPrinter::verify_begin_trans_id_(ObLogBR &oblog_br,
    const char *begin_trans_id)
{
  int ret = OB_SUCCESS;

  static const int64_t TRANS_ID_BUF_LENGTH = 1024;
  char trans_id_buf[TRANS_ID_BUF_LENGTH];
  int64_t pos = 0;
  ObLogEntryTask *log_entry_task = NULL;
  PartTransTask *task = NULL;

  if (OB_ISNULL(begin_trans_id)) {
    LOG_ERROR("begin_trans_id is null");
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(log_entry_task = static_cast<ObLogEntryTask *>(oblog_br.get_host()))) {
    LOG_ERROR("log_entry_task is NULL", KPC(log_entry_task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(task = static_cast<PartTransTask *>(log_entry_task->get_host()))) {
    LOG_ERROR("part trans task is NULL", KPC(task), KPC(log_entry_task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const uint64_t tenant_id = task->get_tenant_id();
    const transaction::TxID &trans_id = task->get_trans_id();

    if (OB_FAIL(common::databuff_printf(trans_id_buf, TRANS_ID_BUF_LENGTH, pos,
        "%lu_%ld", tenant_id, trans_id))) {
      LOG_ERROR("databuff_printf fail", KR(ret), K(tenant_id), K(trans_id), K(trans_id_buf), K(TRANS_ID_BUF_LENGTH), K(pos));
    } else {
      trans_id_buf[pos] = '\0';

      if (0 == strcmp(begin_trans_id, trans_id_buf)) {
        LOG_DEBUG("verify_begin_trans_id_ succ", K(begin_trans_id), K(trans_id_buf));
      } else {
        LOG_ERROR("current trans_id is not equal to begin_trans_id", K(begin_trans_id), K(trans_id_buf));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::output_data_file_column_data(IBinlogRecord *br,
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
    int64_t &pos)
{
  OB_ASSERT(NULL != br && NULL != table_meta && NULL != ptr && size > 0 && index >= 0 && pos >= 0);

  int ret = OB_SUCCESS;
  int64_t new_cols_count = 0;
  int64_t old_cols_count = 0;
  binlogBuf *new_cols = br->newCols((unsigned int &)new_cols_count);
  binlogBuf *old_cols = br->oldCols((unsigned int &)old_cols_count);
  IColMeta *col_meta = table_meta ? table_meta->getCol((int)index) : NULL;
  IStrArray *values_of_enum_set = col_meta ? col_meta->getValuesOfEnumSet() : NULL;
  const char *cname = col_meta ? col_meta->getName() : "NULL";
  int ctype = col_meta ? col_meta->getType() : -1;
  const char *is_pk = col_meta ? (col_meta->isPK() ? "true" : "false") : "NULL";
  const char *encoding = col_meta ? col_meta->getEncoding() : "NULL";
  const char *is_not_null = col_meta ? (col_meta->isNotNull() ? "true" : "false") : "NULL";
//  const char *default_val = col_meta ? col_meta->getDefault() : "NULL";
  const char *is_signed = col_meta ? (col_meta->isSigned() ? "true" : "false") : "NULL";
  const long scale = col_meta ? col_meta->getScale(): 0;
  const long precision = col_meta ? col_meta->getPrecision(): 0;
  const long col_data_length = col_meta ? col_meta->getLength(): 0;
  bool is_generated_column = col_meta ? col_meta->isGenerated() : false;
  bool is_hidden_row_key_column = col_meta ? col_meta->isHiddenRowKey() : false;
  bool is_partition_column = col_meta ? col_meta->isPartitioned() : false;
  bool is_generate_dep_column = col_meta ? col_meta->isDependent() : false;
  bool is_lob = is_lob_type(ctype);
  bool is_json = is_json_type(ctype);
  ObArenaAllocator str_allocator;
  ObStringBuffer enum_set_values_str(&str_allocator);
  bool is_geometry = is_geometry_type(ctype);
  bool is_xml = is_xml_type(ctype);

  int64_t column_index = index + 1;
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_name:%s", column_index, cname);
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_type:%s", column_index, get_ctype_string(ctype));
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_rowkey:%s", column_index, is_pk);
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_signed:%s", column_index, is_signed);
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_encoding:%s", column_index, encoding);
  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_not_null:%s", column_index, is_not_null);
  if (enable_print_detail) {
    if (is_hidden_row_key_column) {
      ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_hidden_rowkey:%d", column_index, is_hidden_row_key_column);
    }
    if (is_partition_column) {
      ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_partition_col:true", column_index);
    }
    if (is_generate_dep_column) {
      ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_is_dep_col_of_gen_col:true", column_index);
    }
    //  print the length of varchar only in print detail mode,
    //  because there have been many test cases with varchar type before the varchar length info is added into column meta
    if (oceanbase::obmysql::MYSQL_TYPE_VAR_STRING == ctype) {
      ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_define_length:%ld", column_index, col_data_length);
    }
    else if ((oceanbase::obmysql::MYSQL_TYPE_ENUM == ctype) || (oceanbase::obmysql::MYSQL_TYPE_SET == ctype)) {
      const char *delim = ",";
      for (int i = 0; i < values_of_enum_set->size(); i++) {
        const char *elem_str = (*values_of_enum_set)[i];
        if (OB_FAIL(enum_set_values_str.append(elem_str))) {
          LOG_ERROR("enum_set_value_str append failed", K(i), K(elem_str),
            "enum_set_value_str", enum_set_values_str.ptr());
        } else if (i != values_of_enum_set->size() - 1 && OB_FAIL(enum_set_values_str.append(delim))) {
          LOG_ERROR("enum_set_value_str append failed", K(i), K(delim),
            "enum_set_value_str", enum_set_values_str.ptr());
        }
      }
      ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_extend_info:%s", column_index, enum_set_values_str.ptr());
    }
    // print precision & scale only in print detail mode, becacuse INT in oracle mode is also a kind of NUMBER(DECIMAL)
    // whose precision is 38 and scale is 0
    // the default value of precision is -1(PRECISION_UNKNOWN_YET) and the default value of scale is -85
    // (ORA_NUMBER_SCALE_UNKNOWN_YET), when using default precision & scale, the number type would behave adaptively
    else if ((oceanbase::obmysql::MYSQL_TYPE_DECIMAL == ctype) || (oceanbase::obmysql::MYSQL_TYPE_NEWDECIMAL == ctype)) {
      // Not sure if MYSQL_TYPE_DECIMAL is deprecated, DECIMAL in mysql & oracle mode should be MYSQL_TYPE_NEWDECIMAL
      ROW_PRINTF(ptr, size, pos , ri, "[C%ld] column_precision:%ld", column_index, precision);
      ROW_PRINTF(ptr, size, pos , ri, "[C%ld] column_scale:%ld", column_index, scale);
    } else if (oceanbase::obmysql::MYSQL_TYPE_TIMESTAMP == ctype ||
               oceanbase::obmysql::MYSQL_TYPE_DATETIME == ctype ||
               oceanbase::obmysql::MYSQL_TYPE_TIME == ctype) {
      ROW_PRINTF(ptr, size, pos , ri, "[C%ld] column_scale:%ld", column_index, scale);
    } else if (oceanbase::obmysql::MYSQL_TYPE_BIT == ctype) {
      ROW_PRINTF(ptr, size, pos , ri, "[C%ld] column_precision:%ld", column_index, precision);
    } else { }
  }
  if (is_generated_column) {
    ROW_PRINTF(ptr, size, pos, ri, "[C%ld] is_generated_column:%d", column_index, is_generated_column);
  }

  // FIXME: does not check the value of the field until the length of the default value can be obtained
  //  ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_default_value:%s", column_index, default_val);

  if (OB_SUCC(ret)) {
    if (index < new_cols_count) {
      const char *new_col_value = new_cols[index].buf;
      size_t new_col_value_len = new_cols[index].buf_used_size;

      if ((is_lob || is_json || is_geometry || is_xml) && enable_print_lob_md5) {
        ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_new_md5:[%s](%ld)",
            column_index, calc_md5_cstr(new_col_value, new_col_value_len), new_col_value_len);
      } else {
        if (! only_print_hex) {
          ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_new:[%.*s](%ld)",
              column_index, COL_PRINT_VALUE(new_col_value, new_col_value_len));
        }

        if (OB_SUCCESS == ret && enable_print_hex && need_print_hex(ctype)) {
          ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_new_hex:", column_index);
          pos--;

          if (OB_SUCCESS == ret && OB_FAIL(print_hex(new_col_value, new_col_value_len, ptr, size, pos))) {
            LOG_ERROR("print_hex fail", K(ret));
          }
        }
      }
    }

    if (OB_SUCCESS == ret && index < old_cols_count) {
      const char *old_col_value = old_cols[index].buf;
      size_t old_col_value_len = old_cols[index].buf_used_size;

      if (EMySQLFieldType::MYSQL_TYPE_BIT == ctype) {
        ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_old_hex:", column_index);
        pos--;

        if (OB_SUCCESS == ret && OB_FAIL(print_hex(old_col_value, old_col_value_len, ptr, size, pos))) {
          LOG_ERROR("print_hex fail", K(ret));
        }
      } else if ((is_lob || is_json || is_geometry || is_xml) && enable_print_lob_md5) {
        ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_old_md5:[%s](%ld)",
            column_index, calc_md5_cstr(old_col_value, old_col_value_len), old_col_value_len);
      } else {
        ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_old:[%.*s](%ld)",
            column_index, COL_PRINT_VALUE(old_col_value, old_col_value_len));

        if (OB_SUCCESS == ret && enable_print_hex && need_print_hex(ctype)) {
          ROW_PRINTF(ptr, size, pos, ri, "[C%ld] column_value_old_hex:", column_index);
          pos--;

          if (OB_SUCCESS == ret && OB_FAIL(print_hex(old_col_value, old_col_value_len, ptr, size, pos))) {
            LOG_ERROR("print_hex fail", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::print_hex(const char *str, int64_t len, char *buf, int64_t size, int64_t &pos)
{
  OB_ASSERT(NULL != buf && size > 0 && pos >= 0);

  int ret = OB_SUCCESS;
  int64_t hex_len = 0;

  DATABUFF_PRINTF(buf, size, pos, "[");

  int64_t last_pos = pos;
  if (NULL != str && len > 0)
  {
    for (int64_t i=0; i<len; i++)
    {
      DATABUFF_PRINTF(buf, size, pos, "%02x", (unsigned char)str[i]);
    }
  }

  hex_len = pos - last_pos;
  DATABUFF_PRINTF(buf, size, pos, "](%ld)%s", hex_len, DATA_DELEMITER);

  return ret;
}

void ObBinlogRecordPrinter::console_print_begin(IBinlogRecord *br, ObLogBR *oblog_br)
{
  int ret = OB_SUCCESS;
  if (NULL != br && NULL != oblog_br) {
    int64_t delta = ObTimeUtility::current_time() - get_precise_timestamp(*br);
    double delay_sec = static_cast<double>(delta) / 1000000.0;
    int64_t timestamp_usec = br->getTimestamp() * 1000000 + br->getRecordUsec();
    int64_t filter_rv_count = 0;
    BinlogRecordImpl *filter_rv_impl = static_cast<BinlogRecordImpl *>(br);
    const binlogBuf *filter_rv = filter_rv_impl->filterValues((unsigned int &) filter_rv_count);
    LOG_STD("BEGIN  TM=[%ld] DELAY=[%.3lf sec] ORG_CLUSTER_ID=%u ", timestamp_usec, delay_sec, br->getThreadId());
    // The forth slot is major_version
    int32_t major_version;
    if (OB_FAIL(parse_major_version_(filter_rv, major_version))) {
      LOG_ERROR("failed to parse major version", KR(ret), K(oblog_br), K(filter_rv), K(major_version));
    } else if (major_version > 0) {
      LOG_STD(" major version=[%d]\n\n", major_version);
    } else {
      LOG_STD("\n\n");
    }
  }
}

void ObBinlogRecordPrinter::console_print_commit(IBinlogRecord *br, ObLogBR *oblog_br)
{
  if (NULL != br && NULL != oblog_br) {
    int64_t delta = ObTimeUtility::current_time() - get_precise_timestamp(*br);
    double delay_sec = static_cast<double>(delta) / 1000000.0;
    int64_t timestamp_usec = br->getTimestamp() * 1000000 + br->getRecordUsec();

    LOG_STD("\nCOMMIT  TM=[%ld] DELAY=[%.3lf sec]\n\n", timestamp_usec, delay_sec);
  }
}

void ObBinlogRecordPrinter::console_print_heartbeat(IBinlogRecord *br, ObLogBR *oblog_br)
{
  if (NULL != br && NULL != oblog_br) {
    int64_t delta = ObTimeUtility::current_time() - get_precise_timestamp(*br);
    double delay_sec = static_cast<double>(delta) / 1000000.0;
    int64_t timestamp_usec = br->getTimestamp() * 1000000 + br->getRecordUsec();

    LOG_STD("HEARTBEAT  TM=[%ld] DELAY=[%.3lf sec]\n\n", timestamp_usec, delay_sec);
  }
}

void ObBinlogRecordPrinter::console_print_statements(IBinlogRecord *br, ObLogBR *oblog_br)
{
  if (NULL != br && NULL != oblog_br) {
    int64_t old_cols_count = 0;
    int64_t new_cols_count = 0;
    binlogBuf *old_cols = br->oldCols((unsigned int &)old_cols_count);
    binlogBuf *new_cols = br->newCols((unsigned int &)new_cols_count);
    BinlogRecordImpl *filter_rv_impl = static_cast<BinlogRecordImpl *>(br);
    unsigned int filter_rv_count = 0;
    const binlogBuf *filter_rv = filter_rv_impl->filterValues((unsigned int &) filter_rv_count);
    ObString trace_id;
    ObString unique_id;

    if (filter_rv != NULL && filter_rv_count > 2) {
      unique_id.assign_ptr(filter_rv[1].buf, filter_rv[1].buf_used_size);
      trace_id.assign_ptr(filter_rv[2].buf, filter_rv[2].buf_used_size);
    }

    int64_t delta = ObTimeUtility::current_time() - get_precise_timestamp(*br);
    double delay_sec = static_cast<double>(delta) / 1000000.0;
    int64_t timestamp_usec = br->getTimestamp() * 1000000 + br->getRecordUsec();

    const char *padding = (EDDL == br->recordType() ? "" : "  ");

    LOG_STD("%s[%s] DB=[%s] TB=[%s] TM=[%ld] CHKP=[%s] DELAY=[%.3lf sec] SRC_CAT=[%s] TRACE_ID=[%.*s](%d)\n"
        "%s  UNIQUE_ID=[%.*s](%d)\n",
        padding, print_record_type(br->recordType()), br->dbname(), br->tbname(),
        timestamp_usec, br->getCheckpoint(), delay_sec, print_src_category(br->getSrcCategory()),
        trace_id.length(), trace_id.ptr(), trace_id.length(),
        padding, unique_id.length(), unique_id.ptr(), unique_id.length());

    LOG_STD("%s  NewCols[%ld]  ", padding, new_cols_count);
    for (int64_t index = 0; NULL != new_cols && index < new_cols_count; index++) {
      LOG_STD("  [%.*s](%ld)", COL_PRINT_VALUE(new_cols[index].buf, new_cols[index].buf_used_size));
    }
    LOG_STD("\n");
    LOG_STD("%s  OldCols[%ld]  ", padding, old_cols_count);
    for (int64_t index = 0; NULL != old_cols && index < old_cols_count; index++) {
      LOG_STD("  [%.*s](%ld)", COL_PRINT_VALUE(old_cols[index].buf, old_cols[index].buf_used_size));
    }
    LOG_STD("\n");

    if (EDDL == br->recordType()) {
      LOG_STD("\n");
    }
  }
}

bool ObBinlogRecordPrinter::need_print_hex(int ctype)
{
  return (obmysql::MYSQL_TYPE_VARCHAR == ctype
      || obmysql::MYSQL_TYPE_VAR_STRING == ctype
      || obmysql::MYSQL_TYPE_STRING == ctype
      || obmysql::MYSQL_TYPE_OB_NVARCHAR2 == ctype
      || obmysql::MYSQL_TYPE_OB_NCHAR == ctype
      || obmysql::MYSQL_TYPE_JSON == ctype
      || obmysql::MYSQL_TYPE_GEOMETRY == ctype);
}

int ObBinlogRecordPrinter::write_data_file(const int fd,
    char *buf,
    const int64_t size,
    const int64_t pos,
    const bool is_line_end,
    bool &need_rotate_file)
{
  OB_ASSERT(0 <= fd && NULL != buf && 0 < size && size >= pos);

  int ret = OB_SUCCESS;
  int64_t  left_len = pos;
  const char *ptr = buf;

  need_rotate_file = false;

  if (is_line_end) {
    DATABUFF_PRINTF(buf, size, left_len, "\n");
  }

  while (OB_SUCCESS == ret && left_len > 0) {
    int64_t write_len = write(fd, ptr, left_len);
    if (write_len < 0) {
      LOG_ERROR("write data file fail", K(errno), KERRMSG, K(fd));
      ret = OB_ERR_UNEXPECTED;
      break;
    } else {
      left_len -= write_len;
      ptr += write_len;
    }
  }

  if (OB_SUCCESS == ret && is_line_end) {
    off_t offset = ::lseek(fd, 0, SEEK_END);
    if (static_cast<int64_t>(offset) >= MAX_DATA_FILE_SIZE) {
      need_rotate_file = true;
    }
  }

  return ret;
}

int ObBinlogRecordPrinter::rotate_data_file_()
{
  OB_ASSERT(NULL != data_file_ && data_file_fd_ >= 0);

  int ret = OB_SUCCESS;

  char old_file_name[256];
  time_t t;
  time(&t);
  struct tm tm;
  localtime_r((const time_t*)&t, &tm);

  snprintf(old_file_name, sizeof(old_file_name), "%s.%04d%02d%02d%02d%02d%02d",
      data_file_, tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
      tm.tm_hour, tm.tm_min, tm.tm_sec);

  if (0 != rename(data_file_, old_file_name)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("rename data file failed", KR(ret), K_(data_file), K(old_file_name), K(errno), KERRMSG);
  } else {
    int fd = open(data_file_, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    if (0 > fd) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("open data file fail", KR(ret), K_(data_file), K(errno), KERRMSG);
    } else {
      close(data_file_fd_);
      data_file_fd_ = fd;
    }
  }

  return ret;
}

void ObBinlogRecordPrinter::do_br_statistic_(IBinlogRecord &br)
{
  if (only_print_dml_tx_checksum_) {
    int record_type = br.recordType();
    int64_t new_cols_count = 0;
    int64_t old_cols_count = 0;

    if (ECOMMIT == record_type) {
      total_tx_count_++;
    } else if (EDDL == record_type) {
      total_tx_count_++;
      total_br_count_++;
    } else if (EINSERT == record_type || EUPDATE == record_type || EDELETE == record_type) {
      dml_tx_br_count_++;
      total_br_count_++;
      binlogBuf *new_cols = br.newCols((unsigned int &)new_cols_count);
      binlogBuf *old_cols = br.oldCols((unsigned int &)old_cols_count);

      for (int index = 0; index < new_cols_count; index++) {
        const char *new_col_value = new_cols[index].buf;
        size_t new_col_value_len = new_cols[index].buf_used_size;
        if (OB_NOT_NULL(new_col_value)) {
          calc_crc_checksum(dml_data_crc_, new_col_value, new_col_value_len);
        }
      }

      for (int index = 0; index < old_cols_count; index++) {
        const char *old_col_value = old_cols[index].buf;
        size_t old_col_value_len = old_cols[index].buf_used_size;
        if (OB_NOT_NULL(old_col_value)) {
          calc_crc_checksum(dml_data_crc_, old_col_value, old_col_value_len);
        }
      }
    }

    if (ECOMMIT == record_type) {
      output_statistics_();
    }
  }
}

void ObBinlogRecordPrinter::output_statistics_()
{
  _LOG_INFO("[CDC_STAT] [TX_COUNT: %ld] [TOTAL_BR_COUNT: %ld] [DATA_CRC: %lu]", total_tx_count_, total_br_count_, dml_data_crc_);
}

} // namespace libobcdc
} // namespace oceanbase
