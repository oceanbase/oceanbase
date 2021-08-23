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

#define USING_LOG_PREFIX CLOG
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "ob_log_entry_parser.h"
#include "ob_ilog_entry_parser.h"
#include "cmd_args_parser.h"

#include "ob_admin_clog_v2_executor.h"
#include "ob_func_utils.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_version.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::clog;

namespace oceanbase {
namespace tools {

#define getcfg(key) getenv(key)

ObAdminClogV2Executor::ObAdminClogV2Executor() : DB_port_(-1), is_ofs_open_(false)
{}

ObAdminClogV2Executor::~ObAdminClogV2Executor()
{}

int ObAdminClogV2Executor::execute(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 4) || OB_ISNULL(argv)) {
    print_usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_options(argc, argv))) {
    LOG_WARN("failed to parse options", K(ret));
  } else {
    int new_argc = argc - optind;
    char** new_argv = argv + optind;
    if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_all) : OB_NEED_RETRY)) {
      LOG_INFO("finish dump_all ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_filter) : OB_NEED_RETRY)) {
      LOG_INFO("finish dump_filter ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_hex) : OB_NEED_RETRY)) {
      LOG_INFO("finish dump_hex ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_format) : OB_NEED_RETRY)) {
      LOG_INFO("finish dump_format ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, stat_clog) : OB_NEED_RETRY)) {
      LOG_INFO("finish stat_clog ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, grep) : OB_NEED_RETRY)) {
      LOG_INFO("finish encode", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_ilog) : OB_NEED_RETRY)) {
      LOG_INFO("finish encode", K(ret));
    } else {
      fprintf(stderr, "failed %d", ret);
      print_usage();
    }
  }
  return ret;
}

int ObAdminClogV2Executor::parse_options(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  int option_index = 0;
  struct option long_options[] = {{"host", 1, NULL, 'h'}, {"port", 1, NULL, 'p'}, {NULL, 0, NULL, 0}};
  int c;
  while (-1 != (c = getopt_long(argc, argv, "h:p:", long_options, &option_index))) {
    switch (c) {
      case 'h':
        DB_host_.assign_ptr(optarg, strlen(optarg));
        break;
      case 'p':
        DB_port_ = static_cast<int32_t>(strtol(optarg, NULL, 10));
        break;
      case '?':
      case ':':
        ret = OB_ERR_UNEXPECTED;
        break;
      default:
        break;
    }
  }
  return ret;
}

void ObAdminClogV2Executor::print_usage()
{
  fprintf(stdout,
      "Usages:\n"
      "$ob_admin clog_tool dump_ilog ilog_files ## ./ob_admin clog_tool dump_ilog 1 2 3\n"
      "$ob_admin clog_tool dump_all log_files ## ./ob_admin clog_tool dump_all 1 2 3\n"
      "$ob_admin clog_tool dump_filter filter_str log_files ## ./ob_admin clog_tool dump_filter "
      "'table_id=123;partition_id=123;log_id=123' 1 2 3\n"
      "$ob_admin clog_tool dump_hex log_files ## ./ob_admin clog_tool dump_hex 1 2 3\n"
      "$ob_admin clog_tool dump_format log_files ## ./ob_admin clog_tool dump_format 1 2 3\n");
}

int ObAdminClogV2Executor::dump_all(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  const bool is_hex = false;
  if (OB_FAIL(dump_inner(argc, argv, is_hex))) {
    LOG_WARN("failed to dump all", K(ret));
  }
  return ret;
}

int ObAdminClogV2Executor::dump_filter(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  const bool is_hex = false;
  if (OB_FAIL(filter_.parse(argv[0]))) {
    LOG_WARN("parse filter failed", K(ret), K(argv[0]));
  } else {
    LOG_INFO("dump with filter", K_(filter), K(argv[0]));
    if (OB_FAIL(dump_inner(argc - 1, argv + 1, is_hex))) {
      LOG_WARN("failed to dump all", K(ret));
    }
  }
  return ret;
}

int ObAdminClogV2Executor::dump_hex(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  const bool is_hex = true;
  if (OB_FAIL(dump_inner(argc, argv, is_hex))) {
    LOG_WARN("failed to dump hex", K(ret));
  }
  return ret;
}

int ObAdminClogV2Executor::dump_inner(int argc, char* argv[], bool is_hex)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump all ", K(is_hex), K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_single_clog(argv[i], is_hex)) && OB_ITER_END != ret) {
        LOG_WARN("failed to dump log ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObAdminClogV2Executor::dump_format(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump format", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_format_single_file(argv[i])) && (OB_ITER_END != ret)) {
        LOG_WARN("failed to dump format ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObAdminClogV2Executor::stat_clog(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to stat_clog", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(stat_single_log(argv[i])) && (OB_ITER_END != ret)) {
        LOG_WARN("failed to stat_single_log", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObAdminClogV2Executor::dump_ilog(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump ilog ", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_single_ilog(argv[i])) && OB_ITER_END != ret) {
        LOG_WARN("failed to dump ilog ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObAdminClogV2Executor::grep(int argc, char* argv[])
{
  int ret = OB_SUCCESS;
  const int64_t BUF_LEN = 1024;
  char result[BUF_LEN] = {0};
  if (OB_UNLIKELY(1 > argc) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to grep", K(ret));
    char arg_str[BUF_LEN] = {0};
    int64_t pos = 0;
    memcpy(arg_str, argv[0], strlen(argv[0]));
    char* input_str = arg_str;
    char encode_type[64] = {0};
    int64_t int_value = 0;
    char origin_str[BUF_LEN] = {0};
    char* token = NULL;
    int32_t local_ret = 0;
    while (OB_SUCC(ret) && NULL != (token = STRSEP(&input_str, "%"))) {
      MEMSET(origin_str, '\0', BUF_LEN);
      if (0 == STRLEN(token)) {
        // do nothing
      } else if (0 == (local_ret = sscanf(token, "%[0-9a-z]:%ld%s", encode_type, &int_value, origin_str)) ||
                 1 == local_ret) {
        if (0 > (local_ret = snprintf(result + pos, BUF_LEN - pos, "%s", token))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buf is not enough", "token_len", STRLEN(token), K(local_ret), K(BUF_LEN), K(ret));
        } else {
          pos += local_ret;
          LOG_DEBUG("match normal str", K(token), K(pos));
        }
      } else if (2 == local_ret || 3 == local_ret) {
        if (0 != STRCMP("i4", encode_type) && 0 != STRCMP("i8", encode_type) && 0 != STRCMP("v4", encode_type) &&
            0 != STRCMP("v8", encode_type)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid encode type", K(token), K(encode_type), K(ret));
          print_usage();
        } else if (OB_FAIL(encode_int(result, pos, BUF_LEN, encode_type, int_value))) {
          LOG_WARN("failed to encode_int", K(result), K(pos), K(BUF_LEN), K(encode_type), K(int_value), K(ret));
        } else {
          if (0 > (local_ret = snprintf(result + pos, BUF_LEN - pos, "%s", origin_str))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("buf is not enough", "token_len", STRLEN(token), K(BUF_LEN), K(ret));
          } else {
            pos += local_ret;
            LOG_DEBUG("match normal str", K(token), K(pos));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid token", K(token), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (1 == argc) {
      int fd = 0;
      struct stat sb;
      if (-1 == fstat(fd, &sb)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to fstat", K(errno), K(ret));
      } else {
        if (S_ISFIFO(sb.st_mode) || S_ISREG(sb.st_mode)) {
          execl("/usr/bin/xargs", "xargs", "-n", "1", "-P", "30", "grep", "-UP", result, NULL);
          if (-1 == errno) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to execve", K(errno), K(ret));
          }
        } else {
          fprintf(stdout, "ls store/clog/{1..3} |xargs -n 1 -P 30 grep -UP '%s' \n", result);
        }
      }
    } else if (argc > 1) {
      const int64_t COMMAND_BUF_SIZE = 1024;
      char command[COMMAND_BUF_SIZE] = {0};
      snprintf(command, COMMAND_BUF_SIZE, "xargs -n 1 -P 30 grep -UP '%s' \n", result);
      FILE* file = popen(command, "w");

      if (NULL == file) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to popen", K(errno), K(ret));
      } else {
        for (int64_t i = 1; OB_SUCC(ret) && i < argc; ++i) {
          size_t w_len = 0;
          size_t arg_len = STRLEN(argv[i]);
          const size_t element_size = 1;
          if (arg_len != (w_len = fwrite(argv[i], element_size, arg_len, file))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to fwrite", K(argv[i]), K(i), K(ret));
          } else {
            fwrite((char*)"\n", 1, 1, file);
          }
        }
        if (OB_SUCC(ret)) {
          if (-1 == pclose(file)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to pclose", K(errno), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminClogV2Executor::encode_int(
    char* buf, int64_t& pos, int64_t buf_len, const char* encode_type, int64_t int_value)
{
  int ret = OB_SUCCESS;
  const int64_t BUF_LEN = 128;
  char buf_local[BUF_LEN] = {0};
  int64_t local_pos = 0;
  if (0 == STRCMP("i8", encode_type)) {
    if (OB_FAIL(serialization::encode_i64(buf_local, BUF_LEN, local_pos, int_value))) {
      LOG_WARN("failed to encode i64", K(BUF_LEN), K(local_pos), K(int_value), K(ret));
    } else {
      CLOG_LOG(DEBUG, "succ to encode i64", K(buf_local), K(buf_len), K(local_pos), K(int_value), K(ret));
    }
  } else if (0 == STRCMP("i4", encode_type)) {
    if (OB_FAIL(serialization::encode_i32(buf_local, BUF_LEN, local_pos, static_cast<int32_t>(int_value)))) {
      LOG_WARN("failed to encode i64", K(BUF_LEN), K(local_pos), K(int_value), K(ret));
    } else {
      CLOG_LOG(DEBUG, "succ to encode i32", K(buf_local), K(buf_len), K(local_pos), K(int_value), K(ret));
    }
  } else if (0 == STRCMP("v8", encode_type)) {
    if (OB_FAIL(serialization::encode_vi64(buf_local, BUF_LEN, local_pos, int_value))) {
      LOG_WARN("failed to encode i64", K(BUF_LEN), K(local_pos), K(int_value), K(ret));
    } else {
      CLOG_LOG(DEBUG, "succ to encode vi64", K(buf_local), K(buf_len), K(local_pos), K(int_value), K(ret));
    }
  } else if (0 == STRCMP("v4", encode_type)) {
    if (OB_FAIL(serialization::encode_vi32(buf_local, BUF_LEN, local_pos, static_cast<int32_t>(int_value)))) {
      LOG_WARN("failed to encode i64", K(BUF_LEN), K(local_pos), K(int_value), K(ret));
    } else {
      CLOG_LOG(DEBUG, "succ to encode vi32", K(buf_local), K(buf_len), K(local_pos), K(int_value), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid encode_type", K(encode_type), K(ret));
  }

  if (OB_SUCC(ret)) {
    int ret_len = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_pos; ++i) {
      if (4 != (ret_len = snprintf(buf + pos, buf_len - pos, "\\x%02x", (unsigned int)(unsigned char)(buf_local[i])))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to snprinf", K(buf), K(buf_local[i]), K(ret_len), K(ret));
      } else {
        pos += 4;
      }
    }
  }

  return ret;
}

int ObAdminClogV2Executor::dump_single_clog(const char* path, bool is_hex)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char* buf = NULL;
  int64_t buf_len = -1;
  uint64_t file_id = -1;
  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parse file name", K(path), K(ret));
  } else {
    const bool is_ofs = is_ofs_file(path);
    ObLogEntryParser entry_parser;
    if (OB_FAIL(entry_parser.init(file_id, buf, buf_len, filter_, DB_host_, DB_port_, config_file_, is_ofs))) {
      LOG_WARN("failed to init entry parser", K(path), K(ret));
    } else if (OB_FAIL(entry_parser.dump_all_entry(is_hex))) {
      if (OB_ITER_END == ret) {
        LOG_INFO("succ to dump_all_entry", K(path));
      } else {
        LOG_WARN("failed to dump_all_entry", K(path), K(ret));
      }
    } else { /*do nothing*/
    }
  }

  close_fd(path, fd, buf, buf_len);
  return ret;
}

int ObAdminClogV2Executor::dump_single_ilog(const char* path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char* buf = NULL;
  int64_t buf_len = -1;
  uint64_t file_id = -1;
  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parse file name", K(path), K(ret));
  } else {
    ObILogEntryParser entry_parser;
    if (OB_FAIL(entry_parser.init(file_id, buf, buf_len))) {
      LOG_WARN("failed to init entry parser", K(path), K(ret));
    } else if (OB_FAIL(entry_parser.parse_all_entry())) {
      if (OB_ITER_END == ret) {
        LOG_INFO("succ to dump_all_entry", K(path));
      } else {
        LOG_WARN("failed to dump_all_entry", K(path), K(ret));
      }
    } else { /*do nothing*/
    }
  }

  close_fd(path, fd, buf, buf_len);
  return ret;
}

int ObAdminClogV2Executor::dump_format_single_file(const char* path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char* buf = NULL;
  int64_t buf_len = -1;
  uint64_t file_id = -1;
  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parser file name ", K(path), K(ret));
  } else {
    const bool is_ofs = is_ofs_file(path);
    ObLogEntryParser entry_parser;
    if (OB_FAIL(entry_parser.init(file_id, buf, buf_len, filter_, DB_host_, DB_port_, config_file_, is_ofs))) {
      LOG_WARN("failed to init entry parser", K(path), K(ret));
    } else if (OB_FAIL(entry_parser.format_dump_entry())) {
      if (OB_ITER_END == ret) {
        LOG_INFO("succ to format_dump_all_entry", K(path));
      } else {
        LOG_WARN("failed to format_dump_entry", K(path), K(ret));
      }
    } else { /*do nothing*/
    }
  }

  close_fd(path, fd, buf, buf_len);
  return ret;
}

int ObAdminClogV2Executor::stat_single_log(const char* path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char* buf = NULL;
  int64_t buf_len = -1;
  uint64_t file_id = -1;
  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parser file name", K(path), K(ret));
  } else {
    const bool is_ofs = is_ofs_file(path);
    ObLogEntryParser entry_parser;
    if (OB_FAIL(entry_parser.init(file_id, buf, buf_len, filter_, DB_host_, DB_port_, config_file_, is_ofs))) {
      LOG_WARN("failed to init entry parser", K(ret));
    } else if (OB_FAIL(entry_parser.stat_log()) && (OB_ITER_END != ret)) {
      LOG_WARN("failed to stat log", K(path), K(ret));
    } else {
      const ObLogStat& log_stat = entry_parser.get_log_stat();
      fprintf(stdout, "log_file:%s\t stat_info:%s\n ", path, to_cstring(log_stat));
      fprintf(stdout,
          "log_file:%s\t stat_info:\n          data_block_header_size = %lf M;\n          log_header_size = %lf M;\n   "
          "       log_size = %lf M;\n "
          "         trans_log_size = %lf M;\n          mutator_size = %lf M;\n          padding_size = %lf M;\n        "
          "  new_row_size = %lf M;\n          old_row_size = %lf M;\n          total_row_size = %lf M;\n"
          "          new_primary_row_size = %lf M;\n          primary_row_count = %ld;\n          total_row_count = "
          "%ld;\n          total_log_count = %ld;\n          dist_trans_count = %ld;\n          sp_trans_count = %ld;\n"
          "          non_compressed_log_cnt = %ld;\n          compressed_log_cnt = %ld;\n          compressed_log_size "
          "= %lf M;\n          original_log_size = %lf M;\n          compress_ratio = %lf;\n          "
          "compressed_tenant_ids:[%s];\n",
          path,
          (double)log_stat.data_block_header_size_ / 1024 / 1024,
          (double)log_stat.log_header_size_ / 1024 / 1024,
          (double)log_stat.log_size_ / 1024 / 1024,
          (double)log_stat.trans_log_size_ / 1024 / 1024,
          (double)log_stat.mutator_size_ / 1024 / 1024,
          (double)log_stat.padding_size_ / 1024 / 1024,
          (double)log_stat.new_row_size_ / 1024 / 1024,
          (double)log_stat.old_row_size_ / 1024 / 1024,
          (double)(log_stat.old_row_size_ + log_stat.new_row_size_) / 1024 / 1024,
          (double)log_stat.new_primary_row_size_ / 1024 / 1024,
          log_stat.primary_row_count_,
          log_stat.total_row_count_,
          log_stat.total_log_count_,
          log_stat.dist_trans_count_,
          log_stat.sp_trans_count_,
          log_stat.non_compressed_log_cnt_,
          log_stat.compressed_log_cnt_,
          (double)log_stat.compressed_log_size_ / 1024 / 1024,
          (double)log_stat.original_log_size_ / 1024 / 1024,
          (0 == log_stat.original_log_size_) ? 1 : (double)log_stat.compressed_log_size_ / log_stat.original_log_size_,
          to_cstring(log_stat.compressed_tenant_ids_));
    }
  }

  close_fd(path, fd, buf, buf_len);
  return ret;
}

int ObAdminClogV2Executor::mmap_log_file(const char* path, char*& buf_out, int64_t& buf_len, int& fd)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  struct stat stat_buf;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log file path is NULL", K(ret));
  } else if (is_ofs_file(path)) {  // OFS file
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support ofs", K(ret));
  } else {  // Local file
    if (-1 == (fd = open(path, O_RDONLY))) {
      ret = OB_IO_ERROR;
      CLOG_LOG(ERROR, "open file fail", K(path), KERRMSG, K(ret));
    } else if (-1 == fstat(fd, &stat_buf)) {
      ret = OB_IO_ERROR;
      CLOG_LOG(ERROR, "stat_buf error", K(path), KERRMSG, K(ret));
    } else if (stat_buf.st_size > LOG_FILE_MAX_SIZE) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(ERROR, "invalid file size", K(path), K(stat_buf.st_size), K(ret));
    } else if (MAP_FAILED == (buf = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0)) || NULL == buf) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to mmap file", K(path), K(errno), KERRMSG, K(ret));
    } else {
      buf_out = static_cast<char*>(buf);
      buf_len = stat_buf.st_size;
    }
  }
  return ret;
}

int ObAdminClogV2Executor::close_fd(const char* path, const int fd, char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (is_ofs_file(path)) {
    if (fd > 0) {
      // close fd for ofs
    }
    if (nullptr != buf) {
      ob_free(buf);
    }
  } else {
    if (nullptr != buf) {
      munmap(buf, buf_len);
    }
    if (fd >= 0) {
      close(fd);
    }
  }
  return ret;
}
}  // namespace tools
}  // namespace oceanbase
