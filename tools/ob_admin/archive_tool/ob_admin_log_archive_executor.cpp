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

#include "ob_admin_log_archive_executor.h"
#include "../clog_tool/ob_func_utils.h"
#include "../clog_tool/ob_log_entry_filter.h"
#include "../clog_tool/ob_log_entry_parser.h"
#include "../clog_tool/cmd_args_parser.h"
#include "ob_fake_archive_log_file_store.h"
#include "archive/ob_archive_entry_iterator.h"
#include "clog/ob_log_entry.h"
#include "share/ob_version.h"
#include "archive/ob_log_archive_struct.h"
#include "ob_archive_entry_parser.h"

using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::archive;

namespace oceanbase
{
namespace tools
{

#define getcfg(key) getenv(key)


int ObAdminLogArchiveExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 4) || OB_ISNULL(argv)) {
    print_usage();
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(parse_options(argc, argv))) {
    LOG_WARN("failed to parse options", K(ret));
  } else {
    int new_argc = argc - optind;
    char **new_argv = argv + optind;
    if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_log):OB_NEED_RETRY)) {
      LOG_INFO("finish dump_log ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_index):OB_NEED_RETRY)) {
      LOG_INFO("finish dump_index ", K(ret));
    } else if (OB_NEED_RETRY != (ret = CmdCallSimple(new_argc, new_argv, dump_key):OB_NEED_RETRY)) {
      LOG_INFO("finish dump_key ", K(ret));
    } else {
      fprintf(stderr, "failed %d", ret);
      print_usage();
    }
  }
  return ret;
}

void ObAdminLogArchiveExecutor::print_usage()
{
  fprintf(stdout,
          "Usages:\n"
          "ob_admin archive_tool dump_log data_files ## ./ob_admin archive_tool dump_log 1 2 3\n"
          "ob_admin archive_tool dump_index index_files ## ./ob_admin archive_tool dump_index 1 2 3\n"
          "ob_admin archive_tool dump_key archive_key_files ## ./ob_admin archive_tool dump_key 1100611139403779_0\n"
         );
}

int ObAdminLogArchiveExecutor::dump_log(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump all archive log file", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_single_file(argv[i])) && OB_ITER_END != ret) {
        LOG_WARN("failed to dump log ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {/*do nothing*/}
    }
    if (OB_FAIL(ret)) {
      print_usage();
    }
  }
  return ret;
}

int ObAdminLogArchiveExecutor::dump_index(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump archive index file", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_single_index_file(argv[i])) && OB_ITER_END != ret) {
        LOG_WARN("failed to dump log ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAdminLogArchiveExecutor::dump_key(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(argc < 1) || OB_ISNULL(argv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(argc), K(ret));
  } else {
    LOG_INFO("begin to dump archive index file", K(ret));
    for (int64_t i = 0; i < argc; ++i) {
      if (OB_FAIL(dump_single_key_file(argv[i])) && OB_ITER_END != ret) {
        LOG_WARN("failed to dump log ", K(argv[i]), K(ret));
      } else if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObAdminLogArchiveExecutor::dump_single_file(const char *path)
{
  int ret = OB_SUCCESS;
  uint64_t file_id = 0;
  ObArchiveEntryParser entry_parser;
  if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parse file name", K(path), K(ret));
  } else if (OB_FAIL(entry_parser.init(file_id, tenant_id_, DB_host_, DB_port_))) {
    LOG_WARN("failed to init entry parser", K(path), K(ret));
  } else if (OB_FAIL(entry_parser.dump_all_entry(path))) {
    if (OB_ITER_END == ret) {
      LOG_INFO("succ to dump_all_entry", K(path));
    } else {
      LOG_WARN("failed to dump_all_entry", K(path), K(ret));
    }
  } else {/*do nothing*/}

  return ret;
}

int ObAdminLogArchiveExecutor::dump_single_index_file(const char *path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char *buf = NULL;
  int64_t buf_len = -1;
  uint64_t file_id = -1;

  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else if (OB_FAIL(file_name_parser(path, file_id))) {
    LOG_WARN("failed to parse file name", K(path), K(ret));
  } else {
    ObArchiveIndexFileInfo info;
    int64_t pos = 0;
    while (OB_SUCCESS == ret && pos < buf_len) {
      info.reset();
      if (OB_FAIL(info.deserialize(buf, buf_len, pos))) {
        LOG_WARN("deserialize info fail", K(ret), K(buf), K(buf_len));
      } else {
        fprintf(stdout, "$$$ %s\n", to_cstring(info));
      }
    }
  }
  if (fd >= 0) {
    close(fd);
  }
  return ret;
}

int ObAdminLogArchiveExecutor::dump_single_key_file(const char *path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char *buf = NULL;
  int64_t buf_len = -1;

  if (OB_FAIL(mmap_log_file(path, buf, buf_len, fd))) {
    LOG_WARN("failed to mmap_log_file", K(path), K(ret));
  } else {
    ObArchiveKeyContent key_content;
    int64_t pos = 0;
    if (OB_FAIL(key_content.deserialize(buf, buf_len, pos))) {
      LOG_WARN("deserialize info fail", K(ret), K(buf), K(buf_len));
    } else {
      fprintf(stdout, "$$$ %s, %s\n", path, to_cstring(key_content));
    }
  }

  if (fd >= 0) {
    close(fd);
  }
  return ret;
}

int ObAdminLogArchiveExecutor::mmap_log_file(const char *path, char *&buf_out, int64_t &buf_len, int &fd)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  struct stat stat_buf;
  const int64_t FILE_MAX_SIZE = 64 * 1024 * 1024;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log file path is NULL", K(ret));
  } else if (-1 == (fd = open(path, O_RDONLY))) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "open file fail", K(path), KERRMSG, K(ret));
  } else if (-1 == fstat(fd, &stat_buf)) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "stat_buf error", K(path), KERRMSG, K(ret));
  } else if (stat_buf.st_size > FILE_MAX_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid file size", K(path), K(stat_buf.st_size), K(ret));
  } else if (MAP_FAILED == (buf = mmap(NULL, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0)) || NULL == buf) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to mmap file", K(path), K(errno), KERRMSG, K(ret));
  } else {
    buf_out = static_cast<char *>(buf);
    buf_len = stat_buf.st_size;
  }
  return ret;
}

}//namespace tools
}//namespace oceanbase
