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
#include "ob_admin_dump_block.h"
#include <cstdio>
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_meta.h"
#include "../parser/ob_admin_parser_log_entry.h"
#include "../parser/ob_admin_parser_group_entry.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;
namespace tools
{
int mmap_log_file(char *&buf_out, const int64_t buf_len, const char *path, int &fd)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  struct stat stat_buf;
  if (-1 == (fd = open(path, O_RDONLY))) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file fail", K(path), KERRMSG, K(ret));
  } else if (MAP_FAILED == (buf = ::mmap(NULL, buf_len, PROT_READ, MAP_SHARED, fd, 0 + MAX_INFO_BLOCK_SIZE)) || NULL == buf) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to mmap file", K(path), K(errno), KERRMSG, K(ret));
  } else {
    buf_out = static_cast<char *>(buf);
    LOG_INFO("map_log_file success", K(path), K(buf), K(fd));
  }
  return ret;
}

void unmap_log_file(char *buf_in, const int64_t buf_len, const int64_t fd)
{
  if (NULL != buf_in && -1 != fd) {
    ::munmap(reinterpret_cast<void*>(buf_in), buf_len);
    ::close(fd);
  }
}

ObAdminDumpBlock::ObAdminDumpBlock(const char *block_path,
                                   share::ObAdminMutatorStringArg &str_arg)
    : block_path_(block_path)
{
  str_arg_ = str_arg;
}

int ObAdminDumpBlock::dump()
{
  int ret = OB_SUCCESS;
  ObAdminDumpIterator iter;
  const block_id_t block_id = static_cast<palf::block_id_t>(atoi(block_path_));
  const LSN start_lsn(block_id * PALF_BLOCK_SIZE);
  const LSN end_lsn((block_id + 1) * PALF_BLOCK_SIZE);
  MemoryStorage mem_storage;
  auto get_file_size = [&]() -> LSN { return end_lsn; };
  char *buf = NULL;
  int fd = -1;
  if (mem_storage.init(start_lsn)) {
    LOG_WARN("MemoryIteratorStorage init failed", K(ret), K(block_path_));
  } else if (OB_FAIL(mmap_log_file(buf, PALF_BLOCK_SIZE, block_path_, fd))) {
    LOG_WARN("mmap_log_file_ failed", K(ret));
  } else if (OB_FAIL(mem_storage.append(buf, PALF_BLOCK_SIZE))) {
    LOG_WARN("MemoryStorage append failed", K(ret));
  } else if (OB_FAIL(iter.init(start_lsn, get_file_size, &mem_storage))) {
    LOG_WARN("LogIteratorImpl init failed", K(ret), K(start_lsn), K(end_lsn));
  } else if (OB_FAIL(do_dump_(iter, block_id))) {
    LOG_WARN("ObAdminDumpIterator do_dump_ failed", K(ret), K(iter));
  } else {
    LOG_INFO("ObAdminDumpBlock dump success", K(ret), K(block_path_));
  }
  unmap_log_file(buf, PALF_BLOCK_SIZE, fd);
  return ret;
}

int ObAdminDumpBlock::do_dump_(ObAdminDumpIterator &iter,
                               block_id_t block_id)
{
  int ret = OB_SUCCESS;
  LogGroupEntry entry;
  LSN lsn;
  while (OB_SUCC(iter.next())) {
    if (OB_FAIL(iter.get_entry(entry, lsn))) {
      LOG_WARN("ObAdminDumpIterator get_entry failed", K(ret), K(entry), K(lsn), K(iter));
    } else if (true == entry.get_header().is_padding_log()) {
      LOG_INFO("is_padding_log, no need parse", K(entry), K(iter));
    } else {
      str_arg_.log_stat_->total_group_entry_count_++;
      str_arg_.log_stat_->group_entry_header_size_ += entry.get_header_size();
      const int64_t total_size = entry.get_serialize_size();
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT
          && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
        fprintf(stdout, "BlockID:%ld, LSN:%s, SIZE:%ld, GROUP_ENTRY:%s\n", block_id, to_cstring(lsn), total_size,
                to_cstring(entry));
      }
      if (OB_FAIL(parse_single_group_entry_(entry, block_id, lsn))) {
        LOG_WARN("parser_single_group_entry_ success", K(ret), K(entry));
      }
    }
  }
  if (OB_INVALID_DATA == ret) {
    if (false == iter.check_is_the_last_entry()) {
      LOG_ERROR("data has been corrupted!!!", K(ret), K(iter));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObAdminDumpBlock::parse_single_group_entry_(const LogGroupEntry &group_entry,
                                                block_id_t block_id,
                                                LSN lsn)
{
  int ret = OB_SUCCESS;
  ObAdminParserGroupEntry parser_ge(group_entry.get_data_buf(), group_entry.get_data_len(), str_arg_);
  do {
    LogEntry entry;
    if (OB_FAIL(parser_ge.get_next_log_entry(entry))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("ObAdminParserGroupEntry get_next_log_entry failed",
                 K(ret), K(block_id), K(lsn), K(group_entry));
      }
    } else {
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT
          && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
        fprintf(stdout, "LSN:%s, LOG_ENTRY:%s", to_cstring(lsn), to_cstring(entry));
      }
      str_arg_.log_stat_->log_entry_header_size_ += entry.get_header_size();
      str_arg_.log_stat_->total_log_entry_count_++;
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = parse_single_log_entry_(entry, block_id, lsn))) {
        LOG_WARN("parse_single_log_entry_ failed", K(tmp_ret), K(entry));
      }
    }
  } while (OB_SUCC(ret));

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObAdminDumpBlock::parse_single_log_entry_(const LogEntry &entry,
                                              block_id_t block_id,
                                              palf::LSN lsn)
{
  int ret = OB_SUCCESS;
  ObAdminParserLogEntry parser_le(entry, block_id, lsn, str_arg_);
  if (OB_FAIL(parser_le.parse())) {
    LOG_WARN("ObAdminParserLogEntry failed", K(ret), K(entry), K(block_id), K(lsn));
  } else {
    LOG_INFO("YYY parse_single_log_entry_ success",K(entry), K(str_arg_));
  }
  return ret;
}

ObAdminDumpMetaBlock::ObAdminDumpMetaBlock(const char *block_path,
                                           share::ObAdminMutatorStringArg &str_arg)
    : block_path_(block_path)
{
  str_arg_ = str_arg;
}

int ObAdminDumpMetaBlock::dump()
{
  int ret = OB_SUCCESS;
  ObAdminDumpIterator iter;
  const block_id_t block_id = static_cast<palf::block_id_t>(atoi(block_path_));
  const LSN start_lsn(block_id * PALF_BLOCK_SIZE);
  const LSN end_lsn((block_id + 1) * PALF_BLOCK_SIZE);
  MemoryStorage mem_storage;
  auto get_file_size = [&]() -> LSN { return end_lsn; };
  char *buf = NULL;
  int fd = -1;
  if (mem_storage.init(start_lsn)) {
    LOG_WARN("MemoryIteratorStorage init failed", K(ret), K(block_path_));
  } else if (OB_FAIL(mmap_log_file(buf, PALF_BLOCK_SIZE, block_path_, fd))) {
    LOG_WARN("mmap_log_file_ failed", K(ret));
  } else if (OB_FAIL(mem_storage.append(buf, PALF_BLOCK_SIZE))) {
    LOG_WARN("MemoryStorage append failed", K(ret));
  } else if (OB_FAIL(iter.init(start_lsn, get_file_size, &mem_storage))) {
    LOG_WARN("LogIteratorImpl init failed", K(ret), K(start_lsn), K(end_lsn));
  } else if (OB_FAIL(do_dump_(iter, block_id))) {
    LOG_WARN("ObAdminDumpIterator do_dump_ failed", K(ret), K(iter));
  } else {
    LOG_INFO("ObAdminDumpBlock dump success", K(ret), K(block_path_));
  }
  unmap_log_file(buf, PALF_BLOCK_SIZE, fd);
  return ret;
}

int ObAdminDumpMetaBlock::do_dump_(ObAdminDumpIterator &iter, palf::block_id_t block_id)
{
  int ret = OB_SUCCESS;
  LogMetaEntry entry;
  LSN lsn;
  while (OB_SUCC(iter.next())) {
    if (OB_FAIL(iter.get_entry(entry, lsn))) {
      LOG_WARN("ObAdminDumpIterator get_entry failed", K(ret), K(entry), K(lsn), K(iter));
    } else {
      const int64_t total_size = entry.get_serialize_size();
      LogMeta log_meta;
      int64_t pos = 0;
      if (OB_FAIL(log_meta.deserialize(entry.get_buf(), entry.get_data_len(), pos))) {
        LOG_WARN("deserialize log_meta failed", K(ret), K(iter));
      } else {
        fprintf(stdout, "BlockID:%ld, LSN:%s, SIZE:%ld, META_ENTRY:%s\n", block_id, to_cstring(lsn), total_size,
                to_cstring(log_meta));
      }
    }
  }
  if (OB_INVALID_DATA == ret) {
    if (false == iter.check_is_the_last_entry()) {
      LOG_ERROR("data has been corrupted!!!", K(ret), K(iter));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

}
}
