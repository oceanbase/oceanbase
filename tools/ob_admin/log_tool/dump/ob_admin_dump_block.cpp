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
#define private public
#include "logservice/archiveservice/ob_archive_define.h"
#undef private
#include "../parser/ob_admin_parser_log_entry.h"
#include "../parser/ob_admin_parser_group_entry.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;
namespace tools
{
int ObAdminDumpBlockHelper::mmap_log_file(char *&buf_out,
                                          const int64_t buf_len,
                                          const char *path,
                                          int &fd_out)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  struct stat stat_buf;
  if (-1 == (fd_out = ::open(path, O_RDONLY))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("open file fail", K(path), KERRMSG, K(ret));
  } else if (MAP_FAILED== (buf = ::mmap(NULL, buf_len, PROT_READ, MAP_PRIVATE, fd_out, 0)) || NULL == buf) {
    ret = palf::convert_sys_errno();
    LOG_WARN("failed to mmap file", K(buf_len), K(path), K(errno), KERRMSG, K(ret), K(fd_out));
  } else {
    buf_out = static_cast<char *>(buf);
    LOG_INFO("map_log_file success", K(path), KP(buf), K(fd_out), KP(buf_out));
  }
  return ret;
}

void ObAdminDumpBlockHelper::unmap_log_file(char *buf_in,
                                            const int64_t buf_len,
                                            const int64_t fd_in)
{
  if (NULL != buf_in && -1 != fd_in) {
    ::munmap(reinterpret_cast<void*>(buf_in), buf_len);
    ::close(fd_in);
  }
}

int ObAdminDumpBlockHelper::get_file_meta(const char *path,
                                          palf::LSN &start_lsn,
                                          int64_t &header_size,
                                          int64_t &body_size)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  ReadBufGuard read_buf_guard("ObAdmin", MAX_INFO_BLOCK_SIZE);
  char *read_buf = read_buf_guard.read_buf_.buf_;
  int64_t out_read_size = 0;
  int16_t magic_num = 0;
  int64_t pos = 0;
  struct stat block_stat;
  if (-1 == (fd = open(path, O_RDONLY))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("open file fail", K(path), KERRMSG, K(ret));
  } else if (-1 == (::fstat(fd, &block_stat))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("fstat file fail", K(path), KERRMSG, K(ret));
  } else if (MAX_INFO_BLOCK_SIZE != (out_read_size = ob_pread(fd, read_buf, MAX_INFO_BLOCK_SIZE, 0))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("read file fail", K(path), KERRMSG, K(ret));
  } else if (OB_FAIL(serialization::decode_i16(read_buf, MAX_INFO_BLOCK_SIZE, pos, &magic_num))) {
    LOG_WARN("serialization::decode_i16 failed", K(path), KERRMSG, K(ret));
  } else if (archive::ObArchiveFileHeader::ARCHIVE_FILE_HEADER_MAGIC == magic_num) {
    ret = parse_archive_header_(read_buf, MAX_INFO_BLOCK_SIZE, start_lsn);
  } else if (palf::LogBlockHeader::MAGIC == magic_num) {
    ret = parse_palf_header_(read_buf, MAX_INFO_BLOCK_SIZE, start_lsn);
  } else {
    LOG_ERROR("Invalid block header", K(path), K(magic_num), K(read_buf));
  }
  if (OB_SUCC(ret)) {
    header_size = MAX_INFO_BLOCK_SIZE;
    body_size = block_stat.st_size - MAX_INFO_BLOCK_SIZE;
    LOG_INFO("get_file_meta success", K(path), K(start_lsn), K(header_size), K(body_size));
  }
  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}

int ObAdminDumpBlockHelper::parse_archive_header_(const char *buf_in,
                                                  const int64_t buf_len,
                                                  palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  archive::ObArchiveFileHeader header;
  int64_t pos = 0;
  if (OB_FAIL(header.deserialize(buf_in, buf_len, pos))) {
    LOG_WARN("deserialize ObArchiveFileHeader failed", K(pos), K(buf_len));
  } else if (false == header.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("ObArchiveFileHeader data invalid", K(header));
  } else {
    start_lsn = LSN(header.start_lsn_);
    LOG_INFO("parse_archive_header_ success", K(header));
  }
  return ret;
}

int ObAdminDumpBlockHelper::parse_palf_header_(const char *buf_in,
                                                const int64_t buf_len,
                                                palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  palf::LogBlockHeader header;
  int64_t pos = 0;
  if (OB_FAIL(header.deserialize(buf_in, buf_len, pos))) {
    LOG_WARN("deserialize LogBlockHeader failed", K(pos), K(buf_len));
  } else if (false == header.check_integrity()) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("LogBlockHeader data invalid", K(header));
  } else {
    start_lsn = header.get_min_lsn();
    LOG_INFO("parse_palf_header_ success", K(header));
  }
  return ret;
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
  char *block_name  = basename((char*)block_path_);
  if (NULL == block_name) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid block_path", K(block_path_), KP(block_name));
    return ret;
  }
  ObAdminDumpIterator iter;
  ObAdminDumpBlockHelper helper;
  int64_t header_size, body_size;
  LSN start_lsn;
  if (OB_FAIL(helper.get_file_meta(block_path_, start_lsn, header_size, body_size))) {
    LOG_WARN("ObAdminDumpBlockHelper get_file_meta failed", K(block_path_));
  } else {
    MemoryStorage mem_storage;
    char *mmap_buf = NULL;
    char *data_buf = NULL;
    int fd_out = -1;
    const LSN end_lsn = start_lsn + body_size;
    auto get_file_size = [&]() -> LSN { return end_lsn; };
    if (mem_storage.init(start_lsn)) {
      LOG_WARN("MemoryIteratorStorage init failed", K(ret), K(block_path_));
    } else if (OB_FAIL(helper.mmap_log_file(mmap_buf, header_size + body_size, block_path_, fd_out))) {
      LOG_WARN("mmap_log_file_ failed", K(ret));
    } else if (FALSE_IT(data_buf = mmap_buf + header_size)) {
    } else if (OB_FAIL(mem_storage.append(data_buf, body_size))) {
      LOG_WARN("MemoryStorage append failed", K(ret));
    } else if (OB_FAIL(iter.init(start_lsn, get_file_size, &mem_storage))) {
      LOG_WARN("LogIteratorImpl init failed", K(ret), K(start_lsn), K(end_lsn));
    } else if (OB_FAIL(do_dump_(iter, block_name))) {
      LOG_WARN("ObAdminDumpIterator do_dump_ failed", K(ret), K(iter));
    } else {
      LOG_INFO("ObAdminDumpBlock dump success", K(ret), K(block_path_), K(start_lsn), K(header_size),
               K(body_size));
    }
    helper.unmap_log_file(mmap_buf, header_size + body_size, fd_out);
  }
  return ret;
}

int ObAdminDumpBlock::do_dump_(ObAdminDumpIterator &iter,
                               const char *block_name)
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
        fprintf(stdout, "BlockID:%s, LSN:%s, SIZE:%ld, GROUP_ENTRY:%s\n", block_name, to_cstring(lsn), total_size,
                to_cstring(entry));
      }
      if (OB_FAIL(parse_single_group_entry_(entry, block_name, lsn))) {
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
                                                const char *block_name,
                                                LSN lsn)
{
  int ret = OB_SUCCESS;
  LSN curr_lsn = lsn;
  ObAdminParserGroupEntry parser_ge(group_entry.get_data_buf(), group_entry.get_data_len(), str_arg_);
  do {
    LogEntry entry;
    if (OB_FAIL(parser_ge.get_next_log_entry(entry))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("ObAdminParserGroupEntry get_next_log_entry failed",
                 K(ret), K(block_name), K(lsn), K(group_entry));
      }
    } else {
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT
          && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
        fprintf(stdout, "LSN:%s, LOG_ENTRY:%s", to_cstring(curr_lsn), to_cstring(entry));
      }
      str_arg_.log_stat_->log_entry_header_size_ += entry.get_header_size();
      str_arg_.log_stat_->total_log_entry_count_++;
      curr_lsn = curr_lsn + entry.get_serialize_size();
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = parse_single_log_entry_(entry, block_name, lsn))) {
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
                                              const char *block_name,
                                              palf::LSN lsn)
{
  int ret = OB_SUCCESS;
  ObAdminParserLogEntry parser_le(entry, block_name, lsn, str_arg_);
  if (OB_FAIL(parser_le.parse())) {
    LOG_WARN("ObAdminParserLogEntry failed", K(ret), K(entry), K(block_name), K(lsn));
  } else {
    LOG_TRACE("parse_single_log_entry_ success",K(entry), K(str_arg_));
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
  char *block_name  = basename((char*)block_path_);
  if (NULL == block_name) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid block_path", K(block_path_), KP(block_name));
    return ret;
  }
  ObAdminDumpIterator iter;
  MemoryStorage mem_storage;
  int64_t header_size, body_size;
  LSN start_lsn;
  ObAdminDumpBlockHelper helper;
  if (OB_FAIL(helper.get_file_meta(block_path_, start_lsn, header_size, body_size))) {
    LOG_WARN("ObAdminDumpBlockHelper init failed", K(ret), K(block_path_));
  } else {
    // The LSN (Log Sequence Number) used for metadata files does not have any real significance or use.
    // Therefore, just set start_lsn to 0
    start_lsn = LSN(PALF_INITIAL_LSN_VAL);
    const LSN end_lsn = start_lsn + body_size;
    auto get_file_size = [&]() -> LSN { return end_lsn; };
    char *mmap_buf = NULL;
    char *data_buf = NULL;
    int fd_out = -1;
    if (mem_storage.init(start_lsn)) {
      LOG_WARN("MemoryIteratorStorage init failed", K(ret), K(block_path_));
    } else if (OB_FAIL(helper.mmap_log_file(mmap_buf, header_size + body_size, block_path_, fd_out))) {
      LOG_WARN("mmap_log_file_ failed", K(ret));
    } else if (FALSE_IT(data_buf = mmap_buf + header_size)) {
    } else if (OB_FAIL(mem_storage.append(data_buf, body_size))) {
      LOG_WARN("MemoryStorage append failed", K(ret));
    } else if (OB_FAIL(iter.init(start_lsn, get_file_size, &mem_storage))) {
      LOG_WARN("LogIteratorImpl init failed", K(ret), K(start_lsn), K(end_lsn));
    } else if (OB_FAIL(do_dump_(iter, block_name))) {
      LOG_WARN("ObAdminDumpIterator do_dump_ failed", K(ret), K(iter));
    } else {
      LOG_INFO("ObAdminDumpBlock dump success", K(ret), K(block_path_));
    }
    helper.unmap_log_file(mmap_buf, header_size + body_size, fd_out);
  }
  return ret;
}

int ObAdminDumpMetaBlock::do_dump_(ObAdminDumpIterator &iter,
                                   const char *block_name)
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
        fprintf(stdout, "BlockID:%s, LSN:%s, SIZE:%ld, META_ENTRY:%s\n", block_name, to_cstring(lsn), total_size,
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
