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
#include "logservice/palf/log_meta.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_common_define.h"
#endif
#define private public
#include "logservice/archiveservice/ob_archive_define.h"
#undef private
#include "../parser/ob_admin_parser_log_entry.h"
#include "../parser/ob_admin_parser_group_entry.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
#include "logservice/ob_log_compression.h"
#endif
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
using namespace libpalf;
#endif
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
  if (LogFormatFlag::DECOMPRESS_FORMAT == str_arg_.flag_) {
#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
    ret = decompress_();
#endif
  } else {
    ret = dump_();
  }
  return ret;
}

#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
int ObAdminDumpBlock::decompress_()
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  char *block_name = basename((char*)block_path_);
  const char *path= block_path_;
  struct stat stat_buf;
  int64_t log_len = 0;
  int fd_out = 0;
  char *buf_out = NULL;
  if (-1 == (fd_out = ::open(path, O_RDONLY))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("open file fail", K(path), KERRMSG, K(ret));
  } else if (-1 == fstat(fd_out, &stat_buf)) {
    ret = OB_IO_ERROR;
    CLOG_LOG(ERROR, "stat_buf error", K(path), KERRMSG, K(ret));
  } else if (MAP_FAILED == (buf = ::mmap(NULL, stat_buf.st_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd_out, 0)) || NULL == buf) {
    ret = palf::convert_sys_errno();
    LOG_WARN("failed to mmap file", K(path), K(errno), KERRMSG, K(ret), K(fd_out));
  } else {
    buf_out = static_cast<char *>(buf);
    log_len = stat_buf.st_size;
    LOG_INFO("map_log_file success", K(path), KP(buf), K(fd_out), KP(buf_out), K(log_len));
  }

  if (OB_SUCC(ret)) {
    int64_t total_len_if_not_compressed = 0;// used to compute compression radio
    int64_t total_compressed_len = 0;
    int64_t total_decompressed_len = 0;
    int64_t log_cnt = 0;
    int64_t compressed_log_cnt = 0;
    MemPalfBufferIterator iter;
    ObAdminDumpBlockHelper helper;
    LSN start_lsn(0);
    MemoryStorage mem_storage;
    const LSN end_lsn = start_lsn + log_len;
    ObGetFileSize get_file_size(end_lsn);

    if (OB_FAIL(mem_storage.init(start_lsn))) {
      LOG_WARN("MemoryIteratorStorage init failed", K(ret), K(block_path_));
    } else if (OB_FAIL(mem_storage.append(buf_out, log_len))) {
      LOG_WARN("MemoryStorage append failed", K(ret));
    } else if (OB_FAIL(iter.init(start_lsn, get_file_size, &mem_storage))) {
      LOG_WARN("LogIteratorImpl init failed", K(ret), K(start_lsn), K(end_lsn));
    } else {
      LSN cur_lsn;
      SCN cur_scn;
      bool is_raw_write = false;
      const int64_t start_ts = ObTimeUtility::fast_current_time();
      int64_t before_get_entry_ts = start_ts;
      int64_t after_get_entry_ts = 0;
      int64_t after_decompress_ts = 0;
      int64_t iter_log_used = 0;
      int64_t decompress_log_used = 0;
      LOG_INFO("[START]uncompress begin");
      while (OB_SUCC(ret) && OB_SUCC(iter.next())) {
        const char *buf = NULL;
        int64_t buf_len = 0;
        if (OB_FAIL(iter.get_entry(buf, buf_len, cur_scn, cur_lsn, is_raw_write))) {
          if (OB_ITER_END != ret) {
            LOG_ERROR("ObAdminDumpIterator get_entry failed", K(iter));
          } else {
            LOG_INFO("end iterator all logs");
          }
        } else {
          logservice::ObLogBaseHeader log_base_header;
          int64_t pos = 0;
          if (OB_FAIL(log_base_header.deserialize(buf, buf_len, pos))) {
            LOG_ERROR("deserialize log_base_header failed", K(buf), K(buf_len), K(pos));
          } else {
            after_get_entry_ts = ObTimeUtility::fast_current_time();
            iter_log_used += (after_get_entry_ts - before_get_entry_ts);
            char *decompression_buf = NULL;
            int64_t decompressed_len = 0;
            int64_t final_decompressed_len = 0;
            const int64_t is_compressed = log_base_header.is_compressed();
            if (is_compressed) {
              if (OB_FAIL(logservice::decompress(buf + pos, buf_len - pos, str_arg_.decompress_buf_,
                      str_arg_.decompress_buf_len_, final_decompressed_len))) {
                LOG_ERROR("failed to decompress", K(pos), K(log_base_header));
              } else {
                compressed_log_cnt++;
              }
            }
            after_decompress_ts = ObTimeUtility::fast_current_time();
            decompress_log_used += (after_decompress_ts - after_get_entry_ts);
            total_len_if_not_compressed += (is_compressed ? final_decompressed_len : (buf_len - pos));
            total_compressed_len += buf_len - pos;
            total_decompressed_len += (is_compressed ? (buf_len - pos) : 0);
            log_cnt++;
            before_get_entry_ts = after_decompress_ts;
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_FAIL(ret)) {
        CLOG_LOG(ERROR, "uncompressed failed", K(total_len_if_not_compressed), K(total_compressed_len), K(total_decompressed_len));
      } else {
        const int64_t end_ts = ObTimeUtility::fast_current_time();
        double time_used = (end_ts - start_ts) * 1.0 / 1000 / 1000;
        CLOG_LOG(INFO, "\nDECOMPRESS DONE", K(time_used),
            "\ntotal_len_if_not_compressed(M):", total_len_if_not_compressed / 1024 / 1024,
            "\ntotal_compressed_len(M)", total_compressed_len / 1024 / 1024,
            "\naverage_log_size_origin(B)", total_len_if_not_compressed / log_cnt,
            "\naverage_log_size_compressed(B)", total_compressed_len / log_cnt,
            "\ncompress_radio", total_compressed_len * 1.0 / total_len_if_not_compressed,
            "\niter_speed(M/S)", (log_len * 1.0 / 1024 / 1024) / (iter_log_used *1.0/1000/1000),
            "\ndecompress_speed(M/S)", (log_len * 1.0 / 1024 / 1024) / (decompress_log_used*1.0/1000/1000),
            "\nlog_count", log_cnt,
            "\ncompressed_log_cnt", compressed_log_cnt,
            "\ndecompress_speed(M/S)", (total_decompressed_len * 1.0 / 1024 / 1024)/(decompress_log_used*1.0/1000/1000));
      }
    }
  }

  if (NULL != buf && -1 != fd_out) {
    ::munmap(reinterpret_cast<void*>(buf), log_len);
    ::close(fd_out);
  }

  return ret;
}
#endif

int ObAdminDumpBlock::dump_()
{
  int ret = OB_SUCCESS;
  char *block_name = basename((char*)block_path_);
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
    if (OB_FAIL(mem_storage.init(start_lsn))) {
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
  bool has_encount_error = false;
  while (OB_SUCC(iter.next())) {
    if (OB_FAIL(iter.get_entry(entry, lsn))) {
      LOG_ERROR("ObAdminDumpIterator get_entry failed", K(ret), K(entry), K(lsn), K(iter));
      has_encount_error = true;
    } else if (true == entry.get_header().is_padding_log()) {
      LOG_INFO("is_padding_log, no need parse", K(entry), K(iter));
    } else {
      str_arg_.log_stat_->total_group_entry_count_++;
      str_arg_.log_stat_->group_entry_header_size_ += entry.get_header_size();
      const int64_t total_size = entry.get_serialize_size();
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT
          && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
        ObCStringHelper helper;
        fprintf(stdout, "BlockID:%s, LSN:%s, SIZE:%ld, GROUP_ENTRY:%s\n",
                block_name, helper.convert(lsn), total_size, helper.convert(entry));
      }
      if (OB_FAIL(parse_single_group_entry_(entry, block_name, lsn, has_encount_error))) {
        LOG_ERROR("parser_single_group_entry_ failed", K(ret), K(entry));
        has_encount_error = true;
      }
    }
  }
  if (OB_INVALID_DATA == ret) {
    if (false == iter.check_is_the_last_entry()) {
      LOG_ERROR("data has been corrupted!!!", K(ret), K(iter));
      has_encount_error = true;
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (has_encount_error) {
    fprintf(stderr, "dumping block(%s) encounts error. Retrieve detailed error information from the ob_admin.log.\n", block_name);
  }
  return ret;
}

int ObAdminDumpBlock::parse_single_group_entry_(const LogGroupEntry &group_entry,
                                                const char *block_name,
                                                LSN lsn,
                                                bool &has_encount_error)
{
  int ret = OB_SUCCESS;
  LSN curr_lsn = lsn;
  ObAdminParserGroupEntry parser_ge(group_entry.get_data_buf(), group_entry.get_data_len(), str_arg_);
  has_encount_error = false;
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
        logservice::ObLogBaseHeader header;
        int64_t pos = 0;
        if (OB_FAIL(header.deserialize(entry.get_data_buf(), entry.get_header().get_data_len(), pos))) {
          LOG_WARN("deserialize BaseHeader failed", K(entry));
        } else {
          ObCStringHelper helper;
          fprintf(stdout, "LSN:%s, LOG_ENTRY:%s BaseHeader:%s",
                  helper.convert(curr_lsn), helper.convert(entry), helper.convert(header));
        }
      }
      str_arg_.log_stat_->log_entry_header_size_ += entry.get_header_size();
      str_arg_.log_stat_->total_log_entry_count_++;
      curr_lsn = curr_lsn + entry.get_serialize_size();
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = parse_single_log_entry_(entry, block_name, curr_lsn))) {
        if (OB_ITER_END != tmp_ret) {
          LOG_ERROR("parse_single_log_entry_ failed", K(tmp_ret), K(entry));
          has_encount_error = true;
        }
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
  ObAdminParserLogEntry parser_le(&entry, block_name, lsn, str_arg_);
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
    if (OB_FAIL(mem_storage.init(start_lsn))) {
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
        ObCStringHelper helper;
        fprintf(stdout, "BlockID:%s, LSN:%s, SIZE:%ld, META_ENTRY:%s\n",
                block_name, helper.convert(lsn), total_size, helper.convert(log_meta));
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

#ifdef OB_BUILD_SHARED_LOG_SERVICE
ObAdminLogServiceDumpBlock::ObAdminLogServiceDumpBlock(const char *block_path,
                                                       share::ObAdminMutatorStringArg &str_arg)
    : block_path_(block_path)
{
  str_arg_ = str_arg;
}

int ObAdminLogServiceDumpBlock::mmap_log_file_(char *&mmap_buf,
                                              const char *block_path_,
                                              int &fd,
                                              int64_t &body_size)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  struct stat block_stat;
  int64_t header_size = 4 * 1024;
  if (-1 == (fd = ::open(block_path_, O_RDONLY))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("open file fail", K(block_path_), K(ret));
  } else if (-1 == (::fstat(fd, &block_stat))) {
    ret = palf::convert_sys_errno();
    LOG_WARN("fstat file fail", K(block_path_), K(ret));
  } else if (FALSE_IT(body_size = block_stat.st_size - header_size)){
  } else if (MAP_FAILED == (buf = ::mmap(NULL, block_stat.st_size, PROT_READ, MAP_PRIVATE, fd, 0)) || NULL == buf) {
    ret = palf::convert_sys_errno();
    LOG_WARN("failed to mmap file", K(block_stat.st_size), K(block_path_), KERRMSG, K(ret), K(fd));
  } else {
    mmap_buf = static_cast<char *>(buf);
    LOG_INFO("map_log_file success", K(block_path_), KP(buf), K(fd), KP(mmap_buf));
  }
  return ret;
}

int ObAdminLogServiceDumpBlock::dump()
{
  int ret = OB_SUCCESS;
  char *block_name = basename((char*)block_path_);
  if (NULL == block_name) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid block_path", K(block_path_), KP(block_name), K(ret));
    return ret;
  }
  const LibPalfGroupEntryIteratorFFI *iter = NULL;
  int64_t header_size = 4 * 1024;
  int64_t body_size = 0;
  LSN start_lsn(0);
  size_t pos = 0;
  struct stat block_stat;
  int fd = -1;
  const LibPalfIteratorMemoryStorageFFI *memory_storage = NULL;
  char *mmap_buf = NULL;
  char *data_buf = NULL;

  if (OB_FAIL(mmap_log_file_(mmap_buf, block_path_, fd, body_size))) {
    LOG_WARN("mmap_log_file_ failed", K(ret), K(block_path_), K(fd), K(body_size));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_block_header_get_min_lsn(mmap_buf, &pos, &start_lsn.val_)))) {
    LOG_WARN("failed to get started lsn", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_seek_mem_group_log_iterator(start_lsn.val_, &iter, &memory_storage)))) {
    LOG_WARN("MemoryGroupIteratorStorage seek failed", K(ret), K(block_path_));
  } else if (FALSE_IT(data_buf = mmap_buf + header_size)) {
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_memory_storage_buffer(memory_storage, start_lsn.val_, data_buf, body_size)))) {
    LOG_WARN("MemoryGroupIteratorStorage set failed", K(ret), K(start_lsn), KP(data_buf), K(body_size));
  } else if (OB_FAIL(do_dump_(iter, block_name))) {
    LOG_WARN("ObAdminDumpIterator do_dump_ failed", K(ret), K(iter), KP(block_name));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_free_group_log_iterator(iter, memory_storage)))) {
    LOG_WARN("libpalf_free_group_log_iterator failed", K(ret));
  } else {
    LOG_INFO("ObAdminLogServiceDumpBlock dump success", K(ret), K(block_path_), K(start_lsn), K(header_size), K(body_size));
  }
  if (NULL != mmap_buf && -1 != fd) {
    ::munmap(reinterpret_cast<void*>(mmap_buf), header_size + body_size);
    ::close(fd);
  }
  return ret;
}

int ObAdminLogServiceDumpBlock::do_dump_(const LibPalfGroupEntryIteratorFFI *iter,
                                         const char *block_name)
{
  int ret = OB_SUCCESS;
  LSN lsn(0);
  bool has_encount_error = false;
  const int64_t PADDING_TYPE_MASK = 1ll << 63;
  share::SCN upper_bound_scn = SCN::max_scn();
  while (OB_SUCC(LIBPALF_ERRNO_CAST(libpalf_group_iterator_next(iter, upper_bound_scn.get_val_for_logservice())))) {
    // ((magic, version, data_size, proposal_id, committed_end_lsn, max_scn, acc_crc, log_id, flag), *body)
    LibPalfLogGroupEntry group_entry = LibPalfLogGroupEntry(LibPalfLogGroupEntryHeader(0/*magic*/,
        0/*version*/, 0/*data_size*/, 0/*proposal_id*/, 0/*committed_end_lsn*/, 0/*max_scn*/, 0/*acc_crc*/, 0/*log_id*/, 0/*flag*/), NULL/*body*/);
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_group_iterator_get_entry(iter, &lsn.val_, &group_entry)))) {
      LOG_ERROR("LibPalfGroupEntryIterator get_group_entry failed", K(ret), K(lsn), K(iter));
      has_encount_error = true;
    } else if ((group_entry.header.flag & PADDING_TYPE_MASK)!= 0) {
      LOG_INFO("is_padding_log, no need parse", K(iter));
    } else {
      LibPalfLogGroupEntryHeader &header = group_entry.header;
      str_arg_.log_stat_->total_group_entry_count_++;
      str_arg_.log_stat_->group_entry_header_size_ += sizeof(LibPalfLogGroupEntryHeader);
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT) {
        ObCStringHelper helper;
        LibPalfLogGroupEntryToString group_entry_to_string(header);
        int64_t total_size = header.data_size + sizeof(LibPalfLogGroupEntryHeader);
        fprintf(stdout, "BlockID:%s, LSN:%s, SIZE:%ld, GROUP_ENTRY:%s\n",
                block_name, helper.convert(lsn), total_size, helper.convert(group_entry_to_string));
      }
      if (OB_FAIL(LIBPALF_ERRNO_CAST(parse_single_group_entry_(group_entry, has_encount_error, block_name, lsn)))) {
        LOG_WARN("parse_single_group_entry_ failed", K(ret), K(block_name), K(lsn));
        has_encount_error = true;
      } else {
        LOG_INFO("ObAdminDumpIterator get_group_entry success",
            K(lsn), K(header.log_id), K(header.data_size), K(header.flag), K(header.magic));
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (has_encount_error) {
    fprintf(stderr, "dumping block(%s) encounts error. Retrieve detailed error information from the ob_admin.log.\n", block_name);
  }
  return ret;
}

int ObAdminLogServiceDumpBlock::parse_single_group_entry_(LibPalfLogGroupEntry &group_entry,
                                                          bool &has_encount_error,
                                                          const char *block_name,
                                                          const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  const LibPalfLogEntryIteratorFFI *log_iter = NULL;
  const LibPalfIteratorMemoryStorageFFI *memory_storage = NULL;
  share::SCN log_upper_bound_scn = SCN::max_scn();
  uint64_t next_min_scn_val = 0;
  bool iterate_end_by_upper_bound = false;
  size_t group_entry_body_size = 0;

  if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_seek_mem_log_iterator(lsn.val_ + sizeof(LibPalfLogGroupEntryHeader),
      &log_iter, &memory_storage)))) {
    LOG_WARN("MemoryLogIteratorStorage seek failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_group_log_entry_get_body_size(&group_entry.header, lsn.val_, &group_entry_body_size)))) {
    LOG_WARN("get group_entry_body_size failed", K(ret));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_set_memory_storage_buffer(memory_storage,
      lsn.val_ + sizeof(LibPalfLogGroupEntryHeader), group_entry.body, group_entry_body_size)))) {
    LOG_WARN("MemoryLogIteratorStorage seek failed", K(ret), K(group_entry_body_size), KP(group_entry.body));
  } else if (OB_FAIL(extract_log_entries_from_group_entry_(log_iter, log_upper_bound_scn, next_min_scn_val, iterate_end_by_upper_bound, has_encount_error, block_name, lsn))) {
    LOG_WARN("parse_single_group_entry_ failed", K(ret), K(log_upper_bound_scn), K(next_min_scn_val), K(iterate_end_by_upper_bound), K(has_encount_error));
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_free_log_iterator(log_iter, memory_storage)))) {
    LOG_WARN("libpalf_free_log_iterator failed", K(ret));
  } else {
    LOG_INFO("parse_single_group_entry_ success", K(ret), K(log_upper_bound_scn), K(next_min_scn_val), K(iterate_end_by_upper_bound), K(has_encount_error));
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObAdminLogServiceDumpBlock::extract_log_entries_from_group_entry_(const LibPalfLogEntryIteratorFFI *log_iter,
                                                                      share::SCN &log_upper_bound_scn,
                                                                      uint64_t next_min_scn_val,
                                                                      bool iterate_end_by_upper_bound,
                                                                      bool &has_encount_error,
                                                                      const char *block_name,
                                                                      const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(LIBPALF_ERRNO_CAST(libpalf_log_iterator_next(log_iter,
    log_upper_bound_scn.get_val_for_logservice(), &next_min_scn_val, &iterate_end_by_upper_bound)))) {
    LSN log_lsn(0);
    LibPalfLogEntry log_entry = LibPalfLogEntry(LibPalfLogEntryHeader(0/*magic*/,
        0/*version*/, 0/*data_size*/, 0/*scn*/, 0/*data_crc*/, 0/*flag*/), NULL/*data_buf*/);
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_log_iterator_get_entry(log_iter, &log_lsn.val_, &log_entry)))) {
      LOG_ERROR("LibPalfLogEntryIterator get_log_entry failed", K(ret), K(log_lsn), K(log_iter));
      has_encount_error = true;
    } else {
      str_arg_.log_stat_->total_log_entry_count_++;
      str_arg_.log_stat_->log_entry_header_size_ += sizeof(LibPalfLogEntryHeader);
      if (str_arg_.flag_ == LogFormatFlag::NO_FORMAT && str_arg_.flag_ != LogFormatFlag::STAT_FORMAT ) {
        logservice::ObLogBaseHeader header;
        int64_t pos = 0;
        if (OB_FAIL(header.deserialize(reinterpret_cast<const char*>(log_entry.data_buf), log_entry.header.data_size, pos))) {
          LOG_WARN("deserialize BaseHeader failed", KP(log_entry.data_buf), K(log_entry.header.data_size), K(pos));
        } else {
          ObCStringHelper helper;
          LibPalfLogEntryToString log_entry_to_string(log_entry.header);
          fprintf(stdout, "LSN:%s, LOG_ENTRY:%s BaseHeader:%s",
                  helper.convert(log_lsn), helper.convert(log_entry_to_string), helper.convert(header));
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = parse_single_log_entry_(log_entry, block_name, log_lsn))) {
        if (OB_ITER_END != tmp_ret) {
          LOG_ERROR("parse_single_log_entry_ failed", K(tmp_ret));
          has_encount_error = true;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObAdminLogServiceDumpBlock::parse_single_log_entry_(const LibPalfLogEntry &entry,
                                                        const char *block_name,
                                                        const palf::LSN &lsn)
{
  int ret = OB_SUCCESS;
  ObAdminParserLogEntry parser_le(&entry, block_name, lsn, str_arg_);
  if (OB_FAIL(parser_le.parse())) {
    LOG_WARN("ObAdminParserLogEntry failed", K(ret), K(block_name), K(lsn));
  } else {
    LOG_TRACE("parse_single_log_entry_ success", K(str_arg_));
  }
  return ret;
}

LibPalfLogGroupEntryToString::LibPalfLogGroupEntryToString(libpalf::LibPalfLogGroupEntryHeader &header)
  : header(header)
{}

LibPalfLogEntryToString::LibPalfLogEntryToString(libpalf::LibPalfLogEntryHeader &header)
   : header(header)
{}
#endif
}
}