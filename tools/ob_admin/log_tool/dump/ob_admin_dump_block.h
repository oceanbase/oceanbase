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

#ifndef OB_ADMIN_DUMP_BLOCK_H
#define OB_ADMIN_DUMP_BLOCK_H
#include "logservice/palf/log_iterator_impl.h"
#include "logservice/palf/log_reader.h"
#include "logservice/palf/palf_iterator.h"
#include "../ob_admin_log_tool_executor.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif

namespace oceanbase
{
namespace tools
{
class ObGetFileSize
{
public :
  ObGetFileSize(palf::LSN end_lsn)
      : end_lsn_(end_lsn) {}
  palf::LSN operator()() const {return end_lsn_;}
private :
  palf::LSN end_lsn_;
};

class ObAdminDumpBlockHelper {
public:
  int mmap_log_file(char *&buf_out,
                    const int64_t buf_len,
                    const char *path,
                    int &fd_out);
  void unmap_log_file(char *buf_in,
                      const int64_t buf_len,
                      const int64_t fd);

  int get_file_meta(const char *path,
                    palf::LSN &start_lsn,
                    int64_t &header_size,
                    int64_t &body_size);
private:
  int parse_archive_header_(const char *buf_in,
                            const int64_t buf_len,
                            palf::LSN &start_lsn);
  int parse_palf_header_(const char *buf_in,
                         const int64_t buf_len,
                         palf::LSN &start_lsn);
};

class ObAdminDumpBlock
{
public:
  ObAdminDumpBlock(const char *block_path,
                   share::ObAdminMutatorStringArg &str_arg);
  int dump();

private:
  typedef palf::MemPalfGroupBufferIterator ObAdminDumpIterator;

private:
  int dump_();
  int do_dump_(ObAdminDumpIterator &iter, const char *block_namt);
  int parse_single_group_entry_(const palf::LogGroupEntry &entry,
                                const char *block_name,
                                palf::LSN lsn,
                                bool &has_encount_error);
  int parse_single_log_entry_(const palf::LogEntry &entry,
                              const char *block_name,
                              palf::LSN lsn);

#ifdef OB_BUILD_LOG_STORAGE_COMPRESS
  int decompress_();
#endif

private:
  const char *block_path_;
  share::ObAdminMutatorStringArg str_arg_;
};

class ObAdminDumpMetaBlock
{
public:
  ObAdminDumpMetaBlock(const char *block_path, share::ObAdminMutatorStringArg &str_arg);
  int dump();
private:
  typedef palf::MemPalfMetaBufferIterator ObAdminDumpIterator;
private:
  int do_dump_(ObAdminDumpIterator &iter,
               const char *block_name);
private:
  const char *block_path_;
  share::ObAdminMutatorStringArg str_arg_;
};

#ifdef OB_BUILD_SHARED_LOG_SERVICE
class ObAdminLogServiceDumpBlock
{
public:
  ObAdminLogServiceDumpBlock(const char *block_path,
                             share::ObAdminMutatorStringArg &str_arg);
  int dump();

private:
  int mmap_log_file_(char *&mmap_buf,
                     const char *block_path_,
                     int &fd,
                     int64_t &body_size);
  int do_dump_(const libpalf::LibPalfGroupEntryIteratorFFI *iter, const char *block_namt);
  int parse_single_group_entry_(libpalf::LibPalfLogGroupEntry &group_entry,
                                bool &has_encount_error,
                                const char *block_name,
                                const palf::LSN &lsn);
  int extract_log_entries_from_group_entry_(const libpalf::LibPalfLogEntryIteratorFFI *log_iter,
                                            share::SCN &log_upper_bound_scn,
                                            uint64_t next_min_scn_val,
                                            bool iterate_end_by_upper_bound,
                                            bool &has_encount_error,
                                            const char *block_name,
                                            const palf::LSN &lsn);
  int parse_single_log_entry_(const libpalf::LibPalfLogEntry &log_entry,
                              const char *block_name,
                              const palf::LSN &lsn);

private:
  const char *block_path_;
  share::ObAdminMutatorStringArg str_arg_;
};

class LibPalfLogGroupEntryToString
{
public:
  LibPalfLogGroupEntryToString(libpalf::LibPalfLogGroupEntryHeader &header);

  TO_STRING_KV("magic", header.magic,
               "version", header.version,
               "group_size", header.data_size,
               "proposal_id", header.proposal_id,
               "committed_lsn", header.committed_end_lsn,
               "max_scn", header.max_scn,
               "accumulated_checksum", header.acc_crc,
               "log_id", header.log_id,
               "flag", header.flag);
  libpalf::LibPalfLogGroupEntryHeader &header;
};

class LibPalfLogEntryToString
{
public:
  LibPalfLogEntryToString(libpalf::LibPalfLogEntryHeader &header);

  TO_STRING_KV("magic", header.magic,
               "version", header.version,
               "log_size", header.data_size,
               "scn_", header.scn,
               "data_checksum", header.data_crc,
               "flag", header.flag);
  libpalf::LibPalfLogEntryHeader &header;
};
#endif
}
}
#endif
