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

namespace oceanbase
{
namespace tools
{

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
  int do_dump_(ObAdminDumpIterator &iter, const char *block_namt);
  int parse_single_group_entry_(const palf::LogGroupEntry &entry,
                                const char *block_name,
                                palf::LSN lsn);
  int parse_single_log_entry_(const palf::LogEntry &entry,
                              const char *block_name,
                              palf::LSN lsn);

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
}
}
#endif
