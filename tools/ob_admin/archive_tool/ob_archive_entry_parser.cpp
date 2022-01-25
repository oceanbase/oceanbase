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

#include "ob_archive_entry_parser.h"
#include "archive/ob_log_archive_define.h"
#include "archive/ob_archive_entry_iterator.h"
#include "archive/ob_archive_block.h"
#include "ob_archive_fake_entry_iterator.h"
#include "ob_archive_fake_file_store.h"
#include <string.h>

namespace oceanbase
{
using namespace common;
using namespace memtable;
using namespace storage;
using namespace archive;
namespace archive
{
int ObArchiveEntryParser::init(uint64_t file_id,
                               const uint64_t tenant_id,
                               const common::ObString &host,
                               const int32_t port)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(OB_INVALID_FILE_ID == file_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid buf or buf_len", K(file_id), K(ret));
  } else {
    file_id_ = file_id;
    tenant_id_ = tenant_id;
    if (!host.empty() && port > 0) {
      if (OB_SUCCESS != client_.init()) {
        CLOG_LOG(WARN, "failed to init net client", K(ret));
      } else if (OB_SUCCESS != client_.get_proxy(rpc_proxy_)) {
        CLOG_LOG(WARN, "failed to get_proxy", K(ret));
      }
      host_addr_.set_ip_addr(host, port);
      rpc_proxy_.set_server(host_addr_);
    }
    is_inited_ = true;
  }
  return ret;
}

int ObArchiveEntryParser::dump_all_entry(const char *path)
{
  int ret = OB_SUCCESS;
  ObArchiveFakeFileStore file_store;
  ObArchiveFakeEntryIterator iter;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "log entry parser is not inited", K(is_inited_), K(ret));
  } else if (OB_FAIL(file_store.init_for_dump(path))) {
    CLOG_LOG(WARN, "init for dump fail", K(ret), K(path));
  } else if (OB_FAIL(iter.init(tenant_id_, &file_store, &rpc_proxy_))) {
    CLOG_LOG(WARN, "iter init fail", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      clog::ObLogEntry entry;
      const int64_t fake_pos = 1;
      bool unused_flag = false;
      int64_t unused_checksum = -1;
      if (OB_FAIL(iter.next_entry(entry, unused_flag, unused_checksum))) {
        CLOG_LOG(WARN, "next entry fail", K(ret));
      } else if (OB_FAIL(dump_clog_entry(entry, fake_pos))) {
        CLOG_LOG(WARN, "dump clog entry fail", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}//end of namespace clog
}//end of namespace oceanbase
