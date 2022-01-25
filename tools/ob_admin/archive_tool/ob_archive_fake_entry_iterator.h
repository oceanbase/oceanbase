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

#ifndef OCEANBASE_ARCHIVE_FAKE_ENTRY_ITERATOR_
#define OCEANBASE_ARCHIVE_FAKE_ENTRY_ITERATOR_

#include "archive/ob_archive_entry_iterator.h"
#include "../clog_tool/ob_log_entry_parser.h"


namespace oceanbase
{
namespace archive
{
class ObArchiveFakeEntryIterator : public ObArchiveEntryIterator
{
public:
  ObArchiveFakeEntryIterator(): inited_(false), rpc_proxy_(nullptr) {}
  virtual ~ObArchiveFakeEntryIterator();
  int init(const uint64_t tenant_id,
      ObIArchiveLogFileStore *file_store,
      obrpc::ObSrvRpcProxy *rpc_proxy);
private:
  bool    inited_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_FAKE_ENTRY_ITERATOR_ */
