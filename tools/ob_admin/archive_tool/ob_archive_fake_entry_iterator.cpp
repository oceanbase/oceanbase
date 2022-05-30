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

#include "ob_archive_fake_entry_iterator.h"

namespace oceanbase
{
using namespace clog;
namespace archive
{
ObArchiveFakeEntryIterator::~ObArchiveFakeEntryIterator()
{
  inited_ = false;
  rpc_proxy_ = nullptr;
}

int ObArchiveFakeEntryIterator::init(const uint64_t tenant_id,
    ObIArchiveLogFileStore *file_store,
    obrpc::ObSrvRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  common::ObPGKey fake_key(1, 0, 0);
  const uint64_t fake_file_id = 1;
  const uint64_t tmp_tenant_id = 0 == tenant_id ? 1 : tenant_id;
  if (OB_ISNULL(file_store) || NULL == rpc_proxy) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", K(ret), K(file_store), K(rpc_proxy));
  } else if (OB_FAIL(ObArchiveEntryIterator::init(file_store, fake_key, fake_file_id, 0, 1000, false, tmp_tenant_id))) {
    ARCHIVE_LOG(WARN, "init fail", K(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
  }

  return ret;
}


}
}
