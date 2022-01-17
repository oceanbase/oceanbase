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

#ifndef OCEANBASE_ARCHIVE_CLOG_READER_
#define OCEANBASE_ARCHIVE_CLOG_READER_

#include "ob_log_fetcher_impl.h"

namespace oceanbase {
namespace logservice {
class ObArchiveLogFetcher : public ObLogFetcherImpl {
public:
  ObArchiveLogFetcher()
  {}
  ~ObArchiveLogFetcher()
  {}
  int fetch_log(
      const common::ObPGKey& pg_key, const clog::ObReadParam& param, clog::ObReadBuf& rbuf, clog::ObReadRes& res);
};
}  // end of namespace logservice
}  // end of namespace oceanbase
#endif  // OCEANBASE_ARCHIVE_CLOG_READER_
