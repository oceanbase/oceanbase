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

#ifndef OB_FUSE_ROW_CACHE_FETCHER_H_
#define OB_FUSE_ROW_CACHE_FETCHER_H_

#include "blocksstable/ob_fuse_row_cache.h"

namespace oceanbase {
namespace storage {

class ObFuseRowCacheFetcher final {
public:
  ObFuseRowCacheFetcher();
  ~ObFuseRowCacheFetcher() = default;
  int init(const ObTableAccessParam& access_param, ObTableAccessContext& access_ctx);
  int get_fuse_row_cache(const ObStoreRowkey& rowkey, blocksstable::ObFuseRowValueHandle& handle);

  int put_fuse_row_cache(const ObStoreRowkey& rowkey, const int64_t sstable_end_log_ts, ObStoreRow& row,
      blocksstable::ObFuseRowValueHandle& handle);

private:
  static const int64_t FUSE_ROW_CACHE_PUT_INTERVAL = 10 * 1000 * 1000;  // 10s
  bool is_inited_;
  const ObTableAccessParam* access_param_;
  ObTableAccessContext* access_ctx_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_FUSE_ROW_CACHE_FETCHER_H_
