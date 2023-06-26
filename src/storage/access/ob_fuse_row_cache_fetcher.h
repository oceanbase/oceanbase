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

#include "storage/blocksstable/ob_fuse_row_cache.h"
#include "ob_table_access_param.h"
#include "ob_table_access_context.h"

namespace oceanbase
{
namespace storage
{

class ObTableAccessContext;

class ObFuseRowCacheFetcher final
{
public:
  ObFuseRowCacheFetcher();
  ~ObFuseRowCacheFetcher() = default;
  int init(const ObTabletID &tablet_id, const ObITableReadInfo *read_info, const int64_t tablet_version);
  int get_fuse_row_cache(const blocksstable::ObDatumRowkey &rowkey, blocksstable::ObFuseRowValueHandle &handle);
  int put_fuse_row_cache(const blocksstable::ObDatumRowkey &rowkey, blocksstable::ObDatumRow &row, const int64_t read_snapshot_version);
private:
  bool is_inited_;
  ObTabletID tablet_id_;
  const ObITableReadInfo *read_info_;
  int64_t tablet_version_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_FUSE_ROW_CACHE_FETCHER_H_
