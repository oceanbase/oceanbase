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

#ifndef OCEANBASE_MEMBER_TABLE_OB_MEMBER_TABLE_ITERATOR
#define OCEANBASE_MEMBER_TABLE_OB_MEMBER_TABLE_ITERATOR

#include "share/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_active_resource_list.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/member_table/ob_member_table.h"


namespace oceanbase
{
namespace common
{
class ObISQLClient;

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace storage
{
class ObInnerTableReadCtx;
class ObMemberTableIterator final
{
public:
  ObMemberTableIterator();
  ~ObMemberTableIterator();
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObNewRange &new_range,
      const bool is_get,
      const int64_t abs_timeout_ts);
  int get_next_row(
      ObMemberTableData &member_table_data);
  int get_next_row(
      ObMemberTableData &member_table_data,
      share::SCN &trans_scn);
private:
  int prepare_scan_iter_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObNewRange &new_range,
      const bool is_get,
      const int64_t abs_timeout_ts);
  int get_local_read_scn_(
      const uint64_t tenant_id,
      const int64_t abs_timeout_ts,
      share::SCN &read_scn);
  int build_read_ctx_(
      const share::ObLSID &ls_id,
      const int64_t abs_timeout_us,
      const bool is_get,
      const common::ObNewRange &new_range,
      const share::SCN &snapshot);
  int build_scan_iter_();
  int inner_get_next_row_(
      ObMemberTableData &member_table_data,
      share::SCN &trans_scn,
      int64_t &sql_no);
  void free_scan_iter_(ObNewRowIterator *iter);

private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObInnerTableReadCtx *ctx_;
  ObTableScanIterator *scan_iter_;
};


} //storage
} //oceanbase

#endif
