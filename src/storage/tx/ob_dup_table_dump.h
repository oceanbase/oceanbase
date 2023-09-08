// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_DUMP_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_DUMP_H

#include "src/share/ob_admin_dump_helper.h"
#include "storage/tx/ob_dup_table_lease.h"
#include "storage/tx/ob_dup_table_tablets.h"

namespace oceanbase
{
namespace transaction
{

class ObDupTableLogDumpIterator
{
public:
  ObDupTableLogDumpIterator()
      : str_arg_(nullptr), iter_buf_(nullptr), iter_buf_len_(0), dup_tablet_map_(1),
        dup_table_lease_map_()
  {}
  void reset();

  bool is_inited();
  int init_with_log_buf(const char *buf,
                        const int64_t buf_len,
                        share::ObAdminMutatorStringArg *str_arg_ptr);

  int dump_dup_table_log();
  int64_t get_iter_buf_pos() { return iter_buf_pos_; }

  TO_STRING_KV(KP(str_arg_),
               KP(iter_buf_),
               K(iter_buf_len_),
               K(dup_tablet_map_.size()),
               K(dup_table_lease_map_.size()));

private:
  int iter_stat_log_(const int64_t deser_buf_len, int64_t &deser_pos);
  int iter_tablet_log_(const int64_t deser_buf_len, int64_t &deser_pos);
  int iter_lease_log_(const int64_t deser_buf_len, int64_t &deser_pos);

private:
  share::ObAdminMutatorStringArg *str_arg_;

  const char *iter_buf_;
  int64_t iter_buf_len_;
  int64_t iter_buf_pos_;

  ObTxBigSegmentBuf big_segment_;

  DupTabletChangeMap dup_tablet_map_;
  DupTableLeaderLeaseMap dup_table_lease_map_;
  DupTableStatLog stat_log_;
};

} // namespace transaction

} // namespace oceanbase

#endif
