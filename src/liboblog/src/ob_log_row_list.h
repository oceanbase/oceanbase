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

#ifndef OCEANBASE_LIBOBLOG_BR_LIST_H__
#define OCEANBASE_LIBOBLOG_BR_LIST_H__

#include "ob_log_row_data_index.h"                  // ObLogRowDataIndex

namespace oceanbase
{
namespace liboblog
{
// Reassembles the partition transaction based on the row data ObLogEntryTask, the class that holds all ObLogRowDataIndex
class SortedDmlRowList
{
public:
  SortedDmlRowList();
  ~SortedDmlRowList();

public:
  void reset();
  bool is_valid() const;
  // 1. Each ObLogEntryTask contains single/multiple br data and has been concatenated
  // 2. When it contains rollback, it needs to be handled
  int push(ObLogRowDataIndex *row_head,
      ObLogRowDataIndex *row_tail,
      const int64_t row_num,
      const bool is_contain_rollback_row);

  int64_t get_row_num() const { return row_num_; }
  ObLogRowDataIndex *get_head() { return head_; }
  ObLogRowDataIndex *get_tail() { return tail_; }

  TO_STRING_KV(K_(row_num),
      K_(head),
      K_(tail));

private:
  int push_when_not_contain_rollback_row_(ObLogRowDataIndex *row_head,
      ObLogRowDataIndex *row_tail,
      const int64_t row_num);
  int push_when_contain_rollback_row_(ObLogRowDataIndex *row_head,
      ObLogRowDataIndex *row_tail,
      const int64_t row_num);
  // is_single_row = false: insert a linklist to SortedDmlRowList
  // is_single_row = true: insert a row to SortedDmlRowList
  int push_(ObLogRowDataIndex *row_head,
      ObLogRowDataIndex *row_tail,
      const int64_t row_num,
      const bool is_single_row);
  // dml stmt is strictly incremented by sql_no, just find the first statement that is greater than sql_no and roll back that and subsequent statements
  int rollback_row_(const int32_t rollback_sql_no,
      ObLogRowDataIndex &rollback_row_data_index);

private:
  int64_t row_num_;
  ObLogRowDataIndex *head_;
  ObLogRowDataIndex *tail_;
};

} // namespace liboblog
} // namespace oceanbase

#endif
