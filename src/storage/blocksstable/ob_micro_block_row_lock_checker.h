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

#ifndef OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_
#define OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_

#include "share/scn.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase {
namespace blocksstable {

class ObMicroBlockRowLockChecker : public ObMicroBlockRowScanner {
public:
  ObMicroBlockRowLockChecker(common::ObIAllocator &allocator);
  virtual ~ObMicroBlockRowLockChecker();
  virtual int get_next_row(const ObDatumRow *&row) override;
  inline void set_lock_state(ObStoreRowLockState *lock_state)
  {
    lock_state_ = lock_state;
  }
  inline void set_row_state(ObRowState *row_state)
  {
    row_state_ = row_state;
  }
  inline void set_snapshot_version(const share::SCN &snapshot_version)
  {
    snapshot_version_ = snapshot_version;
  }
  inline void set_check_exist(bool check_eixst)
  {
    check_exist_ = check_eixst;
  }
protected:
  virtual int inner_get_next_row(
      bool &row_lock_checked,
      int64_t &current,
      ObStoreRowLockState *&lock_state);
  virtual int check_row(
      const bool row_lock_checked,
      const transaction::ObTransID &trans_id,
      const ObRowHeader *row_header,
      const ObStoreRowLockState &lock_state,
      bool &need_stop);
  virtual void check_row_in_major_sstable(bool &need_stop);
protected:
  bool check_exist_;
  share::SCN snapshot_version_;
  ObStoreRowLockState *lock_state_;
  ObRowState* row_state_;
  ObStoreRowLockState tmp_lock_state_;
};

class ObMicroBlockRowLockMultiChecker : public ObMicroBlockRowLockChecker {
public:
  ObMicroBlockRowLockMultiChecker(common::ObIAllocator &allocator);
  virtual ~ObMicroBlockRowLockMultiChecker();
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const int64_t rowkey_begin_idx,
      const int64_t rowkey_end_idx,
      ObRowsInfo &rows_info);
   void inc_empty_read();
protected:
  virtual int inner_get_next_row(
      bool &row_lock_checked,
      int64_t &current,
      ObStoreRowLockState *&lock_state);
  virtual int check_row(
      const bool row_lock_checked,
      const transaction::ObTransID &trans_id,
      const ObRowHeader *row_header,
      const ObStoreRowLockState &lock_state,
      bool &need_stop);
  virtual void check_row_in_major_sstable(bool &need_stop);
  int seek_forward();
private:
  bool reach_end_;
  int64_t rowkey_current_idx_;
  int64_t rowkey_begin_idx_;
  int64_t rowkey_end_idx_;
  int64_t empty_read_cnt_;
  ObRowsInfo *rows_info_;
};

}
}
#endif /* OB_MICRO_BLOCK_ROW_LOCK_CHECKER_H_ */
