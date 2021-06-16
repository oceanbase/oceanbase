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

#ifndef OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#define OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_
#include "mvcc/ob_mvcc_trans_ctx.h"
#include "ob_memtable_mutator.h"
#include "ob_memtable_interface.h"

namespace oceanbase {
namespace memtable {
class ObRedoLogGenerator {
public:
  ObRedoLogGenerator()
      : is_inited_(false),
        consume_cursor_(NULL),
        generate_cursor_(NULL),
        end_guard_(NULL),
        mem_ctx_(NULL),
        big_row_buf_(NULL),
        big_row_size_(0),
        generate_big_row_pos_(INT64_MAX),
        consume_big_row_pos_(INT64_MAX),
        generate_data_size_(0)
  {}
  ~ObRedoLogGenerator()
  {}
  void reset();
  int set(ObMvccRowCallback* start, ObMvccRowCallback* end, ObIMemtableCtx* mem_ctx);
  int fill_redo_log(char* buf, const int64_t buf_len, int64_t& buf_pos);
  int undo_fill_redo_log();
  ObMvccRowCallback* get_generate_cursor()
  {
    return generate_cursor_;
  }
  int set_if_necessary(ObMvccRowCallback* head, ObMemtable* mem);
  void sync_log_succ(const int64_t log_id, const int64_t log_ts, bool& has_pending_cb);
  int64_t get_generating_data_size()
  {
    return generate_data_size_;
  }

private:
  bool big_row_log_fully_filled() const;
  int generate_and_fill_big_row_redo(const int64_t big_row_size, RedoDataNode& redo, ObMvccRowCallback* callback,
      char* buf, const int64_t buf_len, int64_t& buf_pos);
  int fill_big_row_redo_(char* buf, const int64_t buf_len, int64_t& buf_pos, int64_t& data_size);
  int fill_row_redo(ObMemtableMutatorWriter& mmw, const RedoDataNode& redo);
  ObMemtable* get_first_not_null_memtable_if_exit_(ObMvccRowCallback* start, ObMvccRowCallback* end);

private:
  DISALLOW_COPY_AND_ASSIGN(ObRedoLogGenerator);
  bool is_inited_;
  ObMvccRowCallback* consume_cursor_;
  ObMvccRowCallback* generate_cursor_;
  ObMvccRowCallback* end_guard_;
  ObIMemtableCtx* mem_ctx_;
  char* big_row_buf_;
  int64_t big_row_size_;
  int64_t generate_big_row_pos_;
  int64_t consume_big_row_pos_;
  int64_t generate_data_size_;
};
};  // end namespace memtable
};  // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_REDO_LOG_GENERATOR_ */
