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

#ifndef OCEANBASE_SQL_ENGINE_SORT_RUN_FILE_H_
#define OCEANBASE_SQL_ENGINE_SORT_RUN_FILE_H_

#include "lib/file/ob_file.h"
#include "lib/container/ob_array.h"
#include "common/row/ob_row.h"
namespace oceanbase {
namespace sql {
// run file for merge sort
// support multi-bucket, multi-run in one physical file
// @note not thread-safe
class ObRunFile {
public:
  ObRunFile();
  virtual ~ObRunFile();

  int open(const common::ObString& filename);
  int close();
  bool is_opened() const;

  int begin_append_run(const int64_t bucket_idx);
  int append_row(const common::ObString& compact_row);
  int end_append_run();

  int begin_read_bucket(const int64_t bucket_idx, int64_t& run_count);
  /// @return OB_ITER_END when reaching the end of this run
  int get_next_row(const int64_t run_idx, common::ObNewRow& row);
  int end_read_bucket();

private:
  // types and constants
  static const int64_t MAGIC_NUMBER = 0x656c69666e7572;  // "runfile"
  struct RunTrailer {
    int64_t magic_number_;
    int64_t bucket_idx_;
    int64_t prev_run_trailer_pos_;
    int64_t curr_run_size_;
    RunTrailer() : magic_number_(MAGIC_NUMBER), bucket_idx_(-1), prev_run_trailer_pos_(-1), curr_run_size_(0)
    {}
    TO_STRING_KV(K_(bucket_idx), K_(curr_run_size));
  };
  struct RunBlock : public common::ObFileBuffer {
    static const int64_t BLOCK_SIZE = 2 * 1024 * 1024LL;
    int64_t run_end_offset_;
    int64_t block_offset_;
    int64_t block_data_size_;
    int64_t next_row_pos_;
    RunBlock();
    ~RunBlock();
    void reset();
    void free_buffer();
    bool need_read_next_block() const;
    bool is_end_of_run() const;
    TO_STRING_KV(K_(run_end_offset), K_(block_offset), K_(block_data_size), K_(next_row_pos));
  };

private:
  // function members
  int find_last_run_trailer(const int64_t bucket_idx, RunTrailer*& bucket_info);
  int read_next_run_block(RunBlock& run_block);
  int block_get_next_row(RunBlock& run_block, common::ObNewRow& row);
  // @return OB_BUF_NOT_ENOUGH or OB_SUCCESS or other errors
  int parse_row(const char* buf, const int64_t buf_len, common::ObString& compact_row, common::ObNewRow& row);

private:
  // data members
  common::ObFileAppender file_appender_;
  common::ObFileReader file_reader_;
  common::ObArray<RunTrailer> buckets_last_run_trailer_;
  RunTrailer* cur_run_trailer_;
  common::ObArray<RunBlock> run_blocks_;
  int64_t cur_run_row_count_;
  DISALLOW_COPY_AND_ASSIGN(ObRunFile);
};
}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_RUN_FILE_H_ */
