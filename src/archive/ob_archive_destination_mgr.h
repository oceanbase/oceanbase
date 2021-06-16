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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_DESTINATION_MGR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_DESTINATION_MGR_H_

#include "common/ob_partition_key.h"  // ObPGKey
#include "ob_log_archive_define.h"    // LogArchiveFileType

namespace oceanbase {
namespace archive {
// simultaneous unsafe
class ObArchiveDestination {
public:
  ObArchiveDestination()
  {
    reset();
  }
  ~ObArchiveDestination()
  {
    reset();
  }
  void reset();
  int init(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t round, const bool compatible,
      const uint64_t cur_index_file_id, const int64_t index_file_offset, const uint64_t cur_data_file_id,
      const int64_t data_file_offset);
  int init_with_valid_residual_data_file(const common::ObPGKey& pg_key, const int64_t incarnation, const int64_t round,
      const bool compatible, const uint64_t cur_index_file_id, const int64_t index_file_offset,
      const uint64_t cur_data_file_id, const int64_t data_file_offset, const uint64_t min_log_id,
      const int64_t min_log_ts);

public:
  // switch file when the previous file is full or IO error occur
  // file id: N -> N+1
  int switch_file(const common::ObPGKey& pg_key, const LogArchiveFileType file_type, const int64_t incarnation,
      const int64_t round);
  // update file offset when IO write succeed
  int update_file_offset(const int64_t buf_len, const LogArchiveFileType file_type);
  // for delay index info record, min log info is cached here after the first log in the file writen
  int set_data_file_record_min_log_info(const uint64_t min_log_id, const int64_t min_log_ts);
  // if min log info not cache here, return false, else return true
  bool is_data_file_min_log_info_set()
  {
    return is_data_file_valid_;
  }
  // if IO error occur, set the force switch file flag
  int set_file_force_switch(const LogArchiveFileType file_type);
  // get current archived file info
  int get_file_info(const common::ObPGKey& pg_key, const LogArchiveFileType type, const int64_t incarnation,
      const int64_t round, bool& compatible, int64_t& offset, const int64_t path_len, char* file_path);
  // get current archived file offset
  int get_file_offset(const LogArchiveFileType type, int64_t& offset, bool& flag);
  // get the min log info in the archived file, for index file record
  int get_data_file_min_log_info(uint64_t& min_log_id, int64_t& min_log_ts, bool& is_valid);
  TO_STRING_KV(K(is_inited_), K(compatible_), K(cur_index_file_id_), K(index_file_offset_), K(cur_data_file_id_),
      K(data_file_offset_), K(data_file_min_log_id_), K(data_file_min_log_ts_), K(force_switch_data_file_),
      K(force_switch_index_file_), K(is_data_file_valid_));

private:
  int update_file_meta_info_(const LogArchiveFileType file_type);

public:
  bool is_inited_;
  bool compatible_;
  uint64_t cur_index_file_id_;
  int64_t index_file_offset_;
  uint64_t cur_data_file_id_;
  int64_t data_file_offset_;
  uint64_t data_file_min_log_id_;
  int64_t data_file_min_log_ts_;
  bool force_switch_data_file_;
  bool force_switch_index_file_;
  bool is_data_file_valid_;
};
}  // namespace archive
}  // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_DESTINATION_MGR_H_ */
