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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_HANDLE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_HANDLE_H_
#include "storage/tmp_file/ob_sn_tmp_file_io_handle.h"
#include "storage/blocksstable/ob_i_tmp_file.h"

namespace oceanbase
{
namespace tmp_file
{

class ObTmpFileIOHandle final
{
public:
  ObTmpFileIOHandle();
  ~ObTmpFileIOHandle();
  int wait();
  void reset();
  bool is_valid();
  int64_t get_done_size();
  char *get_buffer();
  ObSNTmpFileIOHandle &get_sn_handle();
  blocksstable::ObSSTmpFileIOHandle &get_ss_handle();

  TO_STRING_KV(K(is_shared_storage_mode_), K(type_decided_), KP(sn_handle_), KP(ss_handle_));
private:
  void check_or_set_handle_type_();

private:
  bool is_shared_storage_mode_;
  bool type_decided_;
  static constexpr int64_t BUF_SIZE
    = MAX(sizeof(ObSNTmpFileIOHandle), sizeof(blocksstable::ObSSTmpFileIOHandle));
  char buf_[BUF_SIZE];

  ObSNTmpFileIOHandle* sn_handle_;
  blocksstable::ObSSTmpFileIOHandle* ss_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOHandle);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_HANDLE_H_
