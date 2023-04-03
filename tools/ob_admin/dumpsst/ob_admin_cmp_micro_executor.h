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

#ifndef OCEANBASE_TOOLS_OB_ADMIN_CMP_MICRO_EXECUTOR_H
#define OCEANBASE_TOOLS_OB_ADMIN_CMP_MICRO_EXECUTOR_H
#include "../ob_admin_executor.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_header.h"

namespace oceanbase
{
namespace tools
{
class ObAdminCmpMicroExecutor : public ObAdminExecutor
{
public:
  ObAdminCmpMicroExecutor() {}
  virtual ~ObAdminCmpMicroExecutor() {}
  virtual int execute(int argc, char *argv[]);
private:
  int parse_cmd(int argc, char *argv[]);
  int open_file();
  int read_header();
  int compare_micro();
  int read_micro(char *&micro_data, const int64_t idx);
  int decompress_micro(const int64_t idx);
  void print_micro_meta(const int64_t idx);
private:
  static const int64_t MACRO_BLOCK_SIZE = 2 << 20;
  char data_file_path_[2][common::OB_MAX_FILE_NAME_LENGTH];
  int64_t macro_id_[2];
  int fd_[2];
  blocksstable::ObRecordHeaderV3 record_header_[2];
  char *micro_buf_[2];
  char *uncomp_micro_[2];
  char *macro_buf_[2];
  blocksstable::ObSSTableMacroBlockHeader *header_[2];
};


}
}
#endif /* OCEANBASE_TOOLS_OB_ADMIN_CMP_MICRO_EXECUTOR_H */
