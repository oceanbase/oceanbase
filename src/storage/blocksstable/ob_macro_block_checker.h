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

#ifndef OB_MACRO_BLOCK_CHECKER_H
#define OB_MACRO_BLOCK_CHECKER_H
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/ob_sparse_micro_block_reader.h"

namespace oceanbase {
namespace blocksstable {
class ObFullMacroBlockMeta;

enum ObMacroBlockCheckLevel {
  CHECK_LEVEL_NOTHING = 0,
  CHECK_LEVEL_MACRO,  // verify checksum of macro buf, return success if checksum not exist
  CHECK_LEVEL_MICRO,  // verify checksum of record buf
  CHECK_LEVEL_ROW,    // verify column checksum by iterating every row
  CHECK_LEVEL_AUTO,   // verify macro if macro checksum exist, else micro
  CHECK_LEVEL_MAX
};

// note: this class is NOT thread safe
class ObSSTableMacroBlockChecker {
public:
  ObSSTableMacroBlockChecker() : flat_reader_(), allocator_(common::ObModIds::OB_MACRO_BLOCK_CHECKER), macro_reader_()
  {}
  virtual ~ObSSTableMacroBlockChecker()
  {}
  int check(const char* macro_block_buf, const int64_t macro_block_buf_size, const ObFullMacroBlockMeta& meta,
      ObMacroBlockCheckLevel check_level = CHECK_LEVEL_AUTO);
  void destroy();

private:
  int check_macro_buf(
      const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const int64_t macro_block_buf_size);
  int check_data_header(const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf,
      const int64_t macro_block_buf_size, const ObFullMacroBlockMeta& meta);
  int check_data_block(const char* macro_block_buf, const int64_t macro_block_buf_size,
      const ObFullMacroBlockMeta& meta, const bool need_check_row);
  int check_lob_block(
      const char* macro_block_buf, const int64_t macro_block_buf_size, const ObFullMacroBlockMeta& meta);
  int check_micro_data(
      const char* micro_buf, const int64_t micro_buf_size, const ObFullMacroBlockMeta& meta, int64_t* checksum);
  int build_column_map(const ObFullMacroBlockMeta& meta);
  int check_sstable_data_header(
      const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta);
  int check_lob_data_header(
      const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta);
  int check_bloomfilter_data_header(
      const ObMacroBlockCommonHeader& common_header, const char* macro_block_buf, const ObFullMacroBlockMeta& meta);
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroBlockChecker);

private:
  ObMicroBlockReader flat_reader_;
  ObSparseMicroBlockReader sparse_reader_;
  common::ObArenaAllocator allocator_;
  ObMacroBlockReader macro_reader_;
  ObColumnMap column_map_;
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(common::ObObj)];  // for reader to get row
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif  // OB_MACRO_BLOCK_CHECKER_H
