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

#include "lib/utility/ob_macro_utils.h"
#include "share/ob_define.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace blocksstable
{
class ObMacroBlockRowBareIterator;
enum ObMacroBlockCheckLevel
{
  CHECK_LEVEL_NONE = 0,  // no check
  CHECK_LEVEL_PHYSICAL = 1, // verify data checksum
  CHECK_LEVEL_LOGICAL  = 2, // verify column checksum
  CHECK_LEVEL_MAX,
};

// note: this class is NOT thread safe
class ObSSTableMacroBlockChecker final
{
public:
  ObSSTableMacroBlockChecker() = default;
  ~ObSSTableMacroBlockChecker() = default;
  static int check(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      ObMacroBlockCheckLevel check_level = CHECK_LEVEL_PHYSICAL);
  static int check_macro_block(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const ObMacroBlockCheckLevel check_level);
private:
  static int check_logical_checksum(
      const ObMacroBlockCommonHeader &common_header,
      const char *macro_block_buf,
      const int64_t macro_block_buf_size);
  static int calc_micro_column_checksum(
      ObIMicroBlockReader &reader,
      ObDatumRow &datum_row,
      int64_t *column_checksum);
  static int check_physical_checksum(
      const ObMacroBlockCommonHeader &common_header,
      const char *macro_block_buf,
      const int64_t macro_block_buf_size);
  static int check_physical_checksum(
      const ObSharedObjectHeader &shared_obj_header,
      const char *macro_block_buf,
      const int64_t macro_block_buf_size);
  static int get_sstable_header_and_column_checksum(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      ObSSTableMacroBlockHeader &header,
      const int64_t *&column_checksum);
  static int check_sstable_macro_block(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const ObMacroBlockCommonHeader &common_header);
  static int check_data_micro_block(
      ObMacroBlockRowBareIterator &macro_iter);
  static int check_index_micro_block(
      ObMacroBlockRowBareIterator &macro_iter);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableMacroBlockChecker);
};

} // namespace blocksstable
} // namespace oceanbase

#endif//OB_MACRO_BLOCK_CHECKER_H
