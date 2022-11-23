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

#ifndef OCEANBASE_UNITTEST_STORAGE_BLOCKSSTABLE_TEST_SSTABLE_GENERATOR_H_
#define OCEANBASE_UNITTEST_STORAGE_BLOCKSSTABLE_TEST_SSTABLE_GENERATOR_H_
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/blocksstable/ob_macro_block_meta.h"
#include "storage/blocksstable/ob_macro_block_marker.h"
namespace oceanbase
{
using namespace common;
namespace blocksstable
{
class ObDataFile;
}
namespace unittest
{
class TestSSTableGenerator
{
public:
  TestSSTableGenerator();
  int open(
      blocksstable::ObDataStoreDesc &desc,
      const char *path,
      const int64_t row_count);
  int generate();
  int close();
  blocksstable::ObDataFile *get_data_file() { return &data_file_; }
private:
  int generate_row(const int64_t index);
private:
  char path_[OB_MAX_FILE_NAME_LENGTH];
  blocksstable::ObDataStoreDesc desc_;
  blocksstable::ObDataFile data_file_;
  blocksstable::ObBaseStorageLogger logger_;
  blocksstable::ObMacroBlockMetaImage image_;
  blocksstable::ObMacroBlockWriter writer_;
  blocksstable::ObMacroBlockMarker marker_;
  int64_t row_count_;
  common::ObObj cells_[common::OB_ROW_MAX_COLUMNS_COUNT];
  common::ModulePageAllocator mod_;
  common::ModuleArena arena_;
};
}//end namespace unittest
}//end namespace oceanbase
#endif
