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

#ifndef OCEABASE_STORAGE_HA_MACRO_BLOCK_WRITER_
#define OCEABASE_STORAGE_HA_MACRO_BLOCK_WRITER_

#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "ob_storage_ha_reader.h"
#include "storage/blocksstable/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{

class ObIStorageHAMacroBlockWriter
{
public:
  enum Type {
    MACRO_BLOCK_OB_WRITER = 0,
    MAX_READER_TYPE
  };
  ObIStorageHAMacroBlockWriter() {}
  virtual ~ObIStorageHAMacroBlockWriter() {}
  virtual int process(blocksstable::ObMacroBlocksWriteCtx &copied_ctx) = 0;
  virtual Type get_type() const = 0;
};


class ObStorageHAMacroBlockWriter : public ObIStorageHAMacroBlockWriter
{
public:
  ObStorageHAMacroBlockWriter();
  virtual ~ObStorageHAMacroBlockWriter() {}
  int init(
      const uint64_t tenant_id,
      ObICopyMacroBlockReader *reader,
      ObIndexBlockRebuilder *index_block_rebuilder);

  virtual int process(blocksstable::ObMacroBlocksWriteCtx &copied_ctx);
  virtual Type get_type() const { return MACRO_BLOCK_OB_WRITER; }
private:
  int check_macro_block_(
      const blocksstable::ObBufferReader &data);
  bool is_inited_;
  uint64_t tenant_id_;
  ObICopyMacroBlockReader *reader_;
  blocksstable::ObSSTableMacroBlockChecker macro_checker_;
  ObIndexBlockRebuilder *index_block_rebuilder_;
};


}
}
#endif
