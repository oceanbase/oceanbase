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
#include "ob_physical_copy_task.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"

namespace oceanbase
{
namespace storage
{

class ObIStorageHAMacroBlockWriter
{
public:
  enum Type {
    LOCAL_MACRO_BLOCK_OB_WRITER = 0,
    SHARED_MACRO_BLOCK_OB_WRITER = 1,
    MAX_WRITER_TYPE
  };
  ObIStorageHAMacroBlockWriter() {}
  virtual ~ObIStorageHAMacroBlockWriter() {}
  virtual int process(blocksstable::ObMacroBlocksWriteCtx &copied_ctx, ObIHADagNetCtx &ha_dag_net_ctx) = 0;
  virtual Type get_type() const = 0;
};


class ObCopyTabletRecordExtraInfo;
class ObStorageHAMacroBlockWriter : public ObIStorageHAMacroBlockWriter
{
public:
  ObStorageHAMacroBlockWriter();
  virtual ~ObStorageHAMacroBlockWriter() {}
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const ObDagId &dag_id,
      const ObMigrationSSTableParam *sstable_param,
      ObICopyMacroBlockReader *reader,
      ObIndexBlockRebuilder *index_block_rebuilder,
      ObCopyTabletRecordExtraInfo *extra_info
  );

  virtual int process(blocksstable::ObMacroBlocksWriteCtx &copied_ctx, ObIHADagNetCtx &ha_dag_net_ctx) override;

protected:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *sstable_param) const = 0;
  virtual int set_macro_write_info_(
      const MacroBlockId &macro_block_id,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectOpt &opt) = 0;
  virtual int append_macro_row_(
      const char *buf,
      const int64_t size,
      const blocksstable::MacroBlockId &macro_id) = 0;

private:
  int check_macro_block_(
      const blocksstable::ObBufferReader &data);
  int write_macro_block_(
      const ObStorageObjectOpt &opt,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectHandle &write_handle,
      blocksstable::ObMacroBlocksWriteCtx &copied_ctx,
      blocksstable::ObBufferReader &data);

protected:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObDagId dag_id_;
  const ObMigrationSSTableParam *sstable_param_;
  ObICopyMacroBlockReader *reader_;
  ObIndexBlockRebuilder *index_block_rebuilder_;
  blocksstable::ObSSTableMacroBlockChecker macro_checker_;
  ObCopyTabletRecordExtraInfo *extra_info_;
};

class ObStorageHALocalMacroBlockWriter final : public ObStorageHAMacroBlockWriter
{
public:
  ObStorageHALocalMacroBlockWriter () : ObStorageHAMacroBlockWriter() {}
  virtual ~ObStorageHALocalMacroBlockWriter() {}

  virtual Type get_type() const override { return LOCAL_MACRO_BLOCK_OB_WRITER; }

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *sstable_param) const override;
  virtual int set_macro_write_info_(
      const MacroBlockId &macro_block_id,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectOpt &opt) override;
  virtual int append_macro_row_(
      const char *buf,
      const int64_t size,
      const blocksstable::MacroBlockId &macro_id) override;
  
  DISALLOW_COPY_AND_ASSIGN(ObStorageHALocalMacroBlockWriter);
};


#ifdef OB_BUILD_SHARED_STORAGE
class ObStorageHASharedMacroBlockWriter final : public ObStorageHAMacroBlockWriter
{
public:
  ObStorageHASharedMacroBlockWriter () : ObStorageHAMacroBlockWriter() {}
  virtual ~ObStorageHASharedMacroBlockWriter() {}
  virtual Type get_type() const override { return SHARED_MACRO_BLOCK_OB_WRITER; }

private:
  virtual int check_sstable_param_for_init_(const ObMigrationSSTableParam *sstable_param) const override;
  virtual int set_macro_write_info_(
      const MacroBlockId &macro_block_id,
      blocksstable::ObStorageObjectWriteInfo &write_info,
      blocksstable::ObStorageObjectOpt &opt) override;
  virtual int append_macro_row_(
      const char *buf,
      const int64_t size,
      const blocksstable::MacroBlockId &macro_id) override;
  
  DISALLOW_COPY_AND_ASSIGN(ObStorageHASharedMacroBlockWriter);
};

#endif

}
}
#endif
