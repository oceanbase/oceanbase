/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertLobTableContext;
class ObDirectLoadInsertDataTableContext;
class ObDirectLoadInsertDataTabletContext;

class ObDirectLoadInsertLobTabletContext : public ObDirectLoadInsertTabletContext
{
public:
  ObDirectLoadInsertLobTabletContext();
  virtual ~ObDirectLoadInsertLobTabletContext();
  int init(ObDirectLoadInsertLobTableContext *table_ctx,
           ObDirectLoadInsertDataTabletContext *data_tablet_ctx, const share::ObLSID &ls_id,
           const common::ObTabletID &origin_tablet_id, const common::ObTabletID &tablet_id);
  int open() override;
  int close() override;
  void cancel() override;

  //////////////////////// write interface ////////////////////////
public:
  int open_sstable_slice(const blocksstable::ObMacroDataSeq &start_seq,
                         const int64_t slice_idx,
                         int64_t &slice_id) override;
  int fill_sstable_slice(const int64_t &slice_id, ObIStoreRowIterator &iter,
                         int64_t &affected_rows) override;
  int fill_sstable_slice(const int64_t &slice_id,
                         const blocksstable::ObBatchDatumRows &datum_rows) override;
  int close_sstable_slice(const int64_t slice_id, const int64_t slice_idx) override;
  // 特殊写lob接口, datum_row是主表数据
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id,
                             share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice(ObIAllocator &allocator, const int64_t &lob_slice_id,
                             share::ObTabletCacheInterval &pk_interval,
                             blocksstable::ObBatchDatumRows &datum_rows);
  const ObLobId &get_min_insert_lob_id() const { return min_insert_lob_id_; }

private:
  int get_pk_interval(uint64_t count, share::ObTabletCacheInterval &pk_interval) override;

  //////////////////////// members ////////////////////////
public:
  INHERIT_TO_STRING_KV("ObDirectLoadInsertTabletContext", ObDirectLoadInsertTabletContext,
                       KP_(data_tablet_ctx), K_(tablet_id_in_lob_id), K_(min_insert_lob_id));

private:
  ObDirectLoadInsertDataTabletContext *data_tablet_ctx_;
  common::ObTabletID tablet_id_in_lob_id_;
  ObLobId min_insert_lob_id_;
};

class ObDirectLoadInsertLobTableContext : public ObDirectLoadInsertTableContext
{
  friend class ObDirectLoadInsertLobTabletContext;

public:
  ObDirectLoadInsertLobTableContext();
  virtual ~ObDirectLoadInsertLobTableContext();
  int init(const ObDirectLoadInsertTableParam &param,
           ObDirectLoadInsertDataTableContext *data_table_ctx,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &data_ls_partition_ids);

private:
  int create_all_tablet_contexts(
    ObDirectLoadInsertDataTableContext *data_table_ctx,
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &data_ls_partition_ids);
};

} // namespace storage
} // namespace oceanbase
