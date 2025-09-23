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

#ifndef OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_AGENT_H
#define OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_AGENT_H

#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"

namespace oceanbase
{

namespace share
{
struct ObTabletCacheInterval;
}

namespace storage
{

struct ObDirectLoadMgrAgent final
{
public:
    ObDirectLoadMgrAgent();
    ~ObDirectLoadMgrAgent();
public:
  int init(
      const int64_t context_id,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id/*always data tablet id.*/,
      const ObDirectLoadType &type);
  int open_sstable_slice(
      const blocksstable::ObMacroDataSeq &start_seq,
      ObDirectLoadSliceInfo &slice_info);
  int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      const blocksstable::ObBatchDatumRows &datum_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObBatchDatumRows &datum_rows);
  int fill_lob_meta_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows);
  int close_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info, 
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq);
  int calc_range(const int64_t context_id, const int64_t thread_cnt);
  int fill_column_group(
      const int64_t thread_cnt,
      const int64_t thread_id,
      ObInsertMonitor *insert_monitor=nullptr);
  int cancel();
  // other utils
  inline ObDirectLoadType get_direct_load_type() const { return direct_load_type_; }
  int get_lob_meta_tablet_id(ObTabletID &lob_meta_tablet_id);
  int update_max_lob_id(const int64_t lob_id);
  TO_STRING_KV(K_(is_inited), K_(direct_load_type), K_(start_scn), K_(execution_id), K_(cgs_count), KPC(mgr_handle_.get_obj()));
private:
  int init_for_sn(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
  int init_for_ss();
  int open_sstable_slice_for_ss(
      const blocksstable::ObMacroDataSeq &start_seq,
      ObDirectLoadSliceInfo &slice_info);
  int fill_sstable_slice_for_ss(
      const ObDirectLoadSliceInfo &slice_info,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_sstable_slice_for_ss(
      const ObDirectLoadSliceInfo &slice_info,
      const blocksstable::ObBatchDatumRows &datum_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_lob_sstable_slice_for_ss(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice_for_ss(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObBatchDatumRows &datum_rows);
  int fill_lob_meta_sstable_slice_for_ss(
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows);
  int close_sstable_slice_for_ss(
      const ObDirectLoadSliceInfo &slice_info, 
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq);

  int open_sstable_slice_for_sn(
      const blocksstable::ObMacroDataSeq &start_seq,
      ObDirectLoadSliceInfo &slice_info);
  int fill_sstable_slice_for_sn(
      const ObDirectLoadSliceInfo &slice_info,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_sstable_slice_for_sn(
      const ObDirectLoadSliceInfo &slice_info,
      const blocksstable::ObBatchDatumRows &datum_rows,
      ObInsertMonitor *insert_monitor = nullptr);
  int fill_lob_sstable_slice_for_sn(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObDatumRow &datum_row);
  int fill_lob_sstable_slice_for_sn(
      ObIAllocator &allocator,
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      share::ObTabletCacheInterval &pk_interval,
      blocksstable::ObBatchDatumRows &datum_rows);
  int fill_lob_meta_sstable_slice_for_sn(
      const ObDirectLoadSliceInfo &slice_info /*contains data_tablet_id, lob_slice_id, start_seq*/,
      ObIStoreRowIterator *iter,
      int64_t &affected_rows);
  int close_sstable_slice_for_sn(
      const ObDirectLoadSliceInfo &slice_info, 
      ObInsertMonitor *insert_monitor,
      blocksstable::ObMacroDataSeq &next_seq);
private:
  bool is_inited_;
  ObDirectLoadType direct_load_type_;
  share::SCN start_scn_; // start scn in the context.
  int64_t execution_id_; // execution_id in the context.
  ObTabletDirectLoadMgrHandle mgr_handle_;
  int64_t cgs_count_; // count of the column groups, used for the sn's statistics when retry after committed.
DISALLOW_COPY_AND_ASSIGN(ObDirectLoadMgrAgent);
};


}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_DDL_OB_DIRECT_LOAD_MGR_AGENT_H
