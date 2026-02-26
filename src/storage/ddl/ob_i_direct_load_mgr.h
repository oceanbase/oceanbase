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

#ifndef OCEANBASE_STORAGE_DDL_OB_I_DIRECT_LOAD_MGR_H
#define OCEANBASE_STORAGE_DDL_OB_I_DIRECT_LOAD_MGR_H

#include "lib/lock/ob_mutex.h"
#include "lib/lock/ob_bucket_lock.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "src/share/ob_ddl_common.h"

namespace oceanbase
{
namespace storage
{

class ObInsertMonitor;
class ObIStoreRowIterator;
class ObTabletDirectLoadBuildCtx;

enum class ObDirectLoadMgrRole
{
  INVALID_TYPE = 0,
  DATA_TABLET_TYPE = 1,
  LOB_TABLET_TYPE = 2,
  MAX_TYPE
};

/*
 *  base_direct_load_mgr have two total different interface
 *  1. v1 interface,
       v1 interface is firstly used for shared nothing mode,
       * global unique param, the sub class will be managed by the ObTenantDirectLoadMgr,
       * rely on different execution id to distinguish from different retry tasks
       * lob hanlde will be bind to to the data direct load mg
      +-------+    +-------------------+    +-----------------+
      | Agent | -> | data_direct_load  | -> | lob_direct_load |
      +-------+    +-------------------+    +-----------------+

    2. v2 interface
       v2 interface use idempodency logic for both shared storage & share nothing,
       * local param, sub classes management don't rely on tenant direct load mgr any more
       * rely on idempodency logic, data will be the same in different retry tasks
       * lob handle & data handle are seperated, direct_load_mgr_agent can directly operate on both of them
      +-------+    +-------------------+
      |       | -> | data_direct_load  |
      |       |    +-------------------+
      | Agent |
      |       |    +-------------------+
      |       | -> | lob_direct_load   |
      +-------+    +-------------------+
 */

class ObBaseTabletDirectLoadMgr
{
public:
  ObBaseTabletDirectLoadMgr();
  virtual ~ObBaseTabletDirectLoadMgr();
  virtual bool is_valid() = 0;
  TO_STRING_KV(K_(table_key), K_(tenant_data_version), K_(direct_load_type),
               K_(ls_id), K_(tablet_id));
public: /* some baisc method */
  void inc_ref() { ATOMIC_INC(&ref_cnt_); };
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }
  virtual int cancel() = 0;
  virtual int update_max_lob_id(const int64_t lob_id) { return OB_NOT_SUPPORTED; };
public:
  /* --------- direct_load_mgr interface  v1 ---------*/
  virtual int update(ObBaseTabletDirectLoadMgr *lob_tablet_mgr, const ObTabletDirectLoadInsertParam &build_param)
  { return OB_NOT_SUPPORTED; }
  virtual int open(const int64_t current_execution_id, share::SCN &start_scn)
  { return OB_NOT_SUPPORTED; }
  virtual int close(const int64_t current_execution_id, const share::SCN &start_scn)
  { return OB_NOT_SUPPORTED; }
  virtual int open_sstable_slice(const bool is_data_tablet_process_for_lob,
                                 const blocksstable::ObMacroDataSeq &start_seq,
                                const ObDirectLoadSliceInfo &slice_info)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_sstable_slice(const ObDirectLoadSliceInfo &slice_info,
                                 const share::SCN &start_scn,
                                 ObIStoreRowIterator *iter,
                                 int64_t &affected_rows,
                                 ObInsertMonitor *insert_monitor = NULL)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_sstable_slice(
      const ObDirectLoadSliceInfo &slice_info,
      const share::SCN &start_scn,
      const blocksstable::ObBatchDatumRows &datum_rows,
      ObInsertMonitor *insert_monitor = NULL)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_lob_sstable_slice(ObIAllocator &allocator,
                                     const ObDirectLoadSliceInfo &slice_info,
                                     const share::SCN &start_scn,
                                     share::ObTabletCacheInterval &pk_interval,
                                     blocksstable::ObDatumRow &datum_row)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_lob_sstable_slice(ObIAllocator &allocator,
                                     const ObDirectLoadSliceInfo &slice_info,
                                     const share::SCN &start_scn,
                                     share::ObTabletCacheInterval &pk_interval,
                                     blocksstable::ObBatchDatumRows &datum_rows)
  {return OB_NOT_SUPPORTED; }
  virtual int close_sstable_slice(const bool is_data_tablet_process_for_lob,
                                  const ObDirectLoadSliceInfo &slice_info,
                                  const share::SCN &start_scn,
                                  const int64_t execution_id,
                                  ObInsertMonitor *insert_monitor,
                                  blocksstable::ObMacroDataSeq &next_seq)
  { return OB_NOT_SUPPORTED; }
  /*
   * a inteferface only used for inc direct load
   */
  virtual int fill_lob_meta_sstable_slice(const ObDirectLoadSliceInfo &slice_info,
                                          const share::SCN &start_scn,
                                          ObIStoreRowIterator *iter,
                                          int64_t &affected_rows) { return OB_NOT_SUPPORTED; };
  virtual int calc_range(const int64_t context_id, const int64_t thread_cnt) { return OB_NOT_SUPPORTED; }
  virtual int fill_column_group(int64_t, int64_t) { return OB_NOT_SUPPORTED; }
public: /* --------- direct_load_mgr interface  v2 ---------*/
  virtual int init_v2(const ObTabletDirectLoadInsertParam &build_param,
                      const int64_t execution_id,
                      const ObDirectLoadMgrRole role)
  { return OB_NOT_SUPPORTED; }
  virtual int prepare_index_builder() { return OB_NOT_SUPPORTED; }
  virtual int fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                    ObIStoreRowIterator *iter,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    blocksstable::ObMacroDataSeq &next_seq,
                                    ObInsertMonitor *insert_monitor,
                                    int64_t &affected_rows)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                    const ObBatchDatumRows &datum_rows,
                                    ObDirectLoadSliceWriter &slice_writer,
                                    ObInsertMonitor *insert_monitorr)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                            const ObDirectLoadSliceInfo &slice_info,
                                            share::ObTabletCacheInterval &pk_interval,
                                            blocksstable::ObDatumRow &datum_row,
                                            ObDirectLoadSliceWriter &slice_writer,
                                            ObTabletDirectLoadMgrHandle &data_direct_load_handle)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_lob_sstable_slice_row_v2(ObIAllocator &allocator,
                                            const ObDirectLoadSliceInfo &slice_info,
                                            share::ObTabletCacheInterval &pk_interval,
                                            ObDirectLoadSliceWriter &slice_writer,
                                            ObTabletDirectLoadMgrHandle &data_direct_load_handle,
                                            blocksstable::ObBatchDatumRows &datum_row)
  { return OB_NOT_SUPPORTED; }
  virtual int close_sstable_slice_v2(const ObDirectLoadSliceInfo &slice_info,
                                     ObDirectLoadSliceWriter &slice_writer,
                                     blocksstable::ObMacroDataSeq &next_seq,
                                     ObInsertMonitor *insert_monitor,
                                     bool &is_all_sliced_finished)
  { return OB_NOT_SUPPORTED; }
  virtual int fill_lob_meta_sstable_slice(const ObDirectLoadSliceInfo &slice_info,
                                          ObIStoreRowIterator *iter,
                                          ObDirectLoadSliceWriter &slice_writer,
                                          int64_t &affected_rows)
  { return OB_NOT_SUPPORTED; };
  virtual int close() { return OB_NOT_SUPPORTED; }

public:
  /* get basic info */
  inline  ObITable::TableKey get_table_key() const { return table_key_; }
  inline uint64_t get_tenant_data_version() const { return tenant_data_version_; }
  inline ObDirectLoadType get_direct_load_type() const { return direct_load_type_; }
  inline share::ObLSID get_ls_id() const { return ls_id_;}
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  /* some getter method for compat
   * which should be remove
  */
  virtual bool need_process_cs_replica() const = 0;
  virtual int64_t get_ddl_task_id() const = 0;
  virtual ObWholeDataStoreDesc &get_data_block_desc() = 0;
  virtual ObTabletDirectLoadInsertParam &get_build_param() = 0;
  virtual int64_t get_task_cnt() = 0;
  virtual int64_t get_cg_cnt()   = 0;
  virtual const ObIArray<ObColumnSchemaItem> &get_column_info() const = 0;
  virtual bool get_micro_index_clustered() = 0;

  /* TODO zhuoran, wait to fullfil in new sub class*/
  virtual bool get_is_no_logging() {return false; }
  virtual int32_t get_private_transfer_epoch()  {return -1; }

protected:
  /* basic info */
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObITable::TableKey table_key_;
  uint64_t tenant_data_version_;
  ObDirectLoadType direct_load_type_;
  /* concurrent control param */
  int64_t ref_cnt_;
  common::ObLatch lock_;
  common::ObThreadCond cond_;
};

}// namespace storage
}// namespace oceanbase

#endif//OCEANBASE_STORAGE_OB_DIRECT_INSERT_SSTABLE_CTX_NEW_H
