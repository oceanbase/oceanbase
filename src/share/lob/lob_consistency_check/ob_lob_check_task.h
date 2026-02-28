/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_LOB_OB_LOB_CHECK_TASK_H_
#define OCEANBASE_SHARE_LOB_OB_LOB_CHECK_TASK_H_

#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/ob_value_row_iterator.h"
#include "share/lob/lob_consistency_check/ob_lob_task_info.h"

namespace oceanbase {

namespace table {
  class ObTabletTTLScheduler;
  struct ObTTLTaskParam;
  struct ObTTLTaskInfo;
}

namespace share {

class ObLobBatchMetaDelIter : public storage::ObValueRowIterator {
public:
  ObLobBatchMetaDelIter();
  ~ObLobBatchMetaDelIter();
  int init(ObDMLBaseParam &dml_base_param);
  OB_INLINE void set_start_seq_no_st(transaction::ObTxSEQ &start_seq_no_st)
  {
    start_seq_no_st_ = start_seq_no_st;
  }
  int get_next_row(blocksstable::ObDatumRow *&row);

private:
  ObDMLBaseParam *dml_base_param_;
  transaction::ObTxSEQ start_seq_no_st_;
  int64_t used_seq_cnt_;

  DISALLOW_COPY_AND_ASSIGN(ObLobBatchMetaDelIter);
};

struct ObLobMetaSeqInfo {
  ObLobMetaSeqInfo() : lob_id_(), seq_ids_()
  {
  }
  ObLobMetaSeqInfo(const ObLobId lob_id) : lob_id_(lob_id), seq_ids_()
  {
  }

  int add_seq_id(const ObString &seq_id, ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    ObString seq_id_ptr;
    if (OB_FAIL(ob_write_string(allocator, seq_id, seq_id_ptr))) {
      TRANS_LOG(WARN, "fail to write seq id", K(ret), K(seq_id));
    } else if (OB_FAIL(seq_ids_.push_back(seq_id_ptr))) {
      TRANS_LOG(WARN, "fail to push back seq id", K(ret), K(seq_id));
    }
    return ret;
  }

  TO_STRING_KV(K_(lob_id));
  ObLobId lob_id_;
  ObSEArray<ObString, 16> seq_ids_;
};

class ObLobCheckTask : public share::ObITask {
public:
  ObLobCheckTask();
  ~ObLobCheckTask();

  virtual int process() override;

  int init(table::ObTabletTTLScheduler *tablet_ttl_scheduler, const table::ObTTLTaskParam &param,
           const table::ObTTLTaskInfo &info);

  static int build_range_from_lob_id(ObLobId &lob_id, ObArenaAllocator &allocator, common::ObNewRange &range);

  // 静态函数：遍历并缓存符合条件的 table_id 和 tablet_id
  static int cache_table_and_tablet_pairs(const uint64_t tenant_id,
                                      storage::ObLS *ls,
                                      const uint64_t start_table_id,
                                      const uint64_t start_tablet_id,
                                      const int64_t max_count,
                                      ObIArray<ObTabletTablePair> &pairs);

  static int check_table_valid_and_return_tablet(const uint64_t tenant_id,
                                                 uint64_t table_id,
                                                 bool &is_valid,
                                                 ObArray<ObTabletID> &tablet_ids);
  static int check_tablet_belongs_to_ls(storage::ObLS *ls, const ObTabletID &tablet_id, bool &belong);
  static int parse_table_and_tablet(const uint64_t tenant_id,
                                    storage::ObLS *ls,
                                    const common::ObString &row_key,
                                    bool is_repair_task,
                                    const uint64_t start_table_id,
                                    const uint64_t start_tablet_id,
                                    const int64_t max_count,
                                    ObIArray<ObTabletTablePair> &pairs);
  static int parse_repair_table_and_tablet(const uint64_t tenant_id,
                                            storage::ObLS *ls,
                                            const common::ObString &row_key,
                                            const uint64_t start_table_id,
                                            const uint64_t start_tablet_id,
                                            const int64_t max_count,
                                            ObIArray<ObTabletTablePair> &pairs);
  static int parse_check_table_and_tablet(const uint64_t tenant_id,
                                          storage::ObLS *ls,
                                          const common::ObString &row_key,
                                          const uint64_t start_table_id,
                                          const uint64_t start_tablet_id,
                                          const int64_t max_count,
                                          ObIArray<ObTabletTablePair> &pairs);
  static int read_orphan_data_to_pairs(const uint64_t tenant_id,
                                       storage::ObLS *ls,
                                       const uint64_t start_table_id,
                                       ObIArray<ObTabletTablePair> &pairs);

public:
  static const int64_t LOB_CHECK_TIMEOUT_US = 30 * 1000 * 1000;  // 30秒超时
  static const ObString LOB_META_SCAN_INDEX;

private:
  int process_one();
  int scan_main_table();
  int find_meta_table_id();
  int scan_auxiliary_table();
  int delete_orphan_auxiliary_data(transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot);
  int init_meta_iter(transaction::ObTxReadSnapshot &snapshot);
  int get_tablet_handle_inner(transaction::ObTxReadSnapshot &snapshot, ObTabletHandle &handle);
  void reuse_iterators();
  int next_tablet_task();
  int init_iterators(transaction::ObTxReadSnapshot &snapshot, ObTableScanParam &main_scan_param);
  int add_table_param(common::ObArenaAllocator &allocator, ObTableScanParam &scan_param);
  int save_current_rowkey(bool is_main_table, ObTableScanParam &scan_param, const blocksstable::ObDatumRow &datum_row);
  int restore_start_key_from_rowkey(bool is_main_table, ObTableScanParam &main_scan_param, ObNewRange &range);
  int get_tablet_id_by_ls_handle(transaction::ObTxReadSnapshot &snapshot, ObLSHandle &ls_handle,
                                 ObTabletHandle &handle);
  int build_table_scan_param(common::ObArenaAllocator &allocator, const transaction::ObTxReadSnapshot &snapshot,
                             bool is_main_table, const common::ObTabletID &tablet_id, ObTableScanParam &scan_param);
  bool check_lob_tablet_id(const common::ObTabletID &data_tablet_id, const common::ObTabletID &lob_meta_tablet_id,
                           const common::ObTabletID &lob_piece_tablet_id);

private:
  static const int64_t BATCH_SIZE = 1024;                   // 1024行为一个单位
  static const int64_t TABLE_AND_TABLET_BATCH_SIZE = 1000;  // 每次缓存多少表以及tablet

private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t meta_table_id_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  ObLS *ls_;
  common::ObArenaAllocator main_allocator_;
  common::ObArenaAllocator aux_allocator_;
  common::ObArenaAllocator allocator_;
  bool is_repair_task_;

  // 扫描相关
  storage::ObTableScanIterator *main_table_iter_;
  storage::ObLobMetaIterator *aux_table_iter_;
  ObSEArray<uint64_t, 4> lob_column_idxs_;

  // TTL相关
  table::ObTabletTTLScheduler *tablet_ttl_scheduler_;
  table::ObTTLTaskParam param_;
  table::ObTTLTaskInfo info_;

  // HashMap 用于辅助表扫描时的 lob_id 查找（可复用）
  common::hash::ObHashMap<uint64_t, bool> lob_id_map_;
  ObSEArray<ObLobMetaSeqInfo, BATCH_SIZE> meta_seq_info_array_;
  ObSEArray<share::ObTabletTablePair, TABLE_AND_TABLET_BATCH_SIZE> tablet_table_pairs_;
  ObSEArray<uint64_t, 4> data_table_rowkey_idxs_;
  uint16_t scan_tablet_idx_;
  bool need_next_tablet_;
  DISALLOW_COPY_AND_ASSIGN(ObLobCheckTask);
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_LOB_LOB_CHECK_TASK_H_