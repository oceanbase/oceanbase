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

#ifndef OCEABASE_STORAGE_HA_DIAGNOSE_
#define OCEABASE_STORAGE_HA_DIAGNOSE_

#include "share/ob_storage_ha_diagnose_struct.h"
#include "ob_storage_ha_diagnose_service.h"
#include "storage/ob_i_store.h"
#include "lib/list/ob_dlist.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
class ObStorageHADiagIterator final
{
public:
  ObStorageHADiagIterator();
  ~ObStorageHADiagIterator() { reset(); }
  int open(const ObStorageHADiagType &type);
  int get_next_info(ObStorageHADiagInfo &info);
  bool is_valid() const;
  int get_cur_key(ObStorageHADiagTaskKey &key) const;
  void reset();
  bool is_opened() const;

  TO_STRING_KV(
    K_(cnt),
    K_(cur_idx),
    K_(is_opened));

private:
  ObArray<ObStorageHADiagTaskKey> keys_;
  int64_t cnt_;
  int64_t cur_idx_;
  bool is_opened_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagIterator);
};

class ObStorageHADiagMgr final
{
public:
  ObStorageHADiagMgr();
  ~ObStorageHADiagMgr() { destroy(); }
  static int mtl_init(ObStorageHADiagMgr *&storage_ha_diag_mgr);
  int init(
      const uint64_t tenant_id,
      const int64_t page_size = INFO_PAGE_SIZE,
      const int64_t max_size = INFO_MAX_SIZE);

  int add_or_update(
      const ObStorageHADiagTaskKey &task_key,
      const ObStorageHADiagInfo &input_info,
      const bool is_report);
  int del(const ObStorageHADiagTaskKey &task_keys);
  int get_iter_array(const ObStorageHADiagType &type, ObIArray<ObStorageHADiagTaskKey> &array) const;
  int get(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo &info) const;
  void destroy();
  void clear();

  static int construct_diagnose_info(
      const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
      const share::ObStorageHADiagTaskType type, const int64_t retry_id,
      const int result_code, const share::ObStorageHADiagModule module,
      share::ObStorageHADiagInfo &info);
  static int construct_diagnose_info_key(
      const share::ObTransferTaskID task_id,
      const share::ObStorageHADiagModule module,
      const share::ObStorageHADiagTaskType type,
      const share::ObStorageHADiagType diag_type,
      const int64_t retry_id,
      const common::ObTabletID &tablet_id,
      share::ObStorageHADiagTaskKey &key);
  static int construct_error_diagnose_info(share::ObTransferErrorDiagInfo &info);

  static int add_info_to_mgr(
      const ObStorageHADiagInfo &info,
      const ObStorageHADiagTaskKey &key,
      const bool is_report);

  static int append_perf_diagnose_info(
    const common::ObTabletID &tablet_id,
    share::ObStorageHADiagInfo &info);

  static int append_error_diagnose_info(
    const common::ObTabletID &tablet_id,
    const share::ObStorageHACostItemName result_msg,
    share::ObStorageHADiagInfo &info);

  static int add_transfer_error_diagnose_info(
      const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
      const share::ObStorageHADiagTaskType type, const int64_t retry_id,
      const int result_code, const share::ObStorageHACostItemName result_msg);
  static int add_transfer_error_diagnose_info(
      const share::ObTransferTaskID task_id, const share::ObLSID &ls_id,
      const share::ObStorageHADiagTaskType type, const int64_t retry_id,
      const int result_code, const common::ObTabletID &tablet_id, const share::ObStorageHACostItemName result_msg);
  static int construct_perf_diagnose_info(
      const int64_t tablet_count,
      const int64_t start_timestamp,
      share::ObTransferPerfDiagInfo &info);
  static int add_transfer_perf_diagnose_info(
    const share::ObStorageHADiagTaskKey &key, const int64_t start_timestamp,
    const int64_t tablet_count, const bool is_report, share::ObTransferPerfDiagInfo &info);
  int report_task(const share::ObTransferTaskID &task_id);
  int add_task_key_to_report_array_(
    const share::ObTransferTaskID &task_id,
    ObIArray<ObStorageHADiagTaskKey> &task_keys) const;
  int add_key_to_service_(const ObIArray<ObStorageHADiagTaskKey> &task_keys);

public:
  static const int64_t INFO_PAGE_SIZE = (1 << 16); // 64KB
  static const int64_t INFO_PAGE_SIZE_LIMIT = (1 << 11); // 2KB
  static const int64_t INFO_MAX_SIZE = 16LL * 1024LL * 1024LL; // 16MB // lowest

private:
  int create_task_(
      const ObStorageHADiagTaskKey &task_key,
      const ObStorageHADiagInfo &input_info,
      ObStorageHADiagTask *&task);
  int purge_task_(ObStorageHADiagTask *task);

  int add_task_(
      const ObStorageHADiagTaskKey &task_key,
      const ObStorageHADiagInfo &input_info,
      ObStorageHADiagTask *&out_task);
  void clear_with_no_lock_();
  int get_task_(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo *&info) const;
  int copy_with_nolock_(const ObStorageHADiagTaskKey &key, ObStorageHADiagInfo &info) const;
  void free_task_(ObStorageHADiagTask *&task);
  void free_info_(ObStorageHADiagInfo *&val);
  void free_(ObStorageHADiagTask *&task);

private:
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  ObDList<ObStorageHADiagTask> task_list_;
  ObStorageHADiagService *service_;
  SpinRWLock lock_;

  DISALLOW_COPY_AND_ASSIGN(ObStorageHADiagMgr);
};

}
}
#endif
