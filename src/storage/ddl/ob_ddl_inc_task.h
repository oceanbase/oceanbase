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

#pragma once

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_tx_seq.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace storage
{
class ObStorageSchema;
class ObDDLKVHandle;
class ObDDLKV;
class ObSSTableArray;
struct ObDDLWriteStat;

class ObDDLIncStartTask final : public share::ObITask
{
public:
  ObDDLIncStartTask(const int64_t tablet_idx);
  int process() override;

private:
  int generate_next_task(ObITask *&next_task) override;
  int record_inc_major_start_info_to_mds(
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const transaction::ObTransID &trans_id,
      const int64_t data_format_version,
      const int64_t snapshot_version,
      const share::SCN start_scn,
      common::ObIAllocator &allocator);
private:
  int64_t tablet_idx_;
};

class ObDDLIncCommitTask final : public share::ObITask
{
public:
  ObDDLIncCommitTask(const int64_t tablet_idx);
  ObDDLIncCommitTask(const ObTabletID &tablet_id);
  int process() override;

private:
  int generate_next_task(ObITask *&next_task) override;
  int record_inc_major_commit_info_to_mds(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const transaction::ObTransID &trans_id,
      const share::SCN &commit_scn,
      const ObDDLWriteStat &write_stat);

private:
  int64_t tablet_idx_;
  ObTabletID tablet_id_;
};

class ObDDLIncWaitDumpTask final : public share::ObITask
{
public:
  ObDDLIncWaitDumpTask(const share::ObLSID &ls_id,
                       const ObTabletID &tablet_id,
                       const transaction::ObTransID &trans_id,
                       const transaction::ObTxSEQ &seq_no);
  int process() override;
private:
  void schedule_ddl_merge_dag(ObDDLKVHandle &first_ddl_kv);
private:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
};

class ObDDLIncPrepareTask final : public share::ObITask
{
public:
  ObDDLIncPrepareTask();
  int process() override;
private:
  int process_one_tablet(const share::ObLSID &ls_id,
                         const ObTabletID &tablet_id);
  int search_sstables(const ObSSTableArray &sstables);
  int search_ddlkvs(const ObIArray<ObDDLKV *> &ddl_kvs);
  int record_trans_id(const transaction::ObTransID &trans_id);
private:
  int64_t tablet_idx_;
  bool is_prepared_;
  hash::ObHashSet<transaction::ObTransID> trans_ids_;
};

} // namespace storage
} // namespace oceanbase