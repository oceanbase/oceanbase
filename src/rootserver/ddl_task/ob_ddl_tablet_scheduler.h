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

#ifndef OCEANBASE_ROOTSERVER_OB_DDL_TABLET_SCHEDULER_H
#define OCEANBASE_ROOTSERVER_OB_DDL_TABLET_SCHEDULER_H

#include "rootserver/ddl_task/ob_ddl_task.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLTabletScheduler final
{
public:
  ObDDLTabletScheduler();
  ~ObDDLTabletScheduler();
  int init(const uint64_t tenant_id,
           const uint64_t table_id,
           const uint64_t ref_data_table_id,
           const int64_t  task_id,
           const int64_t  parallelism,
           const int64_t  snapshot_version,
           const common::ObCurTraceId::TraceId &trace_id,
           const ObIArray<ObTabletID> &tablets);
  int get_next_batch_tablets(int64_t &parallelism, int64_t &new_execution_id, share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets);
  int confirm_batch_tablets_status(const int64_t execution_id, const bool finish_status, const share::ObLSID &ls_id, const ObIArray<ObTabletID> &tablets);
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(table_id), K_(ref_data_table_id),
              K_(task_id), K_(parallelism), K_(snapshot_version), K_(trace_id), K_(all_tablets), K_(running_task_ls_ids_before));
private:
  int get_next_parallelism(int64_t &parallelism);
  int get_running_sql_parallelism(int64_t &parallelism);
  int get_unfinished_tablets(const int64_t execution_id, share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets);
  int get_to_be_scheduled_tablets(share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets);
  int calculate_candidate_tablets(const uint64_t left_space_size, const ObIArray<ObTabletID> &in_tablets, ObIArray<ObTabletID> &out_tablets);
  int get_session_running_lsid(ObIArray<share::ObLSID> &running_ls_ids);
  int get_target_running_ls_tablets(const share::ObLSID &ls_id, ObIArray<ObTabletID> &tablets);
  int get_potential_finished_lsid(const ObIArray<share::ObLSID> &running_ls_ids_now, ObIArray<share::ObLSID> &potential_finished_ls_ids);
  int determine_if_need_to_send_new_task(bool &status);
  int check_target_ls_tasks_completion_status(const share::ObLSID &ls_id);
  bool is_all_tasks_finished();
  bool is_running_tasks_before_finished();
  int refresh_ls_location_map();
  void destroy();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t ref_data_table_id_;
  int64_t task_id_;
  int64_t parallelism_;
  int64_t snapshot_version_;
  common::ObCurTraceId::TraceId trace_id_;
  common::TCRWLock lock_; // this lock is used to protect read and write operations of class members: running_task_ls_ids_before_、 all_ls_to_tablets_map_、 running_ls_to_tablets_map_、 running_ls_to_execution_id_, to avoid conflicts between ddl_builder task and ddl_scheduler task.
  common::ObArenaAllocator allocator_;
  ObRootService *root_service_;
  ObArray<ObTabletID> all_tablets_;
  ObArray<share::ObLSID> running_task_ls_ids_before_; // this is the used lsid array where the tablets is located when init ObDDLTabletScheduler;
  common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> all_ls_to_tablets_map_;
  common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> running_ls_to_tablets_map_;
  common::hash::ObHashMap<share::ObLSID, common::ObAddr> ls_location_map_;
  common::hash::ObHashMap<share::ObLSID, int64_t> running_ls_to_execution_id_;
  common::hash::ObHashMap<int64_t, int64_t> tablet_id_to_data_size_;
  common::hash::ObHashMap<int64_t, int64_t> tablet_id_to_data_row_cnt_;
  common::hash::ObHashMap<int64_t, int64_t> tablet_scheduled_times_statistic_;
};

class ObTabletIdUpdater final
{
public:
  ObTabletIdUpdater(const ObIArray<ObTabletID> &tablets) : tablets_(&tablets) {};
  ~ObTabletIdUpdater() {};
  int operator() (common::hash::HashMapPair<share::ObLSID, ObArray<ObTabletID>> &entry);
private:
  const ObIArray<ObTabletID> *tablets_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletIdUpdater);
};

class HashMapEraseIfNull final
{
public:
  HashMapEraseIfNull() {};
  ~HashMapEraseIfNull() {};
  bool operator() (common::hash::HashMapPair<share::ObLSID, ObArray<ObTabletID>> &entry);
private:
  DISALLOW_COPY_AND_ASSIGN(HashMapEraseIfNull);
};
} // end namespace rootserver
} // end namespace oceanbase

#endif /* OCEANBASE_ROOTSERVER_OB_DDL_TABLET_SCHEDULER_H */