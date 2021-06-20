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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_REBALANCE_TASK_STAT_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_REBALANCE_TASK_STAT_H_

#include "share/ob_virtual_table_projector.h"
#include "rootserver/ob_rebalance_task_mgr.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObRebalanceTask;
class ObRebalanceTaskMgr;

class ObAllRebalanceTaskStat : public common::ObVirtualTableProjector {
public:
  ObAllRebalanceTaskStat();
  virtual ~ObAllRebalanceTaskStat();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObRebalanceTaskMgr& rebalance_task_mgr);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  struct Display {
    void reset();
    uint64_t tenant_id_;
    uint64_t table_id_;
    int64_t partition_id_;
    int64_t partition_cnt_;
    char* src_;
    char* data_src_;
    char* dest_;
    char* offline_;
    char* task_type_;
    char* is_replicate_;
    char* is_scheduled_;
    int64_t waiting_time_;
    int64_t executing_time_;
    char* is_manual_;
    TO_STRING_KV(K_(table_id), K_(partition_id), K_(partition_cnt), K_(src), K_(data_src), K_(dest), K_(offline),
        K_(task_type), K_(is_replicate), K_(is_scheduled), K_(waiting_time), K_(executing_time), K_(is_manual));
  };

  int generate_task_stat(const ObRebalanceTask& task_stat, const ObRebalanceTaskInfo& task_info, Display& display);
  int get_full_row(const share::schema::ObTableSchema* table, const ObRebalanceTask& task_stat,
      const ObRebalanceTaskInfo& task_info, common::ObIArray<Column>& columns);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObRebalanceTaskMgr* rebalance_task_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllRebalanceTaskStat);
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif
