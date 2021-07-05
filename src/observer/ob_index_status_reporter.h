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

#ifndef OCEANBASE_OBSERVER_OB_INDEX_STATUS_REPORTER_H_
#define OCEANBASE_OBSERVER_OB_INDEX_STATUS_REPORTER_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/net/ob_addr.h"
#include "common/ob_partition_key.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace observer {
// report local index build status to __all_local_index_status
class ObIndexStatusReporter : public common::ObDLinkBase<ObIndexStatusReporter> {
public:
  const static int64_t RETRY_INTERVAL_US = 2l * 1000 * 1000;  // 2 seconds.

  ObIndexStatusReporter();
  virtual ~ObIndexStatusReporter();

  int init(const common::ObPartitionKey& part_key, const common::ObAddr& self, const uint64_t index_table_id,
      const share::schema::ObIndexStatus index_status, const int ret_code, common::ObMySQLProxy& sql_proxy);

  bool is_inited() const
  {
    return inited_;
  }

  bool is_valid() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObIndexStatusReporter& other) const;
  virtual bool operator!=(const ObIndexStatusReporter& other) const
  {
    return !(*this == other);
  }
  virtual bool compare_without_version(const ObIndexStatusReporter& other) const
  {
    return (*this == other);
  }
  virtual int process(const volatile bool& stop);
  uint64_t get_group_id() const
  {
    return part_key_.get_tenant_id();
  }
  inline bool is_barrier() const
  {
    return false;
  }
  inline bool need_process_alone() const
  {
    return false;
  }

  TO_STRING_KV(K_(part_key), K_(self), K_(index_table_id), K_(index_status), K_(ret_code));

private:
  int do_process();

private:
  bool inited_;
  common::ObPartitionKey part_key_;
  common::ObAddr self_;
  uint64_t index_table_id_;
  share::schema::ObIndexStatus index_status_;  // index status while build index
  int ret_code_;

  common::ObMySQLProxy* sql_proxy_;
};
class ObIndexStatusUpdater {
public:
  ObIndexStatusUpdater()
  {}
  ~ObIndexStatusUpdater()
  {}
  int process_barrier(ObIndexStatusReporter& task, bool& stopped)
  {
    return task.process(stopped);
  }
  int batch_process_tasks(common::ObIArray<ObIndexStatusReporter>& tasks, bool& stopped)
  {
    int ret = common::OB_SUCCESS;
    FOREACH_CNT_X(task, tasks, OB_SUCC(ret))
    {
      if (OB_ISNULL(task)) {
        ret = common::OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get invalid task", K(ret), K(task));
      } else if (OB_FAIL(task->process(stopped))) {
        SERVER_LOG(WARN, "fail to process task", K(ret), K(*task));
      }
    }
    return ret;
  }
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_INDEX_STATUS_REPORTER_H_
