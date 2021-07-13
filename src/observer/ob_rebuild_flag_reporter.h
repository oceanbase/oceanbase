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

#ifndef OCEANBASE_OBSERVER_OB_REBUILD_FLAG_REPORTER_H_
#define OCEANBASE_OBSERVER_OB_REBUILD_FLAG_REPORTER_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/net/ob_addr.h"
#include "common/ob_partition_key.h"
#include "share/schema/ob_schema_struct.h"
#include "storage/ob_i_partition_report.h"

namespace oceanbase {
namespace observer {
class ObRebuildFlagReporter : public common::ObDLinkBase<ObRebuildFlagReporter> {
public:
  ObRebuildFlagReporter();
  virtual ~ObRebuildFlagReporter();

  int init(const common::ObPartitionKey& part_key, const common::ObAddr& server,
      const storage::ObRebuildSwitch& rebuild_flag);

  bool is_inited() const
  {
    return inited_;
  }

  bool is_valid() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObRebuildFlagReporter& other) const;
  virtual bool operator!=(const ObRebuildFlagReporter& other) const
  {
    return !(*this == other);
  }
  virtual bool compare_without_version(const ObRebuildFlagReporter& other) const
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

  TO_STRING_KV(K_(part_key), K_(server), K_(rebuild_flag));

private:
  int do_process();

private:
  bool inited_;
  common::ObPartitionKey part_key_;
  common::ObAddr server_;
  storage::ObRebuildSwitch rebuild_flag_;
};

class ObRebuildFlagUpdater {
public:
  ObRebuildFlagUpdater()
  {}
  ~ObRebuildFlagUpdater()
  {}
  int process_barrier(ObRebuildFlagReporter& task, bool& stopped)
  {
    return task.process(stopped);
  }
  int batch_process_tasks(common::ObIArray<ObRebuildFlagReporter>& tasks, bool& stopped)
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

#endif  // OCEANBASE_OBSERVER_OB_REBUILD_FLAG_REPORTER_H_
