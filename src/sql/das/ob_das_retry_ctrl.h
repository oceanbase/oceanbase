/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef DEV_SRC_SQL_DAS_OB_DAS_RETRY_CTRL_H_
#define DEV_SRC_SQL_DAS_OB_DAS_RETRY_CTRL_H_

namespace oceanbase {
namespace sql {
class ObIDASTaskOp;
class ObDASRef;
class ObDASRetryCtrl
{
public:
  /**
   * retry_func: is to determine whether tablet-level retry is necessary based on the status of the task
   *             and maybe change some status of the das task in this function,
   *             such as refresh_partition_location_cache,
   *             DAS retry only can be attempted within the current thread
   * [param in]: ObDASRef &: the das context reference
   * [param in/out] ObIDASTaskOp &: which das task need to retry
   * [param out] bool &: whether need DAS retry
   * */
  typedef void (*retry_func)(ObDASRef &, ObIDASTaskOp &, bool &);

  static void tablet_location_retry_proc(ObDASRef &, ObIDASTaskOp &, bool &);
  static void tablet_nothing_readable_proc(ObDASRef &, ObIDASTaskOp &, bool &);
  static void task_network_retry_proc(ObDASRef &, ObIDASTaskOp &, bool &);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* DEV_SRC_SQL_DAS_OB_DAS_RETRY_CTRL_H_ */
