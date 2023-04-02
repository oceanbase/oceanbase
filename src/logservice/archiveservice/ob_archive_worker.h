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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_WORKER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_WORKER_H_
namespace oceanbase
{
namespace archive
{
class ObArchiveTaskStatus;
class ObArchiveWorker
{
public:
  ObArchiveWorker() {}
  virtual ~ObArchiveWorker() {}

  virtual int push_task_status(ObArchiveTaskStatus *task_status) = 0;
};
}
}

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_WORKER_H_ */
