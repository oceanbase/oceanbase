/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
