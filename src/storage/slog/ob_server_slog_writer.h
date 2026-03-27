/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SERVER_SLOG_WRITER
#define OCEANBASE_STORAGE_OB_SERVER_SLOG_WRITER

#include "storage/slog/ob_storage_log_writer.h"

namespace oceanbase
{
namespace storage
{

class ObServerSlogWriter : public ObStorageLogWriter
{
public:
  ObServerSlogWriter() {}
  virtual ~ObServerSlogWriter() {}
  virtual void wait() override;
protected:
  virtual int start() override;
};

}
}

#endif