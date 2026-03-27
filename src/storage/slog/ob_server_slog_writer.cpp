/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/slog/ob_server_slog_writer.h"

namespace oceanbase
{
namespace storage
{

int ObServerSlogWriter::start()
{
  return ObBaseLogWriter::start();
}

void ObServerSlogWriter::wait()
{
  ObBaseLogWriter::wait();
}

}
}