/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_FETCHER_ERR_HANDLER_H_
#define OCEANBASE_LOG_FETCHER_ERR_HANDLER_H_

#include "share/ob_define.h"
#include <stdint.h>
#include "src/logservice/palf/lsn.h"

namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace logfetcher
{
// interface for error handler
class IObLogErrHandler
{
public:
  enum class ErrType
  {
    FETCH_LOG,
    SUBMIT_LOG,
  };
  virtual ~IObLogErrHandler() {}

public:
  virtual void handle_error(const int err_no, const char *fmt, ...) = 0;
  virtual void handle_error(const share::ObLSID &ls_id,
      const ErrType &err_type,
      share::ObTaskId &trace_id,
      const palf::LSN &lsn,
      const int err_no,
      const char *fmt, ...) = 0;
};

} // namespace logfetcher
} // namespace oceanbase

#endif
