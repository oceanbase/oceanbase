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
