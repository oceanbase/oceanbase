/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEABASE_OBSERVER_OMT_OB_WORKER_PROCESSOR_H_
#define _OCEABASE_OBSERVER_OMT_OB_WORKER_PROCESSOR_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{

namespace rpc { class ObRequest; } // end of namespace rpc
namespace rpc { namespace frame { class ObReqTranslator; } }

namespace omt
{
class ObWorkerProcessor
{
public:
  ObWorkerProcessor(rpc::frame::ObReqTranslator &xlator,
                    const common::ObAddr &myaddr);

  virtual void th_created();
  virtual void th_destroy();

  virtual int process(rpc::ObRequest &req);

public:
  int process_err_test();
private:
  int process_one(rpc::ObRequest &req);

private:
  rpc::frame::ObReqTranslator &translator_;
  const common::ObAddr &myaddr_;
}; // end of class ObWorkerProcessor

} // end of namespace omt
} // end of namespace oceanbase


#endif /* _OCEABASE_OBSERVER_OMT_OB_WORKER_PROCESSOR_H_ */
