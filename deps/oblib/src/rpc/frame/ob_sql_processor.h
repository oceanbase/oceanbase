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

#ifndef OCEANBASE_FRAME_OB_SQL_PROCESSOR_H_
#define OCEANBASE_FRAME_OB_SQL_PROCESSOR_H_

#include "rpc/frame/ob_req_processor.h"

namespace oceanbase
{

namespace rpc
{

namespace frame
{
class ObSqlProcessor: public ObReqProcessor
{
public:
  ObSqlProcessor() {}
  virtual ~ObSqlProcessor() {}

  int run();
  virtual common::ObAddr get_peer() const;
  virtual int process() = 0;
protected:
  virtual int setup_packet_sender() = 0;
  virtual int deserialize() = 0;

  virtual int before_process() = 0;
  virtual int after_process(int error_code) = 0;

  virtual void cleanup() = 0;

  virtual int response(const int retcode) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlProcessor);
};
} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* OCEANBASE_FRAME_OB_SQL_PROCESSOR_H_ */
