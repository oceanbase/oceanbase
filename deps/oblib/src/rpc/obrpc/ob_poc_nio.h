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

#ifndef OCEANBASE_OBRPC_OB_POC_NIO_H_
#define OCEANBASE_OBRPC_OB_POC_NIO_H_
#include "rpc/obrpc/ob_nio_interface.h"

namespace oceanbase
{
namespace obrpc
{
class ObPocNio: public ObINio
{
public:
  ObPocNio() {}
  virtual ~ObPocNio() {}
  int post(const common::ObAddr& addr, const char* req, int64_t req_size,  IRespHandler* resp_handler) override;
  int resp(int64_t resp_id, char* buf, int64_t sz) override;
private:
  int do_work(int tid);
};

extern ObPocNio global_poc_nio_;
}; // end namespace obrpc
}; // end namespace oceanbase

#endif /* OCEANBASE_OBRPC_OB_POC_NIO_H_ */
