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

#include "rpc/obrpc/ob_poc_nio.h"
#include "lib/oblog/ob_log.h"
#include <sys/types.h>
#include <sys/socket.h>

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

static struct sockaddr_in* obaddr2sockaddr(struct sockaddr_in *sin, const ObAddr& addr)
{
  if (NULL != sin) {
    sin->sin_port = (uint16_t)htons((uint16_t)(addr.get_port() + 123));
    sin->sin_addr.s_addr = htonl(addr.get_ipv4());
    sin->sin_family = AF_INET;
  }
  return sin;
}

int ObPocNio::post(const common::ObAddr& addr, const char* req, int64_t req_size,  IRespHandler* resp_handler)
{
  int ret = 0;
  int64_t io_limit = 1<<21;
  RLOCAL(char*, resp_buf);
  struct sockaddr_in sin;
  int fd = -1;
  if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    ret = errno;
    RPC_LOG(WARN, "create socket fail", K(errno));
  } else if (connect(fd, (sockaddr*)obaddr2sockaddr(&sin, addr), sizeof(sin)) < 0) {
    ret = errno;
    RPC_LOG(WARN, "connect fail", K(addr), K(errno));
  } else if (write(fd, req, req_size) != req_size) {
    ret = errno;
    RPC_LOG(WARN, "write fail", K(req_size), K(errno));
  } else if (NULL == resp_buf && NULL == (resp_buf = (char*)malloc(io_limit))) {
    ret = ENOMEM;
    RPC_LOG(WARN, "alloc resp buffer fail", K(io_limit), K(errno));
  } else {
    shutdown(fd, SHUT_WR);
    RPC_LOG(INFO, "post succ, shutdown write side, blocking read response");
    int64_t sz = read(fd, resp_buf, io_limit);
    if (sz < 0) {
      RPC_LOG(WARN, "read resp fail", K(errno));
      sz = 0;
    }
    RPC_LOG(INFO, "receive resp finish", K(sz));
    resp_handler->handle_resp(ret, resp_buf, sz);
    RPC_LOG(INFO, "resp callback finish");
  }
  if (fd >= 0) {
    close(fd);
  }
  return ret;
}

int ObPocNio::resp(int64_t resp_id, char* buf, int64_t sz)
{
  int fd = (int)resp_id;
  if (sz > 0 && fd >= 0) {
    if (write(fd, buf, sz) < 0) {
      RPC_LOG_RET(WARN, common::OB_ERR_SYS, "write resp fail", K(errno));
    } else {
      RPC_LOG_RET(WARN, common::OB_SUCCESS, "write resp OK");
    }
  } else {
    RPC_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "resp invalid argument", KP(buf), K(sz), K(fd));
  }
  if (fd >= 0) {
    close(fd);
  }
  return 0;
}

int ObPocNio::do_work(int tid)
{
  UNUSED(tid);
  int64_t io_limit = 1<<22;
  char* io_buf = (char*)malloc(io_limit);
  while(1){
    int fd = -1;
    if (read(accept_queue_fd_, &fd, sizeof(fd)) != sizeof(fd)) {
      RPC_LOG_RET(ERROR, common::OB_ERR_SYS, "read accept queue fail");
      continue;
    } else {
      RPC_LOG(INFO, "read accept queue succ", K(fd));
    }
    int64_t sz = read(fd, io_buf, io_limit);
    if (sz < 0 || sz >= io_limit) {
      RPC_LOG_RET(WARN, common::OB_ERR_SYS, "read socket error", K(errno), K(fd));
      close(fd);
    } else {
      int ret = OB_SUCCESS;
      RPC_LOG(INFO, "read a complete request", K(fd));
      if (OB_FAIL(req_handler_->handle_req(fd, io_buf, sz))) {
        RPC_LOG(WARN, "read socket error", K(ret));
        close(fd);
      } else {
        RPC_LOG(INFO, "handle_req success", K(fd));
      }
    }
  }
}
