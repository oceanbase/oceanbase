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

#define USING_LOG_PREFIX PL

#include "pl/external_routine/ob_java_sandbox_frame.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"

#include <algorithm>
#include <climits>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <arpa/inet.h>
#include <string.h>

namespace oceanbase
{
namespace pl
{

void ObJavaSandboxFrameHeader::encode(char *buf) const
{
  uint16_t net_magic = htons(magic_);
  memcpy(buf, &net_magic, 2);
  buf[2] = version_;
  uint32_t net_req_id = htonl(req_id_);
  memcpy(buf + 3, &net_req_id, 4);
  buf[7] = msg_type_;
  uint32_t net_payload_len = htonl(payload_len_);
  memcpy(buf + 8, &net_payload_len, 4);
}

int ObJavaSandboxFrameHeader::decode(const char *buf, int64_t len, ObJavaSandboxFrameHeader &header)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || len < HEADER_SIZE) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    uint16_t net_magic;
    memcpy(&net_magic, buf, 2);
    header.magic_ = ntohs(net_magic);
    header.version_ = static_cast<uint8_t>(buf[2]);
    uint32_t net_req_id;
    memcpy(&net_req_id, buf + 3, 4);
    header.req_id_ = ntohl(net_req_id);
    header.msg_type_ = static_cast<uint8_t>(buf[7]);
    uint32_t net_payload_len;
    memcpy(&net_payload_len, buf + 8, 4);
    header.payload_len_ = ntohl(net_payload_len);

    if (header.magic_ != MAGIC) {
      ret = common::OB_INVALID_DATA;
      LOG_WARN( "java sandbox frame: invalid magic", K(ret), K(header.magic_));
    } else if (header.payload_len_ > MAX_PAYLOAD_SIZE) {
      ret = common::OB_SIZE_OVERFLOW;
      LOG_WARN( "java sandbox frame: payload too large", K(ret), K(header.payload_len_));
    }
  }
  return ret;
}

static int write_n(int fd, const char *buf, int64_t len)
{
  int ret = common::OB_SUCCESS;
  int64_t written = 0;
  while (OB_SUCC(ret) && written < len) {
    ssize_t n = ::write(fd, buf + written, len - written);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      ret = common::OB_IO_ERROR;
    } else {
      written += n;
    }
  }
  return ret;
}

static int wait_readable(int fd, int64_t deadline_ts)
{
  int ret = common::OB_SUCCESS;
  bool ready = false;
  while (OB_SUCC(ret) && !ready) {
    const int64_t remaining_us = deadline_ts - common::ObTimeUtility::current_time();
    if (remaining_us <= 0) {
      ret = common::OB_TIMEOUT;
    } else {
      struct pollfd pfd;
      pfd.fd = fd;
      pfd.events = POLLIN;
      pfd.revents = 0;
      const int timeout_ms = static_cast<int>(
          std::min((remaining_us + 999) / 1000, static_cast<int64_t>(INT_MAX)));
      const int poll_ret = ::poll(&pfd, 1, timeout_ms);
      if (poll_ret < 0 && EINTR == errno) {
        // Recompute the remaining time before retrying.
      } else if (0 == poll_ret) {
        ret = common::OB_TIMEOUT;
      } else if (poll_ret < 0) {
        ret = common::OB_IO_ERROR;
      } else if (0 != (pfd.revents & POLLIN)) {
        ready = true;
      } else {
        ret = common::OB_ERR_SYS;
      }
    }
  }
  return ret;
}

static int read_n(int fd, char *buf, int64_t len, int64_t deadline_ts)
{
  int ret = common::OB_SUCCESS;
  int64_t total = 0;
  while (OB_SUCC(ret) && total < len) {
    if (deadline_ts > 0 && OB_FAIL(wait_readable(fd, deadline_ts))) {
      // The same deadline covers every partial read of the frame.
    }
    if (OB_SUCC(ret)) {
      ssize_t n = ::read(fd, buf + total, len - total);
      if (n < 0) {
        if (errno != EINTR) {
          ret = common::OB_IO_ERROR;
        }
      } else if (0 == n) {
        ret = common::OB_ERR_SYS;
      } else {
        total += n;
      }
    }
  }
  return ret;
}

int ob_java_sandbox_send_frame(int fd, uint32_t req_id, uint8_t msg_type,
                                const char *payload, int64_t payload_len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(fd < 0)) {
    ret = common::OB_NOT_INIT;
  } else if (OB_UNLIKELY(payload_len < 0 || payload_len > ObJavaSandboxFrameHeader::MAX_PAYLOAD_SIZE)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(payload_len > 0 && OB_ISNULL(payload))) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObJavaSandboxFrameHeader header;
    header.req_id_ = req_id;
    header.msg_type_ = msg_type;
    header.payload_len_ = static_cast<uint32_t>(payload_len);

    char hdr_buf[ObJavaSandboxFrameHeader::HEADER_SIZE];
    header.encode(hdr_buf);

    if (OB_FAIL(write_n(fd, hdr_buf, ObJavaSandboxFrameHeader::HEADER_SIZE))) {
      LOG_WARN( "java sandbox: send frame header failed", K(ret), K(fd));
    } else if (payload_len > 0 && OB_FAIL(write_n(fd, payload, payload_len))) {
      LOG_WARN( "java sandbox: send frame payload failed", K(ret), K(fd), K(payload_len));
    }
  }
  return ret;
}

int ob_java_sandbox_recv_frame(int fd, int64_t timeout_us,
                                ObJavaSandboxFrameHeader &header,
                                common::ObIAllocator &alloc, char *&payload)
{
  int ret = common::OB_SUCCESS;
  payload = nullptr;

  if (OB_UNLIKELY(fd < 0)) {
    ret = common::OB_NOT_INIT;
  } else {
    const int64_t deadline_ts =
        timeout_us > 0 ? common::ObTimeUtility::current_time() + timeout_us : 0;
    char hdr_buf[ObJavaSandboxFrameHeader::HEADER_SIZE];
    if (OB_FAIL(read_n(fd, hdr_buf, ObJavaSandboxFrameHeader::HEADER_SIZE, deadline_ts))) {
      LOG_WARN( "java sandbox: read frame header failed", K(ret), K(fd));
    } else if (OB_FAIL(ObJavaSandboxFrameHeader::decode(hdr_buf, ObJavaSandboxFrameHeader::HEADER_SIZE, header))) {
      LOG_WARN( "java sandbox: decode frame header failed", K(ret));
    } else if (header.payload_len_ > 0) {
      payload = static_cast<char *>(alloc.alloc(header.payload_len_));
      if (OB_ISNULL(payload)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN( "java sandbox: alloc payload buffer failed", K(ret), K(header.payload_len_));
      } else if (OB_FAIL(read_n(fd, payload, header.payload_len_, deadline_ts))) {
        LOG_WARN( "java sandbox: read frame payload failed", K(ret), K(header.payload_len_));
        alloc.free(payload);
        payload = nullptr;
      }
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
