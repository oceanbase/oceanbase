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

#ifndef OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_
#define OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace pl
{

enum ObJavaSandboxMsgType : uint8_t {
  JAVA_SANDBOX_EXECUTE_REQ         = 0x01,
  JAVA_SANDBOX_EXECUTE_RESP        = 0x02,
  JAVA_SANDBOX_LOAD_JAR            = 0x03,
  JAVA_SANDBOX_LOAD_JAR_RESP       = 0x04,
  JAVA_SANDBOX_FETCH_CLASS_REQ     = 0x05,
  JAVA_SANDBOX_FETCH_CLASS_RESP    = 0x06,
  JAVA_SANDBOX_CHECK_OBSOLETE_REQ  = 0x07,
  JAVA_SANDBOX_CHECK_OBSOLETE_RESP = 0x08,
  JAVA_SANDBOX_DESTROY_SESSION     = 0x09,
  JAVA_SANDBOX_CREATE_CHANNEL      = 0x0A,
  JAVA_SANDBOX_CREATE_SESSION      = 0x0B,
  JAVA_SANDBOX_SHUTDOWN            = 0x0C,
  JAVA_SANDBOX_EVICT_STATEMENT     = 0x0D,
};

// Sandbox-internal error codes carried in the error payload of *_RESP frames
// (status byte = 1). These are NOT OB kernel error codes -- the proxy maps them:
//   SANDBOX_ERR_JAVA_TIMEOUT   -> OB_TIMEOUT
//   SANDBOX_ERR_JAVA_EXCEPTION -> OB_JNI_JAVA_EXCEPTION_ERROR
//   SANDBOX_ERR_JAVA_TIMEOUT_STUCK -> OB_TIMEOUT; no JVM rollover
// Values are positive and deliberately outside the OB kernel errno range (all
// OB errnos are <= 0) to avoid ambiguity. Must match the SDK's MsgType constants.
constexpr int32_t SANDBOX_ERR_JAVA_EXCEPTION = 1;
constexpr int32_t SANDBOX_ERR_JAVA_TIMEOUT   = 2;
constexpr int32_t SANDBOX_ERR_JAVA_TIMEOUT_STUCK = 3;

// ---- FETCH_CLASS wire format. Must match the SDK's ObSandboxSessionClassLoader. ----
//
// REQ payload:  session_id(8B) + hint_jar_id(8B) + class_name(var, UTF-8, '/' separated)
// RESP payload: class_id(8B) + jar_id(8B) + kind(1B) + data(var)
//
// class_id identifies the version of the class name itself and drives CHECK_OBSOLETE;
// jar_id identifies the jar that owns it and is what the sandbox keys its class cache on.
// Both are external resource ids, and a fresh one is allocated on every loadjar, but they
// are distinct ids and must not be conflated.
//
// hint_jar_id is the jar the sandbox believes owns class_name, or 0 when it has no guess.
// It is purely a transfer optimization: the observer always resolves the owning jar itself
// and answers with the authoritative jar_id, so a stale or wrong hint costs one extra jar
// transfer and can never yield the wrong class.
constexpr int64_t JAVA_SANDBOX_FETCH_CLASS_REQ_HEADER_LEN = 16;
constexpr int64_t JAVA_SANDBOX_FETCH_CLASS_RESP_HEADER_LEN = 17;
// data = whole jar binary
constexpr uint8_t JAVA_SANDBOX_FETCH_CLASS_FULL_JAR = 1;
// hint matched, data empty, sandbox reuses the copy it already cached under jar_id
constexpr uint8_t JAVA_SANDBOX_FETCH_CLASS_JAR_CACHED = 2;
// no such class, data empty, class_id and jar_id are 0
constexpr uint8_t JAVA_SANDBOX_FETCH_CLASS_NOT_FOUND = 3;

struct ObJavaSandboxFrameHeader {
  static constexpr uint16_t MAGIC = 0x4A55;  // "JU"
  static constexpr uint8_t VERSION = 0x01;
  static constexpr int64_t HEADER_SIZE = 12;
  static constexpr int64_t MAX_PAYLOAD_SIZE = 32 * 1024 * 1024;  // 32MB

  uint16_t magic_;
  uint8_t version_;
  uint32_t req_id_;
  uint8_t msg_type_;
  uint32_t payload_len_;

  ObJavaSandboxFrameHeader()
    : magic_(MAGIC), version_(VERSION), req_id_(0), msg_type_(0), payload_len_(0) {}

  void encode(char *buf) const;
  static int decode(const char *buf, int64_t len, ObJavaSandboxFrameHeader &header);
};

// Write one frame (header + payload) to fd, handling partial writes and EINTR.
int ob_java_sandbox_send_frame(int fd, uint32_t req_id, uint8_t msg_type,
                                const char *payload, int64_t payload_len);

// Read one frame from fd with timeout. Allocates payload buffer from alloc.
// payload is set to nullptr if payload_len is 0.
int ob_java_sandbox_recv_frame(int fd, int64_t timeout_us,
                                ObJavaSandboxFrameHeader &header,
                                common::ObIAllocator &alloc, char *&payload);

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_
