#ifndef OCEANBASE_SHARE_OB_SANDBOX_PROTOCOL_H
#define OCEANBASE_SHARE_OB_SANDBOX_PROTOCOL_H

#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/string/ob_string.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#define OB_SANDBOX_BINARY_PATH "./bin/ob_sandbox"
#define OB_SANDBOX_ROOT_PATH "run/ob_sandbox/"

namespace oceanbase {
namespace observer {

const uint32_t SANDBOX_MAGIC = 0x53424F58; // "SBOX"

enum SandboxMsgType {
  SANDBOX_MSG_UNKNOWN = 0,
  SANDBOX_MSG_CREATE = 1,
  SANDBOX_MSG_DESTROY = 2,
  SANDBOX_MSG_CHECK = 3,
  SANDBOX_MSG_RESPONSE = 4,
  SANDBOX_MSG_MAX
};

// Fixed size header for wire protocol
struct SandboxMsgHeader {
  uint32_t magic_;
  uint32_t type_;
  uint32_t data_len_;
  uint32_t reserved_;
  uint64_t trace_id_[4];
  SandboxMsgHeader() : magic_(SANDBOX_MAGIC), type_(SANDBOX_MSG_UNKNOWN), data_len_(0), reserved_(0)
  {
    trace_id_[0] = 0;
    trace_id_[1] = 0;
    trace_id_[2] = 0;
    trace_id_[3] = 0;
  }

  TO_STRING_KV(K_(magic), K_(type), K_(data_len));
};

struct SandboxMountInfo {
  common::ObString src_;
  common::ObString dest_;
  uint64_t flags_;

  SandboxMountInfo() : src_(), dest_(), flags_(0) {}

  TO_STRING_KV(K_(src), K_(dest), K_(flags));
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(SandboxMountInfo, src_, dest_, flags_);

struct SandboxMsgCreate {
  common::ObString root_path_;
  common::ObSArray<SandboxMountInfo> mount_infos_;
  common::ObString binary_path_;
  common::ObString arg_str_;
  int32_t stdin_fd_;
  int32_t stdout_fd_;
  bool enable_net_;  // whether to enable network, default is true

  SandboxMsgCreate() : root_path_(), mount_infos_(), binary_path_(), arg_str_(), stdin_fd_(-1), stdout_fd_(-1), enable_net_(false) {}

  TO_STRING_KV(K_(root_path), K_(mount_infos), K_(binary_path), K_(arg_str), K_(stdin_fd), K_(stdout_fd), K_(enable_net));
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(SandboxMsgCreate, root_path_, mount_infos_, binary_path_, arg_str_, stdin_fd_, stdout_fd_, enable_net_);

struct SandboxMsgDestroy {
  int32_t pid_;
  SandboxMsgDestroy() : pid_(-1) {}

  TO_STRING_KV(K_(pid));
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(SandboxMsgDestroy, pid_);

struct SandboxMsgCheck {
  int32_t pid_;

  SandboxMsgCheck() : pid_(-1) {}

  TO_STRING_KV(K_(pid));
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(SandboxMsgCheck, pid_);

struct SandboxResponse {
  int32_t ret_code_;
  int32_t pid_;
  int32_t status_;
  int32_t is_running_;

  SandboxResponse() : ret_code_(-1), pid_(-1), status_(0), is_running_(0) {}

  TO_STRING_KV(K_(ret_code), K_(pid), K_(status), K_(is_running));
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(SandboxResponse, ret_code_, pid_, status_, is_running_);

class ObSandboxProtocolHelper {
public:
  static int read_n(int fd, char* buf, int64_t len) {
    int ret = 0;
    int64_t total = 0;
    while (total < len && ret == 0) {
      ssize_t r = read(fd, buf + total, len - total);
      if (r <= 0) {
        if (r < 0 && errno == EINTR) {
          // continue
        } else {
          ret = -1;
        }
      } else {
        total += r;
      }
    }
    return ret;
  }

  static const int64_t MAX_SANDBOX_MSG_BUF_SIZE = 4096;

  template <typename T>
  static int send_request(int fd, SandboxMsgType type, const T& msg) {
    int ret = common::OB_SUCCESS;
    SandboxMsgHeader header;
    header.magic_ = SANDBOX_MAGIC;
    header.type_ = type;
    const uint64_t* trace_id = common::ObCurTraceId::get();
    if (trace_id != NULL) {
      memcpy(header.trace_id_, trace_id, sizeof(header.trace_id_));
    }

    int64_t ser_size = msg.get_serialize_size();
    int64_t total_len = sizeof(header) + ser_size;
    char buf[MAX_SANDBOX_MSG_BUF_SIZE];

    if (ser_size < 0 || total_len > MAX_SANDBOX_MSG_BUF_SIZE) {
      ret = common::OB_SIZE_OVERFLOW;
    } else {
      header.data_len_ = static_cast<uint32_t>(ser_size);
      memcpy(buf, &header, sizeof(header));

      int64_t pos = sizeof(header);
      if (OB_FAIL(msg.serialize(buf, total_len, pos))) {
         // log
      } else {
        if (read_n_write(fd, buf, total_len) < 0) {
          ret = common::OB_IO_ERROR;
        }
      }
    }
    return ret;
  }

  static int send_response(int fd, const SandboxResponse& resp) {
    return send_request(fd, SANDBOX_MSG_RESPONSE, resp);
  }

  static int recv_response(int fd, SandboxResponse& resp) {
    int ret = common::OB_SUCCESS;
    SandboxMsgHeader header;

    if (read_n(fd, (char*)&header, sizeof(header)) < 0) {
      ret = common::OB_IO_ERROR;
    } else {
      if (header.magic_ != SANDBOX_MAGIC || header.type_ != SANDBOX_MSG_RESPONSE) {
        ret = common::OB_INVALID_DATA;
      } else {
        if (header.data_len_ > 0) {
          if (header.data_len_ > MAX_SANDBOX_MSG_BUF_SIZE) {
            ret = common::OB_SIZE_OVERFLOW;
          } else {
            char buf[MAX_SANDBOX_MSG_BUF_SIZE];
            if (read_n(fd, buf, header.data_len_) < 0) {
              ret = common::OB_IO_ERROR;
            } else {
              int64_t pos = 0;
              if (OB_FAIL(resp.deserialize(buf, header.data_len_, pos))) {
                // ret = ...
              }
            }
          }
        }
      }
    }
    return ret;
  }

  // Helper to write N bytes
  static int read_n_write(int fd, const char* buf, int64_t len) {
    int ret = 0;
    int64_t total = 0;
    while (total < len && ret == 0) {
      ssize_t r = write(fd, buf + total, len - total);
      if (r <= 0) {
        if (r < 0 && errno == EINTR) {
          // continue
        } else {
          ret = -1;
        }
      } else {
        total += r;
      }
    }
    return ret;
  }
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SANDBOX_PROTOCOL_H
