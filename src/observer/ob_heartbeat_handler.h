/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_OBSERVER_OB_HEARTBEAT_HANDLER_H_
#define OCEANBASE_OBSERVER_OB_HEARTBEAT_HANDLER_H_
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
namespace oceanbase
{
namespace share
{
  struct ObHBRequest;
  struct ObHBResponse;
}
namespace observer
{
// currently, server health status only covers data disk status.
struct ObServerHealthStatus
{
  OB_UNIS_VERSION(1);
public:
  enum ObDataDiskStatus
  {
    DATA_DISK_STATUS_INVALID = 0,
    DATA_DISK_STATUS_NORMAL = 1,
    DATA_DISK_STATUS_ERROR = 2,
    DATA_DISK_STATUS_MAX =  3
  };
  explicit ObServerHealthStatus();
  virtual ~ObServerHealthStatus();
  int init(ObDataDiskStatus data_disk_status);
  int assign(const ObServerHealthStatus server_health_status);
  void reset();
  bool is_valid() const;
  bool is_healthy() const;
  static const char *data_disk_status_to_str(const ObDataDiskStatus data_disk_status);
  inline bool operator ==(const ObServerHealthStatus &other) const
  {
    return data_disk_status_ == other.data_disk_status_;
  }
  inline bool operator !=(const ObServerHealthStatus &other) const
  {
    return data_disk_status_ != other.data_disk_status_;
  }
  TO_STRING_KV(K(data_disk_status_), "data_disk_status", data_disk_status_to_str(data_disk_status_));
private:
  ObDataDiskStatus data_disk_status_;
};
class ObHeartbeatHandler
{
public:
  explicit ObHeartbeatHandler();
  virtual ~ObHeartbeatHandler();
  static int handle_heartbeat(
      const share::ObHBRequest &hb_request,
      share::ObHBResponse &hb_response);
  static bool is_rs_epoch_id_valid();
private:
  static int check_disk_status_(ObServerHealthStatus &server_health_status);
  static int init_hb_response_(share::ObHBResponse &hb_response);
  static int64_t rs_epoch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHeartbeatHandler);
};
} // observer
} // oceanbase
#endif