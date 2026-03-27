/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  static int64_t get_rs_epoch_id();
  static bool is_rs_epoch_id_valid();
  static int check_disk_status(ObServerHealthStatus &server_health_status);
private:
  static int init_hb_response_(share::ObHBResponse &hb_response);
  static int64_t rs_epoch_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHeartbeatHandler);
};
} // share
} // oceanbase
#endif