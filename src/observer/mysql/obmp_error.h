/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_MYSQL_OBMP_ERROR_H_
#define OCEANBASE_OBSERVER_MYSQL_OBMP_ERROR_H_

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{

class ObMPError : public ObMPBase
{
public:
  explicit ObMPError(const int ret)
      : ObMPBase(GCTX), ret_(ret), need_disconnect_(false)
  {}
  virtual ~ObMPError() {}
  inline bool is_need_disconnect() const {return need_disconnect_;}
  inline void set_need_disconnect(bool value) {need_disconnect_ = value;}

protected:
  int deserialize()
  {
    return OB_SUCCESS;
  }
  inline int process();

private:
  int ret_;
  int need_disconnect_;
  DISALLOW_COPY_AND_ASSIGN(ObMPError);
}; // end of class ObmpError

int ObMPError::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS == ret_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(ERROR, "error packet will not be sended for ret_ is succ", K(ret));
  } else {
    if (OB_FAIL(send_error_packet(ret_, NULL))) {
      SERVER_LOG(WARN, "send error packet fail", K(ret_), K(ret));
    }
    // connect request reaching this means that no prio memory left
    if (need_disconnect_) {
      force_disconnect();
    }
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OBMP_ERROR_H_
