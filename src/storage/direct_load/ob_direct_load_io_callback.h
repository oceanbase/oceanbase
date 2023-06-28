// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace storage
{

class ObDirectLoadIOCallback : public common::ObIOCallback
{
public:
  ObDirectLoadIOCallback(uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  virtual ~ObDirectLoadIOCallback();
  const char *get_data() override { return data_buf_; }
  int64_t size() const override { return sizeof(*this); }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  TO_STRING_KV(K_(data_buf));
protected:
  int inner_process(const char *data_buffer, const int64_t size) override;
  int inner_deep_copy(char *buf, const int64_t buf_len,
                      ObIOCallback *&copied_callback) const override;
private:
  uint64_t tenant_id_;
  char *data_buf_;
};

}  // namespace storage
}  // namespace oceanbase
