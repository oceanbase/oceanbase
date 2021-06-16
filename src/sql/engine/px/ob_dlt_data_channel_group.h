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

#ifndef __OB_SQL_DTL_DATA_CHANNEL_GROUP_H__
#define __OB_SQL_DTL_DATA_CHANNEL_GROUP_H__
namespace oceanbase {
namespace sql {
class ObDTLDataChannelGroup {
public:
  ObDTLDataChannelGroup();
  virtual ~ObDTLDataChannelGroup();
  int wait(int64_t timeout_ts);
  int notify();
  int add_channel_info(ObDtlChannelInfo& producer, common::ObIArray<ObDtlChannelInfo>& consumers);

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObDTLDataChannelGroup);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_DTL_DATA_CHANNEL_GROUP_H__ */
//// end of header file
