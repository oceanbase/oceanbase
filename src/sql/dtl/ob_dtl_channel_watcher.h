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

#ifndef OB_DTL_CHANNEL_WATCHER_H
#define OB_DTL_CHANNEL_WATCHER_H

#include <cstdint>

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlChannel;

// Watcher
class ObDtlChannelWatcher
{
public:
  virtual void notify(ObDtlChannel &chan) = 0;
  virtual void remove_data_list(ObDtlChannel *chan, bool force = false) = 0;
  virtual void add_last_data_list(ObDtlChannel *ch) = 0;
  virtual void set_first_no_data(ObDtlChannel *ch) = 0;
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_CHANNEL_WATCHER_H */
