/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
