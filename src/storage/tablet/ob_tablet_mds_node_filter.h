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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_NODE_FILTER
#define OCEANBASE_STORAGE_OB_TABLET_MDS_NODE_FILTER

#include "lib/ob_errno.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_range.h"

namespace oceanbase
{
namespace storage
{

struct ObMdsReadInfoCollector final
{
public:
  ObMdsReadInfoCollector()
    : exist_uncommitted_node_(false),
      exist_new_committed_node_(false)
  {}
  void reset()
  {
    exist_uncommitted_node_ = false;
    exist_new_committed_node_ = false;
  }
  void add(const ObMdsReadInfoCollector &input)
  {
    if (!exist_uncommitted_node_ && input.exist_uncommitted_node_) {
      exist_uncommitted_node_ = true;
    }
    if (!exist_new_committed_node_ && input.exist_new_committed_node_) {
      exist_new_committed_node_ = true;
    }
  }
  bool exist_new_node() const
  {
    return exist_uncommitted_node_ || exist_new_committed_node_;
  }
  TO_STRING_KV(K_(exist_uncommitted_node), K_(exist_new_committed_node));
  bool exist_uncommitted_node_;
  bool exist_new_committed_node_;
};

template <typename Key, typename Value>
class ObTabletMdsNodeFilter{
public:
  explicit ObTabletMdsNodeFilter(
    const common::ObVersionRange &read_version_range,
    ObMdsReadInfoCollector *collector)
    : read_version_range_(read_version_range),
      collector_(collector)
  {
    collector_->reset();
  }
  int assign(const ObTabletMdsNodeFilter &other)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!other.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      MDS_LOG(WARN, "invalid argument", KR(ret), K(other));
    } else {
      read_version_range_ = other.read_version_range_;
      collector_ = other.collector_;
    }
    return ret;
  }
  bool is_valid() const { return read_version_range_.is_valid() && nullptr != collector_; }
  int operator()(mds::UserMdsNode<Key, Value> &node,
                 bool &need_skip) {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(collector_)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "invalid collector", KR(ret), KP(collector_));
    } else if (!node.is_decided_()) {
      if (node.get_prepare_version_().get_val_for_tx() <= read_version_range_.snapshot_version_) {
        ret = OB_EAGAIN;// release row latch and see if status is decided
      } else {
        collector_->exist_uncommitted_node_ = true;
        need_skip = true;
      }
    } else if (node.is_committed_()) {
      if (node.get_commit_version_().get_val_for_tx() > read_version_range_.snapshot_version_) {
        collector_->exist_new_committed_node_ = true;
        need_skip = true;
      } else if (node.get_commit_version_().get_val_for_tx() <= read_version_range_.base_version_) {
        need_skip = true;
      } else {
        need_skip = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "aborted node should not seen", KR(ret), K(node));
    }
    MDS_LOG(TRACE, "use filter to filter mds row", KR(ret), KPC_(collector), K(node), K_(read_version_range));
    return ret;
  }
  TO_STRING_KV(K_(read_version_range), KPC_(collector));
private:
  common::ObVersionRange read_version_range_;
  ObMdsReadInfoCollector *collector_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_NODE_FILTER
