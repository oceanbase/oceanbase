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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DUMP_MDS_NODE_OPERATOR
#define OCEANBASE_STORAGE_OB_TABLET_DUMP_MDS_NODE_OPERATOR

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
namespace mds
{
struct MdsDumpKV;
}

class ObTabletMdsData;
class ObTabletDumpedMediumInfo;

class ObTabletDumpMdsNodeOperator
{
public:
  ObTabletDumpMdsNodeOperator(ObTabletMdsData &mds_data, common::ObIAllocator &allocator);
public:
  int operator()(const mds::MdsDumpKV &kv);
  bool dumped() const { return dumped_; }
private:
  template <typename K, typename T>
  int dump(const mds::MdsDumpKV &kv, bool &dumped);
private:
  ObTabletMdsData &mds_data_;
  common::ObIAllocator &allocator_;
  bool dumped_;
};

class ObTabletMediumInfoNodeOperator
{
public:
  ObTabletMediumInfoNodeOperator(ObTabletDumpedMediumInfo &medium_info_list, common::ObIAllocator &allocator);
public:
  int operator()(const mds::MdsDumpKV &kv);
  bool dumped() const { return dumped_; }
private:
  ObTabletDumpedMediumInfo &medium_info_list_;
  common::ObIAllocator &allocator_;
  bool dumped_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_DUMP_MDS_NODE_OPERATOR
