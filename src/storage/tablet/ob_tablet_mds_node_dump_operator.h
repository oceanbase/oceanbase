/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_DUMP_MDS_NODE_OPERATOR
