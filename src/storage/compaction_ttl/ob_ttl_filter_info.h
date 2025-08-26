//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_
#define OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_
namespace oceanbase
{
namespace storage
{
struct ObTTLFilterInfoKey final
{
public:
  ObTTLFilterInfoKey() = default;
  ~ObTTLFilterInfoKey() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int mds_serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int mds_deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t mds_get_serialize_size() const { return 0; }
  int assign(const ObTTLFilterInfoKey &other) { return OB_SUCCESS; }
};

struct ObTTLFilterInfo final
{
public:
  ObTTLFilterInfo() = default;
  ~ObTTLFilterInfo() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(common::ObIAllocator &allocator, const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int assign(ObIAllocator &allocator, const ObTTLFilterInfo &other) { return OB_SUCCESS; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTTLFilterInfo);
};

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TTL_TTL_FILTER_INFO_H_