//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_TRUNCATE_INFO_H_
#define OB_STORAGE_TRUNCATE_INFO_H_
namespace oceanbase
{
namespace storage
{
struct ObTruncateInfoKey final
{
public:
  ObTruncateInfoKey() = default;
  ~ObTruncateInfoKey() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int mds_serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int mds_deserialize(const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t mds_get_serialize_size() const { return 0; }
  int assign(const ObTruncateInfoKey &other) { return OB_SUCCESS; }
};

struct ObTruncateInfo final
{
public:
  ObTruncateInfo() = default;
  ~ObTruncateInfo() = default;
  int64_t to_string(char *, const int64_t) const { return 0; }
  int serialize(char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int deserialize(common::ObIAllocator &allocator, const char *, const int64_t, int64_t &) const { return OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int assign(ObIAllocator &allocator, const ObTruncateInfo &other) { return OB_SUCCESS; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTruncateInfo);
};

} // namespace strorage
} // namespace oceanbase

#endif // OB_STORAGE_TRUNCATE_INFO_H_
