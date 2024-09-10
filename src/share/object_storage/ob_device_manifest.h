/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_H_
#define OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_H_

#include "share/object_storage/ob_object_storage_struct.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_array.h"
#include "lib/stat/ob_latch_define.h"

namespace oceanbase
{
namespace share
{
class ObDeviceManifest
{
public:
  class HeadSection
  {
  public:
    HeadSection()
      : version_(0), cluster_id_(0), head_checksum_(0), device_checksum_(0), device_num_(0),
        modify_timestamp_us_(0), last_op_id_(UINT64_MAX), last_sub_op_id_(UINT64_MAX)
    {
    }

    void reset()
    {
      version_ = 0;
      cluster_id_ = 0;
      head_checksum_ = 0;
      device_checksum_ = 0;
      device_num_ = 0;
      modify_timestamp_us_ = 0;
      last_op_id_ = UINT64_MAX;
      last_sub_op_id_ = UINT64_MAX;
    }

    void assign(const HeadSection &other)
    {
      if (this != &other) {
        version_ = other.version_;
        cluster_id_ = other.cluster_id_;
        head_checksum_ = other.head_checksum_;
        device_checksum_ = other.device_checksum_;
        device_num_ = other.device_num_;
        modify_timestamp_us_ = other.modify_timestamp_us_;
        last_op_id_ = other.last_op_id_;
        last_sub_op_id_ = other.last_sub_op_id_;
      }
    }

    int64_t calc_head_checksum() const
    {
      uint64_t checksum = 0;
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&version_),
                                  sizeof(version_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&cluster_id_),
                                  sizeof(cluster_id_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&device_checksum_),
                                  sizeof(device_checksum_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&device_num_),
                                  sizeof(device_num_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&modify_timestamp_us_),
                                  sizeof(modify_timestamp_us_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&last_op_id_),
                                  sizeof(last_op_id_));
      checksum = common::ob_crc64(checksum, reinterpret_cast<const void *>(&last_sub_op_id_),
                                  sizeof(last_sub_op_id_));
      return static_cast<int64_t>(checksum);
    }

    TO_STRING_KV(K_(version), K_(cluster_id), K_(head_checksum), K_(device_checksum),
                 K_(device_num), K_(modify_timestamp_us), K_(last_op_id), K_(last_sub_op_id));

  public:
    int64_t version_;
    int64_t cluster_id_;
    int64_t head_checksum_;
    int64_t device_checksum_;
    int64_t device_num_;
    int64_t modify_timestamp_us_;
    uint64_t last_op_id_;
    uint64_t last_sub_op_id_;
  };

  ObDeviceManifest()
    : is_inited_(false), data_dir_(nullptr) {}
  virtual ~ObDeviceManifest() {}

  int init(const char *data_dir);
  void destroy();
  int load(common::ObIArray<ObDeviceConfig> &config_arr, HeadSection &head);
  int dump2file(const common::ObIArray<ObDeviceConfig> &config_arr, HeadSection &head) const;

private:
  static const char MANIFEST_FILE_NAME[];
  static const char HEAD_SECTION[];
  static const char DEVICE_SECTION[];
  enum SectionType
  {
    SECTION_TYPE_INVLAID,
    SECTION_TYPE_HEAD,
    SECTION_TYPE_DEVICE
  };

  static const int64_t MANIFEST_VERSION;
  static const int64_t MAX_FILE_LINE_LEN;
  static const char COMMENT_SYMBOL;
  static const char VERSION_KEY[];  // belong to HEAD_SECTION
  static const char CLUSTER_ID_KEY[];  // belong to HEAD_SECTION
  static const char HEAD_CHECKSUM_KEY[];  // belong to HEAD_SECTION
  static const char DEVICE_CHECKSUM_KEY[];  // belong to HEAD_SECTION
  static const char DEVICE_NUM_KEY[]; // belong to HEAD_SECTION
  static const char MODIFY_TIMESTAMP_KEY[]; // belong to HEAD_SECTION
  static const char LAST_OP_ID_KEY[]; // belong to HEAD_SECTION
  static const char LAST_SUB_OP_ID_KEY[]; // belong to HEAD_SECTION

private:
  int parse_file_(FILE *fp, common::ObIArray<ObDeviceConfig> &config_arr, HeadSection &head);
  int parse_section_type_(const char *buf, SectionType &type);
  int parse_head_section_(const char *buf, HeadSection &head);
  int parse_device_section_(char *buf, ObDeviceConfig &device_config);
  int write_head_(FILE *fp, const HeadSection &head) const;
  // sort device configs according to used_for_, path_ and endpoint_
  int sort_device_configs_(common::ObIArray<ObDeviceConfig> &config_arr) const;
  int write_device_config_(FILE *fp, const common::ObIArray<ObDeviceConfig> &config_arr) const;
  int64_t calc_device_checksum_(const common::ObIArray<ObDeviceConfig> &config_arr) const;
  // update head.device_checksum_ and head.head_checksum_
  void update_config_checksum_(const common::ObIArray<ObDeviceConfig> &config_arr,
                               HeadSection &head) const;
  int validate_config_checksum_(const common::ObIArray<ObDeviceConfig> &config_arr,
                                const HeadSection &head) const;

private:
  bool is_inited_;
  const char *data_dir_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_DEVICE_OB_DEVICE_MANIFEST_H_
