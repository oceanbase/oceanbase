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

#define USING_LOG_PREFIX SHARE

#include "share/object_storage/ob_device_manifest.h"
#include "share/object_storage/ob_device_config_parser.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace share
{
const int64_t ObDeviceManifest::MANIFEST_VERSION = 1;
const int64_t ObDeviceManifest::MAX_FILE_LINE_LEN = 16384; // 16K
const char ObDeviceManifest::MANIFEST_FILE_NAME[] = "manifest";
const char ObDeviceManifest::HEAD_SECTION[] = "[head]";
const char ObDeviceManifest::DEVICE_BEGIN_SECTION[] = "[device begin]";
const char ObDeviceManifest::DEVICE_END_SECTION[] = "[device end]";
const char ObDeviceManifest::COMMENT_SYMBOL = '#';
const char ObDeviceManifest::VERSION_KEY[] = "version=";
const char ObDeviceManifest::CLUSTER_ID_KEY[] = "cluster_id=";
const char ObDeviceManifest::HEAD_CHECKSUM_KEY[] = "head_checksum=";
const char ObDeviceManifest::DEVICE_CHECKSUM_KEY[] = "device_checksum=";
const char ObDeviceManifest::DEVICE_NUM_KEY[] = "device_num=";
const char ObDeviceManifest::MODIFY_TIMESTAMP_KEY[] = "modify_timestamp_us=";
const char ObDeviceManifest::LAST_OP_ID_KEY[] = "last_op_id=";
const char ObDeviceManifest::LAST_SUB_OP_ID_KEY[] = "last_sub_op_id=";

int ObDeviceManifest::init(const char *data_dir)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data_dir is nullptr", KR(ret));
  } else {
    data_dir_ = data_dir;
    is_inited_ = true;
  }
  return ret;
}

void ObDeviceManifest::destroy()
{
  data_dir_ = nullptr;
  is_inited_ = false;
  LOG_INFO("device manifest finish to destroy");
}

int ObDeviceManifest::load(ObIArray<ObDeviceConfig> &config_arr, HeadSection &head)
{
  int ret = OB_SUCCESS;
  FILE *fp = nullptr;
  bool is_exist = false;
  char manifest_path[OB_MAX_FILE_NAME_LENGTH];
  const int64_t cluster_id_in_GCONF = GCONF.cluster_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDeviceManifest not init", KR(ret));
  } else if (OB_ISNULL(data_dir_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data dir is nullptr", KR(ret));
  } else if (OB_FAIL(databuff_printf(manifest_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",
                                     data_dir_, MANIFEST_FILE_NAME))) {
    LOG_WARN("construct manifest path fail", KR(ret));
  } else if (OB_ISNULL(fp = fopen(manifest_path, "r"))) {
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
    LOG_WARN("fail to open device manifest file", KR(ret), K(errno), KERRMSG);
  } else if (OB_FAIL(parse_file_(fp, config_arr, head))) {
    LOG_WARN("fail to parse file", KR(ret), K(manifest_path));
  } else if (OB_FAIL(validate_config_checksum_(config_arr, head))) {
    LOG_WARN("fail to validate config checksum", KR(ret));
  } else if (cluster_id_in_GCONF != head.cluster_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid device manifest", K(cluster_id_in_GCONF), "manifest_cluster_id", head.cluster_id_);
  }
  if (OB_NOT_NULL(fp)) {
    if (OB_UNLIKELY(0 != fclose(fp))) {
      ret = ObIODeviceLocalFileOp::convert_sys_errno();
      LOG_ERROR("fail to close file", KR(ret), K(errno), KERRMSG);
    }
  }
  LOG_INFO("finish to load device manifest", KR(ret), K(config_arr), K(head));
  return ret;
}

int ObDeviceManifest::parse_file_(FILE *fp, ObIArray<ObDeviceConfig> &config_arr, HeadSection &head)
{
  int ret = OB_SUCCESS;
  SectionType curr_section_type = SECTION_TYPE_INVLAID;
  SMART_VARS_2((char[MAX_FILE_LINE_LEN + 2], line_buf), (ObDeviceConfig, device_config)) {
    while (OB_SUCC(ret)) {
      if (0 != feof(fp)) {
        break; // end of file
      } else if (OB_UNLIKELY(0 != ferror(fp))) {
        ret = OB_IO_ERROR;
        LOG_WARN("fail to read manifest file", KR(ret), K(errno));
      } else if (OB_ISNULL(fgets(line_buf, sizeof(line_buf), fp))) {
        // do nothing
      } else if (OB_UNLIKELY(STRLEN(line_buf) > MAX_FILE_LINE_LEN)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("file line len is too long", KR(ret), K(MAX_FILE_LINE_LEN));
      } else if (FALSE_IT(line_buf[STRLEN(line_buf) - 1] = '\0')) {  // remove the '\n'
      } else if (0 == STRLEN(line_buf) || (COMMENT_SYMBOL == line_buf[0])) {
        // skip blank line or comment line
      } else if (OB_SUCC(parse_section_type_(line_buf, curr_section_type))) {
        // this line is a section name.
        // SECTION_TYPE_DEVICE_END means finishing parsing one device config, push back the
        // parsed device config and reset device_config to parse the next device config.
        if (SECTION_TYPE_DEVICE_END == curr_section_type) {
          if (OB_UNLIKELY(!device_config.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid device config", KR(ret), K(device_config));
          } else if (OB_FAIL(config_arr.push_back(device_config))) {
            LOG_WARN("fail to push back", KR(ret), K(device_config));
          } else {
            device_config.reset();
          }
        }
      } else if (OB_ITEM_NOT_MATCH != ret) {
        LOG_WARN("fail to parse section type", KR(ret), K(line_buf));
      } else if (SECTION_TYPE_HEAD == curr_section_type) {
        ret = OB_SUCCESS; // ignore ret, and then parse head section
        if (OB_FAIL(parse_head_section_(line_buf, head))) {
          LOG_WARN("fail to parse head section", KR(ret), K(line_buf), K(head));
        }
      } else if (SECTION_TYPE_DEVICE_BEGIN == curr_section_type) {
        ret = OB_SUCCESS; // ignore ret, and then parse device section
        if (OB_FAIL(parse_device_section_(line_buf, device_config))) {
          LOG_WARN("fail to parse device section", KR(ret), K(line_buf));
        }
      } else {
        LOG_WARN("unknow manifest section", KR(ret), K(curr_section_type), K(line_buf));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY((config_arr.count() != head.device_num_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device num does not match between head and device sections", KR(ret), K(head), K(config_arr));
  }
  LOG_INFO("finish to parse file", KR(ret), K(config_arr), K(head));
  return ret;
}

int ObDeviceManifest::parse_section_type_(const char *buf, SectionType &type)
{
  int ret = OB_SUCCESS;
  if (0 == STRCMP(buf, HEAD_SECTION)) {
    type = SECTION_TYPE_HEAD;
  } else if (0 == STRCMP(buf, DEVICE_BEGIN_SECTION)) {
    type = SECTION_TYPE_DEVICE_BEGIN;
  } else if (0 == STRCMP(buf, DEVICE_END_SECTION)) {
    type = SECTION_TYPE_DEVICE_END;
  } else {
    ret = OB_ITEM_NOT_MATCH;
  }
  return ret;
}

int ObDeviceManifest::parse_head_section_(const char *buf, HeadSection &head)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is nullptr", KR(ret));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(VERSION_KEY, buf, head.version_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(VERSION_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(CLUSTER_ID_KEY, buf,
                                                                 head.cluster_id_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(CLUSTER_ID_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(HEAD_CHECKSUM_KEY, buf,
                                                                 head.head_checksum_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(HEAD_CHECKSUM_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(DEVICE_CHECKSUM_KEY, buf,
                                                                 head.device_checksum_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(DEVICE_CHECKSUM_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(DEVICE_NUM_KEY, buf,
                                                                 head.device_num_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(DEVICE_NUM_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_int(MODIFY_TIMESTAMP_KEY, buf,
                                                                 head.modify_timestamp_us_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(MODIFY_TIMESTAMP_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_uint(LAST_OP_ID_KEY, buf,
                                                                  head.last_op_id_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(LAST_OP_ID_KEY), K(buf));
  } else if (OB_SUCC(ObDeviceConfigParser::parse_config_type_uint(LAST_SUB_OP_ID_KEY, buf,
                                                                  head.last_sub_op_id_))) {
  } else if (OB_ITEM_NOT_MATCH != ret) {
    LOG_WARN("fail to parse_config_type_int", KR(ret), K(LAST_SUB_OP_ID_KEY), K(buf));
  } else {
    LOG_WARN("unknow manifest head section line", KR(ret), K(buf));
  }
  return ret;
}

int ObDeviceManifest::parse_device_section_(const char *buf, ObDeviceConfig &device_config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is nullptr", KR(ret));
  } else if (OB_FAIL(ObDeviceConfigParser::parse_device_config_field(buf, device_config))) {
    if (OB_ITEM_NOT_MATCH == ret) {
      LOG_WARN("unknow manifest device section line", KR(ret), K(buf));
    } else {
      LOG_WARN("fail to parse device config field", KR(ret), K(buf));
    }
  }
  return ret;
}

int ObDeviceManifest::dump2file(
    const ObIArray<ObDeviceConfig> &config_arr,
    HeadSection &head) const
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObArray<ObDeviceConfig>, configs) {
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDeviceManifest not init", KR(ret));
    } else if (FALSE_IT(head.version_ = MANIFEST_VERSION)) {
    } else if (FALSE_IT(head.cluster_id_ = GCONF.cluster_id)) {
    } else if (OB_FAIL(configs.assign(config_arr))) {
      LOG_WARN("fail to assign", KR(ret));
    } else if (OB_FAIL(sort_device_configs_(configs))) {
      LOG_WARN("fail to sort device configs", KR(ret));
    } else if (FALSE_IT(update_config_checksum_(configs, head))) {
    } else {
      int fd = 0;
      FILE *fp = nullptr;
      int pret =0;
      char *manifest_path = nullptr;
      char *tmp_manifest_path = nullptr;
      char *his_manifest_path = nullptr;
      ObArenaAllocator allocator;
      if (OB_ISNULL(manifest_path = reinterpret_cast<char*>(
                                    allocator.alloc(OB_MAX_FILE_NAME_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to malloc memory for manifest_path", KR(ret), K(OB_MAX_FILE_NAME_LENGTH));
      } else if (OB_ISNULL(tmp_manifest_path = reinterpret_cast<char*>(
                                               allocator.alloc(OB_MAX_FILE_NAME_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to malloc memory for tmp_manifest_path", KR(ret), K(OB_MAX_FILE_NAME_LENGTH));
      } else if (OB_ISNULL(his_manifest_path = reinterpret_cast<char*>(
                                               allocator.alloc(OB_MAX_FILE_NAME_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to malloc memory for his_manifest_path", KR(ret), K(OB_MAX_FILE_NAME_LENGTH));
      } else if (OB_ISNULL(data_dir_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data dir is nullptr", KR(ret));
      } else if (OB_FAIL(databuff_printf(manifest_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",
                                         data_dir_, MANIFEST_FILE_NAME))) {
        LOG_WARN("fail to construct manifest path", KR(ret));
      } else if (OB_FAIL(databuff_printf(tmp_manifest_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s.tmp",
                                         data_dir_, MANIFEST_FILE_NAME))) {
        LOG_WARN("fail to construct tmp manifest path", KR(ret));
      } else if (OB_FAIL(databuff_printf(his_manifest_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s.history",
                                         data_dir_, MANIFEST_FILE_NAME))) {
        LOG_WARN("fail to construct history manifest path", KR(ret));
      } else if ((fd = ::open(tmp_manifest_path, O_WRONLY | O_CREAT | O_TRUNC,
                                S_IRUSR | S_IWUSR | S_IRGRP)) < 0) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to create tmp manifest", KR(ret), K(tmp_manifest_path), K(errno), KERRMSG);
      } else if (OB_ISNULL(fp = ::fdopen(fd, "w")))  {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fdopen", KR(ret), K(fd), K(errno), KERRMSG);
      } else if (OB_FAIL(print_file_line_(fp, "# THIS FILE IS AUTOMATICALLY GENERATED BY OBSERVER. PLEASE DO NOT MODIFY IT MANUALLY!!!\n"))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(write_head_(fp, head))) {
        LOG_WARN("fail to write head", KR(ret), K(head));
      } else if (OB_FAIL(write_device_config_(fp, configs))) {
        LOG_WARN("fail to write device config", KR(ret), K(configs));
      } else if (0 != ::fflush(fp)) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fflush manifest", KR(ret), K(errno), KERRMSG);
      } else if (0 != fsync(fd)) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fsync", KR(ret), K(fd), K(errno), KERRMSG);
      } else if (0 != fclose(fp)) {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        LOG_WARN("fail to fclose", KR(ret), K(fd), K(errno), KERRMSG);
      } else {
        LOG_INFO("write tmp device manifest successfully", K(tmp_manifest_path), K(configs));
        if (0 != ::rename(manifest_path, his_manifest_path) && errno != ENOENT) {
          ret = ObIODeviceLocalFileOp::convert_sys_errno();
          LOG_WARN("fail to backup history device manifest", KR(ret), K(manifest_path),
                   K(his_manifest_path), K(errno), KERRMSG);
        } else if (0 != ::rename(tmp_manifest_path, manifest_path)) {
          ret = ObIODeviceLocalFileOp::convert_sys_errno();
          LOG_WARN("fail to rename device manifest", KR(ret), K(tmp_manifest_path),
                   K(manifest_path), K(errno), KERRMSG);
        }
      }
    }
  }
  LOG_INFO("finish to dump device manifest", KR(ret), K(config_arr), K(head));
  return ret;
}

// The format of head section is like:
// [head]
// version=1
// cluster_id=0
// head_checksum=790215355
// device_checksum=2094936029
// device_num=2
// modify_timestamp_us=1725551141912558
// last_op_id=3
// last_sub_op_id=4
int ObDeviceManifest::write_head_(FILE *fp, const HeadSection &head) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_file_line_(fp, "%s\n", HEAD_SECTION))) { // [head]
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", VERSION_KEY, head.version_))) { // version=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", CLUSTER_ID_KEY, head.cluster_id_))) { // cluster_id=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", HEAD_CHECKSUM_KEY, head.head_checksum_))) { // head_checksum=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", DEVICE_CHECKSUM_KEY, head.device_checksum_))) { // device_checksum=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", DEVICE_NUM_KEY, head.device_num_))) { // device_num=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", MODIFY_TIMESTAMP_KEY, head.modify_timestamp_us_))) { // modify_timestamp_us=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", LAST_OP_ID_KEY, head.last_op_id_))) { // last_op_id=
    LOG_WARN("fail to print file line", KR(ret));
  } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", LAST_SUB_OP_ID_KEY, head.last_sub_op_id_))) { // last_sub_op_id=
    LOG_WARN("fail to print file line", KR(ret));
  }
  LOG_INFO("finish to write manifest head", KR(ret));
  return ret;
}

// The format of device section is like:
// [device begin]
// used_for=DATA
// path=oss://bucket-name/root_dir
// endpoint=host=cn-xxx.com
// access_info=access_id=ccccc&encrypt_key=ddddd
// extension=appid=456&checksum_type=md5
// old_access_info=access_id=aaa&encrypt_key=bbb
// old_extension=appid=123&checksum_type=crc32
// state=ADDED
// state_info=is_connective=true
// create_timestamp=1679579590156
// last_check_timestamp=1679579590256
// op_id=3
// sub_op_id=4
// storage_id=2
// max_iops=10000
// max_bandwidth=1024000000
// [device end]
// [device begin]
// used_for=LOG
// path=oss://bucket-name/root_dir
// endpoint=host=cn-xxx.com
// access_info=access_id=aaa&encrypt_key=bbb
// extension=appid=123&checksum_type=crc32
// state=ADDED
// state_info=is_connective=true
// create_timestamp=1679579590156
// last_check_timestamp=1679579590256
// op_id=1
// sub_op_id=2
// storage_id=1
// max_iops=10000
// max_bandwidth=1024000000
// [device end]
// Note: not all the fields are needed.
int ObDeviceManifest::write_device_config_(
    FILE *fp,
    const ObIArray<ObDeviceConfig> &config_arr) const
{
  int ret = OB_SUCCESS;
  const char *device_type_str = nullptr;
  int pret = 0;
  int64_t config_cnt = config_arr.count();
  SMART_VAR(char[MAX_FILE_LINE_LEN], buf) {
    for (int64_t i = 0; OB_SUCC(ret) && (i < config_cnt); i++) {
      const ObDeviceConfig &config = config_arr.at(i);
      if (!config.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("device config is invalid", KR(ret), K(config));
      } else if (OB_FAIL(print_file_line_(fp, "%s\n", DEVICE_BEGIN_SECTION))) { // [device begin]
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::USED_FOR, config.used_for_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::PATH, config.path_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::ENDPOINT, config.endpoint_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::ACCESS_INFO, config.access_info_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if ((0 < STRLEN(config.encrypt_info_)) &&
                 OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::ENCRYPT_INFO, config.encrypt_info_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if ((0 < STRLEN(config.extension_)) &&
                 OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::EXTENSION, config.extension_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if ((0 < STRLEN(config.old_access_info_)) &&
                 OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::OLD_ACCESS_INFO, config.old_access_info_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if ((0 < STRLEN(config.old_encrypt_info_)) &&
                 OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::OLD_ENCRYPT_INFO, config.old_encrypt_info_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if ((0 < STRLEN(config.old_extension_)) &&
                 OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::OLD_EXTENSION, config.old_extension_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::STATE, config.state_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%s\n", ObDeviceConfigParser::STATE_INFO, config.state_info_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", ObDeviceConfigParser::CREATE_TIMESTAMP, config.create_timestamp_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", ObDeviceConfigParser::LAST_CHECK_TIMESTAMP, config.last_check_timestamp_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%lu\n", ObDeviceConfigParser::OP_ID, config.op_id_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%lu\n", ObDeviceConfigParser::SUB_OP_ID, config.sub_op_id_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%lu\n", ObDeviceConfigParser::STORAGE_ID, config.storage_id_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", ObDeviceConfigParser::MAX_IOPS, config.max_iops_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s%ld\n", ObDeviceConfigParser::MAX_BANDWIDTH, config.max_bandwidth_))) {
        LOG_WARN("fail to print file line", KR(ret));
      } else if (OB_FAIL(print_file_line_(fp, "%s\n", DEVICE_END_SECTION))) { // [device end]
        LOG_WARN("fail to print file line", KR(ret));
      }
      LOG_INFO("finish to write manifest device", KR(ret), K(buf));
    }
  }
  LOG_INFO("finish to write device config", KR(ret));
  return ret;
}

int ObDeviceManifest::sort_device_configs_(ObIArray<ObDeviceConfig> &config_arr) const
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, tmp_config) {
    int64_t config_cnt = config_arr.count();
    for (int64_t i = 0; i < config_cnt - 1; ++i) {
      bool finish = true;
      for (int64_t j = 0; j < config_cnt - 1 - i; ++j) {
        if (STRCMP(config_arr.at(j).used_for_, config_arr.at(j + 1).used_for_) > 0) {
          tmp_config = config_arr.at(j);
          config_arr.at(j) = config_arr.at(j + 1);
          config_arr.at(j + 1) = tmp_config;
          finish = false;
        } else if (0 == STRCMP(config_arr.at(j).used_for_, config_arr.at(j + 1).used_for_)) {
          if (STRCMP(config_arr.at(j).path_, config_arr.at(j + 1).path_) > 0) {
            tmp_config = config_arr.at(j);
            config_arr.at(j) = config_arr.at(j + 1);
            config_arr.at(j + 1) = tmp_config;
            finish = false;
          } else if (0 == STRCMP(config_arr.at(j).path_, config_arr.at(j + 1).path_)) {
            if (STRCMP(config_arr.at(j).endpoint_, config_arr.at(j + 1).endpoint_) > 0) {
              tmp_config = config_arr.at(j);
              config_arr.at(j) = config_arr.at(j + 1);
              config_arr.at(j + 1) = tmp_config;
              finish = false;
            } else { // STRCMP(config_arr.at(j).endpoint_, config_arr.at(j + 1).endpoint_) <= 0
            }
          } else { // STRCMP(config_arr.at(j).path_, config_arr.at(j + 1).path_) < 0
          }
        } else { // STRCMP(config_arr.at(j).used_for_, config_arr.at(j + 1).used_for_) < 0
        }
      }
      if (finish) {
        break;
      }
    }
  }
  return ret;
}

int64_t ObDeviceManifest::calc_device_checksum_(
        const common::ObIArray<ObDeviceConfig> &config_arr) const
{
  uint64_t accum_checksum = 0;
  const int64_t config_cnt = config_arr.count();
  for (int64_t i = 0; (i < config_cnt); ++i) {
    const ObDeviceConfig &tmp_config = config_arr.at(i);
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.used_for_)), sizeof(tmp_config.used_for_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.path_)), sizeof(tmp_config.path_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.endpoint_)), sizeof(tmp_config.endpoint_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.access_info_)), sizeof(tmp_config.access_info_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.encrypt_info_)), sizeof(tmp_config.encrypt_info_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.extension_)), sizeof(tmp_config.extension_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.old_access_info_)), sizeof(tmp_config.old_access_info_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.old_encrypt_info_)), sizeof(tmp_config.old_encrypt_info_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.old_extension_)), sizeof(tmp_config.old_extension_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.state_)), sizeof(tmp_config.state_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.state_info_)), sizeof(tmp_config.state_info_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.create_timestamp_)), sizeof(tmp_config.create_timestamp_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.last_check_timestamp_)), sizeof(tmp_config.last_check_timestamp_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.op_id_)), sizeof(tmp_config.op_id_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.sub_op_id_)), sizeof(tmp_config.sub_op_id_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.storage_id_)), sizeof(tmp_config.storage_id_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.max_iops_)), sizeof(tmp_config.max_iops_));
    accum_checksum = common::ob_crc64(accum_checksum, reinterpret_cast<const void *>(&(tmp_config.max_bandwidth_)), sizeof(tmp_config.max_bandwidth_));
  }
  return static_cast<int64_t>(accum_checksum);
}

void ObDeviceManifest::update_config_checksum_(
     const common::ObIArray<ObDeviceConfig> &config_arr,
     HeadSection &head) const
{
  const int64_t device_checksum = calc_device_checksum_(config_arr);
  head.device_checksum_ = device_checksum;
  const int64_t head_checksum = head.calc_head_checksum();
  head.head_checksum_ = head_checksum;
}

int ObDeviceManifest::validate_config_checksum_(
    const common::ObIArray<ObDeviceConfig> &config_arr,
    const ObDeviceManifest::HeadSection &head) const
{
  int ret = OB_SUCCESS;
  const int64_t device_checksum = calc_device_checksum_(config_arr);
  const int64_t head_checksum = head.calc_head_checksum();
  if ((device_checksum != head.device_checksum_) || (head_checksum != head.head_checksum_)) {
    ret = OB_CHECKSUM_ERROR;
    LOG_WARN("config checksum error", KR(ret), K(device_checksum), "device_checksum_record_in_head",
      head.device_checksum_, K(head_checksum), "head_checksum_record_in_head", head.head_checksum_);
  }
  return ret;
}

int ObDeviceManifest::print_file_line_(FILE *fp, const char *fmt, ...) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(fp) || OB_ISNULL(fmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(fp), KP(fmt));
  } else {
    va_list args;
    va_start(args, fmt);
    int pret = vfprintf(fp, fmt, args);
    va_end(args);
    if (OB_UNLIKELY((pret <= 0) || (pret >= MAX_FILE_LINE_LEN))) {
      ret = OB_IO_ERROR;
      LOG_WARN("fail to vfprintf", KR(ret), K(pret), K(errno), KERRMSG);
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
