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

#include "common/ob_tenant_data_version_mgr.h"
#include "common/ob_record_header.h"

#define DV_ILOG_F(fmt, args...) COMMON_LOG(INFO, "[DATA_VERSION] " fmt, ##args)
#define DV_TLOG(fmt, args...) COMMON_LOG(TRACE, "[DATA_VERSION] " fmt, ##args)

namespace oceanbase
{
namespace common
{

ObTenantDataVersionMgr& ObTenantDataVersionMgr::get_instance()
{
  static ObTenantDataVersionMgr mgr;
  return mgr;
}

int ObTenantDataVersionMgr::init(bool enable_compatible_monotonic)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(map_.create(TENANT_DATA_VERSION_BUCKET_NUM, lib::ObLabel("TenantDVMap")))) {
    COMMON_LOG(WARN, "fail to create TenantDataVersionMgr map", K(ret));
  } else {
    is_inited_ = true;
    mock_data_version_ = 0;
    enable_compatible_monotonic_ = enable_compatible_monotonic;
  }

  return ret;
}

int ObTenantDataVersionMgr::get(const uint64_t tenant_id, uint64_t &data_version) const
{
  int ret = OB_SUCCESS;
  ObTenantDataVersion *version = NULL;
  data_version = 0;

  if (OB_UNLIKELY(mock_data_version_ != 0)) {
    data_version = ATOMIC_LOAD(&mock_data_version_);
    DV_TLOG("mock_data_version is set", K(tenant_id), KDV(data_version));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDataVersionMgr doesn't init", K(ret), K(tenant_id));
  } else if (OB_FAIL(map_.get_refactored(tenant_id, version))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
        data_version = LAST_BARRIER_DATA_VERSION;
        ret = OB_SUCCESS;
        COMMON_LOG(WARN, "data_version fallback to LAST_BARRIER_DATA_VERSION",
                   K(tenant_id), KDV(data_version));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else {
      COMMON_LOG(WARN, "fail to get data_version", K(ret), K(tenant_id));
    }
  } else if (NULL == version) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "data_version is NULL", K(ret));
  } else if (version->is_removed()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    data_version = version->get_version();
  }

  return ret;
}

int ObTenantDataVersionMgr::set(const uint64_t tenant_id, const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  ObTenantDataVersion *version = NULL;
  bool need_to_set = false;

  if (OB_UNLIKELY(mock_data_version_ != 0)) {
    // do nothing
    DV_TLOG("mock_data_version is set", K(tenant_id), KDV(data_version), K(mock_data_version_));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDataVersionMgr doesn't init", K(ret),
               K(tenant_id));
  } else if (OB_FAIL(map_.get_refactored(tenant_id, version))) {
    if (OB_HASH_NOT_EXIST == ret) {
      need_to_set = true;
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "fail to get data_version", K(ret), K(tenant_id));
    }
  } else if (NULL == version) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "data_version is NULL", K(ret));
  } else if (version->is_removed() || data_version <= version->get_version()) {
    // if the tenant is dropped or the new data_version is smaller, then
    // no need to set
  } else {
    need_to_set = true;
  }

  if (OB_SUCC(ret) && need_to_set) {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(set_(tenant_id, data_version))) {
      COMMON_LOG(WARN, "fail to set data_version", K(ret), K(tenant_id), KDV(data_version));
    }
  }

  return ret;
}

int ObTenantDataVersionMgr::remove(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantDataVersion *version = NULL;
  const int64_t remove_ts = ObTimeUtility::current_time();

  SpinWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDataVersionMgr doesn't init", K(ret), K(tenant_id));
  } else if (OB_FAIL(map_.get_refactored(tenant_id, version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "fail to get data_version", K(ret), K(tenant_id));
    }
  } else if (NULL == version) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "data_version is NULL", K(ret));
  } else if (version->is_removed()) {
    // already removed
  } else if (OB_FAIL(remove_and_dump_to_file_(tenant_id, remove_ts))) {
    COMMON_LOG(WARN, "fail to dump data_version file", K(ret), K(tenant_id));
  } else {
    version->set_removed(true, remove_ts);
    DV_ILOG_F("Tenant DATA_VERSION is removed", K(ret), K(tenant_id), K(remove_ts), K(*version));
  }

  return ret;
}

int ObTenantDataVersionMgr::load_from_file()
{
  int ret = OB_SUCCESS;
  int fd = 0;
  const char *file_path = TENANT_DATA_VERSION_FILE_PATH;
  SpinWLockGuard guard(lock_);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObTenantDataVersionMgr doesn't init", K(ret));
  } else if ((fd = ::open(file_path, O_RDONLY)) < 0) {
    if (ENOENT != errno) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "fail to open data_version file", K(ret), K(errno), K(file_path));
    } else {
      // when errno is ENOENT, the file does not exist
      COMMON_LOG(WARN, "data_version file doesn't exist, skip load");
    }
  } else {
    char *load_buf = NULL;
    int64_t buf_size = TENANT_DATA_VERSION_FILE_MAX_SIZE;
    PageArena<> pa;

    if (OB_ISNULL(load_buf = pa.alloc(buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "fail to alloc buf", K(ret), K(buf_size));
    } else {
      size_t read_len = ::read(fd, load_buf, buf_size);
      if (read_len < 0) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to read data_version file", K(ret), K(read_len), K(errno), K(fd));
      } else {
        // deserialize header
        // checksum check
        // load data_version
        ObRecordHeader header;
        int64_t pos = 0;
         if (OB_FAIL(header.deserialize(load_buf, read_len, pos))) {
          COMMON_LOG(ERROR, "deserialize header failed", K(ret), K(read_len), K(pos));
        } else {
          const int64_t header_length = header.header_length_;
          const int64_t data_length = read_len - header_length;
          const char *const p_data = load_buf + header_length;
          if (data_length <= 0 || data_length != header.data_zlength_) {
            ret = OB_INVALID_DATA;
            COMMON_LOG(ERROR, "invalid data length", K(ret), K(header_length),
                       K(data_length), K(buf_size), K(read_len), K(header));
          } else if (OB_FAIL(header.check_header_checksum())) {
            COMMON_LOG(ERROR, "check header checksum failed", K(ret), K(header));
          } else if (OB_CONFIG_MAGIC != header.magic_) {
            ret = OB_INVALID_DATA;
            COMMON_LOG(ERROR, "check magic number failed", K(ret),
                       K_(header.magic));
          } else if (OB_FAIL(header.check_payload_checksum(p_data, data_length))) {
            COMMON_LOG(ERROR, "check data checksum failed", K(ret));
          } else {
            while (OB_SUCC(ret)) {
              ret = load_data_version_(load_buf, pos);
            }
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
    if (0 != close(fd)) {
      if (OB_SUCC(ret)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to close data_version file fd", K(ret), K(errno), K(fd));
      }
    }
  }

  COMMON_LOG(INFO, "[DATA_VERSION] load data_version file", K(ret), K(map_.size()));

  return ret;
}

int ObTenantDataVersionMgr::set_(const uint64_t tenant_id,
                                 const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  ObTenantDataVersion *version = NULL;
  bool need_to_insert = false;
  uint64_t old_version = 0;

  if (OB_FAIL(map_.get_refactored(tenant_id, version))) {
    if (OB_HASH_NOT_EXIST == ret) {
      need_to_insert = true;
      version = NULL;
      old_version = 0;
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "fail to get result from data_version_map", K(ret),
                 K(tenant_id), KDV(data_version));
    }
  } else if (NULL == version) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "data_version is NULL", K(ret));
  } else {
    old_version = version->get_version();
  }

  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(version) && (version->is_removed() || data_version <= old_version)) {
      COMMON_LOG(INFO,
                 "tenant is removed or new data_version is not bigger than old "
                 "value, no need to update tenant data_version", K(tenant_id),
                 "is_removed", version->is_removed(),
                 "old_version", old_version,
                 "new_version", data_version);
    } else if (OB_FAIL(set_and_dump_to_file_(tenant_id, data_version, need_to_insert))) {
      COMMON_LOG(WARN, "fail to dump data_version file", K(ret), K(tenant_id));
    } else {
      if (need_to_insert) {
        void *version_buf = NULL;
        if (OB_ISNULL(version_buf = allocator_.alloc(sizeof(ObTenantDataVersion)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(ERROR, "fail to alloc buf", K(ret), K(tenant_id),
                     K(sizeof(ObTenantDataVersion)));
        } else if (FALSE_IT(version = new (version_buf) ObTenantDataVersion(data_version))) {

        } else if (OB_FAIL(map_.set_refactored(tenant_id, version))) {
          COMMON_LOG(WARN, "fail to set data_version", K(ret), K(tenant_id));
        }
      } else {
        version->set_version(data_version);
      }

      DV_ILOG_F("Tenant DATA_VERSION is changed", K(ret), K(tenant_id),
                "old_version", DVP(old_version),
                "new_version", DVP(data_version));
    }
  }

  return ret;
}

int ObTenantDataVersionMgr::set_and_dump_to_file_(const uint64_t tenant_id,
                                                  const uint64_t data_version,
                                                  const bool need_to_insert) {
  int ret = OB_SUCCESS;
  ObRecordHeader header;
  int64_t header_length = header.get_serialize_size();
  char *dump_buf = NULL;
  // we may need to insert a new entry to the map, so map_size + 1 to ensure the memory is enough
  int64_t buf_length = header_length + (map_.size() + 1) * ObTenantDataVersion::MAX_DUMP_BUF_SIZE;
  PageArena<> pa;

  if (OB_ISNULL(dump_buf = pa.alloc(buf_length))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "fail to alloc buf", K(ret), K(tenant_id), K(buf_length));
  } else {
    int64_t pos = 0;
    const int64_t data_pos = pos + header_length;
    pos += header_length;
    ObTenantDataVersionMap::const_iterator it = map_.begin();
    for (; it != map_.end() && OB_SUCC(ret); ++it) {
      const uint64_t iter_tenant_id = it->first;
      const ObTenantDataVersion *iter_version = it->second;
      if (OB_ISNULL(iter_version)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "iter_version is null", K(iter_tenant_id));
      } else if (iter_tenant_id == tenant_id) {
        if (OB_FAIL(dump_data_version_(
                dump_buf, buf_length, pos, iter_tenant_id,
                iter_version->is_removed(),
                iter_version->get_remove_timestamp(),
                data_version))) {
          COMMON_LOG(WARN, "fail to dump data_version", K(ret),
                     K(iter_tenant_id), KDV(data_version), K(*iter_version));
        }
      } else {
        if (OB_FAIL(dump_data_version_(dump_buf, buf_length, pos, iter_tenant_id,
                                       iter_version->is_removed(),
                                       iter_version->get_remove_timestamp(),
                                       iter_version->get_version()))) {
          COMMON_LOG(WARN, "fail to dump data_version", K(ret),
                     K(iter_tenant_id), K(*iter_version));
        }
      }
    }
    if (OB_SUCC(ret) && need_to_insert) {
      if (OB_FAIL(dump_data_version_(dump_buf, buf_length, pos, tenant_id,
                                     false /*removed*/, 0 /*remove_ts*/,
                                     data_version))) {
        COMMON_LOG(WARN, "fail to dump data_version", K(ret), K(tenant_id), KDV(data_version));
      }
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(write_to_file_(dump_buf, buf_length, pos - data_pos))) {
      COMMON_LOG(WARN, "fail to write data_version file", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int ObTenantDataVersionMgr::remove_and_dump_to_file_(const uint64_t tenant_id,
                                                     const int64_t remove_ts) {
  int ret = OB_SUCCESS;
  ObRecordHeader header;
  int64_t header_length = header.get_serialize_size();
  char *dump_buf = NULL;
  int64_t buf_length = header_length + map_.size() * ObTenantDataVersion::MAX_DUMP_BUF_SIZE;
  PageArena<> pa;

  if (OB_ISNULL(dump_buf = pa.alloc(buf_length))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "fail to alloc buf", K(ret), K(tenant_id), K(buf_length));
  } else {
    int64_t pos = 0;
    const int64_t data_pos = pos + header_length;
    pos += header_length;
    ObTenantDataVersionMap::const_iterator it = map_.begin();
    for (; it != map_.end() && OB_SUCC(ret); ++it) {
      const uint64_t iter_tenant_id = it->first;
      const ObTenantDataVersion *iter_version = it->second;
      if (OB_ISNULL(iter_version)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "iter_version is null", K(iter_tenant_id));
      } else if (iter_tenant_id == tenant_id) {
        if (OB_FAIL(dump_data_version_(dump_buf, buf_length, pos, iter_tenant_id,
                                       true /*removed*/,
                                       remove_ts,
                                       iter_version->get_version()))) {
          COMMON_LOG(WARN, "fail to dump data_version", K(ret),
                     K(iter_tenant_id), K(remove_ts), K(*iter_version));
        }
      } else {
        if (OB_FAIL(dump_data_version_(dump_buf, buf_length, pos,
                                       iter_tenant_id,
                                       iter_version->is_removed(),
                                       iter_version->get_remove_timestamp(),
                                       iter_version->get_version()))) {
          COMMON_LOG(WARN, "fail to dump data_version", K(ret),
                     K(iter_tenant_id), K(*iter_version));
        }
      }
    }
    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(write_to_file_(dump_buf, buf_length, pos - data_pos))) {
      COMMON_LOG(WARN, "fail to write data_version file", K(ret), K(tenant_id));
    }
  }

  return ret;
}

int64_t ObTenantDataVersionMgr::get_max_dump_buf_size_() const
{
  ObRecordHeader header;
  int64_t header_size = header.get_serialize_size();
  // we may need to insert a new entry to the map
  int64_t data_size = (map_.size() + 1) * ObTenantDataVersion::MAX_DUMP_BUF_SIZE;
  return header_size + data_size;
}

int ObTenantDataVersionMgr::dump_data_version_(char *buf, int64_t buf_length, int64_t &pos,
                                               const uint64_t tenant_id,
                                               const bool removed,
                                               const int64_t remove_ts,
                                               const uint64_t data_version) {
  int ret = OB_SUCCESS;
  char tmp_buf[ObTenantDataVersion::MAX_DUMP_BUF_SIZE]{0};
  char version_str[OB_SERVER_VERSION_LENGTH]{0};
  int res = 0;
  if (OB_INVALID_INDEX ==
      VersionUtil::print_version_str(version_str, OB_SERVER_VERSION_LENGTH, data_version)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "fail to print data_version str", K(ret), KDV(data_version));
  } else if (OB_FAIL(databuff_printf(
                 buf, buf_length, pos, ObTenantDataVersion::DUMP_BUF_FORMAT,
                 tenant_id, version_str, data_version, removed, remove_ts))) {
    COMMON_LOG(WARN, "fail to printf", K(ret), K(tenant_id), K(buf_length), K(pos));
  } else if (pos >= buf_length) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "buffer size overflow", K(ret), K(tenant_id), K(buf_length), K(pos));
  } else {
    // we use '\n' as the separator of tenant, we'll use STRTOK to parse this
    buf[pos] = '\n';
    pos += 1;
  }

  return ret;
}

int ObTenantDataVersionMgr::load_data_version_(char *buf, int64_t &pos) {
  int ret = OB_SUCCESS;
  ObTenantDataVersion *version = NULL;
  uint64_t tenant_id = 0;
  int removed = false;
  uint64_t remove_timestamp = 0;
  uint64_t version_val = 0;
  char version_str[OB_SERVER_VERSION_LENGTH]{0};
  int res = 0;
  const int expected_item_size = 5;
  char *saveptr = NULL;
  char *token = NULL;

  if (NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "buf is null", K(ret), K(pos));
  } else if (NULL == (token = STRTOK_R(buf + pos, "\n", &saveptr))) {
    ret = OB_ITER_END;
  } else {
    res = sscanf(token, ObTenantDataVersion::DUMP_BUF_FORMAT, &tenant_id,
                 version_str, &version_val, &removed, &remove_timestamp);
    if (res != expected_item_size) {
      ret = OB_INVALID_DATA;
      COMMON_LOG(ERROR, "fail to parse data_version", K(ret), K(res), K(pos),
                 K(token), K(tenant_id), K(version_val), K(version_str),
                 K(removed), K(remove_timestamp));
    } else {
      COMMON_LOG(INFO, "[DATA_VERSION] successfully parse data_version",
                 K(tenant_id), K(version_val), K(version_str), K(removed),
                 K(remove_timestamp), K(pos));
      void *version_buf = NULL;
      if (OB_ISNULL(version_buf =
                        allocator_.alloc(sizeof(ObTenantDataVersion)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "fail to alloc buf", K(ret), K(sizeof(ObTenantDataVersion)));
      } else if (FALSE_IT(version = new (version_buf) ObTenantDataVersion(
                              removed, remove_timestamp, version_val))) {

      } else if (OB_FAIL(map_.set_refactored(tenant_id, version))) {
        COMMON_LOG(WARN, "fail to set data_version", K(ret), K(tenant_id), K(version));
      } else {
        pos += (saveptr - token);
      }
    }
  }
  return ret;
}

int ObTenantDataVersionMgr::write_to_file_(char *buf, int64_t buf_length, int64_t data_length)
{
  int ret = OB_SUCCESS;
  int fd = 0;
  ObRecordHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t total_length = header_length + data_length;
  const int64_t max_length = TENANT_DATA_VERSION_FILE_MAX_SIZE;
  int64_t header_pos = 0;

  if (total_length > buf_length || total_length > max_length) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "dump buffer overflow", K(ret), K(total_length),
               K(data_length), K(buf_length), K(max_length));
  } else {
    header.magic_ = OB_CONFIG_MAGIC;
    header.header_length_ = static_cast<int16_t>(header_length);
    header.version_ = OB_CONFIG_VERSION;
    header.data_length_ = static_cast<int32_t>(data_length);
    header.data_zlength_ = header.data_length_;
    header.data_checksum_ = ob_crc64(buf + header_length, data_length);
    header.set_header_checksum();
    if (OB_FAIL(header.serialize(buf, buf_length, header_pos))) {
      COMMON_LOG(WARN, "fail to serialize header", K(ret), K(header), K(buf_length), K(header_pos));
    } else {
      const char *file_path = TENANT_DATA_VERSION_FILE_PATH;
      char tmp_path[MAX_PATH_SIZE]{0};
      char hist_path[MAX_PATH_SIZE]{0};
      if (OB_FAIL(databuff_printf(tmp_path, MAX_PATH_SIZE, "%s.tmp", file_path))) {
        COMMON_LOG(WARN, "fail to printf", K(ret));
      } else if (OB_FAIL(databuff_printf(hist_path, MAX_PATH_SIZE, "%s.history", file_path))) {
        COMMON_LOG(WARN, "fail to printf", K(ret));
      } else if ((fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC,
                              S_IRUSR | S_IWUSR | S_IRGRP)) < 0) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to open data_version file", K(ret), K(errno),
                  K(fd), K(total_length), K(tmp_path));
      } else if (total_length != ::write(fd, buf, total_length)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to write data_version file", K(ret), K(errno),
                  K(fd), K(total_length));
        if (0 != ::close(fd)) {
          COMMON_LOG(WARN, "fail to close data_version file fd", K(ret), K(errno),
                    K(fd), K(total_length));
        }
      } else if (0 != ::fsync(fd)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to sync data_version file", K(ret), K(errno),
                  K(fd), K(total_length));
        if (0 != ::close(fd)) {
          COMMON_LOG(WARN, "fail to close data_version file fd", K(ret), K(errno),
                    K(fd), K(total_length));
        }
      } else if (0 != ::close(fd)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fail to close data_version file fd", K(ret), K(errno),
                  K(fd), K(total_length));
      }
      if (OB_SUCC(ret)) {
        if (0 != ::rename(file_path, hist_path) && errno != ENOENT) {
          // it's OK to continue if we fail to backup history file, so we ignore the err ret here
          COMMON_LOG(WARN, "fail to backup history config file", KERRMSG, K(ret));
        }
        // 运行到这里的时候可能掉电，导致没有 conf 文件，需要 DBA 手工拷贝 tmp 文件到这里
        if (0 != ::rename(tmp_path, file_path)) {
          ret = OB_ERR_SYS;
          COMMON_LOG(WARN, "fail to move tmp config file", KERRMSG, K(ret));
        }
      }
    }

    COMMON_LOG(INFO, "[DATA_VERSION] write data_version file", K(ret),
              K(header_length), K(data_length), K(total_length));
  }

  return ret;
}

ObDataVersionPrinter::ObDataVersionPrinter(const uint64_t data_version)
    : version_val_(data_version), version_str_{0}
{
  if (OB_INVALID_INDEX ==
      VersionUtil::print_version_str(version_str_, OB_SERVER_VERSION_LENGTH, data_version)) {
    MEMSET(version_str_, 0, OB_SERVER_VERSION_LENGTH);
  }
}

} // namespace common
} // namespace oceanbase