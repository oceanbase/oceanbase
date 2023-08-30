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

#include <string.h>
#include <algorithm>
#include <math.h>  // for fabs, fabsf
#define USING_LOG_PREFIX COMMON
#include "common/object/ob_object.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "lib/checksum/ob_crc64.h"
#include "common/object/ob_obj_compare.h"
#include "common/ob_action_flag.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/number/ob_number_v2.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/string/ob_sql_string.h"
#include "lib/worker.h"
#include "common/object/ob_obj_funcs.h"
#include "lib/charset/ob_charset.h"

using namespace oceanbase;
using namespace oceanbase::common;

bool ObLobId::operator==(const ObLobId &other) const
{
  return tablet_id_ == other.tablet_id_ && lob_id_ == other.lob_id_;
}

bool ObLobId::operator!=(const ObLobId &other) const
{
  return !(operator==(other));
}

bool ObLobId::operator <(const ObLobId &other) const
{
  bool bool_ret = false;

  if (tablet_id_ < other.tablet_id_) {
    bool_ret= true;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret = false;
  } else if (lob_id_ < other.lob_id_) {
    bool_ret= true;
  } else if (lob_id_ > other.lob_id_) {
    bool_ret = false;
  }

  return bool_ret;
}

bool ObLobId::operator >(const ObLobId &other) const
{
  bool bool_ret = false;

  if (tablet_id_ < other.tablet_id_) {
    bool_ret = false;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret= true;
  } else if (lob_id_ < other.lob_id_) {
    bool_ret = false;
  } else if (lob_id_ > other.lob_id_) {
    bool_ret= true;
  }

  return bool_ret;

}

void ObLobId::reset()
{
  tablet_id_ = 0;
  lob_id_ = 0;
}

void ObLobCommon::reset()
{
  if (is_init_) {
    ObLobData *lob_data = reinterpret_cast<ObLobData*>(buffer_);
    lob_data->reset();
  }
  version_ = LOB_DATA_VERSION;
  is_init_ = 0;
  is_empty_ = 0;
  in_row_ = 1;
  opt_encrypt_ = 0;
  opt_compress_ = 0;
  opt_deduplicate_ = 0;
  has_content_type_ = 0;
  reserve_ = 0;
}

bool ObLobCommon::operator==(const ObLobCommon &other) const
{
  bool ret = version_ == other.version_ && is_init_ == other.is_init_ &&
    is_empty_ == other.is_empty_ && in_row_ == other.in_row_ && opt_encrypt_ == other.opt_encrypt_ &&
    opt_compress_ == other.opt_compress_ && opt_deduplicate_ == other.opt_deduplicate_ &&
    has_content_type_ == other.has_content_type_;
  if (ret) {
    if (is_init_) {
      const ObLobData *lob_data = reinterpret_cast<const ObLobData*>(buffer_);
      const ObLobData *other_lob_data = reinterpret_cast<const ObLobData*>(other.buffer_);
      ret = (*lob_data) == (*other_lob_data);
    }
  }
  return ret;
}

bool ObLobCommon::operator!=(const ObLobCommon &other) const
{
  return !(operator==(other));
}

void ObLobData::reset()
{
  id_.reset();
  byte_size_ = 0;
}

bool ObLobData::operator==(const ObLobData &other) const
{
  return id_ == other.id_ && byte_size_ == other.byte_size_;
}

bool ObLobData::operator!=(const ObLobData &other) const
{
  return !(operator==(other));
}

int ObLobLocator::init(const uint64_t table_id,
                       const uint32_t column_id,
                       const int64_t snapshot_version,
                       const uint16_t flags,
                       const ObString &rowid,
                       const ObString &payload)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid_id(table_id)
              || !is_valid_id(column_id) || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument to init ObLobLocator",
               K(table_id), K(column_id), K(snapshot_version), K(rowid), K(payload));
  } else {
    magic_code_ = MAGIC_CODE;
    version_ = LOB_LOCATOR_VERSION;
    snapshot_version_ = snapshot_version;
    table_id_ = table_id;
    column_id_ = column_id;
    option_ = 0;
    flags_ = flags;
    if (rowid.empty()) {
      // for old heap table withou rowid
      set_compat_mode();
      payload_offset_ = 0;
    } else {
      set_inline_mode();
      payload_offset_ = rowid.length();
      MEMCPY(data_, rowid.ptr(), rowid.length());
    }
    if (OB_NOT_NULL(payload.ptr())) {
      MEMCPY(data_ + payload_offset_, payload.ptr(), payload.length());
      payload_size_ = payload.length();
    } else {
      payload_size_ = 0;
    }
  }

  return ret;
}

int ObLobLocator::init(const ObString &payload)
{
  int ret = OB_SUCCESS;
  magic_code_ = MAGIC_CODE;
  version_ = LOB_LOCATOR_VERSION;
  snapshot_version_ = 0;
  table_id_ = 0;
  column_id_ = 0;
  option_ = 0;
  flags_ = LOB_DEFAULT_FLAGS;
  set_compat_mode();
  payload_offset_ = 0;
  if (OB_NOT_NULL(payload.ptr())) {
    MEMCPY(data_ + payload_offset_, payload.ptr(), payload.length());
    payload_size_ = payload.length();
  } else {
    payload_size_ = 0;
  }
  return ret;
}

int ObLobLocator::get_rowid(ObString &rowid) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObLobLocator is not init", K(ret), K(*this));
  } else if (!is_inline_mode()) {
    ret = OB_NOT_SUPPORTED;
    COMMON_LOG(WARN, "ObLobLocator with compat mode does not support rowid ", K(ret), K(*this));
  } else if (payload_offset_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Unexcepted payload offset to get rowid", K(ret), K(*this));
  } else {
    rowid = ObString(payload_offset_, data_);
  }
  return ret;
}

int ObLobLocator::get_payload(ObString &payload) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "ObLobLocator is not init", K(ret), K(*this));
  } else if(payload_size_ > 0) {
    payload.assign_ptr(data_ + payload_offset_, payload_size_);
  } else {
    payload.reset();
  }
  return ret;
}

DEF_TO_STRING(ObLobLocator)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(magic_code),
       K_(version),
       K_(snapshot_version),
       K_(table_id),
       K_(column_id),
       K_(flags),
       K_(option),
       K_(payload_offset),
       K_(payload_size));
  J_COMMA();
  if (buf_len > pos && is_valid()) {
    int64_t max_len = buf_len - pos;
    ObString payload(MIN(payload_size_, max_len), get_payload_ptr());
    J_KV("data", payload);
  } else {
    J_KV(K_(data));
  }
  J_OBJ_END();
  return pos;
}

DEF_TO_STRING(ObLobLocatorV2)
{
  int64_t pos = 0;
  uint32_t offset = 0;
  J_OBJ_START();
  J_KV(K_(ptr), K_(size), K_(has_lob_header));
  J_COMMA();

  if (OB_ISNULL(ptr_)) {
    // do-nothing
  } else if (is_lob_disk_locator() && size_ >= sizeof(ObLobCommon)) {
    ObLobCommon *loc = reinterpret_cast<ObLobCommon *>(ptr_);
    J_KV(K(*loc));
  } else if (has_lob_header_ && size_ >= MEM_LOB_COMMON_HEADER_LEN) {
    ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
    offset += MEM_LOB_COMMON_HEADER_LEN;
    J_KV(K(loc));
    J_COMMA();
    if (is_valid()) {
      if (buf_len > pos) {
        if (loc->has_extern()) {
          ObMemLobExternHeader *extern_header = reinterpret_cast<ObMemLobExternHeader *>(ptr_ + offset);
          offset += MEM_LOB_EXTERN_HEADER_LEN;
          J_KV(K(*extern_header));
          J_COMMA();
          J_KV("extern size", *(uint16_t *)extern_header->data_);
          J_COMMA();
          if (buf_len > pos && extern_header->flags_.has_tx_info_
              && size_ >= offset + MEM_LOB_EXTERN_TXINFO_LEN) {
            ObMemLobTxInfo *tx_info = reinterpret_cast<ObMemLobTxInfo *>(ptr_ + offset);
            offset += MEM_LOB_EXTERN_TXINFO_LEN;
            J_KV(K(*tx_info));
            J_COMMA();
          }
          if (buf_len > pos && extern_header->flags_.has_location_info_
              && size_ >= offset + MEM_LOB_EXTERN_LOCATIONINFO_LEN) {
            ObMemLobLocationInfo *location_info = reinterpret_cast<ObMemLobLocationInfo *>(ptr_ + offset);
            offset += MEM_LOB_EXTERN_LOCATIONINFO_LEN;
            J_KV(K(*location_info));
            J_COMMA();
          }
          if (buf_len > pos) {
            ObString rowkey_str(MIN(extern_header->rowkey_size_, buf_len - pos), ptr_ + offset);
            offset += extern_header->rowkey_size_;
            J_KV("rowkey", rowkey_str);
            J_COMMA();
          }
          if (buf_len > pos) {
            ObString payload_str(MIN(size_ - offset, buf_len - pos), ptr_ + offset);
            J_KV("disk locator", payload_str);
          }
        } else {
           ObString payload_str(MIN(size_ - offset, buf_len - pos), ptr_ + offset);
           J_KV("inrow data", payload_str);
        }
      }
    } else {
      ObString payload_str(MIN(size_ - offset, buf_len - pos), ptr_ + offset);
      J_KV("inrow", payload_str);
    }
  } else if (has_lob_header_ && size_ < MEM_LOB_COMMON_HEADER_LEN) {
    ObString payload_str(MIN(size_ - offset, buf_len - pos), ptr_ + offset);
    J_KV("content", payload_str);
  } else if (!has_lob_header_) {
    ObString payload_str(MIN(size_ - offset, buf_len - pos), ptr_ + offset);
    J_KV("compatable inrow data", payload_str);
  }

  J_OBJ_END();
  return pos;
}

// Notice: disk_lob_full_size = (disk locator header size if any) + inline buffer
uint32_t ObLobLocatorV2::calc_locator_full_len(const ObMemLobExternFlags &flags,
                                               uint32_t rowkey_size,
                                               uint32_t disk_lob_full_size,
                                               bool is_simple)
{
  uint32_t loc_len = MEM_LOB_COMMON_HEADER_LEN;
  if (!flags.is_empty()) {
    loc_len += MEM_LOB_EXTERN_HEADER_LEN;
    loc_len += MEM_LOB_EXTERN_SIZE_LEN;
    if (flags.has_tx_info_) {
      loc_len += MEM_LOB_EXTERN_TXINFO_LEN;
    }
    if (flags.has_location_info_) {
      loc_len += MEM_LOB_EXTERN_LOCATIONINFO_LEN;
    }
    loc_len += MEM_LOB_ADDR_LEN; //ToDo:@gehao server address.
    loc_len += rowkey_size;
  }
  if (is_simple) {
    loc_len += disk_lob_full_size;
  } else {
    loc_len += disk_lob_full_size;
    if (disk_lob_full_size != 0) {
      OB_ASSERT(disk_lob_full_size >= sizeof(ObLobCommon));
    } else {
      loc_len += sizeof(ObLobCommon);
    }
  }
  return loc_len;
}

// fill mem header & rowkey
int ObLobLocatorV2::fill(ObMemLobType type,
                         const ObMemLobExternFlags &flags,
                         const ObString &rowkey_str,
                         const ObLobCommon *disk_loc,
                         uint32_t disk_lob_full_size,
                         uint32_t disk_lob_header_size,
                         bool is_simple)
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)
      || (type <= INVALID_LOB || type >= MAX_LOB_TYPE)
      || size_ < MEM_LOB_COMMON_HEADER_LEN) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: invalid argument",
      K(ret), K(type), KP(ptr_), K(size_), KP(disk_loc), K(disk_lob_full_size), K(is_simple));
  } else {
    uint32_t offset = 0;
    ObMemLobCommon *loc = new (ptr_ + offset) ObMemLobCommon(type, is_simple);
    loc->lob_common_.is_mem_loc_ = 1;
    loc->set_read_only(false);
    offset += MEM_LOB_COMMON_HEADER_LEN;
    if (flags.is_empty()) {
      // if no extern segment:
      // 1. simple case (only tinytext currently) only ObMemLobCommon and payload without disklocator
      // 2. not simple case:
      //  2.1 payload is empty, disk_lob_full_size should be 0, need to mock a disklocator
      //  2.2 payload is not empty, must has disklocator
      if (is_simple) {
        offset += disk_lob_full_size;
      } else if (disk_lob_full_size != 0) {
        offset += disk_lob_full_size;
        if (disk_lob_full_size < sizeof(ObLobCommon)) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid buffer size for disk locator",
            K(ret), K(type), K(disk_lob_full_size), K(sizeof(ObLobCommon)));
        }
      } else if (disk_lob_full_size == 0) {
        offset += sizeof(ObLobCommon);
      }
    } else {
      loc->set_extern(true);
      if (OB_UNLIKELY(offset + MEM_LOB_EXTERN_HEADER_LEN + MEM_LOB_EXTERN_SIZE_LEN > size_)) {
        ret = OB_BUF_NOT_ENOUGH;
        COMMON_LOG(WARN, "Lob: invalid buffer size for extern header",
          K(ret), K(type), KP(offset), K(size_));
      } else {
        ObMemLobExternHeader *extern_header =
          new (ptr_ + offset) ObMemLobExternHeader(flags, rowkey_str.length());
        offset += MEM_LOB_EXTERN_HEADER_LEN;

        uint16_t *extern_len = reinterpret_cast<uint16_t *>(ptr_ + offset);
        offset += MEM_LOB_EXTERN_SIZE_LEN;
        *extern_len = 0;

        if (flags.has_tx_info_) {
          offset += MEM_LOB_EXTERN_TXINFO_LEN;
          *extern_len += MEM_LOB_EXTERN_TXINFO_LEN;
        }
        if (flags.has_location_info_) {
          offset += MEM_LOB_EXTERN_LOCATIONINFO_LEN;
          *extern_len += MEM_LOB_EXTERN_LOCATIONINFO_LEN;
        }

        if ((offset + rowkey_str.length()) && OB_UNLIKELY(offset > size_)) {
          ret = OB_BUF_NOT_ENOUGH;
          COMMON_LOG(WARN, "Lob: invalid buffer size for rowkey",
            K(ret), K(type), KP(offset), K(size_), K(flags), K(rowkey_str.length()));
        } else {
          MEMCPY(ptr_ + offset, rowkey_str.ptr(), rowkey_str.length());
          offset += rowkey_str.length();
        }

        if (OB_FAIL(ret)) {
        } else if (disk_lob_full_size == 0) {
          extern_header->payload_offset_ = (offset - MEM_LOB_COMMON_HEADER_LEN - MEM_LOB_EXTERN_HEADER_LEN);
          extern_header->payload_size_ = 0;
        } else if (OB_ISNULL(disk_loc)) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: building mem-loblocator has externs without disk locator",
            K(ret), K(type), KP(offset), K(size_), K(flags), K(rowkey_str.length()));
        } else if (disk_lob_full_size < sizeof(ObLobCommon)) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid buffer size for disk locator",
            K(ret), K(type), K(disk_lob_full_size), K(sizeof(ObLobCommon)));
        } else {
          uint32_t disk_loc_header_size = sizeof(ObLobCommon);
          if (disk_loc->in_row_) {
            if (disk_loc->is_init_) {
              disk_loc_header_size += sizeof(ObLobData);
            }
          } else if (disk_lob_header_size != 0) {
            disk_loc_header_size = disk_lob_header_size;
          } else {
            int64_t tbz = disk_loc->get_byte_size(disk_lob_full_size);
            int64_t thz = disk_loc->get_handle_size(tbz);
            disk_loc_header_size = thz;
          }
          if (offset + disk_loc_header_size > size_ || disk_lob_full_size < disk_loc_header_size) {
            ret = OB_INVALID_ARGUMENT;
            COMMON_LOG(WARN, "Lob: invalid disk locator",
              K(ret), K(type), K(offset), K(size_), K(flags), K(disk_lob_header_size),
              K(disk_loc_header_size), K(disk_lob_full_size), K(*disk_loc));
          } else {
            offset += disk_loc_header_size;
            // offset of disk locator inrow payload
            extern_header->payload_offset_ = (offset - MEM_LOB_COMMON_HEADER_LEN - MEM_LOB_EXTERN_HEADER_LEN);
            extern_header->payload_size_ = disk_lob_full_size - disk_loc_header_size; // size with lob full size
            offset += extern_header->payload_size_;
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(offset > size_)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(WARN, "Lob: invalid buffer size for disk data",
        K(ret), K(type), KP(offset), K(size_), K(flags), K(disk_lob_full_size));
    }
  }
  return ret;
}

int ObLobLocatorV2::copy(const ObLobLocatorV2* src_locator) const
{
  return OB_NOT_IMPLEMENT;
}

int ObLobLocatorV2:: get_mem_locator(ObMemLobCommon *&mem_loc) const
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_))) {
    ret = OB_ERR_NULL_VALUE;
  } else if (is_lob_disk_locator()) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    mem_loc = reinterpret_cast<ObMemLobCommon*>(ptr_);
  }
  return ret;
}

int ObLobLocatorV2::get_extern_header(ObMemLobExternHeader *&extern_header) const
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon*>(ptr_);
  if (OB_UNLIKELY(!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_))) {
    ret = OB_ERR_NULL_VALUE;
  } else if (is_lob_disk_locator()) {
    ret = OB_ERR_NULL_VALUE;
  } else if (!loc->has_extern()) {
    ret = OB_ERR_NULL_VALUE;
  } else {
    extern_header = reinterpret_cast<ObMemLobExternHeader *>(loc->data_);
  }
  return ret;
}

int ObLobLocatorV2::get_rowkey(ObString &rowkey_str) const
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobExternHeader *extern_header = NULL;
  if (OB_FAIL(get_extern_header(extern_header))) {
  } else {
    uint16_t extern_body_size = *((uint16_t *)(extern_header->data_));
    rowkey_str.assign(extern_header->data_ + MEM_LOB_EXTERN_SIZE_LEN + extern_body_size,
                      extern_header->rowkey_size_);
  }
  return ret;
}

int ObLobLocatorV2::get_disk_locator(ObLobCommon *&disk_loc) const
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (OB_UNLIKELY(!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_))) {
    ret = OB_ERR_NULL_VALUE;
    COMMON_LOG(WARN, "Lob: try to get disk locator without lob header", K(ret));
  } else if (is_lob_disk_locator()) {
    disk_loc = reinterpret_cast<ObLobCommon *>(ptr_);
  } else if (loc->is_simple()) {
    ret = OB_ERR_NULL_VALUE;
    COMMON_LOG(WARN, "Lob: simple lob locator does not has disk locator", K(ret));
  } else if (loc->has_extern()) {
    ObString rowkey_str;
    if (OB_FAIL(get_rowkey(rowkey_str))) {
      COMMON_LOG(WARN, "Lob: get rowkey failed", K(ret));
    } else {
      disk_loc = reinterpret_cast<ObLobCommon *>(rowkey_str.ptr() + rowkey_str.length());
    }
  } else { // not simple, no extern
    disk_loc = reinterpret_cast<ObLobCommon *>(loc->data_);
  }
  return ret;
}

int ObLobLocatorV2::get_disk_locator(ObString &disc_loc_buff) const
{
  int ret = OB_SUCCESS;
  ObLobCommon *disk_loc = NULL;
  if (OB_FAIL(get_disk_locator(disk_loc))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret));
  } else {
    int64_t handle_size = reinterpret_cast<intptr_t>(disk_loc) - reinterpret_cast<intptr_t>(ptr_);
    if (handle_size > size_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid handle size", K(ret), K(size_), K(disk_loc), K(ptr_));
    } else {
      if (disk_loc->in_row_) {
        handle_size = size_ - handle_size;
      } else {
        handle_size = disk_loc->get_handle_size(0);
      }
      disc_loc_buff.assign_ptr(reinterpret_cast<const char *>(disk_loc), handle_size);
    }
  }
  return ret;
}

int ObLobLocatorV2::get_inrow_data(ObString &inrow_data) const
{
  int ret =  OB_SUCCESS;
  ObString disk_loc_buff;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (!has_lob_header_ || size_ == 0) {
    inrow_data.assign_ptr(ptr_, size_);
  } else if (OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get null ptr", K(ret), K(size_), K(ptr_));
  } else if (is_freed()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: has been freed", K(ret), KPC(loc));
  } else if (!is_lob_disk_locator() && loc->is_simple()) {
    inrow_data.assign_ptr(ptr_ + MEM_LOB_COMMON_HEADER_LEN, size_ - MEM_LOB_COMMON_HEADER_LEN);
  } else if (OB_FAIL(get_disk_locator(disk_loc_buff))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret));
  } else {
    ObLobCommon *disk_loc = reinterpret_cast<ObLobCommon *>(disk_loc_buff.ptr());
    if (disk_loc->in_row_) {
      if (!is_lob_disk_locator() && !loc->has_inrow_data()) { // never disk locator inrow, but mem locator outrow
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid outrow data", K(ret));
      } else {
        inrow_data.assign_ptr(disk_loc->get_inrow_data_ptr(), disk_loc->get_byte_size(disk_loc_buff.length()));
      }
    } else if (!is_lob_disk_locator() && has_inrow_data()) {
      if (has_extern()) {
        ObMemLobExternHeader *ext_header = nullptr;
        if (OB_FAIL(get_extern_header(ext_header))) {
          COMMON_LOG(WARN, "Lob: fail to get extern header", K(ret));
        } else if (ext_header->payload_offset_ + ext_header->payload_size_ > size_) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid payload data", K(ret), K(*ext_header), K(size_));
        } else {
          inrow_data.assign_ptr(ext_header->data_ + ext_header->payload_offset_, ext_header->payload_size_);
        }
      } else { // no extern [mem lob common][lob common][inrow data]
        int64_t handle_offset = reinterpret_cast<intptr_t>(disk_loc) - reinterpret_cast<intptr_t>(ptr_);
        int64_t byte_size = disk_loc->get_byte_size(disk_loc_buff.length());
        int64_t handle_size = disk_loc->get_handle_size(byte_size);
        if (byte_size + handle_size + handle_offset > size_) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid inrow data", K(ret), K(byte_size), K(handle_size),
                     K(*disk_loc), K(handle_offset), K(size_));
        } else {
          inrow_data.assign_ptr(disk_loc_buff.ptr() + handle_size, byte_size);
        }
      }
    } else { // out row
      ret = OB_ERR_NULL_VALUE;
      COMMON_LOG(WARN, "Lob: Maybe a bug, get inrow data of outrow lob", K(ret), K(lbt()));
    }
  }
  if (OB_SUCC(ret) && inrow_data.length() == 0 && lib::is_oracle_mode()) {
    // Compatible with null string without header (old impliemnt of orale empty lob)
    // refer to mysqltest regula_expression_sqlqa.regular_replace_mysql
    inrow_data.assign_ptr(NULL, 0);
  }
  return ret;
}

bool ObLobLocatorV2::is_inrow() const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  ObString disk_loc_buff;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_)) {
    bret = true;
  } else if (!is_lob_disk_locator() && loc->is_simple()) {
    bret = true;
  } else if (OB_FAIL(get_disk_locator(disk_loc_buff))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret));
  } else {
    ObLobCommon *disk_loc = reinterpret_cast<ObLobCommon *>(disk_loc_buff.ptr());
    bret = disk_loc->in_row_;
  }
  return bret;
}

bool ObLobLocatorV2::is_empty_lob() const
{
  int ret =  OB_SUCCESS;
  bool bret = false;
  ObString disk_loc_buff;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_)) {
    bret = (size_ == 0);
  } else if (!is_lob_disk_locator() && loc->is_simple()) {
    bret = (size_ - MEM_LOB_COMMON_HEADER_LEN == 0);
  } else if (OB_FAIL(get_disk_locator(disk_loc_buff))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret));
  } else {
    ObLobCommon *disk_loc = reinterpret_cast<ObLobCommon *>(disk_loc_buff.ptr());
    if (disk_loc->in_row_) {
      bret = (disk_loc->get_byte_size(disk_loc_buff.length()) == 0);
    }
  }
  return (ret == OB_SUCCESS ? bret : false);
}

int ObLobLocatorV2::get_lob_data_byte_len(int64_t &len) const
{
  int ret =  OB_SUCCESS;
  ObString disk_loc_buff;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_)) {
    len = size_;
  } else if (!is_lob_disk_locator() && loc->is_simple()) {
    len = size_ - MEM_LOB_COMMON_HEADER_LEN;
  } else if (OB_FAIL(get_disk_locator(disk_loc_buff))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret));
  } else {
    ObLobCommon *disk_loc = reinterpret_cast<ObLobCommon *>(disk_loc_buff.ptr());
    len = disk_loc->get_byte_size(disk_loc_buff.length());
  }
  return ret;
}

int ObLobLocatorV2::get_table_info(uint64_t &table_id, uint32_t &column_idx)
{
  int ret = OB_SUCCESS;
  ObMemLobExternHeader *extern_header = NULL;
  if (OB_SUCC(get_extern_header(extern_header))) {
    table_id = extern_header->table_id_;
    column_idx = extern_header->column_idx_;
  }
  return ret;
}

int ObLobLocatorV2::get_tx_info(ObMemLobTxInfo *&tx_info) const
{
  int ret =  OB_SUCCESS;
  ObMemLobExternHeader *extern_header = NULL;
  if (OB_SUCC(get_extern_header(extern_header))) {
    if (extern_header->flags_.has_tx_info_) {
      tx_info = reinterpret_cast<ObMemLobTxInfo *>(extern_header->data_ + MEM_LOB_EXTERN_SIZE_LEN);
    } else {
      ret = OB_ERR_NULL_VALUE;
      COMMON_LOG(WARN, "Lob: does not have tx info", K(this), K(ret));
    }
  }
  return ret;
}

int ObLobLocatorV2::get_location_info(ObMemLobLocationInfo *&location_info) const
{
  int ret =  OB_SUCCESS;
  ObMemLobExternHeader *extern_header = NULL;
  if (OB_SUCC(get_extern_header(extern_header))) {
    char *cur_pos = extern_header->data_ + MEM_LOB_EXTERN_SIZE_LEN;
    if (extern_header->flags_.has_tx_info_) {
      cur_pos += MEM_LOB_EXTERN_TXINFO_LEN;
    }
    if (extern_header->flags_.has_location_info_) {
      location_info = reinterpret_cast<ObMemLobLocationInfo *>(cur_pos);
    } else {
      ret = OB_ERR_NULL_VALUE;
      COMMON_LOG(WARN, "Lob: does not have location info", K(this), K(ret));
    }
  }
  return ret;
}

int ObLobLocatorV2::get_real_locator_len(int64_t &real_len) const
{
  int ret = OB_SUCCESS;
  ObLobCommon *disk_loc = NULL;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  real_len = size_;
  if (!has_lob_header_ || size_ == 0 || OB_ISNULL(ptr_)) {
  } else if (!is_lob_disk_locator() && loc->is_simple_) {
  } else if (OB_FAIL(get_disk_locator(disk_loc))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed", K(ret), K(*this));
  } else {
    real_len = (uintptr_t)disk_loc - (uintptr_t)ptr_;
    if (disk_loc->in_row_) {
      real_len += sizeof(ObLobCommon);
      if (disk_loc->is_init_) {
        real_len += sizeof(ObLobData);
      }
    } else {
      real_len += disk_loc->get_handle_size(0);
    }
  }
  return ret;
}

// Notice: this payload is payload with disk locator if it exist
int ObLobLocatorV2::set_payload_data(const ObString& payload)
{
  OB_ASSERT(has_lob_header_); // only used in build_lob_locator_v2, must has lob header
  int ret = OB_SUCCESS;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (size_ == 0 || OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get null ptr", K(ret), K(size_), K(ptr_));
  } else if (loc->lob_common_.is_mem_loc_ != 1) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get disk loc", K(ret), K(loc->lob_common_));
  } else {
    uint32_t buf_len = size_ - (loc->data_ - ptr_);
    if (!loc->has_extern()) {
      OB_ASSERT(payload.length() == buf_len); // debug
      MEMCPY(loc->data_, payload.ptr(), buf_len);
    } else {
      ObString disk_loc_buff;
      if (OB_SUCC(get_disk_locator(disk_loc_buff))) {
        buf_len = size_ - (disk_loc_buff.ptr() - ptr_);
        OB_ASSERT(payload.length() == buf_len);
        MEMCPY(disk_loc_buff.ptr(), payload.ptr(), payload.length());
      }
    }
  }
  return ret;
}

int ObLobLocatorV2::set_payload_data(const ObLobCommon *lob_comm, const ObString& payload)
{
  OB_ASSERT(has_lob_header_); // only used for fill temp lob header or default value lob locater v2
  int ret = OB_SUCCESS;
  uint32_t buf_len = 0;
  ObMemLobCommon *loc = reinterpret_cast<ObMemLobCommon *>(ptr_);
  if (size_ == 0 || OB_ISNULL(ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get null ptr", K(ret), K(size_), K(ptr_));
  } else if (loc->lob_common_.is_mem_loc_ != 1) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get disk loc", K(ret), K(loc->lob_common_));
  } else if (loc->is_simple()) {
    buf_len = size_ - (loc->data_ - ptr_);
    OB_ASSERT(payload.length() <= buf_len);
    if (payload.length() > 0) {
      MEMCPY(loc->data_, payload.ptr(), buf_len);
    }
  } else {
    char *buf = NULL;
    ObString disk_loc_buff;
    if (loc->has_extern()) {
      if (OB_SUCC(get_disk_locator(disk_loc_buff))) {
        buf = disk_loc_buff.ptr();
        buf_len = (size_ - (disk_loc_buff.ptr() - ptr_));
      }
    } else if (!loc->has_extern()) {
      buf = loc->data_;
      buf_len = size_ - (loc->data_ - ptr_);
    }
    if (OB_SUCC(ret)) {
      uint32 disk_lob_header_len = sizeof(ObLobCommon);
      if (lob_comm->in_row_) {
        disk_lob_header_len += lob_comm->is_init_ ? sizeof(ObLobData) : 0;
      } else {
        disk_lob_header_len = lob_comm->get_handle_size(0);
      }
      OB_ASSERT(payload.length() + disk_lob_header_len <= buf_len);
      MEMCPY(buf, lob_comm, disk_lob_header_len);
      if (payload.length() > 0) {
        MEMCPY(buf + disk_lob_header_len, payload.ptr(), payload.length());
      }
    }
  }
  return ret;
}

int ObLobLocatorV2::set_table_info(const uint64_t &table_id, const uint32_t &column_idx)
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobExternHeader *extern_header = NULL;
  if (OB_SUCC(get_extern_header(extern_header))) {
    extern_header->table_id_ = table_id;
    extern_header->column_idx_ = column_idx;
  }
  return ret;
}

int ObLobLocatorV2::set_tx_info(const ObMemLobTxInfo &tx_info)
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobTxInfo *tx_info_ptr = NULL;
  if (OB_SUCC(get_tx_info(tx_info_ptr))) {
    *tx_info_ptr = tx_info;
  }
  return ret;
}

int ObLobLocatorV2::set_location_info(const ObMemLobLocationInfo &location_info)
{
  validate_has_lob_header(has_lob_header_);
  int ret = OB_SUCCESS;
  ObMemLobLocationInfo *loc_info_ptr = NULL;
  if (OB_SUCC(get_location_info(loc_info_ptr))) {
    *loc_info_ptr = location_info;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObLobLocatorV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (new_pos + size_ + sizeof(size_) + sizeof(has_lob_header_) > buf_len) {
    ret = OB_SERIALIZE_ERROR;
  } else {
    *reinterpret_cast<uint32_t*>(buf + new_pos) = size_;
    new_pos += sizeof(size_);
    *reinterpret_cast<bool*>(buf + new_pos) = has_lob_header_;
    new_pos += sizeof(has_lob_header_);
    MEMCPY(buf + new_pos, ptr_, size_);
    new_pos += size_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLobLocatorV2)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (new_pos + size_ + sizeof(size_) + sizeof(has_lob_header_) > data_len) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    size_ = *reinterpret_cast<const uint32_t*>(buf + new_pos);
    new_pos += sizeof(size_);
    has_lob_header_ = *reinterpret_cast<const uint32_t*>(buf + new_pos);
    new_pos += sizeof(has_lob_header_);
    ptr_ = const_cast<char*>(buf + new_pos);
    new_pos += size_;
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLobLocatorV2)
{
  return size_ + sizeof(size_) + sizeof(has_lob_header_);
}

#define PRINT_META()
//#define PRINT_META() BUF_PRINTO(obj.get_meta()); J_COLON();

const char *ObObj::MIN_OBJECT_VALUE_STR       = "__OB__MIN__";
const char *ObObj::MAX_OBJECT_VALUE_STR       = "__OB__MAX__";
const char *ObObj::NOP_VALUE_STR = "__OB__NOP__";
const char OB_JSON_NULL[2] = {'\0', '\0'}; // binary json null

OB_SERIALIZE_MEMBER(ObDataType, meta_, accuracy_, is_zero_fill_);
OB_SERIALIZE_MEMBER(ObEnumSetInnerValue, numberic_value_, string_value_);

DEFINE_SERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  OB_UNIS_ENCODE(cs_level_);
  OB_UNIS_ENCODE(cs_type_);
  OB_UNIS_ENCODE(scale_);
  return ret;
}

DEFINE_DESERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  OB_UNIS_DECODE(cs_level_);
  OB_UNIS_DECODE(cs_type_);
  OB_UNIS_DECODE(scale_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObjMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  OB_UNIS_ADD_LEN(cs_level_);
  OB_UNIS_ADD_LEN(cs_type_);
  OB_UNIS_ADD_LEN(scale_);
  return len;
}

////////////////////////////////////////////////////////////////

bool ObObj::is_zero() const
{
  bool ret = is_numeric_type() || is_interval_ym() || is_interval_ds();
  if (ret) {
    // float/double comparison using "==" or "!=" matches MySQL
    // and Oracle doesn't support raw float/double
    switch(meta_.get_type()) {
      case ObTinyIntType:
        // fall through
      case ObSmallIntType:
        // fall through
      case ObMediumIntType:
        // fall through
      case ObInt32Type:
        // fall through
      case ObIntType:
        ret = (0 == v_.int64_);
        break;
      case ObUTinyIntType:
        // fall through
      case ObUSmallIntType:
        // fall through
      case ObUMediumIntType:
        // fall through
      case ObUInt32Type:
        // fall through
      case ObUInt64Type:
        ret = (0 == v_.uint64_);
        break;
      //Please do not bother yourself too much to take +0 and -0 into consideration
      //According to the IEEE754 standard, +0 equals to -0
      //https://en.wikipedia.org/wiki/Signed_zero
      case ObFloatType:
        ret = (0 == v_.float_);
        break;
      case ObDoubleType:
        ret = (0 == v_.double_);
        break;
      case ObUFloatType:
        ret = (0 == v_.float_);
        break;
      case ObUDoubleType:
        ret = (0 == v_.double_);
        break;
      case ObNumberType:
        // fall through
      case ObUNumberType:
      case ObNumberFloatType: {
        ret = is_zero_number();
        break;
      }
      case ObBitType: {
        ret = (0 == v_.uint64_);
        break;
      }
      case ObIntervalDSType : {
        ret = (0 == v_.nsecond_ && 0 == interval_fractional_);
        break;
      }
      case ObIntervalYMType : {
        ret = (0 == v_.nmonth_);
        break;
      }
      default:
        BACKTRACE(ERROR, true, "unexpected numeric type=%u", meta_.get_type());
        right_to_die_or_duty_to_live();
    }
  }
  return ret;
}

int ObObj::build_not_strict_default_value()
{
  int ret = OB_SUCCESS;
  const ObObjType &data_type = meta_.get_type();
  switch(data_type) {
    case ObTinyIntType:
      set_tinyint(0);
      break;
    case ObSmallIntType:
      set_smallint(0);
      break;
    case ObMediumIntType:
      set_mediumint(0);
      break;
    case ObInt32Type:
      set_int32(0);
      break;
    case ObIntType:
      set_int(0);
      break;
    case ObUTinyIntType:
      set_utinyint(0);
      break;
    case ObUSmallIntType:
      set_usmallint(0);
      break;
    case ObUMediumIntType:
      set_umediumint(0);
      break;
    case ObUInt32Type:
      set_uint32(0);
      break;
    case ObUInt64Type:
      set_uint64(0);
      break;
    case ObFloatType:
      set_float(0);
      break;
    case ObDoubleType:
      set_double(0);
      break;
    case ObUFloatType:
      set_ufloat(0);
      break;
    case ObUDoubleType:
      set_udouble(0);
      break;
    case ObNumberType: {
      number::ObNumber zero;
      zero.set_zero();
      set_number(zero);
      break;
    }
    case ObUNumberType: {
      number::ObNumber zero;
      zero.set_zero();
      set_unumber(zero);
      break;
    }
    case ObDateTimeType:
      set_datetime(ObTimeConverter::ZERO_DATETIME);
      break;
    case ObTimestampType:
      set_timestamp(ObTimeConverter::ZERO_DATETIME);
      break;
    case ObDateType:
      set_date(ObTimeConverter::ZERO_DATE);
      break;
    case ObTimeType:
      set_time(0);
      break;
    case ObYearType:
      set_year(0);
      break;
    case ObVarcharType: {
        ObString null_str;
        set_varchar(null_str);
      }
      break;
    case ObCharType: {
        ObString null_str;
        set_char(null_str);
      }
      break;
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType:
    case ObGeometryType: {
        ObString null_str;
        set_string(data_type, null_str);
        meta_.set_inrow();
      }
      break;
    case ObJsonType: {
        set_json_value(data_type, OB_JSON_NULL, 2);
      }
      break;
    case ObBitType:
      set_bit(0);
      break;
    case ObEnumType:
      set_enum(1);
      break;
    case ObSetType:
      set_set(0);
      break;
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      set_otimestamp_null(data_type);
      break;
    }
    case ObRawType: {
        ObString null_str;
        set_raw(null_str);
      break;
    }
    case ObIntervalYMType: {
      const ObIntervalYMValue empty_value;
      set_interval_ym(empty_value);
      break;
    }
    case ObIntervalDSType: {
      const ObIntervalDSValue empty_value;
      set_interval_ds(empty_value);
      break;
    }
    case ObNumberFloatType: {
      number::ObNumber zero;
      zero.set_zero();
      set_number_float(zero);
      break;
    }
    case ObURowIDType: {
      ObURowIDData urowid_data;
      set_urowid(urowid_data);
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      _OB_LOG(WARN, "unexpected data type=%u", data_type);
  }
  return ret;
}

int ObObj::deep_copy(const ObObj &src, char *buf, const int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (ob_is_string_type(src.get_type()) || ob_is_json(src.get_type()) || ob_is_geometry(src.get_type())) {
    ObString src_str = src.get_string();
    if (OB_UNLIKELY(size < (pos + src_str.length()))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src_str.ptr(), src_str.length());
      *this = src;
      this->set_string(src.get_type(), buf + pos, src_str.length());
      // set_string lose orign cs_level
      if (CS_LEVEL_INVALID != src.get_collation_level()) {
        this->set_collation_level(src.get_collation_level());
      }
      pos += src_str.length();
    }
  } else if (ob_is_raw(src.get_type())) {
    const ObString &src_str = src.get_string();
    if (OB_UNLIKELY(size < (pos + src_str.length()))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src_str.ptr(), src_str.length());
      *this = src;
      this->set_raw(buf + pos, src_str.length());
      pos += src_str.length();
    }
  } else if (ob_is_number_tc(src.get_type())) {
    const int64_t number_size =  src.get_number_byte_length();
    if (OB_UNLIKELY(size < (int64_t)(pos + number_size))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src.get_number_digits(), number_size);
      *this = src;
      this->set_number(src.get_type(), src.get_number_desc(), (uint32_t *)(buf + pos));
      pos += number_size;
    }
  } else if (ob_is_rowid_tc(src.get_type())) {
    if (OB_UNLIKELY(size < (int64_t)(pos + src.get_string_len()))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src.get_string_ptr(), src.get_string_len());
      *this = src;
      this->set_urowid(buf + pos, src.get_string_len());
      pos += src.get_string_len();
    }
  } else if (ob_is_lob_locator(src.get_type())) {
    if (OB_UNLIKELY(size < (pos + src.get_val_len()))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      // copy all the value
      MEMCPY(buf + pos, src.get_string_ptr(), src.get_val_len());
      *this = src;
      ObLobLocator *res = reinterpret_cast<ObLobLocator *>((buf + pos));
      this->set_lob_locator(*res);
      pos += src.get_val_len();
    }
  } else if (ob_is_user_defined_sql_type(src.get_type())) {
    ObString src_str = src.get_string();
    if (OB_UNLIKELY(size < (pos + src_str.length()))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + pos, src_str.ptr(), src_str.length());
      *this = src; // meta copied
      this->set_udt_value(buf + pos, src_str.length());
      pos += src_str.length();
    }
  } else {
    *this = src;
  }
  return ret;
}

void* ObObj::get_deep_copy_obj_ptr()
{
  void * ptr = NULL;
  if (ob_is_string_type(this->get_type()) || ob_is_json(this->get_type())
      || ob_is_geometry(this->get_type()) || ob_is_user_defined_sql_type(this->get_type())) {
    // val_len_ == 0 is empty string, and it may point to unexpected address
    // Therefore, reset it to NULL
    if (val_len_ != 0) {
      ptr = (void *)v_.string_;
    }
  } else if (ob_is_raw(this->get_type())) {
    ptr = (void *)v_.string_;
  } else if (ob_is_number_tc(this->get_type())
            && 0 != nmb_desc_.len_
            && NULL != v_.nmb_digits_) {
    ptr = (void *)v_.nmb_digits_;
  } else if (ob_is_rowid_tc(this->get_type())) {
    ptr = (void *)v_.string_;
  } else if (ob_is_lob_locator(this->get_type())) {
    ptr = (void *)&v_.lob_locator_;
  } else {
    // do nothing
  }
  return ptr;
}

bool ObObj::can_compare(const ObObj &other) const
{
  obj_cmp_func cmp_func = NULL;
  return (is_min_value()
          || is_max_value()
          || other.is_min_value()
          || other.is_max_value()
          || ObObjCmpFuncs::can_cmp_without_cast(get_meta(),
                                                 other.get_meta(),
                                                 CO_CMP,
                                                 cmp_func));
}

int ObObj::check_collation_free_and_compare(const ObObj &other, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  if (CS_TYPE_COLLATION_FREE != get_collation_type() &&
      CS_TYPE_COLLATION_FREE != other.get_collation_type()) {
    ret = compare(other, CS_TYPE_INVALID, cmp);
  } else if (is_null() || other.is_null() || is_min_value() || is_max_value() || other.is_min_value() || other.is_max_value()) {
    ret = ObObjCmpFuncs::compare(*this, other, CS_TYPE_INVALID, cmp);
  } else if (OB_UNLIKELY(get_collation_type() != other.get_collation_type())
             || CS_TYPE_COLLATION_FREE != get_collation_type()
             || get_type() != other.get_type()
             || !is_character_type()) {
    LOG_ERROR("unexpected error, invalid argument", K(*this), K(other));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // 只用于sortkey转换后的Obj比较
    const int32_t lhs_len = get_val_len();
    const int32_t rhs_len = other.get_val_len();
    const int32_t cmp_len = std::min(lhs_len, rhs_len);
    const bool is_oracle = lib::is_oracle_mode();
    bool need_skip_tail_space = false;
    cmp = memcmp(get_string_ptr(), other.get_string_ptr(), cmp_len);
    if (is_oracle) {
      if (0 == cmp) {
        // 如果两个字符串只有尾部空格不同，在oracle varchar模式下，认为字符串是不同的
        if (!is_varying_len_char_type()) {
          // 如果两个字符串只有尾部空格不同，在oracle char模式下，认为字符串是相同的
          need_skip_tail_space = true;
        } else if (lhs_len != cmp_len || rhs_len != cmp_len) {
          cmp = lhs_len > cmp_len ? 1 : -1;
        }
      }
    } else if (0 == cmp && (lhs_len != cmp_len || rhs_len != cmp_len)) {
      // m如果两个字符串只有尾部空格不同，在mysql模式下，认为字符串是相同的
      need_skip_tail_space = true;
    }
    if (need_skip_tail_space) {
      bool has_non_space = false;
      const int32_t left_len = (lhs_len > cmp_len) ? lhs_len - cmp_len : rhs_len - cmp_len;
      const char *ptr = (lhs_len > cmp_len) ? get_string_ptr() : other.get_string_ptr();
      const unsigned char *uptr = reinterpret_cast<const unsigned char *>(ptr);
      int32_t i = 0;
      uptr += cmp_len;
      // varchar或char有长度限制，不可能超过int32_t
      for (; i < left_len; ++i) {
        if (*(uptr + i) != ' ') {
          has_non_space = true;
          break;
        }
      }
      if (has_non_space) {
        // mysql特殊行为：a\1 < a，但ab > a
        if (*(uptr + i) < ' ') {
          cmp = lhs_len > cmp_len ? -1 : 1;
        } else {
          cmp = lhs_len > cmp_len ? 1 : -1;
        }
      }
    }
  }
  return ret;
}

// TODO by fengshuo.fs: remove this function
int ObObj::check_collation_free_and_compare(const ObObj &other) const
{
  int cmp = 0;
  if (CS_TYPE_COLLATION_FREE != get_collation_type() &&
      CS_TYPE_COLLATION_FREE != other.get_collation_type()) {
    cmp = compare(other, CS_TYPE_INVALID);
  } else if (is_null() || other.is_null() || is_min_value() || is_max_value() || other.is_min_value() || other.is_max_value()) {
    cmp = ObObjCmpFuncs::compare_nullsafe(*this, other, CS_TYPE_INVALID);
  } else if (OB_UNLIKELY(get_collation_type() != other.get_collation_type())
      || CS_TYPE_COLLATION_FREE != get_collation_type()
      || get_type() != other.get_type()
      || !is_character_type()) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "unexpected error, invalid argument", K(*this), K(other));
    right_to_die_or_duty_to_live();
  } else {
    // 只用于sortkey转换后的Obj比较
    const int32_t lhs_len = get_val_len();
    const int32_t rhs_len = other.get_val_len();
    const int32_t cmp_len = std::min(lhs_len, rhs_len);
    const bool is_oracle = lib::is_oracle_mode();
    bool need_skip_tail_space = false;
    cmp = memcmp(get_string_ptr(), other.get_string_ptr(), cmp_len);
    if (is_oracle) {
      if (0 == cmp) {
        // 如果两个字符串只有尾部空格不同，在oracle varchar模式下，认为字符串是不同的
        if (!is_varying_len_char_type()) {
          // 如果两个字符串只有尾部空格不同，在oracle char模式下，认为字符串是相同的
          need_skip_tail_space = true;
        } else if (lhs_len != cmp_len || rhs_len != cmp_len) {
          cmp = lhs_len > cmp_len ? 1 : -1;
        }
      }
    } else if (0 == cmp && (lhs_len != cmp_len || rhs_len != cmp_len)) {
      // m如果两个字符串只有尾部空格不同，在mysql模式下，认为字符串是相同的
      need_skip_tail_space = true;
    }
    if (need_skip_tail_space) {
      bool has_non_space = false;
      const int32_t left_len = (lhs_len > cmp_len) ? lhs_len - cmp_len : rhs_len - cmp_len;
      const char *ptr = (lhs_len > cmp_len) ? get_string_ptr() : other.get_string_ptr();
      const unsigned char *uptr = reinterpret_cast<const unsigned char *>(ptr);
      int32_t i = 0;
      uptr += cmp_len;
      // varchar或char有长度限制，不可能超过int32_t
      for (; i < left_len; ++i) {
        if (*(uptr + i) != ' ') {
          has_non_space = true;
          break;
        }
      }
      if (has_non_space) {
        // mysql特殊行为：a\1 < a，但ab > a
        if (*(uptr + i) < ' ') {
          cmp = lhs_len > cmp_len ? -1 : 1;
        } else {
          cmp = lhs_len > cmp_len ? 1 : -1;
        }
      }
    }
  }
  return cmp;
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */

int ObObj::compare(const ObObj &other, int &cmp) const
{
  return ObObjCmpFuncs::compare(*this, other, CS_TYPE_INVALID, cmp);
}

// TODO by fengshuo.fs: remove this function
int ObObj::compare(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_nullsafe(*this, other, CS_TYPE_INVALID);
}

int ObObj::compare(const ObObj &other, ObCollationType cs_type, int &cmp) const
{
  return ObObjCmpFuncs::compare(*this, other, cs_type, cmp);
}

// TODO by fengshuo.fs: remove this function
int ObObj::compare(const ObObj &other, ObCollationType cs_type/*COLLATION_TYPE_MAX*/) const
{
  return ObObjCmpFuncs::compare_nullsafe(*this, other, cs_type);
}

int ObObj::compare(const ObObj &other, ObCompareCtx &cmp_ctx, int &cmp) const
{
  return ObObjCmpFuncs::compare(*this, other, cmp_ctx, cmp);
}

// TODO by fengshuo.fs: remove this function
int ObObj::compare(const ObObj &other, ObCompareCtx &cmp_ctx) const
{
  return ObObjCmpFuncs::compare_nullsafe(*this, other, cmp_ctx);
}

int ObObj::compare(const ObObj &other, ObCollationType cs_type, const ObCmpNullPos null_pos) const
{
  ObCompareCtx cmp_ctx(ObMaxType, cs_type, true, INVALID_TZ_OFF, null_pos);
  return ObObjCmpFuncs::compare_nullsafe(*this, other, cmp_ctx);
}

int ObObj::equal(const ObObj &other, bool &is_equal) const
{
  return ObObjCmpFuncs::compare_oper(*this, other, CS_TYPE_INVALID, CO_EQ, is_equal);
}
/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::is_equal(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_EQ);
}

int ObObj::equal(const ObObj &other, ObCollationType cs_type, bool &is_equal) const
{
  return ObObjCmpFuncs::compare_oper(*this, other, cs_type, CO_EQ, is_equal);
}
/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::is_equal(const ObObj &other, ObCollationType cs_type) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, cs_type, CO_EQ);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator<(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_LT);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator>(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_GT);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator>=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_GE);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator<=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_LE);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator==(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_EQ);
}

/*
 * ATTENTION:
 *
 * that_obj MUST have same type with this obj (*this)
 */
bool ObObj::operator!=(const ObObj &other) const
{
  return ObObjCmpFuncs::compare_oper_nullsafe(*this, other, CS_TYPE_INVALID, CO_NE);
}

int ObObj::apply(const ObObj &mutation)
{
  int ret = OB_SUCCESS;
  int org_type = get_type();
  int mut_type = mutation.get_type();
  if (OB_UNLIKELY(ObMaxType <= mut_type
                  || (ObExtendType != org_type
                      && ObNullType != org_type
                      && ObExtendType != mut_type
                      && ObNullType != mut_type
                      && org_type != mut_type
                      && !(ObLongTextType == org_type && ObLobType == mut_type)
                      && !(ObJsonType == org_type && ObLobType == mut_type)
                      && !(ObGeometryType == org_type && ObLobType == mut_type)))) {
    _OB_LOG(WARN, "type not coincident or invalid type[this->type:%d,mutation.type:%d]",
              org_type, mut_type);
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch (mut_type) {
      case ObNullType:
        set_null();
        break;
      case ObExtendType: {
        int64_t org_ext = get_ext();
        switch (mutation.get_ext()) {
          case ObActionFlag::OP_DEL_ROW:
          case ObActionFlag::OP_DEL_TABLE:
            /// used for join, if right row was deleted, set the cell to null
            set_null();
            break;
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            /// do nothing
            break;
          case ObActionFlag::OP_NOP:
            if (org_ext == ObActionFlag::OP_ROW_DOES_NOT_EXIST
                || org_ext == ObActionFlag::OP_DEL_ROW) {
              set_null();
            }
            break;
          default:
            ret = OB_INVALID_ARGUMENT;
            _OB_LOG(ERROR, "unsupported ext value [value:%ld]", mutation.get_ext());
            break;
        }  // end switch
        break;
      }
      default:
        *this = mutation;
        break;
    }  // end switch
  }
  return ret;
}

int (*oceanbase::common::serialize_composite_callback)(const ObObj&, char*, const int64_t, int64_t&) = NULL;
int (*oceanbase::common::deserialize_composite_callback)(ObObj&, const char*, const int64_t, int64_t&) = NULL;
int64_t (*oceanbase::common::composite_serialize_size_callback)(const ObObj&) = NULL;

#define DEF_FUNC_ENTRY(OBJTYPE)                 \
  {                                             \
      obj_print_sql<OBJTYPE>,                   \
      obj_print_str<OBJTYPE>,                   \
      obj_print_plain_str<OBJTYPE>,             \
      obj_print_json<OBJTYPE>,                  \
      obj_crc64<OBJTYPE>,                       \
      obj_crc64_v2<OBJTYPE>,                    \
      obj_batch_checksum<OBJTYPE>,              \
      obj_murmurhash<OBJTYPE>,                  \
      ObjHashCalculator<OBJTYPE, ObDefaultHash, ObObj>::calc_hash_value,  \
      obj_val_serialize<OBJTYPE>,               \
      obj_val_deserialize<OBJTYPE>,             \
      obj_val_get_serialize_size<OBJTYPE>,      \
      ObjHashCalculator<OBJTYPE, ObWyHash, ObObj>::calc_hash_value,  \
      obj_crc64_v3<OBJTYPE>,                    \
      ObjHashCalculator<OBJTYPE, ObXxHash, ObObj>::calc_hash_value,  \
      ObjHashCalculator<OBJTYPE, ObMurmurHash, ObObj>::calc_hash_value,  \
  }

ObObjTypeFuncs OBJ_FUNCS[ObMaxType] =
{
  DEF_FUNC_ENTRY(ObNullType),       // 0
  DEF_FUNC_ENTRY(ObTinyIntType),    // 1
  DEF_FUNC_ENTRY(ObSmallIntType),   // 2
  DEF_FUNC_ENTRY(ObMediumIntType),  // 3
  DEF_FUNC_ENTRY(ObInt32Type),      // 4
  DEF_FUNC_ENTRY(ObIntType),        // 5
  DEF_FUNC_ENTRY(ObUTinyIntType),   // 6
  DEF_FUNC_ENTRY(ObUSmallIntType),  // 7
  DEF_FUNC_ENTRY(ObUMediumIntType), // 8
  DEF_FUNC_ENTRY(ObUInt32Type),     // 9
  DEF_FUNC_ENTRY(ObUInt64Type),     // 10
  DEF_FUNC_ENTRY(ObFloatType),      // 11
  DEF_FUNC_ENTRY(ObDoubleType),     // 12
  DEF_FUNC_ENTRY(ObUFloatType),     // 13
  DEF_FUNC_ENTRY(ObUDoubleType),    // 14
  DEF_FUNC_ENTRY(ObNumberType),     // 15
  DEF_FUNC_ENTRY(ObUNumberType),    // 16: unumber is the same as number
  DEF_FUNC_ENTRY(ObDateTimeType),   // 17
  DEF_FUNC_ENTRY(ObTimestampType),  // 18
  DEF_FUNC_ENTRY(ObDateType),       // 19
  DEF_FUNC_ENTRY(ObTimeType),       // 20
  DEF_FUNC_ENTRY(ObYearType),       // 21
  DEF_FUNC_ENTRY(ObVarcharType),    // 22, varchar
  DEF_FUNC_ENTRY(ObCharType),       // 23, char
  DEF_FUNC_ENTRY(ObHexStringType),  // 24, hex_string
  DEF_FUNC_ENTRY(ObExtendType),     // 25, ext
  DEF_FUNC_ENTRY(ObUnknownType),    // 26, unknown
  DEF_FUNC_ENTRY(ObTinyTextType),   // 27, tiny_text
  DEF_FUNC_ENTRY(ObTextType),       // 28, text
  DEF_FUNC_ENTRY(ObMediumTextType), // 29, medium_text
  DEF_FUNC_ENTRY(ObLongTextType),   // 30, longtext
  DEF_FUNC_ENTRY(ObBitType),        // 31, bit
  DEF_FUNC_ENTRY(ObEnumType),       // 32, enum
  DEF_FUNC_ENTRY(ObSetType),        // 33, set
  DEF_FUNC_ENTRY(ObEnumInnerType),  // 34, enum
  DEF_FUNC_ENTRY(ObSetInnerType),   // 35, set
  DEF_FUNC_ENTRY(ObTimestampTZType), // 36, timestamp with time zone
  DEF_FUNC_ENTRY(ObTimestampLTZType), // 37, timestamp with local time zone
  DEF_FUNC_ENTRY(ObTimestampNanoType), // 38, timestamp (9)
  DEF_FUNC_ENTRY(ObRawType),           // 39, timestamp (9)
  DEF_FUNC_ENTRY(ObIntervalYMType),    // 40, interval year to month
  DEF_FUNC_ENTRY(ObIntervalDSType),    // 41, interval day to second
  DEF_FUNC_ENTRY(ObNumberFloatType),   // 42, number float
  DEF_FUNC_ENTRY(ObNVarchar2Type),     // 43, nvarchar2
  DEF_FUNC_ENTRY(ObNCharType),         // 44, nchar
  DEF_FUNC_ENTRY(ObURowIDType),        // 45, urowid
  DEF_FUNC_ENTRY(ObLobType),           // 46, lob
  DEF_FUNC_ENTRY(ObJsonType),          // 47, json
  DEF_FUNC_ENTRY(ObGeometryType),      // 48, geometry TODO!!!!!
  DEF_FUNC_ENTRY(ObUserDefinedSQLType),// 49, udt
};

ob_obj_hash ObObjUtil::get_murmurhash_v3(ObObjType type)
{
  return OBJ_FUNCS[type].murmurhash_v3;
}

ob_obj_hash ObObjUtil::get_murmurhash_v2(ObObjType type)
{
  return OBJ_FUNCS[type].murmurhash_v2;
}

ob_obj_hash ObObjUtil::get_wyhash(ObObjType type)
{
  return OBJ_FUNCS[type].wyhash;
}

ob_obj_crc64_v3 ObObjUtil::get_crc64_v3(ObObjType type)
{
  return ::OBJ_FUNCS[type].crc64_v3;
}

ob_obj_hash ObObjUtil::get_xxhash64(ObObjType type)
{
  return ::OBJ_FUNCS[type].xxhash64;
}

////////////////////////////////////////////////////////////////
int ObObj::print_sql_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params) const
{
  return OBJ_FUNCS[meta_.get_type()].print_sql(*this, buffer, length, pos, params);
}

int ObObj::print_sql_literal(char *&buffer, int64_t &length,
                             int64_t &pos, ObIAllocator &alloc,
                             const ObObjPrintParams &params) const
{
  int ret = OB_SUCCESS;
  int64_t saved_pos = pos;
  while (OB_SUCC(ret) && pos == saved_pos) {
    if (OB_FAIL(OBJ_FUNCS[meta_.get_type()].print_sql(*this, buffer, length, pos, params))) {
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(multiple_extend_buf(buffer, length, alloc))) {
          LOG_WARN("failed to auto extend stmt buf", K(ret));
        } else {
          pos = saved_pos;
        }
      } else {
        LOG_WARN("failed to print sql", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

//used for show create table default value
//for example:
// `a` int(11) NOT NULL DEFAULT '0'  (with '')
//always with ''
int ObObj::print_varchar_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params) const
{
  return OBJ_FUNCS[meta_.get_type()].print_str(*this, buffer, length, pos, params);
}

int ObObj::print_plain_str_literal(char *buffer, int64_t length, int64_t &pos, const ObObjPrintParams &params) const
{
  return OBJ_FUNCS[meta_.get_type()].print_plain_str(*this, buffer, length, pos, params);
}

void ObObj::print_str_with_repeat(char *buf, int64_t buf_len, int64_t &pos) const
{
  const unsigned char *uptr = reinterpret_cast<const unsigned char*>(v_.string_);
  int32_t real_len = val_len_;
  int32_t repeats = 0;
  int8_t cnt_space = 0;//There is no space for whole multibyte character, then add trailing spaces.
  if (NULL != uptr && real_len > 0) {
    while (real_len > 0 && ' ' == uptr[real_len - 1]) {
      --real_len;
      ++cnt_space;
    }
    // for utf-8 character set, pad BFBFEF as the tailing characters in a loop
    while (real_len - 2 > 0 && 0xBF == uptr[real_len - 1]  && 0xBF == uptr[real_len - 2]  && 0xEF == uptr[real_len - 3]) {
      real_len -= 3;
      ++repeats;
    }
  }
  if (0 == repeats) {
    real_len = val_len_;
  }
  BUF_PRINTO(ObString(0, real_len, v_.string_));
  if (repeats > 0) {
    BUF_PRINTF(" \'<%X%X%X><repeat %d times>\' ", uptr[real_len], uptr[real_len + 1], uptr[real_len + 2], repeats);
    //There is no space for whole multibyte character, then add trailing spaces.
    if (1 == cnt_space) {
      BUF_PRINTO(" ");
    } else if (2 == cnt_space) {
      BUF_PRINTO("  ");
    }
  }
}

int ObObj::print_smart(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (get_type() < ObMaxType && get_type() >= ObNullType) {
    ObObjPrintParams params;
    bool can_print = true;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <=0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!(meta_.is_string_or_lob_locator_type() && ObHexStringType != meta_.get_type())
               && (!meta_.is_json()) && (!meta_.is_geometry())) {
      ret = OBJ_FUNCS[meta_.get_type()].print_json(*this, buf, buf_len, pos, params);
    } else if (OB_FAIL(is_printable(get_string_ptr(), get_string_len(), can_print))) {
    } else if (can_print) {
      ret = OBJ_FUNCS[meta_.get_type()].print_json(*this, buf, buf_len, pos, params);
    } else {
      J_OBJ_START();
      PRINT_META();
      BUF_PRINTO(ob_obj_type_str(get_type()));
      J_COLON();
      if (OB_FAIL(obj_print_sql<ObHexStringType>(*this, buf, buf_len, pos, params))) {
      } else {
        J_COMMA();
        J_KV(N_COLLATION, ObCharset::collation_name(get_collation_type()));
        J_OBJ_END();
      }
    }
  }
  return ret;
}

int ObObj::print_format(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (get_type() < ObMaxType && get_type() >= ObNullType) {
    ObObjPrintParams params;
    bool can_print = true;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <=0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (!(meta_.is_string_type() && ObHexStringType != meta_.get_type())) {
      ret = OBJ_FUNCS[meta_.get_type()].print_sql(*this, buf, buf_len, pos, params);
    } else if (OB_FAIL(is_printable(get_string_ptr(), get_string_len(), can_print))) {
    } else if (can_print) {
      ret = OBJ_FUNCS[meta_.get_type()].print_sql(*this, buf, buf_len, pos, params);
    } else {
      ret = obj_print_sql<ObHexStringType>(*this, buf, buf_len, pos, params);
    }
  }
  return ret;
}

void ObObj::print_range_value(char *buf, int64_t buf_len, int64_t &pos) const
{
  if (is_string_type()) {
    J_OBJ_START();
    BUF_PRINTO(ob_obj_type_str(this->get_type()));
    J_COLON();
    //for Unicode character set
    print_str_with_repeat(buf, buf_len, pos);
    J_COMMA();
    J_KV(N_COLLATION, ObCharset::collation_name(this->get_collation_type()));
    J_OBJ_END();
  } else {
    (void)databuff_print_obj(buf, buf_len, pos, *this);
  }
}

int64_t ObObj::to_string(char *buf, const int64_t buf_len, const ObObjPrintParams &params) const
{
  int64_t pos = 0;
  if (get_type() < ObMaxType && get_type() >= ObNullType) {
    (void)OBJ_FUNCS[meta_.get_type()].print_json(*this, buf, buf_len, pos, params);
  }
  return pos;
}

bool ObObj::check_collation_integrity() const
{
  bool is_ok = true;
#ifndef NDEBUG
  if (ObNullType == get_type()) {
    // ignore null
    //is_ok = (CS_TYPE_BINARY == get_collation_type() && CS_LEVEL_IGNORABLE == get_collation_level());
  } else if (ob_is_numeric_type(get_type()) || ob_is_temporal_type(get_type())){
    is_ok = (CS_TYPE_BINARY == get_collation_type() && CS_LEVEL_NUMERIC == get_collation_level());
  } else {
    // ignore: varchar, char, binary, varbinary, unknown, ext
  }
  if (!is_ok) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      BACKTRACE_RET(WARN, true, common::OB_ERR_UNEXPECTED, "unexpected collation type: %s", to_cstring(get_meta()));
    }
  }
#endif
  return is_ok;
}

int ObObj::hash_v1(uint64_t &res, uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash(*this, seed, res);
}

int ObObj::hash(uint64_t &res) const
{
  return hash(res, 0);
}

int ObObj::hash(uint64_t &res, uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash_v2(*this, seed, res);
}

int ObObj::hash_murmur(uint64_t &res, uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].murmurhash_v3(*this, seed, res);
}

int ObObj::hash_wy(uint64_t &res, uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].wyhash(*this, seed, res);
}

int ObObj::hash_xx(uint64_t &res, uint64_t seed) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].xxhash64(*this, seed, res);
}

int64_t ObObj::checksum(const int64_t current) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].crc64(*this, current);
}

int64_t ObObj::checksum_v2(const int64_t current) const
{
  check_collation_integrity();
  return OBJ_FUNCS[meta_.get_type()].crc64_v2(*this, current);
}

void ObObj::checksum(ObBatchChecksum &bc) const
{
  check_collation_integrity();
  OBJ_FUNCS[meta_.get_type()].batch_checksum(*this, bc);
}

void ObObj::dump(const int32_t log_level /*= OB_LOG_LEVEL_DEBUG*/) const
{
  _OB_NUM_LEVEL_LOG(log_level, 0, "%s", S(*this));
}

int ObObj::print_varchar_literal(const ObIArray<ObString> &type_infos, char *buffer, int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSqlString str_val;
  if (OB_UNLIKELY(!meta_.is_enum_or_set())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type", KPC(this), K(ret));
  } else if (is_enum()) {
    if (OB_FAIL(get_enum_str_val(str_val, type_infos))) {
      LOG_WARN("fail to get enum str val", K(str_val), K(type_infos), K(ret));
    }
  } else {
    if (OB_FAIL(get_set_str_val(str_val, type_infos))) {
      LOG_WARN("fail to get set str val", K(str_val), K(type_infos), K(ret));
    }
  }
  if (OB_SUCC(ret) && databuff_printf(buffer, length, pos, "'%.*s'",
                                      static_cast<int32_t>(str_val.length()), str_val.ptr())) {
    LOG_WARN("fail to print string", KP(buffer), K(length), K(pos), K(str_val), K(ret));
  }
  return ret;
}

int ObObj::print_plain_str_literal(const ObIArray<ObString> &type_infos, char *buffer, int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObSqlString str_val;
  if (OB_UNLIKELY(!meta_.is_enum_or_set())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected obj type", KPC(this), K(ret));
  } else if (is_enum()) {
    if (OB_FAIL(get_enum_str_val(str_val, type_infos))) {
      LOG_WARN("fail to get enum str val", K(str_val), K(type_infos), K(ret));
    }
  } else {
    if (OB_FAIL(get_set_str_val(str_val, type_infos))) {
      LOG_WARN("fail to get set str val", K(str_val), K(type_infos), K(ret));
    }
  }
  if (OB_SUCC(ret) && databuff_printf(buffer, length, pos, "%.*s",
                                      static_cast<int32_t>(str_val.length()), str_val.ptr())) {
    LOG_WARN("fail to print string", KP(buffer), K(length), K(pos), K(str_val), K(ret));
  }
  return ret;
}

int ObObj::get_enum_str_val(ObSqlString &str_val, const ObIArray<ObString> &type_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_.is_enum())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", KPC(this), K(ret));
  } else {
    uint64_t val = get_enum();
    if (OB_UNLIKELY(val > type_infos.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected obj value", K(type_infos), KPC(this), K(ret));
    } else if (0 == val) {
      if (OB_FAIL(str_val.append(ObString("")))) {
        LOG_WARN("fail to append string", K(str_val), K(ret));
      }
    } else {
      const ObString &type_info = type_infos.at(val - 1);//enum value start from 1
      if (OB_FAIL(str_val.append(type_info))) {
        LOG_WARN("fail to append string", K(str_val), K(type_info), K(ret));
      }
    }
  }
  return ret;
}

int ObObj::get_set_str_val(ObSqlString &str_val, const ObIArray<ObString> &type_infos) const
{
 int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta_.is_set())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", KPC(this), K(ret));
  } else {
    uint64_t val = get_set();
    int64_t type_info_cnt = type_infos.count();
    if (OB_UNLIKELY(type_info_cnt > 64 || type_info_cnt <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type infos", K(type_infos), K(ret));
    } else if (OB_UNLIKELY(type_info_cnt < 64
                           && (val > ((1ULL << type_info_cnt) - 1)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected obj value", K(val), K(type_infos), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < type_info_cnt; ++i) {
      if (val & (1ULL << i)) {
        if (OB_FAIL(str_val.append(type_infos.at(i)))) {
          LOG_WARN("fail to append string", K(str_val), K(type_infos.at(i)), K(ret));
        } else if (OB_FAIL(str_val.append(","))) {
          LOG_WARN("fail to print string", K(str_val), K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (val != 0 && OB_FAIL(str_val.set_length(str_val.length() - 1))) {//remove last comma
      LOG_WARN("fail to str length", K(str_val), K(ret));
    }
  }
  return ret;
}

// 当租户模式为 mysql 时，返回 char 的字符长度
// 当租户模式为 oracle 时，如果 char 的 len 类型为 char，返回 char 的字符长度
// 当租户模式为 oracle 时，如果 char 的 len 类型为 byte，返回 char 的字节长度
int ObObj::get_char_length(const ObAccuracy accuracy, int32_t &char_len, bool is_oracle_mode) const
{
  int ret = OB_SUCCESS;

  if (!is_fixed_len_char_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("type must be char", K(get_type()));
  } else {
    if (is_oracle_byte_length(is_oracle_mode, accuracy.get_length_semantics())) {
      // get byte length
      char_len = static_cast<int32_t>(get_val_len());
    } else {
      // get char length
      char_len = static_cast<int32_t>(ObCharset::strlen_char(
                 get_collation_type(), get_string_ptr(), get_val_len()));
    }
  }

  return ret;
}

int ObObj::convert_string_value_charset(ObCharsetType charset_type, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString str;
  if (OB_FAIL(get_string(str))) {
    LOG_WARN("Failed to get payload from string", K(ret), K(*this));
  } else if (ObCharset::is_valid_charset(charset_type) && CHARSET_BINARY != charset_type) {
    ObCollationType collation_type = ObCharset::get_default_collation(charset_type);
    const ObCharsetInfo *from_charset_info = ObCharset::get_charset(get_collation_type());
    const ObCharsetInfo *to_charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(from_charset_info) || OB_ISNULL(to_charset_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("charsetinfo is null", K(ret), K(get_collation_type()), K(collation_type));
    } else if (CS_TYPE_INVALID == get_collation_type() || CS_TYPE_INVALID == collation_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid collation", K(get_collation_type()), K(collation_type), K(ret));
    } else if (CS_TYPE_BINARY != get_collation_type() && CS_TYPE_BINARY != collation_type
        && strcmp(from_charset_info->csname, to_charset_info->csname) != 0) {
      char *buf = NULL;
      int32_t buf_len = str.length() * ObCharset::CharConvertFactorNum;
      uint32_t result_len = 0;
      if (0 == buf_len) {
        //do noting
      } else if (OB_UNLIKELY(NULL == (buf = static_cast<char *>(
                allocator.alloc(buf_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
      } else {
        ret = ObCharset::charset_convert(get_collation_type(), str.ptr(),
            str.length(),collation_type, buf, buf_len, result_len);
        if (OB_SUCCESS != ret) {
          int32_t str_offset = 0;
          int64_t buf_offset = 0;
          ObString question_mark = ObCharsetUtils::get_const_str(collation_type, '?');
          while (str_offset < str.length() && buf_offset + question_mark.length() <= buf_len) {
            int64_t offset = ObCharset::charpos(get_collation_type(),
                                                str.ptr() + str_offset,
                                                str.length() - str_offset,
                                                1);
            ret = ObCharset::charset_convert(get_collation_type(),
                                              str.ptr() + str_offset,
                                              offset,
                                              collation_type,
                                              buf + buf_offset,
                                              buf_len - buf_offset,
                                              result_len);
            str_offset += offset;
            if (OB_SUCCESS == ret) {
              buf_offset += result_len;
            } else {
              MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
              buf_offset += question_mark.length();
            }
          }
          if (str_offset < str.length()) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("size overflow", K(ret), K(str), KPHEX(str.ptr(), str.length()));
          } else {
            result_len = buf_offset;
            ret = OB_SUCCESS;
            LOG_WARN("charset convert failed", K(ret),
                K(get_collation_type()), K(collation_type));
          }
        }
        if (OB_SUCC(ret)) {
          set_string(get_type(), buf, static_cast<int32_t>(result_len));
          set_collation_type(collation_type);
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
DEFINE_SERIALIZE(ObObj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(meta_);
  if (OB_SUCC(ret)) {
    if (meta_.is_invalid()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OBJ_FUNCS[meta_.get_type()].serialize(*this, buf, buf_len, pos);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObObj)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(meta_);
  if (OB_SUCC(ret)) {
    if (meta_.is_invalid()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OBJ_FUNCS[meta_.get_type()].deserialize(*this, buf, data_len, pos);
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObj)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(meta_);
  len += OBJ_FUNCS[meta_.get_type()].get_serialize_size(*this);
  return len;
}

DEFINE_SERIALIZE(ObObjParam)
{
  int ret = ObObj::serialize(buf, buf_len, pos);
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(accuracy_);
    OB_UNIS_ENCODE(res_flags_);
    if (OB_SUCC(ret) && is_ext_sql_array()) {
      const ObSqlArrayObj *array_obj = reinterpret_cast<const ObSqlArrayObj*>(get_ext());
      int64_t n = sizeof(ObSqlArrayObj);
      if (OB_ISNULL(array_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL ptr", K(ret), KP(array_obj));
      } else if (buf_len - pos < n) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("serialize buf not enough", K(ret), "remain", buf_len - pos, "needed", n);
      } else {
        MEMCPY(buf + pos, array_obj, n);
        pos += n;
        if (array_obj->count_ == 0) {
          /* do nothing */
        } else if (OB_ISNULL(array_obj->data_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data is NULL ptr", K(ret), KP(array_obj->data_));
        } else {
          n = sizeof(array_obj->data_[0]) * array_obj->count_;
          if (buf_len - pos < n) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("serialize buf not enough", K(ret), "remain", buf_len - pos, "needed", n);
          } else {
            MEMCPY(buf + pos, static_cast<const void*>(array_obj->data_), n);
            pos += n;
          }
        }
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObObjParam)
{
  int ret = ObObj::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(accuracy_);
    OB_UNIS_DECODE(res_flags_);
    if (OB_SUCC(ret) && is_ext_sql_array()) {
      ObSqlArrayObj *array_obj = NULL;
      int64_t n = sizeof(ObSqlArrayObj);
      if (data_len - pos < n) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("deserialize buf not enough", K(ret), "remain", data_len - pos, "needed", n);
      } else {
        array_obj = reinterpret_cast<ObSqlArrayObj *>(const_cast<char *>(buf + pos));
        pos += n;
      }
      if (OB_SUCC(ret) && array_obj->count_ > 0) {
        n = sizeof(ObObjParam) * array_obj->count_;
        if (data_len - pos < n) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_WARN("deserialize buf not enough", K(ret), "remain", data_len - pos, "needed", n);
        } else {
          array_obj->data_ = reinterpret_cast<ObObjParam *>(const_cast<char *>(buf + pos));
          pos += n;
        }
      }
      if (OB_SUCC(ret)) {
        set_extend(reinterpret_cast<int64_t>(array_obj), T_EXT_SQL_ARRAY);
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObjParam)
{
  int64_t len = ObObj::get_serialize_size();
  OB_UNIS_ADD_LEN(accuracy_);
  OB_UNIS_ADD_LEN(res_flags_);
  if (is_ext_sql_array()) {
    len += sizeof(ObSqlArrayObj);
    const ObSqlArrayObj *array_obj = reinterpret_cast<const ObSqlArrayObj*>(get_ext());
    if (NULL != array_obj) {
      len += sizeof(ObSqlArrayObj);
      if (array_obj->count_ == 0) {
        /* do nothing */
      } else if (NULL != array_obj->data_) {
        len += sizeof(array_obj->data_[0]) * array_obj->count_;
      }
    }
  }
  return len;
}

OB_SERIALIZE_MEMBER(ParamFlag, flag_);

void ObObjParam::reset()
{
  ObObj::reset();
  accuracy_.reset();
  res_flags_ = 0;
  flag_.reset();
}

void ParamFlag::reset()
{
  need_to_check_type_ = true;
  need_to_check_bool_value_ = false;
  expected_bool_value_ = false;
  need_to_check_extend_type_ = true;
  is_ref_cursor_type_ = false;
  is_pl_mock_default_param_ = false;
  is_boolean_ = false;
  is_batch_parameter_ = false;
  ignore_scale_check_ = false;
}


DEF_TO_STRING(ObHexEscapeSqlStr)
{
  int64_t buf_pos = 0;
  if (buf != NULL && buf_len > 0 && !str_.empty()) {
    const char *end = str_.ptr() + str_.length();
    if (do_oracle_mode_escape_) {
      for (const char *cur = str_.ptr(); cur < end && buf_pos < buf_len; ++cur) {
        if ('\'' == *cur) {
          //在oracle模式中,只处理单引号转义
          buf[buf_pos++] = '\'';
          if (buf_pos < buf_len) {
            buf[buf_pos++] = *cur;
          }
        } else {
          buf[buf_pos++] = *cur;
        }
      }
    } else if (skip_escape_) {
      // do not escape_ while in NO_BACKSLASH_ESCAPES mode
      for (const char *cur = str_.ptr(); cur < end && buf_pos < buf_len; ++cur) {
        buf[buf_pos++] = *cur;
      }
    } else {
      for (const char *cur = str_.ptr(); cur < end && buf_pos < buf_len; ++cur) {
        switch (*cur) {
          case '\\': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = '\\';
            }
            break;
          }
          case '\0': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = '0';
            }
            break;
          }
          case '\'':
          case '\"': {
            //字符串中出现了'或者"，需要进行转义
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = *cur;
            }
            break;
          }
          case '\n': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 'n';
            }
            break;
          }
          case '\r': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 'r';
            }
            break;
          }
          case '\t': {
            buf[buf_pos++] = '\\';
            if (buf_pos < buf_len) {
              buf[buf_pos++] = 't';
            }
            break;
          }
          default: {
            buf[buf_pos++] = *cur;
            break;
          }
        }
      }
    }
  }
  return buf_pos;
}

int64_t ObHexEscapeSqlStr::get_extra_length() const
{
  int64_t ret_length = 0;
  if (!str_.empty()) {
    const char *end = str_.ptr() + str_.length();
    if (do_oracle_mode_escape_) {
      for (const char *cur = str_.ptr(); cur < end; ++cur) {
        if ('\'' == *cur) {
          ++ret_length;
        }
      }
    } else {
      for (const char *cur = str_.ptr(); cur < end; ++cur) {
        switch (*cur) {
          case '\\':
          case '\0':
          case '\'':
          case '\"':
          case '\n':
          case '\r':
          case '\t': {
            ++ret_length;
            break;
          }
          default: {
            //do nothing
          }
        }
      }
    }
  }
  return ret_length;
}
