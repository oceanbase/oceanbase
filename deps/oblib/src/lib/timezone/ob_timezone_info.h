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

#ifndef OCEANBASE_LIB_TIMEZONE_INFO_
#define OCEANBASE_LIB_TIMEZONE_INFO_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace common
{
struct ObIntervalScaleUtil;
//timestamp(9)  store local time
//timestamp tz  store utc time + time zone
//timestamp ltz store utc time
//sizeof()=12
class ObOTimestampData
{
public:
  static const int32_t MIN_OFFSET_MINUTES = -15 * 60 - 59;
  static const int32_t MAX_OFFSET_MINUTES = 15 * 60 + 59;
  static const int32_t MAX_OFFSET_MINUTES_STRICT = 15 * 60;
  static const int32_t MIN_TAIL_NSEC = 0;
  static const int32_t MAX_TAIL_NSEC = 999;
  static const int32_t BASE_TAIL_NSEC = 1000;
  static const int32_t MAX_BIT_OF_TAIL_NSEC = ((1 << 10) - 1);
  static const int32_t MAX_BIT_OF_OFFSET_MIN = ((1 << 11) - 1);
  static const int32_t MAX_BIT_OF_TZ_ID = ((1 << 11) - 1);
  static const int32_t MAX_BIT_OF_TRAN_TYPE_ID = ((1 << 5) - 1);
  static const int32_t MIN_TZ_ID = 1;
  static const int32_t MAX_TZ_ID = MAX_BIT_OF_TZ_ID;
  static const int32_t MIN_TRAN_TYPE_ID = 0;
  static const int32_t MAX_TRAN_TYPE_ID = MAX_BIT_OF_TRAN_TYPE_ID;

  struct UnionTZCtx {
    UnionTZCtx() : desc_(0) {}
    void set_tail_nsec(const int32_t value)
    {
      tail_nsec_ = static_cast<uint16_t>(value & MAX_BIT_OF_TAIL_NSEC);
    }
    void set_tz_id(const int32_t value)
    {
      tz_id_ = static_cast<uint16_t>(value & MAX_BIT_OF_TZ_ID);
    }
    void set_tran_type_id(const int32_t value)
    {
      tran_type_id_ = static_cast<uint16_t>(value & MAX_BIT_OF_TRAN_TYPE_ID);
    }
    int32_t get_offset_min() const
    {
      return (is_neg_offset_
              ? (0 - static_cast<int32_t>(offset_min_))
              : static_cast<int32_t>(offset_min_));
    }
    void set_offset_min(const int32_t value)
    {
      int32_t tmp_value = value;
      if (value < 0) {
        tmp_value = 0 - value;
        is_neg_offset_ = 1;
      } else {
        is_neg_offset_ = 0;
      }
      offset_min_ = static_cast<int16_t>(tmp_value & MAX_BIT_OF_OFFSET_MIN);
    }


    union {
      uint32_t desc_;
      struct {
        union {
          uint16_t time_desc_;
          struct {
            uint16_t tail_nsec_           : 10; //append nanosecond to the tailer, [0, 999]
            uint16_t version_             : 2;  //default 0
            uint16_t store_tz_id_         : 1;  //true mean store tz_id
            uint16_t is_null_             : 1;  //oracle null timestamp
            uint16_t time_reserved_       : 2;  //reserved
          };
        };
        union {
          uint16_t tz_desc_;
          struct {
            uint16_t is_neg_offset_   : 1;  //reserved
            uint16_t offset_min_      : 11; //tz offset min
            uint16_t offset_reserved_ : 4;  //reserved
          };
          struct {
            uint16_t tz_id_           : 11;//Time_zone_id of oceanbase.__all_time_zone_transition_type, [1, 2047]
            uint16_t tran_type_id_    : 5; //Transition_type_id of oceanbase.__all_time_zone_transition_type, [0,31]
          };
        };
      };
    };
  }__attribute__ ((packed));

public:
  ObOTimestampData() : time_ctx_(), time_us_(0) { }
  ObOTimestampData(const int64_t time_us, const UnionTZCtx tz_ctx) :
	                 time_ctx_(tz_ctx), time_us_(time_us) { }
  ~ObOTimestampData() {}
  void reset() { memset(this, 0, sizeof(ObOTimestampData)); }
  bool is_null_value() const { return time_ctx_.is_null_; }
  void set_null_value() { time_ctx_.is_null_ = 1; }
  static bool is_valid_offset_min(const int32_t offset_min)
  {
    return MIN_OFFSET_MINUTES <= offset_min && offset_min <= MAX_OFFSET_MINUTES;
  }
  static bool is_valid_offset_min_strict(const int32_t offset_min)
  {
    return MIN_OFFSET_MINUTES <= offset_min && offset_min <= MAX_OFFSET_MINUTES_STRICT;
  }
  static bool is_valid_tz_id(const int32_t tz_id)
  {
    return (MIN_TZ_ID <= tz_id && tz_id <= MAX_TZ_ID);
  }
  static bool is_valid_tran_type_id(const int32_t tran_type_id)
  {
    return (MIN_TRAN_TYPE_ID <= tran_type_id && tran_type_id <= MAX_TRAN_TYPE_ID);
  }
  inline int compare(const ObOTimestampData &other) const
  {
    int result = 0;
    if (time_us_ > other.time_us_) {
      result = 1;
    } else if (time_us_ < other.time_us_) {
      result = -1;
    } else if (time_ctx_.tail_nsec_ > other.time_ctx_.tail_nsec_) {
      result = 1;
    } else if (time_ctx_.tail_nsec_ < other.time_ctx_.tail_nsec_) {
      result = -1;
    } else {
      result = 0;
    }
    return result;
  }

  inline bool operator<(const ObOTimestampData &other) const
  {
    return compare(other) < 0;
  }

  inline bool operator<=(const ObOTimestampData &other) const
  {
    return compare(other) <= 0;
  }

  inline bool operator>(const ObOTimestampData &other) const
  {
    return compare(other) > 0;
  }

  inline bool operator>=(const ObOTimestampData &other) const
  {
    return compare(other) >= 0;
  }

  inline bool operator==(const ObOTimestampData &other) const
  {
    return compare(other) == 0;
  }

  inline bool operator!=(const ObOTimestampData &other) const
  {
    return compare(other) != 0;
  }

  DECLARE_TO_STRING;

public:
  UnionTZCtx time_ctx_;   //time ctx, such as version, tail_ns, tz_id...
  int64_t time_us_;     //full time with usec, same as timestamp
}__attribute__ ((packed));

// 老版本的ObOTimestampData，原来是time_us_在前，time_ctx_在后。
// 对于ObTimestampNanoType类型，time_ctx_只有前两个字节有效，因此crc64_v2中直接计算前10个字节的checksum。
// 实现新引擎时做了一点优化，将ObOTimestampData中两个成员换了位置，导致crc64_v2计算出的结果与老版本不兼容，
// 存储层巡检时报错。 添加ObOTimestampDataOld类，用于计算与老版本兼容的checksum值。
struct ObOTimestampDataOld {
  int64_t time_us_;
  int32_t time_ctx_;
  ObOTimestampDataOld() : time_us_(0), time_ctx_(0) { }
  ObOTimestampDataOld(const int64_t time_us, const int32_t tz_ctx) :
	                 time_us_(time_us), time_ctx_(tz_ctx) { }
};

struct ObOTimestampTinyData {
  uint16_t desc_;
  int64_t time_us_;
}__attribute__ ((packed));
static_assert(sizeof(ObOTimestampTinyData) == 10, "size of ObOTimestampTinyData is not 10");

class ObTimeZoneInfoManager;
struct ObTimeZoneName
{
  ObString  name_;
  int32_t   lower_idx_;   // index of ObTimeZoneTrans array.
  int32_t   upper_idx_;   // index of ObTimeZoneTrans array.
  int32_t   tz_id;        // tmp members, will be removed later.
};

struct ObTimeZoneTrans
{
  int64_t   trans_;
  int32_t   offset_;
  int32_t   tz_id;        // tmp members, will be removed later.
};

static const int32_t INVALID_TZ_OFF = INT32_MAX;

class ObTZInfoMap;
class ObTenantTimezone;

// wrap ObTZInfoMap with ref count
class ObTZMapWrap
{
public:
  ObTZMapWrap() : tz_info_map_(nullptr)
  { }
  ~ObTZMapWrap()
  { }
  const ObTZInfoMap *get_tz_map() const { return tz_info_map_; }
  void set_tz_map(const common::ObTZInfoMap *tz_info_map);
  TO_STRING_KV(KP_(tz_info_map));
private:
  ObTZInfoMap *tz_info_map_;
};

class ObTimeZoneInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTimeZoneInfo()
      : error_on_overlap_time_(false), tz_map_wrap_(), tz_id_(common::OB_INVALID_TZ_ID), offset_(0)
  {}
  virtual ~ObTimeZoneInfo() {}
  int assign(const ObTimeZoneInfo &src);
  int set_timezone(const ObString &str);//only used for liboblog
  void set_offset(int32_t offset) { offset_= offset; }
  int32_t get_offset() const { return offset_; }
  void set_error_on_overlap_time(bool is_error) { error_on_overlap_time_ = is_error; }
  void set_tz_info_map(const ObTZInfoMap *tz_info_map) { tz_map_wrap_.set_tz_map(tz_info_map); }
  ObTZMapWrap &get_tz_map_wrap() { return tz_map_wrap_; }
  const ObTZInfoMap *get_tz_info_map() const { return tz_map_wrap_.get_tz_map(); }
  bool is_error_on_overlap_time() const { return error_on_overlap_time_; }
  int32_t get_tz_id() const { return tz_id_; }
  virtual int get_timezone_offset(int64_t value, int32_t &offset_sec) const;
  virtual int get_timezone_sub_offset(int64_t value,
                                      const ObString &tz_abbr_str,
                                      int32_t &offset_sec,
                                      int32_t &tz_id,
                                      int32_t &tran_type_id) const;
  virtual int get_timezone_offset(int64_t value,
                                  int32_t &offset_sec,
                                  common::ObString &tz_abbr_str,
                                  int32_t &tran_type_id) const;
  virtual int timezone_to_str(char *buf, const int64_t len, int64_t &pos) const;
  void reset()
  {
    tz_id_ = common::OB_INVALID_TZ_ID;
    offset_ = 0;
    error_on_overlap_time_ = false;
    tz_map_wrap_.set_tz_map(NULL);
  }
  TO_STRING_KV(N_ID, tz_id_, N_OFFSET, offset_, N_ERROR_ON_OVERLAP_TIME, error_on_overlap_time_);

private:
  static ObTimeZoneName TIME_ZONE_NAMES[];
  static ObTimeZoneTrans TIME_ZONE_TRANS[];

protected:
  bool error_on_overlap_time_;

  //do not serialize it
  ObTZMapWrap tz_map_wrap_;
private:
  int32_t tz_id_;
  int32_t offset_;
};

struct ObTZTransitionStruct {
  ObTZTransitionStruct() :
      offset_sec_(0),
      tran_type_id_(common::OB_INVALID_INDEX),
      is_dst_(false)
  {
    abbr_[0] = '\0';
  }
  ~ObTZTransitionStruct() {}
  int assign(const ObTZTransitionStruct &src)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(this != &src)) {
      MEMCPY(this, &src, sizeof(ObTZTransitionStruct));
    }
    return ret;
  }
  void reset()
  {
    offset_sec_ = 0;
    tran_type_id_ = common::OB_INVALID_INDEX;
    is_dst_ = false;
    abbr_[0] = '\0';
  }
  void set_tz_abbr(const common::ObString &abbr)
  {
    if (!abbr.empty()) {
      const int64_t min_len = std::min(static_cast<int64_t>(abbr.length()), common::OB_MAX_TZ_ABBR_LEN - 1);
      MEMCPY(abbr_, abbr.ptr(), min_len);
      abbr_[min_len] = '\0';
    } else {
      abbr_[0] = '\0';
    }
  }
  bool operator==(const ObTZTransitionStruct &other) const
  {
    return (offset_sec_ == other.offset_sec_
            && tran_type_id_ == other.tran_type_id_
            && is_dst_ == other.is_dst_
            && 0 == STRCASECMP(abbr_, other.abbr_));

  }
  bool operator!=(const ObTZTransitionStruct &other) const { return !(*this == other); }

  TO_STRING_KV(K_(offset_sec),
               K_(tran_type_id),
               K_(is_dst),
               "abbr", common::ObString(abbr_));

  int32_t offset_sec_;
  int32_t tran_type_id_;
  bool is_dst_;
  char abbr_[common::OB_MAX_TZ_ABBR_LEN];
};

class ObTZTransitionTypeInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTZTransitionTypeInfo()
      : lower_time_(common::OB_INVALID_TZ_TRAN_TIME),
        info_()
  { }
  virtual ~ObTZTransitionTypeInfo() {}
  int assign(const ObTZTransitionTypeInfo &src);
  void reset();

  void set_tz_abbr(const common::ObString &abbr) { info_.set_tz_abbr(abbr); }
  OB_INLINE bool is_dst() const { return info_.is_dst_; }
  bool operator==(const ObTZTransitionTypeInfo &other) const
  {
    return (lower_time_ == other.lower_time_
            && info_ == other.info_);
  }
  bool operator!=(const ObTZTransitionTypeInfo &other) const { return !(*this == other); }
  virtual int get_offset_according_abbr(const common::ObString &tz_abbr_str,
                                        int32_t &offset_sec, int32_t &tran_type_id) const;
  VIRTUAL_TO_STRING_KV(K_(lower_time), K_(info));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTZTransitionTypeInfo);
public:
  int64_t lower_time_;
  ObTZTransitionStruct info_;
};

class ObTZRevertTypeInfo : public ObTZTransitionTypeInfo
{
  OB_UNIS_VERSION(1);
public:
  enum TypeInfoClass
  {
    NONE = 0,
    NORMAL,
    OVERLAP,
    GAP
  };
public:
  ObTZRevertTypeInfo()
      :ObTZTransitionTypeInfo(),
      type_class_(NONE),
      extra_info_()
  {}
  virtual ~ObTZRevertTypeInfo() {}
  void reset();
  int assign_normal(const ObTZTransitionTypeInfo &src);
  int assign_extra(const ObTZTransitionTypeInfo &src);
  int assign(const ObTZRevertTypeInfo &src);
  OB_INLINE bool is_normal() const { return NORMAL == type_class_; }
  OB_INLINE bool is_gap() const { return GAP == type_class_; }
  OB_INLINE bool is_overlap() const { return OVERLAP == type_class_; }
  int get_offset_according_abbr(const common::ObString &tz_abbr_str,
                                int32_t &offset_sec, int32_t &tran_type_id) const;
  bool operator==(const ObTZRevertTypeInfo &other) const
  {
    return (type_class_ == other.type_class_
            && extra_info_ == other.extra_info_
            && lower_time_ == other.lower_time_
            && info_ == other.info_);

  }
  bool operator!=(const ObTZRevertTypeInfo &other) const { return !(*this == other); }

  VIRTUAL_TO_STRING_KV(K_(lower_time), K_(info), K_(type_class), K_(extra_info));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTZRevertTypeInfo);
public:
  TypeInfoClass type_class_;
  //当type_class_为overlap时，下成员记录另一份type info
  ObTZTransitionStruct extra_info_;
};

class ObTZIDKey
{
public:
  ObTZIDKey() : tz_id_(0) { }
  ObTZIDKey(const int64_t tz_id) : tz_id_(tz_id) {}
  uint64_t hash() const  { return common::murmurhash(&tz_id_, sizeof(tz_id_), 0); };
  int hash(uint64_t &hash_val) const  { hash_val = hash(); return OB_SUCCESS; };
  int compare(const ObTZIDKey & r)
  {
    int cmp = 0;
    if (tz_id_ < r.tz_id_) {
      cmp = -1;
    } else if (tz_id_ == r.tz_id_) {
      cmp = 0;
    } else {
      cmp = 1;
    }
    return cmp;
  }
  TO_STRING_KV(K_(tz_id));
public:
  int64_t tz_id_;
};

class ObTZNameKey
{
public:
  ObTZNameKey(const common::ObString &tz_key_str);
  ObTZNameKey(const ObTZNameKey &key);
  ObTZNameKey()
  {
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
  }
  ~ObTZNameKey() {}
  void reset();
  int assign(const ObTZNameKey &key);
  void operator=(const ObTZNameKey &key);
  int compare(const ObTZNameKey &key) const
  {
    ObString self_str(strlen(tz_name_), tz_name_);
    return self_str.case_compare(key.tz_name_);
  }
  bool operator==(const ObTZNameKey &key) const { return 0 == compare(key); }

  uint64_t hash() const;
  int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  TO_STRING_KV("tz_name", common::ObString(common::OB_MAX_TZ_NAME_LEN, tz_name_));
private:
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};

typedef common::LinkHashNode<ObTZIDKey> ObTZIDHashNode;
typedef common::LinkHashNode<ObTZNameKey> ObTZNameHashNode;
typedef common::LinkHashValue<ObTZIDKey> ObTZIDHashValue;
typedef common::LinkHashValue<ObTZNameKey> ObTZNameHashValue;

class ObTimeZoneInfo;
class ObTimeZoneInfoPos : public ObTimeZoneInfo, public ObTZIDHashValue
{
  OB_UNIS_VERSION(1);
public:
  ObTimeZoneInfoPos()
    : tz_id_(common::OB_INVALID_TZ_ID),
      default_type_(),
      tz_tran_types_(),
      tz_revt_types_(),
      curr_idx_(0)
  {
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
  }
  virtual ~ObTimeZoneInfoPos() {}
  int assign(const ObTimeZoneInfoPos &src);
  void reset();
  bool is_valid() const { return tz_id_ != common::OB_INVALID_TZ_ID; }
  int compare_upgrade(const ObTimeZoneInfoPos &other, bool &is_equal) const;

  // get_timezone_offset is used to get timezone offset at specified utc time and position.
  // First parameter is utc time second value.
  virtual int get_timezone_offset(int64_t value,
                                  int32_t &offset_sec,
                                  common::ObString &tz_abbr_str,
                                  int32_t &tran_type_id) const;
  int get_timezone_offset(const int32_t tran_type_id,
                          common::ObString &tz_abbr_str,
                          int32_t &offset_sec) const;
  virtual int get_timezone_offset(int64_t value, int32_t &offset_sec) const;
  // get_timezone_sub_offset is used to get timezone offset at specified local time and position.
  // First parameter is local time second value.
  virtual int get_timezone_sub_offset(int64_t value,
                                      const common::ObString &tz_abbr_str,
                                      int32_t &offset_sec,
                                      int32_t &tz_id,
                                      int32_t &tran_type_id) const;

  inline void set_tz_id(int64_t tz_id) { tz_id_ = tz_id; }
  inline int64_t get_tz_id() const { return tz_id_; }
  int set_tz_name(const char *name, int64_t name_len);
  int get_tz_name(common::ObString &tz_name) const;
  common::ObString get_tz_name() const { return common::ObString(tz_name_);}
  int add_tran_type_info(const ObTZTransitionTypeInfo &type_info);
  int set_default_tran_type(const ObTZTransitionTypeInfo &tran_type);
  inline const ObTZTransitionTypeInfo &get_default_trans_type() const { return default_type_; }
  // 这里用了一个 trick 来做“多版本”
  // 有2个槽位，一个是当前槽位，一个是未来新 timezone 信息槽位
  // 当有新的 timezone 信息从内部表读入后，会写入未来新 timezone 槽位，
  // 然后通过 curr_idx_++ 操作，将未来新 timezone 槽位升级为“当前槽位”
  //
  // 每个槽位里，记录了 tz_id 时区下所有 timezone offset 信息（夏令时）
  inline int32_t get_curr_idx() const { return curr_idx_; }
  inline int32_t get_next_idx() const { return curr_idx_ + 1; }
  inline void inc_curr_idx() { ++curr_idx_; }
  const common::ObSArray<ObTZTransitionTypeInfo> &get_tz_tran_types() const { return tz_tran_types_[get_curr_idx() % 2]; }
  const common::ObSArray<ObTZRevertTypeInfo> &get_tz_revt_types() const { return tz_revt_types_[get_curr_idx() % 2]; }
  const common::ObSArray<ObTZTransitionTypeInfo> &get_next_tz_tran_types() const { return tz_tran_types_[get_next_idx() % 2]; }
  const common::ObSArray<ObTZRevertTypeInfo> &get_next_tz_revt_types() const { return tz_revt_types_[get_next_idx() % 2]; }
  common::ObSArray<ObTZTransitionTypeInfo> &get_next_tz_tran_types() { return tz_tran_types_[get_next_idx() % 2]; }
  common::ObSArray<ObTZRevertTypeInfo> &get_next_tz_revt_types() { return tz_revt_types_[get_next_idx() % 2]; }
  int calc_revt_types();
  bool operator==(const ObTimeZoneInfoPos &other) const;
  void set_tz_type_attr(const lib::ObMemAttr &attr);
  virtual int timezone_to_str(char *buf, const int64_t len, int64_t &pos) const;
  VIRTUAL_TO_STRING_KV("tz_name", common::ObString(common::OB_MAX_TZ_NAME_LEN, tz_name_),
                       "tz_id", tz_id_,
                       "default_transition_type", default_type_,
                       "tz_transition_types", get_tz_tran_types(),
                       "tz_revert_types", get_tz_revt_types(),
                       K_(curr_idx),
                       "next_tz_transition_types", get_next_tz_tran_types(),
                       "next_tz_revert_types", get_next_tz_revt_types());
private:
  int find_time_range(int64_t value, const common::ObIArray<ObTZTransitionTypeInfo> &tz_tran_types,
                      int64_t &type_idx) const;
  int find_offset_range(const int32_t tran_type_id, const common::ObIArray<ObTZTransitionTypeInfo> &tz_tran_types,
                        int64_t &type_idx) const;
  int find_revt_time_range(int64_t value, const common::ObIArray<ObTZRevertTypeInfo> &tz_revt_types,
                           int64_t &type_idx) const;

private:
  int64_t tz_id_;
  /*default_type_ is used for times smaller than first transition or if
    there are no transitions at all.*/
  ObTZTransitionTypeInfo default_type_;
  //used for utc time -> local time
  common::ObSArray<ObTZTransitionTypeInfo> tz_tran_types_[2];
  //used for local time -> utc time
  common::ObSArray<ObTZRevertTypeInfo> tz_revt_types_[2];
  uint32_t curr_idx_;
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};

class ObTZNameIDInfo: public ObTZNameHashValue
{
public:
  ObTZNameIDInfo() : tz_id_(common::OB_INVALID_TZ_ID) { MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN); }
  void set(const int64_t tz_id, const common::ObString &tz_name)
  {
    tz_id_ = tz_id;
    if (tz_name.empty()) {
      MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    } else {
      const int64_t min_len = std::min(static_cast<int64_t>(tz_name.length()), common::OB_MAX_TZ_ABBR_LEN - 1);
      MEMCPY(tz_name_, tz_name.ptr(), min_len);
      tz_name_[min_len] = '\0';
    }
  }

  ObTZNameIDInfo(const int64_t tz_id, const common::ObString &tz_name)
    : tz_id_(tz_id)
  {
    set(tz_id, tz_name);
  }
  ~ObTZNameIDInfo() {}
  TO_STRING_KV(K_(tz_id), KCSTRING_(tz_name));
  bool operator==(const ObTZNameIDInfo &other) const
  {
    return (tz_id_ == other.tz_id_
            && 0 == STRCASECMP(tz_name_, other.tz_name_));

  }
public:
  int64_t tz_id_;
  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
};

class ObTZIDPosAlloc
{
public:
  ObTZIDPosAlloc() {}
  ~ObTZIDPosAlloc() {}
  ObTimeZoneInfoPos* alloc_value();
  void free_value(ObTimeZoneInfoPos *tz_info);

  ObTZIDHashNode* alloc_node(ObTimeZoneInfoPos *value);
  void free_node(ObTZIDHashNode *node);
};

class ObTZNameIDAlloc
{
public:
  ObTZNameIDAlloc() {}
  ~ObTZNameIDAlloc() {}
  ObTZNameIDInfo* alloc_value();
  void free_value(ObTZNameIDInfo *info);

  ObTZNameHashNode* alloc_node(ObTZNameIDInfo *value);
  void free_node(ObTZNameHashNode *node);
};

typedef common::ObLinkHashMap<ObTZIDKey, ObTimeZoneInfoPos, ObTZIDPosAlloc> ObTZInfoIDPosMap;
typedef common::ObLinkHashMap<ObTZNameKey, ObTZNameIDInfo, ObTZNameIDAlloc> ObTZInfoNameIDMap;

class ObTZInfoMap
{
public:
  ObTZInfoMap() : inited_(false), id_map_(&id_map_buf_), name_map_(&name_map_buf_) {}
  ~ObTZInfoMap() {}
  int init(const lib::ObMemAttr &attr);
  void destroy();
  int print_tz_info_map();
  bool is_inited() { return inited_; }
  int get_tz_info_by_id(const int64_t tz_id, ObTimeZoneInfoPos &tz_info_by_id);
  int get_tz_info_by_name(const common::ObString &tz_name, ObTimeZoneInfoPos &tz_info_by_name);
  int get_tz_info_by_id(const int64_t tz_id, ObTimeZoneInfoPos *&tz_info_by_id);
  int get_tz_info_by_name(const common::ObString &tz_name, ObTimeZoneInfoPos *&tz_info_by_name);
public:
  bool inited_;
  ObTZInfoIDPosMap *id_map_;
  ObTZInfoNameIDMap *name_map_;
  ObTZInfoIDPosMap id_map_buf_; // tz_id => ObTimeZoneInfoPos
  ObTZInfoNameIDMap name_map_buf_; // tz_name => tz_id

private:
  DISALLOW_COPY_AND_ASSIGN(ObTZInfoMap);
};

class ObTimeZoneInfoWrap
{
  enum ObTZInfoClass
  {
    NONE = 0,
    POSITION = 1,
    OFFSET = 2
  };
  OB_UNIS_VERSION(1);
public:
  ObTimeZoneInfoWrap()
      : tz_info_pos_(),
      tz_info_offset_(),
      tz_info_(NULL),
      class_(NONE),
      cur_version_(0),
      error_on_overlap_time_(false)
      {
      }
  virtual ~ObTimeZoneInfoWrap() {}
  bool is_valid() const { return POSITION == class_ || OFFSET == class_; }
  void reset();
  const ObTimeZoneInfo *get_time_zone_info() const { return tz_info_; }
  ObTimeZoneInfoPos &get_tz_info_pos() { return tz_info_pos_; }
  const ObTimeZoneInfo &get_tz_info_offset() const { return tz_info_offset_; }
  int64_t get_cur_version() const { return cur_version_; }
  void set_cur_version(const int64_t version) { cur_version_ = version; }
  ObTZInfoClass get_tz_info_class() const { return class_; }
  bool is_position_class() const { return POSITION == class_; }
  bool is_error_on_overlap_time() const { return error_on_overlap_time_; }
  int set_error_on_overlap_time(bool is_error);
  int init_time_zone(const ObString &str_val, const int64_t curr_version, ObTZInfoMap &tz_info_map);
  void set_tz_info_map(const ObTZInfoMap *tz_info_map);
  int deep_copy(const ObTimeZoneInfoWrap &tz_inf_wrap);
  void set_tz_info_offset(const int32_t offset)
  {
    tz_info_offset_.set_offset(offset);
    tz_info_ = &tz_info_offset_;
    tz_info_->set_error_on_overlap_time(error_on_overlap_time_);
    class_ = OFFSET;
  }
  void set_tz_info_position()
  {
    tz_info_ = &tz_info_pos_;
    tz_info_->set_error_on_overlap_time(error_on_overlap_time_);
    class_ = POSITION;
  }
  VIRTUAL_TO_STRING_KV(K_(cur_version), "class", class_, KP_(tz_info),
                       K_(error_on_overlap_time), K_(tz_info_pos), K_(tz_info_offset));
private:
  common::ObTimeZoneInfoPos tz_info_pos_;
  common::ObTimeZoneInfo tz_info_offset_;
  common::ObTimeZoneInfo *tz_info_;
  ObTZInfoClass class_;
  int64_t cur_version_;
  bool error_on_overlap_time_;
};

struct ObIntervalYMValue
{
  static const int32_t MAX_YEAR_VALUE = 1000000000;
  static const int64_t MONTHS_IN_YEAR = 12;
  static const int64_t MAX_NMONTH_VALUE = MONTHS_IN_YEAR * MAX_YEAR_VALUE - 1;

  ObIntervalYMValue() : nmonth_(0) {}
  ObIntervalYMValue(int64_t nmonth)
    : nmonth_(nmonth)
  {}
  ObIntervalYMValue(const bool is_negative, const int64_t years, const int64_t month)
  {
    set_value(is_negative, years, month);
  }

  inline void set_value(const bool is_negative, const int64_t years, const int64_t month) {
    nmonth_ = (is_negative ? -1 : 1) * (years * MONTHS_IN_YEAR + month);
  }

  inline int64_t get_nmonth() const
  {
    return nmonth_;
  }

  inline bool operator<(const ObIntervalYMValue &other) const
  {
    return nmonth_ < other.nmonth_;
  }
  inline bool operator<=(const ObIntervalYMValue &other) const
  {
    return nmonth_ <= other.nmonth_;
  }
  inline bool operator>(const ObIntervalYMValue &other) const
  {
    return nmonth_ > other.nmonth_;
  }
  inline bool operator>=(const ObIntervalYMValue &other) const
  {
    return nmonth_ >= other.nmonth_;
  }
  inline bool operator==(const ObIntervalYMValue &other) const
  {
    return nmonth_ == other.nmonth_;
  }
  inline bool operator!=(const ObIntervalYMValue &other) const
  {
    return nmonth_ != other.nmonth_;
  }
  inline ObIntervalYMValue operator+(const ObIntervalYMValue &other) const
  {
    return ObIntervalYMValue(nmonth_ + other.nmonth_);
  }
  inline ObIntervalYMValue operator-(const ObIntervalYMValue &other) const
  {
    return ObIntervalYMValue(nmonth_ - other.nmonth_);
  }

  inline int validate()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(std::abs(nmonth_) > MAX_NMONTH_VALUE)) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
    }
    return ret;
  }

  int8_t calc_leading_scale();

  inline bool is_negative() const
  {
    return (nmonth_ < 0) ? true : false;
  }

  static inline int64_t get_store_size() { return sizeof(int64_t); }
  inline int decode(const char *buf)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      nmonth_ = *reinterpret_cast<int64_t *>(const_cast<char *>(buf));
    }
    return ret;
  }

  inline int encode(char *buf) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      *reinterpret_cast<int64_t *>(buf) = nmonth_;
    }
    return ret;
  }

  TO_STRING_KV(K_(nmonth));

  int64_t nmonth_;

  OB_UNIS_VERSION(1);

};

struct ObIntervalDSValue
{
  static const int64_t HOURS_IN_DAY = 24;
  static const int64_t MINUTES_IN_HOUR = 60;
  static const int64_t SECONDS_IN_MINUTE = 60;
  static const int32_t MAX_DAY_VALUE = 1000000000;
  static const int32_t MAX_FS_VALUE = 1000000000;
  static const int64_t MAX_NSECOND_VALUE = SECONDS_IN_MINUTE * MINUTES_IN_HOUR * HOURS_IN_DAY * MAX_DAY_VALUE - 1;
  static const int64_t SECONDS_IN_DAY = HOURS_IN_DAY * MINUTES_IN_HOUR * SECONDS_IN_MINUTE;

  static_assert(number::ObNumber::BASE == ObIntervalDSValue::MAX_FS_VALUE,
                "the div caculation between interval day to second and number "
                "is base on this constrain");

  ObIntervalDSValue() : fractional_second_(0), nsecond_(0) {}
  ObIntervalDSValue(int64_t nsecond, int32_t fractional_second)
    : fractional_second_(fractional_second),
      nsecond_(nsecond)
  {}
  ObIntervalDSValue(bool is_negative, int64_t days, int64_t hour, int64_t minute,
                    int64_t second, int32_t fractional_second)
  {
    set_value(is_negative, days, hour, minute, second, fractional_second);
  }

  inline void set_value(bool is_negative, int64_t days, int64_t hour, int64_t minute,
                        int64_t second, int32_t fractional_second) {
    nsecond_ = (is_negative ? -1 : 1) *
        (((days * HOURS_IN_DAY + hour) * MINUTES_IN_HOUR + minute) * SECONDS_IN_MINUTE + second);
    fractional_second_ = (is_negative ? -1 : 1) * fractional_second;
  }

  inline int64_t get_nsecond() const
  {
    return nsecond_;
  }

  inline int32_t get_fs() const
  {
    return fractional_second_;
  }

  inline bool is_negative() const
  {
    return nsecond_ != 0 ? (nsecond_ < 0 ? true : false) : (fractional_second_ < 0 ? true : false);
  }

  //e.g.  to keep 2 digit, set round_mask_base_10 = 10^7
  //      to keep 8 digit, set round_mask_base_10 = 10^1
  inline int round_fractional_second(const int32_t round_mask_base_10)
  {
    int32_t remains = fractional_second_ % round_mask_base_10;
    if (remains != 0) {
      fractional_second_ -= remains;
      if (std::abs(remains + remains) >= round_mask_base_10) {
        *this = *this + ObIntervalDSValue(0, remains > 0 ? round_mask_base_10 : -round_mask_base_10);
      }
    }
    return validate();
  }

  static inline uint32_t get_store_size() { return static_cast<uint32_t>(sizeof(int64_t) + sizeof(int32_t)); }
  inline int decode(const char *buf)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      nsecond_ = *reinterpret_cast<int64_t *>(const_cast<char *>(buf));
      buf += sizeof(int64_t);
      fractional_second_ = *reinterpret_cast<int32_t *>(const_cast<char *>(buf));
    }
    return ret;
  }
  inline int encode(char *buf) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      *reinterpret_cast<int64_t *>(buf) = nsecond_;
      buf += sizeof(int64_t);
      *reinterpret_cast<int32_t *>(buf) = fractional_second_;
    }
    return ret;
  }

  inline int compare(const ObIntervalDSValue &other) const
  {
    int result = 0;
    if (nsecond_ > other.nsecond_) {
      result = 1;
    } else if (nsecond_ < other.nsecond_) {
      result = -1;
    } else if (fractional_second_ > other.fractional_second_) {
      result = 1;
    } else if (fractional_second_ < other.fractional_second_) {
      result = -1;
    } else {
      result = 0;
    }
    return result;
  }
  inline bool operator<(const ObIntervalDSValue &other) const
  {
    return compare(other) < 0;
  }
  inline bool operator<=(const ObIntervalDSValue &other) const
  {
    return compare(other) <= 0;
  }
  inline bool operator>(const ObIntervalDSValue &other) const
  {
    return compare(other) > 0;
  }
  inline bool operator>=(const ObIntervalDSValue &other) const
  {
    return compare(other) >= 0;
  }
  inline bool operator==(const ObIntervalDSValue &other) const
  {
    return compare(other) == 0;
  }
  inline bool operator!=(const ObIntervalDSValue &other) const
  {
    return compare(other) != 0;
  }
  inline ObIntervalDSValue operator+(const ObIntervalDSValue &other) const
  {
    return calc(other, 1);
  }
  inline ObIntervalDSValue operator-(const ObIntervalDSValue &other) const
  {
    return calc(other, -1);
  }

  inline ObIntervalDSValue calc(const ObIntervalDSValue &other, int32_t op) const
  {
    int32_t fs_calc = fractional_second_ + op * other.fractional_second_;
    int64_t nsecond_calc = nsecond_ + op * other.nsecond_;
    if ((fs_calc < 0 && nsecond_calc > 0)
        || (fs_calc > 0 && nsecond_calc < 0)
        || std::abs(fs_calc) >= MAX_FS_VALUE) {
      int32_t sign = (fs_calc < 0) ? -1 : 1;
      fs_calc -= sign * MAX_FS_VALUE;
      nsecond_calc += sign;
    }
    return ObIntervalDSValue(nsecond_calc, fs_calc);
  }

  inline int validate()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(std::abs(nsecond_) > MAX_NSECOND_VALUE || std::abs(fractional_second_) >= MAX_FS_VALUE)) {
      ret = OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL;
    } else if (OB_UNLIKELY((nsecond_ < 0 && fractional_second_ > 0)
                           || (nsecond_ > 0 && fractional_second_ < 0))) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  int8_t calc_leading_scale();

  TO_STRING_KV(K_(nsecond), K_(fractional_second));

  //compat with object, store order is int32_t + int64_t
  int32_t fractional_second_;
  int64_t nsecond_;

  OB_UNIS_VERSION(1);
}__attribute__ ((packed));

struct ObIntervalScaleUtil
{
  //day_scale and second_scale must be in the range [0, 9]
  inline static int8_t interval_ds_scale_to_ob_scale(const int8_t day_scale, const int8_t second_scale)
  {
    return static_cast<int8_t>(day_scale * 10 + second_scale);
  }

  inline static int8_t ob_scale_to_interval_ds_day_scale(const int8_t scale)
  {
    return scale / 10;
  }

  inline static int8_t ob_scale_to_interval_ds_second_scale(const int8_t scale)
  {
    return scale % 10;
  }

  inline static int8_t interval_ym_scale_to_ob_scale(const int8_t year_scale)
  {
    return year_scale;
  }

  inline static int8_t ob_scale_to_interval_ym_year_scale(const int8_t scale)
  {
    return scale;
  }

  inline static int8_t calc_digits_base10(const int64_t value)
  {
    return static_cast<int8_t>(ceil(log10(value + 1)));
  }

  inline static bool scale_check(int32_t scale) {
    int ret_bool = true;
    if (scale < 0 || scale > MAX_SCALE_FOR_ORACLE_TEMPORAL) {
      ret_bool = false;
    }
    return ret_bool;
  }

};

inline int8_t ObIntervalYMValue::calc_leading_scale()
{
  return ObIntervalScaleUtil::calc_digits_base10(std::abs(nmonth_) / MONTHS_IN_YEAR);
}

inline int8_t ObIntervalDSValue::calc_leading_scale()
{
  return ObIntervalScaleUtil::calc_digits_base10(std::abs(nsecond_) / SECONDS_IN_DAY);
}


} // end of common
} // end of oceanbase

#endif // OCEANBASE_LIB_TIMEZONE_INFO_
