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

#ifndef _ALLOC_STRUCT_H_
#define _ALLOC_STRUCT_H_

#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <utility>
#include "lib/ob_define.h"
#include "lib/ob_abort.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/alloc/abit_set.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/list/ob_dlink_node.h"

#ifndef NDEBUG
#define MEMCHK_LEVEL 1
#endif

namespace oceanbase
{
namespace lib
{
#define ALIGN_UP(x, y) (((x) + ((y) - 1)) / (y) * (y))

// Align chunk we allocate by mmap function to MEMCHK_CHUNK_ALIGN if
// we want to coordinate the owner chunk of an arbitrary memory
// address. Notice size should large enough to hold most mmap
// allocates because we can only process with memory address past in
// ranged between 0 and this size.
static const uint64_t MEMCHK_CHUNK_ALIGN_BITS = 20;
static const uint64_t MEMCHK_CHUNK_ALIGN = 2UL << MEMCHK_CHUNK_ALIGN_BITS;

static const uint32_t AOBJECT_TAIL_SIZE = 16;
static const uint32_t AOBJECT_LABEL_SIZE = 15;
static const uint32_t MIN_AOBJECT_SIZE = 16;      // 16 bytes, 2 pointers
static const uint32_t AOBJECT_CELL_BYTES = 8;     // 8 bytes
static const uint32_t INTACT_ACHUNK_SIZE = 1 << 21;               // 2M
static const int64_t  INVISIBLE_CHARACTER = char(127);

static const int64_t ALLOC_ABLOCK_CONCURRENCY = 4;

static ssize_t get_page_size()
{
  static ssize_t ps = getpagesize();
  return ps;
}

class BlockSet;
class ObjectSet;

enum ObAllocPrio
{
  OB_NORMAL_ALLOC,
  OB_HIGH_ALLOC
};


struct ObLabel
{
  ObLabel()
    : str_(nullptr)
  {}
  template<std::size_t N>
  ObLabel(const char (&str)[N])
  {
    STATIC_ASSERT(N - 1 <= AOBJECT_LABEL_SIZE,
        "label length longer than 15 is not allowed!");
    str_ = str;
  }
  template <typename T, typename DUMP_T=
            typename std::enable_if<std::is_convertible<T, const char*>::value>::type>
  ObLabel(T str)
  {
    str_ = str;
  }
  // The caller needs to ensure that it ends with'\0'
  template<std::size_t N>
  ObLabel& operator=(const char (&str)[N])
  {
    STATIC_ASSERT(N - 1 <= AOBJECT_LABEL_SIZE,
        "label length longer than 15 is not allowed!");
    str_ = str;
    return *this;
  }
  template <typename T, typename DUMP_T=
            typename std::enable_if<std::is_convertible<T, const char*>::value>::type>
  ObLabel& operator=(T str)
  {
    str_ = str;
    return *this;
  }
  bool operator==(const ObLabel &other) const;
  template<typename T>
  bool operator==(const T &t) const
  {
    return operator==(ObLabel(t));
  }
  template<typename T>
  bool operator!=(const T &t) const
  {
    return !(*this == t);
  }
  // for format print
  operator const char*() const;
  bool is_valid() const { return nullptr != str_ && '\0' != str_[0]; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  const char *str_;
};

struct ObMemVersionNode : public ObDLinkBase<ObMemVersionNode>
{
  static uint32_t global_version;
  static __thread bool tl_ignore_node;
  static __thread ObMemVersionNode* tl_node;
  uint32_t version_ = UINT32_MAX;
};

struct ObMemAttr
{
  friend ObMemAttr DoNotUseMe(ObMemAttr &attr);
  friend ObMemAttr UseUnexpected500(ObMemAttr &attr);
  friend ObMemAttr IgnoreVersion(ObMemAttr &attr);
  uint64_t tenant_id_;
  ObLabel label_;
  uint64_t ctx_id_;
  uint64_t sub_ctx_id_;
  ObAllocPrio prio_;
  explicit ObMemAttr(
    uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
    ObLabel label = ObLabel(),
    uint64_t ctx_id = 0,
    ObAllocPrio prio = OB_NORMAL_ALLOC)
      : tenant_id_(tenant_id),
        label_(label),
        ctx_id_(ctx_id),
        sub_ctx_id_(ObSubCtxIds::MAX_SUB_CTX_ID),
        prio_(prio),
        use_500_(false),
        expect_500_(true),
        ignore_version_(ObMemVersionNode::tl_ignore_node),
        alloc_extra_info_(false)
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const;
  bool use_500() const { return use_500_; }
  bool expect_500() const { return expect_500_; }
  bool ignore_version() const { return ignore_version_; }
public:
  union {
    char padding__[4];
    struct {
      struct {
        uint8_t use_500_ : 1;
        uint8_t expect_500_ : 1;
        uint8_t ignore_version_ : 1;
        uint8_t alloc_extra_info_ : 1;
      };
    };
  };
};

inline ObMemAttr DoNotUseMe(ObMemAttr &attr)
{
  attr.use_500_ = true;
  attr.ignore_version_ = true;
  return attr;
}

inline ObMemAttr UseUnexpected500(ObMemAttr &attr)
{
  attr.use_500_ = true;
  attr.expect_500_ = false;
  attr.ignore_version_ = true;
  return attr;
}

inline ObMemAttr IgnoreVersion(ObMemAttr &attr)
{
  attr.ignore_version_ = true;
  return attr;
}

#define ObMemAttrFriendFunc(func_name)                                    \
  inline ObMemAttr func_name(const ObMemAttr &&attr)                      \
  {                                                                       \
    ObMemAttr attr_cpy = attr;                                            \
    return func_name(attr_cpy);                                           \
  }                                                                       \
  inline ObMemAttr func_name(const ObLabel &label)                        \
  {                                                                       \
    ObMemAttr attr(OB_SERVER_TENANT_ID, label);                           \
    return func_name(attr);                                               \
  }                                                                       \
  inline ObMemAttr func_name(const ObLabel &label, const uint64_t ctx_id) \
  {                                                                       \
    ObMemAttr attr(OB_SERVER_TENANT_ID, label, ctx_id);                   \
    return func_name(attr);                                               \
  }

ObMemAttrFriendFunc(DoNotUseMe);
ObMemAttrFriendFunc(UseUnexpected500);
ObMemAttrFriendFunc(IgnoreVersion);

#define SET_USE_500(args...) ::oceanbase::lib::DoNotUseMe(args)
#define SET_USE_UNEXPECTED_500(args...) ::oceanbase::lib::UseUnexpected500(args)
#define SET_IGNORE_MEM_VERSION(args...) ::oceanbase::lib::IgnoreVersion(args)

struct AllocHelper
{
  static uint32_t cells_per_block(uint32_t ablock_size)
  { return ablock_size >> 3; }
};

struct ABlock;
struct AChunk {
  static constexpr int MAX_BLOCKS_CNT = 256;
  OB_INLINE AChunk();
  OB_INLINE bool is_valid() const;
  OB_INLINE uint64_t hold(uint64_t *payload=nullptr) const;
  OB_INLINE uint64_t aligned();
  OB_INLINE static uint64_t calc_hold(int64_t size, int64_t washed_size=0, uint64_t *payload=nullptr);
  OB_INLINE static uint64_t aligned(int64_t size);
  OB_INLINE static AChunk *ptr2chunk(const void *ptr);
  OB_INLINE ABlock *ptr2blk(const void *ptr) const;
  OB_INLINE void mark_blk_offset_bit(int offset);
  OB_INLINE void unmark_blk_offset_bit(int offset);
  OB_INLINE ABlock *offset2blk(int offset) const;
  OB_INLINE bool is_first_blk_offset(int offset, int *prev_offset=nullptr) const;
  OB_INLINE bool is_last_blk_offset(int offset, int *next_offset=nullptr) const;
  OB_INLINE int max_blk_offset_le(int offset) const;
  OB_INLINE int min_blk_offset_ge(int offset) const;
  OB_INLINE int blk_offset(const ABlock *block) const;
  OB_INLINE int blk_nblocks(const ABlock *block) const;
  OB_INLINE char *blk_data(const ABlock *block) const;
  OB_INLINE void mark_unused_blk_offset_bit(int offset);
  OB_INLINE void unmark_unused_blk_offset_bit(int offset);
  OB_INLINE bool is_all_blks_unused();
  union {
    uint32_t MAGIC_CODE_;
    struct {
      struct {
        uint8_t is_hugetlb_ : 1;
      };
    };
  };
  BlockSet *block_set_;
  uint64_t washed_blks_;
  uint64_t washed_size_;
  uint64_t alloc_bytes_;
  AChunk *prev_, *next_; // ObTenantCtxAllocator's free_list or BlockSet's using_list
  AChunk *prev2_, *next2_; // ObTenantCtxAllocator's using_list
  ASimpleBitSet<MAX_BLOCKS_CNT> blk_bs_;
  ASimpleBitSet<MAX_BLOCKS_CNT> unused_blk_bs_;
  char data_[0];
} __attribute__ ((aligned (16)));


struct ABlock {
  OB_INLINE ABlock();
  OB_INLINE AChunk *chunk() const;
  OB_INLINE void clear_magic_code();
  OB_INLINE bool is_valid() const;
  OB_INLINE uint64_t hold(uint64_t *payload=nullptr) const;
  OB_INLINE char *data() const;

  union {
    uint32_t MAGIC_CODE_;
    struct {
      struct {
        uint8_t in_use_ : 1;
        uint8_t is_large_ : 1;
        uint8_t is_washed_ : 1;
      };
    };
  };

  uint64_t alloc_bytes_;
  uint32_t ablock_size_;
  ObjectSet *obj_set_;
  int64_t mem_context_;
  ABlock *prev_, *next_;
};

struct AObject {
  OB_INLINE AObject();
  OB_INLINE AObject *phy_next(int32_t steps) const;
  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_first() const;
  OB_INLINE bool is_last(uint32_t cells_per_block) const;
  OB_INLINE ABlock *block() const;
  OB_INLINE uint64_t hold(uint32_t cells_per_block) const;
  OB_INLINE ObLabel label() const;
  OB_INLINE char *bt();

  // members
  union {
    const uint16_t MAGIC_CODE_;
    struct {
      uint16_t in_use_ : 1;
      uint16_t is_large_ : 1;
    };
  } __attribute__((packed));

  uint16_t nobjs_;
  uint16_t nobjs_prev_;
  uint16_t obj_offset_;

  uint32_t alloc_bytes_;
  uint32_t version_;
  char label_[AOBJECT_LABEL_SIZE + 1];

  // padding to ensure data_ is 16x offset
  union {
    char padding__[16];
    struct {
      struct {
        uint8_t on_leak_check_ : 1;
        uint8_t on_malloc_sample_ : 1;
        uint8_t ignore_version_ : 1;

      };
    };
  };

  union {
    struct {
      AObject *prev_, *next_;
    };
    char data_[0];
  };
} __attribute__((aligned (32)));

static const uint64_t AOBJECT_TAIL_MAGIC_CODE     = 0X8F8F7E7E00E7F8F8;
static const uint32_t ACHUNK_MAGIC_CODE_MASK      = 0XFFFFFF00;
static const uint32_t ACHUNK_MAGIC_CODE           = 0XCEDEEC00;
static const uint32_t ABLOCK_MAGIC_CODE_MASK      = 0XFFFFFF00;
static const uint32_t ABLOCK_MAGIC_CODE           = 0XCCEEDD00;
static const uint16_t AOBJECT_MAGIC_CODE_MASK     = 0XFFF0;
static const uint16_t FREE_AOBJECT_MAGIC_CODE     = 0XCED0;
static const uint16_t AOBJECT_MAGIC_CODE          = 0XCED1;
static const uint16_t FREE_BIG_AOBJECT_MAGIC_CODE = 0XCED2;
static const uint16_t BIG_AOBJECT_MAGIC_CODE      = 0XCED3;

static const uint32_t AOBJECT_HEADER_SIZE = offsetof(AObject, data_);
static const uint32_t AOBJECT_META_SIZE = AOBJECT_HEADER_SIZE + AOBJECT_TAIL_SIZE;
static const uint32_t INTACT_NORMAL_AOBJECT_SIZE = 8L << 10;
static const uint32_t INTACT_MIDDLE_AOBJECT_SIZE = 64L << 10;

static const int32_t AOBJECT_BACKTRACE_COUNT = 16;
static const int32_t AOBJECT_BACKTRACE_SIZE = sizeof(void*) * AOBJECT_BACKTRACE_COUNT;
static const int32_t AOBJECT_EXTRA_INFO_SIZE = AOBJECT_BACKTRACE_SIZE;

static const int32_t MAX_BACKTRACE_LENGTH = 512;

static const uint32_t ABLOCK_HEADER_SIZE = sizeof(ABlock);
static const uint32_t ABLOCK_SIZE = INTACT_NORMAL_AOBJECT_SIZE;

static const uint32_t ACHUNK_PURE_HEADER_SIZE = offsetof(AChunk, data_);
static const uint32_t ACHUNK_HEADER_SIZE = 16L << 10;
static const uint32_t ACHUNK_SIZE = INTACT_ACHUNK_SIZE - ACHUNK_HEADER_SIZE;
static const uint64_t BLOCKS_PER_CHUNK = ACHUNK_SIZE / ABLOCK_SIZE;
static const uint64_t ABLOCK_ALIGN = 1L << 12;
STATIC_ASSERT(ACHUNK_HEADER_SIZE < ACHUNK_PRESERVE_SIZE &&
              0 == (ACHUNK_HEADER_SIZE & (ABLOCK_ALIGN - 1)) &&
              0 == (ABLOCK_SIZE & (ABLOCK_ALIGN - 1)) &&
              AChunk::MAX_BLOCKS_CNT > BLOCKS_PER_CHUNK &&
              ACHUNK_HEADER_SIZE >= BLOCKS_PER_CHUNK * ABLOCK_HEADER_SIZE &&
              0 == (ACHUNK_SIZE & (ABLOCK_SIZE - 1)), "meta check");

inline uint64_t align_up(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) / align * align;
}

inline uint64_t align_up2(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

AChunk::AChunk() :
    MAGIC_CODE_(ACHUNK_MAGIC_CODE),
    block_set_(nullptr),
    washed_blks_(0), washed_size_(0), alloc_bytes_(0),
    prev_(this), next_(this),
    prev2_(this), next2_(this)
{}

bool AChunk::is_valid() const
{
  return (MAGIC_CODE_ & ACHUNK_MAGIC_CODE_MASK) == ACHUNK_MAGIC_CODE;
}

uint64_t AChunk::hold(uint64_t *payload) const
{
  return calc_hold(alloc_bytes_, washed_size_, payload);
}

uint64_t AChunk::aligned()
{
  return aligned(alloc_bytes_);
}

uint64_t AChunk::calc_hold(int64_t size, int64_t washed_size, uint64_t *payload)
{
  const int64_t all_size = align_up2(size + ACHUNK_HEADER_SIZE, INTACT_ACHUNK_SIZE);
  uint64_t hold = (all_size == INTACT_ACHUNK_SIZE ? all_size : align_up2(size + ACHUNK_HEADER_SIZE, get_page_size()))
    - washed_size;
  if (payload) *payload = hold - ACHUNK_HEADER_SIZE;
  return hold;
}

uint64_t AChunk::aligned(int64_t size)
{
  return align_up2(size + ACHUNK_HEADER_SIZE, INTACT_ACHUNK_SIZE);
}

AChunk *AChunk::ptr2chunk(const void *ptr)
{
  uint64_t aligned_addr = (uint64_t)(ptr) & ~(MEMCHK_CHUNK_ALIGN - 1);
  return (AChunk*)aligned_addr;
}

ABlock *AChunk::ptr2blk(const void *ptr) const
{
  int from = (int)(((char*)ptr - (char*)this - ACHUNK_HEADER_SIZE) / ABLOCK_SIZE);
  int offset = max_blk_offset_le(from);
  ABlock *block = (ABlock*)data_ + offset;
  return block;
}

void AChunk::mark_blk_offset_bit(int offset)
{
  blk_bs_.set(offset);
}

void AChunk::mark_unused_blk_offset_bit(int offset)
{
  unused_blk_bs_.set(offset);
}

void AChunk::unmark_blk_offset_bit(int offset)
{
  blk_bs_.unset(offset);
}

void AChunk::unmark_unused_blk_offset_bit(int offset)
{
  unused_blk_bs_.unset(offset);
}

bool AChunk::is_all_blks_unused()
{
  bool ret = false;
  if (0 != washed_size_) {
    auto blk_bs = blk_bs_;
    blk_bs.combine(unused_blk_bs_,
          [](int64_t left, int64_t right) { return (left ^ right); });
    ret = -1 == blk_bs.min_bit_ge(0);
  } else {
    ret = -1 == blk_bs_.min_bit_ge(1);
  }
  return ret;
}

ABlock *AChunk::offset2blk(int offset) const
{
  return (ABlock*)data_ + offset;
}

bool AChunk::is_first_blk_offset(int offset, int *prev_offset) const
{
  bool bret = 0 == offset;
  if (!bret && prev_offset != nullptr) {
    *prev_offset = max_blk_offset_le(offset - 1);
  }
  return bret;
}

bool AChunk::is_last_blk_offset(int offset, int *next_offset) const
{
  int tmp_offset = -1;
  int *p_next_offset = next_offset ?: &tmp_offset;
  return -1 == (*p_next_offset = min_blk_offset_ge(offset + 1));
}

int AChunk::max_blk_offset_le(int offset) const
{
  return blk_bs_.max_bit_le(offset);
}

int AChunk::min_blk_offset_ge(int offset) const
{
  return blk_bs_.min_bit_ge(offset);
}

int AChunk::blk_offset(const ABlock *block) const
{
  int offset = (int)(block - (ABlock*)data_);
  abort_unless(offset >=0 && offset < BLOCKS_PER_CHUNK);
  return offset;
}

int AChunk::blk_nblocks(const ABlock *block) const
{
  int offset = blk_offset(block);
  int next_offset = -1;
  int nblocks = is_last_blk_offset(offset, &next_offset) ? BLOCKS_PER_CHUNK - offset : next_offset - offset;
  return nblocks;
}

char *AChunk::blk_data(const ABlock *block) const
{
  int32_t c_offset = (int32_t)((char*)block - data_);
  int32_t b_offset = c_offset / ABLOCK_HEADER_SIZE;
  return (char*)this + ACHUNK_HEADER_SIZE + b_offset * ABLOCK_SIZE;
}

ABlock::ABlock() :
    MAGIC_CODE_(ABLOCK_MAGIC_CODE),
    alloc_bytes_(0),
    ablock_size_(0),
    obj_set_(NULL), mem_context_(0),
    prev_(this), next_(this)
{}

AChunk *ABlock::chunk() const
{
  return AChunk::ptr2chunk(this);
}

void ABlock::clear_magic_code()
{
  MAGIC_CODE_ = (~ABLOCK_MAGIC_CODE & ~((1 << 8) - 1)) | (MAGIC_CODE_ & ((1 << 8) - 1));
}

bool ABlock::is_valid() const
{
  return (MAGIC_CODE_ & ABLOCK_MAGIC_CODE_MASK) == ABLOCK_MAGIC_CODE;
}

uint64_t ABlock::hold(uint64_t *payload) const
{
  AChunk *chunk = this->chunk();
  int offset = chunk->blk_offset(this);
  uint64_t hold = 0;
  if (is_large_ || (chunk->is_first_blk_offset(offset) && chunk->is_last_blk_offset(offset))) {
    hold = chunk->hold(payload);
  } else {
    hold = chunk->blk_nblocks(this) * ABLOCK_SIZE;
    if (payload) *payload = hold;
  }
  return hold;
}

char *ABlock::data() const
{
  AChunk *chunk = this->chunk();
  abort_unless(chunk->is_valid());
  return chunk->blk_data(this);
}

AObject::AObject()
    : MAGIC_CODE_(FREE_AOBJECT_MAGIC_CODE),
      nobjs_(0), nobjs_prev_(0), obj_offset_(0),
      alloc_bytes_(0), on_leak_check_(false), on_malloc_sample_(false)
{
}

AObject *AObject::phy_next(int32_t steps) const
{
  AObject *obj = (AObject *)((char*)this + static_cast<int32_t>(steps * AOBJECT_CELL_BYTES));
  return obj;
}

bool AObject::is_valid() const
{
  return (MAGIC_CODE_ & AOBJECT_MAGIC_CODE_MASK) == FREE_AOBJECT_MAGIC_CODE;
}

bool AObject::is_first() const
{
  return 0 == obj_offset_;
}

bool AObject::is_last(uint32_t cells_per_block) const
{
  return obj_offset_ + nobjs_ >= cells_per_block;
}

ABlock *AObject::block() const
{
  AChunk *chunk = AChunk::ptr2chunk(this);
  abort_unless(chunk->is_valid());
  ABlock *block = chunk->ptr2blk(this);
  return block;
}

uint64_t AObject::hold(uint32_t cells_per_block) const
{
  return is_large_ || (is_last(cells_per_block) && is_first())
      ? block()->hold()
      : nobjs_ * AOBJECT_CELL_BYTES;
}

ObLabel AObject::label() const
{
  return ObLabel(label_);
}
char *AObject::bt()
{
  return &data_[alloc_bytes_ + AOBJECT_TAIL_SIZE];
}

class Label
{
  constexpr static int MAX_LEN = AOBJECT_LABEL_SIZE;
public:
  Label()
    : label_(nullptr)
  {}
  Label(const char *str)
    : label_(str)
  {}
  template <typename... Args>
  Label(Args && ... args)
    : label_(buf_)
  {
    typedef int expander[];
    int64_t pos = 0;
    (void)expander{0, (void(fmt(buf_, MAX_LEN + 1, pos, std::forward<Args>(args))), 0)...};
  }
  operator const char*() const { return label_; }
  const char *label_;
private:
  static void fmt(char *buf, int64_t buf_len, int64_t &pos, const char *str);
private:
  char buf_[MAX_LEN + 1];
};

class ObMallocHookAttrGuard
{
public:
  ObMallocHookAttrGuard(const ObMemAttr& attr);
  ~ObMallocHookAttrGuard();
  static ObMemAttr get_tl_mem_attr()
  {
    return tl_mem_attr;
  }
private:
  static thread_local ObMemAttr tl_mem_attr;
  ObMemAttr old_attr_;
};

class ObLightBacktraceGuard
{
public:
  ObLightBacktraceGuard(const bool enable)
    : last_(tl_enable())
  {
    tl_enable() = enable;
  }
  ~ObLightBacktraceGuard()
  {
    tl_enable() = last_;
  }
public:
  static bool is_enabled()
  {
    return tl_enable();
  }
private:
  static bool &tl_enable()
  {
    static __thread bool enable = false;
    return enable;
  }
private:
  const bool last_;
};

extern void inc_divisive_mem_size(const int64_t size);
extern void dec_divisive_mem_size(const int64_t size);
extern int64_t get_divisive_mem_size();

extern void set_ob_mem_mgr_path();
extern void unset_ob_mem_mgr_path();
extern bool is_ob_mem_mgr_path();

extern void enable_memleak_light_backtrace(const bool);
extern bool is_memleak_light_backtrace_enabled();

#define FORCE_EXPLICT_500_MALLOC() \
  OB_UNLIKELY(oceanbase::lib::ObMallocAllocator::get_instance()->force_explict_500_malloc_)

#define FORCE_MALLOC_FOR_ABSENT_TENANT() \
  OB_UNLIKELY(oceanbase::lib::ObMallocAllocator::get_instance()->force_malloc_for_absent_tenant_)
} // end of namespace lib
} // end of namespace oceanbase

#endif /* _ALLOC_STRUCT_H_ */
