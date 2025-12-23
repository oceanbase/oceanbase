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
#include "lib/atomic/ob_atomic.h"

#ifndef NDEBUG
#define MEMCHK_LEVEL 1
#endif
class ObjectSetV2;
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
  int32_t numa_id_;
  ObAllocPrio prio_;
  explicit ObMemAttr(
    uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
    ObLabel label = ObLabel(),
    uint64_t ctx_id = 0,
    ObAllocPrio prio = OB_NORMAL_ALLOC)
      : tenant_id_(tenant_id),
        label_(label),
        ctx_id_(ctx_id),
        numa_id_(0),
        prio_(prio),
        use_500_(false),
        expect_500_(true),
        ignore_version_(ObMemVersionNode::tl_ignore_node),
        enable_malloc_hang_(false),
        extra_size_(0)
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const;
  bool use_500() const { return use_500_; }
  bool expect_500() const { return expect_500_; }
  bool ignore_version() const { return ignore_version_; }
public:
  union { //FARM COMPAT WHITELIST
    char padding__[4];
    struct {
      struct {
        uint8_t use_500_ : 1;
        uint8_t expect_500_ : 1;
        uint8_t ignore_version_ : 1;
        uint8_t enable_malloc_hang_ : 1;
      };
      uint16_t extra_size_;
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
  union {
    uint32_t MAGIC_CODE_;
    struct {
      struct {
        uint8_t is_hugetlb_ : 1;
      };
    };
  };
#ifdef ENABLE_SANITY
  void *ref_;
#endif
  int32_t numa_id_;
  int32_t using_cnt_;
  BlockSet *block_set_;
  uint64_t washed_blks_;
  uint64_t washed_size_;
  uint64_t alloc_bytes_;
  AChunk *prev_, *next_; // ObTenantCtxAllocator's free_list or BlockSet's using_list
  AChunk *prev2_, *next2_; // ObTenantCtxAllocator's using_list
  ASimpleBitSet<MAX_BLOCKS_CNT> blk_bs_;
#ifdef ENABLE_SANITY
  char padding_[8];
#else
  char padding_[16];
#endif
  char data_[0];
} __attribute__ ((aligned (16)));

class AObject;
struct AObjectList
{
  struct Head {
    int64_t cnt_      : 10;
    int64_t addr_     : 54;
  };
  AObjectList()
    : v_(0)
  {}
  void reset(AObject *obj, int64_t cnt)
  {
    Head h;
    h.cnt_ = cnt;
    h.addr_ = (int64_t)obj;
    ATOMIC_BCAS(&v_, 0, *(int64_t*)&h);
  }
  void reset() { v_ = 0; }
  int64_t push(AObject *obj);
  AObject *popall(int64_t *cnt = NULL)
  {
    int64_t v = ATOMIC_TAS(&v_, 0);
    Head *h = (Head*)&v;
    if (NULL != cnt) *cnt = h->cnt_;
    return (AObject*)h->addr_;
  }
  int64_t v_;
};

struct AObjectListSafe
{
  struct List {
    int64_t cnt_ : 10;
    int64_t head_ : 27;
    int64_t tail_ : 27;
  };
  AObjectListSafe()
    : v_(0)
  {}
  void reset() { v_ = 0; }
  OB_NOINLINE int64_t push(AObject *obj);
  int64_t popall(AObject *&head, AObject *&tail)
  {
    int64_t v = ATOMIC_TAS(&v_, 0);
    List *l = (List*)&v;
    tail = (AObject*)((char*)this + l->tail_);
    head = (AObject*)((char*)this + l->head_);
    return l->cnt_;
  }
  int64_t v_;
};

struct AObjectListUnsafe
{
  static const int64_t HEAD_BITS = 54;
  static const int64_t HEAD_MASK = (1LL<<HEAD_BITS) - 1;
  AObjectListUnsafe()
    : v_(mask(0))
  {}
  int64_t mask(int64_t v)
  {
    return (int64_t)this ^ v;
  }
  int64_t remask(int64_t v)
  {
    return v ^ (int64_t)this;
  }
  bool is_empty()
  {
    return mask(0) == v_;
  }
  void reset(AObject *obj, int64_t cnt)
  {
    v_ = mask((int64_t)obj) | (cnt<<HEAD_BITS);
  }
  void reset() { v_ = mask(0); }
  int64_t push(AObject *obj)
  {
    return push(obj, obj, 1);
  }
  int64_t push(AObject *head, AObject *tail, int64_t cnt);
  AObject *popall()
  {
    AObject *head = (AObject*)remask(v_ & HEAD_MASK);
    reset();
    return head;
  }
  int64_t v_;
};

struct ABlock {
  enum {
    FULL,
    PARTITIAL,
    EMPTY,
  };
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
        uint8_t status_ : 2;
      };
    };
  };

  uint32_t alloc_bytes_;
  uint32_t ablock_size_;
  uint16_t sc_idx_;
  uint16_t max_cnt_;

  union { //FARM COMPAT WHITELIST
    // for malloc
    AObjectList freelist_;
    // for malloc_v2
    struct {
      AObjectListUnsafe local_free_;
      AObjectListSafe remote_free_;
    };
  };
  union { //FARM COMPAT WHITELIST
    ObjectSet *obj_set_;
    ObjectSetV2 *obj_set_v2_;
  };
  ABlock *prev_, *next_;
  ABlock *next2_;
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
  OB_INLINE void set_label(const char* label)
  {
    if (nullptr != label) {
      STRNCPY(label_, label, AOBJECT_LABEL_SIZE);
      label_[AOBJECT_LABEL_SIZE] = '\0';
    } else {
      MEMSET(label_, '\0', AOBJECT_LABEL_SIZE + 1);
    }
  }

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
      ABlock *block_;
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
              ACHUNK_HEADER_SIZE >= ACHUNK_PURE_HEADER_SIZE + BLOCKS_PER_CHUNK * ABLOCK_HEADER_SIZE &&
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
#ifdef ENABLE_SANITY
    ref_(nullptr),
#endif
    numa_id_(0),
    using_cnt_(0),
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

void AChunk::unmark_blk_offset_bit(int offset)
{
  blk_bs_.unset(offset);
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


inline int64_t AObjectListUnsafe::push(AObject *head, AObject *tail, int64_t cnt)
{
  int64_t n_cnt = (v_ >> HEAD_BITS) + cnt;
  tail->next_ = (AObject*)remask(v_ & HEAD_MASK);
  reset(head, n_cnt);
  return n_cnt;
}

inline int64_t AObjectList::push(AObject *obj)
{
  int64_t ov = ATOMIC_LOAD(&v_);
  Head nh;
  nh.addr_ = (int64_t)obj;
  do {
    Head oh = *(Head*)&ov;
    obj->next_ = (AObject*)oh.addr_;
    nh.cnt_ = oh.cnt_ + 1;
  } while (!ATOMIC_CMP_AND_EXCHANGE(&v_, &ov, *(int64_t*)&nh));
  return nh.cnt_;
}

inline int64_t AObjectListSafe::push(AObject *obj)
{
  int64_t ov = ATOMIC_LOAD(&v_);
  List nl;
  nl.head_ = (int64_t)obj - (int64_t)this;
  do {
    List ol = *(List*)&ov;
    if (0 == ol.head_) {
      obj->next_ = NULL;
      nl.tail_ = nl.head_;
    } else {
      obj->next_ = (AObject*)((char*)this + ol.head_);
      nl.tail_ = ol.tail_;
    }
    nl.cnt_ = ol.cnt_ + 1;
  } while (!ATOMIC_CMP_AND_EXCHANGE(&v_, &ov, *(int64_t*)&nl));
  return nl.cnt_;
}

ABlock::ABlock() :
    MAGIC_CODE_(ABLOCK_MAGIC_CODE),
    alloc_bytes_(0),
    ablock_size_(0),
    obj_set_(NULL),
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
      alloc_bytes_(0), on_leak_check_(false), on_malloc_sample_(false),
      block_(NULL)
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
  DEBUG_ASSERT(chunk->is_valid());
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
  ObMallocHookAttrGuard(const ObMemAttr& attr, const bool use_500 = true);
  ~ObMallocHookAttrGuard();
  static ObMemAttr &get_tl_mem_attr()
  {
    static thread_local ObMemAttr tl_mem_attr(OB_SERVER_TENANT_ID,
                                              "glibc_malloc",
                                              ObCtxIds::GLIBC);
    return tl_mem_attr;
  }
  static bool &get_tl_use_500()
  {
    static __thread bool tl_use_500 = true;
    return tl_use_500;
  }
private:
  ObMemAttr old_attr_;
  bool old_use_500_;
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
    static __thread bool enable = true;
    return enable;
  }
private:
  const bool last_;
};

class ObUnmanagedMemoryStat
{
public:
  class DisableGuard
  {
  public:
    DisableGuard()
      : last_(tl_disabled())
    {
      tl_disabled() = true;
    }
    ~DisableGuard()
    {
      tl_disabled() = last_;
    }
    static bool &tl_disabled()
    {
      static __thread bool disabled = false;
      return disabled;
    }
  private:
    bool last_;
  };
  static ObUnmanagedMemoryStat &get_instance()
  {
    static ObUnmanagedMemoryStat instance;
    return instance;
  }
  static bool is_disabled()
  {
    return DisableGuard::tl_disabled();
  }
  void inc(const int64_t size);
  void dec(const int64_t size);
  int64_t get_total_hold();
private:
  ObUnmanagedMemoryStat()
  {}
  int64_t hold_[OB_MAX_CPU_NUM];
};

#define UNMAMAGED_MEMORY_STAT ObUnmanagedMemoryStat::get_instance()

extern int64_t get_unmanaged_memory_size();

extern void enable_memleak_light_backtrace(const bool);
extern bool is_memleak_light_backtrace_enabled();

#define FORCE_EXPLICT_500_MALLOC() \
  OB_UNLIKELY(oceanbase::lib::ObMallocAllocator::get_instance()->force_explict_500_malloc_)

#define FORCE_MALLOC_FOR_ABSENT_TENANT() \
  OB_UNLIKELY(oceanbase::lib::ObMallocAllocator::get_instance()->force_malloc_for_absent_tenant_)
} // end of namespace lib
} // end of namespace oceanbase

#endif /* _ALLOC_STRUCT_H_ */
