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

/*
*
* Version: $Id
*
* Authors:
*      - initial release
*
*/

#ifndef OB_BUILD_FULL_CHARSET

#include "lib/charset/ob_dtoa.h"
#include "lib/charset/ob_mysql_global.h"

#define DTOA_OVERFLOW 9999
#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define NOT_FIXED_DEC 31
#define DTOA_BUF_MAX_SIZE (460 * sizeof(void *))
#define Scale_Bit 0x10
#define n_bigtens 5

static void dtoa_free(char *, char *, size_t);
static double ob_strtod_int(const char *, char **, int *, char *, size_t);
static char *dtoa(double, int, int, int *, int *, char **, char *, size_t);
void ob_fcvt_help(char **end, char **dst, char **dend, int sign, int decpt,
    int *precision, char **res);
void ob_fcvt_help_opt(char **end, char **dst, char **dend, int sign, int decpt,
    int *precision, char **res, bool add_padding_zero);
size_t ob_fcvt_overflow(char *to, bool *error)
{
  *to++= '0';
  *to= '\0';
  if (error != NULL) {
    *error= TRUE;
  }
  return 1;
}


size_t ob_fcvt(double x, int precision, int width, char *to, bool *error)
{
  int decpt, sign;
  char *res, *end, *dst= to, *dend= to + width;
  char buf[DTOA_BUF_MAX_SIZE];
  if (!(precision >= 0 && precision < 31 && to != NULL)) {
    return 0;
  }
  res = dtoa(x, 5, precision, &decpt, &sign, &end, buf, sizeof(buf));
  if (decpt == DTOA_OVERFLOW) {
    dtoa_free(res, buf, sizeof(buf));
    return ob_fcvt_overflow(to, error);
  }
  ob_fcvt_help(&end, &dst, &dend, sign, decpt, &precision, &res);
  *dst= '\0';
  if (error != NULL) {
    *error= FALSE;
  }
  dtoa_free(res, buf, sizeof(buf));
  return dst - to;
}

size_t ob_fcvt_opt(double x, int precision, int width, char *to, bool *error, bool add_padding_zero)
{
  int decpt, sign;
  char *res, *end, *dst= to, *dend= to + width;
  char buf[DTOA_BUF_MAX_SIZE];
  if (!(precision >= 0 && precision < 31 && to != NULL)) {
    return 0;
  }
  res = dtoa(x, 5, precision, &decpt, &sign, &end, buf, sizeof(buf));
  if (decpt == DTOA_OVERFLOW) {
    dtoa_free(res, buf, sizeof(buf));
    return ob_fcvt_overflow(to, error);
  }
  ob_fcvt_help_opt(&end, &dst, &dend, sign, decpt, &precision, &res, add_padding_zero);
  *dst= '\0';
  if (error != NULL)
    *error= FALSE;
  dtoa_free(res, buf, sizeof(buf));
  return dst - to;
}


void ob_fcvt_help(char **end, char **dst, char **dend, int sign, int decpt,
    int *precision, char **res)
{
   const int len = (*end) - (*res);
   const char *dend_ptr = *dend;
   char *dst_ptr = *dst;
   char *src = (*res);
   int i = 0;

   if (dst_ptr < dend_ptr) {
     if (sign)
       *dst_ptr++= '-';
     if (decpt <= 0)
     {
       if ((dst_ptr + 1) < dend_ptr) {
         *dst_ptr++= '0';
         *dst_ptr++= '.';
       }
       for (i= decpt; i < 0 && dst_ptr < dend_ptr; i++)
         *dst_ptr++= '0';
     }
     for (i= 1; i <= len && dst_ptr < dend_ptr; i++)
     {
       *dst_ptr++= *src++;
       if (i == decpt && i < len && dst_ptr < dend_ptr)
         *dst_ptr++= '.';
     }
     while (i++ <= decpt && dst_ptr < dend_ptr)
       *dst_ptr++= '0';
     if (*precision > 0)
     {
       if (len <= decpt && dst_ptr < dend_ptr)
         *dst_ptr++= '.';
       for (i= *precision - OB_MAX(0, (len - decpt)); i > 0 && dst_ptr < dend_ptr; i--)
         *dst_ptr++= '0';
     }
     *dst = dst_ptr;
   }
}

void ob_fcvt_help_opt(char **end, char **dst, char **dend, int sign, int decpt,
    int *precision, char **res, bool add_padding_zero)
{
   const int len = (*end) - (*res);
   const char *dend_ptr = *dend;
   char *dst_ptr = *dst;
   char *src = (*res);
   int i = 0;

   if (dst_ptr < dend_ptr) {
     if (sign)
       *dst_ptr++= '-';
     if (decpt <= 0)
     {
       if ((dst_ptr + 1) < dend_ptr) {
         *dst_ptr++= '0';
         *dst_ptr++= '.';
       }
       for (i= decpt; i < 0 && dst_ptr < dend_ptr; i++)
         *dst_ptr++= '0';
     }
     for (i= 1; i <= len && dst_ptr < dend_ptr; i++)
     {
       *dst_ptr++= *src++;
       if (i == decpt && i < len && dst_ptr < dend_ptr)
         *dst_ptr++= '.';
     }
     while (i++ <= decpt && dst_ptr < dend_ptr)
       *dst_ptr++= '0';
     if (*precision > 0 && add_padding_zero)
     {
       if (len <= decpt && dst_ptr < dend_ptr)
         *dst_ptr++= '.';
       for (i= *precision - OB_MAX(0, (len - decpt)); i > 0 && dst_ptr < dend_ptr; i--)
         *dst_ptr++= '0';
     }
     *dst = dst_ptr;
   }
}


size_t ob_gcvt_overflow(char *to, bool *error)
{
  *to++= '0';
  *to= '\0';
  if (error != NULL)
    *error= TRUE;
  return 1;
}

void ob_gcvt_help1(int *width, int *len, char **dend, char **src,
                   char **end, char **dst, int sign, int decpt, char *buf, size_t sizeofbuf,
                   char **res, bool *error, double x)
{
  const char *dend_ptr = *dend;
  int i = 0;
  char *dst_ptr = *dst;
  (*width) -= (decpt < *len) + (decpt <= 0 ? 1 - decpt : 0);
  if ((*width) < *len)
  {
    if ((*width) < decpt)
    {
      if (error != NULL)
        *error= TRUE;
      (*width)= decpt;
    }
    dtoa_free(*res, buf, sizeofbuf);
    *res= dtoa(x, 5, (*width) - decpt, &decpt, &sign, &(*end), buf, sizeofbuf);
    *src= *res;
    *len= (*end) - *res;
  }
  if (*len == 0)
  {
    if ((*dst) < (*dend))
      *(*dst)++= '0';
    return ;
  }

  if (sign && dst_ptr < dend_ptr)
    *dst_ptr++= '-';
  if (decpt <= 0)
  {
    if (dst_ptr < dend_ptr)
      *dst_ptr++= '0';
    if (*len > 0 && dst_ptr < dend_ptr)
      *dst_ptr++= '.';
    for (; decpt < 0 && dst_ptr < dend_ptr; decpt++)
      *dst_ptr++= '0';
  }
  for (i= 1; i <= *len && dst_ptr < dend_ptr; i++)
  {
    *dst_ptr++= *(*src)++;
    if (i == decpt && i < *len && dst_ptr < dend_ptr)
      *dst_ptr++= '.';
  }
  while (i++ <= decpt && dst_ptr < dend_ptr)
    *dst_ptr++= '0';
  *dst = dst_ptr;
}

void ob_gcvt_help2(int *width, int *len, char **dend, char **src,
                   char **end, char **dst, int sign, int decpt, char *buf, size_t sizeofbuf,
                   int *exp_len, char **res, bool *error, double x, bool use_oracle_mode)
{
  const char *dend_ptr = *dend;
  char *dst_ptr = *dst;
  char *src_ptr = *src;
  int decpt_sign= 0;
  const int MAX_DOUBLE_SIZE = 30;
  if (--decpt < 0)
  {
    decpt= -decpt;
    (*width)--;
    decpt_sign= 1;
  }
  (*width)-= 1 + *exp_len;
  if ((*len) > 1)
    (*width)--;

  if ((*width) <= 0)
  {
    if (error != NULL)
      *error= TRUE;
    (*width)= 0;
  }
  if ((*width) < (*len))
  {
    dtoa_free((*res), buf, sizeofbuf);
    (*res)= dtoa(x, 4, (*width), &decpt, &sign, &(*end), buf, sizeofbuf);
    src_ptr= (*res);
    (*len)= (*end) - (*res);
    if (--decpt < 0)
      decpt= -decpt;
  }

  const int need_check_buf = (*dend - *dst) < MAX_DOUBLE_SIZE;
  if (need_check_buf) {
    if (sign && dst_ptr < dend_ptr)
      *dst_ptr++= '-';
    if (dst_ptr < dend_ptr)
      *dst_ptr++= *src_ptr++;
    const int is_zero = (dst_ptr < dend_ptr && use_oracle_mode && (*(src_ptr - 1) == '0') && ((*len) == 1));
    if (is_zero) {
      if (sign) {
        *(dst_ptr - 2) = '0';
        dst_ptr--;
      }
    } else {
      if ((*len) > 1 && dst_ptr < dend_ptr)
      {
        *dst_ptr++= '.';
        while (src_ptr < (*end) && dst_ptr < dend_ptr)
          *dst_ptr++= *src_ptr++;
      } else if (use_oracle_mode && (*len) == 1 && (dst_ptr + 2) < dend_ptr) {
        *dst_ptr++= '.';
        *dst_ptr++= '0';
      }
      if (dst_ptr < dend_ptr)
        *dst_ptr++= (use_oracle_mode ? 'E' : 'e');
      if (dst_ptr < dend_ptr) {
        if (decpt_sign) {
          *dst_ptr++= '-';
        } else if (use_oracle_mode) {
          *dst_ptr++= '+';
        }
      }
      if (decpt >= 100 && dst_ptr < dend_ptr) {
        *dst_ptr++= decpt / 100 + '0';
        decpt%= 100;
        if (dst_ptr < dend_ptr) {
          *dst_ptr++= decpt / 10 + '0';
          if (dst_ptr < dend_ptr)
            *dst_ptr++= decpt % 10 + '0';
        }
      } else if (decpt >= 10 && dst_ptr < dend_ptr) {
        if (use_oracle_mode && dst_ptr + 1 < dend_ptr) {
          *dst_ptr++= '0';
        }
        *dst_ptr++= decpt / 10 + '0';
        if (dst_ptr < dend_ptr)
          *dst_ptr++= decpt % 10 + '0';
      } else {
        if (use_oracle_mode && dst_ptr + 2 < dend_ptr) {
          *dst_ptr++= '0';
          *dst_ptr++= '0';
        }
        if (dst_ptr < dend_ptr)
          *dst_ptr++= decpt % 10 + '0';
      }
    }
  } else {
    if (sign)
      *dst_ptr++= '-';
    *dst_ptr++= *src_ptr++;
    int is_zero = (use_oracle_mode && (*(src_ptr - 1) == '0') && ((*len) == 1));
    if (is_zero) {
      if (sign) {
        *(dst_ptr - 2) = '0';
        dst_ptr--;
      }
    } else {
      if ((*len) > 1)
      {
        *dst_ptr++= '.';
        while (src_ptr < (*end))
          *dst_ptr++= *src_ptr++;
      } else if (use_oracle_mode && (*len) == 1) {
        *dst_ptr++= '.';
        *dst_ptr++= '0';
      }
      *dst_ptr++= (use_oracle_mode ? 'E' : 'e');
      if (decpt_sign) {
        *dst_ptr++= '-';
      } else if (use_oracle_mode) {
        *dst_ptr++= '+';
      }
      if (decpt >= 100) {
        *dst_ptr++= decpt / 100 + '0';
        decpt%= 100;
        *dst_ptr++= decpt / 10 + '0';
        *dst_ptr++= decpt % 10 + '0';
      } else if (decpt >= 10) {
        if (use_oracle_mode) {
          *dst_ptr++= '0';
        }
        *dst_ptr++= decpt / 10 + '0';
        *dst_ptr++= decpt % 10 + '0';
      } else {
        if (use_oracle_mode) {
          *dst_ptr++= '0';
          *dst_ptr++= '0';
        }
        *dst_ptr++= decpt % 10 + '0';
      }
    }
  }

  *dst = dst_ptr;
  *src = src_ptr;
}

size_t ob_gcvt(double x, ob_gcvt_arg_type type, int width, char *to, bool *error)
{
  return ob_gcvt_strict(x, type, width, to, error, FALSE, TRUE, FALSE);
}

size_t ob_gcvt_opt(double x, ob_gcvt_arg_type type, int width, char *to,
                      bool *error, bool use_oracle_mode, bool is_binary_double)
{
  return ob_gcvt_strict(x, type, width, to, error, use_oracle_mode, 
                        is_binary_double, use_oracle_mode);
}

size_t ob_gcvt_strict(double x, ob_gcvt_arg_type type, int width, char *to,
    bool *error, bool use_oracle_mode, bool is_binary_double, bool use_force_e_format)
{
  int width_check = width;
  int decpt, sign, len, exp_len;
  char *res, *src, *end, *dst= to, *dend= dst + width;
  char buf[DTOA_BUF_MAX_SIZE];
  bool have_space, force_e_format;
  const int TMP_MAX_DECPT_FOR_F_FORMAT = MAX_DECPT_FOR_F_FORMAT;
  if (!(width > 0 && to != NULL)) {
    return 0;
  }
  if (x < 0.)
    width--;
  use_oracle_mode &= is_binary_double;
  const int ORACLE_FLT_DIG = 9;
  const int ORACLE_DBL_DIG = 17;
  const int CURR_FLT_DIG = (use_oracle_mode ? ORACLE_FLT_DIG : FLT_DIG);
  const int dtoa_width = (type == OB_GCVT_ARG_DOUBLE
                          ? (use_oracle_mode ? OB_MIN(ORACLE_DBL_DIG, width) : width)
                          : (OB_MIN(width, CURR_FLT_DIG)));
  const int mode = use_oracle_mode ? 2 : 4;
  res= dtoa(x, mode, dtoa_width, &decpt, &sign, &end, buf, sizeof(buf));
  if (decpt == DTOA_OVERFLOW) {
    dtoa_free(res, buf, sizeof(buf));
    return ob_gcvt_overflow(to, error);
  }
  if (error != NULL)
    *error= FALSE;
  src= res;
  len= end - res;

  exp_len= 1 + (decpt >= 101 || decpt <= -99) + (decpt >= 11 || decpt <= -9);
  have_space= (decpt <= 0
               ? (len - decpt + 2)
               : ((decpt > 0 && decpt < len)
                  ? (len + 1)
                  : decpt))
               <= width;

  force_e_format= (decpt <= 0 && width <= (2 - decpt) && width >= (3 + exp_len));

  if (!use_force_e_format
      && ((have_space && (decpt >= -TMP_MAX_DECPT_FOR_F_FORMAT + 1 && (decpt <= TMP_MAX_DECPT_FOR_F_FORMAT || len > decpt)))
          || (!have_space && !force_e_format && (decpt <= width && (decpt >= -1 || decpt == -2)))))
  {
    ob_gcvt_help1(&width, &len, &dend, &src,
                  &end, &dst, sign, decpt, buf, sizeof(buf),
                  &res, error, x);
  }
  else
  {
    ob_gcvt_help2(&width, &len, &dend, &src,
                  &end, &dst, sign, decpt, buf, sizeof(buf),
                  &exp_len, &res, error, x, use_oracle_mode);
  }
  dtoa_free(res, buf, sizeof(buf));
  if (dst - to < width_check) {
    *dst= '\0';
  } else {
    return 0;
  }
  return dst - to;
}

double ob_strtod(const char *str, char **end, int *error)
{
  char buf[DTOA_BUF_MAX_SIZE];
  double res = 0.0;
  if (!(end != NULL && ((str != NULL && *end != NULL) ||
                              (str == NULL && *end == NULL)) &&
              error != NULL)) {
    return 0.0;
  }
  res= ob_strtod_int(str, end, error, buf, sizeof(buf));
  return (*error == 0) ? res : (res < 0 ? -DBL_MAX : DBL_MAX);
}


double ob_atof(const char *nptr)
{
  int error;
  const char *end= nptr+65535;               
  return (ob_strtod(nptr, (char**) &end, &error));
}


typedef int32 Long;
typedef uint32 ULong;
typedef int64 LLong;
typedef uint64 ULLong;

typedef union { double d; ULong L[2]; } U;

#if defined(WORDS_BIGENDIAN) || (defined(__FLOAT_WORD_ORDER) &&        \
                                 (__FLOAT_WORD_ORDER == __BIG_ENDIAN))
COPY_BIGINT WORD0(x) (x)->L[0]
#define WORD1(x) (x)->L[1]
#else
#define WORD0(x) (x)->L[1]
#define WORD1(x) (x)->L[0]
#endif

#define dval(x) (x)->d

#define Exp_shift  20
#define Exp_shift1 20
#define Exp_msk1    0x100000
#define Exp_mask  0x7ff00000
#define P 53
#define Bias 1023
#define Emin (-1022)
#define Exp_1  0x3ff00000
#define Exp_11 0x3ff00000
#define Ebits 11
#define Frac_mask  0xfffff
#define Frac_mask1 0xfffff
#define Ten_pmax 22
#define Bletch 0x10
#define Bndry_mask  0xfffff
#define Bndry_mask1 0xfffff
#define LSB 1
#define Sign_bit 0x80000000
#define Log2P 1
#define Tiny1 1
#define Quick_max 14
#define Int_max 14

#ifndef Flt_Rounds
#ifdef FLT_ROUNDS
#define Flt_Rounds FLT_ROUNDS
#else
#define Flt_Rounds 1
#endif
#endif /*Flt_Rounds*/

#ifdef Honor_FLT_ROUNDS
#define Rounding rounding
#undef Check_FLT_ROUNDS
#define Check_FLT_ROUNDS
#else
#define Rounding Flt_Rounds
#endif

#define rounded_product(a,b) a*= b
#define rounded_quotient(a,b) a/= b

#define Big0 (Frac_mask1 | Exp_msk1*(DBL_MAX_EXP+Bias-1))
#define Big1 0xffffffff
#define FFFFFFFF 0xffffffffUL


#define Kmax 15

#define COPY_BIGINT(x,y) memcpy((char *)&x->sign, (char *)&y->sign,   \
                          2*sizeof(int) + y->wds*sizeof(ULong))


typedef struct _bigint
{
  union {
    ULong *x;              
    struct _bigint *next;   
  } p;
  int k;                   
  int maxwds;             
  int sign;               
  int wds;                
} Bigint;



typedef struct ObStackAllocator
{
  char *begin;
  char *free;
  char *end;
  Bigint *freelist[Kmax+1];
} ObStackAllocator;


static Bigint *alloc_bigint(int k, ObStackAllocator *alloc)
{
  Bigint *rv;
  assert(k <= Kmax);
  if (k <= Kmax &&  alloc->freelist[k]) {
    rv = alloc->freelist[k];
    alloc->freelist[k] = rv->p.next;
  } else {
    int x, len;
    x = 1 << k;
    len = MY_ALIGN(sizeof(Bigint) + x * sizeof(ULong), SIZEOF_CHARP);
    if (alloc->free + len <= alloc->end) {
      rv = (Bigint*) alloc->free;
      alloc->free += len;
    } else {
      rv = (Bigint*) malloc(len);
    }
    rv->k = k;
    rv->maxwds = x;
  }
  rv->sign = rv->wds = 0;
  rv->p.x = (ULong*) (rv + 1);
  return rv;
}



static void free_bigint(Bigint *v, ObStackAllocator *alloc)
{
  if (v != NULL) {
    char *g_ptr= (char*) v;                     
    if (g_ptr < alloc->begin || g_ptr >= alloc->end) {
      free(g_ptr);
    } else if (v->k <= Kmax) {
      v->p.next= alloc->freelist[v->k];
      alloc->freelist[v->k]= v;
    }
  }
}


static char *dtoa_alloc(int i, ObStackAllocator *alloc)
{
  char *rv;
  int aligned_size = MY_ALIGN(i, SIZEOF_CHARP);
  if (alloc->free + aligned_size <= alloc->end) {
    rv = alloc->free;
    alloc->free += aligned_size;
  } else {
    rv = (char*)malloc(i);
  }
  return rv;
}

static void dtoa_free(char *g_ptr, char *buf, size_t buf_size)
{
  if (g_ptr < buf || g_ptr >= buf + buf_size) {
    free(g_ptr);
  }
}


static Bigint *mult_and_add(Bigint *b, int m, int a, ObStackAllocator *alloc)
{
  int i, wds;
  ULong *x;
  ULLong carry, y;
  Bigint *b1;

  wds= b->wds;
  x= b->p.x;
  i= 0;
  carry= a;
  do
  {
    y= *x * (ULLong)m + carry;
    carry= y >> 32;
    *x++= (ULong)(y & FFFFFFFF);
  }
  while (++i < wds);
  if (carry)
  {
    if (wds >= b->maxwds)
    {
      b1= alloc_bigint(b->k+1, alloc);
      COPY_BIGINT(b1, b);
      free_bigint(b, alloc);
      b= b1;
    }
    b->p.x[wds++]= (ULong) carry;
    b->wds= wds;
  }
  return b;
}

static Bigint *string2bigint(const char *s, int nd0, int nd, ULong y9, ObStackAllocator *alloc)
{
  Bigint *b;
  int i, k;
  Long x, y;

  x= (nd + 8) / 9;
  for (k= 0, y= 1; x > y; y <<= 1, k++) ;
  b= alloc_bigint(k, alloc);
  b->p.x[0]= y9;
  b->wds= 1;

  i= 9;
  if (9 < nd0)
  {
    s+= 9;
    do
      b= mult_and_add(b, 10, *s++ - '0', alloc);
    while (++i < nd0);
    s++;                                        
  }
  else
    s+= 10;
  for(; i < nd; i++)
    b= mult_and_add(b, 10, *s++ - '0', alloc);
  return b;
}


static int hi0bits(register ULong x)
{
  register int k= 0;

  if (!(x & 0xffff0000))
  {
    k= 16;
    x<<= 16;
  }
  if (!(x & 0xff000000))
  {
    k+= 8;
    x<<= 8;
  }
  if (!(x & 0xf0000000))
  {
    k+= 4;
    x<<= 4;
  }
  if (!(x & 0xc0000000))
  {
    k+= 2;
    x<<= 2;
  }
  if (!(x & 0x80000000))
  {
    k++;
    if (!(x & 0x40000000))
      return 32;
  }
  return k;
}


static int lo0bits(ULong *y)
{
  register int k;
  register ULong x= *y;

  if (x & 7)
  {
    if (x & 1)
      return 0;
    if (x & 2)
    {
      *y= x >> 1;
      return 1;
    }
    *y= x >> 2;
    return 2;
  }
  k= 0;
  if (!(x & 0xffff))
  {
    k= 16;
    x>>= 16;
  }
  if (!(x & 0xff))
  {
    k+= 8;
    x>>= 8;
  }
  if (!(x & 0xf))
  {
    k+= 4;
    x>>= 4;
  }
  if (!(x & 0x3))
  {
    k+= 2;
    x>>= 2;
  }
  if (!(x & 1))
  {
    k++;
    x>>= 1;
    if (!x)
      return 32;
  }
  *y= x;
  return k;
}


static Bigint *integer2bigint(int i, ObStackAllocator *alloc)
{
  Bigint *b;
  b= alloc_bigint(1, alloc);
  b->p.x[0]= i;
  b->wds= 1;
  return b;
}



static Bigint *bigint_mul_bigint(Bigint *a, Bigint *b, ObStackAllocator *alloc)
{
  Bigint *c;
  int k, wa, wb, wc;
  ULong *x, *xa, *xae, *xb, *xbe, *xc, *xc0;
  ULong y;
  ULLong carry, z;

  if (a->wds < b->wds)
  {
    c= a;
    a= b;
    b= c;
  }
  k= a->k;
  wa= a->wds;
  wb= b->wds;
  wc= wa + wb;
  if (wc > a->maxwds)
    k++;
  c= alloc_bigint(k, alloc);
  for (x= c->p.x, xa= x + wc; x < xa; x++)
    *x= 0;
  xa= a->p.x;
  xae= xa + wa;
  xb= b->p.x;
  xbe= xb + wb;
  xc0= c->p.x;
  for (; xb < xbe; xc0++)
  {
    if ((y= *xb++))
    {
      x= xa;
      xc= xc0;
      carry= 0;
      do
      {
        z= *x++ * (ULLong)y + *xc + carry;
        carry= z >> 32;
        *xc++= (ULong) (z & FFFFFFFF);
      }
      while (x < xae);
      *xc= (ULong) carry;
    }
  }
  for (xc0= c->p.x, xc= xc0 + wc; wc > 0 && !*--xc; --wc) ;
  c->wds= wc;
  return c;
}


static ULong powers5[]=
{
  625UL,

  390625UL,

  2264035265UL, 35UL,

  2242703233UL, 762134875UL,  1262UL,

  3211403009UL, 1849224548UL, 3668416493UL, 3913284084UL, 1593091UL,

  781532673UL,  64985353UL,   253049085UL,  594863151UL,  3553621484UL,
  3288652808UL, 3167596762UL, 2788392729UL, 3911132675UL, 590UL,

  2553183233UL, 3201533787UL, 3638140786UL, 303378311UL, 1809731782UL,
  3477761648UL, 3583367183UL, 649228654UL, 2915460784UL, 487929380UL,
  1011012442UL, 1677677582UL, 3428152256UL, 1710878487UL, 1438394610UL,
  2161952759UL, 4100910556UL, 1608314830UL, 349175UL
};


static Bigint p5_a[]=
{
  { { powers5 }, 1, 1, 0, 1 },
  { { powers5 + 1 }, 1, 1, 0, 1 },
  { { powers5 + 2 }, 1, 2, 0, 2 },
  { { powers5 + 4 }, 2, 3, 0, 3 },
  { { powers5 + 7 }, 3, 5, 0, 5 },
  { { powers5 + 12 }, 4, 10, 0, 10 },
  { { powers5 + 22 }, 5, 19, 0, 19 }
};

#define P5A_MAX (sizeof(p5_a)/sizeof(*p5_a) - 1)

static Bigint *pow5mult(Bigint *b, int k, ObStackAllocator *alloc)
{
  Bigint *b1, *p5, *p51=NULL;
  int i;
  static int p05[3]= { 5, 25, 125 };
  bool overflow= FALSE;

  if ((i= k & 3))
    b= mult_and_add(b, p05[i-1], 0, alloc);

  if (!(k>>= 2))
    return b;
  p5= p5_a;
  for (;;)
  {
    if (k & 1)
    {
      b1= bigint_mul_bigint(b, p5, alloc);
      free_bigint(b, alloc);
      b= b1;
    }
    if (!(k>>= 1))
      break;
    if (overflow)
    {
      p51= bigint_mul_bigint(p5, p5, alloc);
      free_bigint(p5, alloc);
      p5= p51;
    }
    else if (p5 < p5_a + P5A_MAX)
      ++p5;
    else if (p5 == p5_a + P5A_MAX)
    {
      p5= bigint_mul_bigint(p5, p5, alloc);
      overflow= TRUE;
    }
  }
  if (p51)
    free_bigint(p51, alloc);
  return b;
}


static Bigint *left_shift(Bigint *b, int k, ObStackAllocator *alloc)
{
  int i, k1, n, n1;
  Bigint *b1;
  ULong *x, *x1, *xe, z;

  n= k >> 5;
  k1= b->k;
  n1= n + b->wds + 1;
  for (i= b->maxwds; n1 > i; i<<= 1)
    k1++;
  b1= alloc_bigint(k1, alloc);
  x1= b1->p.x;
  for (i= 0; i < n; i++)
    *x1++= 0;
  x= b->p.x;
  xe= x + b->wds;
  if (k&= 0x1f)
  {
    k1= 32 - k;
    z= 0;
    do
    {
      *x1++= *x << k | z;
      z= *x++ >> k1;
    }
    while (x < xe);
    if ((*x1= z))
      ++n1;
  }
  else
    do
      *x1++= *x++;
    while (x < xe);
  b1->wds= n1 - 1;
  free_bigint(b, alloc);
  return b1;
}


static int bigint_cmp(Bigint *a, Bigint *b)
{
  ULong *xa, *xa0, *xb, *xb0;
  int i, j;

  i= a->wds;
  j= b->wds;
  if (i-= j)
    return i;
  xa0= a->p.x;
  xa= xa0 + j;
  xb0= b->p.x;
  xb= xb0 + j;
  for (;;)
  {
    if (*--xa != *--xb)
      return *xa < *xb ? -1 : 1;
    if (xa <= xa0)
      break;
  }
  return 0;
}


static Bigint *bigint_diff(Bigint *a, Bigint *b, ObStackAllocator *alloc)
{
  Bigint *c;
  int i, wa, wb;
  ULong *xa, *xae, *xb, *xbe, *xc;
  ULLong borrow, y;

  i= bigint_cmp(a,b);
  if (!i)
  {
    c= alloc_bigint(0, alloc);
    c->wds= 1;
    c->p.x[0]= 0;
    return c;
  }
  if (i < 0)
  {
    c= a;
    a= b;
    b= c;
    i= 1;
  }
  else
    i= 0;
  c= alloc_bigint(a->k, alloc);
  c->sign= i;
  wa= a->wds;
  xa= a->p.x;
  xae= xa + wa;
  wb= b->wds;
  xb= b->p.x;
  xbe= xb + wb;
  xc= c->p.x;
  borrow= 0;
  do
  {
    y= (ULLong)*xa++ - *xb++ - borrow;
    borrow= y >> 32 & (ULong)1;
    *xc++= (ULong) (y & FFFFFFFF);
  }
  while (xb < xbe);
  while (xa < xae)
  {
    y= *xa++ - borrow;
    borrow= y >> 32 & (ULong)1;
    *xc++= (ULong) (y & FFFFFFFF);
  }
  while (!*--xc)
    wa--;
  c->wds= wa;
  return c;
}


static double ulp(U *x)
{
  register Long L;
  U u;

  L= (WORD0(x) & Exp_mask) - (P - 1)*Exp_msk1;
  WORD0(&u) = L;
  WORD1(&u) = 0;
  return dval(&u);
}


static double b2d(Bigint *a, int *e)
{
  ULong *xa, *xa0, w, y, z;
  int k;
  U d;
#define d0 WORD0(&d)
#define d1 WORD1(&d)

  xa0= a->p.x;
  xa= xa0 + a->wds;
  y= *--xa;
  k= hi0bits(y);
  *e= 32 - k;
  if (k < Ebits)
  {
    d0= Exp_1 | y >> (Ebits - k);
    w= xa > xa0 ? *--xa : 0;
    d1= y << ((32-Ebits) + k) | w >> (Ebits - k);
    goto ret_d;
  }
  z= xa > xa0 ? *--xa : 0;
  if (k-= Ebits)
  {
    d0= Exp_1 | y << k | z >> (32 - k);
    y= xa > xa0 ? *--xa : 0;
    d1= z << k | y >> (32 - k);
  }
  else
  {
    d0= Exp_1 | y;
    d1= z;
  }
 ret_d:
#undef d0
#undef d1
  return dval(&d);
}


static Bigint *d2b(U *d, int *e, int *bits, ObStackAllocator *alloc)
{
  Bigint *b;
  int de, k;
  ULong *x, y, z;
  int i;
#define d0 WORD0(d)
#define d1 WORD1(d)

  b= alloc_bigint(1, alloc);
  x= b->p.x;

  z= d0 & Frac_mask;
  d0 &= 0x7fffffff;      
  if ((de= (int)(d0 >> Exp_shift)))
    z|= Exp_msk1;
  if ((y= d1))
  {
    if ((k= lo0bits(&y)))
    {
      x[0]= y | z << (32 - k);
      if (k >= 0 && k <32) {
        z >>= k;
      } else {
      }
    }
    else
      x[0]= y;
    i= b->wds= (x[1]= z) ? 2 : 1;
  }
  else
  {
    k= lo0bits(&z);
    x[0]= z;
    i= b->wds= 1;
    k+= 32;
  }
  if (de)
  {
    *e= de - Bias - (P-1) + k;
    *bits= P - k;
  }
  else
  {
    *e= de - Bias - (P-1) + 1 + k;
    *bits= 32*i - hi0bits(x[i-1]);
  }
  return b;
#undef d0
#undef d1
}


static double ratio(Bigint *a, Bigint *b)
{
  U da, db;
  int k, ka, kb;

  dval(&da)= b2d(a, &ka);
  dval(&db)= b2d(b, &kb);
  k= ka - kb + 32*(a->wds - b->wds);
  if (k > 0)
    WORD0(&da)+= k*Exp_msk1;
  else
  {
    k= -k;
    WORD0(&db)+= k*Exp_msk1;
  }
  return dval(&da) / dval(&db);
}

static const double tens[] =
{
  1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
  1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
  1e20, 1e21, 1e22
};

static const double bigtens[]= { 1e16, 1e32, 1e64, 1e128, 1e256 };
static const double tinytens[]=
{ 1e-16, 1e-32, 1e-64, 1e-128,
  9007199254740992.*9007199254740992.e-256 
};

static double ob_strtod_int(const char *s00, char **se, int *error, char *buf, size_t buf_size)
{
  int scale;
  int bb2, bb5, bbe, bd2, bd5, bbbits, bs2, c = 0, dsign,
     e, e1, esign, i, j, k, nd, nd0, nf, nz, nz0, sign;
  const char *s, *s0, *s1, *end = *se;
  double aadj, aadj1;
  U aadj2, adj, rv, rv0;
  Long L;
  ULong y, z;
  Bigint *bb, *bb1, *bd, *bd0, *bs, *delta;
#ifdef SET_INEXACT
  int inexact, oldinexact;
#endif
#ifdef Honor_FLT_ROUNDS
  int rounding;
#endif
  ObStackAllocator alloc;

  *error= 0;

  alloc.begin= alloc.free= buf;
  alloc.end= buf + buf_size;
  memset(alloc.freelist, 0, sizeof(alloc.freelist));

  sign= nz0= nz= 0;
  dval(&rv)= 0.;
  for (s= s00; s < end; s++)
    switch (*s) {
    case '-':
      sign= 1;
    case '+':
      s++;
      goto break2;
    case '\t':
    case '\n':
    case '\v':
    case '\f':
    case '\r':
    case ' ':
      continue;
    default:
      goto break2;
    }
 break2:
  if (s >= end)
    goto ret0;

  if (*s == '0')
  {
    nz0= 1;
    while (++s < end && *s == '0') ;
    if (s >= end)
      goto ret;
  }
  s0= s;
  y= z= 0;
  for (nd= nf= 0; s < end && (c= *s) >= '0' && c <= '9'; nd++, s++)
    if (nd < 9)
      y= 10*y + c - '0';
    else if (nd < 16)
      z= 10*z + c - '0';
  nd0= nd;
  if (s < end && c == '.')
  {
    s++;
    if(s < end) {
      c = *s;
    }
    if (!nd)
    {
      for (; s < end; ++s)
      {
        c= *s;
        if (c != '0')
          break;
        nz++;
      }
      if (s < end && c > '0' && c <= '9')
      {
        s0= s;
        nf+= nz;
        nz= 0;
      }
      else
        goto dig_done;
    }
    for (; s < end; ++s)
    {
      c= *s;
      if (c < '0' || c > '9')
        break;
      if (nd < 2 * DBL_DIG)
      {
        nz++;
        if (c-= '0')
        {
          nf+= nz;
          for (i= 1; i < nz; i++)
            if (nd++ < 9)
              y*= 10;
            else if (nd <= DBL_DIG + 1)
              z*= 10;
          if (nd++ < 9)
            y= 10*y + c;
          else if (nd <= DBL_DIG + 1)
            z= 10*z + c;
          nz= 0;
        }
      }
    }
  }
 dig_done:
  e= 0;
  if (s < end && (c == 'e' || c == 'E'))
  {
    if (!nd && !nz && !nz0)
      goto ret0;
    s00= s;
    esign= 0;
    if (++s < end)
      switch (c= *s) {
      case '-':
        esign= 1;
        if (++s < end)
          c = *s;
        break;
      case '+':
        if (++s < end)
          c = *s;
        break;
      }
    if (s < end && c >= '0' && c <= '9')
    {
      while (s < end && c == '0') {
        s++;
        if(s < end)
          c= *s;
      }
      if (s < end && c > '0' && c <= '9') {
        L= c - '0';
        s1= s;
        while (++s < end && (c= *s) >= '0' && c <= '9')
          L= 10*L + c - '0';
        if (s - s1 > 8 || L > 19999)
          e= 19999; 
        else
          e= (int)L;
        if (esign)
          e= -e;
      }
      else
        e= 0;
    }
    else
      s= s00;
  }
  if (!nd)
  {
    if (!nz && !nz0)
    {
 ret0:
      s= s00;
      sign= 0;
    }
    goto ret;
  }
  e1= e -= nf;
  if (!nd0)
    nd0= nd;
  k= nd < DBL_DIG + 1 ? nd : DBL_DIG + 1;
  dval(&rv)= y;
  if (k > 9)
  {
#ifdef SET_INEXACT
    if (k > DBL_DIG)
      oldinexact = get_inexact();
#endif
    dval(&rv)= tens[k - 9] * dval(&rv) + z;
  }
  bd0= 0;
  if (nd <= DBL_DIG
#ifndef Honor_FLT_ROUNDS
    && Flt_Rounds == 1
#endif
      )
  {
    if (!e)
      goto ret;
    if (e > 0)
    {
      if (e <= Ten_pmax)
      {
#ifdef Honor_FLT_ROUNDS
        if (sign)
        {
          rv.d= -rv.d;
          sign= 0;
        }
#endif
        rounded_product(dval(&rv), tens[e]);
        goto ret;
      }
      i= DBL_DIG - nd;
      if (e <= Ten_pmax + i)
      {
#ifdef Honor_FLT_ROUNDS
        if (sign)
        {
          rv.d= -rv.d;
          sign= 0;
        }
#endif
        e-= i;
        dval(&rv)*= tens[i];
        rounded_product(dval(&rv), tens[e]);
        goto ret;
      }
    }
#ifndef Inaccurate_Divide
    else if (e >= -Ten_pmax)
    {
#ifdef Honor_FLT_ROUNDS
      if (sign)
      {
        rv.d= -rv.d;
        sign= 0;
      }
#endif
      rounded_quotient(dval(&rv), tens[-e]);
      goto ret;
    }
#endif
  }
  e1+= nd - k;

#ifdef SET_INEXACT
  inexact= 1;
  if (k <= DBL_DIG)
    oldinexact= get_inexact();
#endif
  scale= 0;
#ifdef Honor_FLT_ROUNDS
  if ((rounding= Flt_Rounds) >= 2)
  {
    if (sign)
      rounding= rounding == 2 ? 0 : 2;
    else
      if (rounding != 2)
        rounding= 0;
  }
#endif


  if (e1 > 0)
  {
    if ((i= e1 & 15))
      dval(&rv)*= tens[i];
    if (e1&= ~15)
    {
      if (e1 > DBL_MAX_10_EXP)
      {
 ovfl:
        *error= EOVERFLOW;
        
#ifdef Honor_FLT_ROUNDS
        switch (rounding)
        {
        case 0:
        case 3:
          WORD0(&rv)= Big0;
          WORD1(&rv)= Big1;
          break;
        default:
          WORD0(&rv)= Exp_mask;
          WORD1(&rv)= 0;
        }
#else 
        WORD0(&rv)= Exp_mask;
        WORD1(&rv)= 0;
#endif 
#ifdef SET_INEXACT
        dval(&rv0)= 1e300;
        dval(&rv0)*= dval(&rv0);
#endif
        if (bd0)
          goto retfree;
        goto ret;
      }
      e1>>= 4;
      for(j= 0; e1 > 1; j++, e1>>= 1)
        if (e1 & 1)
          dval(&rv)*= bigtens[j];
      WORD0(&rv)-= P*Exp_msk1;
      dval(&rv)*= bigtens[j];
      if ((z= WORD0(&rv) & Exp_mask) > Exp_msk1 * (DBL_MAX_EXP + Bias - P))
        goto ovfl;
      if (z > Exp_msk1 * (DBL_MAX_EXP + Bias - 1 - P))
      {
        WORD0(&rv)= Big0;
        WORD1(&rv)= Big1;
      }
      else
        WORD0(&rv)+= P*Exp_msk1;
    }
  }
  else if (e1 < 0)
  {
    e1= -e1;
    if ((i= e1 & 15))
      dval(&rv)/= tens[i];
    if ((e1>>= 4))
    {
      if (e1 >= 1 << n_bigtens)
        goto undfl;
      if (e1 & Scale_Bit)
        scale= 2 * P;
      for(j= 0; e1 > 0; j++, e1>>= 1)
        if (e1 & 1)
          dval(&rv)*= tinytens[j];
      if (scale && (j = 2 * P + 1 - ((WORD0(&rv) & Exp_mask) >> Exp_shift)) > 0)
      {
        if (j >= 32)
        {
          WORD1(&rv)= 0;
          if (j >= 53)
            WORD0(&rv)= (P + 2) * Exp_msk1;
          else
            WORD0(&rv)&= 0xffffffff << (j - 32);
        }
        else
          WORD1(&rv)&= 0xffffffff << j;
      }
      if (!dval(&rv))
      {
 undfl:
          dval(&rv)= 0.;
          if (bd0)
            goto retfree;
          goto ret;
      }
    }
  }

  bd0= string2bigint(s0, nd0, nd, y, &alloc);

  for(;;)
  {
    bd= alloc_bigint(bd0->k, &alloc);
    COPY_BIGINT(bd, bd0);
    bb= d2b(&rv, &bbe, &bbbits, &alloc); 
    bs= integer2bigint(1, &alloc);

    if (e >= 0)
    {
      bb2= bb5= 0;
      bd2= bd5= e;
    }
    else
    {
      bb2= bb5= -e;
      bd2= bd5= 0;
    }
    if (bbe >= 0)
      bb2+= bbe;
    else
      bd2-= bbe;
    bs2= bb2;
#ifdef Honor_FLT_ROUNDS
    if (rounding != 1)
      bs2++;
#endif
    j= bbe - scale;
    i= j + bbbits - 1; 
    if (i < Emin)  
      j+= P - Emin;
    else
      j= P + 1 - bbbits;
    bb2+= j;
    bd2+= j;
    bd2+= scale;
    i= bb2 < bd2 ? bb2 : bd2;
    if (i > bs2)
      i= bs2;
    if (i > 0)
    {
      bb2-= i;
      bd2-= i;
      bs2-= i;
    }
    if (bb5 > 0)
    {
      bs= pow5mult(bs, bb5, &alloc);
      bb1= bigint_mul_bigint(bs, bb, &alloc);
      free_bigint(bb, &alloc);
      bb= bb1;
    }
    if (bb2 > 0)
      bb= left_shift(bb, bb2, &alloc);
    if (bd5 > 0)
      bd= pow5mult(bd, bd5, &alloc);
    if (bd2 > 0)
      bd= left_shift(bd, bd2, &alloc);
    if (bs2 > 0)
      bs= left_shift(bs, bs2, &alloc);
    delta= bigint_diff(bb, bd, &alloc);
    dsign= delta->sign;
    delta->sign= 0;
    i= bigint_cmp(delta, bs);
#ifdef Honor_FLT_ROUNDS
    if (rounding != 1)
    {
      if (i < 0)
      {
        if (!delta->p.x[0] && delta->wds <= 1)
        {
#ifdef SET_INEXACT
          inexact= 0;
#endif
          break;
        }
        if (rounding)
        {
          if (dsign)
          {
            adj.d= 1.;
            goto apply_adj;
          }
        }
        else if (!dsign)
        {
          adj.d= -1.;
          if (!WORD1(&rv) && !(WORD0(&rv) & Frac_mask))
          {
            y= WORD0(&rv) & Exp_mask;
            if (!scale || y > 2*P*Exp_msk1)
            {
              delta= left_shift(delta, Log2P, &alloc);
              if (bigint_cmp(delta, bs) <= 0)
              adj.d= -0.5;
            }
          }
 apply_adj:
          if (scale && (y= WORD0(&rv) & Exp_mask) <= 2 * P * Exp_msk1)
            WORD0(&adj)+= (2 * P + 1) * Exp_msk1 - y;
          dval(&rv)+= adj.d * ulp(&rv);
        }
        break;
      }
      adj.d= ratio(delta, bs);
      if (adj.d < 1.)
        adj.d= 1.;
      if (adj.d <= 0x7ffffffe)
      {
        y= adj.d;
        if (y != adj.d)
        {
          if (!((rounding >> 1) ^ dsign))
            y++;
          adj.d= y;
        }
      }
      if (scale && (y= WORD0(&rv) & Exp_mask) <= 2 * P * Exp_msk1)
        WORD0(&adj)+= (2 * P + 1) * Exp_msk1 - y;
      adj.d*= ulp(&rv);
      if (dsign)
        dval(&rv)+= adj.d;
      else
        dval(&rv)-= adj.d;
      goto cont;
    }
#endif

    if (i < 0)
    {
      if (dsign || WORD1(&rv) || WORD0(&rv) & Bndry_mask ||
          (WORD0(&rv) & Exp_mask) <= (2 * P + 1) * Exp_msk1)
      {
#ifdef SET_INEXACT
        if (!delta->x[0] && delta->wds <= 1)
          inexact= 0;
#endif
        break;
      }
      if (!delta->p.x[0] && delta->wds <= 1)
      {
#ifdef SET_INEXACT
        inexact= 0;
#endif
        break;
      }
      delta= left_shift(delta, Log2P, &alloc);
      if (bigint_cmp(delta, bs) > 0)
        goto drop_down;
      break;
    }
    if (i == 0)
    {
      if (dsign)
      {
        if ((WORD0(&rv) & Bndry_mask1) == Bndry_mask1 &&
            WORD1(&rv) ==
            ((scale && (y = WORD0(&rv) & Exp_mask) <= 2 * P * Exp_msk1) ?
             (0xffffffff & (0xffffffff << (2*P+1-(y>>Exp_shift)))) :
             0xffffffff))
        {
          WORD0(&rv)= (WORD0(&rv) & Exp_mask) + Exp_msk1;
          WORD1(&rv) = 0;
          dsign = 0;
          break;
        }
      }
      else if (!(WORD0(&rv) & Bndry_mask) && !WORD1(&rv))
      {
 drop_down:

        if (scale)
        {
          L= WORD0(&rv) & Exp_mask;
          if (L <= (2 *P + 1) * Exp_msk1)
          {
            if (L > (P + 2) * Exp_msk1)
              break;
            goto undfl;
          }
        }
        L= (WORD0(&rv) & Exp_mask) - Exp_msk1;
        WORD0(&rv)= L | Bndry_mask1;
        WORD1(&rv)= 0xffffffff;
        break;
      }
      if (!(WORD1(&rv) & LSB))
        break;
      if (dsign)
        dval(&rv)+= ulp(&rv);
      else
      {
        dval(&rv)-= ulp(&rv);
        if (!dval(&rv))
          goto undfl;
      }
      dsign= 1 - dsign;
      break;
    }
    if ((aadj= ratio(delta, bs)) <= 2.)
    {
      if (dsign)
        aadj= aadj1= 1.;
      else if (WORD1(&rv) || WORD0(&rv) & Bndry_mask)
      {
        if (WORD1(&rv) == Tiny1 && !WORD0(&rv))
          goto undfl;
        aadj= 1.;
        aadj1= -1.;
      }
      else
      {
        if (aadj < 2. / FLT_RADIX)
          aadj= 1. / FLT_RADIX;
        else
          aadj*= 0.5;
        aadj1= -aadj;
      }
    }
    else
    {
      aadj*= 0.5;
      aadj1= dsign ? aadj : -aadj;
#ifdef Check_FLT_ROUNDS
      switch (Rounding)
      {
      case 2:
        aadj1-= 0.5;
        break;
      case 0:
      case 3:
        aadj1+= 0.5;
      }
#else
      if (Flt_Rounds == 0)
        aadj1+= 0.5;
#endif
    }
    y= WORD0(&rv) & Exp_mask;

    if (y == Exp_msk1 * (DBL_MAX_EXP + Bias - 1))
    {
      dval(&rv0)= dval(&rv);
      WORD0(&rv)-= P * Exp_msk1;
      adj.d= aadj1 * ulp(&rv);
      dval(&rv)+= adj.d;
      if ((WORD0(&rv) & Exp_mask) >= Exp_msk1 * (DBL_MAX_EXP + Bias - P))
      {
        if (WORD0(&rv0) == Big0 && WORD1(&rv0) == Big1)
          goto ovfl;
        WORD0(&rv)= Big0;
        WORD1(&rv)= Big1;
        goto cont;
      }
      else
        WORD0(&rv)+= P * Exp_msk1;
    }
    else
    {
      if (scale && y <= 2 * P * Exp_msk1)
      {
        if (aadj <= 0x7fffffff)
        {
          if ((z= (ULong) aadj) <= 0)
            z= 1;
          aadj= z;
          aadj1= dsign ? aadj : -aadj;
        }
        dval(&aadj2) = aadj1;
        WORD0(&aadj2)+= (2 * P + 1) * Exp_msk1 - y;
        aadj1= dval(&aadj2);
        adj.d= aadj1 * ulp(&rv);
        dval(&rv)+= adj.d;
        if (rv.d == 0.)
          goto undfl;
      }
      else
      {
        adj.d= aadj1 * ulp(&rv);
        dval(&rv)+= adj.d;
      }
    }
    z= WORD0(&rv) & Exp_mask;
#ifndef SET_INEXACT
    if (!scale)
      if (y == z)
      {
        L= (Long)aadj;
        aadj-= L;
        if (dsign || WORD1(&rv) || WORD0(&rv) & Bndry_mask)
        {
          if (aadj < .4999999 || aadj > .5000001)
            break;
        }
        else if (aadj < .4999999 / FLT_RADIX)
          break;
      }
#endif
 cont:
    free_bigint(bb, &alloc);
    free_bigint(bd, &alloc);
    free_bigint(bs, &alloc);
    free_bigint(delta, &alloc);
  }
#ifdef SET_INEXACT
  if (inexact)
  {
    if (!oldinexact)
    {
      WORD0(&rv0)= Exp_1 + (70 << Exp_shift);
      WORD1(&rv0)= 0;
      dval(&rv0)+= 1.;
    }
  }
  else if (!oldinexact)
    clear_inexact();
#endif
  if (scale)
  {
    WORD0(&rv0)= Exp_1 - 2 * P * Exp_msk1;
    WORD1(&rv0)= 0;
    dval(&rv)*= dval(&rv0);
  }
#ifdef SET_INEXACT
  if (inexact && !(WORD0(&rv) & Exp_mask))
  {
    dval(&rv0)= 1e-300;
    dval(&rv0)*= dval(&rv0);
  }
#endif
 retfree:
  free_bigint(bb, &alloc);
  free_bigint(bd, &alloc);
  free_bigint(bs, &alloc);
  free_bigint(bd0, &alloc);
  free_bigint(delta, &alloc);
 ret:
  *se= (char *)s;
  return sign ? -dval(&rv) : dval(&rv);
}


static int quorem(Bigint *b, Bigint *S)
{
  int n;
  ULong *bx, *bxe, q, *sx, *sxe;
  ULLong borrow, carry, y, ys;

  n= S->wds;
  if (b->wds < n)
    return 0;
  sx= S->p.x;
  sxe= sx + --n;
  bx= b->p.x;
  bxe= bx + n;
  q= *bxe / (*sxe + 1); 
  if (q)
  {
    borrow= 0;
    carry= 0;
    do
    {
      ys= *sx++ * (ULLong)q + carry;
      carry= ys >> 32;
      y= *bx - (ys & FFFFFFFF) - borrow;
      borrow= y >> 32 & (ULong)1;
      *bx++= (ULong) (y & FFFFFFFF);
    }
    while (sx <= sxe);
    if (!*bxe)
    {
      bx= b->p.x;
      while (--bxe > bx && !*bxe)
        --n;
      b->wds= n;
    }
  }
  if (bigint_cmp(b, S) >= 0)
  {
    q++;
    borrow= 0;
    carry= 0;
    bx= b->p.x;
    sx= S->p.x;
    do
    {
      ys= *sx++ + carry;
      carry= ys >> 32;
      y= *bx - (ys & FFFFFFFF) - borrow;
      borrow= y >> 32 & (ULong)1;
      *bx++= (ULong) (y & FFFFFFFF);
    }
    while (sx <= sxe);
    bx= b->p.x;
    bxe= bx + n;
    if (!*bxe)
    {
      while (--bxe > bx && !*bxe)
        --n;
      b->wds= n;
    }
  }
  return q;
}

static char *dtoa(double dd, int mode, int ndigits, int *decpt, int *sign,
                  char **rve, char *buf, size_t buf_size)
{
  int bbits, b2, b5, be, dig, i, ieps, ilim = 0, ilim0,
    ilim1 = 0, j, j1, k, k0, k_check, leftright, m2, m5, s2, s5,
    spec_case, try_quick;
  Long L;
  int denorm;
  ULong x;
  Bigint *b, *b1, *delta, *mlo, *mhi, *S;
  U d2, eps, u;
  double ds;
  char *s, *s0;
#ifdef Honor_FLT_ROUNDS
  int rounding;
#endif
  ObStackAllocator alloc;

  alloc.begin= alloc.free= buf;
  alloc.end= buf + buf_size;
  memset(alloc.freelist, 0, sizeof(alloc.freelist));

  u.d= dd;
  if (WORD0(&u) & Sign_bit)
  {
    *sign= 1;
    WORD0(&u) &= ~Sign_bit; 
  }
  else
    *sign= 0;


  if (((WORD0(&u) & Exp_mask) == Exp_mask && (*decpt= DTOA_OVERFLOW)) ||
      (!dval(&u) && (*decpt= 1)))
  {
    char *res= (char*) dtoa_alloc(2, &alloc);
    res[0]= '0';
    res[1]= '\0';
    if (rve)
      *rve= res + 1;
    return res;
  }

#ifdef Honor_FLT_ROUNDS
  if ((rounding= Flt_Rounds) >= 2)
  {
    if (*sign)
      rounding= rounding == 2 ? 0 : 2;
    else
      if (rounding != 2)
        rounding= 0;
  }
#endif

  b= d2b(&u, &be, &bbits, &alloc);
  if ((i= (int)(WORD0(&u) >> Exp_shift1 & (Exp_mask>>Exp_shift1))))
  {
    dval(&d2)= dval(&u);
    WORD0(&d2) &= Frac_mask1;
    WORD0(&d2) |= Exp_11;


    i-= Bias;
    denorm= 0;
  }
  else
  {

    i= bbits + be + (Bias + (P-1) - 1);
    x= i > 32  ? WORD0(&u) << (64 - i) | WORD1(&u) >> (i - 32)
      : WORD1(&u) << (32 - i);
    dval(&d2)= x;
    WORD0(&d2)-= 31*Exp_msk1; 
    i-= (Bias + (P-1) - 1) + 1;
    denorm= 1;
  }
  ds= (dval(&d2)-1.5)*0.289529654602168 + 0.1760912590558 + i*0.301029995663981;
  k= (int)ds;
  if (ds < 0. && ds != k)
    k--;    
  k_check= 1;
  if (k >= 0 && k <= Ten_pmax)
  {
    if (dval(&u) < tens[k])
      k--;
    k_check= 0;
  }
  j= bbits - i - 1;
  if (j >= 0)
  {
    b2= 0;
    s2= j;
  }
  else
  {
    b2= -j;
    s2= 0;
  }
  if (k >= 0)
  {
    b5= 0;
    s5= k;
    s2+= k;
  }
  else
  {
    b2-= k;
    b5= -k;
    s5= 0;
  }
  if (mode < 0 || mode > 9)
    mode= 0;

#ifdef Check_FLT_ROUNDS
  try_quick= Rounding == 1;
#else
  try_quick= 1;
#endif

  if (mode > 5)
  {
    mode-= 4;
    try_quick= 0;
  }
  leftright= 1;
  switch (mode) {
  case 0:
  case 1:
    ilim= ilim1= -1;
    i= 18;
    ndigits= 0;
    break;
  case 2:
    leftright= 0;
  case 4:
    if (ndigits <= 0)
      ndigits= 1;
    ilim= ilim1= i= ndigits;
    break;
  case 3:
    leftright= 0;
  case 5:
    i= ndigits + k + 1;
    ilim= i;
    ilim1= i - 1;
    if (i <= 0)
      i= 1;
  }
  s= s0= dtoa_alloc(i, &alloc);

#ifdef Honor_FLT_ROUNDS
  if (mode > 1 && rounding != 1)
    leftright= 0;
#endif

  if (ilim >= 0 && ilim <= Quick_max && try_quick)
  {
    i= 0;
    dval(&d2)= dval(&u);
    k0= k;
    ilim0= ilim;
    ieps= 2; 
    if (k > 0)
    {
      ds= tens[k&0xf];
      j= k >> 4;
      if (j & Bletch)
      {
        j&= Bletch - 1;
        dval(&u)/= bigtens[n_bigtens-1];
        ieps++;
      }
      for (; j; j>>= 1, i++)
      {
        if (j & 1)
        {
          ieps++;
          ds*= bigtens[i];
        }
      }
      dval(&u)/= ds;
    }
    else if ((j1= -k))
    {
      dval(&u)*= tens[j1 & 0xf];
      for (j= j1 >> 4; j; j>>= 1, i++)
      {
        if (j & 1)
        {
          ieps++;
          dval(&u)*= bigtens[i];
        }
      }
    }
    if (k_check && dval(&u) < 1. && ilim > 0)
    {
      if (ilim1 <= 0)
        goto fast_failed;
      ilim= ilim1;
      k--;
      dval(&u)*= 10.;
      ieps++;
    }
    dval(&eps)= ieps*dval(&u) + 7.;
    WORD0(&eps)-= (P-1)*Exp_msk1;
    if (ilim == 0)
    {
      S= mhi= 0;
      dval(&u)-= 5.;
      if (dval(&u) > dval(&eps))
        goto one_digit;
      if (dval(&u) < -dval(&eps))
        goto no_digits;
      goto fast_failed;
    }
    if (leftright)
    {
      dval(&eps)= 0.5/tens[ilim-1] - dval(&eps);
      for (i= 0;;)
      {
        L= (Long) dval(&u);
        dval(&u)-= L;
        *s++= '0' + (int)L;
        if (dval(&u) < dval(&eps))
          goto ret1;
        if (1. - dval(&u) < dval(&eps))
          goto bump_up;
        if (++i >= ilim)
          break;
        dval(&eps)*= 10.;
        dval(&u)*= 10.;
      }
    }
    else
    {
      dval(&eps)*= tens[ilim-1];
      for (i= 1;; i++, dval(&u)*= 10.)
      {
        L= (Long)(dval(&u));
        if (!(dval(&u)-= L))
          ilim= i;
        *s++= '0' + (int)L;
        if (i == ilim)
        {
          if (dval(&u) > 0.5 + dval(&eps))
            goto bump_up;
          else if (dval(&u) < 0.5 - dval(&eps))
          {
            while (*--s == '0');
            s++;
            goto ret1;
          }
          break;
        }
      }
    }
  fast_failed:
    s= s0;
    dval(&u)= dval(&d2);
    k= k0;
    ilim= ilim0;
  }


  if (be >= 0 && k <= Int_max)
  {
    ds= tens[k];
    if (ndigits < 0 && ilim <= 0)
    {
      S= mhi= 0;
      if (ilim < 0 || dval(&u) <= 5*ds)
        goto no_digits;
      goto one_digit;
    }
    for (i= 1;; i++, dval(&u)*= 10.)
    {
      L= (Long)(dval(&u) / ds);
      dval(&u)-= L*ds;
#ifdef Check_FLT_ROUNDS
      if (dval(&u) < 0)
      {
        L--;
        dval(&u)+= ds;
      }
#endif
      *s++= '0' + (int)L;
      if (!dval(&u))
      {
        break;
      }
      if (i == ilim)
      {
#ifdef Honor_FLT_ROUNDS
        if (mode > 1)
        {
          switch (rounding) {
          case 0: goto ret1;
          case 2: goto bump_up;
          }
        }
#endif
        dval(&u)+= dval(&u);
        if (dval(&u) > ds || (dval(&u) == ds && (L & 1)))
        {
bump_up:
          while (*--s == '9')
            if (s == s0)
            {
              k++;
              *s= '0';
              break;
            }
          ++*s++;
        }
        break;
      }
    }
    goto ret1;
  }

  m2= b2;
  m5= b5;
  mhi= mlo= 0;
  if (leftright)
  {
    i = denorm ? be + (Bias + (P-1) - 1 + 1) : 1 + P - bbits;
    b2+= i;
    s2+= i;
    mhi= integer2bigint(1, &alloc);
  }
  if (m2 > 0 && s2 > 0)
  {
    i= m2 < s2 ? m2 : s2;
    b2-= i;
    m2-= i;
    s2-= i;
  }
  if (b5 > 0)
  {
    if (leftright)
    {
      if (m5 > 0)
      {
        mhi= pow5mult(mhi, m5, &alloc);
        b1= bigint_mul_bigint(mhi, b, &alloc);
        free_bigint(b, &alloc);
        b= b1;
      }
      if ((j= b5 - m5))
        b= pow5mult(b, j, &alloc);
    }
    else
      b= pow5mult(b, b5, &alloc);
  }
  S= integer2bigint(1, &alloc);
  if (s5 > 0)
    S= pow5mult(S, s5, &alloc);


  spec_case= 0;
  if ((mode < 2 || leftright)
#ifdef Honor_FLT_ROUNDS
      && rounding == 1
#endif
     )
  {
    if (!WORD1(&u) && !(WORD0(&u) & Bndry_mask) &&
        (WORD0(&u) & (Exp_mask & (~Exp_msk1)))
       )
    {
      b2+= Log2P;
      s2+= Log2P;
      spec_case= 1;
    }
  }

  if ((i= ((s5 ? 32 - hi0bits(S->p.x[S->wds-1]) : 1) + s2) & 0x1f))
    i= 32 - i;
  if (i > 4)
  {
    i-= 4;
    b2+= i;
    m2+= i;
    s2+= i;
  }
  else if (i < 4)
  {
    i+= 28;
    b2+= i;
    m2+= i;
    s2+= i;
  }
  if (b2 > 0)
    b= left_shift(b, b2, &alloc);
  if (s2 > 0)
    S= left_shift(S, s2, &alloc);
  if (k_check)
  {
    if (bigint_cmp(b,S) < 0)
    {
      k--;
      b= mult_and_add(b, 10, 0, &alloc);
      if (leftright)
        mhi= mult_and_add(mhi, 10, 0, &alloc);
      ilim= ilim1;
    }
  }
  if (ilim <= 0 && (mode == 3 || mode == 5))
  {
    if (ilim < 0 || bigint_cmp(b,S= mult_and_add(S,5,0, &alloc)) <= 0)
    {
no_digits:
      k= -1 - ndigits;
      goto ret;
    }
one_digit:
    *s++= '1';
    k++;
    goto ret;
  }
  if (leftright)
  {
    if (m2 > 0)
      mhi= left_shift(mhi, m2, &alloc);

    mlo= mhi;
    if (spec_case)
    {
      mhi= alloc_bigint(mhi->k, &alloc);
      COPY_BIGINT(mhi, mlo);
      mhi= left_shift(mhi, Log2P, &alloc);
    }

    for (i= 1;;i++)
    {
      dig= quorem(b,S) + '0';
      j= bigint_cmp(b, mlo);
      delta= bigint_diff(S, mhi, &alloc);
      j1= delta->sign ? 1 : bigint_cmp(b, delta);
      free_bigint(delta, &alloc);
      if (j1 == 0 && mode != 1 && !(WORD1(&u) & 1)
#ifdef Honor_FLT_ROUNDS
          && rounding >= 1
#endif
         )
      {
        if (dig == '9')
          goto round_9_up;
        if (j > 0)
          dig++;
        *s++= dig;
        goto ret;
      }
      if (j < 0 || (j == 0 && mode != 1 && !(WORD1(&u) & 1)))
      {
        if (!b->p.x[0] && b->wds <= 1)
        {
          goto accept_dig;
        }
#ifdef Honor_FLT_ROUNDS
        if (mode > 1)
          switch (rounding) {
          case 0: goto accept_dig;
          case 2: goto keep_dig;
          }
#endif 
        if (j1 > 0)
        {
          b= left_shift(b, 1, &alloc);
          j1= bigint_cmp(b, S);
          if ((j1 > 0 || (j1 == 0 && (dig & 1)))
              && dig++ == '9')
            goto round_9_up;
        }
accept_dig:
        *s++= dig;
        goto ret;
      }
      if (j1 > 0)
      {
#ifdef Honor_FLT_ROUNDS
        if (!rounding)
          goto accept_dig;
#endif
        if (dig == '9')
        { 
round_9_up:
          *s++= '9';
          goto roundoff;
        }
        *s++= dig + 1;
        goto ret;
      }
#ifdef Honor_FLT_ROUNDS
keep_dig:
#endif
      *s++= dig;
      if (i == ilim)
        break;
      b= mult_and_add(b, 10, 0, &alloc);
      if (mlo == mhi)
        mlo= mhi= mult_and_add(mhi, 10, 0, &alloc);
      else
      {
        mlo= mult_and_add(mlo, 10, 0, &alloc);
        mhi= mult_and_add(mhi, 10, 0, &alloc);
      }
    }
  }
  else
    for (i= 1;; i++)
    {
      *s++= dig= quorem(b,S) + '0';
      if (!b->p.x[0] && b->wds <= 1)
      {
        goto ret;
      }
      if (i >= ilim)
        break;
      b= mult_and_add(b, 10, 0, &alloc);
    }


#ifdef Honor_FLT_ROUNDS
  switch (rounding) {
  case 0: goto trimzeros;
  case 2: goto roundoff;
  }
#endif
  b= left_shift(b, 1, &alloc);
  j= bigint_cmp(b, S);
  if (j > 0 || (j == 0 && (dig & 1)))
  {
roundoff:
    while (*--s == '9')
      if (s == s0)
      {
        k++;
        *s++= '1';
        goto ret;
      }
    ++*s++;
  }
  else
  {
#ifdef Honor_FLT_ROUNDS
trimzeros:
#endif
    while (*--s == '0');
    s++;
  }
ret:
  free_bigint(S, &alloc);
  if (mhi)
  {
    if (mlo && mlo != mhi)
      free_bigint(mlo, &alloc);
    free_bigint(mhi, &alloc);
  }
ret1:
  free_bigint(b, &alloc);
  *s= 0;
  *decpt= k + 1;
  if (rve)
    *rve= s;
  return s0;
}

#undef P

#endif