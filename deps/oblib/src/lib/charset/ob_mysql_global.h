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

#ifndef OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_
#define OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_

#define MY_GLOBAL_INCLUDED



#if !defined(__WIN__) && defined(_WIN32)
#define __WIN__
#endif

#define INNODB_COMPATIBILITY_HOOKS

#ifdef __CYGWIN__

#undef WIN
#undef WIN32
#undef _WIN
#undef _WIN32
#undef _WIN64
#undef __WIN__
#undef __WIN32__
#define HAS_ERRNO_AS_DEFINE
#endif 

#if defined(i386) && !defined(__i386__)
#define __i386__
#endif


#ifdef __cplusplus
#define C_MODE_START    extern "C" {
#define C_MODE_END	}
#else
#define C_MODE_START
#define C_MODE_END
#endif

#ifdef __cplusplus
#define CPP_UNNAMED_NS_START  namespace {
#define CPP_UNNAMED_NS_END    }
#endif


#include "lib/charset/ob_config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory.h>
#include <stddef.h>
#include <stdarg.h>
#include <pthread.h>		

#include <math.h>
#include <limits.h>
#include <float.h>
#include <fenv.h> 

#include <sys/types.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <alloca.h>

#include <errno.h>				
#include <crypt.h>

#include <assert.h>



#if defined(_lint) || defined(FORCE_INIT_OF_VARS) || \
    (defined(__GNUC__) && defined(__cplusplus))
#define LINT_INIT(var) var= 0
#else
#define LINT_INIT(var)
#endif


#if defined(_lint) || defined(FORCE_INIT_OF_VARS) || \
    defined(__cplusplus) || !defined(__GNUC__)
#define UNINIT_VAR(x) x= 0
#else

#define UNINIT_VAR(x) x= x
#endif


#define set_if_smaller(a,b) do { if ((a) > (b)) (a)=(b); } while(0)



#ifndef TRUE
#define TRUE		(1)	
#define FALSE		(0)	
#endif





typedef int	File;		
typedef int	my_socket;	
#define INVALID_SOCKET -1

#define sig_handler RETSIGTYPE

#define OB_MAX(a, b)    ((a) > (b) ? (a) : (b))
#define OB_MIN(a, b)    ((a) < (b) ? (a) : (b))


#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))


#define NullS		(char *) 0

#ifdef STDCALL
#undef STDCALL
#endif

#ifdef _WIN32
#define STDCALL __stdcall
#else
#define STDCALL
#endif


#ifndef HAS_UCHAR
typedef unsigned char uchar;	
#endif
		
#ifndef HAS_UINT8		
typedef unsigned char uint8;
#endif		
#ifndef HAS_INT16		
typedef short int16;		
#endif		
#ifndef HAS_UINT16		
typedef unsigned short uint16;		
#endif		
#ifndef HAS_INT32		
typedef int int32;		
#endif		
#ifndef HAS_UINT32		
typedef unsigned int uint32;		
#endif


#ifndef longlong_defined

#if SIZEOF_LONG_LONG != 8 || !defined(HAS_LONG_LONG)
typedef unsigned long ulonglong;	  
typedef long longlong;
#else
typedef unsigned long long int ulonglong; 
typedef long long int	longlong;
#endif

#if !defined(__USE_MISC) && !defined(HAS_ULONG)
typedef unsigned long ulong;		  
#endif

#endif
#ifndef HAS_INT64
typedef longlong int64;
#endif
#ifndef HAS_UINT64
typedef ulonglong uint64;
#endif


#if SIZEOF_CHARP == SIZEOF_INT
typedef int int_ptr;
#elif SIZEOF_CHARP == SIZEOF_LONG
typedef long int_ptr;
#elif  SIZEOF_CHARP == SIZEOF_LONG_LONG
typedef long long int_ptr;
#else
#error sizeof(void *) is neither sizeof(int) nor sizeof(long) nor sizeof(long long)
#endif

#if defined(NO_CLIENT_LONG_LONG)		
typedef unsigned long my_ulonglong;		
#elif !defined (__WIN__)		
typedef unsigned long long my_ulonglong;		
#else		
typedef unsigned __int64 my_ulonglong;		
#endif

#if !defined(_WIN32)
typedef off_t os_off_t;
#if SIZEOF_OFF_T <= 4
typedef unsigned long ob_off_t;
#else
typedef ulonglong ob_off_t;
#endif 

#if defined(_WIN32)
typedef unsigned long long ob_off_t;
typedef unsigned long long os_off_t;
#endif
#endif 
#define MY_FILEPOS_ERROR	(~(ob_off_t) 0)

typedef ulonglong table_map;          
typedef ulonglong nesting_map;  

typedef int		myf;	
typedef char		my_bool; 



#define MYF(v)		(myf) (v)

#ifndef LL
#ifdef HAS_LONG_LONG
#define LL(A) A ## LL
#else
#define LL(A) A ## L
#endif
#endif

#ifndef ULL
#ifdef HAS_LONG_LONG
#define ULL(A) A ## ULL
#else
#define ULL(A) A ## UL
#endif
#endif


#if !defined(HAS_UINT)
#undef HAS_UINT
#define HAS_UINT
typedef unsigned int uint;
typedef unsigned short ushort;
#endif


#if defined(__GNUC__) && !defined(_lint)
typedef char	pchar;		
typedef char	puchar;		
typedef char	pbool;		
typedef short	pshort;		
typedef float	pfloat;		
#else
typedef int	pchar;		
typedef uint	puchar;		
typedef int	pbool;		
typedef int	pshort;		
typedef double	pfloat;		
#endif
C_MODE_START
typedef int	(*qsort_cmp)(const void *,const void *);
typedef int	(*qsort_cmp2)(const void*, const void *,const void *);
C_MODE_END
#define qsort_t RETQSORTTYPE	


#if defined(HAS_LONG_LONG) && !defined(LONGLONG_MIN)
#define LONGLONG_MIN	((long long) 0x8000000000000000LL)
#define LONGLONG_MAX	((long long) 0x7FFFFFFFFFFFFFFFLL)
#endif

#if defined(HAS_LONG_LONG) && !defined(ULONGLONG_MAX)

#ifdef ULLONG_MAX
#define ULONGLONG_MAX  ULLONG_MAX
#else
#define ULONGLONG_MAX ((unsigned long long)(~0ULL))
#endif
#endif 

#define INT_MAX8        0x7F
#define INT_MIN8        (~0x7F)
#define UINT_MAX8       0xFF
#define INT_MAX16       0x7FFF
#define INT_MIN16       (~0x7FFF)
#define UINT_MAX16      0xFFFF
#define INT_MAX24       0x007FFFFF
#define INT_MIN24       (~0x007FFFFF)
#define UINT_MAX24      0x00FFFFFF
#define INT_MAX32       0x7FFFFFFFL
#define INT_MIN32       (~0x7FFFFFFFL)
#define UINT_MAX32      0xFFFFFFFFL
#define INT_MAX64       0x7FFFFFFFFFFFFFFFLL
#define INT_MIN64       (~0x7FFFFFFFFFFFFFFFLL)

#ifndef DBL_MAX
#define DBL_MAX		1.79769313486231470e+308
#define FLT_MAX		((float)3.40282346638528860e+38)
#endif
#ifndef DBL_MIN
#define DBL_MIN		4.94065645841246544e-324
#define FLT_MIN		((float)1.40129846432481707e-45)
#endif

#define int1store(T,A)  do { *((uint8_t *)(T)) = (uint8_t)(A); } while (0)
#define int2store(T,A)	do { unsigned char *pT= (unsigned char*)(T);\
                             *((unsigned short*)(pT))= (unsigned short) (A);\
                        } while (0)

#define int3store(T,A)  do { *(T)=  (unsigned char) ((A));\
                            *(T+1)=(unsigned char) (((uint) (A) >> 8));\
                            *(T+2)=(unsigned char) (((A) >> 16));\
                        } while (0)
#define int4store(T,A)	do { unsigned char *pT= (unsigned char*)(T);\
                             *((unsigned int *) (pT))= (unsigned int) (A); \
                        } while (0)

#define int5store(T,A)  do { *(T)= (unsigned char)((A));\
                             *((T)+1)=(unsigned char) (((A) >> 8));\
                             *((T)+2)=(unsigned char) (((A) >> 16));\
                             *((T)+3)=(unsigned char) (((A) >> 24));\
                             *((T)+4)=(unsigned char) (((A) >> 32));\
                        } while(0)
#define int6store(T,A)  do { *(T)=    (unsigned char)((A));          \
                             *((T)+1)=(unsigned char) (((A) >> 8));  \
                             *((T)+2)=(unsigned char) (((A) >> 16)); \
                             *((T)+3)=(unsigned char) (((A) >> 24)); \
                             *((T)+4)=(unsigned char) (((A) >> 32)); \
                             *((T)+5)=(unsigned char) (((A) >> 40)); \
                        } while(0)
#define int8store(T,A)	do { unsigned char *pT= (unsigned char*)(T);\
                             *((uint64 *) (pT))= (uint64) (A);\
                        } while(0)

#define uint1korr(A)    (*((uint8_t *)A))
#define uint2korr(A)	(unsigned short) (*((unsigned short *) (A)))
#define uint3korr(A)	(unsigned int) (((unsigned int) ((unsigned char) (A)[0])) +\
				  (((unsigned int) ((unsigned char) (A)[1])) << 8) +\
				  (((unsigned int) ((unsigned char) (A)[2])) << 16))
#define uint4korr(A)	(unsigned int) (*((unsigned int *) (A)))
#define uint5korr(A)	((uint64)(((unsigned int) ((unsigned char) (A)[0])) +\
				    (((unsigned int) ((unsigned char) (A)[1])) << 8) +\
				    (((unsigned int) ((unsigned char) (A)[2])) << 16) +\
				    (((unsigned int) ((unsigned char) (A)[3])) << 24)) +\
				    (((uint64) ((unsigned char) (A)[4])) << 32))
#define uint6korr(A)	((uint64)(((unsigned int)    ((unsigned char) (A)[0]))          + \
                                     (((unsigned int)    ((unsigned char) (A)[1])) << 8)   + \
                                     (((unsigned int)    ((unsigned char) (A)[2])) << 16)  + \
                                     (((unsigned int)    ((unsigned char) (A)[3])) << 24)) + \
                         (((uint64) ((unsigned char) (A)[4])) << 32) +       \
                         (((uint64) ((unsigned char) (A)[5])) << 40))
#define uint8korr(A)	(uint64) (*((uint64 *) (A)))


#define sint1korr(A)    (*((int8_t*)(A)))
#define sint2korr(A)	(short) (*((short *) (A)))
#define sint3korr(A)	((int) ((((unsigned char) (A)[2]) & 128) ? \
				  (((unsigned int) 255L << 24) | \
				   (((unsigned int) (unsigned char) (A)[2]) << 16) |\
				   (((unsigned int) (unsigned char) (A)[1]) << 8) | \
				   ((unsigned int) (unsigned char) (A)[0])) : \
				  (((unsigned int) (unsigned char) (A)[2]) << 16) |\
				  (((unsigned int) (unsigned char) (A)[1]) << 8) | \
				  ((unsigned int) (unsigned char) (A)[0])))
#define sint4korr(A)	(int)  (*((int *) (A)))
#define sint8korr(A)	(int64) (*((int64 *) (A)))



#ifndef MYSQL_PLUGIN_IMPORT
#if (defined(_WIN32) && defined(MYSQL_DYNAMIC_PLUGIN))
#define MYSQL_PLUGIN_IMPORT __declspec(dllimport)
#else
#define MYSQL_PLUGIN_IMPORT
#endif
#endif

enum loglevel {
   ERROR_LEVEL=       0,
   WARNING_LEVEL=     1,
   INFORMATION_LEVEL= 2
};


#endif /* OCEANBASE_LIB_OBMYSQL_OB_MYSQL_GLOBAL_ */


