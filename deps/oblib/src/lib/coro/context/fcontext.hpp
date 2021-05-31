//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_CONTEXT_DETAIL_FCONTEXT_H
#define BOOST_CONTEXT_DETAIL_FCONTEXT_H

// The follow lines are copied from boost/context/detail/config.hpp
// For convenience we only support static linking in X86 platform but
// others can be supported by coping corresponding ASM files.
#if ! defined(BOOST_CONTEXT_DECL)
# define BOOST_CONTEXT_DECL
#endif

#undef BOOST_CONTEXT_CALLDECL
#if (defined(i386) || defined(__i386__) || defined(__i386)              \
     || defined(__i486__) || defined(__i586__) || defined(__i686__)     \
     || defined(__X86__) || defined(_X86_) || defined(__THW_INTEL__)    \
     || defined(__I86__) || defined(__INTEL__) || defined(__IA32__)     \
     || defined(_M_IX86) || defined(_I86_)) && defined(BOOST_WINDOWS)
# define BOOST_CONTEXT_CALLDECL __cdecl
#else
# define BOOST_CONTEXT_CALLDECL
#endif
// Coping from boost/context/detail/config.hpp ends

#include <cstdint>

namespace boost {
namespace context {
namespace detail {

typedef void*   fcontext_t;

struct transfer_t {
  fcontext_t  fctx;
  void    *   data;
};

extern "C" BOOST_CONTEXT_DECL
transfer_t BOOST_CONTEXT_CALLDECL jump_fcontext( fcontext_t const to, void * vp);
extern "C" BOOST_CONTEXT_DECL
fcontext_t BOOST_CONTEXT_CALLDECL make_fcontext( void * sp, std::size_t size, void (* fn)( transfer_t) );

// based on an idea of Giovanni Derreta
extern "C" BOOST_CONTEXT_DECL
transfer_t BOOST_CONTEXT_CALLDECL ontop_fcontext( fcontext_t const to, void * vp, transfer_t (* fn)( transfer_t) );

}}}

#endif // BOOST_CONTEXT_DETAIL_FCONTEXT_H
