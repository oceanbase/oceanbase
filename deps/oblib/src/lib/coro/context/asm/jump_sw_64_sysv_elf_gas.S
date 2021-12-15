#if defined(__sw_64__)
/*
            Copyright Edward Nevill 2015
   Distributed under the Boost Software License, Version 1.0.
      (See accompanying file LICENSE_1_0.txt or copy at
          http://www.boost.org/LICENSE_1_0.txt)
*/
/*******************************************************
 *                                                     *
 *  -------------------------------------------------  *
 *  |  0  |  1  |  2  |  3  |  4  |  5  |  6  |  7  |  *
 *  -------------------------------------------------  *
 *  | 0x0 | 0x4 | 0x8 | 0xc | 0x10| 0x14| 0x18| 0x1c|  *
 *  -------------------------------------------------  *
 *  |    d8     |    d9     |    d10    |    d11    |  *
 *  -------------------------------------------------  *
 *  -------------------------------------------------  *
 *  |  8  |  9  |  10 |  11 |  12 |  13 |  14 |  15 |  *
 *  -------------------------------------------------  *
 *  | 0x20| 0x24| 0x28| 0x2c| 0x30| 0x34| 0x38| 0x3c|  *
 *  -------------------------------------------------  *
 *  |    d12    |    d13    |    d14    |    d15    |  *
 *  -------------------------------------------------  *
 *  -------------------------------------------------  *
 *  |  16 |  17 |  18 |  19 |  20 |  21 |  22 |  23 |  *
 *  -------------------------------------------------  *
 *  | 0x40| 0x44| 0x48| 0x4c| 0x50| 0x54| 0x58| 0x5c|  *
 *  -------------------------------------------------  *
 *  |    x19    |    x20    |    x21    |    x22    |  *
 *  -------------------------------------------------  *
 *  -------------------------------------------------  *
 *  |  24 |  25 |  26 |  27 |  28 |  29 |  30 |  31 |  *
 *  -------------------------------------------------  *
 *  | 0x60| 0x64| 0x68| 0x6c| 0x70| 0x74| 0x78| 0x7c|  *
 *  -------------------------------------------------  *
 *  |    x23    |    x24    |    x25    |    x26    |  *
 *  -------------------------------------------------  *
 *  -------------------------------------------------  *
 *  |  32 |  33 |  34 |  35 |  36 |  37 |  38 |  39 |  *
 *  -------------------------------------------------  *
 *  | 0x80| 0x84| 0x88| 0x8c| 0x90| 0x94| 0x98| 0x9c|  *
 *  -------------------------------------------------  *
 *  |    x27    |    x28    |    FP     |     LR    |  *
 *  -------------------------------------------------  *
 *  -------------------------------------------------  *
 *  |  40 |  41 |  42 | 43  |           |           |  *
 *  -------------------------------------------------  *
 *  | 0xa0| 0xa4| 0xa8| 0xac|           |           |  *
 *  -------------------------------------------------  *
 *  |     PC    |   align   |           |           |  *
 *  -------------------------------------------------  *
 *                                                     *
 *******************************************************/

#.cpu    generic+fp+simd
.text
.align  2
.global jump_fcontext
.type   jump_fcontext, %function
jump_fcontext:
    # prepare stack for GP + FPU
#    sub  sp, sp, #0xb0
        ldih $29,0($27)         !gpdisp!1
        ldi $29,0($29)          !gpdisp!1
	subl $30, 0x98, $30
# Because gcc may save integer registers in fp registers across a
# function call we cannot skip saving the fp registers.
#
# Do not reinstate this test unless you fully understand what you
# are doing.
#
#    # test if fpu env should be preserved
#    cmp  w3, #0
#    b.eq  1f

    # save d8 - d15
#    stp  d8,  d9,  [sp, #0x00]
#    stp  d10, d11, [sp, #0x10]
#    stp  d12, d13, [sp, #0x20]
#    stp  d14, d15, [sp, #0x30]
	fstd $f2,0x00($30)
	fstd $f3,0x08($30)
	fstd $f3,0x10($30)
	fstd $f4,0x18($30)
	fstd $f5,0x20($30)
	fstd $f6,0x28($30)
	fstd $f7,0x30($30)
	fstd $f8,0x38($30)
	fstd $f9,0x40($30)
1:
    # save x19-x30
#    stp  x19, x20, [sp, #0x40]
#    stp  x21, x22, [sp, #0x50]
#    stp  x23, x24, [sp, #0x60]
#    stp  x25, x26, [sp, #0x70]
#    stp  x27, x28, [sp, #0x80]
#    stp  x29, x30, [sp, #0x90]
	stl $9, 0x48($30)	
	stl $10, 0x50($30)	
	stl $11, 0x58($30)	
	stl $12, 0x60($30)	
	stl $13, 0x68($30)	
	stl $14, 0x70($30)	
	stl $15, 0x90($30)
    # save LR as PC
#   str  x30, [sp, #0xa0]
	stl $26, 0x80($30)
	stl $26, 0x88($30)
    # store RSP (pointing to context-data) in first argument (x0).
    # STR cannot have sp as a target register
#   mov  x4, sp
	mov $30,$1
#    str  x4, [x0]
	stl $1,0x00($16)
    # restore RSP (pointing to context-data) from A2 (x1)
#    mov  sp, x1
	mov $17,$30
#    # test if fpu env should be preserved
#    cmp  w3, #0
#    b.eq  2f

    # load d8 - d15
#    ldp  d8,  d9,  [sp, #0x00]
#    ldp  d10, d11, [sp, #0x10]
#    ldp  d12, d13, [sp, #0x20]
#    ldp  d14, d15, [sp, #0x30]
	fldd $f2,0x00($30)
        fldd $f3,0x08($30)
        fldd $f3,0x10($30)
        fldd $f4,0x18($30)
        fldd $f5,0x20($30)
        fldd $f6,0x28($30)
        fldd $f7,0x30($30)
        fldd $f8,0x38($30)
        fldd $f9,0x40($30)
2:
    # load x19-x30
#    ldp  x19, x20, [sp, #0x40]
#    ldp  x21, x22, [sp, #0x50]
#    ldp  x23, x24, [sp, #0x60]
#    ldp  x25, x26, [sp, #0x70]
#    ldp  x27, x28, [sp, #0x80]
#    ldp  x29, x30, [sp, #0x90]
	ldl $9, 0x48($30)
        ldl $10, 0x50($30)
        ldl $11, 0x58($30)
        ldl $12, 0x60($30)
        ldl $13, 0x68($30)
        ldl $14, 0x70($30)
	ldl $26, 0x80($30)
	ldl $15, 0x90($30)
    # use third arg as return value after jump
    # and as first arg in context function
#    mov  x0, x2
	mov $18,$16
	mov $18,$0
    # load pc
#    ldr  x4, [sp, #0xa0]
	ldl $27,0x88($30)
#	ldl $26,0x88($30)
    # restore stack from GP + FPU
#    add  sp, sp, #0xb0
	#mov  $1,$30
	addl $30, 0x98, $30
#	addl $30,32,$30
#    ret x4
#	mov $1,$26
	ret $31,($27),1
.size   jump_fcontext,.-jump_fcontext
# Mark that we don't need executable stack.
.section .note.GNU-stack,"",%progbits
#endif
