#if defined(__sw_64__)
.text
.align  2
.global ontop_fcontext
.type   ontop_fcontext, %function
ontop_fcontext:
    # prepare stack for GP + FPU
    #ldih $29,0($27)
    #ldi $29,0($29)
    subl  $sp, 0x98, $sp

    # save $f2-$f9
    fstd  $f2, 0x00($sp)
    fstd  $f3, 0x08($sp)
    fstd  $f4, 0x10($sp)
    fstd  $f5, 0x18($sp)
    fstd  $f6, 0x20($sp)
    fstd  $f7, 0x28($sp)
    fstd  $f8, 0x30($sp)
    fstd  $f9, 0x38($sp)

    # save $9-$15, fp,$26
    stl  $9, 0x40($sp)
    stl  $10, 0x48($sp)
    stl  $11, 0x50($sp)
    stl  $12, 0x58($sp)
    stl  $13, 0x60($sp)
    stl  $14, 0x68($sp)
    stl  $15, 0x70($sp)
    stl  $fp, 0x78($sp)
    stl  $16, 0x80($sp)  #save ontop_fcontext return address
    stl  $26, 0x88($sp)

    # save LR as PC
    stl  $26, 0x90($sp)

    # store RSP (pointing to context-data) in $16
    mov  $sp, $20


    # restore RSP (pointing to context-data) from $17
    mov  $17, $sp

    # load $f2-$f9
    fldd $f2, 0x00($sp)
    fldd $f3, 0x08($sp)
    fldd $f4, 0x10($sp)
    fldd $f5, 0x18($sp)
    fldd $f6, 0x20($sp)
    fldd $f7, 0x28($sp)
    fldd $f8, 0x30($sp)
    fldd $f9, 0x38($sp)

    # load $9-$15, fp,$26
    ldl  $9, 0x40($sp)
    ldl  $10, 0x48($sp)
    ldl  $11, 0x50($sp)
    ldl  $12, 0x58($sp)
    ldl  $13, 0x60($sp)
    ldl  $14, 0x68($sp)
    ldl  $15, 0x70($sp)
    ldl  $fp, 0x78($sp)
    ldl  $26, 0x88($sp)

    # pass transfer_t as first arg in context function
    # to store $1,$2 to $16 address
    ldl $16, 0x80($sp) #load $16, store return struct do return address
    stl $20,0($16)
    stl $18,8($16)

    # pass transfer_t as first arg in context function,such as f1,f2,f3
    # $16 == FCTX, $17 == DATA
    mov $20,$16   #$16 $17 $18 as first and second arg
    mov $18,$17


    # skip pc
    mov $19, $27


    # restore stack from GP + FPU
    addl  $sp, 0x98, $sp

    ret  $31,($27),0x1 //jmp $31, ($27) //ret ($27)
.size   ontop_fcontext,.-ontop_fcontext
# Mark that we don't need executable stack.
.section .note.GNU-stack,"",%progbits
#endif