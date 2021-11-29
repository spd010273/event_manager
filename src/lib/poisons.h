/*------------------------------------------------------------------------
 *
 * poisons.h
 *     Function / Variable strictures and poisoning
 *
 * Copyright (c) 2021, Nead Werx, Inc.
 * Copyright (c) 2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        src/lib/poisons.h
 *
 *------------------------------------------------------------------------
 */

#ifndef POISONS_H
#define POISONS_H

#ifdef __GNUC__
#pragma GCC poison strcpy strcat
#pragma GCC poison longjmp siglongjmp
#pragma GCC poison setjmp sigsetjmp
#pragma GCC poison getwd mktemp tmpnam tempnam
#pragma GCC poison rexec rexec_af
#pragma GCC poison getlogin cuserid getpass
#endif // __GNUC__

#endif // POISONS_H
