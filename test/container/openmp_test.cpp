//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/15.
// test/container/openmp_test.cpp
//
//===-----------------------------------------------------

/******************************************************************************
// https://computing.llnl.gov/tutorials/openMP/samples/C/omp_hello.c
* FILE: omp_hello.c
* DESCRIPTION:
*   OpenMP Example - Hello World - C/C++ Version
*   In this simple example, the master thread forks a parallel region.
*   All threads in the team obtain their unique thread number and print it.
*   The master thread only prints the total number of threads.  Two OpenMP
*   library routines are used to obtain the number of threads and each
*   thread's number.
* AUTHOR: Blaise Barney  5/99
* LAST REVISED: 04/06/05
******************************************************************************/
#include <omp.h>
#include <cstdio>
#include <cstdlib>

int main (void)
{
  int nthreads, tid;

/* Fork a team of threads giving them their own copies of variables */
#pragma omp parallel private(nthreads, tid)
  {

    /* Obtain thread number */
    tid = omp_get_thread_num();
    printf("Hello World from thread = %d\n", tid);

    /* Only master thread does this */
    if (tid == 0)
    {
      nthreads = omp_get_num_threads();
      printf("Number of threads = %d\n", nthreads);
    }

  }  /* All threads join master thread and disband */
  return 0;
}