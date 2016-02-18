/*
 * =====================================================================================
 *
 *       Filename:  unittest.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/11/2013 04:27:03 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include "ub_log.h"

int main(int argc, char *argv[]) 
{
    ub_log_init("./log/", "unittest.", 0x10000000, 16);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

