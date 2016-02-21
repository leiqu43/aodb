/*
 * =====================================================================================
 *
 *       Filename:  db_imp.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  06/13/2012 11:45:53 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  qulei (), leiqu43@gmail.com
 *        Company:  
 *
 *
 * =====================================================================================
 */

#include "mdb.pb.h"
#include "aodb_data.h"

class Message;
MdbInterface::GenericResErr aodb_put(MdbInterface::GenericReq *req, \
                                    google::protobuf::Message **resMsg, thread_data_t *pthr);
MdbInterface::GenericResErr aodb_get(MdbInterface::GenericReq *req, \
                                    google::protobuf::Message **resMsg, thread_data_t *pthr);

