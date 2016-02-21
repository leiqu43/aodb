
GCC=g++

WORKROOT=../../../../

LIBPATH=$(WORKROOT)lib2-64
THIRDPATH=$(WORKROOT)third-64

ULLIB=$(LIBPATH)/ullib/
NSHEAD=$(WORKROOT)/public/nshead/
UBLIB=$(WORKROOT)/public/ub/
PROTOBUF=$(THIRDPATH)/protobuf/
GTEST=../../lib/gtest-1.5.0/
BOOST=$(THIRDPATH)/boost/
SNAPPY=$(THIRDPATH)/snappy/

INCLUDE_PATH=-I$(ULLIB)/include/ \
			 -I$(NSHEAD) \
			 -I$(UBLIB)/include \
			 -I$(GTEST)/include \
			 -I$(PROTOBUF)/include \
			 -I../../search/include\
			 -I$(BOOST)/include\
			 -I$(SNAPPY)/include\
			 -I./ -I../include/

LIB_PATH=-L$(ULLIB)/lib \
		 -L$(UBLIB)/lib \
		 -L$(NSHEAD) \
		 -L$(GTEST)/lib \
		 -L$(LEVELDB)/lib \
		 -L$(PROTOBUF)/lib/ \
		 -L$(SNAPPY)/lib/\
		 -L$(BOOST)/lib/

LIB = -lub -lullib -lnshead -lpthread -lub_misc -lgtest -lprotobuf -lboost_thread -lssl -lsnappy

OBJ=aodb

GCC = g++
CPPFLAGS = -g -Wall -W -Winline -Werror  -Wno-unused-parameter   -Wno-unused-function \
  -DVERSION="\"$(OBJ) 1.0.0.0\"" -DCVSTAG="\"$(OBJ)_1-0-0-0_PD_BL\"" -DPROJECT_NAME=\"$(OBJ)\" 

.PHONY: all clean

all: $(OBJ)
	rm -f *.o tests/*.o

unittest: tests/unittest.o tests/test_posix_env.o tests/test_table.o table.o tests/test_db.o db.o db_mgr.o tests/test_db_mgr.o
	$(GCC) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LIB)
	rm -rf *.o */*.o

test_table: tests/test_table_mem_used.o table.o
	$(GCC) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LIB)
	rm -rf *.o */*.o

test_db: tests/test_db_mem_used.o db.o table.o
	$(GCC) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LIB)
	rm -rf *.o */*.o

$(OBJ) : aodb.o aodb_imp.o db_imp.o ../../search/include/mdb.pb.o db_mgr.o db.o table.o
	$(GCC) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LIB)

clean:
	rm -f *.o */*.o  $(OBJ) unittest

%.o	: %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

