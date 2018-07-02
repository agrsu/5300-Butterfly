m1b: Milestone1_Butterfly.o
	g++ -L/usr/local/berkeleydb/lib -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

Milestone1_Butterfly.o : Milestone1_Butterfly.cpp 
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<