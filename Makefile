RSORTEXE = rsort
UNAME = $(shell uname -s)

build:
ifeq ($(UNAME), Darwin)
	gcc main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
else
	gcc -pthread main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
endif

run:
	./$(RSORTEXE) -chunk 4 -s 100000000 -kr ' ' -n ../tcount/result.rec > result.rec

clean:
ifeq ($(RSORTEXE), $(wildcard $(RSORTEXE)))
	rm $(RSORTEXE)
endif

ifeq ($(RSORTEXE).dSYM, $(wildcard $(RSORTEXE).dSYM))
	rm -rf $(RSORTEXE).dSYM
endif