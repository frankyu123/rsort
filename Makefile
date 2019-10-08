RSORTEXE = rsort
SEGEXE = seg
SEGDIR = tools/segmentor

build:
ifeq ($(OS), Darwin)
	gcc -pthread main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
else
	gcc main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
endif
	cd $(SEGDIR) && make

test:
	./$(RSORTEXE) -chunk 4 -s 5000000 ../../../tcount/result.rec > result.rec

clean:
ifeq ($(RSORTEXE), $(wildcard $(RSORTEXE)))
	rm $(RSORTEXE)
endif

ifeq ($(RSORTEXE).dSYM, $(wildcard $(RSORTEXE).dSYM))
	rm -rf $(RSORTEXE).dSYM
endif
	cd $(SEGDIR) && make clean
