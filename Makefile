RSORTEXE = rsort

build:
ifeq ($(OS), Darwin)
	gcc -pthread main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
else
	gcc main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
endif

test:
	./$(RSORTEXE) -chunk 4 -s 200000000 -d -r ../dataset/sample_100000.rec

clean:
ifeq ($(RSORTEXE), $(wildcard $(RSORTEXE)))
	rm $(RSORTEXE)
ifeq ($(RSORTEXE).dSYM, $(wildcard $(RSORTEXE).dSYM))
	rm -rf $(RSORTEXE).dSYM
endif
endif