RSORTEXE = rsort

build:
ifeq ($(OS), Darwin)
	gcc main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
else
	gcc -pthread main.c lib/mergesort.c lib/winner_tree.c -o $(RSORTEXE)
endif

test:
	./$(RSORTEXE) --chunk 4 -s 2000000 ../dataset/ettoday.rec

clean:
ifeq ($(RSORTEXE), $(wildcard $(RSORTEXE)))
	rm $(RSORTEXE)
ifeq ($(RSORTEXE).dSYM, $(wildcard $(RSORTEXE).dSYM))
	rm -rf $(RSORTEXE).dSYM
endif
endif