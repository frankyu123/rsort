RSORTEXE = rsort
SEGEXE = seg
SEGDIR = tools/segmentor
UNAME = $(shell uname -s)

build:
ifeq ($(UNAME), Darwin)
	gcc -Iinclude src/rsort.c src/mergesort.c src/winner_tree.c -o $(RSORTEXE)
else
	gcc -Iinclude -pthread src/rsort.c src/mergesort.c src/winner_tree.c -o $(RSORTEXE)
endif
	gcc $(SEGDIR)/termSegByDelim.c -o $(SEGDIR)/$(SEGEXE)

run:
	./$(RSORTEXE) -chunk 8 -rb @Gais_REC: ../dataset/ettoday_2G.rec > test_2.txt

clean:
ifeq ($(RSORTEXE), $(wildcard $(RSORTEXE)))
	rm $(RSORTEXE)
endif

ifeq ($(RSORTEXE).dSYM, $(wildcard $(RSORTEXE).dSYM))
	rm -rf $(RSORTEXE).dSYM
endif

ifeq ($(SEGDIR)/$(SEGEXE), $(wildcard $(SEGDIR)/$(SEGEXE)))
	rm $(SEGDIR)/$(SEGEXE)
endif