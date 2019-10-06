EXE = seg

build:
	gcc termSegByDelim.c -o $(EXE)

clean:
ifeq ($(EXE), $(wildcard $(EXE)))
	rm $(EXE)
endif

ifeq ($(EXE).dSYM, $(wildcard $(EXE).dSYM))
	rm -rf $(EXE).dSYM
endif