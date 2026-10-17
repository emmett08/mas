LATEXMK ?= latexmk
.PHONY: all clean verify
all:
	$(LATEXMK) -pdf -interaction=nonstopmode -halt-on-error -file-line-error -outdir=build mas-protocol.tex
	cp build/mas-protocol.pdf mas-protocol.pdf
clean:
	$(LATEXMK) -C -outdir=build mas-protocol.tex
verify:
	python3 scripts/reproduce-evidence.py
