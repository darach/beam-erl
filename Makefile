REBAR:=rebar

.PHONY: all erl test clean doc 

all: erl test

erl:
	$(REBAR) get-deps compile

test: all
	$(REBAR) skip_deps=true compile ct

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc

dialyzer:
	@dialyzer --verbose --plts .plt --src src -r ebin

doc:
	$(REBAR) doc

build-plt:
	@dialyzer --build_plt --output_plt .plt --apps kernel stdlib erts
