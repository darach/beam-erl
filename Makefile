REBAR:=rebar

.PHONY: all erl test clean doc 

all: erl

erl:
	$(REBAR) get-deps compile

test: all
	$(REBAR) skip_deps=true compile ct

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc

dialyzer:
	@dialyzer  src/beam_flow.erl src/beam_bifs.erl

doc:
	$(REBAR) doc

