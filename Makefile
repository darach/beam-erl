REBAR:=rebar

.PHONY: all erl test clean doc 

all: erl

erl:
	$(REBAR) get-deps compile

test: all
	@mkdir -p .eunit
	$(REBAR) skip_deps=true compile ct eunit

clean:
	$(REBAR) clean
	-rm -rvf deps ebin doc .eunit

dialyzer:
	@dialyzer  src/beam_flow.erl src/beam_bifs.erl

doc:
	$(REBAR) doc

