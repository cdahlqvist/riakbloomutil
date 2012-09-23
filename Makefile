.PHONY: deps

all: deps compile ebloom_c
	./rebar skip_deps=true escriptize

deps:
	./rebar get-deps

compile: deps
	./rebar compile

ebloom_c:
	@cd ./deps/ebloom && make

clean:
	@./rebar clean

distclean: clean
	@rm -rf riakbloomutil deps

