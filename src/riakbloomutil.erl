%% -------------------------------------------------------------------
%%
%% riakbloomutil: Main file for riakbloomutil application
%%
%% Copyright (c) 2012 WhiteNode Software Ltd.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riakbloomutil).

-export([main/1]).

-define(READSIZE, 65536).
-define(MD_USERMETA, <<"X-Riak-Meta">>).
-define(MD_BLOOM_ELEMENTS, "X-Riak-Meta-Bloom-Elements").
-define(MD_BLOOM_PROBABILITY, "X-Riak-Meta-Bloom-Probability").
-define(MD_BLOOM_SEED, "X-Riak-Meta-Bloom-Seed").

main([]) ->
    getopt:usage(option_spec_list(), escript:script_name());
main(Args) ->
    OptSpecList = option_spec_list(),
    case getopt:parse(OptSpecList, Args) of
        {ok, {Options, []}} ->
            case lists:member(help, Options) of
                true ->
                    getopt:usage(option_spec_list(), escript:script_name());
                false ->
                    case proplists:is_defined(update, Options) of
                        true ->
                            ValOpts = [{file, "Error: No key file was specified.\n"},
                                       {filter, "Error: No filter name was specified.\n"}],
                            case validate_options(Options, ValOpts) of
                                ok ->
                                    process_update(Options);
                                _ ->
                                    terminate
                            end;
                        false ->
                            ValOpts = [{file, "Error: No key file was specified.\n"},
                                       {filter, "Error: No filter name was specified.\n"},
                                       {elements, "Error: Estimated number of keys in filter was not specified.\n"},
                                       {probability, "Error: Requested false positive probability was not specified.\n"}],
                            case validate_options(Options, ValOpts) of
                                ok ->
                                    process_create(Options);
                                _ ->
                                    terminate
                            end
                    end
            end;
        {error, {Reason, Data}} ->
            io:format("Error: ~s ~p~n", [Reason, Data]),
            getopt:usage(OptSpecList, escript:script_name());
        {ok, {_, [P | _Rest]}} ->
            io:format("Error: Unknown option ~p specified.~n", [P]),
            getopt:usage(OptSpecList, escript:script_name())
    end.

option_spec_list() ->
    [
     %% {Name, ShortOpt, LongOpt, ArgSpec, HelpMsg}
     {help, $?, "help", undefined, "Show the program options"},
     {host, $h, "host", {string, "127.0.0.1"}, "Riak server host name or IP address. [default: '127.0.0.1']"},
     {port, $p, "port", {integer, 8087}, "Riak server port. [default: 8087]"},
     {bucket, $b, "bucket", {string, "riakbloom"}, "Name of the bucket used for storing riakbloom filters. [default: 'riakbloom']"},
     {filter, $f, "filter", string, "Name of the filter to be created or updated. Mandatory."},
     {update, $u, "update", undefined, "Flag indicating existing filter is to be updated. If not set, a new filter will be created."},
     {elements, $E, "elements", integer, "Estimated number of elements the filter will hold. Mandatory when creating filter."},
     {probability, $P, "probability", float, "Requested false positive probability expressed as float in interval [0 < P < 1]. Mandatory when creating filter."},
     {seed, $S, "seed", {integer, 0}, "Integer seed value to be used for new filter. [default: 0]"},
     {file, undefined, undefined, string, "Name of file containing filter keys. Mandatory."}
    ].

validate_options(_Options, []) ->
    ok;
validate_options(Options, [{Name, ErrorMsg} | Rest]) ->
    case verify_option_defined(Options, Name, ErrorMsg) of
        ok ->
            validate_options(Options, Rest);
        _ ->
            error
    end.

verify_option_defined(Options, OptionName, ErrorMessage) ->
    case proplists:is_defined(OptionName, Options) of
        true ->
            ok;
        false ->
            io:fwrite(ErrorMessage),
            getopt:usage(option_spec_list(), escript:script_name()),
            error
    end.

process_update(PropList) ->
    Host = proplists:get_value(host, PropList),
    Port = proplists:get_value(port, PropList),
    Bucket = proplists:get_value(bucket, PropList),
    FilterName = proplists:get_value(filter, PropList),
    case riakc_pb_client:start(Host, Port) of
        {ok, C} ->
            case riakc_pb_socket:get(C, Bucket, FilterName) of
                {ok, RO} ->
                    reconcile_and_update_filter(C, RO, PropList);
                {error, notfound} ->
                    io:format("Error: Filter ~p could not be found in the database.~n", [FilterName]),
                    terminate;
                {error, E} ->
                    io:format("Unexpected error retrieving filter ~p. [~p]~n", [FilterName, E]),
                    terminate
            end;
        {error, _} ->
            io:fwrite("Error connecting to host ~s on port ~p.~n", [Host, Port]),
            terminate
    end.

reconcile_and_update_filter(C, RO, PropList) ->
    FileName = proplists:get_value(file, PropList),
    case deserialize_and_reconcile_filters(RO) of
        {ok, Filter} ->
            case add_file_keys_to_filter(Filter, FileName) of
                ok ->
                    RO2 = riakc_obj:update_value(RO, ebloom:serialize(Filter)),
                    upload_filter(C, RO2);
                terminate ->
                    terminate
            end;
        none ->
            io:format("Error: No filter found in the database.~n"),
            terminate
    end.
    
%% hidden
deserialize_and_reconcile_filters(RO) ->
    case [D || D <- riakc_obj:get_values(RO), D =/= <<>>] of
        [] ->
            none;
        [V | VL] ->
            {ok, Filter} = ebloom:deserialize(V),
            lists:foreach(fun(F) ->
                              {ok, DF} = ebloom:deserialize(F),
                              ebloom:union(Filter, DF)
                          end, VL),
            {ok, Filter}
    end.

%% hidden
process_create(PropList) ->
    Elements = proplists:get_value(elements, PropList),
    Probability = proplists:get_value(probability, PropList),
    Seed = proplists:get_value(seed, PropList),
    FileName = proplists:get_value(file, PropList),
    case filter_parameters_ok(Elements, Probability, Seed) of
        true ->
            {ok, Filter} = ebloom:new(Elements, Probability, Seed),
                case add_file_keys_to_filter(Filter, FileName) of
                    terminate ->
                        terminate;
                    ok ->
                        FilterMD = [{?MD_BLOOM_ELEMENTS, integer_to_list(Elements)},
                                    {?MD_BLOOM_PROBABILITY, float_to_list(Probability)},
                                    {?MD_BLOOM_SEED, integer_to_list(Seed)}],
                        upload_created_filter(Filter, FilterMD, PropList)
                end;
        false ->
            io:fwrite("Error: Filter can not be created due to invalid configuration parameters.~n"),
            terminate
    end.

%% hidden
upload_created_filter(Filter, FilterMD, PropList) ->
    Host = proplists:get_value(host, PropList),
    Port = proplists:get_value(port, PropList),
    Bucket = proplists:get_value(bucket, PropList),
    FilterName = proplists:get_value(filter, PropList),
    SerializedFilter = ebloom:serialize(Filter),
    case riakc_pb_socket:start_link(Host, Port) of
        {ok, C} ->
            case riakc_pb_socket:get(C, Bucket, FilterName) of
                {ok, RO} ->
                    [Meta | _Rest] = riakc_obj:get_metadatas(RO),
                    Meta2 = dict:store(?MD_USERMETA, FilterMD, Meta),
                    RO2 = riakc_obj:update_metadata(RO, Meta2),
                    upload_filter(C, RO2);
                {error, _} ->
                    RO = riakc_obj:new(Bucket, FilterName, SerializedFilter, "application/octet-stream"),
                    Meta = dict:from_list([{?MD_USERMETA, FilterMD}]),
                    RO2 = riakc_obj:update_metadata(RO, Meta),
                    upload_filter(C, RO2)
            end;
        {error, _} ->
            io:fwrite("Error connecting to host ~s on port ~p.~n", [Host, Port]),
            terminate
    end.

%% hidden
upload_filter(Conn, RO) ->
    case riakc_pb_socket:put(Conn, RO) of
        ok ->
            io:fwrite("Successfully stored filter.~n"),
            ok;
        {ok, _} ->
            io:fwrite("Successfully stored filter.~n"),
            ok;
        {error, Reason} ->
            io:fwrite("Error: Unexpected error storing filter. [~p].~n", [Reason]),
            terminate
    end.

%% hidden
filter_parameters_ok(Elements, Prob, Seed) ->
    case {(Elements > 0),(Prob > 0),(Prob < 1),(Seed >= 0)} of
        {true, true, true, true} ->
            true;
        _ ->
            false
    end.

%% hidden
add_file_keys_to_filter(Filter, FileName) ->
    case file:open(FileName, [binary, read, raw, {read_ahead, ?READSIZE}]) of
        {ok, IoDev} ->
            add_keys_to_filter(IoDev, Filter);
        {error, _} ->
            io:fwrite("Error: Unable to open file ~s.~n", [FileName]),
            terminate
    end.

%% hidden
add_keys_to_filter(IoDev, Filter) ->
    case file:read_line(IoDev) of
        {ok, Bin} ->
            case strip_nl(Bin) of
                <<"">> ->
                    add_keys_to_filter(IoDev, Filter);
                K ->
                    ebloom:insert(Filter, K),
                    add_keys_to_filter(IoDev, Filter)
            end;
        eof ->
            file:close(IoDev),
            ok;
        {error, Reason} ->
            file:close(IoDev),
            {error, Reason}
    end.

%% hidden
strip_nl(Bin) when is_binary(Bin) ->
    case binary:last(Bin) of
        10 ->
            case binary:split(Bin, <<"\n">>) of
                [B] ->
                    B;
                [B, <<"">>] ->
                    B
            end;
        _ ->
            Bin
    end.
