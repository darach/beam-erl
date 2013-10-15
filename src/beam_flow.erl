%% -------------------------------------------------------------------
%% Copyright (c) 2013 Darach Ennis < darach at gmail dot com > 
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the
%% "Software"), to deal in the Software without restriction, including
%% without limitation the rights to use, copy, modify, merge, publish,
%% distribute, sublicense, and/or sell copies of the Software, and to permit
%% persons to whom the Software is furnished to do so, subject to the
%% following conditions:
%%
%% The above copyright notice and this permission notice shall be included
%% in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
%% OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
%% NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
%% DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
%% OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
%% USE OR OTHER DEALINGS IN THE SOFTWARE.
%%
%% File: beam_flow.erl. Flow control library.
%%
%% -------------------------------------------------------------------
-module(beam_flow).

%% flow construction
-export([new/0]).
-export([filter/4]).
-export([filter/2]).
-export([transform/4]).
-export([transform/2]).
-export([branch/4]).
-export([branch/3]).
-export([combine/4]).
-export([pipe/3]).

%% runtime
-export([push/3]).

%% internal
-export([chain/2]).
-export([infill/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type flow() :: { flow, digraph(), dict()}.
-type push_internal() :: {branch | pipe | path, list() | []}.

-export_type([flow/0]).

%%--------------------------------------------------------------------
%% @doc
%% Create a new empty flow
%% @end
%%--------------------------------------------------------------------
-spec new() -> flow().
new() ->
  {flow, digraph:new(), dict:new()}.

%%--------------------------------------------------------------------
%% @doc
%% Define a filter operation given a predicate (boolean returning) function
%% @end
%%--------------------------------------------------------------------
-spec filter(flow(), fun()) -> digraph:vertex().
filter({flow,G, _},Fun) when is_function(Fun) ->
  digraph:add_vertex(G, { filter, fn, Fun }).

%%--------------------------------------------------------------------
%% @doc
%% Define a filter oepration given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------
-spec filter(flow(), atom(), atom(), list()) -> digraph:vertex().
filter({flow,G, _}, M,F,A) when is_atom(M) and is_atom(F) and is_list(A) ->
  digraph:add_vertex(G, { filter, mfa, M, F, A }).

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a function returning a function
%% @end
%%--------------------------------------------------------------------
-spec transform(flow(), fun()) -> digraph:vertex().
transform({flow,G, _},Fun) when is_function(Fun) ->
  digraph:add_vertex(G, { transform, fn, Fun }).

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------

-spec transform(flow(), atom(), atom(), list()) -> digraph:vertex().
transform({flow,G, _}, M,F,A) when is_atom(M) and is_atom(F) and is_list(A) ->
  digraph:add_vertex(G, { transform, mfa, M, F, A }).

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), atom(), atom(), list()) -> flow().
branch({flow,G,Net}, From, As, Pipe) when is_list(Pipe) ->
  Flow = branch({flow,G,Net}, From, As),
  pipe(Flow, As, Pipe).

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), atom(), atom()) -> flow().
branch({flow,G,Net}, From, As) ->
  Root = dict:fetch(From,Net),
  case erlang:element(1,Root) of
    path ->  
      {path,Path} = Root,
      {branch, Flow} = Last = lists:last(Path),
      NewPath = lists:append(lists:delete(Last,Path),[{branch, Flow ++ [As]}]),
      {flow,G,dict:store(From,{path,NewPath},Net)};
    pipe ->
      {pipe,Path} = Root,
      NewPath = lists:append(Path,[{branch,[As]}]),
      {flow,G,dict:store(From, {path,NewPath},Net)}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Combine, or union, a set of pipelines into a single one
%% @end
%%--------------------------------------------------------------------
-spec combine(flow(), atom(), atom(), list()) -> flow().
combine({flow,G,Net}, Label, From, Pipe) ->
  Flow = branch({flow,G,Net}, From, Label),
  Filter = filter({flow,G,Net}, fun(X) -> drop =/= X end),
  pipe(Flow, Label, lists:append([Filter], Pipe)).

%%--------------------------------------------------------------------
%% @doc
%% Connect a set of flow operations in a named pipeline
%% @end
%%--------------------------------------------------------------------
-spec pipe(flow(), atom(), list()) -> flow().
pipe({flow,G,Net}, Label, List) ->
  [H|T] = List,
  case erlang:length(T) of
    0 ->
      {flow,G,dict:store(Label,{pipe,[H]},Net)};
    _ ->
      L = lists:last(T),
      chain(fun(P,N) -> digraph:add_edge(G,P,N) end, List),
      {flow,G,dict:store(Label,{pipe,digraph:get_path(G,H,L)},Net)}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Push Data into a stream identified by Label into a Flow.
%% The data will stream through the flow producing effects
%% as it is handled by a network of connected operators.
%% @end
%%--------------------------------------------------------------------
-spec push(flow(), atom(), any()) -> any().
push(Stream, Label, Data) when is_atom(Label) ->
  {flow, Graph, Net} = Stream,
  push1(Graph, Net, dict:fetch(Label, Net), Data).

-spec push1(digraph(), dict(), push_internal(), any()) -> any().
push1(Graph, Net, {path,Path}, Data) when is_list(Path) ->
  Last = lists:last(Path),
  case erlang:element(1,Last) of
    branch -> 
      {branch, Flow} = Last,
      A = push1(Graph, Net, {pipe,lists:sublist(Path,erlang:length(Path)-1)},Data),
      [ push1(Graph, Net, {branch, Branch}, A) || Branch <- Flow ],
      branch;
    _ -> push1(Graph, Net, {pipe,Path}, Data)
  end;
push1(Graph, Net, {branch,Branch}, Data) ->
  push({flow, Graph, Net}, Branch, Data);
push1(_Graph, _Net, {pipe,[]}, Data) -> Data;
push1(Graph, Net, {pipe,Flow}, Data) when is_list(Flow) ->
  [This|That] = Flow,
  case This of
    { transform, mfa, M, F, A } ->
      R = erlang:apply(M,F,infill(A,Data)),
      push1(Graph, Net, {pipe,That}, R);
    { transform, fn, Fun } ->
      R = Fun(Data),
      push1(Graph, Net, {pipe,That}, R);
    { filter, mfa, M, F, A } ->
      X = erlang:apply(M,F,infill(A,Data)),
      case X of
        true -> push1(Graph, Net, {pipe,That}, Data);
        _ -> drop
      end;
    { filter, fn, Fun } ->
      X = Fun(Data),
      case X of
        true -> push1(Graph, Net, {pipe,That}, Data);
        _ -> drop
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Connect an ordered list of things together, pair by pair.
%% Given a function of the form fun(L,R) and a list this function
%% will call the function with a preceeding and subsequent element
%% in the list, for all elements in the list.
%%
%% This allows setting up a chain of invocations so that the results
%% of a prior function invocation can be offered as arguments to the
%% next function in a pipeline. See uses in this class for usage.
%% @end
%%--------------------------------------------------------------------
-spec chain(fun(),list()) -> [any()].
chain(_Fn, [])       -> [];
chain(_Fn, [_])      -> [];
chain(Fun, [X, Y|T]) -> [Fun(X, Y) | chain(Fun,[Y|T])].

%%--------------------------------------------------------------------
%% @doc
%% Replace occurances of the atom 'in' with the term V in the list
%% @end
%%--------------------------------------------------------------------
-spec infill([any()],any()) -> [any()].
infill(L, V) -> lists:map(fun(in) -> V; (X) -> X end, L).

-ifdef(TEST).

basic_test() ->
  ?assertEqual([], infill([], boop)),
  ?assertEqual([boop], infill([in], boop)),
  ?assertEqual([boop,beep,boop], infill([in,beep,in], boop)),

  ?assertEqual([], chain(fun(_,_) -> ok end, [])),
  ?assertEqual([], chain(fun(_,_) -> ok end, [beep])),
  ?assertEqual([{beep,boop}], chain(fun(C,P) -> {P,C} end, [boop,beep])),
  ?assertEqual([{bo,be},{be,bo}], chain(fun(C,P) -> {P,C} end, [be,bo,be])),

  Flow = new(),
  IsInt = filter(Flow, erlang, is_integer, [in]),
  FlowInt = pipe(Flow, in, [IsInt]),
  ?assertEqual(2, push(FlowInt,in,2)),
  ?assertEqual(drop, push(FlowInt,in,beep)),

  Listify = transform(Flow, erlang, atom_to_list, [in]),
  FlowListify = pipe(Flow, in, [Listify]),
  ?assertEqual("beep", push(FlowListify, in, beep)),

  A0 = transform(Flow, erlang, list_to_atom, [in]),
  A1 = filter(Flow, fun(X) -> ?assertEqual(beep, X), false end),
  A2 = transform(Flow, fun(X) -> ?assertEqual(drop, X), ok end),
  F0 = pipe(Flow, in, [ A0 ]),
  F1 = branch(F0, in, b1, [ A1 ]),
  F2 = branch(F1, in, b2, [ A1 ]),
  F3 = combine(F2, union, b1, []),
  F4 = combine(F3, union, b2, []),
  F5 = pipe(F4, union, [ A2 ]),
  ?assertEqual(branch, push(F5, in, "beep")).
  
-endif.
