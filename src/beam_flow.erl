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
-export([infill/2]).

-type flow() :: { flow, dict()}.
-type operator() :: 
  {filter | transform, fn, fun() } |
  {filter | transform, mfa, atom(), atom(), list()}.
-type push_internal() :: {branch | pipe | path, list() | []}.

-export_type([flow/0]).
-export_type([operator/0]).

%%--------------------------------------------------------------------
%% @doc
%% Create a new empty flow
%% @end
%%--------------------------------------------------------------------
-spec new() -> flow().
new() ->
  {flow, dict:new()}.

%%--------------------------------------------------------------------
%% @doc
%% Define a filter operation given a predicate (boolean returning) function
%% @end
%%--------------------------------------------------------------------
-spec filter(flow(), fun()) -> operator().
filter({flow, _},Fun) when is_function(Fun) ->
  {filter, fn, Fun}.
  

%%--------------------------------------------------------------------
%% @doc
%% Define a filter oepration given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------
-spec filter(flow(), atom(), atom(), list()) -> operator().
filter({flow, _}, M,F,A) when is_atom(M) and is_atom(F) and is_list(A) ->
  {filter, mfa, M, F, A}.

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a function returning a function
%% @end
%%--------------------------------------------------------------------
-spec transform(flow(), fun()) -> operator().
transform({flow, _},Fun) when is_function(Fun) ->
  {transform, fn, Fun }.

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------

-spec transform(flow(), atom(), atom(), list()) -> operator().
transform({flow, _}, M,F,A) when is_atom(M) and is_atom(F) and is_list(A) ->
   { transform, mfa, M, F, A }.

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), atom(), atom(), list()) -> flow().
branch({flow,Net}, From, As, Pipe) when is_list(Pipe) ->
  Flow = branch({flow,Net}, From, As),
  pipe(Flow, As, Pipe).

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), atom(), atom()) -> flow().
branch({flow,Net}, From, As) ->
  Root = dict:fetch(From,Net),
  case erlang:element(1,Root) of
    path ->  
      {path,Path} = Root,
      {branch, Flow} = Last = lists:last(Path),
      NewPath = lists:append(lists:delete(Last,Path),[{branch, Flow ++ [As]}]),
      {flow,dict:store(From,{path,NewPath},Net)};
    pipe ->
      {pipe,Path} = Root,
      NewPath = lists:append(Path,[{branch,[As]}]),
      {flow,dict:store(From, {path,NewPath},Net)}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Combine, or union, a set of pipelines into a single one
%% @end
%%--------------------------------------------------------------------
-spec combine(flow(), atom(), atom(), list()) -> flow().
combine({flow,Net}, Label, From, Pipe) ->
  Flow = branch({flow,Net}, From, Label),
  Filter = filter({flow,Net}, fun(X) -> drop =/= X end),
  pipe(Flow, Label, lists:append([Filter], Pipe)).

%%--------------------------------------------------------------------
%% @doc
%% Connect a set of flow operations in a named pipeline
%% @end
%%--------------------------------------------------------------------
-spec pipe(flow(), atom(), list()) -> flow().
pipe({flow,Net}, Label, List) ->
  [H|T] = List,
  case erlang:length(T) of
    0 ->
      {flow,dict:store(Label,{pipe,[H]},Net)};
    _ ->
      {flow,dict:store(Label,{pipe,List},Net)}
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
  {flow, Net} = Stream,
  push1(Net, dict:fetch(Label, Net), Data).

-spec push1(dict(), push_internal(), any()) -> any().
push1(Net, {path,Path}, Data) when is_list(Path) ->
  Last = lists:last(Path),
  case erlang:element(1,Last) of
    branch -> 
      {branch, Flow} = Last,
      A = push1(Net, {pipe,lists:sublist(Path,erlang:length(Path)-1)},Data),
      [ push1(Net, {branch, Branch}, A) || Branch <- Flow ],
      branch;
    _ -> push1(Net, {pipe,Path}, Data)
  end;
push1(Net, {branch,Branch}, Data) ->
  push({flow, Net}, Branch, Data);
push1(_Net, {pipe,[]}, Data) -> Data;
push1(Net, {pipe,Flow}, Data) when is_list(Flow) ->
  [This|That] = Flow,
  case This of
    { transform, mfa, M, F, A } ->
      R = erlang:apply(M,F,infill(A,Data)),
      push1(Net, {pipe,That}, R);
    { transform, fn, Fun } ->
      R = Fun(Data),
      push1(Net, {pipe,That}, R);
    { filter, mfa, M, F, A } ->
      X = erlang:apply(M,F,infill(A,Data)),
      case X of
        true -> push1(Net, {pipe,That}, Data);
        _ -> drop
      end;
    { filter, fn, Fun } ->
      X = Fun(Data),
      case X of
        true -> push1(Net, {pipe,That}, Data);
        _ -> drop
      end
  end.

%%--------------------------------------------------------------------
%% @doc
%% Replace occurances of the atom 'in' with the term V in the list
%% @end
%%--------------------------------------------------------------------
-spec infill([any()],any()) -> [any()].
infill(L, V) -> lists:map(fun(in) -> V; (X) -> X end, L).
