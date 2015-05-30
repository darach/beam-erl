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
-export([filter/2]).
-export([filter/4]).
-export([transform/2]).
-export([transform/4]).
-export([splitter/2]).
-export([splitter/4]).
-export([branch/3]).
-export([branch/4]).
-export([combine/3]).
-export([combine/4]).
-export([pipe/3]).

%% runtime
-export([push/4]).
-export([push/5]).
-export([net/1]).

%% necessary to expose...?
-export([infill/3]).

-record(beam_state, {
    ctx :: any(),
    should_audit = false :: boolean(),
    audit = [] :: list()
  }).

-type flow()      :: { flow, dict() }.
-type filter()    :: { filter, exec(), label() }.
-type transform() :: { transform, exec(), label() }.
-type splitter()  :: { splitter, exec(), label() }.
-type operator()  :: filter() | transform().
-type label()     :: any().
-type data()      :: any().
-type ctx()       :: any().
-type audit()     :: { filter|transform|splitter|branch, label(), {ctx,ctx()}, {in,data()}, {out,data()}}.
-type exec()      :: { fn, function() } | { mfa, module(), atom(), [data()] }.

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
-spec filter(function(), label()) -> filter().
filter(Fun, As) when is_function(Fun) ->
  {filter, maybe_ignore_ctx(filter, {fn, Fun}), As}.
  
%%--------------------------------------------------------------------
%% @doc
%% Define a filter oepration given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------
-spec filter(module(), atom(), [data()], label()) -> filter().
filter(M,F,A, As) when is_atom(M), is_atom(F), is_list(A) ->
  {filter, {mfa, M,F,A}, As}.

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a function returning a function
%% @end
%%--------------------------------------------------------------------
-spec transform(function(), label()) -> transform().
transform(Fun, As) when is_function(Fun) ->
  {transform, maybe_ignore_ctx(transform, {fn, Fun}), As}.

%%--------------------------------------------------------------------
%% @doc
%% Define a transform operation given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------
-spec transform(module(), atom(), [data()], label()) -> transform().
transform(M,F,A, As) when is_atom(M), is_atom(F), is_list(A) ->
  {transform, {mfa, M,F,A}, As}.

%%--------------------------------------------------------------------
%% @doc
%% Define a splitter operation given a function returning a function
%% @end
%%--------------------------------------------------------------------
-spec splitter(function(), label()) -> splitter().
splitter(Fun, As) when is_function(Fun) ->
  {splitter, maybe_ignore_ctx(splitter, {fn, Fun}), As}.

%%--------------------------------------------------------------------
%% @doc
%% Define a splitter operation given a Module, Fun and Arguments
%% @end
%%--------------------------------------------------------------------
-spec splitter(module(), atom(), [data()], label()) -> splitter().
splitter(M,F,A, As) when is_atom(M), is_atom(F), is_list(A) ->
  {splitter, {mfa, M,F,A}, As}.

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), label(), label()) -> flow().
branch({flow,Net}, From, As) ->
  case dict:is_key(As, Net) of
    true -> ok;
    false -> throw({invalid_as_label, As})
  end,
  case dict:find(From, Net) of
    {ok, FromPipe} ->
      {FromPipeHeads, FromPipeLast} = heads_and_last(FromPipe),
      FromPipe2 = case FromPipeLast of
        {branch, Branches} ->
          % add to existing branches
          FromPipeHeads ++ [{branch, Branches++[As]}];
        _ ->
          % create new branch node
          FromPipe ++ [{branch, [As]}]
      end,
      Net2 = dict:store(From, FromPipe2, Net),
      {flow, Net2};
    error ->
      throw({invalid_from_label, From})
  end.

%%--------------------------------------------------------------------
%% @doc
%% Branch a flow into multiple sub flows
%% @end
%%--------------------------------------------------------------------
-spec branch(flow(), label(), label(), [ operator() ]) -> flow().
branch({flow,_Net}=F, From, As, Pipe) ->
  F2 = pipe(F, As, Pipe),
  branch(F2, From, As).

%%--------------------------------------------------------------------
%% @doc
%% Combine, or union, a set of pipelines into an existing pipe
%% @end
%%--------------------------------------------------------------------
-spec combine(flow(), label(), label()) -> flow().
combine({flow,_Net}=F, Label, From) ->
  branch(F, From, Label).

%%--------------------------------------------------------------------
%% @doc
%% Combine, or union, a set of pipelines into a single one
%% @end
%%--------------------------------------------------------------------
-spec combine(flow(), label(), label(), [ operator() ]) -> flow().
combine({flow,_Net}=F, Label, From, Pipe) ->
  F2 = pipe(F, Label, Pipe),
  combine(F2, Label, From).

%%--------------------------------------------------------------------
%% @doc
%% Connect a set of flow operations in a named pipeline
%% @end
%%--------------------------------------------------------------------
-spec pipe(flow(), label(), [ operator() ]) -> flow().
pipe({flow,Net}, Label, Pipe) ->
  Net2 = dict:store(Label, Pipe, Net),
  {flow, Net2}.

%%--------------------------------------------------------------------
%% @doc
%% Push Data into a stream identified by Label into a Flow.
%% The data will stream through the flow producing effects
%% as it is handled by a network of connected operators.
%% Switch audit off
%% @end
%%--------------------------------------------------------------------
-spec push(flow(), label(), data(), ctx()) -> {data(), ctx()}.
push({flow,_Net}=F, Label, Data, InitCtx) ->
  {Res, Ctx, []} = push(F, Label, Data, InitCtx, false),
  {Res, Ctx}.

%%--------------------------------------------------------------------
%% @doc
%% Push Data into a stream identified by Label into a Flow.
%% The data will stream through the flow producing effects
%% as it is handled by a network of connected operators.
%% Optionally audit.
%% @end
%%--------------------------------------------------------------------
-spec push(flow(), label(), data(), ctx(), boolean()) -> {data(), ctx(), [audit()]}.
push({flow,Net}, Label, Data, InitCtx, ShouldAudit) ->
  case dict:find(Label, Net) of
    {ok, Pipe} ->
      BeamState = #beam_state{ctx=InitCtx, should_audit=ShouldAudit},
      {Res, #beam_state{ctx=Ctx, audit=Audit}} = push2(Net, Pipe, Data, BeamState),
      {Res, Ctx, Audit};
    error -> throw({invalid_label, Label})
  end.

%%--------------------------------------------------------------------
%% @doc
%% Produce a human eye friendly representation of the flow.
%% @end
%%--------------------------------------------------------------------
-spec net(flow()) -> proplists:proplist().
net({flow, Net}) -> dict:to_list(Net).

%% internals
push2(_Net, [], Data, BeamState) ->
  {{ok, Data}, BeamState};
push2(Net, [{filter, {fn, Fun}, _As}=Filter|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  R = Fun(Data, Ctx),
  BeamState2 = maybe_audit(Filter, Ctx, Data, R, BeamState),
  case R of
    true -> push2(Net, T, Data, BeamState2);
    _ -> {drop, BeamState2}
  end;
push2(Net, [{filter, {mfa, M,F,A}, _As}=Filter|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  R = erlang:apply(M,F,infill(A, Data, Ctx)),
  BeamState2 = maybe_audit(Filter, Ctx, Data, R, BeamState),
  case R of
    true -> push2(Net, T, Data, BeamState2);
    _ -> {drop, BeamState2}
  end;
push2(Net, [{transform, {fn, Fun}, _As}=Transform|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  {R, Ctx2} = Fun(Data, Ctx),
  BeamState2 = BeamState#beam_state{ctx=Ctx2},
  BeamState3 = maybe_audit(Transform, Ctx, Data, R, BeamState2),
  push2(Net, T, R, BeamState3);
push2(Net, [{transform, {mfa, M,F,A}, _As}=Transform|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  % if no ctx is provided in params - don't expect context back
  {R, Ctx2} = case lists:member(ctx, A) of
    true -> erlang:apply(M,F,infill(A, Data, Ctx));
    false -> {erlang:apply(M,F,infill(A, Data, ignore)), Ctx}
  end,
  BeamState2 = BeamState#beam_state{ctx=Ctx2},
  BeamState3 = maybe_audit(Transform, Ctx, Data, R, BeamState2),
  push2(Net, T, R, BeamState3);
push2(Net, [{splitter, {fn, Fun}, As}=Splitter|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  {Results, Ctx2} = Fun(Data, Ctx),
  case is_list(Results) of
    true -> ok;
    _ -> throw({splitter_returns_non_list,As,Results})
  end,
  BeamState2 = BeamState#beam_state{ctx=Ctx2},
  BeamState3 = maybe_audit(Splitter, Ctx, Data, Results, BeamState2),
  BeamState4 = lists:foldl(
    fun(R, BS) ->
      {_, BS2} = push2(Net, T, R, BS),
      BS2
    end,
    BeamState3,
    Results),
  {branch, BeamState4};
push2(Net, [{splitter, {mfa, M,F,A}, As}=Splitter|T], Data, #beam_state{ctx=Ctx}=BeamState) ->
  % if no ctx is provided in params - don't expect context back
  {Results, Ctx2} = case lists:member(ctx, A) of
    true -> erlang:apply(M,F,infill(A, Data, Ctx));
    false -> {erlang:apply(M,F,infill(A, Data, ignore)), Ctx}
  end,
  case is_list(Results) of
    true -> ok;
    _ -> throw({splitter_returns_non_list,As,Results})
  end,
  BeamState2 = BeamState#beam_state{ctx=Ctx2},
  BeamState3 = maybe_audit(Splitter, Ctx, Data, Results, BeamState2),
  BeamState4 = lists:foldl(
    fun(R, BS) ->
      {_, BS2} = push2(Net, T, R, BS),
      BS2
    end,
    BeamState3,
    Results),
  {branch, BeamState4};
push2(Net, [{branch, Branches}|T], Data, BeamState) ->
  BeamState2 = lists:foldl(
    fun(Branch, #beam_state{ctx=Ctx}=Bs) ->
      Pipe = dict:fetch(Branch, Net),
      {R, Bs2} = push2(Net, Pipe, Data, Bs),
      Bs3 = maybe_audit({branch, Branch}, Ctx, Data, branch_res(R), Bs2),
      Bs3
    end,
    BeamState,
    Branches),
  {_, BeamState3} = push2(Net, T, Data, BeamState2),
  {branch, BeamState3}.

%%--------------------------------------------------------------------
%% @doc
%% Replace occurances of the atom 'in' and 'ctx' with the term In
%% and Ctx in the list.
%% @end
%%--------------------------------------------------------------------
infill(L, In, Ctx) ->
  lists:map(
    fun(in) -> In;
       (ctx) -> Ctx;
       (X) -> X
    end,
    L).

heads_and_last([]) -> {[], undefined};
heads_and_last(L) ->
  [Last|T] = lists:reverse(L),
  {lists:reverse(T), Last}.

%%--------------------------------------------------------------------
%% @doc
%% if 'should_audit' is set, produce an audit log on filters,
%% transforms and branches.
%% @end
%%--------------------------------------------------------------------
maybe_audit(_, _, _, _, #beam_state{should_audit=false}=BeamState) ->
  BeamState;
maybe_audit({filter, _, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{filter, As, {ctx,Ctx}, {in,In}, {out,Out}}]};
maybe_audit({transform, _, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{transform, As, {ctx,Ctx}, {in,In}, {out,Out}}]};
maybe_audit({splitter, _, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{splitter, As, {ctx,Ctx}, {in,In}, {out,Out}}]};
maybe_audit({branch, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{branch, As, {ctx,Ctx}, {in,In}, {out,Out}}]}.

%%--------------------------------------------------------------------
%% @doc
%% Deal with transforms/filters that do not expect Ctx propagation,
%% and only work off the streamed events.
%% @end
%%--------------------------------------------------------------------
maybe_ignore_ctx(filter, {fn, Fun}=FunSpec) ->
  {arity, N} = erlang:fun_info(Fun, arity),
  case N of
    1 -> {fn, fun(X, _Ctx) -> Fun(X) end};
    2 -> FunSpec;
    _ -> throw({invalid_arity, FunSpec})
  end;
maybe_ignore_ctx(Type, {fn, Fun}=FunSpec) when Type=:=transform; Type=:=splitter ->
  {arity, N} = erlang:fun_info(Fun, arity),
  case N of
    1 -> {fn, fun(X, Ctx) -> {Fun(X), Ctx} end};
    2 -> FunSpec;
    _ -> throw({invalid_arity, FunSpec})
  end.

branch_res({ok, R}) -> R;
branch_res(branch) -> branch;
branch_res(drop) -> drop.
