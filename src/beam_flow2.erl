%% like beam_flow, but adding:
%% - ctx passing
%% - audit for filters, transforms and branches
-module(beam_flow2).

%% flow construction
-export([new/0]).
-export([filter/2]).
-export([filter/4]).
-export([transform/2]).
-export([transform/4]).
-export([branch/3]).
-export([branch/4]).
-export([combine/4]).
-export([pipe/3]).

%% runtime
-export([push/4]).
-export([push_audit/4]).
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
-type operator()  :: filter() | transform().
-type label()     :: any().
-type data()      :: any().
-type ctx()       :: any().
-type audit()     :: { filter|transform|branch, label(), {ctx,ctx()}, {in,data()}, {out,data()}}.
-type exec()      :: { fn, function() } | { mfa, module(), function(), [data()] }.

-export_type([flow/0]).
-export_type([operator/0]).

% @doc create empty flow
-spec new() -> flow().
new() ->
  {flow, dict:new()}.

% @doc create filter pipe element
-spec filter(function(), label()) -> filter().
filter(Fun, As) when is_function(Fun) ->
  {filter, maybe_ignore_ctx(filter, {fn, Fun}), As}.
  
% @doc create filter pipe element
-spec filter(module(), function(), [data()], label()) -> filter().
filter(M,F,A, As) when is_atom(M), is_atom(F), is_list(A) ->
  {filter, {mfa, M,F,A}, As}.

% @doc create transform pipe element
-spec transform(module(), function(), [data()], label()) -> transform().
transform(Fun, As) when is_function(Fun) ->
  {transform, maybe_ignore_ctx(transform, {fn, Fun}), As}.

% @doc create transform pipe element
-spec transform(function(), label()) -> transform().
transform(M,F,A, As) when is_atom(M), is_atom(F), is_list(A) ->
  {transform, {mfa, M,F,A}, As}.

% @doc create branch to already existing pipe
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

% @doc create named pipe and branch pointing to it
-spec branch(flow(), label(), label(), [ operator() ]) -> flow().
branch({flow,_Net}=F, From, As, Pipe) ->
  F2 = pipe(F, As, Pipe),
  branch(F2, From, As).

% opposite of branch
-spec combine(flow(), label(), label(), [ operator() ]) -> flow().
combine({flow,_Net}=F, Label, From, Pipe) ->
  F2 = pipe(F, Label, Pipe),
  branch(F2, From, Label).

% @doc create named pipe
-spec pipe(flow(), label(), [ operator() ]) -> flow().
pipe({flow,Net}, Label, Pipe) ->
  Net2 = dict:store(Label, Pipe, Net),
  {flow, Net2}.

% @doc push event, obtain result
-spec push(flow(), label(), data(), ctx()) -> {data(), ctx()}.
push({flow,Net}, Label, Data, InitCtx) ->
  {Res, Ctx, _Audit} = push1({flow,Net}, Label, Data, InitCtx, false),
  {Res, Ctx}.

% @doc push event, obtain result and audit log
-spec push_audit(flow(), label(), data(), ctx()) -> {data(), ctx(), [audit()]}.
push_audit({flow,Net}, Label, Data, InitCtx) ->
  push1({flow,Net}, Label, Data, InitCtx, true).

% @doc visual flow description
-spec net(flow()) -> proplists:proplists().
net({flow, Net}) -> dict:to_list(Net). 

%% internals
push1({flow,Net}, Label, Data, InitCtx, ShouldAudit) ->
  case dict:find(Label, Net) of
    {ok, Pipe} ->
      BeamState = #beam_state{ctx=InitCtx, should_audit=ShouldAudit},
      {Res, #beam_state{ctx=Ctx, audit=Audit}} = push2(Net, Pipe, Data, BeamState),
      {Res, Ctx, Audit};
    error -> throw({invalid_label, Label})
  end.
 
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
  % assumption: if no ctx is provided in params - don't expect context back
  {R, Ctx2} = case lists:member(ctx, A) of
    true -> erlang:apply(M,F,infill(A, Data, Ctx));
    false -> {erlang:apply(M,F,infill(A, Data, ignore)), Ctx}
  end,
  BeamState2 = BeamState#beam_state{ctx=Ctx2},
  BeamState3 = maybe_audit(Transform, Ctx, Data, R, BeamState2),
  push2(Net, T, R, BeamState3);
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

% @doc in list L, swap 'in' atoms for X
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

% @doc if should_audit, update audit logs
maybe_audit(_, _, _, _, #beam_state{should_audit=false}=BeamState) ->
  BeamState;
maybe_audit({filter, _, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{filter, As, {ctx,Ctx}, {in,In}, {out,Out}}]};
maybe_audit({transform, _, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{transform, As, {ctx,Ctx}, {in,In}, {out,Out}}]};
maybe_audit({branch, As}, Ctx, In, Out, #beam_state{audit=Audit}=BeamState) ->
  BeamState#beam_state{audit=Audit++[{branch, As, {ctx,Ctx}, {in,In}, {out,Out}}]}.

% @doc allow for funs with arity of 2 (Data, Ctx) and 1 (just Data, for contextless processing)
maybe_ignore_ctx(filter, {fn, Fun}=FunSpec) ->
  {arity, N} = erlang:fun_info(Fun, arity),
  case N of
    1 -> {fn, fun(X, _Ctx) -> Fun(X) end};
    2 -> FunSpec;
    _ -> throw({invalid_arity, FunSpec})
  end;
maybe_ignore_ctx(transform, {fn, Fun}=FunSpec) ->
  {arity, N} = erlang:fun_info(Fun, arity),
  case N of
    1 -> {fn, fun(X, Ctx) -> {Fun(X), Ctx} end};
    2 -> FunSpec;
    _ -> throw({invalid_arity, FunSpec})
  end.

branch_res({ok, R}) -> R;
branch_res(branch) -> branch;
branch_res(drop) -> drop.
