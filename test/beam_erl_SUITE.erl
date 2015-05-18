%% -------------------------------------------------------------------
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
%% -------------------------------------------------------------------
%%  @author Michael Coles < michael dot coles at gmail dot com >
%%  @author Darach Ennis <darach.ennis@gmail.com>
%%  @copyright (C) 2013, Michael Coles, Darach Ennis
%%  @doc
%% 
%%  @end
%%  Created : 2013-10-14 14:14:55.186464
%% -------------------------------------------------------------------
-module(beam_erl_SUITE).

%% API
-compile(export_all).
-include_lib("common_test/include/ct.hrl").

all() -> [
          {group, samples},
          {group, beam_erl},
          {group, beam_bifs} 
         ].

suite() -> [{ct_hooks,[cth_surefire]}, {timetrap, {seconds, 30}}].

groups() ->
    [
     {samples, [], [
                      t_sample_usage
                    , t_sample_branch
                    , t_sample_union
                    ]},
     {beam_erl, [], [
                      t_infill_basic
                    , t_push_basic
                    , t_filter_pipe_push
                    , t_transform_pipe_push
                    , t_ctx_propagation
                    , t_audit
                    ]},
     {beam_bifs, [], [
                       t_eq
                     , t_neq
                     , t_lte
                     , t_lt
                     , t_gt
                     , t_gte
                     , t_seq
                     , t_sne
                     , t_uplus
                     , t_uminus
                     , t_plus
                     , t_minus
                     , t_mul
                     , t_fdiv
                     , t_idiv
                     , t_irem
                     , t_ibnot
                     , t_iband
                     , t_ibor
                     , t_ibxor
                     , t_ibsl
                     , t_ibsr
                     , t_bonot
                     , t_boand
                     , t_boor
                     , t_boxor
                     ]}
    ].

init_per_suite(Config) ->
    [{new_flow, beam_flow:new()} | Config].

%%%===================================================================
%%% Samples
%%%===================================================================

t_sample_usage(_Config) ->
    Empty = beam_flow:new(),
    IsEven = beam_flow:filter(fun(X) -> X rem 2 == 0 end, is_even),
    Square = beam_flow:transform(fun(X) -> X * X end, square),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    Stream = beam_flow:pipe(Empty, source, [IsEven, Square]),
    %% push events into the stream, even numbers get squared
    {{ok, 4}, no_ctx} = beam_flow:push(Stream, source, 2, no_ctx),
    {{ok, 16}, no_ctx} = beam_flow:push(Stream, source, 4, no_ctx),
    %% odd numbers get dropped
    {drop, no_ctx} = beam_flow:push(Stream, source, 3, no_ctx),
    {drop, no_ctx} = beam_flow:push(Stream, source, 9, no_ctx).

t_sample_branch(_Config) ->
    Empty = beam_flow:new(),
    IsEven = beam_flow:filter(fun(X) -> X rem 2 == 0 end, is_even),
    IsOdd = beam_flow:filter(fun(X) -> X rem 2 == 1 end, is_odd),
    Sink1 = beam_flow:filter(
      fun(X) -> % evens
         0 = X rem 2
      end, sink1),
    Sink2 = beam_flow:filter(
      fun(X) -> % odds
         1 = X rem 2
      end, sink2),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    S0 = beam_flow:pipe(Empty, source, []),
    S1 = beam_flow:branch(S0, source, evens, [IsEven, Sink1]),
    S2 = beam_flow:branch(S1, source, odds, [IsOdd, Sink2]), 
    %% push events into the stream, even numbers get squared
    {branch, no_ctx} = beam_flow:push(S2, source, 2, no_ctx),
    {branch, no_ctx} = beam_flow:push(S2, source, 4, no_ctx),
    %% odd numbers get dropped
    {branch, no_ctx} = beam_flow:push(S2, source, 3, no_ctx),
    {branch, no_ctx} = beam_flow:push(S2, source, 9, no_ctx).

t_sample_union(_Config) ->
    Empty = beam_flow:new(),
    Union = beam_flow:filter(
      fun(X) ->
         true = erlang:is_number(X)
      end, union),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    S0 = beam_flow:pipe(Empty, source1, []),
    S1 = beam_flow:pipe(S0, source2, []),
    S2 = beam_flow:pipe(S1, union, [Union]),
    S3 = beam_flow:combine(S2, union, source1, []),
    S4 = beam_flow:combine(S3, union, source2, []),
    {branch, no_ctx} = beam_flow:push(S4, source1, 1, no_ctx),
    {branch, no_ctx} = beam_flow:push(S4, source2, 2, no_ctx),
    {branch, no_ctx} = beam_flow:push(S4, source1, 3, no_ctx),
    {branch, no_ctx} = beam_flow:push(S4, source2, 4, no_ctx).

%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================
t_infill_basic(_Config) ->
    [] = beam_flow:infill([], boop, baah),
    [boop] = beam_flow:infill([in], boop, baah),
    [boop, beep, boop, baah, baah] = beam_flow:infill([in, beep, in, ctx, ctx], boop, baah).

t_push_basic(_Config) ->
    Flow = beam_flow:new(),
    IsInt = beam_flow:filter(erlang, is_integer, [in], is_int),
    FlowInt = beam_flow:pipe(Flow, in, [IsInt]),

    {{ok, 2}, no_ctx} = beam_flow:push(FlowInt, in, 2, no_ctx),
    {drop, no_ctx} = beam_flow:push(FlowInt, in, beep, no_ctx).

t_filter_pipe_push(_Config) ->
    Flow = beam_flow:new(),
    Listify = beam_flow:transform(erlang, atom_to_list, [in], listify),
    FlowListify = beam_flow:pipe(Flow, in, [Listify]),

    {{ok, "beep"}, no_ctx} = beam_flow:push(FlowListify, in, beep, no_ctx).

t_transform_pipe_push(_Config) ->
    Flow = beam_flow:new(),

    A0 = beam_flow:transform(erlang, list_to_atom, [in], a0),
    F0 = beam_flow:pipe(Flow, in, [ A0 ]),

    A1 = beam_flow:filter(fun(X) -> beep = X, false end, a1),
    F1 = beam_flow:branch(F0, in, b1, [ A1 ]),
    F2 = beam_flow:branch(F1, in, b2, [ A1 ]),
    F3 = beam_flow:combine(F2, union, b1, []),
    F4 = beam_flow:combine(F3, union, b2, []),

    A2 = beam_flow:transform(fun(X) -> drop = X, ok end, a2),
    F5 = beam_flow:pipe(F4, union, [ A2 ]),

    {branch, no_ctx} = beam_flow:push(F5, in, "beep", no_ctx).


%%--------------------------------------------------------------------
%% beam_bifs
%%--------------------------------------------------------------------
t_eq(_Config) ->
    Eq = beam_bifs:eq(2),
    FlowEq = beam_flow:pipe(beam_flow:new(), in, [Eq]),
    {drop, no_ctx} = beam_flow:push(FlowEq, in, 1, no_ctx),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowEq, in, 2, no_ctx).

t_neq(_Config) ->
    Neq = beam_bifs:neq(2),
    FlowNeq = beam_flow:pipe(beam_flow:new(), in, [Neq]),
    {drop, no_ctx} = beam_flow:push(FlowNeq, in, 2, no_ctx),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowNeq, in, 1, no_ctx).

t_lte(_Config) ->
    Lte = beam_bifs:lte(2),
    FlowLte = beam_flow:pipe(beam_flow:new(), in, [Lte]),
    {drop, no_ctx} = beam_flow:push(FlowLte, in, 3, no_ctx),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowLte, in, 2, no_ctx),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowLte, in, 1, no_ctx).

t_lt(_Config) ->
    Lt = beam_bifs:lt(2),
    FlowLt = beam_flow:pipe(beam_flow:new(), in, [Lt]),
    {drop, no_ctx} = beam_flow:push(FlowLt, in, 3, no_ctx),
    {drop, no_ctx} = beam_flow:push(FlowLt, in, 2, no_ctx),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowLt, in, 1, no_ctx).

t_gt(_Config) ->
    Gt = beam_bifs:gt(2),
    FlowGt = beam_flow:pipe(beam_flow:new(), in, [Gt]),
    {drop, no_ctx} = beam_flow:push(FlowGt, in, 1, no_ctx),
    {drop, no_ctx} = beam_flow:push(FlowGt, in, 2, no_ctx),
    {{ok, 3}, no_ctx} = beam_flow:push(FlowGt, in, 3, no_ctx).

t_gte(_Config) ->
    Gte = beam_bifs:gte(2),
    FlowGte = beam_flow:pipe(beam_flow:new(), in, [Gte]),
    {drop, no_ctx} = beam_flow:push(FlowGte, in, 1, no_ctx),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowGte, in, 2, no_ctx),
    {{ok, 3}, no_ctx} = beam_flow:push(FlowGte, in, 3, no_ctx).

t_seq(_Config) ->
    Seq = beam_bifs:seq(2),
    FlowSeq = beam_flow:pipe(beam_flow:new(), in, [Seq]),
    {drop, no_ctx} = beam_flow:push(FlowSeq, in, 1, no_ctx),
    {drop, no_ctx} = beam_flow:push(FlowSeq, in, 2.0, no_ctx),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowSeq, in, 2, no_ctx).

t_sne(_Config) ->
    Sne = beam_bifs:sne(2),
    FlowSne = beam_flow:pipe(beam_flow:new(), in, [Sne]),
    {drop, no_ctx} = beam_flow:push(FlowSne, in, 2, no_ctx),
    {{ok, 2.0}, no_ctx} = beam_flow:push(FlowSne, in, 2.0, no_ctx),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowSne, in, 1, no_ctx).

t_uplus(_Config) ->
    Uplus = beam_bifs:uplus(),
    FlowUplus = beam_flow:pipe(beam_flow:new(), in, [Uplus]),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowUplus, in, 2, no_ctx).

t_uminus(_Config) ->
    Uminus = beam_bifs:uminus(),
    FlowUminus = beam_flow:pipe(beam_flow:new(), in, [Uminus]),
    {{ok, -2}, no_ctx} = beam_flow:push(FlowUminus, in, 2, no_ctx),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowUminus, in, -2, no_ctx).

t_plus(_Config) ->
    Plus = beam_bifs:plus(3),
    FlowPlus = beam_flow:pipe(beam_flow:new(), in, [Plus]),
    {{ok, 6}, no_ctx} = beam_flow:push(FlowPlus, in, 3, no_ctx).

t_minus(_Config) ->
    Minus = beam_bifs:minus(3),
    FlowMinus = beam_flow:pipe(beam_flow:new(), in, [Minus]),
    {{ok, 6}, no_ctx} = beam_flow:push(FlowMinus, in, 9, no_ctx).

t_mul(_Config) ->
    Mul = beam_bifs:mul(3),
    FlowMul = beam_flow:pipe(beam_flow:new(), in, [Mul]),
    {{ok, 9}, no_ctx} = beam_flow:push(FlowMul, in, 3, no_ctx).

t_fdiv(_Config) ->
    Fdiv = beam_bifs:fdiv(3),
    FlowFdiv = beam_flow:pipe(beam_flow:new(), in, [Fdiv]),
    {{ok, 3.0}, no_ctx} = beam_flow:push(FlowFdiv , in, 9, no_ctx).

t_idiv(_Config) ->
    Idiv = beam_bifs:idiv(3),
    FlowIdiv = beam_flow:pipe(beam_flow:new(), in, [Idiv]),
    {{ok, 3}, no_ctx} = beam_flow:push(FlowIdiv , in, 9, no_ctx).

t_irem(_Config) ->
    Irem = beam_bifs:irem(2),
    FlowIrem = beam_flow:pipe(beam_flow:new(), in, [Irem]),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowIrem, in, 1, no_ctx),
    {{ok, 0}, no_ctx} = beam_flow:push(FlowIrem, in, 2, no_ctx).

t_ibnot(_Config) ->
    Ibnot = beam_bifs:ibnot(),
    FlowIbnot = beam_flow:pipe(beam_flow:new(), in, [Ibnot]),
    {{ok, -2}, no_ctx} = beam_flow:push(FlowIbnot, in, 1, no_ctx),
    {{ok, 0}, no_ctx} = beam_flow:push(FlowIbnot, in, -1, no_ctx).

t_iband(_Config) ->
    Iband = beam_bifs:iband(6),
    FlowIband = beam_flow:pipe(beam_flow:new(), in, [Iband]),
    {{ok, 2}, no_ctx} = beam_flow:push(FlowIband, in, 2, no_ctx).

t_ibor(_Config) ->
    Ibor = beam_bifs:ibor(2),
    FlowIbor = beam_flow:pipe(beam_flow:new(), in, [Ibor]),
    {{ok, 3}, no_ctx} = beam_flow:push(FlowIbor, in, 1, no_ctx).

t_ibxor(_Config) ->
    Ibxor = beam_bifs:ibxor(6),
    FlowIbxor = beam_flow:pipe(beam_flow:new(), in, [Ibxor]),
    {{ok, 4}, no_ctx} = beam_flow:push(FlowIbxor, in, 2, no_ctx).

t_ibsl(_Config) ->
    Ibsl = beam_bifs:ibsl(6),
    FlowIbsl = beam_flow:pipe(beam_flow:new(), in, [Ibsl]),
    {{ok, 128}, no_ctx} = beam_flow:push(FlowIbsl, in, 2, no_ctx).

t_ibsr(_Config) ->
    Ibsr = beam_bifs:ibsr(1),
    FlowIbsr = beam_flow:pipe(beam_flow:new(), in, [Ibsr]),
    {{ok, 1}, no_ctx} = beam_flow:push(FlowIbsr, in, 2, no_ctx).

t_bonot(_Config) ->
    Bonot = beam_bifs:bonot(),
    FlowBonot = beam_flow:pipe(beam_flow:new(), in, [Bonot]),
    {{ok, true}, no_ctx} = beam_flow:push(FlowBonot, in, false, no_ctx),
    {{ok, false}, no_ctx} = beam_flow:push(FlowBonot, in, true, no_ctx).

t_boand(_Config) ->
    Boand = beam_bifs:boand(true),
    FlowBoand = beam_flow:pipe(beam_flow:new(), in, [Boand]),
    {{ok, true}, no_ctx} = beam_flow:push(FlowBoand, in, true, no_ctx),
    {{ok, false}, no_ctx} = beam_flow:push(FlowBoand, in, false, no_ctx).

t_boor(_Config) ->
    Boor = beam_bifs:boor(true),
    FlowBoor = beam_flow:pipe(beam_flow:new(), in, [Boor]),
    {{ok, true}, no_ctx} = beam_flow:push(FlowBoor, in, true, no_ctx),
    {{ok, true}, no_ctx} = beam_flow:push(FlowBoor, in, false, no_ctx).

t_boxor(_Config) ->
    Boxor = beam_bifs:boxor(true),
    FlowBoxor = beam_flow:pipe(beam_flow:new(), in, [Boxor]),
    {{ok, false}, no_ctx} = beam_flow:push(FlowBoxor, in, true, no_ctx),
    {{ok, true}, no_ctx} = beam_flow:push(FlowBoxor, in, false, no_ctx).

%%--------------------------------------------------------------------
%% context propagation
%%--------------------------------------------------------------------
t_ctx_propagation(_Config) ->
    Sink = fun(Var) -> beam_flow:transform(fun(X, Ctx) -> {X, dict:store(Var, X, Ctx)} end, {sink, Var}) end,
    Gt = fun(Const) -> beam_flow:filter(fun(X, _Ctx) -> X>Const end, {gt, Const}) end,
    GtVar = fun(Var) -> beam_flow:filter(fun(X, Ctx) -> X>dict:fetch(Var, Ctx) end, {gt_var, Var}) end,
    F = beam_flow:pipe(beam_flow:new(), source, [Gt(0), Sink(a)]),
    F2 = beam_flow:branch(F, source, next, [GtVar(x), Sink(b)]),
    F3 = beam_flow:branch(F2, next, one_after, [GtVar(y), Sink(c)]),
    InitCtx = dict:from_list([{x, 10}, {y, 20}, {z, -99}]),

    {drop, InitCtx} = beam_flow:push(F, source, -10, InitCtx),

    ExpCtx1 = dict:from_list([{x, 10}, {y, 20}, {z, -99}, {a, 50}]),
    {{ok, 50}, ExpCtx1} = beam_flow:push(F, source, 50, InitCtx),

    {drop, InitCtx} = beam_flow:push(F3, source, 0, InitCtx),

    ExpCtx2 = dict:from_list([{x, 10}, {y, 20}, {z, -99}, {a, 15}, {b, 15}]),
    {branch, ExpCtx2} = beam_flow:push(F3, source, 15, InitCtx),

    ExpCtx3 = dict:from_list([{x, 10}, {y, 20}, {z, -99}, {a, 50}, {b, 50}, {c, 50}]),
    {branch, ExpCtx3} = beam_flow:push(F3, source, 50, InitCtx).

%%--------------------------------------------------------------------
%% audit generation
%%--------------------------------------------------------------------
t_audit(_Config) ->
    Noop = fun(Id) -> beam_flow:transform(fun(X, Ctx) -> {X, Ctx} end, {noop, Id}) end,
    Pass = fun(Id) -> beam_flow:filter(fun(_X, _Ctx) -> true end, {pass, Id}) end,
    F = beam_flow:pipe(beam_flow:new(), source, [Noop(a)]),
    F2 = beam_flow:branch(F, source, next, [Pass(b)]),
    F3 = beam_flow:branch(F2, next, one_after, [Noop(c)]),
    F4 = beam_flow:branch(F3, next, one_after2, [Pass(d)]),

    ExpAudit = [{transform,{noop,a},{ctx,no_ctx},{in,0},{out,0}},
                {filter,{pass,b},{ctx,no_ctx},{in,0},{out,true}},
                {transform,{noop,c},{ctx,no_ctx},{in,0},{out,0}},
                {branch,one_after,{ctx,no_ctx},{in,0},{out,0}},
                {filter,{pass,d},{ctx,no_ctx},{in,0},{out,true}},
                {branch,one_after2,{ctx,no_ctx},{in,0},{out,0}},
                {branch,next,{ctx,no_ctx},{in,0},{out,branch}}],
    {branch, no_ctx, ExpAudit} = beam_flow:push_audit(F4, source, 0, no_ctx).
