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
-export([all/0,
         suite/0,
         init_per_suite/1,
         groups/0
        ]).

%% samples
-export([
          t_sample_usage/1
        , t_sample_branch/1
        , t_sample_union/1
        ]).

%% test cases
-export([
           t_infill_basic/1
         , t_push_basic/1
         , t_filter_pipe_push/1
         , t_transform_pipe_push/1
         , t_drop_ignore/1
        ]).
-export([
           t_eq/1
         , t_neq/1
         , t_lte/1
         , t_lt/1
         , t_gt/1
         , t_gte/1
         , t_seq/1
         , t_sne/1
         , t_uplus/1
         , t_uminus/1
         , t_plus/1
         , t_minus/1
         , t_mul/1
         , t_fdiv/1
         , t_idiv/1
         , t_irem/1
         , t_ibnot/1
         , t_iband/1
         , t_ibor/1
         , t_ibxor/1
         , t_ibsl/1
         , t_ibsr/1
         , t_bonot/1
         , t_boand/1
         , t_boor/1
         , t_boxor/1
        ]).

%% we have warn_unused_import + warnings_as_errors -> bad time without PROPER_NO_IMPORTS
%-define(PROPER_NO_IMPORTS, true).
%
%-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").

%-define(PROPTEST(M,F), true = proper:quickcheck(M:F())).

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
                    , t_drop_ignore
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
    IsEven = beam_flow:filter(Empty, fun(X) -> X rem 2 == 0 end),
    Square = beam_flow:transform(Empty, fun(X) -> X * X end),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    Stream = beam_flow:pipe(Empty, source, [IsEven, Square]),
    %% push events into the stream, even numbers get squared
    4 = beam_flow:push(Stream, source, 2),
    16 = beam_flow:push(Stream, source, 4),
    %% odd numbers get dropped
    drop = beam_flow:push(Stream, source, 3),
    drop = beam_flow:push(Stream, source, 9).

t_sample_branch(_Config) ->
    Empty = beam_flow:new(),
    IsEven = beam_flow:filter(Empty, fun(X) -> X rem 2 == 0 end),
    IsOdd = beam_flow:filter(Empty, fun(X) -> X rem 2 == 1 end),
    Sink1 = beam_flow:filter(Empty,
      fun(X) -> % evens
         0 = X rem 2
      end),
    Sink2 = beam_flow:filter(Empty,
      fun(X) -> % odds
         1 = X rem 2
      end),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    S0 = beam_flow:pipe(Empty, source, []),
    S1 = beam_flow:branch(S0, source, evens, [IsEven, Sink1]),
    S2 = beam_flow:branch(S1, source, odds, [IsOdd, Sink2]), 
    %% push events into the stream, even numbers get squared
    branch = beam_flow:push(S2, source, 2),
    branch = beam_flow:push(S2, source, 4),
    %% odd numbers get dropped
    branch = beam_flow:push(S2, source, 3),
    branch = beam_flow:push(S2, source, 9).

t_sample_union(_Config) ->
    Empty = beam_flow:new(),
    Union = beam_flow:filter(Empty,
      fun(X) ->
         true = erlang:is_number(X)
      end),
    %% configure a simple stream
    %% - create a named pipeline, squaring all even events
    S0 = beam_flow:pipe(Empty, source1, []),
    S1 = beam_flow:pipe(S0, source2, []),
    S2 = beam_flow:pipe(S1, union, [Union]),
    S3 = beam_flow:combine(S2, union, source1, []),
    S4 = beam_flow:combine(S3, union, source2, []),
    branch = beam_flow:push(S4, source1, 1),
    branch = beam_flow:push(S4, source2, 2),
    branch = beam_flow:push(S4, source1, 3),
    branch = beam_flow:push(S4, source2, 4),
    ok.

%%%===================================================================
%%% Individual Test Cases (from groups() definition)
%%%===================================================================
t_infill_basic(_Config) ->
    [] = beam_flow:infill([], boop),
    [boop] = beam_flow:infill([in], boop),
    [boop, beep, boop] = beam_flow:infill([in, beep, in], boop).

t_push_basic(_Config) ->
    Flow = beam_flow:new(),
    IsInt = beam_flow:filter(Flow, erlang, is_integer, [in]),
    FlowInt = beam_flow:pipe(Flow, in, [IsInt]),

    2 = beam_flow:push(FlowInt, in, 2),
    drop = beam_flow:push(FlowInt, in, beep).

t_filter_pipe_push(_Config) ->
    Flow = beam_flow:new(),
    Listify = beam_flow:transform(Flow, erlang, atom_to_list, [in]),
    FlowListify = beam_flow:pipe(Flow, in, [Listify]),

    "beep" = beam_flow:push(FlowListify, in, beep).

t_transform_pipe_push(_Config) ->
    Flow = beam_flow:new(),

    A0 = beam_flow:transform(Flow, erlang, list_to_atom, [in]),
    F0 = beam_flow:pipe(Flow, in, [ A0 ]),

    A1 = beam_flow:filter(Flow, fun(X) -> beep = X, false end),
    F1 = beam_flow:branch(F0, in, b1, [ A1 ]),
    F2 = beam_flow:branch(F1, in, b2, [ A1 ]),
    F3 = beam_flow:combine(F2, union, b1, []),
    F4 = beam_flow:combine(F3, union, b2, []),

    A2 = beam_flow:transform(Flow, fun(X) -> drop = X, ok end),
    F5 = beam_flow:pipe(F4, union, [ A2 ]),

    branch = beam_flow:push(F5, in, "beep").


%%--------------------------------------------------------------------
%% beam_bifs
%%--------------------------------------------------------------------
t_eq(_Config) ->

    F = beam_flow:new(),

    Eq = beam_bifs:eq(F, 2),
    FlowEq = beam_flow:pipe(beam_flow:new(), in, [Eq]),
    drop = beam_flow:push(FlowEq, in, 1),
    2 = beam_flow:push(FlowEq, in, 2).

t_neq(_Config) ->

    F = beam_flow:new(),

    Neq = beam_bifs:neq(F, 2),
    FlowNeq = beam_flow:pipe(beam_flow:new(), in, [Neq]),
    drop = beam_flow:push(FlowNeq, in, 2),
    1 = beam_flow:push(FlowNeq, in, 1).

t_lte(_Config) ->

    F = beam_flow:new(),

    Lte = beam_bifs:lte(F, 2),
    FlowLte = beam_flow:pipe(beam_flow:new(), in, [Lte]),
    drop = beam_flow:push(FlowLte, in, 3),
    2 = beam_flow:push(FlowLte, in, 2),
    1 = beam_flow:push(FlowLte, in, 1).

t_lt(_Config) ->

    F = beam_flow:new(),

    Lt = beam_bifs:lt(F, 2),
    FlowLt = beam_flow:pipe(beam_flow:new(), in, [Lt]),
    drop = beam_flow:push(FlowLt, in, 3),
    drop = beam_flow:push(FlowLt, in, 2),
    1 = beam_flow:push(FlowLt, in, 1).

t_gt(_Config) ->

    F = beam_flow:new(),

    Gt = beam_bifs:gt(F, 2),
    FlowGt = beam_flow:pipe(beam_flow:new(), in, [Gt]),
    drop = beam_flow:push(FlowGt, in, 1),
    drop = beam_flow:push(FlowGt, in, 2),
    3 = beam_flow:push(FlowGt, in, 3).


t_gte(_Config) ->

    F = beam_flow:new(),

    Gte = beam_bifs:gte(F, 2),
    FlowGte = beam_flow:pipe(beam_flow:new(), in, [Gte]),
    drop = beam_flow:push(FlowGte, in, 1),
    2 = beam_flow:push(FlowGte, in, 2),
    3 = beam_flow:push(FlowGte, in, 3).

t_seq(_Config) ->

    F = beam_flow:new(),

    Seq = beam_bifs:seq(F, 2),
    FlowSeq = beam_flow:pipe(beam_flow:new(), in, [Seq]),
    drop = beam_flow:push(FlowSeq, in, 1),
    drop = beam_flow:push(FlowSeq, in, 2.0),
    2 = beam_flow:push(FlowSeq, in, 2).

t_sne(_Config) ->

    F = beam_flow:new(),

    Sne = beam_bifs:sne(F, 2),
    FlowSne = beam_flow:pipe(beam_flow:new(), in, [Sne]),
    drop = beam_flow:push(FlowSne, in, 2),
    2.0 = beam_flow:push(FlowSne, in, 2.0),
    1 = beam_flow:push(FlowSne, in, 1).

t_uplus(_Config) ->

    F = beam_flow:new(),

    Uplus = beam_bifs:uplus(F),
    FlowUplus = beam_flow:pipe(beam_flow:new(), in, [Uplus]),
    2 = beam_flow:push(FlowUplus, in, 2).

t_uminus(_Config) ->

    F = beam_flow:new(),

    Uminus = beam_bifs:uminus(F),
    FlowUminus = beam_flow:pipe(beam_flow:new(), in, [Uminus]),
    -2 = beam_flow:push(FlowUminus, in, 2),
    2 = beam_flow:push(FlowUminus, in, -2).

t_plus(_Config) ->

    F = beam_flow:new(),

    Plus = beam_bifs:plus(F,3),
    FlowPlus = beam_flow:pipe(beam_flow:new(), in, [Plus]),
    6 = beam_flow:push(FlowPlus, in, 3).

t_minus(_Config) ->

    F = beam_flow:new(),

    Minus = beam_bifs:minus(F,3),
    FlowMinus = beam_flow:pipe(beam_flow:new(), in, [Minus]),
    6 = beam_flow:push(FlowMinus, in, 9).

t_mul(_Config) ->

    F = beam_flow:new(),

    Mul = beam_bifs:mul(F,3),
    FlowMul = beam_flow:pipe(beam_flow:new(), in, [Mul]),
    9 = beam_flow:push(FlowMul, in, 3).

t_fdiv(_Config) ->

    F = beam_flow:new(),

    Fdiv = beam_bifs:fdiv(F,3),
    FlowFdiv = beam_flow:pipe(beam_flow:new(), in, [Fdiv]),
    3.0 = beam_flow:push(FlowFdiv , in, 9).

t_idiv(_Config) ->

    F = beam_flow:new(),

    Idiv = beam_bifs:idiv(F,3),
    FlowIdiv = beam_flow:pipe(beam_flow:new(), in, [Idiv]),
    3 = beam_flow:push(FlowIdiv , in, 9).

t_irem(_Config) ->

    F = beam_flow:new(),

    Irem = beam_bifs:irem(F,2),
    FlowIrem = beam_flow:pipe(beam_flow:new(), in, [Irem]),
    1 = beam_flow:push(FlowIrem, in, 1),
    0 = beam_flow:push(FlowIrem, in, 2).

t_ibnot(_Config) ->

    F = beam_flow:new(),

    Ibnot = beam_bifs:ibnot(F),
    FlowIbnot = beam_flow:pipe(beam_flow:new(), in, [Ibnot]),
    -2 = beam_flow:push(FlowIbnot, in, 1),
    0 = beam_flow:push(FlowIbnot, in, -1).

t_iband(_Config) ->

    F = beam_flow:new(),
    
    Iband = beam_bifs:iband(F,6),
    FlowIband = beam_flow:pipe(beam_flow:new(), in, [Iband]),
    2 = beam_flow:push(FlowIband, in, 2).

t_ibor(_Config) ->

    F = beam_flow:new(),

    Ibor = beam_bifs:ibor(F,2),
    FlowIbor = beam_flow:pipe(beam_flow:new(), in, [Ibor]),
    3 = beam_flow:push(FlowIbor, in, 1).

t_ibxor(_Config) ->

    F = beam_flow:new(),

    Ibxor = beam_bifs:ibxor(F,6),
    FlowIbxor = beam_flow:pipe(beam_flow:new(), in, [Ibxor]),
    4 = beam_flow:push(FlowIbxor, in, 2).

t_ibsl(_Config) ->

    F = beam_flow:new(),

    Ibsl = beam_bifs:ibsl(F,6),
    FlowIbsl = beam_flow:pipe(beam_flow:new(), in, [Ibsl]),
    128 = beam_flow:push(FlowIbsl, in, 2).

t_ibsr(_Config) ->

    F = beam_flow:new(),

    Ibsr = beam_bifs:ibsr(F,1),
    FlowIbsr = beam_flow:pipe(beam_flow:new(), in, [Ibsr]),
    1 = beam_flow:push(FlowIbsr, in, 2).

t_bonot(_Config) ->

    F = beam_flow:new(),

    Bonot = beam_bifs:bonot(F),
    FlowBonot = beam_flow:pipe(beam_flow:new(), in, [Bonot]),
    true = beam_flow:push(FlowBonot, in, false),
    false = beam_flow:push(FlowBonot, in, true).

t_boand(_Config) ->

    F = beam_flow:new(),

    Boand = beam_bifs:boand(F,true),
    FlowBoand = beam_flow:pipe(beam_flow:new(), in, [Boand]),
    true = beam_flow:push(FlowBoand, in, true),
    false = beam_flow:push(FlowBoand, in, false).

t_boor(_Config) ->

    F = beam_flow:new(),

    Boor = beam_bifs:boor(F,true),
    FlowBoor = beam_flow:pipe(beam_flow:new(), in, [Boor]),
    true = beam_flow:push(FlowBoor, in, true),
    true = beam_flow:push(FlowBoor, in, false).

t_boxor(_Config) ->

    F = beam_flow:new(),

    Boxor = beam_bifs:boxor(F,true),
    FlowBoxor = beam_flow:pipe(beam_flow:new(), in, [Boxor]),
    false = beam_flow:push(FlowBoxor, in, true),
    true = beam_flow:push(FlowBoxor, in, false).

t_drop_ignore(_Config) ->

    F = beam_flow:new(),

    Audit = spawn(fun audit_loop/0),
    Block1 = beam_flow:filter(F, fun(X) -> Audit ! {add, {block1, X}}, false end),
    Block2 = beam_flow:filter(F, fun(X) -> Audit ! {add, {block2, X}}, false end),
    Passthru1 = beam_flow:transform(F, fun(X) -> Audit ! {add, {passthru1, X}}, X end),
    Passthru2 = beam_flow:transform(F, fun(X) -> Audit ! {add, {passthru2, X}}, X end),
    Passthru3 = beam_flow:transform(F, fun(X) -> Audit ! {add, {passthru3, X}}, X end),
    F2 = beam_flow:pipe(F, source, [Block1, Passthru1]),
    F3 = beam_flow:branch(F2, source, next, [Passthru2, Block2]),
    F4 = beam_flow:branch(F3, next, one_after, [Passthru3]),
    beam_flow:push(F4, source, a),

    % validate
    Audit ! {get, self()},
    Audited = receive {audit, Ops2} -> Ops2 after 1000 -> throw(timeout) end,
    [{block1,a}] = Audited.

audit_loop() -> audit_loop([]).
audit_loop(OpsSoFar) ->
    receive
        close ->
            die;
        {add, Op} ->
            audit_loop(OpsSoFar ++ [Op]);
        {get, Pid} ->
            Pid ! {audit, OpsSoFar},
            audit_loop(OpsSoFar)
    end.
