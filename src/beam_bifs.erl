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
%% File: beam_bifs.erl. Builtin flow control functions.
%%
%% -------------------------------------------------------------------
-module(beam_bifs).

%% comparisons
-export([eq/2]).
-export([neq/2]).
-export([lte/2]).
-export([lt/2]).
-export([gt/2]).
-export([gte/2]).
-export([seq/2]).
-export([sne/2]).
%% arithmetics
-export([uplus/1]).
-export([uminus/1]).
-export([plus/2]).
-export([minus/2]).
-export([mul/2]).
-export([fdiv/2]).
-export([idiv/2]).
-export([irem/2]).
%% bitwise
-export([ibnot/1]).
-export([iband/2]).
-export([ibor/2]).
-export([ibxor/2]).
-export([ibsl/2]).
-export([ibsr/2]).
%% boolean
-export([bonot/1]).
-export([boand/2]).
-export([boor/2]).
-export([boxor/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec eq(beam_flow:flow(), any()) -> digraph:vertex().
eq(Flow, Y) -> 
  beam_flow:filter(Flow, fun(X) -> X == Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value not equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec neq(beam_flow:flow(), any()) -> digraph:vertex().
neq(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X /= Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value less than or equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec lte(beam_flow:flow(), any()) -> digraph:vertex().
lte(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X =< Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value less than to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec lt(beam_flow:flow(), any()) -> digraph:vertex().
lt(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X < Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value greater than to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec gt(beam_flow:flow(), any()) -> digraph:vertex().
gt(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X > Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value greater than or equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec gte(beam_flow:flow(), any()) -> digraph:vertex().
gte(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X >= Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value exactly equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec seq(beam_flow:flow(), any()) -> digraph:vertex().
seq(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X =:= Y end).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value exactly not equal to some a priori known value
%% @end
-spec sne(beam_flow:flow(), any()) -> digraph:vertex().
sne(Flow, Y) ->
  beam_flow:filter(Flow, fun(X) -> X =/= Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the unary plus of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec uplus(beam_flow:flow()) -> digraph:vertex().
uplus(Flow) ->
  beam_flow:transform(Flow, fun(X) -> X end).

%%--------------------------------------------------------------------
%% @doc
%% Emits the unary minus of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec uminus(beam_flow:flow()) -> digraph:vertex().
uminus(Flow) ->
  beam_flow:transform(Flow, fun(X) -> -X end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the addition of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec plus(beam_flow:flow(), number()) -> digraph:vertex().
plus(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X + Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the subtraction of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec minus(beam_flow:flow(), number()) -> digraph:vertex().
minus(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X - Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the multiplication of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec mul(beam_flow:flow(), number()) -> digraph:vertex().
mul(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X * Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the floating point division of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec fdiv(beam_flow:flow(), float()) -> digraph:vertex().
fdiv(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X / Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the integral division of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec idiv(beam_flow:flow(), integer()) -> digraph:vertex().
idiv(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X div Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the integral remainder of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec irem(beam_flow:flow(), integer()) -> digraph:vertex().
irem(Flow, Y) ->
  beam_flow:transform(Flow, fun(X) -> X rem Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary not of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec ibnot(beam_flow:flow()) -> digraph:vertex().
ibnot(Flow) ->
  beam_flow:transform(Flow, fun(X) -> bnot X end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary and of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec iband(beam_flow:flow(), integer()) -> digraph:vertex().
iband(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X band Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary or of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibor(beam_flow:flow(), integer()) -> digraph:vertex().
ibor(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X bor Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary xor of the streaming value and an a priori known vlaue
%% @end
%%--------------------------------------------------------------------
-spec ibxor(beam_flow:flow(), integer()) -> digraph:vertex().
ibxor(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X bxor Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary shift left of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibsl(beam_flow:flow(), integer()) -> digraph:vertex().
ibsl(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X bsl Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary shift right of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibsr(beam_flow:flow(), integer()) -> digraph:vertex().
ibsr(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X bsr Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean not of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec bonot(beam_flow:flow()) -> digraph:vertex().
bonot(Flow) ->
  beam_flow:transform(Flow, fun(X) -> not X end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean and of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boand(beam_flow:flow(), boolean()) -> digraph:vertex().
boand(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X and Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean or of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boor(beam_flow:flow(), boolean()) -> digraph:vertex().
boor(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X or Y end).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean xor of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boxor(beam_flow:flow(), boolean()) -> digraph:vertex().
boxor(Flow,Y) ->
  beam_flow:transform(Flow, fun(X) -> X xor Y end).

-ifdef(TEST).

basic_test() ->
  F = beam_flow:new(),

  Eq = eq(F, 2),
  FlowEq = beam_flow:pipe(beam_flow:new(), in, [Eq]),
  ?assertEqual(drop, beam_flow:push(FlowEq, in, 1)),
  ?assertEqual(2, beam_flow:push(FlowEq, in, 2)),

  Neq = neq(F, 2),
  FlowNeq = beam_flow:pipe(beam_flow:new(), in, [Neq]),
  ?assertEqual(drop, beam_flow:push(FlowNeq, in, 2)),
  ?assertEqual(1, beam_flow:push(FlowNeq, in, 1)),

  Lte = lte(F, 2),
  FlowLte = beam_flow:pipe(beam_flow:new(), in, [Lte]),
  ?assertEqual(drop, beam_flow:push(FlowLte, in, 3)),
  ?assertEqual(2, beam_flow:push(FlowLte, in, 2)),
  ?assertEqual(1, beam_flow:push(FlowLte, in, 1)),

  Lt = lt(F, 2),
  FlowLt = beam_flow:pipe(beam_flow:new(), in, [Lt]),
  ?assertEqual(drop, beam_flow:push(FlowLt, in, 3)),
  ?assertEqual(drop, beam_flow:push(FlowLt, in, 2)),
  ?assertEqual(1, beam_flow:push(FlowLt, in, 1)),

  Gt = gt(F, 2),
  FlowGt = beam_flow:pipe(beam_flow:new(), in, [Gt]),
  ?assertEqual(drop, beam_flow:push(FlowGt, in, 1)),
  ?assertEqual(drop, beam_flow:push(FlowGt, in, 2)),
  ?assertEqual(3, beam_flow:push(FlowGt, in, 3)),

  Gte = gte(F, 2),
  FlowGte = beam_flow:pipe(beam_flow:new(), in, [Gte]),
  ?assertEqual(drop, beam_flow:push(FlowGte, in, 1)),
  ?assertEqual(2, beam_flow:push(FlowGte, in, 2)),
  ?assertEqual(3, beam_flow:push(FlowGte, in, 3)),

  Seq = seq(F, 2),
  FlowSeq = beam_flow:pipe(beam_flow:new(), in, [Seq]),
  ?assertEqual(drop, beam_flow:push(FlowSeq, in, 1)),
  ?assertEqual(drop, beam_flow:push(FlowSeq, in, 2.0)),
  ?assertEqual(2, beam_flow:push(FlowSeq, in, 2)),

  Sne = sne(F, 2),
  FlowSne = beam_flow:pipe(beam_flow:new(), in, [Sne]),
  ?assertEqual(drop, beam_flow:push(FlowSne, in, 2)),
  ?assertEqual(2.0, beam_flow:push(FlowSne, in, 2.0)),
  ?assertEqual(1, beam_flow:push(FlowSne, in, 1)),

  Uplus = uplus(F),
  FlowUplus = beam_flow:pipe(beam_flow:new(), in, [Uplus]),
  ?assertEqual(2, beam_flow:push(FlowUplus, in, 2)),

  Uminus = uminus(F),
  FlowUminus = beam_flow:pipe(beam_flow:new(), in, [Uminus]),
  ?assertEqual(-2, beam_flow:push(FlowUminus, in, 2)),
  ?assertEqual(2, beam_flow:push(FlowUminus, in, -2)),

  Plus = plus(F,3),
  FlowPlus = beam_flow:pipe(beam_flow:new(), in, [Plus]),
  ?assertEqual(6, beam_flow:push(FlowPlus, in, 3)),

  Minus = minus(F,3),
  FlowMinus = beam_flow:pipe(beam_flow:new(), in, [Minus]),
  ?assertEqual(6, beam_flow:push(FlowMinus, in, 9)),

  Mul = mul(F,3),
  FlowMul = beam_flow:pipe(beam_flow:new(), in, [Mul]),
  ?assertEqual(9, beam_flow:push(FlowMul, in, 3)),

  Fdiv = fdiv(F,3),
  FlowFdiv = beam_flow:pipe(beam_flow:new(), in, [Fdiv]),
  ?assertEqual(3.0, beam_flow:push(FlowFdiv , in, 9)),

  Idiv = idiv(F,3),
  FlowIdiv = beam_flow:pipe(beam_flow:new(), in, [Idiv]),
  ?assertEqual(3, beam_flow:push(FlowIdiv , in, 9)),

  Irem = irem(F,2),
  FlowIrem = beam_flow:pipe(beam_flow:new(), in, [Irem]),
  ?assertEqual(1, beam_flow:push(FlowIrem, in, 1)),
  ?assertEqual(0, beam_flow:push(FlowIrem, in, 2)),

  Ibnot = ibnot(F),
  FlowIbnot = beam_flow:pipe(beam_flow:new(), in, [Ibnot]),
  ?assertEqual(-2, beam_flow:push(FlowIbnot, in, 1)),
  ?assertEqual(0, beam_flow:push(FlowIbnot, in, -1)),

  Iband = iband(F,6),
  FlowIband = beam_flow:pipe(beam_flow:new(), in, [Iband]),
  ?assertEqual(2, beam_flow:push(FlowIband, in, 2)),

  Ibor = ibor(F,2),
  FlowIbor = beam_flow:pipe(beam_flow:new(), in, [Ibor]),
  ?assertEqual(3, beam_flow:push(FlowIbor, in, 1)),

  Ibxor = ibxor(F,6),
  FlowIbxor = beam_flow:pipe(beam_flow:new(), in, [Ibxor]),
  ?assertEqual(4, beam_flow:push(FlowIbxor, in, 2)),

  Ibsl = ibsl(F,6),
  FlowIbsl = beam_flow:pipe(beam_flow:new(), in, [Ibsl]),
  ?assertEqual(128, beam_flow:push(FlowIbsl, in, 2)),

  Ibsr = ibsr(F,1),
  FlowIbsr = beam_flow:pipe(beam_flow:new(), in, [Ibsr]),
  ?assertEqual(1, beam_flow:push(FlowIbsr, in, 2)),

  Bonot = bonot(F),
  FlowBonot = beam_flow:pipe(beam_flow:new(), in, [Bonot]),
  ?assertEqual(true, beam_flow:push(FlowBonot, in, false)),
  ?assertEqual(false, beam_flow:push(FlowBonot, in, true)),

  Boand = boand(F,true),
  FlowBoand = beam_flow:pipe(beam_flow:new(), in, [Boand]),
  ?assertEqual(true, beam_flow:push(FlowBoand, in, true)),
  ?assertEqual(false, beam_flow:push(FlowBoand, in, false)),

  Boor = boor(F,true),
  FlowBoor = beam_flow:pipe(beam_flow:new(), in, [Boor]),
  ?assertEqual(true, beam_flow:push(FlowBoor, in, true)),
  ?assertEqual(true, beam_flow:push(FlowBoor, in, false)),

  Boxor = boxor(F,true),
  FlowBoxor = beam_flow:pipe(beam_flow:new(), in, [Boxor]),
  ?assertEqual(false, beam_flow:push(FlowBoxor, in, true)),
  ?assertEqual(true, beam_flow:push(FlowBoxor, in, false)).

-endif.
