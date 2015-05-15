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
-module(beam_bifs2).

%% comparisons
-export([eq/1]).
-export([neq/1]).
-export([lte/1]).
-export([lt/1]).
-export([gt/1]).
-export([gte/1]).
-export([seq/1]).
-export([sne/1]).
%% arithmetics
-export([uplus/0]).
-export([uminus/0]).
-export([plus/1]).
-export([minus/1]).
-export([mul/1]).
-export([fdiv/1]).
-export([idiv/1]).
-export([irem/1]).
%% bitwise
-export([ibnot/0]).
-export([iband/1]).
-export([ibor/1]).
-export([ibxor/1]).
-export([ibsl/1]).
-export([ibsr/1]).
%% boolean
-export([bonot/0]).
-export([boand/1]).
-export([boor/1]).
-export([boxor/1]).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec eq(any()) -> beam_flow2:operator().
eq(Y) -> 
  beam_flow2:filter(fun(X) -> X == Y end, eq).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value not equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec neq(any()) -> beam_flow2:operator().
neq(Y) ->
  beam_flow2:filter(fun(X) -> X /= Y end, neq).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value less than or equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec lte(any()) -> beam_flow2:operator().
lte(Y) ->
  beam_flow2:filter(fun(X) -> X =< Y end, lte).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value less than to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec lt(any()) -> beam_flow2:operator().
lt(Y) ->
  beam_flow2:filter(fun(X) -> X < Y end, lt).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value greater than to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec gt(any()) -> beam_flow2:operator().
gt(Y) ->
  beam_flow2:filter(fun(X) -> X > Y end, gt).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value greater than or equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec gte(any()) -> beam_flow2:operator().
gte(Y) ->
  beam_flow2:filter(fun(X) -> X >= Y end, gte).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value exactly equal to some a priori known value
%% @end
%%--------------------------------------------------------------------
-spec seq(any()) -> beam_flow2:operator().
seq(Y) ->
  beam_flow2:filter(fun(X) -> X =:= Y end, seq).

%%--------------------------------------------------------------------
%% @doc
%% Is the streaming value exactly not equal to some a priori known value
%% @end
-spec sne(any()) -> beam_flow2:operator().
sne(Y) ->
  beam_flow2:filter(fun(X) -> X =/= Y end, sne).

%%--------------------------------------------------------------------
%% @doc
%% Emit the unary plus of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec uplus() -> beam_flow2:operator().
uplus() ->
  beam_flow2:transform(fun(X) -> X end, uplus).

%%--------------------------------------------------------------------
%% @doc
%% Emits the unary minus of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec uminus() -> beam_flow2:operator().
uminus() ->
  beam_flow2:transform(fun(X) -> -X end, uminus).

%%--------------------------------------------------------------------
%% @doc
%% Emit the addition of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec plus(number()) -> beam_flow2:operator().
plus(Y) ->
  beam_flow2:transform(fun(X) -> X + Y end, plus).

%%--------------------------------------------------------------------
%% @doc
%% Emit the subtraction of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec minus(number()) -> beam_flow2:operator().
minus(Y) ->
  beam_flow2:transform(fun(X) -> X - Y end, minus).

%%--------------------------------------------------------------------
%% @doc
%% Emit the multiplication of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec mul(number()) -> beam_flow2:operator().
mul(Y) ->
  beam_flow2:transform(fun(X) -> X * Y end, mul).

%%--------------------------------------------------------------------
%% @doc
%% Emit the floating point division of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec fdiv(float()) -> beam_flow2:operator().
fdiv(Y) ->
  beam_flow2:transform(fun(X) -> X / Y end, fdiv).

%%--------------------------------------------------------------------
%% @doc
%% Emit the integral division of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec idiv(integer()) -> beam_flow2:operator().
idiv(Y) ->
  beam_flow2:transform(fun(X) -> X div Y end, idiv).

%%--------------------------------------------------------------------
%% @doc
%% Emit the integral remainder of the streaming value to an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec irem(integer()) -> beam_flow2:operator().
irem(Y) ->
  beam_flow2:transform(fun(X) -> X rem Y end, irem).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary not of the streaming value
%% @end
%%--------------------------------------------------------------------
-spec ibnot() -> beam_flow2:operator().
ibnot() ->
  beam_flow2:transform(fun(X) -> bnot X end, ibnot).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary and of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec iband(integer()) -> beam_flow2:operator().
iband(Y) ->
  beam_flow2:transform(fun(X) -> X band Y end, iband).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary or of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibor(integer()) -> beam_flow2:operator().
ibor(Y) ->
  beam_flow2:transform(fun(X) -> X bor Y end, ibor).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary xor of the streaming value and an a priori known vlaue
%% @end
%%--------------------------------------------------------------------
-spec ibxor(integer()) -> beam_flow2:operator().
ibxor(Y) ->
  beam_flow2:transform(fun(X) -> X bxor Y end, ibxor).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary shift left of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibsl(integer()) -> beam_flow2:operator().
ibsl(Y) ->
  beam_flow2:transform(fun(X) -> X bsl Y end, ibsl).

%%--------------------------------------------------------------------
%% @doc
%% Emit the binary shift right of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec ibsr(integer()) -> beam_flow2:operator().
ibsr(Y) ->
  beam_flow2:transform(fun(X) -> X bsr Y end, ibsr).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean not of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec bonot() -> beam_flow2:operator().
bonot() ->
  beam_flow2:transform(fun(X) -> not X end, bonot).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean and of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boand(boolean()) -> beam_flow2:operator().
boand(Y) ->
  beam_flow2:transform(fun(X) -> X and Y end, boand).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean or of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boor(boolean()) -> beam_flow2:operator().
boor(Y) ->
  beam_flow2:transform(fun(X) -> X or Y end, boor).

%%--------------------------------------------------------------------
%% @doc
%% Emit the boolean xor of the streaming value and an a priori known value
%% @end
%%--------------------------------------------------------------------
-spec boxor(boolean()) -> beam_flow2:operator().
boxor(Y) ->
  beam_flow2:transform(fun(X) -> X xor Y end, boxor).
