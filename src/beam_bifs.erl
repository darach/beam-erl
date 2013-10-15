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
