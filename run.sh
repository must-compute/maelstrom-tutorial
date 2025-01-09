#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" == "broadcast-2s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 2 --rate 10
elif [ "$1" = "broadcast-20s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --rate 100
else
  echo "unknown command"
fi
