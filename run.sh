#!/bin/bash

if [ "$1" == "broadcast" ]; then
  cargo build && ./maelstrom/maelstrom test -w broadcast --bin ./target/debug/maelstrom-tutorial --time-limit 5 --rate 10
else
  echo "unknown command"
fi
