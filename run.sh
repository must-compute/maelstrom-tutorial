#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" == "broadcast-2s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 2 --rate 10 --log-stderr
elif [ "$1" = "broadcast-10s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 10 --log-stderr
elif [ "$1" = "broadcast-15s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 15 --log-stderr
elif [ "$1" = "broadcast-20s" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --log-stderr
elif [ "$1" = "broadcast-20s-partition" ]; then
  cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --nemesis partition --topology tree4 --log-stderr
elif [ "$1" = "loop" ]; then
  # while true; do
      # cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 2 --log-stderr | tee >(if [[ $(tail -n 1) == *"good"* ]]; then
      #     echo "Build failed!"
      #     exit 0
      # fi)
      # echo "Build succeeded, running again..."
  # done

  for i in {1..100}; do
      echo "Attempt $i"
      output=$(cargo build && $MAELSTROM test -w broadcast --bin $BINARY --time-limit 20 --nemesis partition --topology tree4 --log-stderr )
      last_line=$(echo "$output" | tail -n 1)

      echo "$last_line"

      if [ $? -ne 0 ] || echo "$last_line" | grep -q "invalid"; then
          echo "Command failed on attempt $i, aborting..."
          echo "Command output was:\n$output"
          exit 1
      fi
  done

  echo "All attempts completed successfully"

  

else
  echo "unknown command"
fi
