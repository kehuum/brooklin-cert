#!/usr/bin/env bash

starttmux() {
    echo -n "Please provide a range string [ENTER]: "
    read range

    window=$(tmux display-message -p '#I')
    for i in $(ergo $range); do
        tmux split-window -h "ssh $i"
        tmux select-layout -t :$window tiled > /dev/null
    done
    tmux set-window-option synchronize-panes on > /dev/null
    pane=$(tmux break-pane -P -s 0)
    tmux select-layout -t :$window tiled > /dev/null
    tmux kill-pane -t ${pane}
}

starttmux
