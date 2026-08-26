#!/usr/bin/env bash
#
# add_missing_empty_reports.sh - append empty reports for topics a run has no
# Report line for, so every run in an archive covers every topic in the topics
# file.
#
# Some released runs skip topics entirely (e.g. rag26 'alex' and 'will' have no
# line for rag2026-113). Downstream, meta-evaluate then only works with
# --on-missing default. Fixing the archive instead: an explicit empty report is
# an honest "this run answered nothing here" and scores 0 by construction.
#
# The appended record matches the archives' round-trip (ragtime26) format:
#
#   {"is_ragtime": true,
#    "metadata": {"team_id": ..., "run_id": ..., "topic_id": ..., "request_id": ...,
#                 "narrative_id": ..., "run_desc": ...},
#    "responses": [], "answer": [], "references": [], "documents": {}}
#
# team_id, run_id and run_desc are copied from the run file's first record; the
# three topic-id mirrors are set to the missing topic id.
#
# Only identifiers and run metadata are read -- report content is never
# inspected or printed.
#
# Usage:
#   ./add_missing_empty_reports.sh --topics topics/queries.jsonl --runs runs/generation           # report only
#   ./add_missing_empty_reports.sh --topics topics/queries.jsonl --runs runs/generation --apply   # append fixes
#
# Requires jq.

set -euo pipefail

TOPICS=""
RUNS=""
APPLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --topics) shift; case "${1:-}" in ""|--*) echo "--topics needs a value (is your variable empty?)" >&2; exit 1;; esac; TOPICS="$1";;
    --runs)   shift; case "${1:-}" in ""|--*) echo "--runs needs a value (is your variable empty?)" >&2; exit 1;; esac; RUNS="$1";;
    --apply)  APPLY=1;;
    -h|--help) grep '^#' "$0" | sed 's/^#\s\?//'; exit 0;;
    *) echo "unknown argument: $1" >&2; exit 1;;
  esac
  shift
done

[ -n "$TOPICS" ] && [ -n "$RUNS" ] || { echo "need --topics and --runs (see --help)" >&2; exit 1; }
[ -f "$TOPICS" ] || { echo "topics file not found: $TOPICS" >&2; exit 1; }
[ -d "$RUNS" ]   || { echo "runs directory not found: $RUNS" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

expected=$(jq -r '.request_id' "$TOPICS" | sort -u)
[ -n "$expected" ] || { echo "no request_id values in $TOPICS" >&2; exit 1; }

fixed=0
for run_file in "$RUNS"/*; do
  [ -f "$run_file" ] || continue
  present=$(jq -r '.metadata.topic_id // .metadata.narrative_id' "$run_file" | sort -u)
  missing=$(comm -23 <(printf '%s\n' "$expected") <(printf '%s\n' "$present"))
  [ -n "$missing" ] || continue

  run_id=$(jq -r '.metadata.run_id' "$run_file" | head -n1)
  team_id=$(jq -r '.metadata.team_id' "$run_file" | head -n1)
  run_desc=$(jq -r '.metadata.run_desc // ""' "$run_file" | head -n1)
  n_missing=$(wc -l <<<"$missing")
  echo "$(basename "$run_file"): missing $n_missing topic(s): $(tr '\n' ' ' <<<"$missing")"

  if [ "$APPLY" -eq 1 ]; then
    # Guard against a run file whose last line has no trailing newline
    [ -z "$(tail -c1 "$run_file")" ] || echo >> "$run_file"
    while IFS= read -r topic_id; do
      jq -cn --arg team "$team_id" --arg run "$run_id" --arg desc "$run_desc" --arg topic "$topic_id" \
        '{is_ragtime: true,
          metadata: {team_id: $team, run_id: $run, topic_id: $topic, request_id: $topic,
                     narrative_id: $topic, run_desc: $desc},
          responses: [], answer: [], references: [], documents: {}}' >> "$run_file"
    done <<<"$missing"
    echo "  -> appended $n_missing empty report(s) to $run_file"
    fixed=$((fixed + 1))
  fi
done

if [ "$APPLY" -eq 1 ]; then
  echo "Done: fixed $fixed run file(s)."
else
  echo "(report only -- re-run with --apply to append the empty reports)"
fi
