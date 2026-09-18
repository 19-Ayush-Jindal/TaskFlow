#!/usr/bin/env bash
# TaskFlow experiments. Run one at a time:  bash experiments.sh 3
# Keep `docker compose logs -f` open in a second terminal while you do.

API=${API:-http://localhost:3000}

hr() { printf '\n\033[1m%s\033[0m\n' "$1"; }
die() { printf '\n\033[31mFAILED: %s\033[0m\n' "$1" >&2; exit 1; }

# Nothing below works if the API is down, so check before doing anything.
preflight() {
  if ! curl -sf "$API/health" > /dev/null 2>&1; then
    echo
    echo "The API at $API is not responding."
    echo
    echo "Container status:"
    docker compose ps 2>/dev/null || echo "  (docker compose not reachable from here)"
    echo
    echo "Last 20 lines of api logs:"
    docker compose logs api --tail 20 2>/dev/null || true
    echo
    echo "Most likely fix - you edited files but did not rebuild the image:"
    echo "  docker compose up -d --build --scale worker=3"
    exit 1
  fi
}

# Submits a job and prints its id. Dies with the actual server response
# if anything comes back that is not a job.
post_id() {
  local body resp
  body="$1"
  resp=$(curl -s -X POST "$API/jobs" -H 'content-type: application/json' -d "$body")
  if [ -z "$resp" ]; then
    die "empty response from POST /jobs (is the api container up?)"
  fi
  echo "$resp" | python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    sys.stderr.write('server did not return JSON:\n' + raw[:400] + '\n'); sys.exit(1)
if 'id' not in d:
    sys.stderr.write('server rejected the job:\n' + json.dumps(d, indent=2) + '\n'); sys.exit(1)
print(d['id'])
" || die "could not enqueue job"
}

post() {
  curl -s -X POST "$API/jobs" -H 'content-type: application/json' -d "$1"
}
get() { curl -s "$API/jobs/$1"; }

# Reads one field from a job record, or prints '?' rather than a traceback.
field() {
  get "$1" | python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    print(json.loads(raw).get('$2', '?'))
except Exception:
    print('?')
"
}
id_of() { python3 -c "
import sys, json
try:
    print(json.load(sys.stdin)['id'])
except Exception:
    print('')
"; }

if [ -n "${1:-}" ]; then preflight; fi

case "${1:-}" in

0)
hr "EXPERIMENT 0 - is the system alive and idle?"
echo "health:"; curl -s "$API/health"; echo
echo "known job types:"; curl -s "$API/job-types"; echo
echo "queue depth:"; curl -s "$API/stats"; echo
hr "WHAT THIS PROVES"
cat <<'TXT'
Every count is zero and nothing is running, yet three workers are alive.
They are parked inside BLPOP - a blocking call that consumes no CPU and
no polling. An idle queue costs nothing.
TXT
;;

1)
hr "EXPERIMENT 1 - one job, three workers"
echo "submitting..."
RESP=$(post '{"type":"demo.echo","payload":{"hello":"world"}}')
echo "$RESP"
ID=$(echo "$RESP" | id_of)
sleep 1
hr "the record afterwards"
get "$ID"; echo
hr "WHAT TO LOOK FOR IN THE LOGS"
cat <<'TXT'
Exactly ONE worker printed job.started and job.succeeded.
The other two printed nothing at all.

That silence is the point. Redis keeps a FIFO list of clients blocked on
each key. One LPUSH wakes exactly one of them, atomically, because Redis
runs commands on a single thread. No locks, no leader election, no
coordination protocol - you got work distribution for free.

Note the response was 202, not 200. It came back in ~1ms, before any
work happened. That is the entire purpose of the system.
TXT
;;

2)
hr "EXPERIMENT 2 - watch the state machine"
RESP=$(post '{"type":"demo.slow","payload":{"ms":6000}}')
ID=$(echo "$RESP" | id_of)
echo "job $ID submitted (6 second handler)"
for i in 1 2 3 4 5 6 7 8; do
  STATE=$(get "$ID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['state'], d.get('worker_id') or '')")
  printf '  t=%ss  state=%s\n' "$i" "$STATE"
  sleep 1
done
hr "WHAT THIS PROVES"
cat <<'TXT'
ready -> leased -> succeeded.

'leased' is stamped with worker_id the moment a worker picks the job up.
Right now that stamp is decoration: nothing checks it, nothing expires it.
Step 4 turns it into real ownership with a deadline. Experiment 6 shows
why that matters.
TXT
;;

3)
hr "EXPERIMENT 3 - priority jumps the queue"
echo "filling all 3 workers with 8-second jobs..."
for i in 1 2 3; do post '{"type":"demo.slow","payload":{"ms":8000}}' > /dev/null; done
sleep 1
echo "queueing LOW priority (2) job..."
LOW=$(post '{"type":"demo.echo","payload":{"tag":"LOW"},"priority":2}' | id_of)
echo "queueing HIGH priority (0) job, submitted LAST..."
HIGH=$(post '{"type":"demo.echo","payload":{"tag":"HIGH"},"priority":0}' | id_of)
echo
echo "queue depth right now:"; curl -s "$API/stats"; echo
echo
echo "waiting for a worker to free up..."
sleep 9
echo "  LOW  finished at: $(get "$LOW"  | python3 -c 'import sys,json;print(json.load(sys.stdin)["updated_at"])')"
echo "  HIGH finished at: $(get "$HIGH" | python3 -c 'import sys,json;print(json.load(sys.stdin)["updated_at"])')"
hr "WHAT THIS PROVES"
cat <<'TXT'
HIGH was submitted after LOW but ran first.

There is no sorting code anywhere. Each priority gets its own Redis list,
and the worker calls:
    BLPOP ready:default:0  ready:default:1  ready:default:2  5
BLPOP scans its keys strictly left to right and returns from the first
non-empty one. Priority is a property of the command, not of your logic.
TXT
;;

4)
hr "EXPERIMENT 4 - delayed jobs and the promoter"
RESP=$(post '{"type":"demo.echo","payload":{"late":true},"delay_ms":8000}')
echo "$RESP"
ID=$(echo "$RESP" | id_of)
echo
for i in $(seq 1 11); do
  printf '  t=%2ss  stats=%s  state=%s\n' "$i" \
    "$(curl -s "$API/stats" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("scheduled="+str(d["scheduled"]))')" \
    "$(get "$ID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["state"])')"
  sleep 1
done
hr "WHAT THIS PROVES"
cat <<'TXT'
scheduled -> (8s pass) -> ready -> leased -> succeeded

The job sat in a Redis ZSET scored by run_at. A separate scheduler process
ticks every 200ms and runs one Lua script that moves anything due onto a
ready list. Workers never learned about delays - the promoter feeds the
same pipe they were already reading.

This is why step 3 came before retries: a retry is just a job with
run_at = now + backoff. Same machinery, no new concepts.
TXT
;;

5)
hr "EXPERIMENT 5 - failure is terminal (for now)"
ID=$(post '{"type":"demo.fail"}' | id_of)
sleep 2
get "$ID"; echo
hr "WHAT THIS PROVES"
cat <<'TXT'
state=failed, last_error recorded, attempts still 0.

The job will never run again. max_attempts is 3 but nothing reads it yet.
That is step 5: increment attempts, compute exponential backoff, and
re-enqueue via the scheduled ZSET - or route to the dead-letter queue
once attempts are spent.
TXT
;;

6)
hr "EXPERIMENT 6 - kill -9 a busy worker (THE IMPORTANT ONE)"
ID=$(post '{"type":"demo.slow","payload":{"ms":30000}}' | id_of)
echo "submitted long job: $ID"
sleep 2
OWNER=$(get "$ID" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("worker_id") or "")')
echo "picked up by: $OWNER"
CONTAINER=$(docker compose ps -q worker | while read -r c; do
  if docker logs "$c" 2>&1 | grep -q "$ID"; then echo "$c"; break; fi
done)
echo "that is container: $CONTAINER"
echo
echo "SIGKILLing it..."
docker kill --signal=SIGKILL "$CONTAINER" > /dev/null
sleep 3
hr "the job, 3 seconds after its worker died"
get "$ID"; echo
echo
echo "queue depth (note: job is in NO queue):"; curl -s "$API/stats"; echo
sleep 10
hr "the job, 13 seconds after its worker died"
get "$ID" | python3 -c 'import sys,json;print("state:", json.load(sys.stdin)["state"])'
hr "WHAT THIS PROVES"
cat <<'TXT'
The job is stuck on 'leased' and will stay there forever.

BLPOP removed the id from the ready list. Nothing recorded that anyone
owned it in a way that can expire. The worker holding it is gone. No
process will ever notice. The job silently evaporated - no error, no log
line, no retry.

Right now these two situations are INDISTINGUISHABLE in your database:
  - a worker is busy doing this job
  - a worker died holding this job

That is the hole step 4 closes. Leases give ownership a deadline, so
silence becomes evidence of death instead of evidence of nothing.

Bring the worker back:  docker compose up -d --scale worker=3
TXT
;;

7)
hr "EXPERIMENT 7 - graceful stop vs SIGKILL"
ID=$(post '{"type":"demo.slow","payload":{"ms":10000}}' | id_of)
sleep 2
CONTAINER=$(docker compose ps -q worker | while read -r c; do
  if docker logs "$c" 2>&1 | grep -q "$ID"; then echo "$c"; break; fi
done)
echo "job $ID is running in $CONTAINER"
echo "sending SIGTERM (polite stop)..."
docker stop "$CONTAINER" > /dev/null &
sleep 10
hr "result"
get "$ID" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("state:", d["state"])'
hr "WHAT THIS PROVES"
cat <<'TXT'
The job SUCCEEDED even though its worker was told to shut down.

SIGTERM sets running=false. The loop only checks that flag at the top of
each iteration, so the in-flight job runs to completion first. Compare
with experiment 6, where SIGKILL gave the process no chance to finish.

This is why `docker compose up --scale worker=2` is safe to run against
a busy system: scaling down drains, it does not drop.

Restore:  docker compose up -d --scale worker=3
TXT
;;

8)
hr "EXPERIMENT 8 - multiple schedulers do not double-fire"
echo "clearing old logs by noting the current time..."
SINCE=$(date -u +%Y-%m-%dT%H:%M:%S)
echo "scaling to 3 schedulers..."
docker compose up -d --scale scheduler=3 --scale worker=3
sleep 4
echo
echo "scheduler containers now running:"
docker compose ps scheduler --format '  {{.Name}}  {{.Status}}' 2>/dev/null || docker compose ps scheduler
echo
echo "queueing 5 jobs, all due in 4 seconds..."
IDS=()
for i in 1 2 3 4 5; do
  NEW=$(post_id "{\"type\":\"demo.echo\",\"payload\":{\"n\":$i},\"delay_ms\":4000}")
  IDS+=("$NEW")
  echo "  queued $NEW"
done
echo
echo "waiting for them to fire..."
sleep 8

hr "each job should be 'succeeded'"
for id in "${IDS[@]}"; do
  echo "  $id -> $(field "$id" state)"
done

hr "and each should have been STARTED exactly once"
for id in "${IDS[@]}"; do
  N=$(docker compose logs worker --since "$SINCE" 2>/dev/null | grep -c "job.started.*$id" || true)
  if [ "$N" = "1" ]; then
    printf '  \033[32m%s  started %s time\033[0m\n' "$id" "$N"
  else
    printf '  \033[31m%s  started %s times  <-- DUPLICATE\033[0m\n' "$id" "$N"
  fi
done

hr "how many promote batches each scheduler ran"
docker compose logs scheduler --since "$SINCE" 2>/dev/null \
  | grep 'scheduler.promoted' | sed 's/.*scheduler-1/scheduler/' | wc -l \
  | xargs echo "  total promoted log lines:"

hr "WHAT THIS PROVES"
cat <<'TXT'
Three schedulers ticked against the same ZSET every 200ms. All three saw
the same five due jobs. Each job started exactly once.

The Lua script uses ZREM's return value as an ownership token: removing a
ZSET member is destructive and can only succeed once, so exactly one
scheduler gets 1 and promotes the job. The other two get 0 and skip.

No distributed lock, no leader election. Just the fact that Redis runs
each script atomically on a single thread, so no two scripts are ever
both between the ZREM and the LPUSH.

TRY BREAKING IT: in src/lib/scripts.js change
    if redis.call('ZREM', KEYS[1], id) == 1 then
to
    redis.call('ZREM', KEYS[1], id)
    if true then
then `docker compose up -d --build` and run this again. You will see
DUPLICATE lines in red. Put the guard back afterwards.

Restore:  docker compose up -d --scale scheduler=1 --scale worker=3
TXT
;;

9)
hr "EXPERIMENT 9 - a worker dies and the job comes BACK"
echo "run experiment 6 first if you want the before/after contrast."
echo
ID=$(post_id '{"type":"demo.crash","payload":{"after_ms":2000}}')
echo "submitted $ID - the handler hard-exits the process mid-job,"
echo "so the worker gets no signal, no catch block, no chance to log."
echo
sleep 1
printf '  t=1s   state=%s  owner=%s\n' "$(field "$ID" state)" "$(field "$ID" worker_id)"
sleep 2
printf '  t=3s   state=%s  owner=%s  lease_expires_in=%sms\n' \
  "$(field "$ID" state)" "$(field "$ID" worker_id)" "$(field "$ID" lease_expires_in_ms)"
echo "         that worker is gone. watch the lease count down."
for t in 10 20 30 40; do
  sleep 10
  printf '  t=%ss  state=%s  lease_expires_in=%sms  reclaims=%s\n' \
    "$t" "$(field "$ID" state)" "$(field "$ID" lease_expires_in_ms)" "$(field "$ID" reclaims)"
done
hr "reaper log"
docker compose logs scheduler --tail 50 2>/dev/null | grep 'reaper.reclaimed' | tail -3
hr "who picked it up next"
docker compose logs worker --tail 100 2>/dev/null | grep "job.started.*$ID" | tail -2
hr "WHAT THIS PROVES"
cat <<'TXT'
Compare with experiment 6, where the same crash left the job stuck on
'leased' forever.

Now: the worker stopped renewing its lease. Nothing polled it, nothing
health-checked it, nothing noticed it was gone. The lease simply reached
its deadline, and the reaper found it with one ZRANGEBYSCORE.

Silence past the deadline IS the evidence of death.

Two structures made this possible, and experiment 6 was missing both:
  - an INDEX of in-flight work (the leases ZSET) so a reaper can find it
  - a DEADLINE on the claim so "busy" and "dead" stop looking identical

Note reclaims=1 on the record. Also note the job will crash its new
worker too, and get reclaimed again, forever - nothing caps it yet.
That is step 5.
TXT
;;

10)
hr "EXPERIMENT 10 - a poison job is contained instead of eating every worker"
echo "a poison job kills whichever worker picks it up. before step 5 it did"
echo "that forever. now attempts are capped and it ends up in the DLQ."
echo
ID=$(post_id '{"type":"demo.crash","payload":{"after_ms":1000},"max_attempts":3}')
echo "submitted $ID with max_attempts=3"
echo
for t in 10 20 30 40 50 60 70 80 90; do
  sleep 10
  printf '  t=%2ss  state=%-10s attempts=%s  reclaims=%s\n' "$t" \
    "$(field "$ID" state)" "$(field "$ID" attempts)" "$(field "$ID" reclaims)"
done
hr "reaper decisions"
docker compose logs scheduler --tail 200 2>/dev/null \
  | grep -E 'reaper.retried|reaper.buried' | tail -5 | sed 's/^/  /'
hr "the dead-letter queue"
curl -s 'localhost:3000/dead?limit=5'
echo
hr "workers still standing"
docker compose ps worker --format '  {{.Name}}  {{.Status}}' 2>/dev/null
hr "a healthy job still runs fine"
OK=$(post_id '{"type":"demo.echo","payload":{"fine":true}}')
sleep 2
echo "  $OK -> $(field "$OK" state)"
hr "WHAT THIS PROVES"
cat <<'TXT'
The job took three attempts, each spaced further apart than the last, then
went to the dead-letter queue and stopped. It is now inert: no worker will
touch it again until someone calls the replay endpoint.

Compare with what you saw before step 5: the same job cycling forever,
taking out a worker every 30 seconds until none were left and the queue
stalled with jobs nobody could read.

Note that BOTH failure modes route through the same decision. A handler
that throws and a worker that dies mid-job are the same fact - "attempt N
did not succeed" - and only the detection differs. Backoff matters here
too: without it a poison job comes back instantly and kills the next
worker instantly. Spacing the attempts out lets healthy work flow between
them.

Replay one when you have fixed the cause:
  curl -X POST localhost:3000/jobs/<id>/replay
TXT
;;

11)
hr "EXPERIMENT 11 - recurring jobs, and three schedulers that do not double-fire"
api() { curl -s -X "$1" "$API$2" ${3:+-H 'content-type: application/json' -d "$3"}; }

echo "registering a cron that fires every 5 seconds..."
api POST /crons '{"name":"demo-tick","expression":"*/5 * * * * *","type":"demo.echo","payload":{"from":"cron"}}'
echo
echo "scaling to 3 schedulers - all three will see it as due..."
docker compose up -d --scale scheduler=3 --scale worker=3
sleep 3
SINCE=$(date -u +%Y-%m-%dT%H:%M:%S)
echo
echo "watching for 30 seconds..."
for t in 10 20 30; do
  sleep 10
  printf '  t=%2ss  fire_count=%s\n' "$t" "$(curl -s "$API/crons/demo-tick" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fire_count"])')"
done

hr "did any slot fire twice?"
docker compose logs scheduler --since "$SINCE" 2>/dev/null | grep 'cron.fired' | python3 -c "
import sys, json, collections
slots = collections.Counter()
for line in sys.stdin:
    try: d = json.loads(line[line.index('{'):])
    except Exception: continue
    if d.get('msg') == 'cron.fired': slots[d['slot']] += 1
dupes = {k: v for k, v in slots.items() if v > 1}
print(f'  {len(slots)} distinct slots fired, {len(dupes)} fired more than once')
print('  DUPLICATES:', dupes) if dupes else print('  no duplicates - correct')
"

hr "does the schedule drift?"
docker compose logs scheduler --since "$SINCE" 2>/dev/null | grep 'cron.fired' | python3 -c "
import sys, json
rows = []
for line in sys.stdin:
    try: d = json.loads(line[line.index('{'):])
    except Exception: continue
    if d.get('msg') == 'cron.fired': rows.append(d)
rows.sort(key=lambda d: d['slot'])
for d in rows[:4]:
    print('  slot %s -> next %s   (fired %sms late)' % (d['slot'][-9:-1], d['next'][-9:-1], d['late_by_ms']))
print('  gaps stay exactly 5s apart despite the lateness')
"

hr "pause it without deleting"
api PATCH /crons/demo-tick '{"enabled":false}' > /dev/null
B=$(curl -s "$API/crons/demo-tick" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fire_count"])')
sleep 12
A=$(curl -s "$API/crons/demo-tick" | python3 -c 'import sys,json;print(json.load(sys.stdin)["fire_count"])')
echo "  fire_count while paused: $B -> $A"

hr "clean up"
api DELETE /crons/demo-tick; echo
docker compose up -d --scale scheduler=1 --scale worker=3 > /dev/null

hr "WHAT THIS PROVES"
cat <<'TXT'
A cron is a template plus a schedule. When it fires, the beat enqueues an
ordinary job - which then goes through the same ready lists, leases,
retries and DLQ as anything else. No new execution path.

Two things worth understanding:

CLAIMING. Three schedulers all read the same due definitions. Claiming an
occurrence is a ZREM on the schedule ZSET, so exactly one wins it - the
same compare-and-swap you saw in experiment 8, applied to a third thing.

DRIFT. The next fire time is computed from the SLOT that fired, not from
the moment it actually fired. Every fire above was ~150ms late, and the
gaps stayed exactly 5s. Compute from now instead and a daily 09:00 job
walks forward a little every day until it runs at midnight.

The trade-off: if a scheduler dies between winning a claim and writing the
next fire time, that occurrence is skipped. The roster set plus the orphan
sweep put the definition back so nothing is lost permanently. Never
double-fire, occasionally skip - the right side to err on for most work.
TXT
;;

*)
cat <<'TXT'
TaskFlow experiments - run one at a time:

  bash experiments.sh 0    is it alive and idle?
  bash experiments.sh 1    one job, three workers - who takes it?
  bash experiments.sh 2    watch the state machine transition
  bash experiments.sh 3    priority jumps the queue
  bash experiments.sh 4    delayed jobs and the promoter
  bash experiments.sh 5    failure is terminal (for now)
  bash experiments.sh 6    kill -9 a busy worker  <- the important one
  bash experiments.sh 7    graceful stop vs SIGKILL
  bash experiments.sh 8    multiple schedulers do not double-fire
  bash experiments.sh 9    worker dies, the job comes BACK  <- step 4 payoff
  bash experiments.sh 10   poison job is contained in the DLQ <- step 5 payoff
  bash experiments.sh 11   recurring cron jobs, no double-firing <- step 7

Keep this open in a second terminal:
  docker compose logs -f
TXT
;;
esac
