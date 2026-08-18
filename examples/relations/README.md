# examples/relations — a relation store over the KV, as a paper-log replacement

A worked example, like [`examples/genealogy`](../genealogy) beside it: it adds nothing to the core
library and needs no daemon, `kvfsm`, or wire change. Everything here is built out of primitives
this repo already exposes — `set`, `delete`, `compare`, `listRange`, and the atomic multi-key
`txn` that combines them.

What it implements is a general **entity/relation model** on fixed-width binary keys, and on top of
that a **log book**: numbered pages, numbered lines, every value drawn from a dictionary rather than
written out, every record carrying who wrote it, when, and their signature.

---

## The key

Nine bytes. Always nine bytes.

```
 0        1     2      3      4     5     6      7      8
+--------+-----+------+------+----+-----+------+------+----+
| ns     | log | page | type | id | log | page | type | id |
+--------+-----+------+------+----+-----+------+------+----+
          \________ entity A ________/ \________ entity B ________/
```

* **`ns`** — the namespace byte. `0x10` for records as written, `0x11` for their reverse index,
  `0x12` for the dictionary's presence index.
  (`0x00` and `0x01` are reserved by the core — `shmevent.SystemKeyPrefix` and
  `logrecord.LogKeyPrefix` — and an ordinary `Set` of a key starting with either is rejected
  outright by `pkg/daemon`'s `rejectReservedKey`. These two are plain user-namespace bytes,
  which is exactly why no daemon change was needed.)
* **`log`** — which log book, or which schema version of one.
* **`page`** — the page inside it. For entries this is literally the page of the book; for
  everything else it is just the high byte of the id space.
* **`type`** — what kind of thing this is (field, term, entry, actor…).
* **`id`** — the ordinal on that page: the line number.

Entity B all-zero means the record **declares** A rather than relating it to anything. That is the
whole of the "an object with no relations has a zero second entity" rule.

Because SQLite compares BLOB keys byte-wise (`pkg/store`'s `kv` table keys a BLOB column), the byte
order is the index:

| Question | How it is answered | Cost |
| --- | --- | --- |
| What is A? | read `(A, Zero)` | 1 point read |
| What does A point at? | scan `[A,0.0.0.1] .. [A,255.255.255.255]` | 1 range scan |
| What points at A? | same scan in `ns=0x11` | 1 range scan |
| Every entity of type T on page P | scan `[L,P,T,0]..[L,P,T,255]` — one type is contiguous inside a page because `type` sits above `id` | 1 range scan |
| Every line of page P | the same scan with `T = TypeEntry` | 1 range scan |
| Does this column already have this value? | read `(owner, hash(text))` in `ns=0x12` | 1 point read + 1 confirming read |

No secondary index structure, no length prefixes, no key that is ever a byte-prefix of another key.

## The value

```
[version 1][kind 1][author 4][created 8 BE][nameLen 2][name][dataLen 2][data][sigLen 2][sig]
```

* **`name`** is set only on a declaration. It is the one place a piece of text is ever stored.
* **`author`** is four bytes — an entity, whose own declaration carries the 32-byte Ed25519 public
  key. The same dictionary discipline the model applies to names, applied to identities.
* **`sig`** covers **the key** followed by the body. A signed record therefore cannot be lifted out
  from under one pair of entities and replayed under another — which matters here specifically,
  because every relation is stored twice and each copy is signed for its own key.

## Both directions, atomically

`Store.Link(a, b, …)` writes `(a,b)` under `0x10` **and** `(b,a)` under `0x11` in one `txn`. A
half-existing relation is not a state this store can be in, and "who points at me" costs the same
prefix scan as "what do I point at".

`Journal.Append` goes further: interning the terms happens first (a dictionary entry outlives the
line that first used it), and then **the whole line** — the allocation counter bump, the entry's
declaration, and both directions of every cell — is a single atomic `Apply`. A reader never sees a
half-written line.

## Bounded and resumable scans

`Relations`/`Backlinks` read a whole set, which is right for a line's six cells and wrong for a term
used on every line of a large book. `RelationsRange`/`BacklinksRange` take a `Range` — an inclusive
`From`/`To` on the *far* entity plus a `Limit` — and `Range.Resume(last)` returns the range that
continues just past the last entity a batch returned:

```go
r := relations.Range{Limit: 100}
for {
    lines, err := j.EntriesWithIn(ctx, term, r)
    // ... use lines ...
    next, ok := r.Resume(lines[len(lines)-1])
    if !ok || len(lines) < r.Limit {
        break
    }
    r = next
}
```

A cursor is therefore four bytes a caller can store anywhere, not an opaque key, and a resumed scan
picks up correctly even if relations were added or removed in between.

Narrowing the far entity is a **sub-range scan, not a filter**, because both namespaces order
relations by that entity's own `(log, page, type, id)` bytes. So `EntryPages(5, 7)` — every line on
pages 5 to 7 that used a term — reads only those pages, and for lines, page order is the order they
were written, which makes a page window a time window. `TestEntriesWithInReadsOnlyThePagesAsked`
counts the records the store actually hands back to prove it.

The zero `Range` means everything; `To` being the zero entity means "no upper bound" rather than "up
to Zero", which is unambiguous because Zero is the declaration placeholder and never a relation's
endpoint.

## The log book

| type | what it is |
| --- | --- |
| `TypeField` | a column. Its declaration's name is the heading. |
| `TypeTerm` | one admissible value of one column — a dictionary entry. Its declaration's name is the text, stored exactly once. |
| `TypeEntry` | one line. Carries **no text at all**: only references. |
| `TypeActor` | a person or device that writes lines; its declaration holds the public key. |
| `TypePage` | a page itself — what a sign-off is about (see "Signing a page off"). |
| `TypeAllocator` (`0xFF`) | reserved: the id counter for one `(page, type)`, itself an ordinary declaration. |
| the status marker | `TypeEntry` id 0 — never allocated, never declared, the anchor every struck line points at (see "Corrections"). |
| `TypeChain` (`0xFE`) | reserved: the event anchor at id 0 and the running head at id 1 (see "The chain"). |

| kind | relation |
| --- | --- |
| `KindTermOf` | term → field (its mirror is "list this column's vocabulary") |
| `KindCell` | entry → term (its mirror is "every line that used this term") |
| `KindQuantity` | entry → field, with an 8-byte number in the payload. Numbers are the one thing a dictionary is wrong for. |
| `KindRemark` | entry → field, with free text in the payload — the remarks column (see below). |
| `KindCountersign` | entry → actor, authored *by* that actor: the second signature. |
| `KindPageSignoff` | page → status marker: this page is closed, by whom, with how many lines. |
| `KindFieldState` | field → status marker: whether this column's vocabulary is closed. |
| `KindDerivedFrom` | output unit → input unit (see the genealogy layer) |
| `KindSuperseded` / `KindVoided` | line → status marker, with the replacement (or the reason it was struck) in the payload |
| `KindSupersedes` | replacement line → the line it replaced |
| `KindChainLink` | event anchor → sequence number, carrying that event's digest, tag and subject |

### Remarks: the one exception

Every cell is a reference to a dictionary term — except a remark. `RemarkCell(field, text)` puts
free text in the relation's payload, exactly the way `QuantityCell` puts a number there, because
*"bearing sounded rough on the third pass"* is prose about one line and no vocabulary could have
anticipated it.

It is the one place this package stores text without interning it, and that is deliberate rather
than an oversight: two lines that say the same thing store it twice. Everything else still holds —
the text is signed with the line, covered by the line's chain digest, and never rewritten. A remarks
column simply is not a dictionary, and `TestRemarksAreNotADictionary` states exactly that, including
that a vocabulary value in the same log is still stored exactly once.

## Interning: one value, one entity, no races

A dictionary is only worth having if a value really is stored once — and "look it up, and declare it
if it is missing" is a read-then-write, which two writers can both win.

The presence index closes that. Each piece of text has a **bucket key**, in the same 9-byte shape as
everything else:

```
[0x12][owner 4][first 4 bytes of SHA-256(text)]
```

where `owner` is the field for its terms, and the synthetic `{log, 0, TypeField, 0}` for the column
names themselves. The bucket's value lists the entities whose declared name hashes there — 4 bytes
each, and no text, so the index copies nothing.

* **Lookup** is a point read of the bucket plus one read per candidate to confirm the candidate's
  declared name really is the text being looked for. Four bytes of hash is a *bucket*, not an
  identity, and a scheme that trusted it would eventually return the wrong value for a line of a
  log. `TestInternedLookupIsAPointRead` pins the cost: no range scan, whatever the vocabulary's
  size.
* **Create** is one atomic `Apply` holding the entity's declaration, its `KindTermOf` link, the
  id-counter bump **and** a precondition on the bucket — `OpCompareAbsent` if it is new,
  `OpCompare` against its exact current bytes if this text is joining an existing (collided) one.
* **Losing the race** costs one refused transaction and one re-read. `Store.AllocateWith` retries,
  the retry's callback finds the winner's entity in the bucket, and interning returns *that* — no
  second entity, and no id consumed, because the callback runs before anything is applied.
  `TestInterningLoserAdoptsTheWinnersTerm` drives exactly this path deterministically, by making the
  precondition fail on purpose.
* **Collisions** append rather than overwrite, so both texts keep their own entity and each resolves
  to the right one (`TestInterningSurvivesABucketCollision`).

`Journal.Rename` is how a name changes without the index falling out of step with it: dropping the
entity from its old text's bucket, adding it to the new one, and rewriting the declaration are a
single transaction, each bucket write guarded by a compare against the bytes it was read as, so a
concurrent intern into either bucket makes the rename retry rather than silently swallow the other
writer's entry. When both texts hash into the *same* bucket it becomes one precondition and one
write — two of each would leave the second putting the entity straight back into the bucket the
first took it out of (`TestRenameWithinOneBucket` finds a real 4-byte collision by brute force to
exercise exactly that).

Two things a rename means, and neither is a bug:

* It rewrites what **every line referencing that term says** — the lines hold four bytes, not text.
  Renaming is for fixing a name (a typo, a machine relabelled), never for correcting what a
  particular line recorded; a correction to a line is a new line superseding the old one.
* The old text becomes free, so interning it later mints a fresh, unrelated entity.

The declaration keeps its original `Created` and takes the renamer as its author: a rename records
who last stated what the entity is called, not a new creation. It does not retain the old text
anywhere.

## Closing a vocabulary

Until a column is closed, "controlled vocabulary" means only that values are stored once and
referenced — anybody can still add a word, which for a machine list or a result code is exactly what
a log book's fixed set of admissible entries exists to prevent. `CloseField(field)` makes the set
actual: its existing terms go on working, and no new one may be interned into it (`ErrFieldClosed`).

The refusal is a **precondition inside the transaction that would create the term**, not a check
beside it, so a column closed at that same moment cannot have a value slipped past it.

`ReopenField` exists because a shop really does buy a new machine, and the alternative to reopening
is a second column meaning the same thing. Both acts are chained events, so a vocabulary that was
closed while something was written and reopened afterwards reads that way forever — a closure nobody
could see reversed would not be worth recording.

## Corrections: struck through, never erased

`Journal` has no way to change or delete a line, because a log book does not. A wrong line is
**superseded** by a new one (`Correct`) or **struck through** with nothing in its place (`Void`), and
either way the original stays exactly as written and exactly as readable.

A line's standing is one relation from the line to a fixed marker entity — id 0 of `TypeEntry`, which
`Allocate` never hands out, so it is a free anchor that nothing declares. That single key does three
jobs:

* **It makes striking atomic and once-only.** Claiming it carries an `OpCompareAbsent` precondition
  in the same transaction that writes the replacement line, so two writers correcting one line cannot
  fork it into two competing replacements — the loser gets `ErrAlreadyStruck`, and its own
  replacement line is never written at all (the callback runs before anything applies, so no line
  number is consumed either). `TestCorrectionRaceStrikesOnce` drives that deterministically; with the
  precondition removed it fails, which is how the guard is known to be load-bearing.
* **It makes the correction log one scan.** The marker's backlinks are every strike in the book,
  however many pages it has (`Corrections`) — the audit question a paper log answers only by being
  read cover to cover.
* **It is attributable.** A strike is an ordinary record: author, timestamp, signature.

| call | what it does |
| --- | --- |
| `Correct(superseded, cells…)` | appends the replacement and marks the original superseded, in one transaction |
| `Void(entry, reason)` | strikes a line with no replacement; `reason` is a dictionary term, interned like any other value |
| `Status(entry)` | live / superseded (by what) / voided (why), one point read |
| `Corrections()` | every strike in the log, one scan |
| `Page` / `LivePage` | the page as written (struck lines included) / as it now stands |
| `History(entry)` / `Latest(entry)` | the whole chain of versions, oldest first, from any version |

`Store.Link`/`Unlink` still overwrite and delete — that is the generic layer, and a caller building
something other than a log book may want them. The append-only rule is the journal's.

So a shift log line that on paper reads

```
| Day | Ivanova | Lathe-2 | Turning | Scrap | 3 |
```

is stored as one nameless entry plus five 9-byte cell records pointing at five terms, and one
quantity record. "Ivanova" exists in exactly one record in the whole store no matter how many
thousands of lines she signs — and `TestEveryValueIsStoredExactlyOnce` asserts precisely that, by
sweeping every record in all three namespaces.

The query a paper book cannot answer without being read cover to cover — *every line Ivanova ran on
Lathe-2* — is one prefix scan of the index namespace.

## Countersignatures

`Countersign(entry)` is operator-signs, supervisor-countersigns: the endorsement is a relation from
the line to the endorsing actor, **authored by that actor**, so the signature on the record *is* the
countersignature — there is nothing else it could be and nothing to keep in step. In practice that
means a second `Store`, carrying that person's key and actor entity; whoever holds the key is who
the endorsement is from.

Several people may endorse one line (a supervisor and a QA inspector are different keys and
different records). Nobody may endorse twice, endorse their own line, or endorse a line that no
longer stands — and the last two are guarded by compare preconditions *in the transaction*, not by
the check alone, so a line struck at that exact moment cannot have an endorsement slipped past it.

## Signing a page off

`SignOffPage(page)` is the signature at the foot of the page: who closed it, when, and how many
lines it held. It writes the sign-off and closes the page in one transaction — and the closing is
not a rule this layer enforces on the way in, it is the **allocator's own counter gaining a flag**,
so `Append` simply finds the page full and rolls onto the next one exactly as if it had run out of
line numbers. There is no second code path that could disagree with the first.

A correction to a line on a closed page is still possible: a correction is a new line, and new lines
go where new lines go.

## The chain: what a signature cannot prove

A signature proves a record was not *edited*. It cannot prove one was not *removed* — delete a record
outright and everything left still verifies perfectly, with nothing to say the missing one ever
existed. Tearing a page out is the classic way a paper log gets falsified, and per-record signing
does nothing about it.

So every change to the book goes into **one numbered event stream**, and each event's digest covers
the one before it. There are four kinds of change: a line being written, a line being struck, a line
being countersigned, and a page being signed off.

```
line event:   SHA-256( previous | seq | tag=1 | declaration key | the line's content records )
other events: SHA-256( previous | seq | tag   | the record's key | the record itself )
```

Each event is a link from the event anchor to the entity that spells its sequence number, so the
whole stream is one ordered prefix scan (`Events`, bounded and resumable like any other) and a
missing event is a visible gap rather than an inference. Everything but the first kind is an event
*about* a line rather than a line, which is why they are keyed by sequence: a line has exactly one
write, but it can be countersigned by several people, and no key derived from the subject alone
would be unique.

The log keeps a **running head** — one record with the last digest and the event count — advanced
under a compare precondition in the same transaction as the event itself. So the chain cannot fork,
cannot skip, and cannot half-exist.

Two kinds of event are about records that can be **written again**: a rename, and a vocabulary being
closed or reopened. Digesting the record would be wrong for those — rename a term twice and the first
event's digest would no longer match what is stored, through nobody's fault. So those events carry
their own content in the link instead (`Event.Tail`: the two names a rename moved between, the state
a vocabulary was put into), the digest covers *that*, and one check at the end ties it back to
reality: the **latest** event about a record must agree with the record as it now stands. Together
they say the whole sequence of changes happened, in that order, by those people — and that the
current state is the one the last of them left behind.

Chaining renames matters more than it sounds. A line holds four bytes, not text, so renaming the term
those bytes point at changes what every line using it says. Before, that was signed but not evident;
now it is an event, with both names on the record.

A line's *status* is never inside the line's own digest: a strike is written after the line's digest
is fixed, so including it would make the digest change under it the moment anything was corrected.
(Mutation-tested: put it back and `TestChainVerifiesAWholeBook` fails as soon as anything is
corrected.) Countersignatures are excluded for the same reason. Both are covered as events instead.

`VerifyChain` replays the whole book and returns how many events it checked, or the first point at
which the record stops adding up. Four things beyond the digests have to hold:

| check | what it catches |
| --- | --- |
| every allocated line id has a line event | a line deleted outright, which leaves no digest of its own to fail — and names the line, which a sequence gap cannot |
| every line event has an allocated line id | the opposite splice: a line the allocator never issued |
| sequence numbers run `1..n` | an event removed from the middle |
| the head's count and digest match the replay | an event removed from the *end*, where no later digest is left to break, or the chain rebuilt to hide any of the above |
| the latest rename / vocabulary event matches the record | a name rewritten, or a vocabulary reopened, with no event to say so |

The head is signed like everything else, so shortening the book and rewriting the head to match needs
the signing key — `TestChainCatchesAForgedHead` does exactly that and is caught twice over.

**The cost is a real serialization point.** Every write reads and rewrites one record, so concurrent
writers contend on it and losers retry (bounded, as everywhere else here). A log book has one pen at
a time, which is the same trade the paper original makes.

## Reading it: the page as a page

`RenderPage(page)` lays a page out the way the paper original was — and this matters more than it
sounds, because a log book's whole point is that somebody can be handed it:

```
log 1, page 1 -- 4 lines

  # | shift | operator | machine | operation | result | pieces | remarks                 | signed  | at               | notes
----+-------+----------+---------+-----------+--------+--------+-------------------------+---------+------------------+------------------------
  1 | Day   | Ivanova  | Lathe-2 | Turning   | OK     |    120 |                         | Ivanova | 2026-08-18 08:12 | countersigned by Petrov
(2) | Day   | Ivanova  | Lathe-2 | Turning   | Scrap  |      3 |                         | Ivanova | 2026-08-18 08:12 | superseded by line 4
(3) | Night | Petrov   | Mill-1  | Milling   | OK     |     80 |                         | Ivanova | 2026-08-18 08:12 | voided (duplicate)
  4 | Day   | Ivanova  | Lathe-2 | Turning   | Scrap  |      5 | recount after the shift | Ivanova | 2026-08-18 08:12 | replaces line 2

page closed by Petrov at 2026-08-18 17:12, 4 lines
```

A struck line is shown, not hidden — its number in parentheses, and what happened to it in the last
column — because that is what a strike-through is *for*. `RenderBook` renders every page in order
and ends with the chain's own word on whether the book still adds up:

```
chain: 12 events, verified 2026-08-18 12:00
```

or, when it does not:

```
chain: BROKEN after 0 events -- relations: chain: event 1 (line on 01.01.03.01) does not match its digest ...
```

## Compatibility with `examples/genealogy`

`genealogy.go` here implements the same model that example does — units, executions, `Ancestors`,
`Descendants`, breadth-first, cycle-safe, same `DefaultTraceDepth` of 50 — over relations instead of
`pkg/logrecord` entries. `TestGenealogyMatchesTheLogRecordExample` asserts the same answers its
sibling's own multi-hop test asserts, and `TestLiveGenealogyAgreesWithTheLogRecordExample` runs
**both implementations against the same live raft node** and requires them to agree.

The differences are the point of the exercise:

| | `examples/genealogy` | this |
| --- | --- | --- |
| unit id | repeated as text in every record naming it (comma-joined `related_units`) | interned once, 4 bytes per reference |
| one execution | N independent `LogAppend`s, explicitly *not* atomic | one atomic `Apply` |
| one trace hop | read **all** of a unit's history and parse it | one prefix scan of its edges |
| direction | both, from one keyspace, by re-reading and filtering on role | both, as two prefix scans |
| coexistence | its own `logrecord` kind | ordinary entities in the same journal, so a unit and the operator who signed for it are entities in one space |

Both are client-asserted at the same trust level: a signature says which declared actor wrote a
record, not that the claim in it is true.

## Running it

```bash
go test ./examples/relations/          # everything, including two live single-node cluster tests
go test ./examples/relations/ -run 'TestPaperLog|TestEveryValue' -v
```

The in-memory `Backend` (`relations.Memory()`) needs no cluster at all, which is how a schema gets
worked out before anything is deployed. `relations.CurrentNode()` binds to whichever node `mage use`
selected, and is what the live tests exercise.

---

# Improving this

Ordered by what would bite first in production use. Each item says what it costs to implement.

### 1. Capacity: 255 ids per `(log, page, type)`

`Allocate` already spills onto the next page when one fills, so a type holds ~65k entities per log
byte and ~16.7M across all 256 logs. What is *not* implemented is rolling the **log** byte when the
pages run out — today that returns `ErrTypeSpaceFull`.

**Implementation** (~20 lines): keep a per-`(log, type)` "log is full" marker in the same allocator
scheme, and have `Allocate` walk log bytes the way it currently walks pages. The `Store.Log` field
becomes a starting point rather than a fixed value, and `Journal` grows a `logs []uint8` it scans
across for `List`. Everything else — keys, values, scans — is unchanged, because the log byte is
already the most significant one.

If a single type genuinely needs more than 16M entities, the 9-byte key is the wrong format and the
honest fix is a 17-byte one (two 8-byte entities), not more cleverness inside four bytes.

### 2. Bulk reads are one round trip per pair

This is the sharpest real limit left. `shmevent`'s `listRange` answers with **one** pair per request
(see `shmclient.Session.ScanRange`'s doc comment on why a descending scan is worse still), so
reading a full 255-line page costs ~1,800 IPC round trips. Interning no longer scans (see
"Interning"), and a scan can now be bounded and resumed rather than read whole (see "Bounded and
resumable scans") — but the per-pair cost of the scan itself is unchanged, and reading a page back
still pays it. In-process that is survivable; over the Android IPC path or a browser client it is
not.

Three options, in increasing order of cost:

1. **Client-side page cache** (cheap, no protocol change): `Journal` already caches terms and
   fields; add a page cache keyed by `(log, page)`, and a `Journal.LoadPage` that does the one scan
   and resolves every line from it. Turns a page read into ~1 scan instead of 1 + N.
2. **A batch read `Command`** (moderate, no new wire bytes): register a command through
   `pkg/kvctl/dispatch.go` whose handler does the scan node-side and returns the whole page as one
   payload. This is the path `CLAUDE.md` explicitly recommends over new `Event*` bytes.
3. **A batched `listRange` wire variant** (expensive, permanent): a genuinely new wire-level
   primitive, which every daemon, the hand-mirrored Rust client, and `kvmobile` would have to agree
   on forever. Only worth it if option 2 proves insufficient across all three clients.

### 3. Server-side enforcement

Everything here is client-asserted. Anyone who can write to the node can write any key in `0x10`/
`0x11`, including a well-formed record signed by a key they generated themselves. The signature
proves *which declared actor* wrote a record; nothing proves the actor was entitled to.

To make this real ACL it has to be checked inside `kvfsm`'s `Apply`, where client code cannot forge
it. The right shape — per `CLAUDE.md`'s explicit guidance — is a registered `Command` with a handler
that validates the transaction before it commits, **not** a new `Event*` byte and not a new
`SystemKeyPrefix` kind. Until that exists, treat this the way `examples/genealogy` documents its own
entries: good enough for an audit trail an operator reviews, not for an adversarial writer.

### 4. Bind actors to node identity

`Store` signs with whatever key the caller supplies. A device should instead sign with the identity
the cluster already knows it by: `shmclient.GetPrivateKey` hands a local caller the node's own
Ed25519 key (see `pkg/shmevent`'s trust-boundary note), and the actor declaration can carry the
node's peer id alongside the public key. Then "who wrote this line" resolves to a cluster member
rather than to an unattested key. A `relations.ActorFromNode(ctx, peerID)` constructor is about 30
lines.

### 5. Time

`Created` is the writer's wall clock and nothing more. What *is* cluster-ordered is the allocation:
the counter CAS commits through raft, so line numbers are authoritative and timestamps are advisory.
Worth stating in any UI built on this. If ordering must be defensible, the chain gives it.

### 6. Ports

The codec is dependency-free byte slicing — `entity.go` plus `record.go` is ~250 lines with no
imports beyond the standard library. Porting it to `web-app`'s Rust client or `mobile/kvmobile`
needs no wire change at all, since every operation is an ordinary `set`/`txn`/`listRange` those
clients already have. The one thing a port must copy exactly is the byte order of `Entity` and the
signed-payload definition (`key || body`), which is why both have their own tests here.

### 7. What a Go API cannot fix

Two limits are inherent to the format as specified, and a caller should design around them rather
than expect a later version to remove them:

* An entity id is meaningful only inside its log. Cross-log relations are expressible (the log byte
  is part of both halves of the key) but a scan is always scoped to one log's byte, so a
  "everything about this entity everywhere" query is one scan per log involved.
* A presence bucket holds at most 16,383 entities (the record's 2-byte data length). Reaching that
  needs ~16k texts colliding into one 4-byte bucket, which does not happen by chance; it is a bound
  worth knowing rather than one worth designing around.
* The relation kind is in the value, not the key, so filtering by kind is a client-side pass over a
  scan's results. That is free when fan-out is small (a line has six cells) and linear when it is
  not (a term used by 100k lines). If a high-fan-out entity needs kind-scoped scans, give the
  relation its own namespace byte rather than widening the key — the format has 253 of them left.
