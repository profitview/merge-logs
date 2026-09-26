#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
"""
Merge and de-duplicate split ProfitView log exports.

Background
----------
Logs are exported as "profitview-YYYY-MM-DD-HH-MM-SS-N.log", split into ~10MB
chunks (N = 1..x, chunk 1 oldest). Exports are taken at irregular intervals and
the source tool purges its oldest entries internally, so consecutive exports
overlap heavily: a newer export re-contains a long tail of what an older one
already had.

This tool:
  1. groups files into export *sets*,
  2. orders the sets oldest-first by their first log entry,
  3. streams each set as one logical text stream (chunks concatenated in order,
     because the 10MB split can cut a single entry in half),
  4. parses that stream into entries (a timestamped line plus any following
     continuation lines, since entry payloads can contain newlines),
  5. emits only entries not already emitted, and
  6. writes one clean, chronologically ordered log.

Incremental merging
-------------------
If the output file already exists it is used as the merge *base*: it is read as
the oldest export set, and the newly dropped exports are merged on top of it.
So the usual workflow is simply: drop new exports next to profitview-merged.log,
run the tool again. The result is written to a temporary file and swapped in
atomically, so an interrupted run never damages the existing log. Use --fresh to
ignore an existing output and rebuild from the export files only.

Split output
------------
--split month|quarter|year writes one file per period instead of a single log,
named after the output file: profitview-merged-2026-02.log (month),
profitview-merged-2026-q1.log (quarter), profitview-merged-2026.log (year).
Concatenated in period order the files are byte-identical to the single log.
Every existing merged output next to the output file, whether unsplit or split
at any granularity, is read back as merge base, so the granularity can be
changed between runs.

Usage
-----
  python merge_logs.py                          # current dir -> profitview-merged.log
  python merge_logs.py . old -o merged.log      # several input dirs
  python merge_logs.py --split month            # profitview-merged-YYYY-MM.log files
  python merge_logs.py --fresh                  # ignore existing output, rebuild
  python merge_logs.py logs/ -o out.log --report report.txt
  python merge_logs.py --dry-run                # analyse only, write nothing
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import sys
import time
from collections import Counter
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Format constants
# ---------------------------------------------------------------------------

# "profitview-2026-03-11-19-22-11-1.log" -> export stamp, chunk number
FILENAME_RE = re.compile(
    r"^(?P<prefix>.+?)-(?P<stamp>\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})-(?P<chunk>\d+)\.log$",
    re.IGNORECASE,
)

# "2025-12-11 19:43:15.556 DEBUG ..." -> start of a new entry.
# Anything not matching this is a continuation of the previous entry.
ENTRY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) ")

# Length of the "YYYY-MM-DD HH:MM:SS.mmm " prefix; the rest is the entry body.
TS_PREFIX_LEN = 24

# The exporter renders *historical* timestamps using the UTC offset in effect at
# EXPORT time, not at event time. An export taken during CEST therefore shows an
# event from the previous winter one hour later than an export of the same event
# taken during CET. Comparing raw timestamps across such a pair silently fails,
# so the shift is measured from content and removed before de-duplicating.
MAX_SHIFT_HOURS = 6

# Observed backward jitter between concurrently-logging threads is well under a
# second (max ~0.63s, median ~8ms in this corpus). Around a set boundary we fall
# back to exact content comparison within this window instead of trusting the
# ordering, so that genuinely-new entries which happen to jitter backwards are
# not mistaken for duplicates.
JITTER_WINDOW = timedelta(seconds=5)

# A previously merged log used as a base can contain a DST seam where the clock
# jumps backwards by an hour. A single export never jumps back this far, so a
# drop larger than this marks such a seam.
SEAM_JUMP = timedelta(minutes=30)

DEFAULT_OUTPUT = "profitview-merged.log"

# Output split granularities; "none" writes a single file.
SPLIT_CHOICES = ("none", "month", "quarter", "year")

# Read/write settings that make the pass byte-exact: surrogateescape round-trips
# bytes that are not valid UTF-8, newline="" keeps original CRLF line endings.
IO_KW = dict(encoding="utf-8", errors="surrogateescape", newline="")


def parse_ts(text):
    """Parse "YYYY-MM-DD HH:MM:SS.mmm" by slicing.

    The format is fixed, so this is several times faster than strptime, which
    otherwise dominates the runtime (one call per entry, millions per run).
    """
    return datetime(
        int(text[0:4]), int(text[5:7]), int(text[8:10]),
        int(text[11:13]), int(text[14:16]), int(text[17:19]),
        int(text[20:23]) * 1000,
    )


# ---------------------------------------------------------------------------
# Console progress
# ---------------------------------------------------------------------------


class Progress:
    """Single-line progress bar on stderr, redrawn in place.

    Dependency-free on purpose. Draws nothing when stderr is not a terminal, so
    output redirected to a file stays free of carriage-return noise.
    """

    REFRESH = 0.1  # seconds between redraws

    def __init__(self, label, total, enabled=True):
        self.label = label
        self.total = max(total, 1)
        self.done = 0
        self.enabled = enabled and sys.stderr.isatty()
        self.start = time.monotonic()
        self._last_draw = 0.0
        # Legacy Windows code pages cannot encode block characters.
        try:
            "\u2588\u2591".encode(sys.stderr.encoding or "ascii")
            self.fill, self.empty = "\u2588", "\u2591"
        except (UnicodeEncodeError, LookupError):
            self.fill, self.empty = "#", "-"

    def advance(self, n):
        self.done += n
        if self.enabled:
            now = time.monotonic()
            if now - self._last_draw >= self.REFRESH:
                self._last_draw = now
                self._draw(now)

    def write(self, message):
        """Print a message above the bar without garbling it."""
        if not self.enabled:
            print(message, file=sys.stderr)
            return
        cols = shutil.get_terminal_size((80, 20)).columns
        sys.stderr.write("\r" + message.ljust(cols - 1) + "\n")
        self._draw(time.monotonic())

    def close(self):
        """Mark complete (scans may legitimately stop early) and end the line."""
        self.done = self.total
        if self.enabled:
            self._draw(time.monotonic(), final=True)
            sys.stderr.write("\n")
            sys.stderr.flush()

    def _draw(self, now, final=False):
        frac = min(self.done / self.total, 1.0)
        elapsed = now - self.start
        if final:
            tail = f"{human(self.total):>8}  done in {clock(elapsed)}"
        else:
            rate = self.done / elapsed if elapsed > 0 else 0
            eta = (self.total - self.done) / rate if rate else None
            tail = (
                f"{human(self.done):>8}/{human(self.total)}  "
                f"{human(rate):>8}/s  ETA {clock(eta)}"
            )
        stats = f" {frac:6.1%}  {tail}"
        cols = shutil.get_terminal_size((80, 20)).columns
        # Size from the widest (in-progress) stats text so the bar keeps its
        # width when the final "done" line replaces it.
        width = max(10, min(40, cols - len(self.label) - 60))
        filled = int(width * frac)
        bar = self.fill * filled + self.empty * (width - filled)
        line = f"  {self.label} |{bar}|{stats}"
        # Pad instead of using ANSI erase codes: legacy Windows consoles
        # print those literally.
        sys.stderr.write("\r" + line[: cols - 1].ljust(cols - 1))
        sys.stderr.flush()


def clock(seconds):
    if seconds is None:
        return "--:--"
    m, sec = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m:02d}:{sec:02d}"


# ---------------------------------------------------------------------------
# Discovery: files -> export sets
# ---------------------------------------------------------------------------


class ExportSet:
    """One export: the ordered chunks that together form one logical stream."""

    def __init__(self, files):
        self.files = files  # list of (export_stamp, chunk_no, path)
        self.first_ts = None  # timestamp of first entry (filled in later)
        self.last_ts = None  # timestamp of last entry actually read
        self.kept = 0
        self.dropped = 0
        self.orphan_head_lines = 0
        self.chunk_first_ts = []  # first entry timestamp of each chunk
        self.shift = 0  # hours this set is rendered ahead of the previous set
        self.offset = 0  # hours ahead of the first set (cumulative)
        self.shift_confidence = None  # share of votes for the winning shift
        self.name = None  # display label override

    @property
    def paths(self):
        return [p for _, _, p in self.files]

    @property
    def export_stamp(self):
        return self.files[0][0]

    @property
    def label(self):
        return self.name or os.path.basename(self.paths[0])

    def total_bytes(self):
        return sum(os.path.getsize(p) for p in self.paths)


def discover_files(inputs):
    """Collect log files from the given files/directories."""
    found = []
    skipped = []
    for item in inputs:
        if os.path.isdir(item):
            candidates = [os.path.join(item, n) for n in sorted(os.listdir(item))]
        else:
            candidates = [item]
        for path in candidates:
            if not os.path.isfile(path):
                continue
            m = FILENAME_RE.match(os.path.basename(path))
            if not m:
                if path.lower().endswith(".log"):
                    skipped.append(path)
                continue
            stamp = datetime.strptime(m.group("stamp"), "%Y-%m-%d-%H-%M-%S")
            found.append((stamp, int(m.group("chunk")), os.path.abspath(path)))

    # De-duplicate identical paths supplied twice (e.g. overlapping arguments).
    found = sorted(set(found))
    return found, skipped


def group_into_sets(files):
    """Group chunk files into export sets.

    The export stamp is written per-chunk as each chunk is flushed, so it drifts
    by a second or two *within* one export (…-19-22-11-1, …-19-22-12-2,
    …-19-22-13-3). Grouping on the stamp string alone would therefore shred a
    single export into several fragments. Chunk numbering restarting at 1 is the
    reliable set delimiter; a large stamp gap or a break in the numbering also
    starts a new set, so malformed input degrades gracefully instead of merging
    two unrelated exports.
    """
    sets = []
    current = []
    for entry in files:
        stamp, chunk, _ = entry
        if current:
            prev_stamp, prev_chunk, _ = current[-1]
            new_set = (
                chunk == 1
                or chunk != prev_chunk + 1
                or (stamp - prev_stamp) > timedelta(minutes=5)
            )
            if new_set:
                sets.append(ExportSet(current))
                current = []
        current.append(entry)
    if current:
        sets.append(ExportSet(current))
    return sets


# ---------------------------------------------------------------------------
# Parsing: chunk files -> entries
# ---------------------------------------------------------------------------


def iter_lines(paths, progress=None):
    """Yield every line of the chunks as one continuous stream.

    Chunks are concatenated *before* entry parsing because a 10MB split can fall
    in the middle of a multi-line entry; parsing each chunk in isolation would
    truncate that entry and orphan its tail.
    """
    for path in paths:
        with open(path, "r", **IO_KW) as fh:
            if progress is None:
                yield from fh
                continue
            # Count characters in batches (cheap) and correct to the exact byte
            # size at end of file, so multi-byte text cannot skew the total.
            counted = pending = 0
            for line in fh:
                pending += len(line)
                if pending >= 1 << 18:
                    progress.advance(pending)
                    counted += pending
                    pending = 0
                yield line
            progress.advance(os.path.getsize(path) - counted)


def iter_entries(paths, progress=None):
    """Yield (timestamp, text) per log entry.

    `text` is the raw entry including its trailing newline(s) and every
    continuation line, so writing it back reproduces the original bytes.

    Yields (None, text) once at the start if the stream begins with continuation
    lines, i.e. an entry whose head was purged or lost. The caller decides what
    to do with such an orphan fragment.
    """
    ts = None
    buf = []
    for line in iter_lines(paths, progress):
        m = ENTRY_RE.match(line)
        if m:
            if buf:
                yield ts, "".join(buf)
            ts = parse_ts(m.group(1))
            buf = [line]
        else:
            if not buf and ts is None:
                # Orphan fragment before the first real entry.
                yield None, line
                continue
            buf.append(line)
    if buf:
        yield ts, "".join(buf)


def body_hash(text):
    """Hash an entry ignoring its timestamp prefix.

    The timestamp text is exactly what differs between two exports of the same
    event across a DST boundary, so identity has to be judged on the body.
    """
    body = text[TS_PREFIX_LEN:].encode("utf-8", "surrogateescape")
    return hashlib.blake2b(body, digest_size=8).digest()


def detect_shift(prev, cur, probe_hours=48, max_probe=200000, progress=None):
    """Measure how many hours `cur` renders timestamps ahead of `prev`.

    Takes the earliest entries of `cur` (which lie inside the overlap with
    `prev`), looks the same bodies up in `prev`, and takes the modal difference
    rounded to whole hours. Content-based, so it needs no timezone database and
    validates itself: a confident result means the two exports really do
    describe the same events.

    Only bodies that occur *exactly once* in the probe window are used as
    anchors. Logs are full of boilerplate ("Saving local data...") that recurs
    hundreds of times; such lines match at essentially every offset and, left
    in, bury the true alignment under noise.

    `progress`, if given, is sized to the bytes of `prev` from the skipped-to
    chunk onward and advanced while scanning them.

    Returns (shift_hours, confidence) or (None, 0.0) if the two sets share no
    recognisable content.
    """
    seen = {}
    probe_first = probe_last = None
    count = 0
    for ts, text in iter_entries(cur.paths):
        if ts is None:
            continue
        if probe_first is None:
            probe_first = ts
        if ts > probe_first + timedelta(hours=probe_hours) or count >= max_probe:
            break
        probe_last = ts
        count += 1
        seen.setdefault(body_hash(text), []).append(ts)

    # Keep only unambiguous anchors.
    probe = {h: v for h, v in seen.items() if len(v) == 1}
    if not probe:
        return None, 0.0

    # Skip whole chunks of `prev` that end before the probe window can start.
    # Chunk heads are already indexed, so this costs nothing and usually reduces
    # the scan to a single 10MB chunk instead of the whole export.
    target = probe_first - timedelta(hours=MAX_SHIFT_HOURS)
    start = 0
    for i, fts in enumerate(prev.chunk_first_ts):
        if fts is not None and fts <= target:
            start = i

    horizon = probe_last + timedelta(hours=MAX_SHIFT_HOURS)
    votes = Counter()
    if progress is not None:
        progress.total = max(sum(os.path.getsize(p) for p in prev.paths[start:]), 1)
    for ts, text in iter_entries(prev.paths[start:], progress):
        if ts is None:
            continue
        if ts > horizon:
            break
        matches = probe.get(body_hash(text))
        if matches:
            delta = (matches[0] - ts).total_seconds() / 3600.0
            if abs(delta) <= MAX_SHIFT_HOURS:
                votes[round(delta)] += 1

    if not votes:
        return None, 0.0
    shift, top = votes.most_common(1)[0]
    return shift, top / sum(votes.values())


def peek_first_timestamp(paths, max_lines=100000):
    """Read just enough of a set's first chunk to find its first entry time."""
    with open(paths[0], "r", **IO_KW) as fh:
        for i, line in enumerate(fh):
            m = ENTRY_RE.match(line)
            if m:
                return parse_ts(m.group(1))
            if i >= max_lines:
                break
    return None


# ---------------------------------------------------------------------------
# Output: single file or one file per period
# ---------------------------------------------------------------------------


def period_of(ts, split):
    """Sortable period key of a timestamp: (year, month|quarter|0)."""
    if split == "month":
        return (ts.year, ts.month)
    if split == "quarter":
        return (ts.year, (ts.month - 1) // 3 + 1)
    return (ts.year, 0)


def split_path(abs_out, split, period):
    """"x/profitview-merged.log" -> "x/profitview-merged-2026-02.log" etc."""
    year, sub = period
    if split == "month":
        tag = f"{year:04d}-{sub:02d}"
    elif split == "quarter":
        tag = f"{year:04d}-q{sub}"
    else:
        tag = f"{year:04d}"
    stem, ext = os.path.splitext(abs_out)
    return f"{stem}-{tag}{ext}"


def find_merged_outputs(abs_out):
    """Existing outputs of earlier merges, as {split: [paths, oldest first]}.

    Covers the unsplit file ("none") and split files of every granularity, so
    a run can pick up where an earlier one left off even if it used a different
    --split setting.
    """
    folder, name = os.path.split(abs_out)
    stem, ext = os.path.splitext(name)
    pattern = re.compile(
        re.escape(stem)
        + r"-(?P<year>\d{4})(?:-(?P<month>0[1-9]|1[0-2])|-q(?P<quarter>[1-4]))?"
        + re.escape(ext)
        + "$",
        re.IGNORECASE,
    )
    found = {}
    if os.path.isfile(abs_out):
        found["none"] = [((0, 0), abs_out)]
    for n in os.listdir(folder):
        m = pattern.match(n)
        path = os.path.join(folder, n)
        if not m or not os.path.isfile(path):
            continue
        year = int(m.group("year"))
        if m.group("month"):
            split, period = "month", (year, int(m.group("month")))
        elif m.group("quarter"):
            split, period = "quarter", (year, int(m.group("quarter")))
        else:
            split, period = "year", (year, 0)
        found.setdefault(split, []).append((period, path))
    return {split: [p for _, p in sorted(items)] for split, items in found.items()}


class OutputWriter:
    """Routes merged entries to the output file, or to one file per period.

    Every file is first written as "<name>.tmp" and only swapped in by
    commit(), so an interrupted run leaves all existing outputs untouched.

    The current period never moves backwards. Thread jitter and DST seams can
    put an entry's clock slightly before the previous one; such an entry stays
    in the current file rather than reopening the previous period. That keeps
    every file contiguous, so the split files concatenated in period order are
    exactly the single merged log, and they can be read back as a merge base.
    """

    def __init__(self, abs_out, split, dry_run):
        self.abs_out = abs_out
        self.split = split
        self.dry_run = dry_run
        self.outputs = []  # [final_path, entries_written] in write order
        self._period = None
        self._fh = None
        self._pending = []  # head-less fragments seen before any file is open
        if split == "none":
            self._open(abs_out)

    def _open(self, final_path):
        self.close()
        self.outputs.append([final_path, 0])
        if not self.dry_run:
            self._fh = open(final_path + ".tmp", "w", **IO_KW)

    def write(self, ts, text):
        """Write one entry; `ts` is None for a head-less fragment."""
        if self.split != "none" and ts is not None:
            period = period_of(ts, self.split)
            if self._period is None or period > self._period:
                self._period = period
                self._open(split_path(self.abs_out, self.split, period))
                for fragment in self._pending:
                    self._emit(fragment)
                self._pending = []
        if not self.outputs:
            self._pending.append(text)
            return
        self._emit(text)

    def _emit(self, text):
        self.outputs[-1][1] += 1
        if self._fh is not None:
            self._fh.write(text)

    def close(self):
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def commit(self):
        self.close()
        if not self.dry_run:
            for final_path, _ in self.outputs:
                os.replace(final_path + ".tmp", final_path)

    def abort(self):
        self.close()
        for final_path, _ in self.outputs:
            tmp = final_path + ".tmp"
            if os.path.exists(tmp):
                os.remove(tmp)


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def merge(sets, out, verbose=True, progress=None):
    """Write de-duplicated entries from `sets` (oldest first) to `out`.

    `out` is an OutputWriter; it receives each entry's timestamp as well so it
    can route the entry to its period's file.

    De-duplication strategy
    -----------------------
    Each set is internally chronological, and sets are processed oldest-first,
    so an entry is a duplicate exactly when it was already emitted. Two rules
    combine:

      * `ts < watermark - JITTER_WINDOW`  -> duplicate, drop. This covers the
        bulk of an overlap (often weeks of entries) at zero memory cost.
      * `ts >= watermark - JITTER_WINDOW` -> compare the entry's body against
        the entries already emitted inside that window.

    All comparisons use *normalised* timestamps (the rendered timestamp minus
    the set's detected DST offset) so that two exports of the same event line up
    even when they were taken under different UTC offsets. The text written out
    is always the original, unmodified entry.

    The second rule exists because timestamps carry sub-second backward jitter
    from concurrent logger threads. Right where an older export ends, a new
    export's genuinely-new entries can carry a timestamp slightly below the
    watermark; a pure watermark would discard them. Holding the recent window's
    exact text keeps the boundary lossless while memory stays bounded (only the
    trailing few seconds are retained, not the whole corpus).
    """
    watermark = None  # highest timestamp emitted so far
    window = []  # [(ts, hash)] for emitted entries within JITTER_WINDOW
    window_hashes = set()

    total_kept = 0
    total_dropped = 0
    gaps = []

    for idx, s in enumerate(sets):
        prev_watermark = watermark

        # Freeze the comparison baseline at the set boundary. Duplicates only
        # ever come from a *previous* export, so entries this set emits must not
        # feed back into its own duplicate test - otherwise a set that logs the
        # same line twice in the same millisecond would lose the second copy.
        boundary_watermark = watermark
        boundary_hashes = set(window_hashes)
        shift = timedelta(hours=s.offset)

        for ts, text in iter_entries(s.paths, progress):
            if ts is None:
                # Head-less fragment at the very start of a set: its parent
                # entry was purged by the source tool, so there is nothing to
                # attach it to. Emit it only if nothing has been written yet.
                s.orphan_head_lines += 1
                if watermark is None:
                    out.write(None, text)
                    total_kept += 1
                continue

            if s.first_ts is None:
                s.first_ts = ts
            s.last_ts = ts

            # Normalised time: comparable across exports taken under different
            # UTC offsets. Only ever used for decisions, never written out.
            key = ts - shift

            if boundary_watermark is not None:
                if key < boundary_watermark - JITTER_WINDOW:
                    s.dropped += 1
                    total_dropped += 1
                    continue
                if key <= boundary_watermark and body_hash(text) in boundary_hashes:
                    s.dropped += 1
                    total_dropped += 1
                    continue

            out.write(ts, text)
            s.kept += 1
            total_kept += 1

            # Track the trailing jitter window of what we have emitted.
            if watermark is None or key > watermark:
                watermark = key
            elif key < watermark - SEAM_JUMP:
                # Backward DST seam inside a merged base log: the entries after
                # it are the newer ones, so follow them rather than the maximum.
                # Otherwise the next set's first hour would look like duplicates.
                watermark = key
                window.clear()
                window_hashes = set()
            h = body_hash(text)
            window.append((key, h))
            window_hashes.add(h)
            if len(window) > 512:  # amortise the trim
                cutoff = watermark - JITTER_WINDOW
                keep_from = 0
                for i, (wts, _) in enumerate(window):
                    if wts >= cutoff:
                        keep_from = i
                        break
                else:
                    keep_from = len(window)
                if keep_from:
                    del window[:keep_from]
                    # Rebuild rather than discarding individually: a hash may be
                    # shared between a dropped and a retained entry.
                    window_hashes = {h for _, h in window}

        # A coverage gap means the source tool purged entries that no export
        # captured - real data loss in the inputs, not something we can fix.
        if prev_watermark is not None and s.first_ts is not None:
            first_key = s.first_ts - shift
            if first_key > prev_watermark + timedelta(seconds=1):
                gaps.append((prev_watermark, first_key, s.label))

        if verbose:
            span = "empty"
            if s.first_ts and s.last_ts:
                span = f"{s.first_ts:%Y-%m-%d %H:%M} .. {s.last_ts:%Y-%m-%d %H:%M}"
            line = (
                f"  [{idx + 1:2d}/{len(sets)}] {s.label:<42} "
                f"{len(s.files)} chunk(s)  {span}  "
                f"kept {s.kept:>7,}  dropped {s.dropped:>7,}"
            )
            if progress is not None:
                progress.write(line)
            else:
                print(line, file=sys.stderr)

    return total_kept, total_dropped, gaps


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Merge overlapping split ProfitView logs into one clean file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "inputs",
        nargs="*",
        default=["."],
        help="log files and/or directories to scan (default: current directory)",
    )
    ap.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT,
        help=(
            f"output file (default: {DEFAULT_OUTPUT}); if it already exists it is "
            "used as the base and the new exports are merged into it"
        ),
    )
    ap.add_argument(
        "--split",
        choices=SPLIT_CHOICES,
        default="none",
        help=(
            "write one file per period instead of a single log, named e.g. "
            "profitview-merged-2026-02.log (month), profitview-merged-2026-q1.log "
            "(quarter) or profitview-merged-2026.log (year); default: none"
        ),
    )
    ap.add_argument(
        "--fresh",
        action="store_true",
        help="ignore an existing output file instead of merging into it",
    )
    ap.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="analyse and report only; do not write the output file",
    )
    ap.add_argument(
        "--report",
        metavar="PATH",
        help="also write the summary to this file",
    )
    ap.add_argument(
        "--no-detect-shift",
        action="store_true",
        help=(
            "do not detect DST/timezone re-rendering between exports; compare raw "
            "timestamps only (faster, but misses duplicates across a DST boundary)"
        ),
    )
    ap.add_argument(
        "-q", "--quiet", action="store_true", help="suppress per-set output and progress bars"
    )
    ap.add_argument(
        "--no-progress", action="store_true", help="do not draw progress bars"
    )
    args = ap.parse_args(argv)

    inputs = args.inputs or ["."]

    abs_out = os.path.abspath(args.output)
    existing = find_merged_outputs(abs_out)
    existing_paths = {os.path.normcase(p) for paths in existing.values() for p in paths}
    files, skipped = discover_files(inputs)
    # Without new exports an existing merged output can still be re-split.
    if not files and (args.fresh or not existing):
        print("No matching log files found.", file=sys.stderr)
        return 1
    for path in skipped:
        if os.path.normcase(os.path.abspath(path)) not in existing_paths:
            print(f"note: ignoring unrecognised filename {path}", file=sys.stderr)

    # Guard against reading and writing the same file.
    if any(abs_out == p for _, _, p in files):
        print(
            f"error: output {args.output} is also an input file; choose another name.",
            file=sys.stderr,
        )
        return 1

    sets = group_into_sets(files)

    # An existing output is the result of an earlier merge: treat it as one more
    # (normally the oldest) export set, so new exports are merged on top of it.
    # Split files are consecutive slices of one merged stream, so each split
    # granularity forms one set with its files as the chunks, oldest first.
    bases = []
    if not args.fresh:
        for split in SPLIT_CHOICES:
            paths = existing.get(split)
            if not paths:
                continue
            base = ExportSet([(datetime.min, i, p) for i, p in enumerate(paths, 1)])
            if len(paths) > 1:
                base.name = f"{os.path.basename(paths[0])} (+{len(paths) - 1} more)"
            bases.append(base)
            sets.append(base)
            print(f"Using existing {base.label} as merge base.", file=sys.stderr)

    # Order sets by the first entry they actually contain. The export stamp in
    # the filename is only a proxy for this and can mislead if an export was
    # renamed or copied.
    for s in sets:
        s.first_ts = peek_first_timestamp(s.paths)
    sets.sort(key=lambda s: (s.first_ts or datetime.max, s.export_stamp))

    # Index each chunk's first entry so shift detection can skip whole chunks.
    for s in sets:
        s.chunk_first_ts = [peek_first_timestamp([p]) for p in s.paths]

    show_progress = not (args.quiet or args.no_progress)
    total_in = sum(s.total_bytes() for s in sets)
    print(
        f"Found {len(files)} file(s) in {len(sets)} export set(s), {human(total_in)} total.",
        file=sys.stderr,
    )

    shift_notes = []
    if not args.no_detect_shift:
        pairs = list(zip(sets, sets[1:]))
        for i, (prev, cur) in enumerate(pairs, 1):
            bar = Progress(f"Aligning {i}/{len(pairs)}", 0, enabled=show_progress)
            shift, conf = detect_shift(prev, cur, progress=bar)
            bar.close()
            if shift is None:
                cur.shift = 0
                shift_notes.append(
                    f"    {cur.label}: no shared content with previous export; assuming no shift"
                )
            else:
                cur.shift, cur.shift_confidence = shift, conf
                if shift and conf < 0.9:
                    shift_notes.append(
                        f"    {cur.label}: shift {shift:+d}h detected with low confidence "
                        f"({conf:.0%}); verify before trusting"
                    )
            cur.offset = prev.offset + cur.shift

    for s in sets:
        s.first_ts = None  # recomputed during the merge

    # Write next to the output(s) and swap in at the end: the outputs may be our
    # own base input, and an interrupted run must not leave a truncated log.
    out = OutputWriter(abs_out, args.split, args.dry_run)
    bar = Progress("Merging     ", total_in, enabled=show_progress)
    try:
        kept, dropped, gaps = merge(sets, out, verbose=not args.quiet, progress=bar)
        bar.close()
        out.commit()
    except BaseException:
        out.abort()
        raise

    first = min((s.first_ts for s in sets if s.first_ts), default=None)
    last = max((s.last_ts for s in sets if s.last_ts), default=None)

    lines = []
    lines.append("")
    lines.append("Summary")
    lines.append("-------")
    lines.append(f"  input files      : {len(files)} in {len(sets)} export sets ({human(total_in)})")
    for base in bases:
        note = f" ({base.dropped:,} superseded)" if base.dropped else ""
        lines.append(f"  merge base       : {base.label}, {base.kept:,} entries carried over{note}")
    lines.append(f"  entries written  : {kept:,}")
    lines.append(f"  duplicates removed: {dropped:,}")
    if kept + dropped:
        lines.append(f"  overlap          : {dropped / (kept + dropped) * 100:.1f}% of parsed entries")
    if first and last:
        lines.append(f"  time range       : {first} .. {last}")
    if args.dry_run:
        lines.append("  output           : (dry run, nothing written)")
    elif args.split == "none":
        lines.append(f"  output           : {args.output} ({human(os.path.getsize(abs_out))})")
    else:
        size = sum(os.path.getsize(p) for p, _ in out.outputs)
        lines.append(f"  output           : {len(out.outputs)} {args.split} file(s) ({human(size)})")
    if args.split != "none":
        verb = "would write" if args.dry_run else "wrote"
        for path, count in out.outputs:
            lines.append(f"    {os.path.basename(path):<40} {verb} {count:>9,} entries")

    # Earlier outputs this run did not rewrite (another --split setting, or a
    # --fresh rebuild that no longer reaches that period) are left in place.
    # They would be read back as merge base next time; point them out.
    written = {os.path.normcase(p) for p, _ in out.outputs}
    leftovers = [
        p for paths in existing.values() for p in paths
        if os.path.normcase(p) not in written
    ]
    if leftovers and not args.dry_run:
        lines.append("")
        lines.append(
            f"  note: {len(leftovers)} earlier merged file(s) were not rewritten by this run;"
        )
        lines.append(
            "  they are read back as merge base on the next run. Delete them if unwanted:"
        )
        for p in leftovers:
            lines.append(f"    {os.path.basename(p)}")

    shifted = [s for s in sets if s.shift]
    if shifted:
        lines.append("")
        lines.append(
            "  Timezone re-rendering detected (the exporter renders historical"
        )
        lines.append(
            "  timestamps in the offset active at EXPORT time, so the same event"
        )
        lines.append(
            "  appears at different clock times in different exports):"
        )
        for s in shifted:
            lines.append(
                f"    {s.label}: {s.shift:+d}h vs previous export "
                f"(cumulative {s.offset:+d}h, confidence {s.shift_confidence:.0%})"
            )
        lines.append(
            "  These were compensated for de-duplication; entry text is unmodified,"
        )
        lines.append(
            "  so the output contains a clock discontinuity at each such seam."
        )
    if shift_notes:
        lines.append("")
        lines.append("  Shift detection notes:")
        lines.extend(shift_notes)

    if gaps:
        lines.append("")
        lines.append(f"  WARNING: {len(gaps)} coverage gap(s) - entries purged before any export caught them:")
        for prev, nxt, label in gaps:
            lines.append(f"    {prev} .. {nxt}  ({nxt - prev}) before {label}")

    text = "\n".join(lines)
    print(text, file=sys.stderr)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as fh:
            fh.write(text.lstrip("\n") + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
