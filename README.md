# merge_logs.py

Merge the split, overlapping log exports from [ProfitView](https://profitview.app) into one clean, de-duplicated, chronologically ordered log file, and keep that file up to date as you export more logs.

## Why

ProfitView exports logs as a set of roughly 10 MB chunks:

```
profitview-2026-09-16-14-57-05-1.log
profitview-2026-09-16-14-57-06-2.log
profitview-2026-09-16-14-57-07-3.log
```

That is awkward for three reasons:

- **Exports overlap.** ProfitView only keeps a limited log history, so each new export repeats a long stretch of entries you already have from the previous one.
- **Chunks split entries.** The 10 MB split can cut a multi-line entry in half, so the chunks cannot simply be deduplicated one by one.
- **Timestamps shift across DST changes.** The exporter shows old timestamps using the UTC offset in effect *at export time*. So the same event can show a different clock time, one hour apart, in exports taken in summer and in winter. A plain text comparison would then miss those duplicates.

`merge_logs.py` handles all of this and produces a single `profitview-merged.log`.

## Requirements

- Python 3.8 or newer
- No third-party packages

## Quick start

1. Put `merge_logs.py` in the folder where you keep your logs.
2. Export the logs from ProfitView and drop the chunk files into that folder.
3. Run:

   ```
   python merge_logs.py
   ```

This creates `profitview-merged.log`.

## Keeping the merged log up to date

This is the intended day-to-day workflow:

1. Export the logs again whenever you like.
2. Drop the new chunk files into the same folder, next to the existing `profitview-merged.log`.
3. Run `python merge_logs.py` again.

If `profitview-merged.log` already exists, the tool uses it as the **base**. It keeps everything already in it and appends only the entries that are genuinely new. After the merge you can delete the export chunks, but you don't have to: the tool recognises and skips entries it already has, so running it again with the same files present leaves the output unchanged.

The existing log is never edited in place. The new result is written to `profitview-merged.log.tmp` and only replaces the old file once it is complete. If a run crashes or is interrupted with Ctrl+C, your existing merged log is left untouched.

> **Tip:** Export often enough that each new export still overlaps the previous one. If ProfitView has already dropped entries that no export captured, the tool cannot recover them, but it tells you about the gap (see [Coverage gaps](#coverage-gaps)).

## Usage

```
python merge_logs.py [inputs ...] [options]
```

`inputs` are log files and/or directories to scan. The default is the current directory.

| Option | Description |
| --- | --- |
| `-o`, `--output FILE` | Output file (default: `profitview-merged.log`). If it already exists, it is used as the merge base. |
| `--fresh` | Ignore an existing output file and rebuild only from the export chunks. |
| `-n`, `--dry-run` | Analyse and print the summary, but write nothing. |
| `--report FILE` | Also write the summary to this file. |
| `--no-detect-shift` | Skip DST/timezone shift detection. Faster, but misses duplicates across a DST change. |
| `-q`, `--quiet` | Hide per-export output and progress bars. |
| `--no-progress` | Hide progress bars only. |

### Examples

```bash
# Default: merge everything in the current folder into profitview-merged.log
python merge_logs.py

# Check first what a merge would do
python merge_logs.py --dry-run

# Collect exports from several folders
python merge_logs.py . archive/2026-q2

# Rebuild the merged log from scratch (e.g. after adding older exports)
python merge_logs.py --fresh

# Write to a different file and save the summary
python merge_logs.py logs/ -o bot-a.log --report bot-a-report.txt
```

Input files must follow the export naming scheme `<prefix>-YYYY-MM-DD-HH-MM-SS-<chunk>.log`. Other `.log` files are ignored, and the tool prints a note for each one it skips.

## Example output

```
Using existing profitview-merged.log as merge base.
Found 3 file(s) in 2 export set(s), 135.6MB total.
  Aligning 1/1 |████████████████████████████████████████| 100.0%   110.0MB  done in 00:04
  [ 1/2] profitview-merged.log                 1 chunk(s)  2025-12-11 19:43 .. 2026-09-12 22:13  kept 349,323  dropped       0
  [ 2/2] profitview-2026-09-16-14-57-05-1.log  3 chunk(s)  2026-06-18 15:40 .. 2026-09-16 14:20  kept   4,944  dropped  79,321
  Merging      |████████████████████████████████████████| 100.0%   135.6MB  done in 00:07

Summary
-------
  input files      : 3 in 2 export sets (135.6MB)
  merge base       : profitview-merged.log, 349,323 entries carried over
  entries written  : 354,267
  duplicates removed: 79,321
  overlap          : 18.3% of parsed entries
  time range       : 2025-12-11 19:43:15.556000 .. 2026-09-16 14:20:10.594000
  output           : profitview-merged.log (111.5MB)
```

Progress bars are only drawn when the output goes to a terminal, so redirected output stays clean.

## How it works

1. **Group chunks into exports.** A chunk numbered `1` starts a new export. The timestamp in the filename changes by a second or so from chunk to chunk, so the tool cannot use it to group chunks.
2. **Order exports** oldest first, by the first log entry each one actually contains. An existing merged log is treated as one more export, normally the oldest.
3. **Read each export as one continuous stream.** The chunks are joined before parsing, so an entry split across two chunks stays whole. An entry is a timestamped line plus any following lines without a timestamp.
4. **Detect timezone shifts.** Entries that appear only once near the start of each export are looked up in the previous export. The most common time difference, rounded to whole hours, is that export's shift.
5. **De-duplicate.**
   - The tool tracks the newest timestamp it has written so far, adjusted for any detected shift.
   - Anything clearly older than that is a duplicate.
   - Within a 5-second window around it, entries are compared by content instead. Log lines from concurrent threads can be slightly out of order, and this avoids dropping genuinely new ones.
6. **Write** the entries byte for byte as they appear in the exports. The tool never changes entry text, line endings or invalid UTF-8.

## Things to know

### Coverage gaps

If there is a time gap between the end of one export and the start of the next, ProfitView discarded those entries before any export captured them. The summary shows a warning like this:

```
WARNING: 1 coverage gap(s) - entries purged before any export caught them:
  2026-07-01 08:12:44 .. 2026-07-03 17:05:02  (2 days, 8:52:18) before profitview-...-1.log
```

The tool cannot fill such gaps; exporting more often avoids them.

### Clock jumps at DST changes

Duplicates are detected using timestamps adjusted for the shift, but the output keeps each entry's original timestamp text. So where the merged log switches from an export taken in winter to one taken in summer (or the other way round), the timestamps can jump by an hour. The summary lists every detected shift.

### Adding older exports later

Exports are always sorted by their contents, so adding an export older than the merged log still works. If you want a clean rebuild from only the chunk files, run with `--fresh`.

## License

Released under the [MIT License](LICENSE).
