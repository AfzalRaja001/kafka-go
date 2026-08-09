# Project instructions for kafka-go

## After every commit or PR: write a full lesson-style explanation

This project is being built by someone learning Go and Kafka's internals from
scratch, alongside the actual implementation - see `C:\Users\Afzal\Desktop\go-lessons\`
for the 17-lesson curriculum this project's early code (`Segment`, `Index`,
`Partition`, the protocol codecs, etc.) came out of.

**After finishing any commit or PR - a full subpart of a phase, not just an
entire phase - write up what was built in the same depth and structure as
those lesson files, not a short wrap-up.** This applies every time, including
small commits. Do not skip it or compress it into a few paragraphs "for
brevity" - brevity is not the goal here, understanding is.

### The structure to follow, modeled directly on the lesson files

1. **What's being built and why it exists at all.** Motivate before explaining
   mechanics - state the problem this code solves before showing the code
   that solves it, the same way every lesson opens with "the problem X leaves
   open" before introducing the fix.
2. **Break it into numbered sections, one idea per section** - don't cover
   three unrelated design decisions in one paragraph. Each section should be
   readable on its own once the sections before it have been read.
3. **Define every concept before using it. No forward references.** If a term
   is going to matter later in the same write-up, define it the moment it
   first appears, not when it becomes convenient.
4. **Show the real code inline and walk through it** - not just "see the
   file," actually paste the relevant code (or the important slice of it) and
   explain the specific lines that matter, the way every lesson's code blocks
   are followed by prose explaining what's non-obvious about them.
5. **Give the reasoning for every non-obvious choice explicitly** - "why this
   design and not an alternative," not just "here's what it does." If
   something was changed from how a lesson originally wrote it (an omission,
   a fix, a different approach), explain why, the same way the storage
   lesson move flagged dropping `IndexedSegment` and fixing the Windows
   `Truncate` bug.
6. **Connect back to earlier lessons and earlier commits by name** where
   relevant (e.g. "this is the same shared-cursor problem Lesson 15 section 5
   avoided with `ReadAt`") so new work visibly builds on what's already
   understood, instead of reading as unrelated new material.
7. **Summarize what got built** at the end - a concrete recap, not vague
   praise.
8. **State what's next** and how this piece connects to it.

### Why this exists

The person building this explicitly does not want terse "done" summaries or
checkbox-style updates after real implementation work - the goal is to
actually understand everything that gets built, at the same depth as the
lessons that got them from zero Go knowledge to writing a concurrency-safe
storage engine. A commit that isn't explained this way might as well not be
explained at all.

This instruction takes precedence over the shorter version of this rule in
the global `~/.claude/CLAUDE.md` - for this project specifically, go deeper
and follow the lesson structure literally, every time.
