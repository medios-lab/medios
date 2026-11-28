---
name: medios-design-writing
description: Write or revise a medios design proposal (docs/design/) for a maintainer deciding whether to approve it. Covers structure (failure, constraint, proposal, matrix, alternatives, recommendation), naming the exact component that acts, host versus medios node, false anchors, standalone headings, verifying design-changing unknowns before writing, and when to use a sequence diagram or a matrix instead of prose. Use for any design page, design record or issue write-up about medios internals.
---

# Medios design writing

The reader is a medios maintainer deciding whether to approve the design. They
know the codebase in general and were not in the session. Worked examples of
every rule are in `docs/design/MEDIOS_TECHNICAL_WRITING.md`; read it before
drafting.

## Before writing

Verify anything that could change the design. If the answer could reshape the
proposal, find it out first and write it as an observation with its date and
method. An unknown may stay in the document only if it changes nothing about
the design, or if it is a step in the recommendation with an owner and a reason
why proceeding without it is acceptable. (Examples 8, 13.)

## Structure

1. The failure: one concrete, observed failure, then its execution path as a
   sequence diagram. Each arrow names sender and receiver; evidence qualifiers
   go in a note on the diagram. (1, 6)
2. The constraint that the existing code hits, with `file:line`.
3. The proposal: the exact classes, files, functions and registered symbols it
   changes. (10)
4. Configuration: what the operator sets when configuring a medios node (which
   flag, which values) and the behaviour each value produces. (11)
5. A comparison matrix: current and proposed paths as rows, properties as
   columns. Properties of configurations go in cells, not paragraphs. (9, 12)
6. Alternatives, each with its actual tradeoff. If the evidence does not rule
   one out, say so.
7. Recommendation: which alternative to take, and the next steps in order. Never
   end on a list of unknowns. (13)

Use prose for reasoning, diagrams for sequences and matrices for comparisons. A
paragraph that restates properties of several configurations is too long; move
it into the matrix.

## Naming

- "Medios" is the whole system. Name the component that acts: the medios
  controller, medios-node, the medios containerizer, an isolator, the command
  executor. (2)
- Claim only what the component does or records: the controller records an
  allocation; it does not know a task "receives" it.
- No agency: nodes, tasks and isolators don't choose, ask, tell, decide or
  remember. Describe the code path the configuration selects. (2, 3)
- A deployment is not the product. Behaviour that depends on node configuration
  is stated with that configuration. (2)
- The host and the medios node are different things. Write "a medios node
  running inside a RunPod pod", never "on a pod". (3, 4)
- Use technical nouns: GPU or device (NVML UUID or index), not "card"; the
  docker_shell containerizer, not "the other place". Define a term such as
  "mode" before using it. (9, 11)
- When something does happen, name what causes it. "No component chooses" is
  false if CUDA's default enumeration picks device 0. (3)

## Sentences and headings

- No false anchors. Don't open a sentence or heading on "that", "there", "this"
  or "such" pointing back at a referent the reader has to find. Name it. (5, 7)
- Each fact gets its own subject and verb; don't compress a component, a flag
  and an absence into one clause. (5)
- Headings state the finding and make sense to a reader skimming headings
  alone. (7)
- Put each qualifier (observed with date, derived from code with `file:line`,
  proposed) next to the claim it limits.

## Check before publishing

Read only the headings: does each one stand alone? Read each sentence opener:
is it a pointer back? Is "medios" anywhere the subject of an action? Does any
paragraph restate matrix content? Does the document end on a recommendation?
