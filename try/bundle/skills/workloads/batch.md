# Process a batch across machines

This choice needs more than one Medios node, and the preview starts one on this
machine. Tell the user that, and that adding a second machine isn't covered by
this bundle revision.

If the user only wants to see the batch shape on one node, write a framework
that launches one task per input file and checks one output per input; it then
proves everything except the placement across machines. Say that the
"more than one node did work" check didn't run.
