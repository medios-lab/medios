---
name: medios-workloads
description: The "What do you want to run next?" menu shown after the first framework passes, with what each choice needs from the machine. Use for step 4 of the Medios preview.
---

# What do you want to run next?

Show the user these four choices, plus "or describe a workload of your own".
Before showing them, check the machine against the "Needs" column and mark the
choices it can't run here, with the reason, so the user doesn't pick one that
will fail.

| Choice | What runs | Needs | Done when |
| --- | --- | --- | --- |
| Serve a local model | A small model service (Mistral 7B or a Qwen 8B variant) kept running by Fennel, the Medios service manager; send it one prompt | Fennel, which this bundle revision doesn't include | The endpoint answers and Fennel still reports the service running afterwards |
| Queue ComfyUI workflows | A small image-generation workflow placed on a compatible GPU | An NVIDIA GPU on a Linux node; a Mac's own GPU doesn't qualify | The workflow ran on the allocated GPU and the image opens |
| Process a batch across machines | Transcode a folder of short clips as independent tasks | More than one Medios node; this preview starts one | More than one node did work, every input has a readable output, and each failure traces to its input |
| Run a multi-step pipeline | A framework you write that transcribes a folder of recordings, then builds a searchable index from the transcripts | One node is enough; a speech-to-text model the user agrees to download | Indexing consumes the finished transcripts and a known phrase returns the expected recording |

Each choice has its own file in this directory: `local-model.md`,
`comfyui.md`, `batch.md`, `pipeline.md`. Read only the one the user picks.

For a workload of the user's own, start from the `euler.py` you wrote: change
the task command and the resources, keep the offer, acknowledge and teardown
handling as it is.
