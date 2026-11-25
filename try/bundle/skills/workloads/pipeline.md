# Run a multi-step pipeline

Two stages in one framework you write, starting from `euler.py`:

1. Transcribe: one task per recording, each writing a transcript file.
2. Index: one task that starts only after every transcribe task reached
   TASK_FINISHED, reads the transcripts and builds a searchable index.

Ask the user for the folder of recordings and for one phrase they know is in
one of them. The speech-to-text model is a download the user must agree to;
tell them its name and size before fetching it, and put it under `state/`.

Tasks run as the current user, so they can read the user's folder and write
under `state/` directly. Write outputs there, not into the task sandbox, so the
index stage can find them.

Done when a search for the user's phrase returns the recording it came from.
Report the per-recording transcribe state, the index task state and the search
result.
