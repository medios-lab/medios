# Queue ComfyUI workflows

ComfyUI needs an NVIDIA GPU on a Linux node. On a Mac, or on a Linux machine
where `nvidia-smi` doesn't list a GPU, tell the user this choice can't run here.

This bundle revision doesn't include steps for the GPU path yet. If the
machine has a GPU and the user wants to go ahead, offer to help them file an
issue asking for it (`skills/file-issue/SKILL.md`) with the `nvidia-smi`
output attached.
