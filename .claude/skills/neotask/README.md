# NeoTask Skill

Code assistance skill for **NeoTask** — a lightweight Python asynchronous task queue manager.

## What it does

Helps Claude write, explain, and debug code that uses the `neotask` library. Covers:

- Instant task submission and result waiting (`TaskPool`)
- Scheduled tasks: delayed, periodic, and Cron-based (`TaskScheduler`)
- Configuration patterns and storage backend selection
- Event callbacks and monitoring setup
- Error handling and retry strategies

## When this skill triggers

Invoke when the user is working with `neotask` code and needs help with:

- Adding `neotask` to their Python project
- Writing task scheduling/scheduling code
- Debugging task execution issues
- Choosing the right configuration for their use case
- Understanding neotask API patterns

## Skill structure

```
neotask/
├── SKILL.md          # Main skill definition (loaded by Claude Code)
├── README.md         # This file
├── references/       # Detailed API and configuration references
├── examples/         # Usage examples and patterns
├── scripts/          # Utility scripts
├── templates/        # Code templates
└── assets/           # Static assets
```
