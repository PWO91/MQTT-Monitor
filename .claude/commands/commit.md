Automatically commit all project changes without asking any questions.

Steps — execute all of them immediately, no confirmation needed:
1. Run `git status` and `git diff` to understand what changed
2. Stage only project files (never build/, never .env):
   - src/
   - CMakeLists.txt
   - .gitignore
   - .env.example
   - .claude/
3. Generate a concise English commit message in conventional commits style (feat/fix/refactor/chore) based on the diff — focus on WHY, not what
4. Run `git commit` immediately with that message — do not ask for approval

If $ARGUMENTS is provided, use it as the commit message subject instead of generating one.
