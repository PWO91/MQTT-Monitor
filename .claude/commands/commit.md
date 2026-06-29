Wykonaj git commit ze wszystkimi zmienionymi plikami projektu (src/, CMakeLists.txt, .gitignore, .env.example).

Kroki:
1. Uruchom `git status` i `git diff` żeby zobaczyć co się zmieniło
2. Dodaj do stage tylko pliki projektu (nie katalog build/, nie .env):
   - src/
   - CMakeLists.txt
   - .gitignore
   - .env.example
   - .claude/
3. Write a concise commit message in English using conventional commits style (feat/fix/refactor/chore) — focus on WHY, not what
4. Create the commit with that message

If the user provided a description in $ARGUMENTS — use it as the basis for the commit message instead of generating one.
