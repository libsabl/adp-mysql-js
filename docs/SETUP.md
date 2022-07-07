# Setup

## Quick-start

For the impatient. But **please** see [Tooling](#tooling) and [Path aliases](#path-aliases). For even more detail about all the individual configuration files, see [CONFIG.md](./CONFIG.md).

### 🚨 Install **pnpm**
**IMPORTANT**: Install `pnpm` if you don't already have it

**Mac**
```sh
brew install pnpm
```

**Win**

```powershell
# Pwsh / Powershell:
iwr https://get.pnpm.io/install.ps1 -useb | iex
```

**Linux / other**

```sh
curl -fsSL https://get.pnpm.io/install.sh | sh -
```

## Package commands 

### Restore packages

```bash
pnpm install  # DO NOT USE npm!!
```

### Clean

```bash
# Recursivley delete `dist` and `coverage` folders:
pnpm clean
```

### Run tests

```bash
# Run tests and show output in console
pnpm test

# Same, then open the html coverage report:
pnpm test:view

# Rebuild the html coverage report but don't reopen it:
pnpm test:refresh
```

### Build

```bash
# Rebuild to dist folder
pnpm build
```

## Tooling

### [nodejs](https://nodejs.org/en/download/) v16

  Install by downloading and running the applicable installer. You
  only need to do this once per developer machine.

### [pnpm](https://pnpm.io/)

  `pnpm` is a performance- and safety-improved package installer that replaces `npm` for package installation and version management.

### VS Code extensions

Your life will be much better with these extensions installed

- [ESLint](vscode:extension/dbaeumer.vscode-eslint)
- [Prettier](vscode:extension/esbenp.prettier-vscode) 
- [Jest](vscode:extension/orta.vscode-jest) 
 
## Path aliases

>
> **TL;DR:** In import statements
>
> - `$` alone means the local `src/index.ts` itself
> - `$/` means the local `src` directory
> - `$test/` means the local `test` directory within a project
>

Path aliases are extremely helpful for writing succint and intuitive imports *between files within the same package*.

Natively, node and TypeScript require `imports` from other files within the same source code repository to be expressed as *relative* file paths:

```ts
// From src/tools/analyzers/entity-linter.ts
import { Domain } from '../../some/directory/domain'
```

```ts
// From test/some/directory/domain.spec.ts
import { Domain } from '../../../src/some/directory/domain'
```

This is tedious and also brittle. Moving either the file that includes the `import` or the file that is imported can break the `import` statement. The TypeScript compiler and Jest both provide ways to define *path aliases* within an particular project. 

With path aliases, you can do something silly like map the prefix `'🍕'` to the directory `./src/some/directory` within that package directory tree. Then in any source file within that same project, *regardless of its relative path within the project*, you can do this:

```ts
// From src/tools/analyzers/domain-linter.ts
// ..or src/some-file.ts
// ..or test/some/directory/thing.spec.ts
import { Domain } from '🍕/domain'
```

In this repo we define two aliases: `$` for the `src` directory and `$test` for the `test` directory within the applicable project. 

For details of how path aliases are defined in `tsconfig.json` and `jest.config.js`, see [CONFIG.md](./CONFIG.md).
