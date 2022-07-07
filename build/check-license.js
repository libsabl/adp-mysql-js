import * as fs from 'fs/promises';
import chalk from 'chalk';
import { glob, packageDirFn, repoDirFn, comment, indent } from './util';
import { EOL } from 'os';

(async () => {
  const verbose =
    process.argv.includes('-v') || process.argv.includes('--verbose');

  const pkgpath = await packageDirFn();
  const srcfiles = await glob(
    pkgpath.join('src', '**', '*.ts').replace(/\\/g, '/')
  );
  const tstfiles = await glob(
    pkgpath.join('test', '**', '*.ts').replace(/\\/g, '/')
  );

  const repopath = await repoDirFn();
  const lichdr = await fs.readFile(repopath('LICENSE_HEADER'), 'utf8');
  const jshdr = lichdr
    .split(/[\r\n]+/)
    .filter((l) => l.trim().length)
    .map(comment);

  console.log(EOL + 'Checking for license header');
  if (verbose) {
    console.log(EOL + chalk.cyanBright(jshdr.join(EOL)));
    console.log();
  }

  const lncnt = jshdr.length;
  let badmatch = false;

  for (let fpath of srcfiles.concat(tstfiles)) {
    let content = await fs.readFile(fpath, 'utf8');
    let header = content.split(/[\r\n]+/).slice(0, lncnt);

    if (header.join(EOL) == jshdr.join(EOL)) {
      if (verbose) {
        console.log(
          chalk.greenBright(`  ✓ ${fpath.substring(pkgpath.root.length)}`)
        );
      }

      continue;
    }

    badmatch = true;
    console.log(chalk.red(EOL + `  X ${fpath.substring(pkgpath.root.length)}`));
    console.log(chalk.redBright(EOL + '    Existing header:' + EOL));
    console.log(chalk.redBright(indent(header.join(EOL), 4)));
    console.log('');
  }

  if (badmatch) {
    console.log(chalk.red(EOL + '  ⚠️ License header check failed' + EOL));
    process.exit(1);
  } else {
    console.log(
      chalk.greenBright(
        EOL + '  ✓ License header check passed for all files' + EOL
      )
    );
  }
})();
