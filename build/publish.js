import * as fs from 'fs/promises';
import rmfr from 'rmfr';
import chalk from 'chalk';
import {
  exec,
  gitGetCommit,
  gitGetTags,
  gitHasChanges,
  packageDirFn,
  relPathFn,
} from './util';
import { EOL } from 'os';

/**
 *
 * @param {string} source
 * @param {string} label
 * @returns {string}
 */
function removeChunk(source, label) {
  const startTag = `<!-- BEGIN:${label} -->`;
  const endTag = `<!-- END:${label} -->`;
  let result = source;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const startIx = result.indexOf(startTag);
    if (startIx < 0) break;
    const endIx = result.indexOf(endTag, startIx);
    if (endIx < 0) break;
    const before = result.substring(0, startIx);
    const after = result.substring(endIx + endTag.length);
    result = before + after;
  }

  return result;
}

async function addReadmeHeaders(source, pkgv) {
  const commit = await gitGetCommit();
  const codeCovImg = `https://codecov.io/gh/libsabl/adp-mysql-js/commit/${commit}/graph/badge.svg?token=TVL1XYSJHA`;
  const codeCovPage = `https://codecov.io/gh/libsabl/adp-mysql-js/tree/${commit}/src`;
  const ghBrowse = `https://github.com/libsabl/adp-mysql-js/tree/${commit}`;
  let docsPath = `https://github.com/libsabl/adp-mysql-js/blob/${commit}/docs/DOCS.md`;

  let md = `[![codecov](${codeCovImg})](${codeCovPage})`;
  md += EOL + EOL + '**version**: `' + pkgv + '`';

  if (pkgv.match(/^\d+\.\d+\.\d+$/)) {
    docsPath = `https://github.com/libsabl/adp-mysql-js/blob/v${pkgv}/docs/DOCS.md`;
    const ghRelease = `https://github.com/libsabl/adp-mysql-js/releases/tag/v${pkgv}`;
    md += ' | **tag**: [`v' + pkgv + '`](' + ghRelease + ') ';
  }
  md += ' | **commit**: [`' + commit.substring(0, 9) + '`](' + ghBrowse + ')';
  // md += ' | See [**Full docs on GitHub**](' + docsPath + ')';
  return md + EOL + source;
}

async function validateVersion(pkgv) {
  // Check version #
  if (pkgv.match(/-.+$/)) {
    console.log(chalk.greenBright(`✓ Prerelease version ${pkgv}`));
  } else {
    console.log(chalk.greenBright(`Validating release version ${pkgv}`));
    const dirty = await gitHasChanges();
    if (dirty) {
      console.error(chalk.red('  Validation failure: Uncommitted changes'));
      process.exit(1);
    } else {
      console.log(chalk.cyanBright('  ✓ No uncommitted changes'));
    }

    const tags = await gitGetTags();
    if (!tags.length || !tags.includes('v' + pkgv)) {
      console.error(
        chalk.red(
          `  Validation failure: Did not find expected git tag v${pkgv}`
        )
      );
      process.exit(1);
    } else {
      console.log(chalk.cyanBright(`  ✓ Found git tag v${pkgv}`));
    }
  }
}

(async () => {
  const pkgpath = await packageDirFn();
  const pubpath = relPathFn(pkgpath.root, 'publish');

  console.log(chalk.greenBright('Building package files'));

  const pkginfo = JSON.parse(
    await fs.readFile(pkgpath('package.json'), 'utf8')
  );

  const pkgv = '' + pkginfo.version;
  await validateVersion(pkgv);

  // Delete existing publish folder
  await rmfr(pubpath.root);

  // Remake empty folder
  await fs.mkdir(pubpath.root);

  // Copy package files
  for (const dir of ['src', 'dist']) {
    console.log(chalk.cyanBright(`  Copying ${dir}/*`));
    await fs.cp(pkgpath(dir), pubpath(dir), {
      recursive: true,
    });
  }

  for (const fl of ['LICENSE']) {
    console.log(chalk.cyanBright(`  Copying ${fl}`));
    await fs.cp(pkgpath(fl), pubpath(fl));
  }

  // Render package.json
  console.log(chalk.cyanBright('  Rendering package.json'));
  delete pkginfo.devDependencies;
  delete pkginfo.scripts;
  await fs.writeFile(
    pubpath('package.json'),
    JSON.stringify(pkginfo, null, 2),
    'utf8'
  );

  // Render README
  console.log(chalk.cyanBright('  Rendering README.md'));
  let rdme = await fs.readFile(pkgpath('README.md'), 'utf8');
  rdme = removeChunk(rdme, 'REMOVE_FOR_NPM');
  rdme = await addReadmeHeaders(rdme, pkgv);
  await fs.writeFile(pubpath('README.md'), rdme);

  // Render tsconfig
  console.log(chalk.cyanBright('  Rendering tsconfig.json'));
  const { stdout: tsconfig } = await exec(
    'pnpx tsc --project ./tsconfig.build.json --showConfig ',
    { cwd: pkgpath.root }
  );
  await fs.writeFile(pubpath('tsconfig.json'), tsconfig, 'utf8');

  console.log();
})();
