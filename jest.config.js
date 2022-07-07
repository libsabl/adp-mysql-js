/* eslint-disable @typescript-eslint/no-var-requires */
const path = require('path');

/** @type {import('@jest/types').Config.InitialOptions} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  moduleNameMapper: {
    '^\\$$': path.join(__dirname, 'src'),
    '^\\$/(.*)$': path.join(__dirname, 'src/$1'),
    '^\\$test/(.*)$': path.join(__dirname, 'test/$1'),
    '^\\$ex/(.*)$': path.join(__dirname, 'test/example/src/$1'),
    '^\\$defs/(.*)$': path.join(__dirname, 'test/example/src/defs/$1'),
  },
  testMatch: ['**/test/**/*.spec.*'],
  collectCoverage: true,
  collectCoverageFrom: ['src/**/*.ts'],
  coverageDirectory: path.join(__dirname, 'coverage'),
  coverageReporters: ['text'],
};
