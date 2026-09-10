import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  external: ['@redis/client'],
  sourcemap: false,
  clean: true,
  splitting: false,
  target: 'node22',
  outDir: 'dist'
});
