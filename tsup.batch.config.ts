import { defineConfig } from 'tsup';

// A–D outputs never enter the existing dist/ or package exports.
export default defineConfig({
  entry: ['src/batch/domain/json.ts', 'src/batch/domain/config.ts', 'src/batch/domain/definition.ts',
    'src/batch/domain/control.ts', 'src/batch/domain/operator.ts', 'src/batch/domain/cursor.ts', 'src/batch/domain/events.ts', 'src/batch/api/types.ts', 'src/batch/api/errors.ts',
    'src/batch/storage/redis/keys.ts', 'src/batch/storage/redis/connection.ts', 'src/batch/storage/redis/store.ts',
    'src/batch/runtime/telemetry.ts', 'src/batch/index.ts'],
  format: ['esm', 'cjs'], dts: true, sourcemap: true, clean: true, splitting: false,
  target: 'node22', outDir: '.temp/batch'
});
