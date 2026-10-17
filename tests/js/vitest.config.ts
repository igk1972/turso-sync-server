import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        // Tests restart servers several times; a loaded machine needs headroom.
        testTimeout: 30_000,
    },
});
