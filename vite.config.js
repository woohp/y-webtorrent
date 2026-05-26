import { defineConfig } from "vite";

export default defineConfig({
    build: {
        outDir: "examples/browser-smoke/dist",
        target: "esnext",
    },
    esbuild: {
        target: "esnext",
    },
});
