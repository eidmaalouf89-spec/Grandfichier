import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ command }) => ({
  plugins: [react()],
  // In production build (npm run build), assets are served from root
  base: "/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
  server: command === "serve" ? {
    port: 5173,
    allowedHosts: "all",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  } : {},
}));
