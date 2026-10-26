import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const apiPort = env.API_EXTERNAL_PORT || "8000";
  const proxy = { target: `http://localhost:${apiPort}`, changeOrigin: true };

  return {
    plugins: [react()],
    server: {
      port: 5173,
      proxy: { "/api": proxy, "/ready": proxy, "/health": proxy },
    },
  };
});
