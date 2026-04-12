import { defineConfig } from "astro/config"
import node from "@astrojs/node"
import react from "@astrojs/react"

export default defineConfig({
  output: "server",
  adapter: node({ mode: "standalone" }),
  trailingSlash: "never",
  integrations: [react()],
  site: "https://www.urbanhazardmaps.com",
  server: {
    host: "0.0.0.0",
    port: 4321,
  },
})
