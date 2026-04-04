import { defineConfig } from "astro/config"
import node from "@astrojs/node"
import react from "@astrojs/react"
import sitemap from "@astrojs/sitemap"

export default defineConfig({
  output: "server",
  adapter: node({ mode: "standalone" }),
  integrations: [
    react(),
    sitemap({
      filter: (page) => !page.includes("/health"),
    }),
  ],
  site: "https://www.urbanhazardmaps.com",
  server: {
    host: "0.0.0.0",
    port: 4321,
  },
})
