import type { APIRoute } from "astro"
import { fetchPageStats } from "../lib/bucket"

const SITE = "https://www.urbanhazardmaps.com"

function slugify(name: string): string {
	return name
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-|-$/g, "")
}

export const GET: APIRoute = async () => {
	const stats = await fetchPageStats("needles")

	const urls = [
		`  <url><loc>${SITE}</loc></url>`,
		`  <url><loc>${SITE}/methodology</loc></url>`,
		`  <url><loc>${SITE}/data-quality</loc></url>`,
		`  <url><loc>${SITE}/fix</loc></url>`,
		...stats.hoods.map(
			(h) => `  <url><loc>${SITE}/neighborhoods/${h.slug || slugify(h.name)}</loc></url>`,
		),
	]

	const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.join("\n")}
</urlset>`

	return new Response(xml, {
		headers: { "Content-Type": "application/xml" },
	})
}
