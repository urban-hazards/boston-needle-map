import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3"
import type { MarkerData, PageStats } from "./types"

const client = new S3Client({
	endpoint: process.env.ENDPOINT || undefined,
	region: process.env.REGION || "us-east-1",
	credentials: {
		accessKeyId: process.env.ACCESS_KEY_ID || "",
		secretAccessKey: process.env.SECRET_ACCESS_KEY || "",
	},
	forcePathStyle: true,
})

const BUCKET = process.env.BUCKET || ""

const cache = new Map<string, { data: unknown; expires: number }>()
const CACHE_TTL = 5 * 60 * 1000

async function readJson<T>(key: string): Promise<T> {
	const now = Date.now()
	const cached = cache.get(key)
	if (cached && cached.expires > now) {
		return cached.data as T
	}

	const command = new GetObjectCommand({ Bucket: BUCKET, Key: key })
	const response = await client.send(command)
	const body = await response.Body?.transformToString()
	if (!body) throw new Error(`Empty response for ${key}`)
	const data = JSON.parse(body) as T

	cache.set(key, { data, expires: now + CACHE_TTL })
	return data
}

export function fetchPageStats(dataset = "needles"): Promise<PageStats> {
	return readJson<PageStats>(`${dataset}/stats.json`)
}

export function fetchPoints(dataset = "needles"): Promise<number[][]> {
	return readJson<number[][]>(`${dataset}/points.json`)
}

export function fetchMarkers(dataset = "needles"): Promise<MarkerData[]> {
	return readJson<MarkerData[]>(`${dataset}/markers.json`)
}
