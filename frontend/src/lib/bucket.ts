import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3"
import type { MarkerData, PageStats } from "./types"

const API_URL = process.env.API_URL || ""
const USE_S3 = !!(process.env.ENDPOINT && process.env.BUCKET)

const client = USE_S3
	? new S3Client({
			endpoint: process.env.ENDPOINT || undefined,
			region: process.env.REGION || "us-east-1",
			credentials: {
				accessKeyId: process.env.ACCESS_KEY_ID || "",
				secretAccessKey: process.env.SECRET_ACCESS_KEY || "",
			},
			forcePathStyle: true,
		})
	: null

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
	const response = await client?.send(command)
	if (!response) throw new Error("S3 client not configured")
	const body = await response.Body?.transformToString()
	if (!body) throw new Error(`Empty response for ${key}`)
	const data = JSON.parse(body) as T

	cache.set(key, { data, expires: now + CACHE_TTL })
	return data
}

// Map dataset names to backend API prefixes
const API_PREFIX: Record<string, string> = {
	needles: "/api",
	encampments: "/api/encampments",
}

const EMPTY_PAGE_STATS: PageStats = {
	total: 0,
	years: [],
	hoods: [],
	hourly: Array(24).fill(0) as number[],
	year_hourly: {},
	year_monthly: {},
	zip_stats: [],
	generated: "",
	peak_hood: "",
	peak_hour: 0,
	peak_dow: "",
	avg_monthly: 0,
	initial_heat: [],
}

interface DashboardStats extends PageStats {
	points: number[][]
	markers: MarkerData[]
}

// Cache the full /api/stats response so we only fetch once per dataset
const statsCache = new Map<string, DashboardStats>()

async function fetchFullStats(dataset: string): Promise<DashboardStats | null> {
	const cached = statsCache.get(dataset)
	if (cached) return cached
	const prefix = API_PREFIX[dataset]
	if (!prefix) return null
	try {
		const res = await fetch(`${API_URL}${prefix}/stats`)
		if (!res.ok) return null
		const data = (await res.json()) as DashboardStats
		statsCache.set(dataset, data)
		return data
	} catch {
		return null
	}
}

export async function fetchPageStats(dataset = "needles"): Promise<PageStats> {
	if (USE_S3) return readJson<PageStats>(`${dataset}/stats.json`)
	const full = await fetchFullStats(dataset)
	if (!full) return EMPTY_PAGE_STATS
	return full
}

export async function fetchPoints(dataset = "needles"): Promise<number[][]> {
	if (USE_S3) return readJson<number[][]>(`${dataset}/points.json`)
	const full = await fetchFullStats(dataset)
	if (!full) return []
	return full.points
}

export interface DistrictBoundary {
	id: string
	geometry: GeoJSON.Geometry
}

export type DistrictBoundaries = Record<string, DistrictBoundary[]>

export async function fetchDistrictBoundaries(): Promise<DistrictBoundaries> {
	if (USE_S3) {
		try {
			return await readJson<DistrictBoundaries>("districts/boundaries_display.json")
		} catch {
			return {}
		}
	}
	return {}
}

export async function fetchMarkers(dataset = "needles"): Promise<MarkerData[]> {
	if (USE_S3) return readJson<MarkerData[]>(`${dataset}/markers.json`)
	const full = await fetchFullStats(dataset)
	if (!full) return []
	return full.markers
}
