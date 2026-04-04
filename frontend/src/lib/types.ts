export interface NeighborhoodStat {
	name: string
	count: number
	pct: number
	top_street: string
	avg_resp: number
	slug: string
}

export interface ZipStat {
	zip: string
	count: number
}

export interface MarkerData {
	lat: number
	lng: number
	dt: string
	hood: string
	street: string
	zip: string
}

export interface PageStats {
	total: number
	years: number[]
	hoods: NeighborhoodStat[]
	hourly: number[]
	year_monthly: Record<string, number[]>
	zip_stats: ZipStat[]
	generated: string
	peak_hood: string
	peak_hour: number
	peak_dow: string
	avg_monthly: number
	initial_heat: number[][]
}

export interface DashboardStats {
	total: number
	years: number[]
	heat_keys: Record<string, number[][]>
	points: number[][]
	hoods: NeighborhoodStat[]
	hourly: number[]
	year_monthly: Record<string, number[]>
	zip_stats: ZipStat[]
	markers: MarkerData[]
	generated: string
	peak_hood: string
	peak_hour: number
	peak_dow: string
	avg_monthly: number
}

export interface HeatmapResponse {
	key: string
	points: number[][]
}

export interface SummaryStats {
	total: number
	years: number[]
	peak_hood: string
	peak_hour: number
	peak_dow: string
	avg_monthly: number
	generated: string
	neighborhood_count: number
}
