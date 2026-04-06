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

export interface RoutingStats {
	total_classified: number
	bpw_rejection_count: number
	bpw_rejection_pct: number
	contractor_dispatch_count: number
	contractor_dispatch_pct: number
	closed_no_action_count: number
	closed_no_action_pct: number
	avg_hrs_no_action: number | null
	avg_hrs_dispatched: number | null
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
	routing_stats?: RoutingStats
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
