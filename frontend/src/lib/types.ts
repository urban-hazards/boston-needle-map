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
	council_district?: string
	police_district?: string
	state_rep_district?: string
	state_senate_district?: string
	source?: "confirmed" | "detected" | null
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
	year_hourly: Record<string, number[]>
	year_monthly: Record<string, number[]>
	zip_stats: ZipStat[]
	council_districts?: string[]
	council_district_labels?: string[]
	police_districts?: string[]
	police_district_labels?: string[]
	state_rep_districts?: string[]
	state_rep_district_labels?: string[]
	state_senate_districts?: string[]
	state_senate_district_labels?: string[]
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
	year_hourly: Record<string, number[]>
	year_monthly: Record<string, number[]>
	zip_stats: ZipStat[]
	markers: MarkerData[]
	council_districts?: string[]
	police_districts?: string[]
	state_rep_districts?: string[]
	state_senate_districts?: string[]
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
