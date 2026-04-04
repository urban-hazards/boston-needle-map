import { useEffect, useRef, useState } from "react"
import type { HeatmapResponse, MarkerData } from "../lib/types"

declare const L: typeof import("leaflet")

interface HeatMapProps {
	initialHeat: number[][]
	years: number[]
	total: number
	apiBase: string
}

const GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(0,170,68,0.3)",
	0.3: "rgba(0,204,0,0.4)",
	0.5: "rgba(255,255,0,0.5)",
	0.7: "rgba(255,136,0,0.55)",
	0.88: "rgba(220,30,0,0.6)",
	1.0: "rgba(150,0,0,0.65)",
}

const MONTHS = [
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
]

export default function HeatMap({ initialHeat, years, total, apiBase }: HeatMapProps) {
	const mapRef = useRef<HTMLDivElement>(null)
	const mapInstance = useRef<L.Map | null>(null)
	const heatLayerRef = useRef<L.Layer | null>(null)
	const markerGroupRef = useRef<L.LayerGroup | null>(null)
	const markersLoaded = useRef(false)

	const [selYear, setSelYear] = useState("all")
	const [selMonth, setSelMonth] = useState(0)
	const [count, setCount] = useState(total)
	const [ready, setReady] = useState(false)
	const [loading, setLoading] = useState(false)
	const [isMobile, setIsMobile] = useState(false)
	const [filterOpen, setFilterOpen] = useState(false)

	const heatCacheRef = useRef<Record<string, number[][]>>({ all: initialHeat })

	useEffect(() => {
		const check = () => setIsMobile(window.innerWidth < 640)
		check()
		window.addEventListener("resize", check)
		return () => window.removeEventListener("resize", check)
	}, [])

	// biome-ignore lint/correctness/useExhaustiveDependencies: one-time map initialization
	useEffect(() => {
		if (!mapRef.current || mapInstance.current) return

		const loadScripts = async () => {
			if (!window.L) {
				const script = document.createElement("script")
				script.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
				document.head.appendChild(script)
				await new Promise<void>((resolve) => {
					script.onload = () => resolve()
				})
			}
			if (!(window as unknown as Record<string, unknown>).HeatLayer) {
				const heatScript = document.createElement("script")
				heatScript.src = "https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js"
				document.head.appendChild(heatScript)
				await new Promise<void>((resolve) => {
					heatScript.onload = () => resolve()
				})
			}

			if (!mapRef.current) return
			const map = L.map(mapRef.current, { center: [42.332, -71.078], zoom: 13 })
			mapInstance.current = map

			L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
				attribution:
					'&copy; <a href="https://carto.com/">CARTO</a> &middot; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
				subdomains: "abcd",
				maxZoom: 19,
			}).addTo(map)

			const layer = createHeatLayer(initialHeat)
			layer.addTo(map)
			heatLayerRef.current = layer

			// Load markers lazily when user zooms in
			const markerGroup = L.layerGroup()
			markerGroupRef.current = markerGroup

			map.on("zoomend", async () => {
				if (map.getZoom() >= 15) {
					if (!markersLoaded.current) {
						markersLoaded.current = true
						try {
							const res = await fetch(`${apiBase}/api/markers?limit=3000`)
							const markers: MarkerData[] = await res.json()
							for (const m of markers) {
								L.circleMarker([m.lat, m.lng], {
									radius: 5,
									fillColor: "#e85a1b",
									fillOpacity: 0.85,
									color: "#fff",
									weight: 1,
									opacity: 0.6,
								})
									.bindPopup(
										`<div style="font-size:12px;line-height:1.6;color:#222"><b style="font-size:13px;color:#e85a1b;display:block">${m.hood || "Unknown"}</b>${m.street || ""}<br>${m.dt}${m.zip ? ` &middot; ${m.zip}` : ""}</div>`,
									)
									.addTo(markerGroup)
							}
						} catch {
							markersLoaded.current = false
						}
					}
					map.addLayer(markerGroup)
				} else {
					map.removeLayer(markerGroup)
				}
			})

			setReady(true)
		}

		loadScripts()

		return () => {
			if (mapInstance.current) {
				mapInstance.current.remove()
				mapInstance.current = null
			}
		}
	}, [])

	// Fetch heatmap data on demand when filter changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: apiBase is stable
	useEffect(() => {
		if (!ready || !mapInstance.current) return

		const yr = selYear
		const mo = selMonth
		const key =
			mo === 0
				? yr === "all"
					? "all"
					: yr
				: `${yr === "all" ? "all" : yr}-${String(mo).padStart(2, "0")}`

		const cached = heatCacheRef.current[key]
		if (cached) {
			updateHeatLayer(cached)
			return
		}

		setLoading(true)
		fetch(`${apiBase}/api/heatmap?year=${yr}&month=${mo}`)
			.then((res) => res.json())
			.then((data: HeatmapResponse) => {
				heatCacheRef.current[data.key] = data.points
				updateHeatLayer(data.points)
			})
			.catch(() => {})
			.finally(() => setLoading(false))
	}, [selYear, selMonth, ready])

	function updateHeatLayer(pts: number[][]) {
		if (!mapInstance.current) return
		if (heatLayerRef.current) {
			mapInstance.current.removeLayer(heatLayerRef.current)
		}
		const layer = createHeatLayer(pts)
		layer.addTo(mapInstance.current)
		heatLayerRef.current = layer
		setCount(pts.reduce((s, p) => s + p[2], 0))
	}

	const showFilterPanel = !isMobile || filterOpen

	return (
		<section className="map-section section" aria-label="Interactive heatmap">
			<h2 className="section-title">Heatmap</h2>
			<div style={{ position: "relative", zIndex: 0, isolation: "isolate" }}>
				<div
					ref={mapRef}
					style={{
						width: "100%",
						height: isMobile ? "380px" : "500px",
						borderRadius: "8px",
						border: "1px solid #e0e0e0",
					}}
				/>

				{isMobile && !filterOpen && (
					<button
						type="button"
						onClick={() => setFilterOpen(true)}
						style={{
							position: "absolute",
							top: 10,
							right: 10,
							zIndex: 500,
							background: "rgba(255,255,255,0.96)",
							backdropFilter: "blur(8px)",
							border: "1px solid rgba(0,0,0,0.1)",
							borderRadius: "20px",
							padding: "6px 14px",
							fontSize: "12px",
							fontWeight: 600,
							color: "#333",
							cursor: "pointer",
							boxShadow: "0 2px 8px rgba(0,0,0,0.1)",
						}}
					>
						Filters
					</button>
				)}

				{showFilterPanel && (
					<div style={filterPanelStyle(isMobile)}>
						{isMobile && (
							<button
								type="button"
								onClick={() => setFilterOpen(false)}
								style={{
									position: "absolute",
									top: 8,
									right: 10,
									background: "none",
									border: "none",
									fontSize: "18px",
									cursor: "pointer",
									color: "#666",
									lineHeight: 1,
									padding: "2px 4px",
								}}
								aria-label="Close filters"
							>
								&times;
							</button>
						)}
						<div style={{ marginBottom: 10 }}>
							<div style={filterLabelStyle}>Year</div>
							<FilterRadio
								name="yr"
								value="all"
								label="All Years"
								checked={selYear === "all"}
								onChange={setSelYear}
							/>
							{years.map((yr) => (
								<FilterRadio
									key={yr}
									name="yr"
									value={String(yr)}
									label={String(yr)}
									checked={selYear === String(yr)}
									onChange={setSelYear}
								/>
							))}
						</div>
						<div>
							<div style={filterLabelStyle}>Month</div>
							<FilterRadio
								name="mo"
								value="0"
								label="All Months"
								checked={selMonth === 0}
								onChange={(v) => setSelMonth(Number(v))}
							/>
							{MONTHS.map((name, i) => (
								<FilterRadio
									key={name}
									name="mo"
									value={String(i + 1)}
									label={name}
									checked={selMonth === i + 1}
									onChange={(v) => setSelMonth(Number(v))}
								/>
							))}
						</div>
					</div>
				)}

				<div
					style={{
						position: "absolute",
						bottom: 28,
						left: 10,
						zIndex: 500,
						background: "rgba(255,255,255,0.95)",
						backdropFilter: "blur(4px)",
						border: "1px solid rgba(0,0,0,0.08)",
						borderRadius: "6px",
						padding: "6px 12px",
						fontSize: "12px",
						color: "#333",
						boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
					}}
				>
					{loading ? (
						"Loading..."
					) : (
						<>
							Showing <strong>{count.toLocaleString()}</strong> requests
						</>
					)}
				</div>
			</div>
			<div
				style={{
					display: "flex",
					alignItems: "center",
					gap: 6,
					fontSize: "11px",
					color: "#666",
					marginTop: 8,
				}}
			>
				<span>Low</span>
				<div
					style={{
						height: 8,
						flex: 1,
						borderRadius: 4,
						background:
							"linear-gradient(90deg, transparent 0%, #00aa44 20%, #ffff00 50%, #ff8800 75%, #cc0000 100%)",
					}}
				/>
				<span>High</span>
			</div>
		</section>
	)
}

function filterPanelStyle(mobile: boolean): React.CSSProperties {
	return {
		position: "absolute",
		top: 10,
		right: 10,
		zIndex: 500,
		background: "rgba(255,255,255,0.98)",
		backdropFilter: "blur(8px)",
		border: "1px solid rgba(0,0,0,0.08)",
		borderRadius: "8px",
		padding: mobile ? "12px 14px 12px" : "12px 14px",
		minWidth: mobile ? 180 : 160,
		boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
		fontSize: "13px",
		lineHeight: "1.4",
		maxHeight: mobile ? "320px" : "460px",
		overflowY: "auto" as const,
	}
}

const filterLabelStyle: React.CSSProperties = {
	fontWeight: 700,
	fontSize: "11px",
	color: "#444",
	textTransform: "uppercase",
	letterSpacing: "0.05em",
	marginBottom: 5,
}

function FilterRadio({
	name,
	value,
	label,
	checked,
	onChange,
}: {
	name: string
	value: string
	label: string
	checked: boolean
	onChange: (v: string) => void
}) {
	return (
		<label
			style={{
				display: "flex",
				alignItems: "center",
				gap: 6,
				padding: "2px 0",
				cursor: "pointer",
				color: checked ? "#e85a1b" : "#333",
				fontSize: "13px",
			}}
		>
			<input
				type="radio"
				name={name}
				value={value}
				checked={checked}
				onChange={() => onChange(value)}
				style={{ cursor: "pointer", accentColor: "#e85a1b" }}
			/>
			{label}
		</label>
	)
}

function createHeatLayer(pts: number[][]): L.Layer {
	const counts = pts.map((p) => p[2]).sort((a, b) => a - b)
	const p95 = counts[Math.floor(counts.length * 0.95)] || 1
	return (
		L as Record<string, unknown> as {
			heatLayer: (pts: number[][], opts: Record<string, unknown>) => L.Layer
		}
	).heatLayer(pts, {
		radius: 38,
		blur: 28,
		maxZoom: 16,
		max: p95,
		minOpacity: 0.25,
		gradient: GRADIENT,
	})
}
