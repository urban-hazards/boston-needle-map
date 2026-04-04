import { useEffect, useRef, useState } from "react"
import type { MarkerData } from "../lib/types"

declare const L: typeof import("leaflet")

interface HeatMapProps {
	needlePoints: number[][]
	encampmentPoints: number[][]
	wastePoints: number[][]
	needleMarkers: MarkerData[]
	encampmentMarkers: MarkerData[]
	wasteMarkers: MarkerData[]
	years: number[]
	encampmentYears: number[]
	wasteYears: number[]
	total: number
	encampmentTotal: number
	wasteTotal: number
}

const NEEDLE_GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(0,170,68,0.3)",
	0.3: "rgba(0,204,0,0.4)",
	0.5: "rgba(255,255,0,0.5)",
	0.7: "rgba(255,136,0,0.55)",
	0.88: "rgba(220,30,0,0.6)",
	1.0: "rgba(150,0,0,0.65)",
}

const ENCAMPMENT_GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(60,60,180,0.25)",
	0.3: "rgba(80,80,220,0.35)",
	0.5: "rgba(120,60,220,0.45)",
	0.7: "rgba(160,40,200,0.5)",
	0.88: "rgba(180,20,160,0.55)",
	1.0: "rgba(140,10,120,0.6)",
}

const WASTE_GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(100,60,20,0.25)",
	0.3: "rgba(140,80,20,0.35)",
	0.5: "rgba(180,120,30,0.45)",
	0.7: "rgba(160,100,20,0.5)",
	0.88: "rgba(120,70,10,0.55)",
	1.0: "rgba(90,50,10,0.6)",
}

type DataLayer = "needles" | "encampments" | "waste" | "both"

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

function filterAndBin(
	points: number[][],
	year: string,
	month: number,
	binSize = 0.0008,
): number[][] {
	const filtered = points.filter(([, , yr, mo]) => {
		if (year !== "all" && yr !== Number(year)) return false
		if (month !== 0 && mo !== month) return false
		return true
	})

	const grid = new Map<string, number>()
	for (const [lat, lng] of filtered) {
		const bLat = Math.round(lat / binSize) * binSize
		const bLng = Math.round(lng / binSize) * binSize
		const key = `${bLat.toFixed(6)},${bLng.toFixed(6)}`
		grid.set(key, (grid.get(key) || 0) + 1)
	}

	return Array.from(grid.entries()).map(([key, count]) => {
		const [lat, lng] = key.split(",").map(Number)
		return [lat, lng, count]
	})
}

export default function HeatMap({
	needlePoints,
	encampmentPoints,
	wastePoints,
	needleMarkers,
	encampmentMarkers,
	wasteMarkers,
	years,
	encampmentYears,
	wasteYears,
	total,
	encampmentTotal,
	wasteTotal,
}: HeatMapProps) {
	const mapRef = useRef<HTMLDivElement>(null)
	const mapInstance = useRef<L.Map | null>(null)
	const heatLayerRef = useRef<L.Layer | null>(null)
	const encampmentHeatLayerRef = useRef<L.Layer | null>(null)
	const wasteHeatLayerRef = useRef<L.Layer | null>(null)
	const markerGroupRef = useRef<L.LayerGroup | null>(null)
	const encampmentMarkerGroupRef = useRef<L.LayerGroup | null>(null)
	const wasteMarkerGroupRef = useRef<L.LayerGroup | null>(null)
	const markersLoaded = useRef(false)
	const encampmentMarkersLoaded = useRef(false)
	const wasteMarkersLoaded = useRef(false)

	const [selYear, setSelYear] = useState("all")
	const [selMonth, setSelMonth] = useState(0)
	const [dataLayer, setDataLayer] = useState<DataLayer>("both")
	const [count, setCount] = useState(total)
	const [encampmentCount, setEncampmentCount] = useState(encampmentTotal)
	const [wasteCount, setWasteCount] = useState(wasteTotal)
	const [ready, setReady] = useState(false)
	const [isMobile, setIsMobile] = useState(false)
	const [filterOpen, setFilterOpen] = useState(false)

	const activeYears =
		dataLayer === "encampments" ? encampmentYears : dataLayer === "waste" ? wasteYears : years

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

			// Both heat layers on by default
			const needleBinned = filterAndBin(needlePoints, "all", 0)
			const needleLayer = createHeatLayer(needleBinned, NEEDLE_GRADIENT)
			needleLayer.addTo(map)
			heatLayerRef.current = needleLayer

			const encampmentBinned = filterAndBin(encampmentPoints, "all", 0)
			const encampmentLayer = createHeatLayer(encampmentBinned, ENCAMPMENT_GRADIENT)
			encampmentLayer.addTo(map)
			encampmentHeatLayerRef.current = encampmentLayer

			// Marker groups
			markerGroupRef.current = L.layerGroup()
			encampmentMarkerGroupRef.current = L.layerGroup()
			wasteMarkerGroupRef.current = L.layerGroup()

			map.on("zoomend", () => handleZoom(map))

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

	function handleZoom(map: L.Map) {
		const zoom = map.getZoom()
		if (zoom >= 15) {
			loadMarkers(map)
		} else {
			if (markerGroupRef.current) map.removeLayer(markerGroupRef.current)
			if (encampmentMarkerGroupRef.current) map.removeLayer(encampmentMarkerGroupRef.current)
			if (wasteMarkerGroupRef.current) map.removeLayer(wasteMarkerGroupRef.current)
		}
	}

	function loadMarkers(map: L.Map) {
		const layer = dataLayer
		if (
			(layer === "needles" || layer === "both") &&
			!markersLoaded.current &&
			markerGroupRef.current
		) {
			markersLoaded.current = true
			for (const m of needleMarkers) {
				L.circleMarker([m.lat, m.lng], {
					radius: 5,
					fillColor: "#e85a1b",
					fillOpacity: 0.85,
					color: "#fff",
					weight: 1,
					opacity: 0.6,
				})
					.bindPopup(
						`<div style="font-size:12px;line-height:1.6;color:#222"><b style="font-size:13px;color:#e85a1b;display:block">Sharps: ${m.hood || "Unknown"}</b>${m.street || ""}<br>${m.dt}${m.zip ? ` &middot; ${m.zip}` : ""}</div>`,
					)
					.addTo(markerGroupRef.current as L.LayerGroup)
			}
		}

		if (
			(layer === "encampments" || layer === "both") &&
			!encampmentMarkersLoaded.current &&
			encampmentMarkerGroupRef.current
		) {
			encampmentMarkersLoaded.current = true
			for (const m of encampmentMarkers) {
				L.circleMarker([m.lat, m.lng], {
					radius: 5,
					fillColor: "#7b2d8e",
					fillOpacity: 0.85,
					color: "#fff",
					weight: 1,
					opacity: 0.6,
				})
					.bindPopup(
						`<div style="font-size:12px;line-height:1.6;color:#222"><b style="font-size:13px;color:#7b2d8e;display:block">Encampment: ${m.hood || "Unknown"}</b>${m.street || ""}<br>${m.dt}${m.zip ? ` &middot; ${m.zip}` : ""}</div>`,
					)
					.addTo(encampmentMarkerGroupRef.current as L.LayerGroup)
			}
		}

		if (layer === "waste" && !wasteMarkersLoaded.current && wasteMarkerGroupRef.current) {
			wasteMarkersLoaded.current = true
			for (const m of wasteMarkers) {
				L.circleMarker([m.lat, m.lng], {
					radius: 5,
					fillColor: "#8B6914",
					fillOpacity: 0.85,
					color: "#fff",
					weight: 1,
					opacity: 0.6,
				})
					.bindPopup(
						`<div style="font-size:12px;line-height:1.6;color:#222"><b style="font-size:13px;color:#8B6914;display:block">Human Waste: ${m.hood || "Unknown"}</b>${m.street || ""}<br>${m.dt}${m.zip ? ` &middot; ${m.zip}` : ""}</div>`,
					)
					.addTo(wasteMarkerGroupRef.current as L.LayerGroup)
			}
		}

		if ((layer === "needles" || layer === "both") && markerGroupRef.current) {
			map.addLayer(markerGroupRef.current)
		}
		if ((layer === "encampments" || layer === "both") && encampmentMarkerGroupRef.current) {
			map.addLayer(encampmentMarkerGroupRef.current)
		}
		if (layer === "waste" && wasteMarkerGroupRef.current) {
			map.addLayer(wasteMarkerGroupRef.current)
		}
	}

	// Update layers when dataLayer changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: intentional dependency on dataLayer
	useEffect(() => {
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		// Remove existing layers
		if (heatLayerRef.current) map.removeLayer(heatLayerRef.current)
		if (encampmentHeatLayerRef.current) map.removeLayer(encampmentHeatLayerRef.current)
		if (wasteHeatLayerRef.current) map.removeLayer(wasteHeatLayerRef.current)
		heatLayerRef.current = null
		encampmentHeatLayerRef.current = null
		wasteHeatLayerRef.current = null

		// Remove marker groups and clear them when switching layers
		if (markerGroupRef.current) {
			map.removeLayer(markerGroupRef.current)
			markerGroupRef.current.clearLayers()
		}
		if (encampmentMarkerGroupRef.current) {
			map.removeLayer(encampmentMarkerGroupRef.current)
			encampmentMarkerGroupRef.current.clearLayers()
		}
		if (wasteMarkerGroupRef.current) {
			map.removeLayer(wasteMarkerGroupRef.current)
			wasteMarkerGroupRef.current.clearLayers()
		}
		markersLoaded.current = false
		encampmentMarkersLoaded.current = false
		wasteMarkersLoaded.current = false

		// Reset filter to "all" when switching layers
		setSelYear("all")
		setSelMonth(0)

		// Add appropriate layers
		if (dataLayer === "needles" || dataLayer === "both") {
			const pts = filterAndBin(needlePoints, "all", 0)
			const layer = createHeatLayer(pts, NEEDLE_GRADIENT)
			layer.addTo(map)
			heatLayerRef.current = layer
			setCount(needlePoints.length)
		}

		if (dataLayer === "encampments" || dataLayer === "both") {
			const pts = filterAndBin(encampmentPoints, "all", 0)
			const layer = createHeatLayer(pts, ENCAMPMENT_GRADIENT)
			layer.addTo(map)
			encampmentHeatLayerRef.current = layer
			setEncampmentCount(encampmentPoints.length)
		}

		if (dataLayer === "waste") {
			const pts = filterAndBin(wastePoints, "all", 0)
			const layer = createHeatLayer(pts, WASTE_GRADIENT)
			layer.addTo(map)
			wasteHeatLayerRef.current = layer
			setWasteCount(wastePoints.length)
		}

		// Re-show markers if zoomed in
		if (map.getZoom() >= 15) {
			loadMarkers(map)
		}
	}, [dataLayer, ready])

	// Filter heatmap data client-side when filter changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: filtering depends on selYear/selMonth/dataLayer
	useEffect(() => {
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		if (dataLayer === "needles" || dataLayer === "both") {
			if (heatLayerRef.current) map.removeLayer(heatLayerRef.current)
			const pts = filterAndBin(needlePoints, selYear, selMonth)
			const layer = createHeatLayer(pts, NEEDLE_GRADIENT)
			layer.addTo(map)
			heatLayerRef.current = layer
			const filteredCount = needlePoints.filter(([, , yr, mo]) => {
				if (selYear !== "all" && yr !== Number(selYear)) return false
				if (selMonth !== 0 && mo !== selMonth) return false
				return true
			}).length
			setCount(filteredCount)
		}

		if (dataLayer === "encampments" || dataLayer === "both") {
			if (encampmentHeatLayerRef.current) map.removeLayer(encampmentHeatLayerRef.current)
			const pts = filterAndBin(encampmentPoints, selYear, selMonth)
			const layer = createHeatLayer(pts, ENCAMPMENT_GRADIENT)
			layer.addTo(map)
			encampmentHeatLayerRef.current = layer
			const filteredCount = encampmentPoints.filter(([, , yr, mo]) => {
				if (selYear !== "all" && yr !== Number(selYear)) return false
				if (selMonth !== 0 && mo !== selMonth) return false
				return true
			}).length
			setEncampmentCount(filteredCount)
		}

		if (dataLayer === "waste") {
			if (wasteHeatLayerRef.current) map.removeLayer(wasteHeatLayerRef.current)
			const pts = filterAndBin(wastePoints, selYear, selMonth)
			const layer = createHeatLayer(pts, WASTE_GRADIENT)
			layer.addTo(map)
			wasteHeatLayerRef.current = layer
			const filteredCount = wastePoints.filter(([, , yr, mo]) => {
				if (selYear !== "all" && yr !== Number(selYear)) return false
				if (selMonth !== 0 && mo !== selMonth) return false
				return true
			}).length
			setWasteCount(filteredCount)
		}
	}, [selYear, selMonth, ready, dataLayer])

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
							<div style={filterLabelStyle}>Data Layer</div>
							<LayerRadio
								value="needles"
								label="Sharps"
								color="#e85a1b"
								checked={dataLayer === "needles"}
								onChange={setDataLayer}
							/>
							<LayerRadio
								value="encampments"
								label="Encampments"
								color="#7b2d8e"
								checked={dataLayer === "encampments"}
								onChange={setDataLayer}
							/>
							<LayerRadio
								value="waste"
								label="Human Waste (Beta)"
								color="#8B6914"
								checked={dataLayer === "waste"}
								onChange={setDataLayer}
							/>
							<LayerRadio
								value="both"
								label="Sharps + Encampments"
								color="#333"
								checked={dataLayer === "both"}
								onChange={setDataLayer}
							/>
						</div>

						<div style={{ marginBottom: 10 }}>
							<div style={filterLabelStyle}>Year</div>
							<FilterRadio
								name="yr"
								value="all"
								label="All Years"
								checked={selYear === "all"}
								onChange={setSelYear}
							/>
							{activeYears.map((yr) => (
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
					{(dataLayer === "needles" || dataLayer === "both") && (
						<span style={{ color: "#e85a1b" }}>
							<strong>{count.toLocaleString()}</strong> sharps
						</span>
					)}
					{dataLayer === "both" && <span> + </span>}
					{(dataLayer === "encampments" || dataLayer === "both") && (
						<span style={{ color: "#7b2d8e" }}>
							<strong>{encampmentCount.toLocaleString()}</strong> encampments
						</span>
					)}
					{dataLayer === "waste" && (
						<span style={{ color: "#8B6914" }}>
							<strong>{wasteCount.toLocaleString()}</strong> human waste reports
							<span style={{ fontSize: "10px", opacity: 0.7 }}> (beta)</span>
						</span>
					)}
				</div>
			</div>

			{/* Legend */}
			<div
				style={{
					display: "flex",
					flexDirection: "column",
					gap: 4,
					fontSize: "11px",
					color: "#666",
					marginTop: 8,
				}}
			>
				{(dataLayer === "needles" || dataLayer === "both") && (
					<div style={{ display: "flex", alignItems: "center", gap: 6 }}>
						<span style={{ color: "#e85a1b", fontWeight: 600, minWidth: 90 }}>Sharps</span>
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
				)}
				{(dataLayer === "encampments" || dataLayer === "both") && (
					<div style={{ display: "flex", alignItems: "center", gap: 6 }}>
						<span style={{ color: "#7b2d8e", fontWeight: 600, minWidth: 90 }}>Encampments</span>
						<span>Low</span>
						<div
							style={{
								height: 8,
								flex: 1,
								borderRadius: 4,
								background:
									"linear-gradient(90deg, transparent 0%, #3c3cb4 20%, #7840dc 50%, #a028c8 75%, #8c0a78 100%)",
							}}
						/>
						<span>High</span>
					</div>
				)}
				{dataLayer === "waste" && (
					<div style={{ display: "flex", alignItems: "center", gap: 6 }}>
						<span style={{ color: "#8B6914", fontWeight: 600, minWidth: 90 }}>Human Waste</span>
						<span>Low</span>
						<div
							style={{
								height: 8,
								flex: 1,
								borderRadius: 4,
								background:
									"linear-gradient(90deg, transparent 0%, #8B6914 20%, #B4891E 50%, #A06414 75%, #5A320A 100%)",
							}}
						/>
						<span>High</span>
						<span style={{ fontSize: "10px", color: "#999", marginLeft: 4 }}>BETA</span>
					</div>
				)}
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

function LayerRadio({
	value,
	label,
	color,
	checked,
	onChange,
}: {
	value: DataLayer
	label: string
	color: string
	checked: boolean
	onChange: (v: DataLayer) => void
}) {
	return (
		<label
			style={{
				display: "flex",
				alignItems: "center",
				gap: 6,
				padding: "2px 0",
				cursor: "pointer",
				color: checked ? color : "#333",
				fontWeight: checked ? 600 : 400,
				fontSize: "13px",
			}}
		>
			<input
				type="radio"
				name="layer"
				value={value}
				checked={checked}
				onChange={() => onChange(value)}
				style={{ cursor: "pointer", accentColor: color }}
			/>
			{label}
		</label>
	)
}

function createHeatLayer(pts: number[][], gradient: Record<number, string>): L.Layer {
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
		gradient,
	})
}
