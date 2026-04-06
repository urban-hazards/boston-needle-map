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
	0.12: "rgba(0,170,68,0.15)",
	0.3: "rgba(0,204,0,0.2)",
	0.5: "rgba(255,255,0,0.25)",
	0.7: "rgba(255,136,0,0.3)",
	0.88: "rgba(220,30,0,0.32)",
	1.0: "rgba(150,0,0,0.35)",
}

const ENCAMPMENT_GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(60,60,180,0.18)",
	0.3: "rgba(80,80,220,0.25)",
	0.5: "rgba(120,60,220,0.32)",
	0.7: "rgba(160,40,200,0.35)",
	0.88: "rgba(180,20,160,0.38)",
	1.0: "rgba(140,10,120,0.42)",
}

const WASTE_GRADIENT: Record<number, string> = {
	0.0: "rgba(0,0,0,0)",
	0.12: "rgba(100,60,20,0.18)",
	0.3: "rgba(140,80,20,0.25)",
	0.5: "rgba(180,120,30,0.32)",
	0.7: "rgba(160,100,20,0.35)",
	0.88: "rgba(120,70,10,0.38)",
	1.0: "rgba(90,50,10,0.42)",
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

function filterPoints(points: number[][], year: string, month: number): number[][] {
	const filtered = points.filter(([, , yr, mo]) => {
		if (year !== "all" && yr !== Number(year)) return false
		if (month !== 0 && mo !== month) return false
		return true
	})
	return filtered.map(([lat, lng]) => [lat, lng, 1])
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

	const defaultYear = String(years[years.length - 1] || "all")
	const [selYear, setSelYear] = useState(defaultYear)
	const [selMonth, setSelMonth] = useState(0)
	const [dataLayer, setDataLayer] = useState<DataLayer>("both")
	const dataLayerRef = useRef<DataLayer>("both")
	const selYearRef = useRef(defaultYear)
	const selMonthRef = useRef(0)
	const [count, setCount] = useState(
		() => needlePoints.filter(([, , yr]) => String(yr) === defaultYear).length,
	)
	const [encampmentCount, setEncampmentCount] = useState(
		() => encampmentPoints.filter(([, , yr]) => String(yr) === defaultYear).length,
	)
	const [wasteCount, setWasteCount] = useState(wasteTotal)
	const [ready, setReady] = useState(false)
	const [isMobile, setIsMobile] = useState(false)
	const [filterOpen, setFilterOpen] = useState(false)
	const [showPins, setShowPins] = useState(true)

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

			// Both heat layers on by default, filtered to latest year
			const needleBinned = filterPoints(needlePoints, defaultYear, 0)
			const needleLayer = createHeatLayer(needleBinned, NEEDLE_GRADIENT)
			needleLayer.addTo(map)
			heatLayerRef.current = needleLayer

			const encampmentBinned = filterPoints(encampmentPoints, defaultYear, 0)
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

	function removeAllMarkers(map: L.Map) {
		if (markerGroupRef.current) map.removeLayer(markerGroupRef.current)
		if (encampmentMarkerGroupRef.current) map.removeLayer(encampmentMarkerGroupRef.current)
		if (wasteMarkerGroupRef.current) map.removeLayer(wasteMarkerGroupRef.current)
	}

	function handleZoom(map: L.Map) {
		const zoom = map.getZoom()
		if (zoom >= 15 && showPins) {
			rebuildMarkers(map)
		} else {
			removeAllMarkers(map)
		}
	}

	function filterMarker(m: MarkerData): boolean {
		const yr = selYearRef.current
		const mo = selMonthRef.current
		if (yr !== "all") {
			const markerYear = m.dt.slice(0, 4)
			if (markerYear !== yr) return false
		}
		if (mo !== 0) {
			const markerMonth = Number.parseInt(m.dt.slice(5, 7), 10)
			if (markerMonth !== mo) return false
		}
		return true
	}

	function rebuildMarkers(map: L.Map) {
		const layer = dataLayerRef.current

		// Clear all marker groups
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

		if ((layer === "needles" || layer === "both") && markerGroupRef.current) {
			for (const m of needleMarkers.filter(filterMarker)) {
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
			map.addLayer(markerGroupRef.current)
		}

		if ((layer === "encampments" || layer === "both") && encampmentMarkerGroupRef.current) {
			for (const m of encampmentMarkers.filter(filterMarker)) {
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
			map.addLayer(encampmentMarkerGroupRef.current)
		}

		if (layer === "waste" && wasteMarkerGroupRef.current) {
			for (const m of wasteMarkers.filter(filterMarker)) {
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
			map.addLayer(wasteMarkerGroupRef.current)
		}
	}

	// Toggle marker pins on/off
	// biome-ignore lint/correctness/useExhaustiveDependencies: intentional dependency on showPins
	useEffect(() => {
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current
		if (showPins && map.getZoom() >= 15) {
			rebuildMarkers(map)
		} else {
			removeAllMarkers(map)
		}
	}, [showPins, ready])

	// Update layers when dataLayer changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: intentional dependency on dataLayer
	useEffect(() => {
		dataLayerRef.current = dataLayer
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		// Remove existing layers
		if (heatLayerRef.current) map.removeLayer(heatLayerRef.current)
		if (encampmentHeatLayerRef.current) map.removeLayer(encampmentHeatLayerRef.current)
		if (wasteHeatLayerRef.current) map.removeLayer(wasteHeatLayerRef.current)
		heatLayerRef.current = null
		encampmentHeatLayerRef.current = null
		wasteHeatLayerRef.current = null

		// Reset filter to default year when switching layers
		selYearRef.current = defaultYear
		selMonthRef.current = 0
		setSelYear(defaultYear)
		setSelMonth(0)

		// Add appropriate layers
		if (dataLayer === "needles" || dataLayer === "both") {
			const pts = filterPoints(needlePoints, defaultYear, 0)
			const layer = createHeatLayer(pts, NEEDLE_GRADIENT)
			layer.addTo(map)
			heatLayerRef.current = layer
			setCount(needlePoints.filter(([, , yr]) => String(yr) === defaultYear).length)
		}

		if (dataLayer === "encampments" || dataLayer === "both") {
			const pts = filterPoints(encampmentPoints, defaultYear, 0)
			const layer = createHeatLayer(pts, ENCAMPMENT_GRADIENT)
			layer.addTo(map)
			encampmentHeatLayerRef.current = layer
			setEncampmentCount(encampmentPoints.filter(([, , yr]) => String(yr) === defaultYear).length)
		}

		if (dataLayer === "waste") {
			const pts = filterPoints(wastePoints, defaultYear, 0)
			const layer = createHeatLayer(pts, WASTE_GRADIENT)
			layer.addTo(map)
			wasteHeatLayerRef.current = layer
			setWasteCount(wastePoints.filter(([, , yr]) => String(yr) === defaultYear).length)
		}

		// Re-show markers if zoomed in and pins enabled
		if (showPins && map.getZoom() >= 15) {
			rebuildMarkers(map)
		}
	}, [dataLayer, ready])

	// Filter heatmap data client-side when filter changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: filtering depends on selYear/selMonth/dataLayer
	useEffect(() => {
		selYearRef.current = selYear
		selMonthRef.current = selMonth
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		if (dataLayer === "needles" || dataLayer === "both") {
			if (heatLayerRef.current) map.removeLayer(heatLayerRef.current)
			const pts = filterPoints(needlePoints, selYear, selMonth)
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
			const pts = filterPoints(encampmentPoints, selYear, selMonth)
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
			const pts = filterPoints(wastePoints, selYear, selMonth)
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

		// Rebuild markers with new filter
		if (map.getZoom() >= 15) {
			rebuildMarkers(map)
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

						<label
							style={{
								display: "flex",
								alignItems: "center",
								gap: 6,
								padding: "4px 0 8px",
								cursor: "pointer",
								fontSize: "12px",
								color: "#555",
								borderBottom: "1px solid rgba(0,0,0,0.06)",
								marginBottom: 10,
							}}
						>
							<input
								type="checkbox"
								checked={showPins}
								onChange={(e) => setShowPins(e.target.checked)}
								style={{ cursor: "pointer", accentColor: "#e85a1b" }}
							/>
							Show pins when zoomed in
						</label>

						<div style={{ marginBottom: 8 }}>
							<div style={filterLabelStyle}>Year</div>
							<select
								value={selYear}
								onChange={(e) => setSelYear(e.target.value)}
								style={selectStyle}
							>
								<option value="all">All Years</option>
								{activeYears.map((yr) => (
									<option key={yr} value={String(yr)}>
										{yr}
									</option>
								))}
							</select>
						</div>
						<div>
							<div style={filterLabelStyle}>Month</div>
							<select
								value={selMonth}
								onChange={(e) => setSelMonth(Number(e.target.value))}
								style={selectStyle}
							>
								<option value={0}>All Months</option>
								{MONTHS.map((name, i) => (
									<option key={name} value={i + 1}>
										{name}
									</option>
								))}
							</select>
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

const selectStyle: React.CSSProperties = {
	width: "100%",
	padding: "5px 8px",
	fontSize: "13px",
	border: "1px solid #ddd",
	borderRadius: "4px",
	background: "#fff",
	color: "#333",
	cursor: "pointer",
}

const filterLabelStyle: React.CSSProperties = {
	fontWeight: 700,
	fontSize: "11px",
	color: "#444",
	textTransform: "uppercase",
	letterSpacing: "0.05em",
	marginBottom: 5,
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
	return (
		L as Record<string, unknown> as {
			heatLayer: (pts: number[][], opts: Record<string, unknown>) => L.Layer
		}
	).heatLayer(pts, {
		radius: 25,
		blur: 20,
		maxZoom: 16,
		minOpacity: 0.1,
		gradient,
	})
}
