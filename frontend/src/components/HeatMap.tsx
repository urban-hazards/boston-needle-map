import { useEffect, useMemo, useRef, useState } from "react"
import type { DistrictBoundaries } from "../lib/bucket"
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
	councilDistricts: string[]
	councilLabels: string[]
	policeDistricts: string[]
	policeLabels: string[]
	stateRepDistricts: string[]
	stateRepLabels: string[]
	stateSenateDistricts: string[]
	stateSenateLabels: string[]
	districtBoundaries: DistrictBoundaries
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
type WasteSource = "all" | "confirmed" | "detected"

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

type BoundaryLayer = "council" | "police" | "state_rep" | "state_senate"

interface DistrictFilters {
	council?: number
	police?: number
	rep?: number
	senate?: number
}

function filterPoints(
	points: number[][],
	year: string,
	month: number,
	sourceFilter?: WasteSource,
	df?: DistrictFilters,
): number[][] {
	const filtered = points.filter(([, , yr, mo, src, council, police, rep, senate]) => {
		if (year !== "all" && yr !== Number(year)) return false
		if (month !== 0 && mo !== month) return false
		if (sourceFilter && sourceFilter !== "all") {
			const isDetected = src === 1
			if (sourceFilter === "confirmed" && isDetected) return false
			if (sourceFilter === "detected" && !isDetected) return false
		}
		if (df) {
			if (df.council && df.council > 0 && council !== df.council) return false
			if (df.police && df.police > 0 && police !== df.police) return false
			if (df.rep && df.rep > 0 && rep !== df.rep) return false
			if (df.senate && df.senate > 0 && senate !== df.senate) return false
		}
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
	councilDistricts,
	councilLabels,
	policeDistricts,
	policeLabels,
	stateRepDistricts,
	stateRepLabels,
	stateSenateDistricts,
	stateSenateLabels,
	districtBoundaries,
}: HeatMapProps) {
	const mapRef = useRef<HTMLDivElement>(null)
	const mapInstance = useRef<L.Map | null>(null)
	const heatLayerRef = useRef<L.Layer | null>(null)
	const encampmentHeatLayerRef = useRef<L.Layer | null>(null)
	const wasteHeatLayerRef = useRef<L.Layer | null>(null)
	const markerGroupRef = useRef<L.LayerGroup | null>(null)
	const encampmentMarkerGroupRef = useRef<L.LayerGroup | null>(null)
	const wasteMarkerGroupRef = useRef<L.LayerGroup | null>(null)
	const boundaryLayerRef = useRef<L.GeoJSON | null>(null)

	const defaultYear = String(years[years.length - 1] || "all")
	const [selYear, setSelYear] = useState(defaultYear)
	const [selMonth, setSelMonth] = useState(0)
	const [dataLayer, setDataLayer] = useState<DataLayer>("both")
	const dataLayerRef = useRef<DataLayer>("both")
	const [wasteSource, setWasteSource] = useState<WasteSource>("all")
	const wasteSourceRef = useRef<WasteSource>("all")
	const [selCouncil, setSelCouncil] = useState(0)
	const [selPolice, setSelPolice] = useState(0)
	const [selRep, setSelRep] = useState(0)
	const [selSenate, setSelSenate] = useState(0)
	const selCouncilRef = useRef(0)
	const selPoliceRef = useRef(0)
	const selRepRef = useRef(0)
	const selSenateRef = useRef(0)
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
	const [showBoundaries, setShowBoundaries] = useState<BoundaryLayer | null>(null)
	const [heatIntensity, setHeatIntensity] = useState(50)
	const heatIntensityRef = useRef(50)
	const [heatMethod, setHeatMethod] = useState<HeatMethod>("tableau")
	const heatMethodRef = useRef<HeatMethod>("tableau")

	// Time scrubber state
	const [isPlaying, setIsPlaying] = useState(false)
	const playIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

	const activeYears =
		dataLayer === "encampments" ? encampmentYears : dataLayer === "waste" ? wasteYears : years

	// Compute which years/months actually have data for the active layer
	const activePoints = useMemo(() => {
		if (dataLayer === "encampments") return encampmentPoints
		if (dataLayer === "waste") return wastePoints
		if (dataLayer === "both") return [...needlePoints, ...encampmentPoints]
		return needlePoints
	}, [dataLayer, needlePoints, encampmentPoints, wastePoints])

	const availableYears = useMemo(() => {
		const s = new Set<number>()
		for (const [, , yr] of activePoints) s.add(yr)
		return s
	}, [activePoints])

	const availableMonths = useMemo(() => {
		const s = new Set<number>()
		for (const [, , yr, mo] of activePoints) {
			if (selYear === "all" || yr === Number(selYear)) s.add(mo)
		}
		return s
	}, [activePoints, selYear])

	// Build sorted timeline of all year-month pairs for scrubber
	const timeline = useMemo(() => {
		const pairs = new Set<string>()
		for (const [, , yr, mo] of activePoints) {
			pairs.add(`${yr}-${String(mo).padStart(2, "0")}`)
		}
		return [...pairs].sort().map((key) => ({
			year: Number(key.slice(0, 4)),
			month: Number(key.slice(5)),
			key,
		}))
	}, [activePoints])

	// Current timeline index based on selYear/selMonth
	const timelineIndex = useMemo(() => {
		if (selYear === "all" && selMonth === 0) return -1 // "all" state
		const target = selYear === "all" ? "" : `${selYear}-${String(selMonth || 1).padStart(2, "0")}`
		return timeline.findIndex((t) => t.key === target)
	}, [timeline, selYear, selMonth])

	// Cleanup play interval on unmount
	useEffect(() => {
		return () => {
			if (playIntervalRef.current) clearInterval(playIntervalRef.current)
		}
	}, [])

	// Play/pause animation
	function togglePlay() {
		if (isPlaying) {
			if (playIntervalRef.current) clearInterval(playIntervalRef.current)
			playIntervalRef.current = null
			setIsPlaying(false)
			return
		}
		if (timeline.length === 0) return
		setIsPlaying(true)
		let idx = timelineIndex >= 0 ? timelineIndex : 0
		const advance = () => {
			idx = (idx + 1) % timeline.length
			const t = timeline[idx]
			if (t) {
				setSelYear(String(t.year))
				setSelMonth(t.month)
			}
			if (idx === timeline.length - 1) {
				if (playIntervalRef.current) clearInterval(playIntervalRef.current)
				playIntervalRef.current = null
				setIsPlaying(false)
			}
		}
		advance()
		playIntervalRef.current = setInterval(advance, 800)
	}

	function scrubTo(idx: number) {
		if (idx < 0) {
			setSelYear("all")
			setSelMonth(0)
		} else if (timeline[idx]) {
			setSelYear(String(timeline[idx].year))
			setSelMonth(timeline[idx].month)
		}
		if (isPlaying) {
			if (playIntervalRef.current) clearInterval(playIntervalRef.current)
			playIntervalRef.current = null
			setIsPlaying(false)
		}
	}

	// Reset month if current selection has no data
	useEffect(() => {
		if (selMonth !== 0 && !availableMonths.has(selMonth)) {
			setSelMonth(0)
		}
	}, [availableMonths, selMonth])

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

			// Custom panes so boundaries always render above heatmaps
			const heatPane = map.createPane("heatmapPane")
			heatPane.style.zIndex = "400"
			const boundaryPane = map.createPane("boundaryPane")
			boundaryPane.style.zIndex = "450"

			L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
				attribution:
					'&copy; <a href="https://carto.com/">CARTO</a> &middot; <a href="https://www.openstreetmap.org/copyright">OSM</a>',
				subdomains: "abcd",
				maxZoom: 19,
			}).addTo(map)

			// Both heat layers on by default, filtered to latest year
			const needleBinned = filterPoints(needlePoints, defaultYear, 0)
			const needleLayer = createHeatLayer(
				needleBinned,
				NEEDLE_GRADIENT,
				13,
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			needleLayer.addTo(map)
			heatLayerRef.current = needleLayer

			const encampmentBinned = filterPoints(encampmentPoints, defaultYear, 0)
			const encampmentLayer = createHeatLayer(
				encampmentBinned,
				ENCAMPMENT_GRADIENT,
				13,
				heatIntensityRef.current,
				heatMethodRef.current,
			)
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

	const lastZoomRef = useRef(13)

	function handleZoom(map: L.Map) {
		const zoom = map.getZoom()

		// Rebuild heat layers when zoom crosses a threshold
		const prevParams = getHeatParams(lastZoomRef.current, heatMethodRef.current)
		const newParams = getHeatParams(zoom, heatMethodRef.current)
		if (prevParams.radius !== newParams.radius) {
			const df: DistrictFilters = {
				council: selCouncilRef.current,
				police: selPoliceRef.current,
				rep: selRepRef.current,
				senate: selSenateRef.current,
			}
			const layer = dataLayerRef.current
			if ((layer === "needles" || layer === "both") && heatLayerRef.current) {
				map.removeLayer(heatLayerRef.current)
				const pts = filterPoints(
					needlePoints,
					selYearRef.current,
					selMonthRef.current,
					undefined,
					df,
				)
				heatLayerRef.current = createHeatLayer(
					pts,
					NEEDLE_GRADIENT,
					zoom,
					heatIntensityRef.current,
					heatMethodRef.current,
				)
				heatLayerRef.current.addTo(map)
			}
			if ((layer === "encampments" || layer === "both") && encampmentHeatLayerRef.current) {
				map.removeLayer(encampmentHeatLayerRef.current)
				const pts = filterPoints(
					encampmentPoints,
					selYearRef.current,
					selMonthRef.current,
					undefined,
					df,
				)
				encampmentHeatLayerRef.current = createHeatLayer(
					pts,
					ENCAMPMENT_GRADIENT,
					zoom,
					heatIntensityRef.current,
					heatMethodRef.current,
				)
				encampmentHeatLayerRef.current.addTo(map)
			}
			if (layer === "waste" && wasteHeatLayerRef.current) {
				map.removeLayer(wasteHeatLayerRef.current)
				const pts = filterPoints(
					wastePoints,
					selYearRef.current,
					selMonthRef.current,
					wasteSourceRef.current,
					df,
				)
				wasteHeatLayerRef.current = createHeatLayer(
					pts,
					WASTE_GRADIENT,
					zoom,
					heatIntensityRef.current,
					heatMethodRef.current,
				)
				wasteHeatLayerRef.current.addTo(map)
			}
		}
		lastZoomRef.current = zoom

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
		const cf = selCouncilRef.current
		if (cf > 0) {
			const distName = councilDistricts[cf - 1] || ""
			if ((m.council_district || "") !== distName) return false
		}
		const pf = selPoliceRef.current
		if (pf > 0) {
			const distName = policeDistricts[pf - 1] || ""
			if ((m.police_district || "") !== distName) return false
		}
		const rf = selRepRef.current
		if (rf > 0) {
			const distName = stateRepDistricts[rf - 1] || ""
			if ((m.state_rep_district || "") !== distName) return false
		}
		const sf = selSenateRef.current
		if (sf > 0) {
			const distName = stateSenateDistricts[sf - 1] || ""
			if ((m.state_senate_district || "") !== distName) return false
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
			const srcFilter = wasteSourceRef.current
			for (const m of wasteMarkers.filter(filterMarker)) {
				if (srcFilter !== "all") {
					const mSrc = m.source || "confirmed"
					if (srcFilter !== mSrc) continue
				}
				const isDetected = m.source === "detected"
				const color = isDetected ? "#C4841D" : "#8B6914"
				const label = isDetected ? "Missed Report" : "311 Ticket"
				L.circleMarker([m.lat, m.lng], {
					radius: 5,
					fillColor: color,
					fillOpacity: isDetected ? 0.7 : 0.85,
					color: "#fff",
					weight: 1,
					opacity: 0.6,
				})
					.bindPopup(
						`<div style="font-size:12px;line-height:1.6;color:#222"><b style="font-size:13px;color:${color};display:block">Human Waste (${label}): ${m.hood || "Unknown"}</b>${m.street || ""}<br>${m.dt}${m.zip ? ` &middot; ${m.zip}` : ""}</div>`,
					)
					.addTo(wasteMarkerGroupRef.current as L.LayerGroup)
			}
			map.addLayer(wasteMarkerGroupRef.current)
		}
	}

	// Draw district boundary outlines on the map
	function drawBoundaries(
		map: L.Map,
		layer: BoundaryLayer | null,
		selectedIdx: number,
		districts: string[],
	) {
		if (boundaryLayerRef.current) {
			map.removeLayer(boundaryLayerRef.current)
			boundaryLayerRef.current = null
		}
		if (!layer || !districtBoundaries[layer]) return

		const features = districtBoundaries[layer]
		if (!features || features.length === 0) return

		// If a specific district is selected, only show that one with a slightly stronger style
		// Otherwise show all boundaries for the toggled layer
		const selectedDistId = selectedIdx > 0 ? districts[selectedIdx - 1] : null

		const geojson: GeoJSON.FeatureCollection = {
			type: "FeatureCollection",
			features: features
				.filter((f) => (selectedDistId ? f.id === selectedDistId : true))
				.map((f) => ({
					type: "Feature" as const,
					properties: { id: f.id, selected: f.id === selectedDistId },
					geometry: f.geometry,
				})),
		}

		boundaryLayerRef.current = L.geoJSON(geojson, {
			pane: "boundaryPane",
			style: (feature) => ({
				color: feature?.properties?.selected ? "#e85a1b" : "#555",
				weight: feature?.properties?.selected ? 2.5 : 1.5,
				opacity: feature?.properties?.selected ? 0.7 : 0.35,
				fillColor: feature?.properties?.selected ? "#e85a1b" : "transparent",
				fillOpacity: feature?.properties?.selected ? 0.05 : 0,
				dashArray: feature?.properties?.selected ? undefined : "4 4",
			}),
		}).addTo(map)
	}

	// Update boundary outlines when toggle or selected district changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: boundary drawing depends on selection state
	useEffect(() => {
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		// Determine which district is selected for the active boundary layer
		let selectedIdx = 0
		let districts: string[] = []
		if (showBoundaries === "council") {
			selectedIdx = selCouncil
			districts = councilDistricts
		} else if (showBoundaries === "police") {
			selectedIdx = selPolice
			districts = policeDistricts
		} else if (showBoundaries === "state_rep") {
			selectedIdx = selRep
			districts = stateRepDistricts
		} else if (showBoundaries === "state_senate") {
			selectedIdx = selSenate
			districts = stateSenateDistricts
		}

		drawBoundaries(map, showBoundaries, selectedIdx, districts)
	}, [showBoundaries, selCouncil, selPolice, selRep, selSenate, ready])

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
		wasteSourceRef.current = wasteSource
		selCouncilRef.current = selCouncil
		selPoliceRef.current = selPolice
		selRepRef.current = selRep
		selSenateRef.current = selSenate
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

		const df: DistrictFilters = {
			council: selCouncil,
			police: selPolice,
			rep: selRep,
			senate: selSenate,
		}

		// Add appropriate layers
		if (dataLayer === "needles" || dataLayer === "both") {
			const pts = filterPoints(needlePoints, defaultYear, 0, undefined, df)
			const layer = createHeatLayer(
				pts,
				NEEDLE_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			heatLayerRef.current = layer
			setCount(pts.length)
		}

		if (dataLayer === "encampments" || dataLayer === "both") {
			const pts = filterPoints(encampmentPoints, defaultYear, 0, undefined, df)
			const layer = createHeatLayer(
				pts,
				ENCAMPMENT_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			encampmentHeatLayerRef.current = layer
			setEncampmentCount(pts.length)
		}

		if (dataLayer === "waste") {
			const pts = filterPoints(wastePoints, defaultYear, 0, wasteSource, df)
			const layer = createHeatLayer(
				pts,
				WASTE_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			wasteHeatLayerRef.current = layer
			setWasteCount(pts.length)
		}

		// Re-show markers if zoomed in and pins enabled
		if (showPins && map.getZoom() >= 15) {
			rebuildMarkers(map)
		}
	}, [dataLayer, wasteSource, selCouncil, selPolice, selRep, selSenate, ready])

	// Filter heatmap data client-side when filter changes
	// biome-ignore lint/correctness/useExhaustiveDependencies: filtering depends on selYear/selMonth/dataLayer/wasteSource/districts
	useEffect(() => {
		selYearRef.current = selYear
		selMonthRef.current = selMonth
		wasteSourceRef.current = wasteSource
		selCouncilRef.current = selCouncil
		selPoliceRef.current = selPolice
		selRepRef.current = selRep
		selSenateRef.current = selSenate
		heatIntensityRef.current = heatIntensity
		heatMethodRef.current = heatMethod
		if (!ready || !mapInstance.current) return
		const map = mapInstance.current

		const df: DistrictFilters = {
			council: selCouncil,
			police: selPolice,
			rep: selRep,
			senate: selSenate,
		}

		if (dataLayer === "needles" || dataLayer === "both") {
			if (heatLayerRef.current) map.removeLayer(heatLayerRef.current)
			const pts = filterPoints(needlePoints, selYear, selMonth, undefined, df)
			const layer = createHeatLayer(
				pts,
				NEEDLE_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			heatLayerRef.current = layer
			setCount(pts.length)
		}

		if (dataLayer === "encampments" || dataLayer === "both") {
			if (encampmentHeatLayerRef.current) map.removeLayer(encampmentHeatLayerRef.current)
			const pts = filterPoints(encampmentPoints, selYear, selMonth, undefined, df)
			const layer = createHeatLayer(
				pts,
				ENCAMPMENT_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			encampmentHeatLayerRef.current = layer
			setEncampmentCount(pts.length)
		}

		if (dataLayer === "waste") {
			if (wasteHeatLayerRef.current) map.removeLayer(wasteHeatLayerRef.current)
			const pts = filterPoints(wastePoints, selYear, selMonth, wasteSource, df)
			const layer = createHeatLayer(
				pts,
				WASTE_GRADIENT,
				map.getZoom(),
				heatIntensityRef.current,
				heatMethodRef.current,
			)
			layer.addTo(map)
			wasteHeatLayerRef.current = layer
			setWasteCount(pts.length)
		}

		// Rebuild markers with new filter
		if (map.getZoom() >= 15) {
			rebuildMarkers(map)
		}
	}, [
		selYear,
		selMonth,
		selCouncil,
		selPolice,
		selRep,
		selSenate,
		ready,
		dataLayer,
		wasteSource,
		heatIntensity,
		heatMethod,
	])

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

						<div style={{ marginBottom: 6 }}>
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
								gap: 5,
								padding: "3px 0 6px",
								cursor: "pointer",
								fontSize: "11px",
								color: "#555",
								borderBottom: "1px solid rgba(0,0,0,0.06)",
								marginBottom: 6,
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

						{Object.keys(districtBoundaries).length > 0 && (
							<div style={{ marginBottom: 6 }}>
								<div style={filterLabelStyle}>District Outlines</div>
								<select
									value={showBoundaries ?? ""}
									onChange={(e) =>
										setShowBoundaries(e.target.value ? (e.target.value as BoundaryLayer) : null)
									}
									style={selectStyle}
								>
									<option value="">Off</option>
									{districtBoundaries.council && <option value="council">City Council</option>}
									{districtBoundaries.police && <option value="police">Police</option>}
									{districtBoundaries.state_rep && <option value="state_rep">State Rep</option>}
									{districtBoundaries.state_senate && (
										<option value="state_senate">State Senate</option>
									)}
								</select>
							</div>
						)}

						{dataLayer === "waste" && (
							<div style={{ marginBottom: 8 }}>
								<div style={filterLabelStyle}>Source</div>
								<select
									value={wasteSource}
									onChange={(e) => setWasteSource(e.target.value as WasteSource)}
									style={selectStyle}
								>
									<option value="all">All Reports</option>
									<option value="confirmed">311 Tickets</option>
									<option value="detected">Missed (ML)</option>
								</select>
							</div>
						)}

						<div style={{ marginBottom: 8 }}>
							<div style={filterLabelStyle}>Year</div>
							<select
								value={selYear}
								onChange={(e) => setSelYear(e.target.value)}
								style={selectStyle}
							>
								<option value="all">All Years</option>
								{[...activeYears].reverse().map((yr) => (
									<option
										key={yr}
										value={String(yr)}
										disabled={!availableYears.has(yr)}
										style={!availableYears.has(yr) ? { color: "#bbb" } : undefined}
									>
										{yr}
										{!availableYears.has(yr) ? " (no data)" : ""}
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
									<option
										key={name}
										value={i + 1}
										disabled={!availableMonths.has(i + 1)}
										style={!availableMonths.has(i + 1) ? { color: "#bbb" } : undefined}
									>
										{name}
										{!availableMonths.has(i + 1) ? " (no data)" : ""}
									</option>
								))}
							</select>
						</div>

						<div style={{ marginTop: 8 }}>
							<div style={filterLabelStyle}>Render Method</div>
							<select
								value={heatMethod}
								onChange={(e) => setHeatMethod(e.target.value as HeatMethod)}
								style={selectStyle}
							>
								{HEAT_METHODS.map((m) => (
									<option key={m.value} value={m.value}>
										{m.label} — {m.desc}
									</option>
								))}
							</select>
						</div>

						<div style={{ marginTop: 6 }}>
							<div style={filterLabelStyle}>
								Intensity <span style={{ fontWeight: 400, color: "#999" }}>{heatIntensity}%</span>
							</div>
							<input
								type="range"
								min={1}
								max={100}
								value={heatIntensity}
								onChange={(e) => setHeatIntensity(Number(e.target.value))}
								style={{
									width: "100%",
									accentColor: "#e85a1b",
									cursor: "pointer",
									margin: "2px 0",
								}}
								aria-label="Heat map intensity"
							/>
						</div>

						{councilLabels.length > 0 && (
							<div style={{ marginTop: 5 }}>
								<div style={filterLabelStyle}>Council District</div>
								<select
									value={selCouncil}
									onChange={(e) => {
										const v = Number(e.target.value)
										setSelCouncil(v)
										if (v > 0) setShowBoundaries("council")
									}}
									style={selectStyle}
								>
									<option value={0}>All Districts</option>
									{councilLabels.map((label, i) => (
										<option key={label} value={i + 1}>
											{label}
										</option>
									))}
								</select>
							</div>
						)}

						{policeLabels.length > 0 && (
							<div style={{ marginTop: 5 }}>
								<div style={filterLabelStyle}>Police District</div>
								<select
									value={selPolice}
									onChange={(e) => {
										const v = Number(e.target.value)
										setSelPolice(v)
										if (v > 0) setShowBoundaries("police")
									}}
									style={selectStyle}
								>
									<option value={0}>All Districts</option>
									{policeLabels.map((label, i) => (
										<option key={label} value={i + 1}>
											{label}
										</option>
									))}
								</select>
							</div>
						)}

						{stateRepLabels.length > 0 && (
							<div style={{ marginTop: 5 }}>
								<div style={filterLabelStyle}>State Rep</div>
								<select
									value={selRep}
									onChange={(e) => {
										const v = Number(e.target.value)
										setSelRep(v)
										if (v > 0) setShowBoundaries("state_rep")
									}}
									style={selectStyle}
								>
									<option value={0}>All Districts</option>
									{stateRepLabels.map((label, i) => (
										<option key={label} value={i + 1}>
											{label}
										</option>
									))}
								</select>
							</div>
						)}

						{stateSenateLabels.length > 0 && (
							<div style={{ marginTop: 5 }}>
								<div style={filterLabelStyle}>State Senate</div>
								<select
									value={selSenate}
									onChange={(e) => {
										const v = Number(e.target.value)
										setSelSenate(v)
										if (v > 0) setShowBoundaries("state_senate")
									}}
									style={selectStyle}
								>
									<option value={0}>All Districts</option>
									{stateSenateLabels.map((label, i) => (
										<option key={label} value={i + 1}>
											{label}
										</option>
									))}
								</select>
							</div>
						)}
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
							<strong>{wasteCount.toLocaleString()}</strong>{" "}
							{wasteSource === "confirmed"
								? "confirmed 311 tickets"
								: wasteSource === "detected"
									? "missed reports (ML)"
									: "human waste reports"}
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

			{/* Time scrubber */}
			{timeline.length > 1 && (
				<div
					style={{
						display: "flex",
						alignItems: "center",
						gap: 10,
						marginTop: 12,
						padding: "8px 12px",
						background: "var(--color-surface, #fff)",
						border: "1px solid var(--color-border, #e0e0e0)",
						borderRadius: 8,
						fontSize: 12,
					}}
				>
					<button
						type="button"
						onClick={togglePlay}
						aria-label={isPlaying ? "Pause animation" : "Play timeline animation"}
						style={{
							background: "none",
							border: "1px solid #ccc",
							borderRadius: 4,
							padding: "3px 8px",
							cursor: "pointer",
							fontSize: 14,
							lineHeight: 1,
							color: isPlaying ? "#e85a1b" : "#555",
							flexShrink: 0,
						}}
					>
						{isPlaying ? "\u23F8" : "\u25B6"}
					</button>
					<input
						type="range"
						min={-1}
						max={timeline.length - 1}
						value={timelineIndex}
						onChange={(e) => scrubTo(Number(e.target.value))}
						style={{ flex: 1, accentColor: "#e85a1b", cursor: "pointer" }}
						aria-label="Time scrubber"
					/>
					<span
						style={{
							minWidth: 85,
							textAlign: "right",
							color: "#444",
							fontWeight: 600,
							fontVariantNumeric: "tabular-nums",
						}}
					>
						{timelineIndex < 0
							? "All Time"
							: `${MONTHS[(timeline[timelineIndex]?.month ?? 1) - 1]?.slice(0, 3)} ${timeline[timelineIndex]?.year}`}
					</span>
				</div>
			)}
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
		padding: mobile ? "10px 12px" : "10px 12px",
		minWidth: mobile ? 170 : 150,
		boxShadow: "0 4px 16px rgba(0,0,0,0.1)",
		fontSize: "12px",
		lineHeight: "1.3",
		maxHeight: mobile ? "320px" : "480px",
		overflowY: "auto" as const,
	}
}

const selectStyle: React.CSSProperties = {
	width: "100%",
	padding: "3px 6px",
	fontSize: "12px",
	border: "1px solid #ddd",
	borderRadius: "4px",
	background: "#fff",
	color: "#333",
	cursor: "pointer",
}

const filterLabelStyle: React.CSSProperties = {
	fontWeight: 700,
	fontSize: "10px",
	color: "#444",
	textTransform: "uppercase",
	letterSpacing: "0.05em",
	marginBottom: 3,
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

type HeatMethod =
	| "tableau"
	| "tableau-wide"
	| "tableau-soft"
	| "smooth"
	| "tight"
	| "density"
	| "contrast"
	| "coverage"
	| "fine"

const HEAT_METHODS: { value: HeatMethod; label: string; desc: string }[] = [
	{ value: "tableau", label: "Tableau Classic", desc: "Wide gaussian, high coverage" },
	{ value: "tableau-wide", label: "Tableau Wide", desc: "Extra spread, filled regions" },
	{ value: "tableau-soft", label: "Tableau Soft", desc: "Gentle gradient, max blur" },
	{ value: "smooth", label: "Smooth KDE", desc: "Balanced kernel density" },
	{ value: "tight", label: "Tight Clusters", desc: "Precise hotspots" },
	{ value: "fine", label: "Fine Detail", desc: "Small kernels, sharp edges" },
	{ value: "density", label: "Point Density", desc: "Low blur, stacked dots" },
	{ value: "contrast", label: "High Contrast", desc: "Steep peaks vs flat areas" },
	{ value: "coverage", label: "Coverage Map", desc: "Shows where any data exists" },
]

interface HeatParams {
	radius: number
	blur: number
	maxZoom: number
	minOpacity: number
}

function getHeatParams(zoom: number, method: HeatMethod): HeatParams {
	// Continuous zoom scale factor (1.0 at zoom 13)
	const zoomScale =
		zoom <= 11
			? 0.64
			: zoom <= 12
				? 0.79
				: zoom <= 13
					? 1
					: zoom <= 14
						? 1.25
						: zoom <= 15
							? 1.6
							: 1.96

	// Each method defines base params at zoom 13. Radius and blur scale with zoom.
	const methods: Record<HeatMethod, HeatParams> = {
		// Tableau-style: wide kernels, blur ≈ radius, low maxZoom so sparse areas still show
		tableau: { radius: 45, blur: 40, maxZoom: 14, minOpacity: 0.06 },
		"tableau-wide": { radius: 60, blur: 55, maxZoom: 13, minOpacity: 0.08 },
		"tableau-soft": { radius: 50, blur: 50, maxZoom: 14, minOpacity: 0.05 },
		// Standard options
		smooth: { radius: 32, blur: 28, maxZoom: 16, minOpacity: 0.04 },
		tight: { radius: 16, blur: 10, maxZoom: 18, minOpacity: 0.08 },
		fine: { radius: 20, blur: 14, maxZoom: 17, minOpacity: 0.06 },
		density: { radius: 22, blur: 5, maxZoom: 17, minOpacity: 0.06 },
		contrast: { radius: 28, blur: 20, maxZoom: 15, minOpacity: 0.01 },
		coverage: { radius: 70, blur: 65, maxZoom: 12, minOpacity: 0.1 },
	}

	const p = methods[method]
	return {
		radius: Math.round(p.radius * zoomScale),
		blur: Math.round(p.blur * zoomScale),
		maxZoom: p.maxZoom,
		minOpacity: p.minOpacity,
	}
}

function createHeatLayer(
	pts: number[][],
	gradient: Record<number, string>,
	zoom = 13,
	intensity = 50,
	method: HeatMethod = "tableau",
): L.Layer {
	const params = getHeatParams(zoom, method)
	// Intensity 1-100 scales radius exponentially (0.04x–0.2x). Most usable renders
	// land in ~0.06x–0.13x, so slider middle hits that range; top-end caps well
	// short of the map-covering zone.
	const scale = 0.04 * 5 ** (intensity / 100)
	return (
		L as Record<string, unknown> as {
			heatLayer: (pts: number[][], opts: Record<string, unknown>) => L.Layer
		}
	).heatLayer(pts, {
		radius: Math.round(params.radius * scale),
		blur: Math.round(params.blur * scale),
		maxZoom: params.maxZoom,
		minOpacity: params.minOpacity * scale,
		gradient,
		pane: "heatmapPane",
	})
}
