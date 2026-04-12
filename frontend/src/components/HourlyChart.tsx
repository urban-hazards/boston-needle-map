import { BarController, BarElement, CategoryScale, Chart, LinearScale, Tooltip } from "chart.js"
import { useEffect, useRef, useState } from "react"

Chart.register(BarController, BarElement, CategoryScale, LinearScale, Tooltip)
Chart.defaults.font.family = '"Source Sans 3", system-ui, sans-serif'

interface DatasetInfo {
	hourly: number[]
	yearHourly: Record<string, number[]>
	years: number[]
}

interface HourlyChartProps {
	datasets: Record<string, DatasetInfo>
}

const HOUR_LABELS = Array.from({ length: 24 }, (_, i) => {
	if (i === 0) return "12a"
	if (i < 12) return `${i}a`
	if (i === 12) return "12p"
	return `${i - 12}p`
})

const TYPE_COLORS: Record<string, string> = {
	Sharps: "#4e79a7",
	Encampments: "#e15759",
	"Human Waste": "#76b7b2",
}

export default function HourlyChart({ datasets }: HourlyChartProps) {
	const canvasRef = useRef<HTMLCanvasElement>(null)
	const chartRef = useRef<Chart | null>(null)
	const [activeType, setActiveType] = useState("Sharps")
	const [activeYear, setActiveYear] = useState("all")

	const datasetInfo = datasets[activeType]
	const yearHourly = datasetInfo.yearHourly ?? {}
	const hourly =
		activeYear === "all"
			? (datasetInfo.hourly ?? Array(24).fill(0))
			: (yearHourly[activeYear] ?? Array(24).fill(0))

	useEffect(() => {
		if (!canvasRef.current) return

		const maxVal = Math.max(...hourly)
		const baseColor = TYPE_COLORS[activeType] || "#4e79a7"
		const colors = hourly.map((v) => {
			const t = maxVal > 0 ? v / maxVal : 0
			if (t > 0.7) return "#cc0000"
			if (t > 0.4) return "#ff8800"
			return baseColor
		})

		chartRef.current = new Chart(canvasRef.current, {
			type: "bar",
			data: {
				labels: HOUR_LABELS,
				datasets: [
					{
						data: hourly,
						backgroundColor: colors,
						borderWidth: 0,
						borderRadius: 2,
					},
				],
			},
			options: {
				responsive: true,
				plugins: { legend: { display: false } },
				scales: {
					x: { ticks: { font: { size: 9 }, maxRotation: 0 }, grid: { display: false } },
					y: { ticks: { font: { size: 10 } }, grid: { color: "rgba(0,0,0,0.05)" } },
				},
			},
		})

		return () => {
			chartRef.current?.destroy()
		}
	}, [hourly, activeType])

	const types = Object.keys(datasets)
	const years = datasetInfo.years ?? []

	return (
		<div className="card" style={{ padding: "20px 20px 16px" }}>
			<div className="card-title">Requests by Hour of Day</div>
			<div style={{ display: "flex", gap: "8px", flexWrap: "wrap", margin: "8px 0" }}>
				{types.map((t) => (
					<button
						key={t}
						type="button"
						aria-label={`Show ${t} hourly data`}
						onClick={() => {
							setActiveType(t)
							setActiveYear("all")
						}}
						style={{
							padding: "3px 10px",
							fontSize: "11px",
							border: `1px solid ${TYPE_COLORS[t] || "#999"}`,
							borderRadius: "12px",
							background: activeType === t ? TYPE_COLORS[t] || "#999" : "transparent",
							color: activeType === t ? "#fff" : TYPE_COLORS[t] || "#999",
							cursor: "pointer",
							fontFamily: "inherit",
							fontWeight: activeType === t ? 600 : 400,
						}}
					>
						{t}
						{t === "Human Waste" && (
							<span style={{ fontSize: "9px", opacity: 0.7, marginLeft: 2 }}> (beta)</span>
						)}
					</button>
				))}
			</div>
			<div style={{ display: "flex", gap: "4px", flexWrap: "wrap", marginBottom: "8px" }}>
				<button
					type="button"
					aria-label="Show all years"
					onClick={() => setActiveYear("all")}
					style={{
						padding: "2px 8px",
						fontSize: "10px",
						border: "1px solid #ccc",
						borderRadius: "10px",
						background: activeYear === "all" ? "#555" : "transparent",
						color: activeYear === "all" ? "#fff" : "#666",
						cursor: "pointer",
						fontFamily: "inherit",
						fontWeight: activeYear === "all" ? 600 : 400,
					}}
				>
					All
				</button>
				{years.map((y) => (
					<button
						key={y}
						type="button"
						aria-label={`Show year ${y}`}
						onClick={() => setActiveYear(String(y))}
						style={{
							padding: "2px 8px",
							fontSize: "10px",
							border: "1px solid #ccc",
							borderRadius: "10px",
							background: activeYear === String(y) ? "#555" : "transparent",
							color: activeYear === String(y) ? "#fff" : "#666",
							cursor: "pointer",
							fontFamily: "inherit",
							fontWeight: activeYear === String(y) ? 600 : 400,
						}}
					>
						{y}
					</button>
				))}
			</div>
			<div style={{ padding: "4px 0" }}>
				<canvas ref={canvasRef} height={120} aria-label="Requests by hour of day bar chart" />
			</div>
		</div>
	)
}
