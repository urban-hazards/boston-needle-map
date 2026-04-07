import { BarController, BarElement, CategoryScale, Chart, LinearScale, Tooltip } from "chart.js"
import { useEffect, useRef } from "react"

Chart.register(BarController, BarElement, CategoryScale, LinearScale, Tooltip)
Chart.defaults.font.family = '"Source Sans 3", system-ui, sans-serif'

interface HourlyChartProps {
	hourly: number[]
}

const HOUR_LABELS = Array.from({ length: 24 }, (_, i) => {
	if (i === 0) return "12a"
	if (i < 12) return `${i}a`
	if (i === 12) return "12p"
	return `${i - 12}p`
})

export default function HourlyChart({ hourly }: HourlyChartProps) {
	const canvasRef = useRef<HTMLCanvasElement>(null)
	const chartRef = useRef<Chart | null>(null)

	useEffect(() => {
		if (!canvasRef.current) return

		const maxVal = Math.max(...hourly)
		const colors = hourly.map((v) => {
			const t = v / maxVal
			if (t > 0.7) return "#cc0000"
			if (t > 0.4) return "#ff8800"
			return "#4e79a7"
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
	}, [hourly])

	return (
		<div className="card" style={{ padding: "20px 20px 16px" }}>
			<div className="card-title">Requests by Hour of Day</div>
			<div style={{ padding: "4px 0" }}>
				<canvas ref={canvasRef} height={120} aria-label="Requests by hour of day bar chart" />
			</div>
		</div>
	)
}
