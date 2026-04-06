import {
	CategoryScale,
	Chart,
	Legend,
	LinearScale,
	LineController,
	LineElement,
	PointElement,
	Tooltip,
} from "chart.js"
import { useEffect, useRef } from "react"

Chart.register(
	LineController,
	LineElement,
	PointElement,
	CategoryScale,
	LinearScale,
	Tooltip,
	Legend,
)
Chart.defaults.font.family = '"Source Sans 3", system-ui, sans-serif'

interface MonthlyTrendProps {
	yearMonthly: Record<string, number[]>
}

const MONTHS_SHORT = [
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec",
]
const COLORS = [
	"#4e79a7",
	"#f28e2b",
	"#e15759",
	"#76b7b2",
	"#59a14f",
	"#edc949",
	"#af7aa1",
	"#ff9da7",
]

export default function MonthlyTrend({ yearMonthly }: MonthlyTrendProps) {
	const canvasRef = useRef<HTMLCanvasElement>(null)
	const chartRef = useRef<Chart | null>(null)

	useEffect(() => {
		if (!canvasRef.current) return

		// Data is released monthly — the most recent non-zero month in the
		// latest year is almost certainly partial, so null it out along with
		// all future months so the line doesn't misleadingly drop.
		const allYears = Object.keys(yearMonthly)
			.map(Number)
			.sort((a, b) => a - b)
		const latestYear = allYears[allYears.length - 1]
		const latestVals = yearMonthly[String(latestYear)] || []
		let lastNonZero = -1
		for (let i = latestVals.length - 1; i >= 0; i--) {
			if (latestVals[i] > 0) {
				lastNonZero = i
				break
			}
		}
		// Keep months up to but NOT including the last non-zero (it's partial)
		const cutoffMonth = lastNonZero > 0 ? lastNonZero - 1 : -1

		const datasets = Object.entries(yearMonthly).map(([yr, vals], i) => {
			let data: (number | null)[] = vals
			if (Number(yr) === latestYear && cutoffMonth >= 0) {
				data = vals.map((v, monthIdx) => (monthIdx > cutoffMonth ? null : v))
			}
			return {
				label: yr,
				data,
				borderColor: COLORS[i % COLORS.length],
				backgroundColor: `${COLORS[i % COLORS.length]}22`,
				borderWidth: 2,
				pointRadius: 3,
				tension: 0.3,
				fill: false,
				spanGaps: false,
			}
		})

		chartRef.current = new Chart(canvasRef.current, {
			type: "line",
			data: { labels: MONTHS_SHORT, datasets },
			options: {
				responsive: true,
				plugins: {
					legend: { labels: { font: { size: 11 }, boxWidth: 12 } },
				},
				scales: {
					x: { ticks: { font: { size: 10 } }, grid: { color: "rgba(0,0,0,0.05)" } },
					y: {
						ticks: { font: { size: 10 } },
						grid: { color: "rgba(0,0,0,0.05)" },
						title: { display: true, text: "Cases", font: { size: 10 } },
					},
				},
			},
		})

		return () => {
			chartRef.current?.destroy()
		}
	}, [yearMonthly])

	return (
		<div className="card" style={{ padding: "20px 20px 16px" }}>
			<div className="card-title">Monthly Trend by Year</div>
			<div style={{ padding: "4px 0" }}>
				<canvas ref={canvasRef} height={180} />
			</div>
		</div>
	)
}
