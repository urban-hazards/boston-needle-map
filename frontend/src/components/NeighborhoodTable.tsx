import { useEffect, useMemo, useState } from "react"

interface NeighborhoodStat {
	name: string
	count: number
	pct: number
	top_street: string
	avg_resp: number
	slug: string
}

interface DatasetHoods {
	hoods: NeighborhoodStat[]
	years: number[]
	label: string
	color: string
}

interface NeighborhoodTableProps {
	datasets: Record<string, DatasetHoods>
}

const TYPE_COLORS: Record<string, string> = {
	Sharps: "#e85a1b",
	Encampments: "#7b2d8e",
	"Human Waste": "#8B6914",
}

function formatNumber(n: number): string {
	return n.toLocaleString()
}

export default function NeighborhoodTable({ datasets }: NeighborhoodTableProps) {
	const types = Object.keys(datasets)
	const [activeType, setActiveType] = useState(types[0])
	const [windowWidth, setWindowWidth] = useState(1024)

	useEffect(() => {
		const update = () => setWindowWidth(window.innerWidth)
		update()
		window.addEventListener("resize", update)
		return () => window.removeEventListener("resize", update)
	}, [])

	const hideDetail = windowWidth < 768
	const hideBar = windowWidth < 480

	const dataset = datasets[activeType]
	const hoods = dataset?.hoods ?? []
	const color = TYPE_COLORS[activeType] || "#e85a1b"

	const maxCount = hoods[0]?.count ?? 1
	const total = useMemo(() => hoods.reduce((s, h) => s + h.count, 0), [hoods])

	return (
		<section className="section" aria-label="Neighborhood breakdown">
			<h2 style={sectionTitleStyle}>Requests by Neighborhood</h2>

			<div style={{ display: "flex", gap: "8px", flexWrap: "wrap", margin: "8px 0 12px" }}>
				{types.map((t) => (
					<button
						key={t}
						type="button"
						onClick={() => setActiveType(t)}
						style={{
							padding: "4px 12px",
							fontSize: "12px",
							border: `1px solid ${TYPE_COLORS[t] || "#999"}`,
							borderRadius: "14px",
							background: activeType === t ? TYPE_COLORS[t] || "#999" : "transparent",
							color: activeType === t ? "#fff" : TYPE_COLORS[t] || "#999",
							cursor: "pointer",
							fontFamily: "inherit",
							fontWeight: activeType === t ? 600 : 400,
						}}
					>
						{t}
					</button>
				))}
			</div>

			{hoods.length > 0 && (
				<p style={descStyle}>
					The table below shows the distribution of <strong>{activeType.toLowerCase()}</strong>{" "}
					requests across Boston neighborhoods. <strong>{hoods[0].name}</strong> leads with{" "}
					<strong>{formatNumber(hoods[0].count)}</strong> requests ({hoods[0].pct}% of{" "}
					{formatNumber(total)} total), with the most reported location being{" "}
					<strong>{hoods[0].top_street}</strong>.
				</p>
			)}

			<div style={cardStyle}>
				<div style={{ overflowX: "auto" }}>
					<table style={tableStyle}>
						<thead>
							<tr>
								<th style={thStyle}>Neighborhood</th>
								{!hideBar && <th style={thStyle}>Volume</th>}
								<th style={{ ...thStyle, textAlign: "right" }}>Requests</th>
								<th style={{ ...thStyle, textAlign: "right" }}>Share</th>
								{!hideDetail && <th style={thStyle}>Top Street</th>}
								{!hideDetail && (
									<th style={{ ...thStyle, textAlign: "right" }}>Avg Response (hrs)</th>
								)}
							</tr>
						</thead>
						<tbody>
							{hoods.map((h, i) => {
								const barWidth = Math.max(2, Math.round((h.count / maxCount) * 100))
								return (
									<tr
										key={h.name}
										style={{
											background: i % 2 === 1 ? "var(--color-surface-alt, #fafafa)" : undefined,
										}}
									>
										<td style={tdStyle}>
											<a
												href={`/neighborhoods/${h.slug}`}
												style={{ color: "inherit", textDecoration: "none" }}
											>
												{h.name}
											</a>
										</td>
										{!hideBar && (
											<td style={{ ...tdStyle, width: "30%" }}>
												<div style={barTrackStyle}>
													<div
														style={{
															height: 8,
															borderRadius: 4,
															background: color,
															width: `${barWidth}%`,
														}}
													/>
												</div>
											</td>
										)}
										<td
											style={{
												...tdStyle,
												textAlign: "right",
												color,
												fontWeight: 600,
												fontVariantNumeric: "tabular-nums",
											}}
										>
											{formatNumber(h.count)}
										</td>
										<td style={{ ...tdStyle, textAlign: "right" }}>{h.pct}%</td>
										{!hideDetail && <td style={{ ...tdStyle, ...streetStyle }}>{h.top_street}</td>}
										{!hideDetail && (
											<td style={{ ...tdStyle, textAlign: "right" }}>{h.avg_resp}</td>
										)}
									</tr>
								)
							})}
						</tbody>
					</table>
				</div>
			</div>
		</section>
	)
}

const sectionTitleStyle: React.CSSProperties = {
	fontSize: "var(--fs-xl, 20px)",
	fontWeight: 700,
	marginBottom: 4,
}

const descStyle: React.CSSProperties = {
	fontSize: "var(--fs-base, 14px)",
	color: "var(--color-text-muted, #666)",
	marginBottom: 16,
	maxWidth: 800,
	lineHeight: 1.6,
}

const cardStyle: React.CSSProperties = {
	background: "var(--color-surface, #fff)",
	border: "1px solid var(--color-border, #e5e5e5)",
	borderRadius: 8,
	padding: "16px",
	boxShadow: "0 1px 3px rgba(0,0,0,0.04)",
}

const tableStyle: React.CSSProperties = {
	width: "100%",
	borderCollapse: "collapse",
	fontSize: "var(--fs-sm, 13px)",
}

const thStyle: React.CSSProperties = {
	textAlign: "left",
	fontSize: "var(--fs-xs, 11px)",
	fontWeight: 700,
	color: "var(--color-text-muted, #666)",
	textTransform: "uppercase",
	letterSpacing: "0.05em",
	padding: "10px 6px",
	borderBottom: "2px solid var(--color-border, #e5e5e5)",
	background: "var(--color-surface-alt, #fafafa)",
}

const tdStyle: React.CSSProperties = {
	padding: "8px 6px",
	borderBottom: "1px solid var(--color-border-light, #f0f0f0)",
	verticalAlign: "middle",
}

const barTrackStyle: React.CSSProperties = {
	background: "var(--color-border-light, #f0f0f0)",
	borderRadius: 4,
	overflow: "hidden",
}

const streetStyle: React.CSSProperties = {
	color: "var(--color-text-muted, #666)",
	maxWidth: 160,
	overflow: "hidden",
	textOverflow: "ellipsis",
	whiteSpace: "nowrap",
}
