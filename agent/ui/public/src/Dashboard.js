const { useState, useEffect } = React;

function Dashboard() {
    const [data, setData] = useState({ status: {}, anomalies: {}, causes: {} });
    const chartRef = React.useRef(null);

    useEffect(() => {
        const ws = new WebSocket(`ws://${location.host}/ws`);
        ws.onmessage = (event) => setData(JSON.parse(event.data));
        return () => ws.close();
    }, []);

    useEffect(() => {
        if (!chartRef.current) {
            const ctx = document.getElementById('memoryChart').getContext('2d');
            chartRef.current = new Chart(ctx, {
                type: 'line',
                data: { labels: [], datasets: [] },
                options: { scales: { y: { beginAtZero: true, title: { display: true, text: 'Memory (MB)' } } },
                           plugins: { title: { display: true, text: 'Node Memory Usage' } },
                           animation: false }
            });
        }
        const chart = chartRef.current;
        chart.data.labels.push(new Date().toLocaleTimeString());
        const nodes = Object.keys(data.status);
        if (chart.data.datasets.length === 0) {
            chart.data.datasets = nodes.map(node => ({
                label: node,
                data: [],
                borderColor: `hsl(${Math.random() * 360}, 50%, 50%)`,
                fill: false,
                pointRadius: 0
            }));
        }
        nodes.forEach((node, i) => {
            const mem = data.status[node].startsWith("MEMORY:") ? parseFloat(data.status[node].split(":")[1]) : 0;
            chart.data.datasets[i].data.push(mem);
        });
        if (chart.data.labels.length > 60) {
            chart.data.labels.shift();
            chart.data.datasets.forEach(ds => ds.data.shift());
        }
        chart.update();
    }, [data]);

    return React.createElement('div', null,
        React.createElement('canvas', { id: 'memoryChart', className: 'chart' }),
        React.createElement('div', { className: 'panel' },
            React.createElement('h3', null, 'Anomalies'),
            Object.entries(data.anomalies).map(([k, v]) => 
                React.createElement('p', { key: k }, 
                    `${k.split(":")[0]} at ${new Date(parseInt(k.split(":")[1]) * 1000).toLocaleTimeString()}: ${v.split(":")[0]}`)
            )
        ),
        React.createElement('div', { className: 'panel' },
            React.createElement('h3', null, 'Root Causes'),
            Object.entries(data.causes).map(([k, v]) => 
                React.createElement('p', { key: k }, 
                    `${k.split(":")[0]} at ${new Date(parseInt(k.split(":")[1]) * 1000).toLocaleTimeString()}: ${v}`)
            )
        )
    );
}

ReactDOM.render(React.createElement(Dashboard), document.getElementById('root'));