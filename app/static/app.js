const REFRESH_MS = 3000;
let throughputChart = null;
let throughputLineChart = null;
const ctx = document.getElementById('throughputChart').getContext('2d');
const lineCtx = document.getElementById('throughputLine').getContext('2d');
let currentAggregation = 'none';

const COLORS = [
  'rgba(255, 99, 132, 0.5)',
  'rgba(54, 162, 235, 0.5)',
  'rgba(255, 206, 86, 0.5)',
  'rgba(75, 192, 192, 0.5)',
  'rgba(153, 102, 255, 0.5)',
  'rgba(255, 159, 64, 0.5)',
  'rgba(199, 199, 199, 0.5)',
  'rgba(83, 102, 255, 0.5)',
  'rgba(255, 99, 255, 0.5)',
  'rgba(99, 255, 132, 0.5)',
  'rgba(255, 204, 102, 0.5)',
  'rgba(102, 255, 204, 0.5)',
  'rgba(204, 102, 255, 0.5)',
  'rgba(255, 153, 204, 0.5)',
  'rgba(0, 204, 153, 0.5)',
  'rgba(204, 0, 153, 0.5)',
  'rgba(153, 204, 0, 0.5)',
  'rgba(102, 153, 255, 0.5)',
  'rgba(255, 102, 153, 0.5)',
  'rgba(153, 255, 102, 0.5)',
  'rgba(255, 102, 102, 0.5)',
  'rgba(102, 255, 255, 0.5)',
  'rgba(255, 255, 102, 0.5)',
  'rgba(102, 102, 255, 0.5)',
  'rgba(255, 102, 204, 0.5)',
  'rgba(0, 153, 204, 0.5)',
  'rgba(255, 153, 51, 0.5)',
  'rgba(204, 255, 102, 0.5)',
  'rgba(204, 102, 0, 0.5)',
  'rgba(153, 51, 255, 0.5)',
  'rgba(0, 255, 128, 0.5)',
  'rgba(255, 51, 51, 0.5)',
  'rgba(51, 255, 204, 0.5)',
  'rgba(255, 255, 51, 0.5)',
  'rgba(51, 153, 255, 0.5)',
  'rgba(255, 51, 153, 0.5)',
  'rgba(0, 204, 204, 0.5)',
  'rgba(255, 204, 0, 0.5)',
  'rgba(153, 204, 255, 0.5)',
  'rgba(255, 153, 153, 0.5)',
  'rgba(204, 255, 255, 0.5)',
  'rgba(255, 204, 204, 0.5)',
  'rgba(204, 204, 255, 0.5)',
  'rgba(255, 255, 204, 0.5)',
  'rgba(204, 255, 204, 0.5)',
  'rgba(255, 204, 153, 0.5)',
  'rgba(153, 255, 255, 0.5)',
  'rgba(204, 153, 255, 0.5)',
  'rgba(204, 255, 153, 0.5)',
  'rgba(255, 153, 255, 0.5)'
];


// Set up aggregation select listener
document.addEventListener('DOMContentLoaded', () => {
  const aggregationSelect = document.getElementById('aggregation-select');
  
  aggregationSelect.addEventListener('change', (event) => {
    currentAggregation = event.target.value;
    fetchAndRender();
  });
});

async function fetchAndRender() {
  try {
    console.log(`Fetching stats with aggregation: ${currentAggregation}`);
    const apiUrl = currentAggregation === 'none' 
      ? '/api/stats' 
      : `/api/stats?aggregate_by=${currentAggregation}`;
      
    const res = await fetch(apiUrl);
    const { global, stats, history, aggregated_by } = await res.json();

    // render global
    const gEl = document.getElementById('global-stats');
    gEl.innerHTML = `
      <li>Total Bots: ${global.bots}</li>
      <li>Processed: ${global.processed} / ${global.received} (${global.progress.toFixed(2)}%)</li>
      <li>In-Flight: ${global.in_flight}</li>
      <li>Empty Polls: ${global.empty_polls}</li>
      <li>Partitions: ${global.partitions}</li>
      <li>Throughput: ${global.throughput.toFixed(2)} jobs/sec</li>
      <li>Elapsed: ${global.elapsed.toFixed(1)} sec</li>
    `;

    // render table
    const tb = document.querySelector('#bots-table tbody');
    tb.innerHTML = stats.map(bot => {
      const ago = ((Date.now()/1000) - bot.timestamp).toFixed(1);
      const botCount = bot.bots_count ? `<span class="bot-count">(${bot.bots_count} bots)</span>` : '';
      
      return `
        <tr>
          <td>${bot.bot_id} ${botCount}</td>
          <td>${bot.ip_address || '-'}</td>
          <td>${bot.processed}</td>
          <td>${bot.erred || 0}</td>
          <td>${bot.received}</td>
          <td>${bot.queue_size || 0}</td>
          <td>${bot.in_flight}</td>
          <td>${bot.throughput.toFixed(2)} /s</td>
          <td>${bot.transactions || "-"}</td>
          <td>${bot.topic || "-"}</td>
          <td>${bot.group_id || "-"}</td>
          <td>${bot.register_at ? new Date(Number(bot.register_at) * 1000).toLocaleString() : "-"}</td>
          <td>${ago}s ago</td>
        </tr>
      `;
    }).join('');

    // prepare bar-chart data
    const labels = stats.map(b => b.bot_id.split(' (')[0]); // Only use the first part before any count
    const data = stats.map(b => +b.throughput.toFixed(2));

    if (!throughputChart) {
      throughputChart = new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets: [{ label: 'Throughput (/s)', data, backgroundColor: COLORS[0] }] },
        options: { scales: { y: { beginAtZero: true } }, animation: { duration: 0 } }
      });
    } else {
      throughputChart.data.labels = labels;
      throughputChart.data.datasets[0].data = data;
      throughputChart.update();
    }

    // For history chart, we always show individual bots (not aggregated)
    const times = history.map(h => new Date(h.timestamp * 1000)
                                      .toLocaleTimeString());
    const botIds = [...new Set(history.flatMap(h =>
      h.stats.map(b => b.bot_id)
    ))];
    
    // Use top 5 bots by throughput for line chart to avoid overcrowding
    const topBotIds = botIds.slice(0, 5);
    
    const lineDatasets = topBotIds.map((id, i) => ({
      label: id,
      data: history.map(h => {
        const bot = h.stats.find(b => b.bot_id === id);
        return bot ? +bot.throughput.toFixed(2) : null;
      }),
      borderColor: COLORS[i % COLORS.length],
      fill: false,
      tension: 0.1
    }));

    if (!throughputLineChart) {
      throughputLineChart = new Chart(lineCtx, {
        type: 'line',
        data: { labels: times, datasets: lineDatasets },
        options: { 
          scales: { y: { beginAtZero: true } }, 
          animation: { duration: 0 } 
        }
      });
    } else {
      throughputLineChart.data.labels = times;
      throughputLineChart.data.datasets = lineDatasets;
      throughputLineChart.update();
    }

  } catch (err) {
    console.error('Failed to load stats', err);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  fetchAndRender();
  setInterval(fetchAndRender, REFRESH_MS);
});