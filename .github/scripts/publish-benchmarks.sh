#!/bin/bash
set -e

# This script publishes benchmark results to GitHub Pages
# Usage: publish-benchmarks.sh <processed-dir> <framework-name>

PROCESSED_DIR=$1
FRAMEWORK=$2

if [ -z "$PROCESSED_DIR" ] || [ -z "$FRAMEWORK" ]; then
  echo "Usage: publish-benchmarks.sh <processed-dir> <framework-name>"
  exit 1
fi

if [ ! -f "$PROCESSED_DIR/queries.txt" ]; then
  echo "Error: $PROCESSED_DIR/queries.txt not found"
  exit 1
fi

# Clone gh-pages branch
git fetch origin gh-pages:gh-pages || git branch gh-pages
git checkout gh-pages

# Process each query
while IFS= read -r query; do
  echo "Processing $query for $FRAMEWORK..."

  BENCH_DIR="bench/${query}/${FRAMEWORK}"
  DATA_FILE="${BENCH_DIR}/data.js"

  mkdir -p "$BENCH_DIR"

  # Read the benchmark data
  if [ ! -f "$PROCESSED_DIR/${query}.json" ]; then
    echo "Warning: $PROCESSED_DIR/${query}.json not found, skipping"
    continue
  fi

  BENCHES=$(cat "$PROCESSED_DIR/${query}.json")

  # Get commit info
  COMMIT_SHA=$(git rev-parse HEAD)
  COMMIT_MSG=$(git log -1 --pretty=format:"%s")
  COMMIT_AUTHOR=$(git log -1 --pretty=format:"%an")
  COMMIT_EMAIL=$(git log -1 --pretty=format:"%ae")
  COMMIT_DATE=$(git log -1 --pretty=format:"%cI")
  TIMESTAMP=$(date +%s%3N)

  # Create/update data.js
  if [ -f "$DATA_FILE" ]; then
    # Append to existing data
    # TODO: Implement proper merging of benchmark data
    echo "Note: Appending not yet implemented, will overwrite"
  fi

  # Create new data.js
  cat > "$DATA_FILE" << EOF
window.BENCHMARK_DATA = {
  "lastUpdate": $TIMESTAMP,
  "repoUrl": "https://github.com/$GITHUB_REPOSITORY",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "name": "$COMMIT_AUTHOR",
            "email": "$COMMIT_EMAIL",
            "username": "$GITHUB_ACTOR"
          },
          "committer": {
            "name": "$COMMIT_AUTHOR",
            "email": "$COMMIT_EMAIL",
            "username": "$GITHUB_ACTOR"
          },
          "id": "$COMMIT_SHA",
          "message": "$COMMIT_MSG",
          "timestamp": "$COMMIT_DATE",
          "url": "https://github.com/$GITHUB_REPOSITORY/commit/$COMMIT_SHA"
        },
        "date": $TIMESTAMP,
        "tool": "customSmallerIsBetter",
        "benches": $BENCHES
      }
    ]
  }
}
EOF

  # Copy index.html if it doesn't exist
  if [ ! -f "${BENCH_DIR}/index.html" ]; then
    # Create a simple index.html that uses benchmark-action's viewer
    cat > "${BENCH_DIR}/index.html" << 'HTMLEOF'
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Benchmark Results</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css" />
    <script src="https://cdn.jsdelivr.net/npm/chart.js@3.9.1/dist/chart.min.js"></script>
    <script src="./data.js"></script>
  </head>
  <body>
    <section class="section">
      <div class="container">
        <h1 class="title">Benchmark Results</h1>
        <div id="charts"></div>
      </div>
    </section>
    <script>
      // Simple chart rendering
      const data = window.BENCHMARK_DATA;
      const container = document.getElementById('charts');

      if (data && data.entries && data.entries.Benchmark) {
        const entries = data.entries.Benchmark;
        const metrics = {};

        // Group by metric name
        entries.forEach(entry => {
          entry.benches.forEach(bench => {
            if (!metrics[bench.name]) {
              metrics[bench.name] = [];
            }
            metrics[bench.name].push({
              date: new Date(entry.date),
              value: bench.value,
              commit: entry.commit.id.substring(0, 7)
            });
          });
        });

        // Create chart for each metric
        Object.keys(metrics).forEach(metricName => {
          const section = document.createElement('div');
          section.className = 'box';
          section.innerHTML = `<h2 class="subtitle">${metricName}</h2><canvas id="chart-${metricName.replace(/\s+/g, '-')}"></canvas>`;
          container.appendChild(section);

          const ctx = section.querySelector('canvas').getContext('2d');
          const chartData = metrics[metricName].sort((a, b) => a.date - b.date);

          new Chart(ctx, {
            type: 'line',
            data: {
              labels: chartData.map(d => d.commit),
              datasets: [{
                label: metricName,
                data: chartData.map(d => d.value),
                borderColor: 'rgb(75, 192, 192)',
                tension: 0.1
              }]
            },
            options: {
              responsive: true,
              plugins: {
                title: {
                  display: true,
                  text: metricName
                }
              }
            }
          });
        });
      }
    </script>
  </body>
</html>
HTMLEOF
  fi

  echo "Published $query to $BENCH_DIR"
done < "$PROCESSED_DIR/queries.txt"

# Commit and push
git add bench/
git commit -m "Update benchmark results for $FRAMEWORK" || echo "No changes to commit"
git push origin gh-pages

# Return to original branch
git checkout -
