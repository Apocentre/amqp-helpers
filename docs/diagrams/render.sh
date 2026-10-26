#!/usr/bin/env bash
# Re-renders every .mmd file in this directory to a PNG with a solid white background,
# so the diagrams look the same in light/dark mode and in viewers that do not render Mermaid
# (PNG rather than SVG: Mermaid SVGs use <foreignObject> text that many viewers drop).
# Requires Node and a local Chrome (set executablePath in puppeteer.config.json).
set -euo pipefail
cd "$(dirname "$0")"
export PUPPETEER_SKIP_DOWNLOAD=1
for f in *.mmd; do
  npx -y @mermaid-js/mermaid-cli -i "$f" -o "${f%.mmd}.png" \
    -c mermaid.config.json -p puppeteer.config.json -b white -s 2
done
