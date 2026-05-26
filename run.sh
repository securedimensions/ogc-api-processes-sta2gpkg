cat <<'EOF' | python3 sta_to_gpkg.py > out.gpkg 2> export.log
{
  "url":     "https://citiobs.demo.secure-dimensions.de/staplustest/v1.1/Observations",
  "filter":  "phenomenonTime ge 2024-01-01T00:00:00Z",
  "top":     500,
  "timeout": 30,
  "verbose": false,
  "max_observations": 200
}
EOF