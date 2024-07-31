find "./logs" -mindepth 1 -maxdepth 1 -type d |
while IFS= read -r subdir; do
    echo "=== processing $subdir..."
    python generate_d3_input.py $subdir
    python graph.py $subdir
done