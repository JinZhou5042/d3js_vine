async function fetchDataAndUpdateTable() {
    let worker_info_csv = "/data/worker_info.csv";

    const response = await fetch(worker_info_csv);
    if (!response.ok) {
        console.error('Failed to fetch data:', response.status, response.statusText);
        return;
    }
    const csvText = await response.text();
    
    Papa.parse(csvText, {
        header: true,
        complete: function(results) {
            const data = results.data;
            const table = document.getElementById('worker-info-table');
            
            // Clear existing rows except the header
            while (table.rows.length > 1) {
                table.deleteRow(1);
            }
            
            // Insert new rows from the fetched data
            data.forEach(item => {
                // skip empty rows
                if (Object.values(item).every(value => value === "")) {
                    return; 
                }

                const row = table.insertRow();
                for (let key in item) {
                    const cell = row.insertCell();
                    cell.textContent = item[key];
                }
            });
        },
        error: function(error) {
            console.error('Error parsing CSV:', error);
        }
    });
}

// Call the function to fetch data and update the table
fetchDataAndUpdateTable();
