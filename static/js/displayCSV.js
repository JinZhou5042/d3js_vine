export function displayCSV(csvFilePath, containerSelector) {
    d3.csv(csvFilePath).then(function(data) {
        var container = d3.select(containerSelector);
        var table = container.append("table");
        var thead = table.append("thead");
        var tbody = table.append("tbody");

        // add the header row
        thead.append("tr")
          .selectAll("th")
          .data(Object.keys(data[0]))
          .enter()
          .append("th")
            .text(function(d) { return d; });

        // create a row for each object in the data
        var rows = tbody.selectAll("tr")
          .data(data)
          .enter()
          .append("tr");

        var cells = rows.selectAll("td")
          .data(function(row) {
            return Object.keys(row).map(function(column) {
              return {value: row[column]};
            });
          })
          .enter()
          .append("td")
            .text(function(d) { return d.value; });
    });
}