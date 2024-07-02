
export async function plotDAGComponentByID(dagID) {
    try {
        const generalStatisticsDAG = window.generalStatisticsDAG;
        const dag = generalStatisticsDAG.find(d => d.graph_id === dagID.toString());

        if (dag) {
            try {
                let rows = document.querySelectorAll('#general-statistics-dag-table tbody tr');
                rows.forEach(row => {
                    row.style.backgroundColor = 'white';
                });
                const svgElement = d3.select('#dag-components');
                svgElement.selectAll('*').remove();
            
                const svgContent = await d3.svg(`logs/${window.logName}/vine-logs/subgraph_${dagID}.svg`);
                svgElement.node().appendChild(svgContent.documentElement);
                const insertedSVG = svgElement.select('svg');
        
                insertedSVG
                    .attr('preserveAspectRatio', 'xMidYMid meet');

                // highlight the selected row
                rows = document.querySelectorAll('#general-statistics-dag-table tbody tr');
                rows.forEach(row => {
                    if (+row.__data__.graph_id === +dagID) {
                        row.style.backgroundColor = '#f2f2f2';
                    }
                });

            } catch (error) {
                console.error(error);
            }
        } else {
            console.error(`didn't find dag ${dagID}`);
        }
    } catch (error) {
        console.error('error when parsing ', error);
    }
}

window.parent.document.addEventListener('dataLoaded', function() {
    const selectDAG = document.getElementById('dag-id-selector');
    
    const dagIDs = window.generalStatisticsDAG.map(dag => dag.graph_id);

    dagIDs.forEach(dagID => {
        const option = document.createElement('option');
        option.value = dagID;
        option.text = `${dagID}`;
        selectDAG.appendChild(option);
    });

    selectDAG.addEventListener('change', async () => {
        const selectedDAGID = selectDAG.value;
        await plotDAGComponentByID(selectedDAGID);
    });
});

